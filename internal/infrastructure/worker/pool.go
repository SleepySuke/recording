// Package worker 提供异步流水线的 goroutine 池与内存取消表（详设 §3.2~§3.5）：
// worker 池只负责调度（select 唤醒 / 轮询兜底 / 认领→执行回调），任务事实在 MySQL，
// channel 仅传递唤醒信号。事务与业务规则在 application/processing 与 mysql 适配器。
package worker

import (
	"context"
	"sync"
	"time"

	"recording-transcription/internal/application/ports"
)

// 编译期保证 Pool 实现 Notifier 端口（上传提交后非阻塞唤醒，详设 §3.3）。
var _ ports.Notifier = (*Pool)(nil)

// Pool 固定 N 个 worker 的处理池：每个 worker 拥有一个 cap-1 唤醒 channel，
// 空闲时 select 等待 wake / poll ticker / 退出，不忙循环（详设 §3.2/§3.3）。
type Pool struct {
	workers int
	poll    time.Duration
	process func(claimCtx, runCtx context.Context)

	wakes []chan struct{}
	wg    sync.WaitGroup
	done  chan struct{} // 全部 worker 退出后关闭：停机路径有界等待用（详设 §3.5）

	mu          sync.Mutex
	started     bool
	stopped     bool
	drained     bool
	claimCtx    context.Context    // Start 派生（§3.4/§3.5），Drain 取消即停认领
	cancelClaim context.CancelFunc // claimCtx：drain 第一步取消，停止认领（§3.5 第 1 条）
	cancelRun   context.CancelFunc // Start 传入 runCtx 的派生取消（强停用）
}

// NewPool 创建池：唤醒 channel 在构造时建立，Notify 不依赖 Start。
func NewPool(n int, poll time.Duration, process func(claimCtx, runCtx context.Context)) *Pool {
	wakes := make([]chan struct{}, n)
	for i := range wakes {
		wakes[i] = make(chan struct{}, 1)
	}
	return &Pool{workers: n, poll: poll, process: process, wakes: wakes, done: make(chan struct{})}
}

// Start 以 runCtx 启动全部 worker；启动后立即尝试认领一轮（详设 §3.2）。
// claimCtx 在池内派生（runCtx 子级，§3.4 context 树），Drain 取消它即停认领；
// 清理循环挂独立 cleanupCtx（bootstrap 装配，§3.5 第 1 条）。重复调用无效果。
func (p *Pool) Start(runCtx context.Context) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.started {
		return
	}
	p.started = true
	runChild, cancelRun := context.WithCancel(runCtx)
	claimCtx, cancelClaim := context.WithCancel(runChild)
	p.cancelRun, p.cancelClaim, p.claimCtx = cancelRun, cancelClaim, claimCtx
	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go p.loop(runChild, claimCtx, p.wakes[i])
	}
	// 全部 worker 退出后关闭 done；goroutine 随池生命周期（Stop 后即出）。
	go func() {
		p.wg.Wait()
		close(p.done)
	}()
}

// Started 报告 Start 是否已调用（T11：巡检失败阻止启动时池未启动，停机路径据此
// 跳过对 Done 的等待——未启动的池 done 永不关闭）。
func (p *Pool) Started() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.started
}

// loop 单个 worker 主循环（详设 §3.2/§3.5）：认领受 claimCtx（drain 即停），执行链
// 用 runCtx——taskCtx 在 process 内自 runCtx 派生（§3.4），在途任务在 drain 期间
// 继续执行，仅强停（runCtx 取消）中断。
func (p *Pool) loop(runCtx, claimCtx context.Context, wake <-chan struct{}) {
	defer p.wg.Done()
	ticker := time.NewTicker(p.poll)
	defer ticker.Stop()
	for {
		select {
		case <-runCtx.Done():
			return
		case <-claimCtx.Done():
			return // drain：停止认领新任务（§3.5 第 1 条）
		default:
		}
		p.process(claimCtx, runCtx)
		select {
		case <-runCtx.Done():
			return
		case <-claimCtx.Done():
			return
		case <-wake:
		case <-ticker.C:
		}
	}
}

// Notify 向所有 worker 非阻塞投递唤醒（详设 §3.3）：channel 已满说明已有未消费
// 通知，合并本次；不启动新协程、不等 worker 空闲。channel 永不关闭（避免
// send-on-closed），worker 经 context 退出。
func (p *Pool) Notify() {
	for _, ch := range p.wakes {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// Drain drain 第一步（详设 §3.5 第 1 条）：只取消 claimCtx——停止认领新任务并停
// 共享 claimCtx 的清理循环；在途任务继续以 runCtx 执行。幂等、不等待。
func (p *Pool) Drain() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.started || p.drained {
		return
	}
	p.drained = true
	p.cancelClaim()
}

// Done 全部 worker 退出后关闭；停机路径据此对 WaitGroup 收口做有界等待（§3.5 第 4 条）。
func (p *Pool) Done() <-chan struct{} { return p.done }

// Stop 强停：取消认领与执行，并等待全部 worker 退出（WaitGroup 收口）。幂等。
func (p *Pool) Stop() {
	p.mu.Lock()
	started, stopped := p.started, p.stopped
	p.stopped = true
	cancelClaim, cancelRun := p.cancelClaim, p.cancelRun
	p.mu.Unlock()
	if !started || stopped {
		return
	}
	if cancelClaim != nil {
		cancelClaim()
	}
	if cancelRun != nil {
		cancelRun()
	}
	p.wg.Wait()
}

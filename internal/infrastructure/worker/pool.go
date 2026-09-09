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
	process func(ctx context.Context)

	wakes []chan struct{}
	wg    sync.WaitGroup

	mu          sync.Mutex
	started     bool
	stopped     bool
	cancelClaim context.CancelFunc // claimCtx：drain 第一步取消，停止认领（§3.5 前置，T10 补全）
	cancelRun   context.CancelFunc // Start 传入 runCtx 的派生取消
}

// NewPool 创建池：唤醒 channel 在构造时建立，Notify 不依赖 Start。
func NewPool(n int, poll time.Duration, process func(ctx context.Context)) *Pool {
	wakes := make([]chan struct{}, n)
	for i := range wakes {
		wakes[i] = make(chan struct{}, 1)
	}
	return &Pool{workers: n, poll: poll, process: process, wakes: wakes}
}

// Start 以 runCtx 启动全部 worker；启动后立即尝试认领一轮（详设 §3.2）。
// 重复调用无效果。优雅 drain（停 HTTP→停认领→等在途）由 T10 完成，当前 Stop
// 直接取消认领与执行。
func (p *Pool) Start(runCtx context.Context) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.started {
		return
	}
	p.started = true
	runChild, cancelRun := context.WithCancel(runCtx)
	claimCtx, cancelClaim := context.WithCancel(runChild)
	p.cancelRun, p.cancelClaim = cancelRun, cancelClaim
	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go p.loop(runChild, claimCtx, p.wakes[i])
	}
}

// loop 单个 worker 主循环（详设 §3.2）：执行一轮 → 等待唤醒/轮询/退出。
// 本任务 taskCtx 由 claimCtx 派生（process 内部）；T10 两阶段退出落地后改由
// runCtx 派生，使在途任务在 drain 期间继续执行（详设 §3.5 第 3 条）。
func (p *Pool) loop(ctx, claimCtx context.Context, wake <-chan struct{}) {
	defer p.wg.Done()
	ticker := time.NewTicker(p.poll)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-claimCtx.Done():
			return // drain：停止认领新任务
		default:
		}
		p.process(claimCtx)
		select {
		case <-ctx.Done():
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

// Stop 取消认领（drain 第一步）、取消执行并等待全部 worker 退出；幂等。
func (p *Pool) Stop() {
	p.mu.Lock()
	started, stopped := p.started, p.stopped
	p.stopped = true
	cancelClaim, cancelRun := p.cancelClaim, p.cancelRun
	p.mu.Unlock()
	if !started || stopped {
		return
	}
	cancelClaim()
	cancelRun()
	p.wg.Wait()
}

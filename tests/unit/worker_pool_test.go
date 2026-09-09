package unit

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	domain "recording-transcription/internal/domain/recording"
	"recording-transcription/internal/infrastructure/worker"
)

// 测试依据：T06 步骤 2；设计依据：详设 §3.3（channel 唤醒：非阻塞投递、已满合并）、
// §3.4（内存取消表：锁外调用 Cancel）、§3.2（WaitGroup 管理，worker 不泄漏）。

// TestUT09_NotifyNeverBlocks：唤醒 channel 已满时连续 Notify 立即返回，无阻塞、无 panic
// （UT-09，详设 §3.3——通知不启动新协程、不等 worker 空闲）。
func TestUT09_NotifyNeverBlocks(t *testing.T) {
	p := worker.NewPool(3, time.Second, func(context.Context) {})
	// 不 Start 也必须可通知：唤醒 channel 在 NewPool 创建，Notify 不依赖 worker 运行。
	p.Notify()
	p.Notify() // 每个 cap-1 channel 至此已满

	done := make(chan struct{})
	var calls atomic.Int64
	go func() {
		defer close(done)
		for i := 0; i < 1000; i++ {
			p.Notify()
			calls.Add(1)
		}
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("channel 已满时 Notify 阻塞超过 2 秒")
	}
	if calls.Load() != 1000 {
		t.Errorf("Notify 执行 %d 次, want 1000", calls.Load())
	}
}

// TestCancelTable_RegisterCancelUnregister：登记后 Cancel 取消对应 taskCtx；
// 注销后 Cancel 不再触达；未登记键 Cancel 不 panic；Cancel 返回即证明锁外调用无死锁
// （详设 §3.4）。
func TestCancelTable_RegisterCancelUnregister(t *testing.T) {
	tbl := worker.NewCancelTable()

	key := domain.ExecutionKey{TaskID: "unit-task-1", Attempt: 1}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tbl.Register(key, cancel)
	tbl.Cancel(key)
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("Cancel 后对应 taskCtx 未取消")
	}

	// 注销后 Cancel 不再触达（删除用例按 task_id+attempt 精确命中）。
	var hit atomic.Bool
	key2 := domain.ExecutionKey{TaskID: "unit-task-2", Attempt: 1}
	tbl.Register(key2, func() { hit.Store(true) })
	tbl.Unregister(key2)
	tbl.Cancel(key2) // 同步语义：若误调用，下面断言立即失败
	if hit.Load() {
		t.Error("注销后 Cancel 仍触达了已移除的 CancelFunc")
	}

	// 未登记的键不 panic。
	tbl.Cancel(domain.ExecutionKey{TaskID: "never-registered", Attempt: 9})
}

// TestPool_StopWaitsForWorkers：Start 后每个 worker 立即执行一轮（详设 §3.2
// 「启动后立即尝试认领」），Stop 阻塞等待全部 worker 退出，不泄漏协程。
func TestPool_StopWaitsForWorkers(t *testing.T) {
	const n = 3
	started := make(chan struct{}, n)
	p := worker.NewPool(n, time.Hour, func(context.Context) { started <- struct{}{} })

	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	p.Start(runCtx)

	for i := 0; i < n; i++ {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatalf("第 %d 个 worker 未在 2 秒内执行首轮", i+1)
		}
	}

	done := make(chan struct{})
	go func() { p.Stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop 未在 2 秒内等待 worker 退出（协程泄漏或死锁）")
	}
	// 重复 Stop 幂等。
	p.Stop()
}

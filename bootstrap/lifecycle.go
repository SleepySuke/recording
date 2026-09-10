// lifecycle.go 两阶段退出协调器（详设 §3.5）：信号（signal.NotifyContext）只通知
// 「开始退出」，不直接充当 runCtx。停机序列：① 取消 claimCtx（停认领/停清理）→
// ② 以独立超时 context 调 Server.Shutdown（HTTP drain 与任务 drain 共用
// SHUTDOWN_TIMEOUT 预算）→ ③ 预算内等在途 worker 收尾；超预算则取消 runCtx 强停 →
// ④ 有界等待 worker/清理循环退出 → 关闭数据库。停机取消不伪造业务 failed（§3.4/§3.5），
// 在途任务保持在途留待重启恢复（T11）。
package bootstrap

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"recording-transcription/internal/infrastructure/worker"
)

// Lifecycle 持有两阶段退出所需的全部资源句柄；Drain/Shutdown 均幂等。
type Lifecycle struct {
	timeout       time.Duration // SHUTDOWN_TIMEOUT：HTTP drain 与任务 drain 共用预算（§10）
	logger        *slog.Logger
	srv           *http.Server
	pool          *worker.Pool
	cancelRun     context.CancelFunc // 强停：取消 runCtx 中断在途外呼（§3.5 第 4 条）
	cancelCleanup context.CancelFunc // 阶段 1 停清理循环（与停认领同时，§3.5 第 1 条）
	waitCleanup   func()             // 清理循环 WaitGroup 收口
	closeDB       func() error       // 关闭数据库连接池
	notReady      func()             // 阶段 1 置 /readyz 非就绪（T11；nil = 无门控）

	once     sync.Once
	dbClosed atomic.Bool
}

// NewLifecycle 构造协调器；各句柄由 NewApp 装配传入。notReady 在 Drain 时调用
// （§3.5 第 1 条起服务不再就绪），可为 nil。
func NewLifecycle(timeout time.Duration, logger *slog.Logger, srv *http.Server, pool *worker.Pool,
	cancelRun, cancelCleanup context.CancelFunc, waitCleanup func(), closeDB func() error, notReady func()) *Lifecycle {
	return &Lifecycle{
		timeout: timeout, logger: logger, srv: srv, pool: pool,
		cancelRun: cancelRun, cancelCleanup: cancelCleanup, waitCleanup: waitCleanup, closeDB: closeDB,
		notReady: notReady,
	}
}

// Drain 阶段 1（§3.5 第 1 条）：停认领（取消 claimCtx）并停清理循环，同时置
// /readyz 非就绪（不再接收新流量）；在途任务继续以 runCtx 执行。幂等、不等待。
func (lc *Lifecycle) Drain() {
	lc.pool.Drain()
	lc.cancelCleanup()
	if lc.notReady != nil {
		lc.notReady()
	}
}

// Shutdown 执行完整两阶段退出；幂等，阻塞至完成。所有等待有界（§3.5 第 4 条）：
// 预算内未收尾即取消 runCtx 强停，最终等待超时则放弃并记 ERROR，绝不无限阻塞退出。
func (lc *Lifecycle) Shutdown() {
	lc.once.Do(func() {
		start := time.Now()
		lc.Drain() // 阶段 1：停认领/停清理（未单独 Drain 过时补上）
		lc.logger.Info("退出阶段 1：停止认领与清理，开始 drain（详设 §3.5）",
			slog.Duration("budget", lc.timeout))

		budgetCtx, cancel := context.WithTimeout(context.Background(), lc.timeout)
		defer cancel()
		// 阶段 2：HTTP drain 与任务 drain 共用同一预算（§3.5 第 2 条）。
		if err := lc.srv.Shutdown(budgetCtx); err != nil {
			lc.logger.Warn("HTTP drain 未在预算内完成", slog.Any("err", err))
		}
		// 池未启动（T11 巡检失败阻止启动的分支）无在途可等，直接收口；
		// 未启动池的 done 永不关闭，等待会白耗整个预算。
		if lc.pool.Started() {
			select {
			case <-lc.pool.Done():
				lc.logger.Info("在途任务收尾完成")
			case <-budgetCtx.Done():
				// 阶段 3（超预算）：取消 runCtx 强停在途外呼，等待 worker 收尾（§3.5 第 4 条）。
				lc.logger.Warn("drain 超预算，取消 runCtx 强停在途任务")
				lc.cancelRun()
				lc.waitBounded("worker 强停收尾", lc.pool.Stop)
			}
		}
		lc.waitBounded("清理循环退出", lc.waitCleanup)
		if err := lc.closeDB(); err != nil {
			lc.logger.Error("关闭数据库失败", slog.Any("err", err))
		} else {
			lc.dbClosed.Store(true)
		}
		lc.logger.Info("停机完成", slog.Duration("elapsed", time.Since(start)))
	})
}

// waitBounded 有界等待（§3.5「最终退出等待也有上限」）：超时放弃并记 ERROR。
// 超时后等待协程可能残留至进程退出——弃保底不弃上限（ponytail: 泄漏上限即进程生命期）。
func (lc *Lifecycle) waitBounded(stage string, wait func()) {
	done := make(chan struct{})
	go func() {
		wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(lc.timeout):
		lc.logger.Error("退出等待超时，放弃等待", slog.String("stage", stage))
	}
}

// DBClosed 数据库是否已正常关闭（停机完成度断言用，IT-18）。
func (lc *Lifecycle) DBClosed() bool { return lc.dbClosed.Load() }

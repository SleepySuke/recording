// cleanup.go 低频删除清理循环（详设 §5.3/§10 CLEANUP_INTERVAL，默认 30s）：
// 周期扫描 deleting_at 非空的记录续做「删文件 → 三表清理」；失败由清理用例
// 记日志不阻塞，标记保留下一轮再试。「启动优先恢复删除」属启动恢复（T11）。
package worker

import (
	"context"
	"sync"
	"time"
)

// StartCleanup 在 ctx 下启动清理循环并纳入 wg：每 interval 触发一次 cleanup，
// ctx 取消即退出——生产装配由 Lifecycle 在 drain 第一步取消（与停认领同时，详设
// §3.5 第 1 条「停止认领新任务/新清理工作」）。独立于池的 claimCtx，使「冻结认领、
// 清理续跑」可分别控制。interval 非正时取生产默认 30s（详设 §10）。重复调用各自
// 启动独立循环（生产仅 bootstrap/wire.go 一处）。
func StartCleanup(ctx context.Context, wg *sync.WaitGroup, interval time.Duration, cleanup func(ctx context.Context)) {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				cleanup(ctx)
			}
		}
	}()
}

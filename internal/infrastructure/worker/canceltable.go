package worker

import (
	"context"
	"sync"

	domain "recording-transcription/internal/domain/recording"
)

// CancelTable 内存取消表（详设 §3.4）：Mutex 保护 map[ExecutionKey]CancelFunc。
// worker 执行前登记自己的键、结束删除；删除用例提交 deleting_at 后按 key 取出
// CancelFunc 在锁外调用——不持内存锁访问数据库或网络。数据库锁与条件更新
// 始终保护最终写入，取消只用于尽早中断外呼等待。
type CancelTable struct {
	mu      sync.Mutex
	cancels map[domain.ExecutionKey]context.CancelFunc
}

// NewCancelTable 构造空取消表。
func NewCancelTable() *CancelTable {
	return &CancelTable{cancels: make(map[domain.ExecutionKey]context.CancelFunc)}
}

// Register 登记 (task_id, attempt) 的取消函数；同键重复登记以后者为准。
func (t *CancelTable) Register(key domain.ExecutionKey, cancel context.CancelFunc) {
	t.mu.Lock()
	t.cancels[key] = cancel
	t.mu.Unlock()
}

// Unregister 移除登记；worker 每轮执行结束必须调用（defer）。
func (t *CancelTable) Unregister(key domain.ExecutionKey) {
	t.mu.Lock()
	delete(t.cancels, key)
	t.mu.Unlock()
}

// Cancel 取出该键的 CancelFunc 并在锁外调用；未登记时无操作（worker 尚未登记的
// 窗口由登记后的复查与数据库条件更新兜底，详设 §3.4）。
func (t *CancelTable) Cancel(key domain.ExecutionKey) {
	t.mu.Lock()
	cancel, ok := t.cancels[key]
	t.mu.Unlock()
	if ok {
		cancel()
	}
}

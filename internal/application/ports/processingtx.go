package ports

import (
	"context"
	"errors"

	domain "recording-transcription/internal/domain/recording"
)

// ErrStaleExecution 条件更新未命中（RowsAffected=0，详设 §4.4）：任务已删除、轮次变化
// 或状态不匹配。当前执行必须丢弃结果、清理登记后返回，不得复活任务，也不是错误。
var ErrStaleExecution = errors.New("stale execution")

// ClaimedExecution 事务②认领成功返回的执行上下文（详设 §4.3）。
type ClaimedExecution struct {
	ExecutionKey     domain.ExecutionKey // task_id + attempt，条件更新与取消表的匹配键
	RecordingID      string
	StoragePath      string
	Extension        string
	CreatedRequestID string
}

// ProcessingTx 异步流水线阶段事务端口（事务②认领 / 事务④保存转写，详设 §4.2~§4.4）：
// 认领是短事务，不跨外部调用持有；端口不暴露 *gorm.DB。
type ProcessingTx interface {
	// ClaimNext 事务②：SKIP LOCKED 按 created_at,id 序选 pending → 锁 recording 行
	// 复查 deleting_at 与关联存在性 → 条件更新 transcribing + task_claimed 事件 →
	// 同事务回读新状态后 COMMIT。无可认领返回 (nil, false, nil)；孤儿任务返回
	// 包装 ErrDataInconsistent 的错误（90004，停止认领；就绪阻断在 T11）。
	ClaimNext(ctx context.Context) (*ClaimedExecution, bool, error)
	// SaveTranscription 事务④：条件更新（id + attempt + expected_status=transcribing）
	// 写 transcript 并推进 summarizing，同事务追加 transcription_completed 事件。
	// 条件未命中（含录音删除中）返回 ErrStaleExecution，结果静默丢弃（详设 §4.4）。
	SaveTranscription(ctx context.Context, key domain.ExecutionKey, transcript string) error
}

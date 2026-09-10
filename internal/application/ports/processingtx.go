package ports

import (
	"context"
	"errors"

	"recording-transcription/internal/application/errorcode"
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

// ProcessingTx 异步流水线阶段事务端口（事务②认领 / 事务④保存转写 / 事务⑤完成 /
// 事务③失败，详设 §4.2~§4.4、§7.3）：认领是短事务，不跨外部调用持有；
// 端口不暴露 *gorm.DB。
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
	// CompleteTask 事务⑤：条件更新（id + attempt + expected_status=summarizing）写
	// summary_json 并推进 done，同事务追加 task_completed 事件；事件插入失败整体回滚。
	// 条件未命中返回 ErrStaleExecution，结果丢弃不复活任务（详设 §4.4/§7.3）。
	CompleteTask(ctx context.Context, key domain.ExecutionKey, s domain.Summary) error
	// FailTask 事务③：条件更新（id + attempt + 当前在途状态）写数字错误码与消息并
	// 落 failed，同事务追加 task_failed 事件。转写失败（40001）与 LLM 失败（50001~
	// 50003）共用；条件未命中返回 ErrStaleExecution（详设 §4.1/§4.4）。
	FailTask(ctx context.Context, key domain.ExecutionKey, code errorcode.ErrorCode, msg string) error
}

// RecoveryTx 启动恢复端口（详设 §6.2 步骤 3/5、§6.3 降级变体、§4.6 关联巡检、
// §5.2 孤儿核对的引用集查询）：由 ProcessingTx 的同一 MySQL 适配器实现；
// 恢复在单实例独占窗口执行（无 worker、无上传，§4.1「恢复 vs 一切」）。
type RecoveryTx interface {
	// InspectIntegrity 关联巡检（§6.2 步骤 3）：LEFT JOIN 孤立任务（task 无 recording）
	// 与非删除中缺任务的录音；异常返回包装 ErrDataInconsistent（90004）的错误并携带
	// 资源 ID，不静默修复（§4.6）——调用方据此阻止 ready。
	InspectIntegrity(ctx context.Context) error
	// ResetInFlight 单事务批量重置在途任务（§6.2 步骤 5 / §6.3）：interrupt=false →
	// transcribing/summarizing → pending、attempt+1、清空产物/错误/本轮时间 +
	// task_recovered（details 记 previous_attempt 与原状态）；true → failed/30003 +
	// task_interrupted。更新与事件同一事务，任一失败整体回滚；返回受影响任务数。
	ResetInFlight(ctx context.Context, interrupt bool) (int, error)
	// ListStoragePaths 全部录音的 storage_path 集合（§5.2 孤儿核对的引用集；
	// 查询失败时调用方不得执行清理）。
	ListStoragePaths(ctx context.Context) ([]string, error)
}

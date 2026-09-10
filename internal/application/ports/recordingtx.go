package ports

import (
	"context"
	"errors"

	domain "recording-transcription/internal/domain/recording"
)

// ErrCommitUnknown 数据库事务提交结果未知（详设 §5.2）：连接在 COMMIT 应答前断开，
// 无法断言成功或失败，调用方必须保守处理（保留文件、不推进），不得重试同一逻辑操作。
var ErrCommitUnknown = errors.New("transaction commit result unknown")

// CreateInput 事务①（架构 §3）的领域输入：应用侧预生成全部 ID（UUID），
// 三行在同一事务提交（详设 §4.6 成对创建、§7.3 事件原子性）。
type CreateInput struct {
	Recording domain.Recording
	Task      domain.ProcessingTask
	Event     domain.TaskEvent
}

// CreateOrReuseResult 表示上传事务的线性化结果。Reused 为 true 时三个字段来自
// 已存在的未删除聚合；为 false 时来自 CreateInput 中新建的聚合。
type CreateOrReuseResult struct {
	RecordingID string
	TaskID      string
	Status      domain.TaskStatus
	Reused      bool
}

// RecordingTx 录音聚合的原子事务端口（详设 §2.4）：创建聚合等操作经端口表达，
// MySQL 适配器负责实际事务与锁顺序；端口不暴露 *gorm.DB。
type RecordingTx interface {
	// CreateOrReuseByContentHash 在一个事务中锁定内容哈希、查询未删除聚合，并决定
	// 复用或创建。相同 hash 的请求由持久 hash lock 串行化；复用不写 task_events。
	// 提交结果未知时返回包装 ErrCommitUnknown 的错误。
	CreateOrReuseByContentHash(ctx context.Context, in CreateInput) (CreateOrReuseResult, error)

	// CreateWithTask 事务①：INSERT recordings + tasks(pending) + task_created 事件，
	// 任一失败整体回滚。提交结果未知时返回包装 ErrCommitUnknown 的错误。
	CreateWithTask(ctx context.Context, in CreateInput) error

	// RetryTask 重试事务（详设 §4.5，架构 §3）：锁 tasks→recordings 行 →
	// 锁内复查 status=failed 且未删除 → 条件更新 attempt+1 回 pending、清空上轮
	// 产物与错误 → 同事务 task_retry_accepted 事件 → 回读并返回新轮次号。
	// 任务不存在或录音删除中 → *errorcode.AppError{CodeTaskNotFound}（HTTP 404/30001）；
	// 复查非 failed → *errorcode.AppError{CodeTaskNotRetryable}（HTTP 409/30002）；
	// 行锁串行化并发 retry，同轮重复请求恰好一个成功；提交结果未知返回包装
	// ErrCommitUnknown 的错误。
	RetryTask(ctx context.Context, taskID string) (attempt int, err error)

	// MarkDeleting 删除标记事务（详设 §5.3 步骤 1）：按 §4.2 锁序锁 tasks→recordings
	// 行 → 录音不存在（含已被彻底清理）返回 ok=false（404/20005）→ 条件置
	// deleting_at 并同事务追加 task_delete_requested 事件（序号在任务行锁内分配）。
	// 已标记 deleting_at 同样返回 ok=true（幂等续做，不重复追加事件）；
	// 提交结果未知返回包装 ErrCommitUnknown 的错误。
	MarkDeleting(ctx context.Context, recordingID string) (taskID string, ok bool, err error)

	// PurgeRecording 三表清理事务（详设 §5.3 步骤 4/§4.6）：同一事务按
	// task_events → tasks → recordings 顺序显式 DELETE，任一步失败整体回滚；
	// 提交结果未知返回包装 ErrCommitUnknown 的错误。行已不存在时各步影响 0 行，
	// 幂等成功（重复 DELETE / 清理续做收敛）。
	PurgeRecording(ctx context.Context, recordingID string) error
}

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

// RecordingTx 录音聚合的原子事务端口（详设 §2.4）：创建聚合等操作经端口表达，
// MySQL 适配器负责实际事务与锁顺序；端口不暴露 *gorm.DB。
type RecordingTx interface {
	// CreateWithTask 事务①：INSERT recordings + tasks(pending) + task_created 事件，
	// 任一失败整体回滚。提交结果未知时返回包装 ErrCommitUnknown 的错误。
	CreateWithTask(ctx context.Context, in CreateInput) error
}

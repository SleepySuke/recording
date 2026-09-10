// 重试用例（详设 §4.5、§8.5）：仅 failed 可重试；事务端口负责锁序/复查/条件更新，
// 用例在 COMMIT 成功后 Notify 唤醒 worker（与上传链同一收口，详设 §2.5/§3.3）。
package recording

import (
	"context"
	"errors"
	"log/slog"

	"recording-transcription/internal/application/errorcode"
	"recording-transcription/internal/application/ports"
	domain "recording-transcription/internal/domain/recording"
)

// RetryResult 重试用例输出：202 响应体所需字段（详设 §8.1）。
type RetryResult struct {
	TaskID  string
	Status  domain.TaskStatus // 恒 pending（受理新一轮）
	Attempt int               // 新轮次号 = 上轮 +1
}

// RetryService 手动重试用例（详设 §4.5）。
type RetryService struct {
	tx       ports.RecordingTx
	notifier ports.Notifier // 提交成功后非阻塞唤醒 worker 池（详设 §3.3；丢失由轮询兜底）
	logger   *slog.Logger
}

// NewRetryService 构造重试用例；notifier 为重试链收口的唤醒出口。
func NewRetryService(tx ports.RecordingTx, notifier ports.Notifier, logger *slog.Logger) *RetryService {
	return &RetryService{tx: tx, notifier: notifier, logger: logger}
}

// Retry 受理一次手动重试：事务失败按错误类型映射（409/30002、404/30001 为端口
// 返回的 AppError 原样透传；其余含 ErrCommitUnknown → 503/90002，详设 §8.3）。
func (s *RetryService) Retry(ctx context.Context, taskID string) (RetryResult, error) {
	attempt, err := s.tx.RetryTask(ctx, taskID)
	if err != nil {
		var appErr *errorcode.AppError
		if errors.As(err, &appErr) {
			return RetryResult{}, appErr
		}
		s.logger.Error("重试事务失败", slog.String("task_id", taskID), slog.Any("err", err))
		return RetryResult{}, errorcode.New(errorcode.CodeDatabaseUnavailable, err)
	}
	// COMMIT 成功后非阻塞唤醒（详设 §4.5/§3.3），唤醒丢失由 1s 轮询兜底。
	if s.notifier != nil {
		s.notifier.Notify()
	}
	s.logger.Info("重试已受理",
		slog.String("task_id", taskID), slog.Int("attempt", attempt))
	return RetryResult{TaskID: taskID, Status: domain.StatusPending, Attempt: attempt}, nil
}

// 事务① 与重试事务适配器（架构 §3、详设 §4.5/§4.6/§7.3）：recordings + tasks +
// task_created 三行同一事务提交；重试按 §4.5 锁内复查 failed + 条件更新。
// 插入/更新失败显式回滚，COMMIT 结果未知返回包装 ErrCommitUnknown 的错误。
package mysql

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"recording-transcription/internal/application/errorcode"
	"recording-transcription/internal/application/ports"
	domain "recording-transcription/internal/domain/recording"
	"recording-transcription/internal/infrastructure/logging"
	"recording-transcription/internal/pkg/uuid"
)

// RecordingTxGORM RecordingTx 端口的 MySQL 实现。
type RecordingTxGORM struct {
	db         *gorm.DB
	instanceID string       // 重试事件产生实例标识（详设 §7.2 instance_id）
	logger     *slog.Logger // 重试事件提交后镜像（详设 §7.4）
}

// NewRecordingTx 构造事务端口实现；instanceID 与 logger 用于 RetryTask 的
// 事件构造与提交后镜像（与 ProcessingTx 同一模式）。
func NewRecordingTx(db *gorm.DB, instanceID string, logger *slog.Logger) *RecordingTxGORM {
	return &RecordingTxGORM{db: db, instanceID: instanceID, logger: logger}
}

// CreateWithTask 事务①：INSERT recordings → tasks → task_events，任一失败整体回滚。
// 显式 Begin/Rollback 以区分「明确回滚」与「提交结果未知」（详设 §5.2）。
func (t *RecordingTxGORM) CreateWithTask(ctx context.Context, in ports.CreateInput) error {
	tx := t.db.WithContext(ctx).Begin()
	if err := tx.Error; err != nil {
		return fmt.Errorf("开启事务失败: %w", err)
	}
	if err := tx.Create(toRecordingPO(in.Recording)).Error; err != nil {
		_ = tx.Rollback().Error
		return fmt.Errorf("插入 recordings 失败: %w", err)
	}
	if err := tx.Create(toTaskPO(in.Task)).Error; err != nil {
		_ = tx.Rollback().Error
		return fmt.Errorf("插入 tasks 失败: %w", err)
	}
	if err := tx.Create(toTaskEventPO(in.Event)).Error; err != nil {
		_ = tx.Rollback().Error
		return fmt.Errorf("插入 task_events 失败: %w", err)
	}
	// COMMIT 应答丢失 = 结果未知（详设 §5.2）：调用方保留文件、返回 503/90002。
	if err := tx.Commit().Error; err != nil {
		return fmt.Errorf("%w: %v", ports.ErrCommitUnknown, err)
	}
	return nil
}

// RetryTask 重试事务（详设 §4.5，架构 §3 失败与重试段）：锁 tasks → recordings 行
// （§4.2 锁顺序）→ 锁内复查 status=failed 且未删除 → 条件更新 attempt+1 回 pending、
// 清空上轮 transcript/summary_json/error_code/error_message/started_at/finished_at →
// 同事务 task_retry_accepted 事件（事件序号在任务行锁内分配，§7.2/§7.3）→ 同事务
// 回读新 attempt → COMMIT。复查非 failed（含同轮并发重复请求，行锁串行化后复查）
// 返回 *errorcode.AppError{CodeTaskNotRetryable}（HTTP 409/30002）；任务不存在或
// 录音删除中返回 CodeTaskNotFound（HTTP 404/30001）。
func (t *RecordingTxGORM) RetryTask(ctx context.Context, taskID string) (int, error) {
	tx := t.db.WithContext(ctx).Begin()
	if tx.Error != nil {
		return 0, fmt.Errorf("开启重试事务失败: %w", tx.Error)
	}

	var task TaskPO
	err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", taskID).First(&task).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		_ = tx.Rollback().Error
		return 0, errorcode.New(errorcode.CodeTaskNotFound, nil)
	}
	if err != nil {
		_ = tx.Rollback().Error
		return 0, fmt.Errorf("重试锁定任务失败: %w", err)
	}

	var rec RecordingPO
	err = tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", task.RecordingID).First(&rec).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		_ = tx.Rollback().Error
		return 0, errorcode.New(errorcode.CodeDataInconsistent,
			fmt.Errorf("任务 %s 关联的录音 %s 不存在", taskID, task.RecordingID))
	}
	if err != nil {
		_ = tx.Rollback().Error
		return 0, fmt.Errorf("重试锁定录音失败: %w", err)
	}
	if rec.DeletingAt != nil {
		// 删除中不可见：同任务查询 404 语义（详设 §8.3 30001「任务不存在或其录音删除中」）。
		_ = tx.Rollback().Error
		return 0, errorcode.New(errorcode.CodeTaskNotFound, nil)
	}
	if domain.TaskStatus(task.Status) != domain.StatusFailed {
		// 行锁内复查：同轮并发重复请求串行到达此处，仅第一个见到 failed（详设 §4.5）。
		_ = tx.Rollback().Error
		return 0, errorcode.New(errorcode.CodeTaskNotRetryable,
			fmt.Errorf("任务 %s 当前状态 %s，仅 failed 允许重试", taskID, task.Status))
	}

	newAttempt := task.Attempt + 1
	domTask := domain.ProcessingTask{EventSeq: task.EventSeq}
	seq := domTask.AllocateEventSeq()
	now := time.Now().UTC()
	res := tx.Model(&TaskPO{}).
		Where("id = ? AND attempt = ? AND status = ?", taskID, task.Attempt, string(domain.StatusFailed)).
		Updates(map[string]any{
			"status":        string(domain.StatusPending),
			"attempt":       newAttempt,
			"transcript":    "",
			"summary_json":  nil,
			"error_code":    nil,
			"error_message": "",
			"started_at":    nil,
			"finished_at":   nil,
			"event_seq":     domTask.EventSeq,
			"updated_at":    now,
		})
	if res.Error != nil {
		_ = tx.Rollback().Error
		return 0, fmt.Errorf("重试条件更新失败: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		// 行锁内理论上不会发生；防御迟到写者（与认领事务同语义）→ 409。
		_ = tx.Rollback().Error
		return 0, errorcode.New(errorcode.CodeTaskNotRetryable, nil)
	}

	from, to := domain.StatusFailed, domain.StatusPending
	event := domain.TaskEvent{
		EventID:          uuid.New(),
		TaskID:           task.ID,
		RecordingID:      task.RecordingID,
		EventSeq:         seq,
		Attempt:          newAttempt,
		Kind:             domain.EventTaskRetryAccepted,
		OccurredAt:       now,
		Level:            "INFO",
		FromStatus:       &from,
		ToStatus:         &to,
		CreatedRequestID: task.CreatedRequestID,
		InstanceID:       t.instanceID,
	}
	if err := tx.Create(toTaskEventPO(event)).Error; err != nil {
		_ = tx.Rollback().Error
		return 0, fmt.Errorf("写入 task_retry_accepted 事件失败: %w", err)
	}

	// 同事务回读新轮次（详设 §4.5「SELECT 新状态」），COMMIT 成功才返回。
	var fresh TaskPO
	if err := tx.Where("id = ?", task.ID).First(&fresh).Error; err != nil {
		_ = tx.Rollback().Error
		return 0, fmt.Errorf("重试回读失败: %w", err)
	}
	if err := tx.Commit().Error; err != nil {
		return 0, fmt.Errorf("%w: %v", ports.ErrCommitUnknown, err)
	}
	logging.MirrorEvent(t.logger, event)
	return fresh.Attempt, nil
}

func toRecordingPO(r domain.Recording) *RecordingPO {
	return &RecordingPO{
		ID:               r.ID,
		OriginalFilename: r.OriginalFilename,
		StoragePath:      r.StoragePath,
		Extension:        r.Extension,
		SizeBytes:        r.SizeBytes,
		ContentHash:      r.ContentHash,
		DeletingAt:       r.DeletingAt,
		CreatedAt:        r.CreatedAt,
		UpdatedAt:        r.UpdatedAt,
	}
}

func toTaskPO(tk domain.ProcessingTask) *TaskPO {
	return &TaskPO{
		ID:               tk.ID,
		RecordingID:      tk.RecordingID,
		Status:           string(tk.Status),
		Attempt:          tk.Attempt,
		EventSeq:         tk.EventSeq,
		CreatedRequestID: tk.CreatedRequestID,
		CreatedAt:        tk.CreatedAt,
		UpdatedAt:        tk.UpdatedAt,
		StartedAt:        tk.StartedAt,
		FinishedAt:       tk.FinishedAt,
	}
}

func toTaskEventPO(e domain.TaskEvent) *TaskEventPO {
	return &TaskEventPO{
		EventID:          e.EventID,
		TaskID:           e.TaskID,
		RecordingID:      e.RecordingID,
		EventSeq:         e.EventSeq,
		Attempt:          e.Attempt,
		Event:            string(e.Kind),
		OccurredAt:       e.OccurredAt,
		Level:            e.Level,
		FromStatus:       statusPtr(e.FromStatus),
		ToStatus:         statusPtr(e.ToStatus),
		Stage:            e.Stage,
		RequestID:        e.RequestID,
		CreatedRequestID: e.CreatedRequestID,
		InstanceID:       e.InstanceID,
		ErrorCode:        e.ErrorCode,
		ErrorMessage:     strPtr(e.ErrorMessage),
		Details:          detailsJSON(e.Details),
	}
}

// strPtr 空串存 NULL（task_events.error_message 可空）。
func strPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func statusPtr(s *domain.TaskStatus) *string {
	if s == nil {
		return nil
	}
	v := string(*s)
	return &v
}

// detailsJSON map → JSON 字符串；空 map 落 "{}"（MySQL JSON 列不接受空字符串）。
func detailsJSON(m map[string]any) string {
	if len(m) == 0 {
		return "{}"
	}
	b, err := json.Marshal(m)
	if err != nil {
		return "{}"
	}
	return string(b)
}

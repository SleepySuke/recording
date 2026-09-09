// 事务① 适配器（架构 §3、详设 §4.6/§7.3）：recordings + tasks + task_created 三行
// 同一事务提交；插入失败显式回滚，COMMIT 结果未知返回包装 ErrCommitUnknown 的错误。
package mysql

import (
	"context"
	"encoding/json"
	"fmt"

	"gorm.io/gorm"

	"recording-transcription/internal/application/ports"
	domain "recording-transcription/internal/domain/recording"
)

// RecordingTxGORM RecordingTx 端口的 MySQL 实现。
type RecordingTxGORM struct {
	db *gorm.DB
}

// NewRecordingTx 构造事务端口实现。
func NewRecordingTx(db *gorm.DB) *RecordingTxGORM {
	return &RecordingTxGORM{db: db}
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

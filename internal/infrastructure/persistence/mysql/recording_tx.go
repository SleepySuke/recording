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

// CreateOrReuseByContentHash 按上传幂等设计完成“锁 hash → 查活跃聚合 → 复用或创建”。
// INSERT IGNORE 后的 FOR UPDATE 锁是跨请求、跨 goroutine 的持久互斥点；不能以应用
// 内存锁或单独的先查后插替代。复用分支不写任务或事件。
func (t *RecordingTxGORM) CreateOrReuseByContentHash(ctx context.Context, in ports.CreateInput) (ports.CreateOrReuseResult, error) {
	// 哈希锁行是永久基础设施元数据，先用独立短语句确保其存在。若把 INSERT IGNORE
	// 与紧随其后的 FOR UPDATE 放进每个并发业务事务，首个 hash 会形成 insert-intention
	// 死锁；业务判定本身仍从下面 BEGIN 后的行锁开始。
	if err := t.ensureHashLock(ctx, in.Recording.ContentHash, in.Recording.CreatedAt); err != nil {
		return ports.CreateOrReuseResult{}, err
	}
	tx := t.db.WithContext(ctx).Begin()
	if err := tx.Error; err != nil {
		return ports.CreateOrReuseResult{}, fmt.Errorf("开启上传幂等事务失败: %w", err)
	}
	rollback := func(err error) (ports.CreateOrReuseResult, error) {
		_ = tx.Rollback().Error
		return ports.CreateOrReuseResult{}, err
	}

	var lock RecordingHashLockPO
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("content_hash = ?", in.Recording.ContentHash).First(&lock).Error; err != nil {
		return rollback(fmt.Errorf("锁定内容哈希失败: %w", err))
	}

	// hash 锁内查询，任务与录音一同加锁。删除中资源不参与命中，允许创建替代聚合。
	var existing struct {
		RecordingID string `gorm:"column:recording_id"`
		TaskID      string `gorm:"column:task_id"`
		Status      string `gorm:"column:status"`
	}
	err := tx.Raw(`SELECT r.id AS recording_id, t.id AS task_id, t.status
		FROM recordings r JOIN tasks t ON t.recording_id = r.id
		WHERE r.content_hash = ? AND r.deleting_at IS NULL
		ORDER BY r.created_at, r.id LIMIT 1 FOR UPDATE`, in.Recording.ContentHash).Scan(&existing).Error
	if err != nil {
		return rollback(fmt.Errorf("查询可复用录音失败: %w", err))
	}
	if existing.RecordingID != "" {
		if err := tx.Commit().Error; err != nil {
			return ports.CreateOrReuseResult{}, fmt.Errorf("%w: %v", ports.ErrCommitUnknown, err)
		}
		return ports.CreateOrReuseResult{
			RecordingID: existing.RecordingID,
			TaskID:      existing.TaskID,
			Status:      domain.TaskStatus(existing.Status),
			Reused:      true,
		}, nil
	}

	if err := tx.Create(toRecordingPO(in.Recording)).Error; err != nil {
		return rollback(fmt.Errorf("插入 recordings 失败: %w", err))
	}
	if err := tx.Create(toTaskPO(in.Task)).Error; err != nil {
		return rollback(fmt.Errorf("插入 tasks 失败: %w", err))
	}
	if err := tx.Create(toTaskEventPO(in.Event)).Error; err != nil {
		return rollback(fmt.Errorf("插入 task_events 失败: %w", err))
	}
	if err := tx.Commit().Error; err != nil {
		return ports.CreateOrReuseResult{}, fmt.Errorf("%w: %v", ports.ErrCommitUnknown, err)
	}
	return ports.CreateOrReuseResult{
		RecordingID: in.Recording.ID,
		TaskID:      in.Task.ID,
		Status:      in.Task.Status,
	}, nil
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
			"next_retry_at": nil,
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
		Level:            domain.LevelInfo,
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

// MarkDeleting 删除标记事务（详设 §5.3 步骤 1）：先无锁查关联任务（tasks.recording_id
// UNIQUE，至多一条）→ 按 §4.2 锁序锁 tasks→recordings 行 → 录音不存在返回 ok=false；
// 已标记 deleting_at 幂等返回 ok=true（不重复追加事件，供重复 DELETE 续做）→
// 条件置 deleting_at + 同事务 task_delete_requested 事件（删除事件保持原状态，
// §7.2）+ 任务行 event_seq 推进（§7.2 序号分配器）→ COMMIT 后镜像事件。
func (t *RecordingTxGORM) MarkDeleting(ctx context.Context, recordingID string) (string, bool, error) {
	// 预查只取得 hash/task 作为 hash 锁入口；真正存在性在事务、锁内复查。
	var hint struct {
		ContentHash string `gorm:"column:content_hash"`
		TaskID      string `gorm:"column:task_id"`
	}
	if err := t.db.WithContext(ctx).Raw(`SELECT r.content_hash, t.id AS task_id
		FROM recordings r LEFT JOIN tasks t ON t.recording_id = r.id WHERE r.id = ?`, recordingID).Scan(&hint).Error; err != nil {
		return "", false, fmt.Errorf("查询录音删除标记前置信息失败: %w", err)
	}
	if hint.ContentHash == "" {
		return "", false, nil
	}
	if err := t.ensureHashLock(ctx, hint.ContentHash, time.Now().UTC()); err != nil {
		return "", false, err
	}

	tx := t.db.WithContext(ctx).Begin()
	if tx.Error != nil {
		return "", false, fmt.Errorf("开启删除标记事务失败: %w", tx.Error)
	}
	var hashLock RecordingHashLockPO
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("content_hash = ?", hint.ContentHash).First(&hashLock).Error; err != nil {
		_ = tx.Rollback().Error
		return "", false, fmt.Errorf("锁定删除哈希失败: %w", err)
	}

	var rec RecordingPO
	err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", recordingID).First(&rec).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		_ = tx.Rollback().Error
		return "", false, nil
	}
	if err != nil {
		_ = tx.Rollback().Error
		return "", false, fmt.Errorf("删除标记锁定录音失败: %w", err)
	}

	var task TaskPO
	haveTask := false
	if hint.TaskID != "" {
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", hint.TaskID).First(&task).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			// 任务已被并发清理删除：录音随三表清理同去 → 404 语义（详设 §4.6）。
			_ = tx.Rollback().Error
			return "", false, nil
		}
		if err != nil {
			_ = tx.Rollback().Error
			return "", false, fmt.Errorf("删除标记锁定任务失败: %w", err)
		}
		haveTask = true
	}

	if rec.DeletingAt != nil {
		// 幂等续做：已标记不重复追加 task_delete_requested（详设 §5.3）。
		_ = tx.Rollback().Error
		return task.ID, true, nil
	}

	now := time.Now().UTC()
	res := tx.Model(&RecordingPO{}).
		Where("id = ? AND deleting_at IS NULL", recordingID).
		Updates(map[string]any{"deleting_at": now, "updated_at": now})
	if res.Error != nil {
		_ = tx.Rollback().Error
		return "", false, fmt.Errorf("置 deleting_at 失败: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		// 行锁内理论上不会发生；防御并发标记者 → 幂等续做。
		_ = tx.Rollback().Error
		return task.ID, true, nil
	}

	if haveTask {
		// 事件序号在任务行锁内分配（详设 §7.2/§7.3）；删除事件保持原状态（§7.2）。
		domTask := domain.ProcessingTask{EventSeq: task.EventSeq}
		seq := domTask.AllocateEventSeq()
		if err := tx.Model(&TaskPO{}).Where("id = ?", task.ID).
			Updates(map[string]any{"event_seq": domTask.EventSeq, "updated_at": now}).Error; err != nil {
			_ = tx.Rollback().Error
			return "", false, fmt.Errorf("推进任务 event_seq 失败: %w", err)
		}
		from, to := domain.TaskStatus(task.Status), domain.TaskStatus(task.Status)
		event := domain.TaskEvent{
			EventID:          uuid.New(),
			TaskID:           task.ID,
			RecordingID:      rec.ID,
			EventSeq:         seq,
			Attempt:          task.Attempt,
			Kind:             domain.EventTaskDeleteRequested,
			OccurredAt:       now,
			Level:            domain.LevelInfo,
			FromStatus:       &from,
			ToStatus:         &to,
			CreatedRequestID: task.CreatedRequestID,
			InstanceID:       t.instanceID,
		}
		if err := tx.Create(toTaskEventPO(event)).Error; err != nil {
			_ = tx.Rollback().Error
			return "", false, fmt.Errorf("写入 task_delete_requested 事件失败: %w", err)
		}
		if err := tx.Commit().Error; err != nil {
			return "", false, fmt.Errorf("%w: %v", ports.ErrCommitUnknown, err)
		}
		logging.MirrorEvent(t.logger, event)
		return task.ID, true, nil
	}

	// 无任务行的删除中录音（§4.6 清理容忍路径）：只置标记，无事件可写。
	if err := tx.Commit().Error; err != nil {
		return "", false, fmt.Errorf("%w: %v", ports.ErrCommitUnknown, err)
	}
	return "", true, nil
}

// ensureHashLock 确保持久哈希锁行存在。该行永不删除，独立提交不会暴露任何 recording
// 或 task；后续业务事务通过 SELECT ... FOR UPDATE 取得真正的串行化点。
func (t *RecordingTxGORM) ensureHashLock(ctx context.Context, contentHash string, createdAt time.Time) error {
	if err := t.db.WithContext(ctx).Exec(
		"INSERT IGNORE INTO recording_hash_locks (content_hash, created_at) VALUES (?, ?)", contentHash, createdAt).Error; err != nil {
		return fmt.Errorf("确保内容哈希锁失败: %w", err)
	}
	return nil
}

// PurgeRecording 三表清理事务（详设 §5.3 步骤 4/§4.6）：同一事务按
// task_events → tasks → recordings 显式 DELETE，任一步失败整体回滚（无半删除）；
// 行已不存在时各步影响 0 行，幂等成功。COMMIT 结果未知返回包装 ErrCommitUnknown
// 的错误（调用方保留 deleting_at 标记续做）。
func (t *RecordingTxGORM) PurgeRecording(ctx context.Context, recordingID string) error {
	tx := t.db.WithContext(ctx).Begin()
	if tx.Error != nil {
		return fmt.Errorf("开启三表清理事务失败: %w", tx.Error)
	}
	if err := tx.Where("recording_id = ?", recordingID).Delete(&TaskEventPO{}).Error; err != nil {
		_ = tx.Rollback().Error
		return fmt.Errorf("清理 task_events 失败: %w", err)
	}
	if err := tx.Where("recording_id = ?", recordingID).Delete(&TaskPO{}).Error; err != nil {
		_ = tx.Rollback().Error
		return fmt.Errorf("清理 tasks 失败: %w", err)
	}
	if err := tx.Where("id = ?", recordingID).Delete(&RecordingPO{}).Error; err != nil {
		_ = tx.Rollback().Error
		return fmt.Errorf("清理 recordings 失败: %w", err)
	}
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
		Level:            string(e.Level),
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

// 事务②③④⑤ 适配器（详设 §4.3 认领协议、§4.1 失败/完成出边、§4.4 条件更新、§7.3
// 原子性协议）：认领 = 短事务（SKIP LOCKED 选 pending → 锁 recording 复查 → 条件更新
// + 事件），不跨外部调用持有；阶段写入条件 id+attempt+expected_status，未命中即 stale 丢弃。
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

// ProcessingTxGORM ProcessingTx 端口的 MySQL 实现。
type ProcessingTxGORM struct {
	db         *gorm.DB
	instanceID string
	logger     *slog.Logger
}

// 编译期保证同一适配器实现 RecoveryTx 端口（T11，详设 §6.2）。
var _ ports.RecoveryTx = (*ProcessingTxGORM)(nil)

// NewProcessingTx 构造流水线事务端口实现；instanceID 与 logger 用于事件构造与提交后镜像（§7.4）。
func NewProcessingTx(db *gorm.DB, instanceID string, logger *slog.Logger) *ProcessingTxGORM {
	return &ProcessingTxGORM{db: db, instanceID: instanceID, logger: logger}
}

// ClaimNext 事务②（详设 §4.3 时序图）：锁 pending（SKIP LOCKED，created_at,id 序）→
// 锁 recording 行复查 deleting_at 与关联存在性 → 条件更新 transcribing + task_claimed
// （同事务，序号在行锁内分配）→ 同事务回读 → COMMIT。事务在函数内闭合，绝不跨外部调用。
func (t *ProcessingTxGORM) ClaimNext(ctx context.Context) (*ports.ClaimedExecution, bool, error) {
	tx := t.db.WithContext(ctx).Begin()
	if tx.Error != nil {
		return nil, false, fmt.Errorf("开启认领事务失败: %w", tx.Error)
	}

	// FOR UPDATE SKIP LOCKED：被其他 worker 锁住的候选直接跳过（详设 §4.3）。
	var task TaskPO
	err := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
		Where("status = ?", string(domain.StatusPending)).
		Order("created_at, id").
		First(&task).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		_ = tx.Rollback().Error
		return nil, false, nil
	}
	if err != nil {
		_ = tx.Rollback().Error
		return nil, false, fmt.Errorf("认领候选查询失败: %w", err)
	}

	// 锁顺序 tasks → recordings（详设 §4.2）；锁后复查关联存在性与 deleting_at。
	var rec RecordingPO
	err = tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("id = ?", task.RecordingID).
		First(&rec).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		_ = tx.Rollback().Error
		// 真正的孤儿任务：90004 停止认领（详设 §4.3/§4.6；巡检阻断就绪在 T11）。
		t.logger.Error("孤儿任务，停止认领并告警",
			slog.String("task_id", task.ID), slog.String("recording_id", task.RecordingID))
		return nil, false, fmt.Errorf("%w: 任务 %s 关联的录音 %s 不存在",
			ports.ErrDataInconsistent, task.ID, task.RecordingID)
	}
	if err != nil {
		_ = tx.Rollback().Error
		return nil, false, fmt.Errorf("认领锁定录音失败: %w", err)
	}
	if rec.DeletingAt != nil {
		// 删除中：跳过交给清理（详设 §4.1「DELETE vs 新认领」）。
		_ = tx.Rollback().Error
		return nil, false, nil
	}

	// 事件序号在任务行锁内分配（详设 §7.2/§7.3），与状态同一 UPDATE 落库。
	domTask := domain.ProcessingTask{EventSeq: task.EventSeq}
	seq := domTask.AllocateEventSeq()
	now := time.Now().UTC()
	res := tx.Model(&TaskPO{}).
		Where("id = ? AND attempt = ? AND status = ?", task.ID, task.Attempt, string(domain.StatusPending)).
		Updates(map[string]any{
			"status":     string(domain.StatusTranscribing),
			"event_seq":  domTask.EventSeq,
			"updated_at": now,
			"started_at": now,
		})
	if res.Error != nil {
		_ = tx.Rollback().Error
		return nil, false, fmt.Errorf("认领条件更新失败: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		// 理论上锁内不会发生（SKIP LOCKED 已互斥），防御迟到写者：静默让出。
		_ = tx.Rollback().Error
		return nil, false, nil
	}

	from, to, stage := domain.StatusPending, domain.StatusTranscribing, "transcribing"
	event := domain.TaskEvent{
		EventID:          uuid.New(),
		TaskID:           task.ID,
		RecordingID:      task.RecordingID,
		EventSeq:         seq,
		Attempt:          task.Attempt,
		Kind:             domain.EventTaskClaimed,
		OccurredAt:       now,
		Level:            "INFO",
		FromStatus:       &from,
		ToStatus:         &to,
		Stage:            &stage,
		CreatedRequestID: task.CreatedRequestID,
		InstanceID:       t.instanceID,
	}
	if err := tx.Create(toTaskEventPO(event)).Error; err != nil {
		_ = tx.Rollback().Error
		return nil, false, fmt.Errorf("写入 task_claimed 事件失败: %w", err)
	}

	// 同事务回读新状态/attempt（详设 §4.3），COMMIT 成功才返回执行权。
	var fresh TaskPO
	if err := tx.Where("id = ?", task.ID).First(&fresh).Error; err != nil {
		_ = tx.Rollback().Error
		return nil, false, fmt.Errorf("认领回读失败: %w", err)
	}
	if err := tx.Commit().Error; err != nil {
		return nil, false, fmt.Errorf("%w: %v", ports.ErrCommitUnknown, err)
	}
	logging.MirrorEvent(t.logger, event)
	return &ports.ClaimedExecution{
		ExecutionKey:     domain.ExecutionKey{TaskID: fresh.ID, Attempt: fresh.Attempt},
		RecordingID:      rec.ID,
		StoragePath:      rec.StoragePath,
		Extension:        rec.Extension,
		CreatedRequestID: task.CreatedRequestID,
	}, true, nil
}

// SaveTranscription 事务④（详设 §4.4/§7.3）：锁任务 → 锁录音确认未删除 →
// 条件更新（id + attempt + expected_status=transcribing）写 transcript 并推进
// summarizing → 同事务 transcription_completed → COMMIT。条件未命中返回
// ErrStaleExecution（任务已删除/轮次变化/状态不匹配），结果丢弃不复活任务。
func (t *ProcessingTxGORM) SaveTranscription(ctx context.Context, key domain.ExecutionKey, transcript string) error {
	tx := t.db.WithContext(ctx).Begin()
	if tx.Error != nil {
		return fmt.Errorf("开启事务④失败: %w", tx.Error)
	}

	// §7.3 事务内顺序：锁任务 → 锁录音 → 验证 → 更新 + event_seq → INSERT 事件。
	var task TaskPO
	err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", key.TaskID).First(&task).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		_ = tx.Rollback().Error
		return ports.ErrStaleExecution // 任务已被清理删除
	}
	if err != nil {
		_ = tx.Rollback().Error
		return fmt.Errorf("事务④锁定任务失败: %w", err)
	}
	if task.Attempt != key.Attempt || domain.TaskStatus(task.Status) != domain.StatusTranscribing {
		_ = tx.Rollback().Error
		return ports.ErrStaleExecution
	}

	var rec RecordingPO
	err = tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", task.RecordingID).First(&rec).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		_ = tx.Rollback().Error
		return ports.ErrStaleExecution
	}
	if err != nil {
		_ = tx.Rollback().Error
		return fmt.Errorf("事务④锁定录音失败: %w", err)
	}
	if rec.DeletingAt != nil {
		// 迟到结果只记日志，不复活任务（详设 §4.1「worker 落结果 vs DELETE」）。
		_ = tx.Rollback().Error
		t.logger.Info("录音删除中，转写结果丢弃",
			slog.String("task_id", key.TaskID), slog.Int("attempt", key.Attempt))
		return ports.ErrStaleExecution
	}

	domTask := domain.ProcessingTask{EventSeq: task.EventSeq}
	seq := domTask.AllocateEventSeq()
	now := time.Now().UTC()
	res := tx.Model(&TaskPO{}).
		Where("id = ? AND attempt = ? AND status = ?", key.TaskID, key.Attempt, string(domain.StatusTranscribing)).
		Updates(map[string]any{
			"status":     string(domain.StatusSummarizing),
			"transcript": transcript,
			"event_seq":  domTask.EventSeq,
			"updated_at": now,
		})
	if res.Error != nil {
		_ = tx.Rollback().Error
		return fmt.Errorf("事务④条件更新失败: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		_ = tx.Rollback().Error
		return ports.ErrStaleExecution
	}

	from, to, stage := domain.StatusTranscribing, domain.StatusSummarizing, "transcribing"
	event := domain.TaskEvent{
		EventID:          uuid.New(),
		TaskID:           task.ID,
		RecordingID:      task.RecordingID,
		EventSeq:         seq,
		Attempt:          key.Attempt,
		Kind:             domain.EventTranscriptionCompleted,
		OccurredAt:       now,
		Level:            "INFO",
		FromStatus:       &from,
		ToStatus:         &to,
		Stage:            &stage,
		CreatedRequestID: task.CreatedRequestID,
		InstanceID:       t.instanceID,
	}
	if err := tx.Create(toTaskEventPO(event)).Error; err != nil {
		_ = tx.Rollback().Error
		return fmt.Errorf("写入 transcription_completed 事件失败: %w", err)
	}
	if err := tx.Commit().Error; err != nil {
		return fmt.Errorf("%w: %v", ports.ErrCommitUnknown, err)
	}
	logging.MirrorEvent(t.logger, event)
	return nil
}

// CompleteTask 事务⑤（详设 §4.4/§7.3，架构 §3）：锁任务 → 锁录音确认未删除 →
// 条件更新（id + attempt + expected_status=summarizing）写 summary_json 并推进 done
// （finished_at 同批落库）→ 同事务 task_completed → COMMIT。摘要解析在事务外完成、
// 校验通过才进事务；条件未命中返回 ErrStaleExecution，结果丢弃不复活任务。
func (t *ProcessingTxGORM) CompleteTask(ctx context.Context, key domain.ExecutionKey, s domain.Summary) error {
	tx := t.db.WithContext(ctx).Begin()
	if tx.Error != nil {
		return fmt.Errorf("开启事务⑤失败: %w", tx.Error)
	}

	var task TaskPO
	err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", key.TaskID).First(&task).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		_ = tx.Rollback().Error
		return ports.ErrStaleExecution
	}
	if err != nil {
		_ = tx.Rollback().Error
		return fmt.Errorf("事务⑤锁定任务失败: %w", err)
	}
	if task.Attempt != key.Attempt || domain.TaskStatus(task.Status) != domain.StatusSummarizing {
		_ = tx.Rollback().Error
		return ports.ErrStaleExecution
	}

	var rec RecordingPO
	err = tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", task.RecordingID).First(&rec).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		// 录音行已被删除清理（T07 评审强修，与事务④同语义）：迟到结果静默丢弃，
		// 不得当硬错误触发 §5.5 重试与停池。
		_ = tx.Rollback().Error
		return ports.ErrStaleExecution
	}
	if err != nil {
		_ = tx.Rollback().Error
		return fmt.Errorf("事务⑤锁定录音失败: %w", err)
	}
	if rec.DeletingAt != nil {
		// 迟到结果只记日志，不复活任务（详设 §4.1「worker 落结果 vs DELETE」）。
		_ = tx.Rollback().Error
		t.logger.Info("录音删除中，摘要结果丢弃",
			slog.String("task_id", key.TaskID), slog.Int("attempt", key.Attempt))
		return ports.ErrStaleExecution
	}

	// summary_json 列形状与 domain.ParseSummary 严格对齐（小写下划线键）；
	// 领域 Summary 无 JSON 标签，不能直接 Marshal。
	summaryJSON, err := json.Marshal(struct {
		Summary   string   `json:"summary"`
		KeyPoints []string `json:"key_points"`
		Todos     []string `json:"todos"`
	}{s.Summary, s.KeyPoints, s.Todos})
	if err != nil {
		_ = tx.Rollback().Error
		return fmt.Errorf("事务⑤序列化摘要失败: %w", err)
	}

	domTask := domain.ProcessingTask{EventSeq: task.EventSeq}
	seq := domTask.AllocateEventSeq()
	now := time.Now().UTC()
	res := tx.Model(&TaskPO{}).
		Where("id = ? AND attempt = ? AND status = ?", key.TaskID, key.Attempt, string(domain.StatusSummarizing)).
		Updates(map[string]any{
			"status":       string(domain.StatusDone),
			"summary_json": string(summaryJSON),
			"event_seq":    domTask.EventSeq,
			"updated_at":   now,
			"finished_at":  now,
		})
	if res.Error != nil {
		_ = tx.Rollback().Error
		return fmt.Errorf("事务⑤条件更新失败: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		_ = tx.Rollback().Error
		return ports.ErrStaleExecution
	}

	from, to, stage := domain.StatusSummarizing, domain.StatusDone, "summarizing"
	event := domain.TaskEvent{
		EventID:          uuid.New(),
		TaskID:           task.ID,
		RecordingID:      task.RecordingID,
		EventSeq:         seq,
		Attempt:          key.Attempt,
		Kind:             domain.EventTaskCompleted,
		OccurredAt:       now,
		Level:            "INFO",
		FromStatus:       &from,
		ToStatus:         &to,
		Stage:            &stage,
		CreatedRequestID: task.CreatedRequestID,
		InstanceID:       t.instanceID,
	}
	if err := tx.Create(toTaskEventPO(event)).Error; err != nil {
		_ = tx.Rollback().Error
		return fmt.Errorf("写入 task_completed 事件失败: %w", err)
	}
	if err := tx.Commit().Error; err != nil {
		return fmt.Errorf("%w: %v", ports.ErrCommitUnknown, err)
	}
	logging.MirrorEvent(t.logger, event)
	return nil
}

// FailTask 事务③（详设 §4.1 transcribing/summarizing→failed、§4.4/§7.3，架构 §3）：
// 锁任务 → 校验 attempt 与在途状态（transcribing/summarizing 均可失败，转写失败 40001
// 与 LLM 失败 50001~50003 共用）→ 锁录音确认未删除 → 条件更新写 error_code/error_message
// 并落 failed（finished_at 同批）→ 同事务 task_failed → COMMIT。条件未命中返回
// ErrStaleExecution，结果丢弃不复活任务。
func (t *ProcessingTxGORM) FailTask(ctx context.Context, key domain.ExecutionKey, code errorcode.ErrorCode, msg string) error {
	tx := t.db.WithContext(ctx).Begin()
	if tx.Error != nil {
		return fmt.Errorf("开启事务③失败: %w", tx.Error)
	}

	var task TaskPO
	err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", key.TaskID).First(&task).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		_ = tx.Rollback().Error
		return ports.ErrStaleExecution
	}
	if err != nil {
		_ = tx.Rollback().Error
		return fmt.Errorf("事务③锁定任务失败: %w", err)
	}
	if task.Attempt != key.Attempt {
		_ = tx.Rollback().Error
		return ports.ErrStaleExecution
	}
	fromStatus := domain.TaskStatus(task.Status)
	if fromStatus != domain.StatusTranscribing && fromStatus != domain.StatusSummarizing {
		// 已终态（done/failed）或未认领（pending）：迟到失败静默丢弃。
		_ = tx.Rollback().Error
		return ports.ErrStaleExecution
	}

	var rec RecordingPO
	err = tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", task.RecordingID).First(&rec).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		// 录音行已被删除清理（T07 评审强修，与事务④同语义）：迟到结果静默丢弃，
		// 不得当硬错误触发 §5.5 重试与停池。
		_ = tx.Rollback().Error
		return ports.ErrStaleExecution
	}
	if err != nil {
		_ = tx.Rollback().Error
		return fmt.Errorf("事务③锁定录音失败: %w", err)
	}
	if rec.DeletingAt != nil {
		_ = tx.Rollback().Error
		t.logger.Info("录音删除中，失败结果丢弃",
			slog.String("task_id", key.TaskID), slog.Int("attempt", key.Attempt))
		return ports.ErrStaleExecution
	}

	domTask := domain.ProcessingTask{EventSeq: task.EventSeq}
	seq := domTask.AllocateEventSeq()
	now := time.Now().UTC()
	res := tx.Model(&TaskPO{}).
		Where("id = ? AND attempt = ? AND status = ?", key.TaskID, key.Attempt, string(fromStatus)).
		Updates(map[string]any{
			"status":        string(domain.StatusFailed),
			"error_code":    int(code),
			"error_message": msg,
			"event_seq":     domTask.EventSeq,
			"updated_at":    now,
			"finished_at":   now,
		})
	if res.Error != nil {
		_ = tx.Rollback().Error
		return fmt.Errorf("事务③条件更新失败: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		_ = tx.Rollback().Error
		return ports.ErrStaleExecution
	}

	to, stage := domain.StatusFailed, string(fromStatus)
	codeInt := int(code)
	event := domain.TaskEvent{
		EventID:          uuid.New(),
		TaskID:           task.ID,
		RecordingID:      task.RecordingID,
		EventSeq:         seq,
		Attempt:          key.Attempt,
		Kind:             domain.EventTaskFailed,
		OccurredAt:       now,
		Level:            "ERROR",
		FromStatus:       &fromStatus,
		ToStatus:         &to,
		Stage:            &stage,
		CreatedRequestID: task.CreatedRequestID,
		InstanceID:       t.instanceID,
		ErrorCode:        &codeInt,
		ErrorMessage:     msg,
	}
	if err := tx.Create(toTaskEventPO(event)).Error; err != nil {
		_ = tx.Rollback().Error
		return fmt.Errorf("写入 task_failed 事件失败: %w", err)
	}
	if err := tx.Commit().Error; err != nil {
		return fmt.Errorf("%w: %v", ports.ErrCommitUnknown, err)
	}
	logging.MirrorEvent(t.logger, event)
	return nil
}

// InspectIntegrity 关联巡检（详设 §6.2 步骤 3、§4.6）：LEFT JOIN 检查孤立任务
// （tasks 无对应 recordings）与非删除中缺任务的录音（recordings.deleting_at IS NULL
// 且无 tasks 行）；删除中缺任务允许继续幂等清理，不算异常。异常返回包装
// ErrDataInconsistent 的错误并携带资源 ID，不静默修复——调用方记录 90004 并阻止
// ready。巡检只读，不加锁、不写库。
func (t *ProcessingTxGORM) InspectIntegrity(ctx context.Context) error {
	var orphans []struct {
		ID          string `gorm:"column:id"`
		RecordingID string `gorm:"column:recording_id"`
	}
	if err := t.db.WithContext(ctx).Raw(
		`SELECT t.id AS id, t.recording_id AS recording_id
		 FROM tasks t LEFT JOIN recordings r ON r.id = t.recording_id
		 WHERE r.id IS NULL`,
	).Scan(&orphans).Error; err != nil {
		return fmt.Errorf("巡检孤立任务失败: %w", err)
	}

	var missing []struct {
		ID string `gorm:"column:id"`
	}
	if err := t.db.WithContext(ctx).Raw(
		`SELECT r.id AS id
		 FROM recordings r LEFT JOIN tasks t ON t.recording_id = r.id
		 WHERE r.deleting_at IS NULL AND t.id IS NULL`,
	).Scan(&missing).Error; err != nil {
		return fmt.Errorf("巡检缺任务录音失败: %w", err)
	}

	if len(orphans) == 0 && len(missing) == 0 {
		return nil
	}
	ids := make([]string, 0, len(orphans)+len(missing))
	for _, o := range orphans {
		ids = append(ids, "task="+o.ID)
	}
	for _, m := range missing {
		ids = append(ids, "recording="+m.ID)
	}
	return fmt.Errorf("%w: 孤立任务 %d 个、非删除中缺任务录音 %d 个: %v",
		ports.ErrDataInconsistent, len(orphans), len(missing), ids)
}

// ResetInFlight 单事务批量重置在途任务（详设 §6.2 步骤 5 / §6.3 降级变体）：
// 读取在途行（事件需逐任务 previous 状态与轮次）→ 每任务经领域 TransitionForRecovery
// 校验出边 → 单条按状态的批量更新（防御性条件）→ 同事务逐任务追加 task_recovered
// （reset）或 task_interrupted（interrupt）→ COMMIT。任一步失败整体回滚并阻止启动
// （§6.2 步骤 5）。批量为单条 UPDATE、绕过 Transition 的 attempt 校验：启动恢复处于
// 单实例独占窗口（无 worker、无并发写者，详设 §4.1「恢复 vs 一切」），attempt 必然
// 匹配；WHERE 的状态条件即防御性复核。
func (t *ProcessingTxGORM) ResetInFlight(ctx context.Context, interrupt bool) (int, error) {
	tx := t.db.WithContext(ctx).Begin()
	if tx.Error != nil {
		return 0, fmt.Errorf("开启重置事务失败: %w", tx.Error)
	}

	var tasks []TaskPO
	if err := tx.Where("status IN ?", []string{string(domain.StatusTranscribing), string(domain.StatusSummarizing)}).
		Order("id").Find(&tasks).Error; err != nil {
		_ = tx.Rollback().Error
		return 0, fmt.Errorf("扫描在途任务失败: %w", err)
	}
	if len(tasks) == 0 {
		_ = tx.Rollback().Error
		return 0, nil
	}

	// 领域校验（详设 §4.1 恢复两行）：出边合法性以状态机为权威。
	target := domain.StatusPending
	if interrupt {
		target = domain.StatusFailed
	}
	events := make([]domain.TaskEvent, 0, len(tasks))
	now := time.Now().UTC()
	for i := range tasks {
		from := domain.TaskStatus(tasks[i].Status)
		// 事件序号沿用任务行分配器（详设 §7.2）：自已提交的 event_seq 递增。
		domTask := domain.ProcessingTask{Status: from, EventSeq: tasks[i].EventSeq}
		if err := domTask.TransitionForRecovery(target); err != nil {
			_ = tx.Rollback().Error
			return 0, fmt.Errorf("任务 %s 恢复出边校验失败: %w", tasks[i].ID, err)
		}
		seq := domTask.AllocateEventSeq()

		eventAttempt := tasks[i].Attempt
		kind, level := domain.EventTaskRecovered, domain.LevelInfo
		var code *int
		msg := ""
		if interrupt {
			kind, level = domain.EventTaskInterrupted, domain.LevelWarn
			c := int(errorcode.CodeServiceInterrupted)
			code, msg = &c, errorcode.CodeServiceInterrupted.Message()
		} else {
			eventAttempt++ // reset 进入新轮次（§6.2 步骤 5 attempt+1）
		}
		to := target
		events = append(events, domain.TaskEvent{
			EventID:          uuid.New(),
			TaskID:           tasks[i].ID,
			RecordingID:      tasks[i].RecordingID,
			EventSeq:         seq,
			Attempt:          eventAttempt,
			Kind:             kind,
			OccurredAt:       now,
			Level:            level,
			FromStatus:       &from,
			ToStatus:         &to,
			CreatedRequestID: tasks[i].CreatedRequestID,
			InstanceID:       t.instanceID,
			ErrorCode:        code,
			ErrorMessage:     msg,
			Details: map[string]any{
				"previous_attempt": tasks[i].Attempt,
				"previous_status":  tasks[i].Status,
			},
		})
	}

	// 单条批量更新（详设 §6.2 步骤 5 SQL 形态），状态条件为防御性复核。
	updates := map[string]any{
		"status":     string(target),
		"event_seq":  gorm.Expr("event_seq + 1"),
		"updated_at": now,
	}
	if interrupt {
		updates["error_code"] = int(errorcode.CodeServiceInterrupted)
		updates["error_message"] = errorcode.CodeServiceInterrupted.Message()
		updates["finished_at"] = now
	} else {
		updates["attempt"] = gorm.Expr("attempt + 1")
		updates["transcript"] = nil
		updates["summary_json"] = nil
		updates["error_code"] = nil
		updates["error_message"] = nil
		updates["started_at"] = nil
		updates["finished_at"] = nil
	}
	res := tx.Model(&TaskPO{}).
		Where("status IN ?", []string{string(domain.StatusTranscribing), string(domain.StatusSummarizing)}).
		Updates(updates)
	if res.Error != nil {
		_ = tx.Rollback().Error
		return 0, fmt.Errorf("批量重置在途任务失败: %w", res.Error)
	}
	if res.RowsAffected != int64(len(tasks)) {
		// 独占窗口内不应发生（无并发写者）；防御回滚（§6.2「任一失败整体回滚」）。
		_ = tx.Rollback().Error
		return 0, fmt.Errorf("批量重置影响 %d 行, want %d（独占窗口内出现并发写者）", res.RowsAffected, len(tasks))
	}

	for i := range events {
		if err := tx.Create(toTaskEventPO(events[i])).Error; err != nil {
			_ = tx.Rollback().Error
			return 0, fmt.Errorf("写入 %s 事件失败: %w", events[i].Kind, err)
		}
	}
	if err := tx.Commit().Error; err != nil {
		return 0, fmt.Errorf("%w: %v", ports.ErrCommitUnknown, err)
	}
	for i := range events {
		logging.MirrorEvent(t.logger, events[i])
	}
	return len(tasks), nil
}

// ListStoragePaths 全部录音的 storage_path（详设 §5.2 孤儿核对引用集）。
func (t *ProcessingTxGORM) ListStoragePaths(ctx context.Context) ([]string, error) {
	var paths []string
	if err := t.db.WithContext(ctx).Model(&RecordingPO{}).Pluck("storage_path", &paths).Error; err != nil {
		return nil, fmt.Errorf("查询 storage_path 引用集失败: %w", err)
	}
	return paths, nil
}

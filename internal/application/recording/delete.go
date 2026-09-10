// 删除用例（详设 §5.3 删除流程图）与低频清理用例（详设 §5.3/§10 CLEANUP_INTERVAL）：
// 标记事务（MarkDeleting）→ 取消在途执行（锁外调用，§3.4）→ 删文件（不存在视为
// 成功）→ 三表清理事务（PurgeRecording）→ task_deleted 仅写文件日志（§7.5）→ 204；
// 文件/清理失败 503/20006 保留标记，供重复 DELETE 或清理循环续做。
package recording

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"recording-transcription/internal/application/errorcode"
	"recording-transcription/internal/application/ports"
	domain "recording-transcription/internal/domain/recording"
	"recording-transcription/internal/infrastructure/logging"
	"recording-transcription/internal/pkg/uuid"
)

// ExecutionCanceller 内存取消表的删除侧最小接口（详设 §3.4）：按 task_id 取出
// 在途执行的 CancelFunc 并在锁外调用（应用层只依赖该接口，不依赖具体设施）。
type ExecutionCanceller interface {
	CancelTask(taskID string)
}

// DeleteService 删除与后台清理用例（详设 §5.3）。
type DeleteService struct {
	tx         ports.RecordingTx
	query      ports.DeletingQuery
	store      ports.FileStore
	canceller  ExecutionCanceller // MarkDeleting 提交后锁外取消失在途执行（§3.4）
	logger     *slog.Logger
	instanceID string // task_deleted 文件日志的实例标识（详设 §7.2）
}

// NewDeleteService 构造删除用例；query 与查询接口由同一 MySQL 适配器实现，
// canceller 与 worker 池共用同一取消表实例。
func NewDeleteService(tx ports.RecordingTx, query ports.DeletingQuery, store ports.FileStore,
	canceller ExecutionCanceller, logger *slog.Logger, instanceID string) *DeleteService {
	return &DeleteService{tx: tx, query: query, store: store, canceller: canceller,
		logger: logger, instanceID: instanceID}
}

// Delete 执行 DELETE /v1/recordings/:id（详设 §5.3）：任意状态可删；不存在/已完全
// 删除 → 404/20005；标记/文件/清理失败 → 503/20006（保留 deleting_at 续做）；
// 成功 → nil（接口层 204）。错误一律包装 *errorcode.AppError。
func (s *DeleteService) Delete(ctx context.Context, recordingID string) error {
	// 步骤 1：标记事务（deleting_at + task_delete_requested，此后查询隐藏该录音）。
	if _, ok, err := s.tx.MarkDeleting(ctx, recordingID); err != nil {
		if errors.Is(err, ports.ErrCommitUnknown) {
			s.logger.Error("删除标记提交结果未知，保留现状待核实",
				slog.String("recording_id", recordingID), slog.Any("err", err))
		}
		return errorcode.New(errorcode.CodeDatabaseUnavailable, err)
	} else if !ok {
		return errorcode.New(errorcode.CodeRecordingNotFound, nil)
	}

	// 清理视图（文件路径 + task_deleted 所需任务元数据）；并发清理已先行完成时
	// found=false —— 资源已消失，DELETE 幂等收敛为 204（详设 §8.5 DELETE 行）。
	row, found, err := s.query.GetDeleting(ctx, recordingID)
	if err != nil {
		return errorcode.New(errorcode.CodeDatabaseUnavailable, err)
	}
	if !found {
		return nil
	}
	return s.finalize(ctx, row)
}

// CleanupPending 低频清理循环体（详设 §5.3/§10）：扫描 deleting_at 非空 → 逐个
// 续做清理；失败记日志不阻塞后续（标记保留，下一轮或重复 DELETE 再续做）。
func (s *DeleteService) CleanupPending(ctx context.Context) {
	rows, err := s.query.ListDeleting(ctx)
	if err != nil {
		s.logger.Error("清理扫描失败", slog.Any("err", err))
		return
	}
	for _, row := range rows {
		if err := s.finalize(ctx, row); err != nil {
			s.logger.Warn("删除清理未完成，保留标记续做",
				slog.String("recording_id", row.RecordingID), slog.Any("err", err))
		}
	}
}

// finalize 步骤 2～4（详设 §5.3）：取消在途执行（锁外，§3.4）→ 删文件（不存在视为
// 成功）→ 三表清理事务 → task_deleted 文件日志（仅 slog，不写回已清理的事件表，§7.5）。
// 文件/清理失败返回 *errorcode.AppError{CodeRecordingDeletePending}（503/20006），
// deleting_at 保留供续做。
func (s *DeleteService) finalize(ctx context.Context, row ports.DeletingCleanup) error {
	// 取消必须在 MarkDeleting 提交后、锁外调用（§3.4）；取消未及时生效时，
	// 数据库锁与条件更新兜底拒绝迟到写入。
	if row.TaskID != "" {
		s.canceller.CancelTask(row.TaskID)
	}

	// 步骤 3：删本地文件；不存在视为成功（详设 §5.3）。
	if err := s.store.Delete(ctx, row.StoragePath); err != nil {
		s.logger.Warn("删除文件失败，保留标记待清理续做",
			slog.String("recording_id", row.RecordingID),
			slog.String("storage_path", row.StoragePath), slog.Any("err", err))
		return errorcode.New(errorcode.CodeRecordingDeletePending, err)
	}

	// 步骤 4：三表显式删除（任一步失败整体回滚）；失败/结果未知均 503/20006 保留标记。
	if err := s.tx.PurgeRecording(ctx, row.RecordingID); err != nil {
		s.logger.Warn("三表清理失败，保留标记待续做",
			slog.String("recording_id", row.RecordingID), slog.Any("err", err))
		return errorcode.New(errorcode.CodeRecordingDeletePending, err)
	}

	// task_deleted 仅写文件日志：独立 event_id、最后 event_seq+1，不写回已清理的
	// 事件表（详设 §7.5）；文件写失败仅报警，资源删除已提交仍返回成功。
	if row.TaskID != "" {
		from, to := domain.TaskStatus(row.Status), domain.TaskStatus(row.Status)
		logging.MirrorEvent(s.logger, domain.TaskEvent{
			EventID:          uuid.New(),
			TaskID:           row.TaskID,
			RecordingID:      row.RecordingID,
			EventSeq:         row.EventSeq + 1,
			Attempt:          row.Attempt,
			Kind:             domain.EventTaskDeleted,
			OccurredAt:       time.Now().UTC(),
			Level:            "INFO",
			FromStatus:       &from,
			ToStatus:         &to,
			CreatedRequestID: "",
			InstanceID:       s.instanceID,
		})
	}
	s.logger.Info("录音删除完成",
		slog.String("recording_id", row.RecordingID), slog.String("task_id", row.TaskID))
	return nil
}

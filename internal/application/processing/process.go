// Package processing 异步流水线用例（详设 §3.2 worker 控制流，本任务实现到 SaveT
// 分支：认领 → 取消表登记 → 复查 → Mock 转写 → 事务④；摘要与失败事务在 T07）。
// 所有出口（成功 / stale 丢弃 / 外呼失败 / 认领失败）都清理取消表登记后返回。
package processing

import (
	"context"
	"errors"
	"log/slog"

	"recording-transcription/internal/application/ports"
	domain "recording-transcription/internal/domain/recording"
)

// CancelRegistry 内存取消表的最小接口（详设 §3.4）：由 infrastructure/worker 实现，
// 应用层只依赖该接口，避免依赖具体设施。
type CancelRegistry interface {
	Register(key domain.ExecutionKey, cancel context.CancelFunc)
	Unregister(key domain.ExecutionKey)
}

// ProcessService 单轮「认领→转写→保存」用例，由 worker 池每轮调用。
type ProcessService struct {
	tx          ports.ProcessingTx
	query       ports.RecordingQuery
	transcriber ports.Transcriber
	table       CancelRegistry
	logger      *slog.Logger
}

// NewProcessService 构造流水线用例。
func NewProcessService(tx ports.ProcessingTx, query ports.RecordingQuery, transcriber ports.Transcriber, table CancelRegistry, logger *slog.Logger) *ProcessService {
	return &ProcessService{tx: tx, query: query, transcriber: transcriber, table: table, logger: logger}
}

// Process 执行一轮：无可认领立即返回（worker 回 select 等待唤醒/轮询）。
// 认领事务在 ProcessingTx 内闭合，绝不跨外部转写调用持有（详设 §4.2）。
func (s *ProcessService) Process(ctx context.Context) {
	claimed, ok, err := s.tx.ClaimNext(ctx)
	if err != nil {
		// 孤儿任务（90004）等：记录日志返回；认领阻断与就绪门控在 T11（详设 §4.3）。
		s.logger.Error("认领失败", slog.Any("err", err))
		return
	}
	if !ok {
		return
	}

	key := claimed.ExecutionKey
	taskCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	s.table.Register(key, cancel)
	defer s.table.Unregister(key)

	// §3.4：登记后、外呼前复查轮次与 deleting_at，覆盖「删除时尚未登记」窗口；
	// 删除中任务经 GetTask 表现为不可见（ErrTaskNotFound）。
	view, err := s.query.GetTask(ctx, key.TaskID)
	if err != nil || view.Attempt != key.Attempt || view.Status != domain.StatusTranscribing {
		s.logger.Warn("复查未通过，丢弃本次执行",
			slog.String("task_id", key.TaskID), slog.Int("attempt", key.Attempt), slog.Any("err", err))
		return
	}

	transcript, err := s.transcriber.Transcribe(taskCtx, key.TaskID)
	if err != nil {
		// 事务③（failed + 40001 + task_failed）在 T07 接入；本任务记录后返回，
		// 任务停留 transcribing，由启动恢复（T11）处理。取消（删除/停机）不落伪 failed。
		s.logger.Warn("转写失败",
			slog.String("task_id", key.TaskID), slog.Int("attempt", key.Attempt), slog.Any("err", err))
		return
	}

	if err := s.tx.SaveTranscription(ctx, key, transcript); err != nil {
		if errors.Is(err, ports.ErrStaleExecution) {
			// §4.4：stale 静默丢弃，不复活任务、不报错 worker。
			s.logger.Info("stale 执行，转写结果丢弃",
				slog.String("task_id", key.TaskID), slog.Int("attempt", key.Attempt))
			return
		}
		s.logger.Error("保存转写失败",
			slog.String("task_id", key.TaskID), slog.Int("attempt", key.Attempt), slog.Any("err", err))
	}
}

// Package processing 异步流水线用例（详设 §3.2 worker 控制流）：
// 分支：认领 → 取消表登记 → 复查 → Mock 转写 → 事务④ → LLM 摘要 → 事务⑤（done）；
// 任何阶段失败（含转写失败 40001）经事务③落 failed。所有出口（成功 / stale 丢弃 /
// 外呼失败 / 认领失败 / 停止认领 / panic 恢复）都清理取消表登记后返回，worker 不退出。
package processing

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync/atomic"
	"time"

	"recording-transcription/internal/application/errorcode"
	"recording-transcription/internal/application/ports"
	domain "recording-transcription/internal/domain/recording"
)

// CancelRegistry 内存取消表的最小接口（详设 §3.4）：由 infrastructure/worker 实现，
// 应用层只依赖该接口，避免依赖具体设施。
type CancelRegistry interface {
	Register(key domain.ExecutionKey, cancel context.CancelFunc)
	Unregister(key domain.ExecutionKey)
}

// §5.5 落库失败重试参数：保留内存产物，间隔 200ms/500ms 再试 ≤2 次，只重试数据库
// 写入、不重调外部服务；persistTimeout 为每次写入的短期限（§3.4：从 runCtx 派生的
// 新短期限 context，绝不用已超时的 llmCtx）。
const (
	persistTimeout = 5 * time.Second
)

var persistRetryDelays = []time.Duration{200 * time.Millisecond, 500 * time.Millisecond}

// ProcessService 单轮「认领→转写→摘要→完成/失败」用例，由 worker 池每轮调用。
type ProcessService struct {
	tx          ports.ProcessingTx
	query       ports.RecordingQuery
	transcriber ports.Transcriber
	summarizer  ports.Summarizer
	table       CancelRegistry
	logger      *slog.Logger
	halted      atomic.Bool // §5.5：落库持续失败后置位，停止认领并受控退出

	// OnHalt §5.5 受控退出回调：halted 置位时同步触发一次（bootstrap 接 Lifecycle
	// 两阶段停机；测试注入观察器）。在 worker 协程内调用，不得阻塞。须在 worker 池
	// Start 前设置（避免数据竞争）。
	OnHalt func()
}

// NewProcessService 构造流水线用例。
func NewProcessService(tx ports.ProcessingTx, query ports.RecordingQuery, transcriber ports.Transcriber,
	summarizer ports.Summarizer, table CancelRegistry, logger *slog.Logger) *ProcessService {
	return &ProcessService{
		tx: tx, query: query, transcriber: transcriber, summarizer: summarizer,
		table: table, logger: logger,
	}
}

// Halted 报告服务是否因落库持续失败停止认领（§5.5；测试断言用）。
func (s *ProcessService) Halted() bool { return s.halted.Load() }

// Process 执行一轮：无可认领立即返回（worker 回 select 等待唤醒/轮询）。
// 认领用 claimCtx（drain 第一步取消即停止认领，§3.5 第 1 条），执行链（taskCtx 与
// 落库）用 runCtx——在途任务在 drain 期间继续推进，仅强停取消中断（§3.4 context 树）。
// 认领事务在 ProcessingTx 内闭合，绝不跨外部转写/摘要调用持有（详设 §4.2）。
func (s *ProcessService) Process(claimCtx, runCtx context.Context) {
	var key domain.ExecutionKey
	var claimed bool
	// §3.2 每轮执行包裹 recover：阶段代码 panic 不击穿 worker。defer 注册在最前、
	// 最后执行——取消表登记与 taskCtx 清理先完成，再做兜底写入。
	defer func() {
		if r := recover(); r != nil {
			s.recoverRound(runCtx, claimed, key, r)
		}
	}()
	if s.halted.Load() {
		// §5.5：落库持续失败后停止认领，保留产物待恢复（已触发受控退出）。
		return
	}
	claimedExec, ok, err := s.tx.ClaimNext(claimCtx)
	if err != nil {
		if claimCtx.Err() != nil {
			// drain 取消认领：非故障，静默返回（§3.5 第 1 条）。
			return
		}
		// 孤儿任务（90004）等：记录日志返回；认领阻断与就绪门控在 T11（详设 §4.3）。
		s.logger.Error("认领失败", slog.Any("err", err))
		return
	}
	if !ok {
		return
	}
	claimed = true
	key = claimedExec.ExecutionKey

	taskCtx, cancel := context.WithCancel(runCtx)
	defer cancel()
	s.table.Register(key, cancel)
	defer s.table.Unregister(key)

	// §3.4：登记后、外呼前复查轮次与 deleting_at，覆盖「删除时尚未登记」窗口；
	// 删除中任务经 GetTask 表现为不可见（ErrTaskNotFound）。
	view, err := s.query.GetTask(runCtx, key.TaskID)
	if err != nil || view.Attempt != key.Attempt || view.Status != domain.StatusTranscribing {
		s.logger.Warn("复查未通过，丢弃本次执行",
			slog.String("task_id", key.TaskID), slog.Int("attempt", key.Attempt), slog.Any("err", err))
		return
	}

	transcript, err := s.transcriber.Transcribe(taskCtx, key.TaskID)
	if err != nil {
		// 取消（删除/停机）不落伪 failed：丢弃结果，任务保持在途供恢复/清理（§3.4）。
		if errors.Is(err, context.Canceled) {
			s.logger.Info("任务已取消，转写中止",
				slog.String("task_id", key.TaskID), slog.Int("attempt", key.Attempt))
			return
		}
		// 事务③：转写失败 40001（§4.1 transcribing→failed），与 LLM 失败共用 FailTask。
		s.failTask(runCtx, key, errorcode.CodeASRFailed, err)
		return
	}

	// 事务④：外部成功但落库失败按 §5.5 有界重试；仍失败则停止推进（不能带着
	// 未落库的 transcript 进入摘要——CompleteTask 的条件更新会因状态不匹配被拒）。
	if err := s.persist(runCtx, key, "保存转写", func(c context.Context) error {
		return s.tx.SaveTranscription(c, key, transcript)
	}); err != nil {
		return
	}

	summary, err := s.summarizer.Summarize(taskCtx, transcript)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			// 删除/停机取消：丢弃摘要结果，不落伪 failed（详设 §3.4）；真实适配器
			// 的超时表现为 ErrLLMTimeout，不会误入此分支。
			s.logger.Info("任务已取消，摘要中止",
				slog.String("task_id", key.TaskID), slog.Int("attempt", key.Attempt))
			return
		}
		// 事务③：LLM 失败按哨兵分类 50001/50002/50003（详设 §9）。
		s.failTask(runCtx, key, llmErrorCode(err), err)
		return
	}

	// 事务⑤：summary_json + done + task_completed 原子提交（§4.4）。
	_ = s.persist(runCtx, key, "完成任务", func(c context.Context) error {
		return s.tx.CompleteTask(c, key, summary)
	})
}

// recoverRound §3.2 panic 防护出口：记录堆栈与任务关联字段 → 条件更新尽力写
// failed/90001（任务已删除或轮次变化被拒即丢弃，不复活）；runCtx 已取消（强停中）
// 不写——停机取消不伪造业务 failed（§3.5），任务保持在途待重启恢复。
func (s *ProcessService) recoverRound(runCtx context.Context, claimed bool, key domain.ExecutionKey, r any) {
	s.logger.Error("单轮执行 panic 已恢复（详设 §3.2）",
		slog.Bool("claimed", claimed),
		slog.String("task_id", key.TaskID), slog.Int("attempt", key.Attempt),
		slog.Any("panic", r), slog.String("stack", string(debug.Stack())))
	if !claimed || runCtx.Err() != nil {
		return
	}
	ctx, cancel := context.WithTimeout(runCtx, persistTimeout)
	defer cancel()
	msg := truncateMessage(fmt.Sprintf("panic: %v", r))
	if err := s.tx.FailTask(ctx, key, errorcode.CodeInternalError, msg); err != nil {
		s.logger.Warn("panic 后尽力写 failed/90001 未成功，丢弃",
			slog.String("task_id", key.TaskID), slog.Int("attempt", key.Attempt), slog.Any("err", err))
	}
}

// failTask 事务③落库：消息限长（task_events.error_message VARCHAR(512)），且不携带
// Key 或堆栈（§9——适配器错误只含状态码与解析原因，此处再截断兜底）。
func (s *ProcessService) failTask(ctx context.Context, key domain.ExecutionKey, code errorcode.ErrorCode, cause error) {
	msg := truncateMessage(cause.Error())
	if err := s.persist(ctx, key, "记录失败", func(c context.Context) error {
		return s.tx.FailTask(c, key, code, msg)
	}); err == nil {
		s.logger.Warn("任务失败已落库",
			slog.String("task_id", key.TaskID), slog.Int("attempt", key.Attempt),
			slog.Int("code", int(code)), slog.String("msg", msg))
	}
}

// persist 执行一次阶段落库并按 §5.5 处理失败：stale 静默丢弃（§4.4）；其他错误保留
// 内存产物，间隔 200ms/500ms 重试 ≤2 次（只重试写入，不重调外部服务）；仍失败记
// ERROR、置 halted 停止认领并触发 OnHalt 受控退出（§5.5）。每次写入使用从 runCtx
// 派生的新短期限 context（§3.4），不用已超时的 llmCtx。
func (s *ProcessService) persist(ctx context.Context, key domain.ExecutionKey, stage string, op func(context.Context) error) error {
	var last error
	for attempt := 0; ; attempt++ {
		opCtx, cancel := context.WithTimeout(ctx, persistTimeout)
		err := op(opCtx)
		cancel()
		if err == nil {
			return nil
		}
		if errors.Is(err, ports.ErrStaleExecution) {
			// §4.4：stale 静默丢弃，不复活任务、不报错 worker、不重试。
			s.logger.Info("stale 执行，结果丢弃",
				slog.String("task_id", key.TaskID), slog.Int("attempt", key.Attempt), slog.String("stage", stage))
			return err
		}
		last = err
		if attempt >= len(persistRetryDelays) {
			break
		}
		s.logger.Warn("落库失败，稍后重试",
			slog.String("task_id", key.TaskID), slog.String("stage", stage),
			slog.Int("attempt_no", attempt+1), slog.Any("err", err))
		// 重试间隔受 ctx 控制（§5.5）；等待被取消则立即停止重试。
		timer := time.NewTimer(persistRetryDelays[attempt])
		select {
		case <-ctx.Done():
			timer.Stop()
			last = ctx.Err()
		case <-timer.C:
		}
		if ctx.Err() != nil {
			break
		}
	}
	if ctx.Err() != nil && errors.Is(last, context.Canceled) {
		// 退出/取消导致落库中断：不是数据库故障，不停止认领（保留产物待恢复）。
		s.logger.Error("落库因退出中断",
			slog.String("task_id", key.TaskID), slog.String("stage", stage), slog.Any("err", last))
		return last
	}
	s.halted.Store(true)
	s.logger.Error("落库持续失败，停止认领并受控退出（保留产物，待恢复）",
		slog.String("task_id", key.TaskID), slog.String("stage", stage), slog.Any("err", last))
	if s.OnHalt != nil {
		s.OnHalt() // §5.5 受控退出：bootstrap 接 Lifecycle 两阶段停机
	}
	return last
}

// llmErrorCode 将 Summarizer 错误映射为异步错误码（详设 §9）：哨兵精确映射，
// 未知错误归 50002（网络/上游类兜底）。
func llmErrorCode(err error) errorcode.ErrorCode {
	switch {
	case errors.Is(err, ports.ErrLLMTimeout):
		return errorcode.CodeLLMTimeout
	case errors.Is(err, ports.ErrLLMInvalidOutput):
		return errorcode.CodeLLMInvalidOutput
	default:
		return errorcode.CodeLLMUpstreamError
	}
}

// truncateMessage 按 UTF-8 安全截断到 ≤500 字节，防溢出 task_events.error_message
// （VARCHAR(512)，超长插入失败会拖垮整个事务③）。
func truncateMessage(msg string) string {
	const limit = 500
	if len(msg) <= limit {
		return msg
	}
	cut := limit
	for cut > 0 && !isUTF8Start(msg[cut]) {
		cut--
	}
	return msg[:cut] + "…"
}

func isUTF8Start(b byte) bool { return b&0xC0 != 0x80 }

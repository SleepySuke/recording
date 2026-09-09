// Package recording 「录音处理」限界上下文的领域层（详设 §2.1）：聚合根 Recording、
// 聚合内实体 ProcessingTask、值对象 Summary 与生命周期事件 TaskEvent。纯领域规则，
// 不依赖 gin/gorm/slog 等外部设施；领域错误用具名 error 表达，由应用层映射数字业务码。
package recording

import (
	"fmt"
	"time"
)

// TaskStatus 任务状态（详设 §4.1）。
type TaskStatus string

const (
	StatusPending      TaskStatus = "pending"
	StatusTranscribing TaskStatus = "transcribing"
	StatusSummarizing  TaskStatus = "summarizing"
	StatusDone         TaskStatus = "done"
	StatusFailed       TaskStatus = "failed"
)

// transitions 是 CanTransition 的唯一依据（详设 §4.1 矩阵）：9 个 (from,to) 组合，
// 其中 transcribing/summarizing→pending 仅用于启动恢复基线，failed→pending 为手动重试；
// 降级恢复的 transcribing/summarizing→failed 由 TransitionForRecovery 单独表达。
var transitions = map[TaskStatus]map[TaskStatus]bool{
	StatusPending:      {StatusTranscribing: true, StatusFailed: true},
	StatusTranscribing: {StatusSummarizing: true, StatusFailed: true, StatusPending: true},
	StatusSummarizing:  {StatusDone: true, StatusFailed: true, StatusPending: true},
	StatusFailed:       {StatusPending: true},
}

// ProcessingTask 聚合内实体（详设 §2.1）：负责转写、摘要状态与执行轮次；重试复用任务 ID、递增 attempt。
type ProcessingTask struct {
	ID               string
	RecordingID      string
	Status           TaskStatus
	Attempt          int
	EventSeq         int64
	Transcript       string
	Summary          *Summary
	ErrorCode        *int
	ErrorMessage     string
	CreatedRequestID string // 最初上传请求关联（详设 §7.1，tasks.created_request_id）
	CreatedAt        time.Time
	UpdatedAt        time.Time
	StartedAt        *time.Time
	FinishedAt       *time.Time
}

// CanTransition 报告当前状态到 to 是否为 §4.1 矩阵中的合法出边（done 无出边，矩阵外全部拒绝）。
func (t *ProcessingTask) CanTransition(to TaskStatus) bool {
	return transitions[t.Status][to]
}

// Transition 校验 attempt 匹配（§4.4 条件更新语义：旧轮次写入被拒）且出边合法后推进状态；
// 任一失败返回领域错误且任务保持原状。
func (t *ProcessingTask) Transition(attempt int, to TaskStatus) error {
	if attempt != t.Attempt {
		return fmt.Errorf("%w: 当前轮次 %d, 请求轮次 %d", ErrAttemptMismatch, t.Attempt, attempt)
	}
	if !t.CanTransition(to) {
		return fmt.Errorf("%w: %s → %s", ErrInvalidTransition, t.Status, to)
	}
	t.Status = to
	return nil
}

// TransitionForRecovery 启动恢复专用边（详设 §4.1 恢复两行、§6.2/§6.3）：仅接受
// transcribing/summarizing → pending（基线恢复）或 → failed（12h 降级恢复）。
// task_recovered 与 task_interrupted 的事件区分由调用方完成，状态语义在此相同。
func (t *ProcessingTask) TransitionForRecovery(to TaskStatus) error {
	if t.Status != StatusTranscribing && t.Status != StatusSummarizing {
		return fmt.Errorf("%w: %s 不是可恢复的在途状态", ErrInvalidTransition, t.Status)
	}
	if to != StatusPending && to != StatusFailed {
		return fmt.Errorf("%w: 恢复目标 %s 不合法", ErrInvalidTransition, to)
	}
	t.Status = to
	return nil
}

// AllocateEventSeq 在任务行锁内调用（详设 §7.2）：自增并返回本次事件的序号，首个事件得到 1。
func (t *ProcessingTask) AllocateEventSeq() int64 {
	t.EventSeq++
	return t.EventSeq
}

// ExecutionKey 条件更新的匹配键（task_id + attempt，详设 §4.4）。
type ExecutionKey struct {
	TaskID  string
	Attempt int
}

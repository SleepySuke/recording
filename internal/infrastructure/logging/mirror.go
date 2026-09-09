// mirror.go 任务生命周期事件的文件日志镜像（详设 §7.4）：与状态同事务提交后，
// 由应用层尽力而为地镜像到 logs/app.jsonl；写文件失败不影响已提交状态。
package logging

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	domain "recording-transcription/internal/domain/recording"
)

// MirrorEvent 将已提交的 TaskEvent 以其 occurred_at 作为 time 字段镜像到文件日志
// （详设 §7.4：time 来自业务发生时刻，不能用输出时刻替代）。直接调用 Handler.Handle
// 绕过 LOG_LEVEL 过滤——任务事件不因调高级别被丢弃；写失败向 stderr 报告并继续。
func MirrorEvent(logger *slog.Logger, evt domain.TaskEvent) {
	if logger == nil {
		return
	}
	var level slog.Level
	_ = level.UnmarshalText([]byte(evt.Level))

	record := slog.NewRecord(evt.OccurredAt, level, string(evt.Kind), 0)
	record.Add(
		"event_id", evt.EventID,
		"event_seq", evt.EventSeq,
		"event", string(evt.Kind),
		"task_id", evt.TaskID,
		"recording_id", evt.RecordingID,
		"attempt", evt.Attempt,
		"instance_id", evt.InstanceID,
		"request_id", evt.RequestID,
		"created_request_id", evt.CreatedRequestID,
		"from_status", evt.FromStatus,
		"to_status", evt.ToStatus,
		"stage", evt.Stage,
		"error_code", evt.ErrorCode,
		"error_message", evt.ErrorMessage,
		"stage_elapsed_ms", evt.StageElapsedMs,
		"attempt_elapsed_ms", evt.AttemptElapsedMs,
		"task_elapsed_ms", evt.TaskElapsedMs,
	)
	if len(evt.Details) > 0 {
		record.Add("details", evt.Details)
	}
	if err := logger.Handler().Handle(context.Background(), record); err != nil {
		fmt.Fprintf(os.Stderr, "事件镜像写文件日志失败（事件已提交，不影响状态）: task_id=%s event=%s err=%v\n",
			evt.TaskID, evt.Kind, err)
	}
}

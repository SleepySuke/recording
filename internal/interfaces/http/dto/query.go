// 查询接口响应体（详设 §8.1）：时间字段由 encoding/json 输出 RFC3339。
package dto

import (
	"time"

	"recording-transcription/internal/application/ports"
)

// TaskErrorBody 任务异步执行错误（详设 §8.4：成功/处理中为 null）。
type TaskErrorBody struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// TaskBody GET /v1/tasks/{task_id} 200 响应体。
type TaskBody struct {
	ID          string         `json:"id"`
	RecordingID string         `json:"recording_id"`
	Status      string         `json:"status"`
	Attempt     int            `json:"attempt"`
	Error       *TaskErrorBody `json:"error"`
	CreatedAt   time.Time      `json:"created_at"`
	StartedAt   *time.Time     `json:"started_at"`
	FinishedAt  *time.Time     `json:"finished_at"`
	NextRetryAt *time.Time     `json:"next_retry_at"`
}

// NewTaskBody 任务视图 → 响应体。
func NewTaskBody(v ports.TaskView) TaskBody {
	var e *TaskErrorBody
	if v.Error != nil {
		e = &TaskErrorBody{Code: v.Error.Code, Message: v.Error.Message}
	}
	return TaskBody{
		ID:          v.ID,
		RecordingID: v.RecordingID,
		Status:      string(v.Status),
		Attempt:     v.Attempt,
		Error:       e,
		CreatedAt:   v.CreatedAt,
		StartedAt:   v.StartedAt,
		FinishedAt:  v.FinishedAt,
		NextRetryAt: v.NextRetryAt,
	}
}

// ResultBody 摘要结果（result 字段，仅 done 非 null）。
type ResultBody struct {
	Summary   string   `json:"summary"`
	KeyPoints []string `json:"key_points"`
	Todos     []string `json:"todos"`
}

// RecordingDetailBody GET /v1/recordings/{id} 200 响应体：录音元数据 + 嵌套任务视图 + 产物。
type RecordingDetailBody struct {
	ID               string      `json:"id"`
	OriginalFilename string      `json:"original_filename"`
	Extension        string      `json:"extension"`
	SizeBytes        int64       `json:"size_bytes"`
	CreatedAt        time.Time   `json:"created_at"`
	Task             TaskBody    `json:"task"`
	Transcript       *string     `json:"transcript"`
	Result           *ResultBody `json:"result"`
}

// NewRecordingDetailBody 详情视图 → 响应体。
func NewRecordingDetailBody(d ports.RecordingDetail) RecordingDetailBody {
	body := RecordingDetailBody{
		ID:               d.Task.RecordingID,
		OriginalFilename: d.OriginalFilename,
		Extension:        d.Extension,
		SizeBytes:        d.SizeBytes,
		CreatedAt:        d.CreatedAt,
		Task:             NewTaskBody(d.Task),
		Transcript:       d.Transcript,
	}
	if d.Result != nil {
		body.Result = &ResultBody{
			Summary:   d.Result.Summary,
			KeyPoints: d.Result.KeyPoints,
			Todos:     d.Result.Todos,
		}
	}
	return body
}

// RecordingListItemBody 列表项：录音概要 + task_id 与最新 status（详设 §8.1）。
type RecordingListItemBody struct {
	ID               string    `json:"id"`
	OriginalFilename string    `json:"original_filename"`
	SizeBytes        int64     `json:"size_bytes"`
	CreatedAt        time.Time `json:"created_at"`
	TaskID           string    `json:"task_id"`
	Status           string    `json:"status"`
}

// RecordingListBody GET /v1/recordings 200 响应体；空页 items 为 [] 非 null。
type RecordingListBody struct {
	Items    []RecordingListItemBody `json:"items"`
	Page     int                     `json:"page"`
	PageSize int                     `json:"page_size"`
	Total    int                     `json:"total"`
}

// NewRecordingListBody 列表视图 → 响应体。
func NewRecordingListBody(l ports.RecordingList) RecordingListBody {
	items := make([]RecordingListItemBody, 0, len(l.Items))
	for _, it := range l.Items {
		items = append(items, RecordingListItemBody{
			ID:               it.RecordingID,
			OriginalFilename: it.OriginalFilename,
			SizeBytes:        it.SizeBytes,
			CreatedAt:        it.CreatedAt,
			TaskID:           it.TaskID,
			Status:           string(it.Status),
		})
	}
	return RecordingListBody{Items: items, Page: l.Page, PageSize: l.PageSize, Total: l.Total}
}

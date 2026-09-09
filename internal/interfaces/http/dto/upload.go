// Package dto 定义接口层响应数据结构（需求 API 契约：202 返回 recording_id/task_id/status）。
package dto

// UploadResponse POST /v1/recordings 202 响应体。
type UploadResponse struct {
	RecordingID string `json:"recording_id"`
	TaskID      string `json:"task_id"`
	Status      string `json:"status"`
}

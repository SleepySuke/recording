package dto

// RetryResponse POST /v1/tasks/{task_id}/retry 202 响应体（详设 §8.1：
// task_id、status=pending、attempt=新轮次号）。
type RetryResponse struct {
	TaskID  string `json:"task_id"`
	Status  string `json:"status"`
	Attempt int    `json:"attempt"`
}

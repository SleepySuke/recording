// SSE 摘要进度流：只读轮询数据库，不伪造 LLM token 流；详见 retry-and-summary-stream.md。
package handler

import (
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"recording-transcription/internal/interfaces/http/dto"
	"recording-transcription/internal/pkg/uuid"
)

// HandleSummaryStream GET /v1/recordings/:id/summary/stream。首次和任务变化均发送 status；
// done/failed/deleted 是终端事件。连接由 Request.Context 取消，避免泄漏 goroutine。
func (h *QueryHandler) HandleSummaryStream(c *gin.Context) {
	id := c.Param("id")
	if !uuid.IsValid(id) {
		h.badParam(c, "recording_id", id)
		return
	}
	if _, err := h.svc.GetRecording(c.Request.Context(), id); err != nil {
		h.fail(c, err)
		return
	}
	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Header("X-Accel-Buffering", "no")
	c.Status(http.StatusOK)
	last, lastPing := "", time.Now()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		detail, err := h.svc.GetRecording(c.Request.Context(), id)
		if err != nil {
			c.SSEvent("deleted", gin.H{"recording_id": id})
			return
		}
		body := dto.NewTaskBody(detail.Task)
		errorKey := ""
		if body.Error != nil {
			errorKey = fmt.Sprintf("%d:%s", body.Error.Code, body.Error.Message)
		}
		key := fmt.Sprintf("%s:%d:%v:%s", body.Status, body.Attempt, body.NextRetryAt, errorKey)
		if key != last {
			c.SSEvent("status", body)
			last = key
		}
		if string(detail.Task.Status) == "done" {
			c.SSEvent("summary", dto.NewRecordingDetailBody(detail).Result)
			return
		}
		if string(detail.Task.Status) == "failed" {
			c.SSEvent("failed", body.Error)
			return
		}
		if time.Since(lastPing) >= 15*time.Second {
			c.SSEvent("ping", gin.H{})
			lastPing = time.Now()
		}
		select {
		case <-c.Request.Context().Done():
			return
		case <-ticker.C:
		}
	}
}

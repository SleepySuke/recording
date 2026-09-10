// 重试接口的 HTTP 适配（详设 §8.1/§4.5）：POST /v1/tasks/{task_id}/retry，
// 仅 failed 任务受理；路径 UUID 非法 → 400/10001（详设 §8.3）。
package handler

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"

	appcode "recording-transcription/internal/application/errorcode"
	apprec "recording-transcription/internal/application/recording"
	"recording-transcription/internal/interfaces/http/dto"
	"recording-transcription/internal/pkg/uuid"
)

// RetryHandler POST /v1/tasks/:id/retry。
type RetryHandler struct {
	svc    *apprec.RetryService
	logger *slog.Logger
}

// NewRetryHandler 构造重试处理器。
func NewRetryHandler(svc *apprec.RetryService, logger *slog.Logger) *RetryHandler {
	return &RetryHandler{svc: svc, logger: logger}
}

// Handle 受理手动重试：202 返回受理结果；非 failed → 409/30002，
// 不存在/删除中 → 404/30001（详设 §8.1/§8.3）。
func (h *RetryHandler) Handle(c *gin.Context) {
	id := c.Param("id")
	if !uuid.IsValid(id) {
		c.Error(appcode.New(appcode.CodeInvalidArgument, fmt.Errorf("task_id = %q 非法", id)))
		return
	}
	res, err := h.svc.Retry(c.Request.Context(), id)
	if err != nil {
		var appErr *appcode.AppError
		if !errors.As(err, &appErr) {
			appErr = appcode.New(appcode.CodeInternalError, err)
		}
		c.Error(appErr)
		return
	}
	c.JSON(http.StatusAccepted, dto.RetryResponse{
		TaskID:  res.TaskID,
		Status:  string(res.Status),
		Attempt: res.Attempt,
	})
}

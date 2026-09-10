// 删除接口的 HTTP 适配（详设 §8.1/§5.3）：DELETE /v1/recordings/{id}，任意状态可删；
// 路径 UUID 非法 → 400/10001；不存在/已完全删除 → 404/20005；清理未完成 → 503/20006；
// 成功 204 无响应体。
package handler

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"

	appcode "recording-transcription/internal/application/errorcode"
	apprec "recording-transcription/internal/application/recording"
	"recording-transcription/internal/pkg/uuid"
)

// DeleteHandler DELETE /v1/recordings/:id。
type DeleteHandler struct {
	svc    *apprec.DeleteService
	logger *slog.Logger
}

// NewDeleteHandler 构造删除处理器。
func NewDeleteHandler(svc *apprec.DeleteService, logger *slog.Logger) *DeleteHandler {
	return &DeleteHandler{svc: svc, logger: logger}
}

// Handle 受理删除：成功 204（无响应体，详设 §8.2）；用例错误按 AppError 映射
// （404/20005、503/20006，详设 §8.3）。
func (h *DeleteHandler) Handle(c *gin.Context) {
	id := c.Param("id")
	if !uuid.IsValid(id) {
		c.Error(appcode.New(appcode.CodeInvalidArgument, fmt.Errorf("recording_id = %q 非法", id)))
		return
	}
	if err := h.svc.Delete(c.Request.Context(), id); err != nil {
		var appErr *appcode.AppError
		if !errors.As(err, &appErr) {
			appErr = appcode.New(appcode.CodeInternalError, err)
		}
		c.Error(appErr)
		return
	}
	c.Status(http.StatusNoContent)
}

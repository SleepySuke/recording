// Package errorcode（接口层）将应用层业务码映射为统一 JSON 错误响应（详设 §8.2/§8.6）。
// 仅补 HTTP 状态映射与渲染；编号与公开消息的唯一权威在 application/errorcode。
package errorcode

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	appcode "recording-transcription/internal/application/errorcode"
)

// Render 写出统一错误结构：{"error":{"code":…,"message":…,"request_id":"…"}}。
// 已写出响应头时不再覆盖，仅记录（详设 §8.6）。
func Render(c *gin.Context, err error) {
	if c.Writer.Written() {
		return
	}
	var appErr *appcode.AppError
	if !errors.As(err, &appErr) {
		appErr = appcode.New(appcode.CodeInternalError, err)
	}
	status := appErr.Code.HTTPStatus()
	if status == 0 {
		status = http.StatusInternalServerError
	}
	c.JSON(status, gin.H{
		"error": gin.H{
			"code":      int(appErr.Code),
			"message":  appErr.Code.Message(),
			"request_id": c.MustGet("request_id"),
		},
	})
}

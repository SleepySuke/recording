package middleware

import (
	"log/slog"
	"runtime/debug"

	"github.com/gin-gonic/gin"

	appcode "recording-transcription/internal/application/errorcode"
)

// PanicRecovery 捕获处理器 panic：记录堆栈（带 request_id），登记 90001，
// 不向响应泄漏内部细节（详设 §8.6）。
func PanicRecovery(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if r := recover(); r != nil {
				logger.Error("panic recovered",
					"request_id", c.MustGet("request_id"),
					"panic", r,
					"stack", string(debug.Stack()),
				)
				_ = c.Error(appcode.New(appcode.CodeInternalError, nil))
			}
		}()
		c.Next()
	}
}

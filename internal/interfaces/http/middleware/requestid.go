// Package middleware 提供 Gin 中间件：request_id、访问日志、错误渲染、panic 恢复（详设 §8.6）。
package middleware

import (
	"github.com/gin-gonic/gin"

	"recording-transcription/internal/pkg/uuid"
)

const HeaderRequestID = "X-Request-ID"

// RequestID 为每个请求生成 request_id，写入响应头与 gin Context（详设 §8.2：头与响应体一致）。
func RequestID() gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.GetHeader(HeaderRequestID)
		if id == "" {
			id = uuid.New()
		}
		c.Set("request_id", id)
		c.Header(HeaderRequestID, id)
		c.Next()
	}
}

package middleware

import (
	"github.com/gin-gonic/gin"

	ierrorcode "recording-transcription/internal/interfaces/http/errorcode"
)

// ErrorRenderer 在 c.Next() 返回后读取处理器登记的错误并统一渲染（详设 §8.6）。
// 处理器用 c.Error(...) 登记业务错误，不自行写错误正文；成功响应已写出则不动。
func ErrorRenderer() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()
		if len(c.Errors) == 0 {
			return
		}
		ierrorcode.Render(c, c.Errors.Last().Err)
	}
}

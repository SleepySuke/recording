// Package httpapi 组装路由与中间件（详设 §8.6：外→内 RequestID → AccessLog → ErrorRenderer → PanicRecovery）。
package httpapi

import (
	"log/slog"

	"github.com/gin-gonic/gin"

	appcode "recording-transcription/internal/application/errorcode"
	"recording-transcription/internal/interfaces/http/middleware"
)

// New 构建 Gin 引擎并注册健康接口。业务路由随任务 T04～T09 逐步挂载。
func New(logger *slog.Logger) *gin.Engine {
	r := gin.New()
	r.Use(
		middleware.RequestID(),
		middleware.AccessLog(logger),
		middleware.ErrorRenderer(),
		middleware.PanicRecovery(logger),
	)
	r.HandleMethodNotAllowed = true
	r.NoRoute(func(c *gin.Context) {
		_ = c.Error(appcode.New(appcode.CodeRouteNotFound, nil))
	})
	r.NoMethod(func(c *gin.Context) {
		_ = c.Error(appcode.New(appcode.CodeMethodNotAllowed, nil))
	})

	r.GET("/healthz", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok"})
	})
	r.GET("/readyz", func(c *gin.Context) {
		// T11 接入真实就绪门控（启动恢复完成后才就绪）
		c.JSON(200, gin.H{"status": "ready"})
	})
	return r
}

// Package httpapi 组装路由与中间件（详设 §8.6：外→内 RequestID → AccessLog → ErrorRenderer → PanicRecovery）。
package httpapi

import (
	"log/slog"

	"github.com/gin-gonic/gin"

	appcode "recording-transcription/internal/application/errorcode"
	"recording-transcription/internal/interfaces/http/handler"
	"recording-transcription/internal/interfaces/http/middleware"
)

// ReadyFunc 就绪门控（T11，详设 §6.2）：返回 true 才置 /readyz 就绪；nil 视为恒就绪
// （单元测试默认形态）。生产装配传入启动恢复完成标志——巡检 90004 或恢复未完时
// /readyz 503，healthz 不受影响（进程活着、数据库异常时可观测）。
type ReadyFunc func() bool

// New 构建 Gin 引擎并注册健康接口。upload 非 nil 时挂载 POST /v1/recordings（T04）；
// query 非 nil 时挂载三个只读查询接口（T05，详设 §8.1）；retry 非 nil 时挂载
// POST /v1/tasks/:id/retry（T08，详设 §8.1）；delete 非 nil 时挂载
// DELETE /v1/recordings/:id（T09，详设 §8.1/§5.3）。
// 另挂 GET /ui：本地联调静态页（T14，需求不考察前端——开发辅助，随镜像交付的
// static/index.html，同源直连避免 CORS；文件缺失时该路径 404，不影响其余路由）。
func New(logger *slog.Logger, upload *handler.UploadHandler, query *handler.QueryHandler, retry *handler.RetryHandler, delete *handler.DeleteHandler, readyz ReadyFunc) *gin.Engine {
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
		// T11 真实就绪门控（详设 §6.2）：启动恢复全部完成才就绪；
		// 巡检 90004 阻止就绪（503），healthz 保持 200 供存活探测。
		if readyz == nil || readyz() {
			c.JSON(200, gin.H{"status": "ready"})
			return
		}
		c.JSON(503, gin.H{"status": "not ready"})
	})
	if upload != nil {
		handle := upload.Handle
		if readyz != nil {
			// §6.2「全部完成前……不接收上传」：未就绪（巡检 90004 阻断或退出 drain
			// 中）POST /v1/recordings → 503/90005（详设 §8.3）。
			handle = func(c *gin.Context) {
				if !readyz() {
					_ = c.Error(appcode.New(appcode.CodeServiceNotReady, nil))
					return
				}
				upload.Handle(c)
			}
		}
		r.POST("/v1/recordings", handle)
	}
	if query != nil {
		r.GET("/v1/tasks/:id", query.HandleTask)
		r.GET("/v1/recordings", query.HandleList)
		r.GET("/v1/recordings/:id", query.HandleRecording)
		r.GET("/v1/recordings/:id/summary/stream", query.HandleSummaryStream)
	}
	// 联调页（T14）：相对路径在本地（仓库根 go run）与容器（WORKDIR /app）下均命中
	// ./static/index.html——Dockerfile 将 static/ 一并 COPY 进镜像。
	r.StaticFile("/ui", "./static/index.html")
	if retry != nil {
		r.POST("/v1/tasks/:id/retry", retry.Handle)
	}
	if delete != nil {
		r.DELETE("/v1/recordings/:id", delete.Handle)
	}
	return r
}

// 录音转写服务入口。启动序列（健康检查→迁移→恢复→就绪）按任务 T02/T11 逐步接入（详设 §6.2）。
package main

import (
	"fmt"
	"os"

	"recording-transcription/bootstrap"
)

func main() {
	cfg, err := bootstrap.LoadConfig()
	if err != nil {
		fmt.Fprintln(os.Stderr, "配置错误:", err)
		os.Exit(1)
	}
	srv, logger, err := bootstrap.NewServer(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "初始化失败:", err)
		os.Exit(1)
	}
	logger.Info("service listening", "addr", cfg.HTTPAddr)
	if err := srv.ListenAndServe(); err != nil {
		logger.Error("server exited", "err", err)
		os.Exit(1)
	}
}

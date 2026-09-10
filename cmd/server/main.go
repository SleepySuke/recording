// 录音转写服务入口。启动序列（健康检查→迁移→恢复→就绪）按任务 T02/T11 逐步接入（详设 §6.2）；
// 信号驱动的两阶段退出在 bootstrap.Run/Lifecycle（T10，详设 §3.5）。
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"recording-transcription/bootstrap"
)

func main() {
	cfg, err := bootstrap.LoadConfig()
	if err != nil {
		fmt.Fprintln(os.Stderr, "配置错误:", err)
		os.Exit(1)
	}
	// sigCtx 只通知「开始退出」，不直接当 runCtx（详设 §3.5）；
	// 两阶段停机（停认领 → drain 20s 预算 → 超预算强停 → 关 DB）在 Lifecycle。
	sigCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := bootstrap.Run(sigCtx, cfg); err != nil {
		// Run 已记日志；非零退出码交由容器重启策略接管（详设 §6.2）。
		os.Exit(1)
	}
}

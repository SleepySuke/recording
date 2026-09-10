// 录音转写服务入口。启动序列（§6.2，T11 全量接入）：健康检查（连接失败以失败退出，
// 交容器重启策略）→ 迁移（失败阻止启动）→ 启动恢复（巡检 90004 阻止 ready、删除
// 恢复、在途重置、孤儿核对）→ 启动 worker 与 HTTP、/readyz 就绪，均在 bootstrap；
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
		// NewApp 在 logger 建立前失败（DB 连接/迁移等）时无日志可写，补 stderr 一行
		// 保容器日志可诊断；非零退出码交由容器重启策略接管（详设 §6.2）。
		fmt.Fprintln(os.Stderr, "启动失败:", err)
		os.Exit(1)
	}
}

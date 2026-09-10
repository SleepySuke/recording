package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"

	"recording-transcription/internal/application/ports"
	"recording-transcription/internal/application/processing"
	apprec "recording-transcription/internal/application/recording"
	"recording-transcription/internal/infrastructure/asr/mock"
	"recording-transcription/internal/infrastructure/filestore/local"
	"recording-transcription/internal/infrastructure/llm"
	"recording-transcription/internal/infrastructure/logging"
	"recording-transcription/internal/infrastructure/persistence/mysql"
	"recording-transcription/internal/infrastructure/worker"
	httpapi "recording-transcription/internal/interfaces/http"
	"recording-transcription/internal/interfaces/http/handler"
	"recording-transcription/internal/pkg/uuid"
)

// App 全装配产物：HTTP Server、logger 与生命周期协调器（§3.5 两阶段退出）。
type App struct {
	Server    *http.Server
	Logger    *slog.Logger
	Lifecycle *Lifecycle

	pool        *worker.Pool
	cancelRun   context.CancelFunc
	waitCleanup func()
	ready       *atomic.Bool // /readyz 就绪门控（§6.2：启动恢复全部完成才置位）
}

// Ready 报告服务是否就绪（T11，详设 §6.2）：启动恢复（巡检 → 删除恢复 → 在途重置
// → 孤儿核对）全部完成且 worker 已启动后为 true；巡检 90004 阻止 ready（HTTP 仍在，
// healthz 200 / readyz 503，等运维介入）。
func (a *App) Ready() bool { return a.ready.Load() }

// NewApp 组装：日志 + DB（连接 + 迁移 = §6.2 步骤 1/2）+ 上传链 + 异步流水线
// （worker 池 + 认领 + Mock 转写 + LLM 摘要 + 完成失败事务）+ 删除清理循环 +
// http.Server + Lifecycle；随后按 §6.2 步骤 3～7 执行启动恢复（巡检 90004 阻止
// ready；删除恢复不阻塞；在途重置失败阻止启动；孤儿核对失败不阻塞）并启动 worker
// 与清理循环、置 /readyz 就绪。巡检失败时返回不就绪的 App（不启 worker、err=nil）；
// 其余恢复失败返回 error（交容器重启）。§5.5 受控退出：落库持续失败置 halted 后经
// OnHalt 触发 Lifecycle 两阶段停机。
func NewApp(cfg *Config) (*App, error) {
	logger, err := logging.New(logging.Options{
		Dir:       cfg.LogDir,
		Level:     cfg.LogLevel,
		MaxSizeMB: cfg.LogMaxSizeMB, MaxBackups: cfg.LogMaxBackups, MaxAgeDays: cfg.LogMaxAgeDays,
	})
	if err != nil {
		return nil, err
	}

	db, err := mysql.Open(cfg.MysqlDSN)
	if err != nil {
		return nil, err
	}
	if err := mysql.Migrate(db, "migrations"); err != nil {
		return nil, fmt.Errorf("执行数据库迁移失败: %w", err)
	}
	logger.Info("db ready", slog.String("dsn_database", dbURL(cfg.MysqlDSN)))

	fileStore, err := local.New(cfg.DataDir, cfg.UploadMaxFileBytes, cfg.UploadMinFreeBytes)
	if err != nil {
		return nil, fmt.Errorf("初始化数据目录失败: %w", err)
	}

	instanceID := uuid.New()
	query := mysql.NewRecordingQuery(db)
	processingTx := mysql.NewProcessingTx(db, instanceID, logger)
	recordingTx := mysql.NewRecordingTx(db, instanceID, logger)
	// MockASRDelay（测试设计 §2）：-1 = 生产 Mock（5～15s 种子延迟 + 种子失败）；
	// ≥0 = 确定性替身（固定延迟），供 E2E/本地演练注入；MOCK_ASR_FAIL_FIRST
	// 叠加「每个 task_id 首次转写失败」（详设 §4.5 重试用例，E2E-02）。
	transcriber := ports.Transcriber(mock.New())
	if cfg.MockASRDelay >= 0 {
		transcriber = &mock.DeterministicTranscriber{Delay: cfg.MockASRDelay, FailFirst: cfg.MockASRFailFirst}
	}
	// 摘要适配器（详设 §9）：OpenAI 兼容渠道（T01 渠道记录：小米 MiMo）；
	// 超时/响应体上限内建于适配器，llmCtx 在其内部自 taskCtx 派生（§3.4）。
	summarizer := llm.New(cfg.LLMBaseURL, cfg.LLMModel, cfg.LLMAPIKey, cfg.LLMTimeout, llm.DefaultMaxResponseBytes)
	// 取消表（详设 §3.4）：worker 登记 / 删除用例按 task 取消，共用同一实例。
	cancelTable := worker.NewCancelTable()
	processSvc := processing.NewProcessService(processingTx, query, transcriber, summarizer, cancelTable, logger)
	var lc *Lifecycle
	// §5.5 受控退出：落库持续失败 → 两阶段停机（异步触发，避免 worker 协程自等死锁）。
	// 须在 pool.Start 前赋值（goroutine 创建边覆盖该写）。
	processSvc.OnHalt = func() { go lc.Shutdown() }
	pool := worker.NewPool(cfg.WorkerConcurrency, cfg.TaskPollInterval, processSvc.Process)

	uploadSvc := apprec.NewUploadService(fileStore, recordingTx, pool, logger, instanceID)
	uploadHandler := handler.NewUploadHandler(uploadSvc, logger, handler.UploadLimits{
		MaxBodyBytes:    cfg.UploadMaxBodyBytes,
		ReadIdleTimeout: cfg.UploadReadTimeout,
		TotalTimeout:    cfg.UploadTotalTimeout,
	})
	queryHandler := handler.NewQueryHandler(apprec.NewQueryService(query, logger), logger)
	// 重试链（详设 §4.5）：RetryService 复用 recordingTx，COMMIT 后经 pool Notify 唤醒。
	retryHandler := handler.NewRetryHandler(apprec.NewRetryService(recordingTx, pool, logger), logger)
	// 删除链（T09，详设 §5.3）：DELETE 用例 + 低频清理循环（§10 CLEANUP_INTERVAL）；
	// 清理循环挂独立 cleanupCtx（runCtx 子级）——Lifecycle 阶段 1 与停认领同时取消（§3.5 第 1 条）。
	deleteSvc := apprec.NewDeleteService(recordingTx, query, fileStore, cancelTable, logger, instanceID)
	deleteHandler := handler.NewDeleteHandler(deleteSvc, logger)

	ready := &atomic.Bool{} // §6.2 就绪门控：恢复完成前 /readyz 503
	srv := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: httpapi.New(logger, uploadHandler, queryHandler, retryHandler, deleteHandler, ready.Load),
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	cleanupCtx, cancelCleanup := context.WithCancel(runCtx)
	cleanupWg := &sync.WaitGroup{}
	closeDB := func() error {
		sqlDB, err := db.DB()
		if err != nil {
			return err
		}
		return sqlDB.Close()
	}
	lc = NewLifecycle(cfg.ShutdownTimeout, logger, srv, pool, cancelRun, cancelCleanup, cleanupWg.Wait, closeDB,
		func() { ready.Store(false) })

	// §6.2 启动恢复序列（步骤 3～6；步骤 1/2 健康检查与迁移已在上方 NewApp 内完成）。
	// 单实例独占窗口：全部完成前不置就绪、不启动 worker、不接收上传（§4.1「恢复 vs 一切」）。
	bootCtx := context.Background() // 恢复为毫秒级批量操作（§6.2），不设外层期限
	recoverer := processing.NewRecoverService(processingTx, deleteSvc, fileStore, logger, cfg.RecoveryMode == "interrupt")
	if err := recoverer.Inspect(bootCtx); err != nil {
		// 巡检 90004：用例 Inspect 内已记带数值码的 ERROR 日志（T06 移交语义），
		// 此处不再重复。阻止 ready、不启动 worker，但保留 HTTP（healthz 200 /
		// readyz 503、上传 503/90005）供观测，等运维修复（§6.2 Block 分支）。
		return &App{
			Server: srv, Logger: logger, Lifecycle: lc,
			pool: pool, cancelRun: cancelRun, waitCleanup: cleanupWg.Wait, ready: ready,
		}, nil
	}
	if err := recoverer.ResumeDeletions(bootCtx); err != nil {
		logger.Warn("恢复未完成删除失败，已记录、不阻塞任务恢复（§6.2 步骤 4）", slog.Any("err", err))
	}
	if _, err := recoverer.ResetInFlight(bootCtx); err != nil {
		// §6.2 步骤 5：更新与事件同一事务，任一失败整体回滚并阻止启动（交容器重启）。
		return nil, fmt.Errorf("重置在途任务失败，阻止启动: %w", err)
	}
	if _, err := recoverer.RemoveOrphanFiles(bootCtx); err != nil {
		// §5.2：查询失败不清理——记日志、不阻塞启动（下次启动再核对）。
		logger.Error("孤儿文件核对未执行", slog.Any("err", err))
	}

	// 步骤 7：启动 worker 池与清理循环，置 /readyz 就绪（恢复出的 pending 与存量
	// pending 一起按 created_at 顺序消费，详设 §6.2）。
	pool.Start(runCtx)
	worker.StartCleanup(cleanupCtx, cleanupWg, cfg.CleanupInterval, deleteSvc.CleanupPending)
	ready.Store(true)
	logger.Info("worker pool started",
		slog.Int("workers", cfg.WorkerConcurrency),
		slog.Duration("poll_interval", cfg.TaskPollInterval))
	logger.Info("cleanup loop started", slog.Duration("interval", cfg.CleanupInterval))
	logger.Info("startup recovery complete, ready", slog.String("mode", cfg.RecoveryMode))

	return &App{
		Server: srv, Logger: logger, Lifecycle: lc,
		pool: pool, cancelRun: cancelRun, waitCleanup: cleanupWg.Wait, ready: ready,
	}, nil
}

// StopWorkers 只停后台（worker 池 + 清理循环），不动 HTTP 与数据库：e2e harness
// 中途停池采集（E2E-08）仍可用；完整两阶段停机走 Lifecycle.Shutdown。
func (a *App) StopWorkers() {
	a.cancelRun()
	a.pool.Stop()
	a.waitCleanup()
}

// NewServer 兼容入口（e2e harness）：装配同 NewApp；返回的 shutdown 为 StopWorkers
// （不 Shutdown HTTP——harness 自管监听）。
func NewServer(cfg *Config) (*http.Server, *slog.Logger, func(), error) {
	app, err := NewApp(cfg)
	if err != nil {
		return nil, nil, nil, err
	}
	return app.Server, app.Logger, app.StopWorkers, nil
}

// Run 生产入口（§3.5）：sigCtx（signal.NotifyContext）只通知开始退出，不直接当
// runCtx；服务直至 sigCtx 取消、HTTP 自身退出或 §5.5 受控退出（OnHalt → 停机 →
// ListenAndServe 返回 ErrServerClosed）后，经 Lifecycle 两阶段停机再返回。
func Run(ctx context.Context, cfg *Config) error {
	app, err := NewApp(cfg)
	if err != nil {
		return err
	}
	defer app.Lifecycle.Shutdown() // 覆盖全部返回路径（幂等）

	app.Logger.Info("service listening", slog.String("addr", cfg.HTTPAddr))
	errCh := make(chan error, 1)
	go func() { errCh <- app.Server.ListenAndServe() }()
	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			app.Logger.Error("server exited", slog.Any("err", err))
			return err
		}
		return nil // 受控退出或外部 Close：停机由 defer 完成
	case <-ctx.Done():
		app.Logger.Info("收到退出信号，开始两阶段停机（详设 §3.5）")
		return nil
	}
}

// dbURL 只保留 DSN 中的库名用于日志，避免泄露口令。
func dbURL(dsn string) string {
	name := dsn[strings.LastIndex(dsn, "/")+1:]
	name, _, _ = strings.Cut(name, "?")
	return name
}

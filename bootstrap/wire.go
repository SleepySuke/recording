package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"

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
}

// NewApp 组装：日志 + DB（连接 + 迁移）+ 上传链 + 异步流水线（worker 池 + 认领 +
// Mock 转写 + LLM 摘要 + 完成失败事务）+ 删除清理循环 + http.Server + Lifecycle。
// 返回前已启动 worker 池与清理循环；信号接收与停机编排见 Run / Lifecycle。
// §5.5 受控退出：落库持续失败置 halted 后经 OnHalt 触发 Lifecycle 两阶段停机。
// 恢复流程与就绪门控在 T11。
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
	// 清理循环挂 claimCtx——drain 第一步随停认领一并退出（§3.5 第 1 条）。
	deleteSvc := apprec.NewDeleteService(recordingTx, query, fileStore, cancelTable, logger, instanceID)
	deleteHandler := handler.NewDeleteHandler(deleteSvc, logger)

	srv := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: httpapi.New(logger, uploadHandler, queryHandler, retryHandler, deleteHandler),
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
	lc = NewLifecycle(cfg.ShutdownTimeout, logger, srv, pool, cancelRun, cancelCleanup, cleanupWg.Wait, closeDB)
	pool.Start(runCtx)
	// 清理循环独立 ctx（runCtx 子级）：Lifecycle 阶段 1 与停认领同时取消（§3.5）。
	worker.StartCleanup(cleanupCtx, cleanupWg, cfg.CleanupInterval, deleteSvc.CleanupPending)
	logger.Info("worker pool started",
		slog.Int("workers", cfg.WorkerConcurrency),
		slog.Duration("poll_interval", cfg.TaskPollInterval))
	logger.Info("cleanup loop started", slog.Duration("interval", cfg.CleanupInterval))

	return &App{
		Server: srv, Logger: logger, Lifecycle: lc,
		pool: pool, cancelRun: cancelRun, waitCleanup: cleanupWg.Wait,
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

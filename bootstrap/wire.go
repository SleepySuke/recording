package bootstrap

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

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

// NewServer 组装：日志 + DB（连接 + 迁移）+ 上传链 + 异步流水线（worker 池 + 认领 +
// Mock 转写 + LLM 摘要 + 完成失败事务）+ http.Server。返回的 shutdown 停止 worker 池
// （优雅 drain 在 T10 完成）；恢复流程与就绪门控在 T11。
func NewServer(cfg *Config) (*http.Server, *slog.Logger, func(), error) {
	logger, err := logging.New(logging.Options{
		Dir:       cfg.LogDir,
		Level:     cfg.LogLevel,
		MaxSizeMB: cfg.LogMaxSizeMB, MaxBackups: cfg.LogMaxBackups, MaxAgeDays: cfg.LogMaxAgeDays,
	})
	if err != nil {
		return nil, nil, nil, err
	}

	db, err := mysql.Open(cfg.MysqlDSN)
	if err != nil {
		return nil, logger, nil, err
	}
	if err := mysql.Migrate(db, "migrations"); err != nil {
		return nil, logger, nil, fmt.Errorf("执行数据库迁移失败: %w", err)
	}
	logger.Info("db ready", slog.String("dsn_database", dbURL(cfg.MysqlDSN)))

	fileStore, err := local.New(cfg.DataDir, cfg.UploadMaxFileBytes, cfg.UploadMinFreeBytes)
	if err != nil {
		return nil, logger, nil, fmt.Errorf("初始化数据目录失败: %w", err)
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
	processSvc := processing.NewProcessService(processingTx, query, transcriber, summarizer, worker.NewCancelTable(), logger)
	pool := worker.NewPool(cfg.WorkerConcurrency, cfg.TaskPollInterval, processSvc.Process)
	runCtx, cancelRun := context.WithCancel(context.Background())
	pool.Start(runCtx)
	logger.Info("worker pool started",
		slog.Int("workers", cfg.WorkerConcurrency),
		slog.Duration("poll_interval", cfg.TaskPollInterval))
	shutdown := func() {
		cancelRun()
		pool.Stop()
	}

	uploadSvc := apprec.NewUploadService(fileStore, recordingTx, pool, logger, instanceID)
	uploadHandler := handler.NewUploadHandler(uploadSvc, logger, handler.UploadLimits{
		MaxBodyBytes:    cfg.UploadMaxBodyBytes,
		ReadIdleTimeout: cfg.UploadReadTimeout,
		TotalTimeout:    cfg.UploadTotalTimeout,
	})
	queryHandler := handler.NewQueryHandler(apprec.NewQueryService(query, logger), logger)
	// 重试链（详设 §4.5）：RetryService 复用 recordingTx，COMMIT 后经 pool Notify 唤醒。
	retryHandler := handler.NewRetryHandler(apprec.NewRetryService(recordingTx, pool, logger), logger)

	srv := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: httpapi.New(logger, uploadHandler, queryHandler, retryHandler),
	}
	return srv, logger, shutdown, nil
}

// dbURL 只保留 DSN 中的库名用于日志，避免泄露口令。
func dbURL(dsn string) string {
	name := dsn[strings.LastIndex(dsn, "/")+1:]
	name, _, _ = strings.Cut(name, "?")
	return name
}

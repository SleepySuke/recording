package bootstrap

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"recording-transcription/internal/application/processing"
	apprec "recording-transcription/internal/application/recording"
	"recording-transcription/internal/infrastructure/asr/mock"
	"recording-transcription/internal/infrastructure/filestore/local"
	"recording-transcription/internal/infrastructure/logging"
	"recording-transcription/internal/infrastructure/persistence/mysql"
	"recording-transcription/internal/infrastructure/worker"
	httpapi "recording-transcription/internal/interfaces/http"
	"recording-transcription/internal/interfaces/http/handler"
	"recording-transcription/internal/pkg/uuid"
)

// NewServer 组装：日志 + DB（连接 + 迁移）+ 上传链 + 异步流水线（worker 池 + 认领 +
// Mock 转写）+ http.Server。返回的 shutdown 停止 worker 池（优雅 drain 在 T10 完成）；
// 恢复流程与就绪门控在 T11。
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
	processSvc := processing.NewProcessService(processingTx, query, mock.New(), worker.NewCancelTable(), logger)
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

	uploadSvc := apprec.NewUploadService(fileStore, mysql.NewRecordingTx(db), pool, logger, instanceID)
	uploadHandler := handler.NewUploadHandler(uploadSvc, logger, handler.UploadLimits{
		MaxBodyBytes:    cfg.UploadMaxBodyBytes,
		ReadIdleTimeout: cfg.UploadReadTimeout,
		TotalTimeout:    cfg.UploadTotalTimeout,
	})
	queryHandler := handler.NewQueryHandler(apprec.NewQueryService(query, logger), logger)

	srv := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: httpapi.New(logger, uploadHandler, queryHandler),
	}
	return srv, logger, shutdown, nil
}

// dbURL 只保留 DSN 中的库名用于日志，避免泄露口令。
func dbURL(dsn string) string {
	name := dsn[strings.LastIndex(dsn, "/")+1:]
	name, _, _ = strings.Cut(name, "?")
	return name
}

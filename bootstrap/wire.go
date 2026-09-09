package bootstrap

import (
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"recording-transcription/internal/application/recording"
	"recording-transcription/internal/infrastructure/filestore/local"
	"recording-transcription/internal/infrastructure/logging"
	"recording-transcription/internal/infrastructure/persistence/mysql"
	httpapi "recording-transcription/internal/interfaces/http"
	"recording-transcription/internal/interfaces/http/handler"
	"recording-transcription/internal/pkg/uuid"
)

// NewServer 组装：日志 + DB（连接 + 迁移）+ 上传链（文件存储 + 事务① + 路由）+ http.Server。
// worker 池在 T06 接入，恢复流程与就绪门控在 T11。
func NewServer(cfg *Config) (*http.Server, *slog.Logger, error) {
	logger, err := logging.New(logging.Options{
		Dir:       cfg.LogDir,
		Level:     cfg.LogLevel,
		MaxSizeMB: cfg.LogMaxSizeMB, MaxBackups: cfg.LogMaxBackups, MaxAgeDays: cfg.LogMaxAgeDays,
	})
	if err != nil {
		return nil, nil, err
	}

	db, err := mysql.Open(cfg.MysqlDSN)
	if err != nil {
		return nil, logger, err
	}
	if err := mysql.Migrate(db, "migrations"); err != nil {
		return nil, logger, fmt.Errorf("执行数据库迁移失败: %w", err)
	}
	logger.Info("db ready", slog.String("dsn_database", dbURL(cfg.MysqlDSN)))

	fileStore, err := local.New(cfg.DataDir, cfg.UploadMaxFileBytes, cfg.UploadMinFreeBytes)
	if err != nil {
		return nil, logger, fmt.Errorf("初始化数据目录失败: %w", err)
	}
	uploadSvc := recording.NewUploadService(fileStore, mysql.NewRecordingTx(db), logger, uuid.New())
	uploadHandler := handler.NewUploadHandler(uploadSvc, logger, handler.UploadLimits{
		MaxBodyBytes:    cfg.UploadMaxBodyBytes,
		ReadIdleTimeout: cfg.UploadReadTimeout,
		TotalTimeout:    cfg.UploadTotalTimeout,
	})
	querySvc := recording.NewQueryService(mysql.NewRecordingQuery(db), logger)
	queryHandler := handler.NewQueryHandler(querySvc, logger)

	srv := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: httpapi.New(logger, uploadHandler, queryHandler),
	}
	return srv, logger, nil
}

// dbURL 只保留 DSN 中的库名用于日志，避免泄露口令。
func dbURL(dsn string) string {
	name := dsn[strings.LastIndex(dsn, "/")+1:]
	name, _, _ = strings.Cut(name, "?")
	return name
}

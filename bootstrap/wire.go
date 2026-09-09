package bootstrap

import (
	"log/slog"
	"net/http"

	"recording-transcription/internal/infrastructure/logging"
	httpapi "recording-transcription/internal/interfaces/http"
)

// NewServer 最小组装：日志 + 路由 + http.Server。
// DB 初始化在 T02 接入，worker 池在 T06，恢复流程与就绪门控在 T11。
func NewServer(cfg *Config) (*http.Server, *slog.Logger, error) {
	logger, err := logging.New(logging.Options{
		Dir:       cfg.LogDir,
		Level:     cfg.LogLevel,
		MaxSizeMB: cfg.LogMaxSizeMB, MaxBackups: cfg.LogMaxBackups, MaxAgeDays: cfg.LogMaxAgeDays,
	})
	if err != nil {
		return nil, nil, err
	}
	srv := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: httpapi.New(logger),
	}
	return srv, logger, nil
}

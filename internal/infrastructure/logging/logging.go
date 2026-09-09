// Package logging 初始化 slog JSON 双写（stdout + logs/app.jsonl，lumberjack 轮转），详设 §7.4。
package logging

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/natefinch/lumberjack.v2"
)

// Options 日志初始化参数（详设 §7.4：轮转 20MiB / 5 备份 / 7 天；目录可写是启动检查项）。
type Options struct {
	Dir         string
	Level       string
	MaxSizeMB   int
	MaxBackups  int
	MaxAgeDays  int
}

// New 创建目录并返回双写 logger；stdout 便于容器观察，文件供事后检索。
func New(opt Options) (*slog.Logger, error) {
	if err := os.MkdirAll(opt.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("日志目录不可创建: %w", err)
	}
	file := &lumberjack.Logger{
		Filename: filepath.Join(opt.Dir, "app.jsonl"),
		MaxSize:  opt.MaxSizeMB,
		MaxBackups: opt.MaxBackups,
		MaxAge:   opt.MaxAgeDays,
	}
	var level slog.Level
	if err := level.UnmarshalText([]byte(opt.Level)); err != nil {
		return nil, fmt.Errorf("LOG_LEVEL 非法 %q: %w", opt.Level, err)
	}
	return slog.New(slog.NewJSONHandler(io.MultiWriter(os.Stdout, file), &slog.HandlerOptions{
		Level: level,
		// 详设 §7.4：time 字段统一 UTC
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey && len(groups) == 0 {
				if t, ok := a.Value.Any().(time.Time); ok {
					a.Value = slog.TimeValue(t.UTC())
				}
			}
			return a
		},
	})), nil
}

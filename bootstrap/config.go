// Package bootstrap 读取并校验环境变量配置（详设 §10 配置初值）。
// 缺失必填项启动即报错，不静默降级。
package bootstrap

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config 全部运行配置；字段与详设 §10 / README 配置表一一对应。
type Config struct {
	HTTPAddr          string
	LogDir            string
	LogLevel          string
	LogMaxSizeMB      int
	LogMaxBackups     int
	LogMaxAgeDays     int
	DataDir           string
	MysqlDSN          string
	WorkerConcurrency int
	TaskPollInterval  time.Duration
	LLMBaseURL        string
	LLMModel          string
	LLMAPIKey         string
	LLMTimeout        time.Duration
	DBQueryTimeout    time.Duration
	ShutdownTimeout   time.Duration
	CleanupInterval   time.Duration
	RecoveryMode      string // reset（默认）/ interrupt（详设 §6.3）
}

// LoadConfig 从环境变量组装配置；必填项缺失返回错误（列出全部缺失项）。
func LoadConfig() (*Config, error) {
	c := &Config{
		HTTPAddr:          envDefault("HTTP_ADDR", ":8080"),
		LogDir:            envDefault("LOG_DIR", "./logs"),
		LogLevel:          envDefault("LOG_LEVEL", "INFO"),
		LogMaxSizeMB:      envInt("LOG_MAX_SIZE_MB", 20),
		LogMaxBackups:     envInt("LOG_MAX_BACKUPS", 5),
		LogMaxAgeDays:     envInt("LOG_MAX_AGE_DAYS", 7),
		DataDir:           envDefault("DATA_DIR", "./data/recordings"),
		MysqlDSN:          os.Getenv("MYSQL_DSN"),
		WorkerConcurrency: envInt("WORKER_CONCURRENCY", 3),
		TaskPollInterval:  envDuration("TASK_POLL_INTERVAL", time.Second),
		LLMBaseURL:        os.Getenv("LLM_BASE_URL"),
		LLMModel:          os.Getenv("LLM_MODEL"),
		LLMAPIKey:         os.Getenv("LLM_API_KEY"),
		LLMTimeout:        envDuration("LLM_TIMEOUT", 60*time.Second),
		DBQueryTimeout:    envDuration("DB_QUERY_TIMEOUT", 3*time.Second),
		ShutdownTimeout:   envDuration("SHUTDOWN_TIMEOUT", 20*time.Second),
		CleanupInterval:   envDuration("CLEANUP_INTERVAL", 30*time.Second),
		RecoveryMode:      envDefault("RECOVERY_MODE", "reset"),
	}

	var missing []string
	for k, v := range map[string]string{
		"MYSQL_DSN":    c.MysqlDSN,
		"LLM_BASE_URL": c.LLMBaseURL,
		"LLM_MODEL":    c.LLMModel,
		"LLM_API_KEY":  c.LLMAPIKey,
	} {
		if v == "" {
			missing = append(missing, k)
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("缺少必填环境变量: %v", missing)
	}
	if c.RecoveryMode != "reset" && c.RecoveryMode != "interrupt" {
		return nil, fmt.Errorf("RECOVERY_MODE 必须为 reset 或 interrupt，当前 %q", c.RecoveryMode)
	}
	return c, nil
}

func envDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

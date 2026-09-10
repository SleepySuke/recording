// Package bootstrap 读取并校验环境变量配置（详设 §10 配置初值）。
// 缺失必填项启动即报错，不静默降级。
package bootstrap

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config 全部运行配置；字段与详设 §10 / README 配置表一一对应。
type Config struct {
	HTTPAddr           string
	LogDir             string
	LogLevel           string
	LogMaxSizeMB       int
	LogMaxBackups      int
	LogMaxAgeDays      int
	DataDir            string
	MysqlDSN           string
	UploadMaxFileBytes int64         // 单文件上限（50MiB = 50×1024×1024，详设 §5.1）
	UploadMaxBodyBytes int64         // 请求体总上限，预留 multipart 开销
	UploadMinFreeBytes uint64        // 数据目录磁盘预检阈值（不足 → 503/90003）
	UploadReadTimeout  time.Duration // 单次读的空闲上限
	UploadTotalTimeout time.Duration // 整个上传的总超时
	WorkerConcurrency  int
	TaskPollInterval   time.Duration
	MockASRDelay       time.Duration // -1（默认）= 生产 Mock；≥0 = 确定性替身固定延迟（测试设计 §2）
	MockASRFailFirst   bool          // MOCK_ASR_FAIL_FIRST：确定性替身每个 task_id 首次转写失败（详设 §4.5 重试用例，E2E-02）
	LLMBaseURL         string
	LLMModel           string
	LLMAPIKey          string
	LLMTimeout         time.Duration
	DBQueryTimeout     time.Duration
	ShutdownTimeout    time.Duration
	CleanupInterval    time.Duration
	RecoveryMode       string // reset（默认）/ interrupt（详设 §6.3）
}

// LoadConfig 从环境变量组装配置；必填项缺失、数值非法或超限配置非法返回错误
// （列出全部问题，启动即失败，不静默降级）。
func LoadConfig() (*Config, error) {
	// 数值/Duration 环境变量：解析失败记为错误（不静默回退默认值），最后统一报告。
	var malformed []error
	envInt := func(key string, def int) int {
		if v := os.Getenv(key); v != "" {
			if n, err := strconv.Atoi(v); err == nil {
				return n
			}
			malformed = append(malformed, fmt.Errorf("环境变量 %s=%q 不是合法整数", key, v))
		}
		return def
	}
	envDuration := func(key string, def time.Duration) time.Duration {
		if v := os.Getenv(key); v != "" {
			if d, err := time.ParseDuration(v); err == nil {
				return d
			}
			malformed = append(malformed, fmt.Errorf("环境变量 %s=%q 不是合法时长（如 30s、10m）", key, v))
		}
		return def
	}
	envBool := func(key string, def bool) bool {
		if v := os.Getenv(key); v != "" {
			if b, err := strconv.ParseBool(v); err == nil {
				return b
			}
			malformed = append(malformed, fmt.Errorf("环境变量 %s=%q 不是合法布尔值（true/false）", key, v))
		}
		return def
	}

	uploadFileMB := envInt("UPLOAD_MAX_FILE_MB", 50)
	uploadBodyMB := envInt("UPLOAD_MAX_BODY_MB", 53)
	uploadFreeMB := envInt("UPLOAD_MIN_FREE_DISK_MB", 512)

	c := &Config{
		HTTPAddr:           envDefault("HTTP_ADDR", ":8080"),
		LogDir:             envDefault("LOG_DIR", "./logs"),
		LogLevel:           envDefault("LOG_LEVEL", "INFO"),
		LogMaxSizeMB:       envInt("LOG_MAX_SIZE_MB", 20),
		LogMaxBackups:      envInt("LOG_MAX_BACKUPS", 5),
		LogMaxAgeDays:      envInt("LOG_MAX_AGE_DAYS", 7),
		DataDir:            envDefault("DATA_DIR", "./data/recordings"),
		MysqlDSN:           os.Getenv("MYSQL_DSN"),
		UploadMaxFileBytes: int64(uploadFileMB) * 1024 * 1024,
		UploadMaxBodyBytes: int64(uploadBodyMB) * 1024 * 1024,
		UploadMinFreeBytes: uint64(uploadFreeMB) * 1024 * 1024,
		UploadReadTimeout:  envDuration("UPLOAD_READ_TIMEOUT", 30*time.Second),
		UploadTotalTimeout: envDuration("UPLOAD_TOTAL_TIMEOUT", 10*time.Minute),
		WorkerConcurrency:  envInt("WORKER_CONCURRENCY", 3),
		TaskPollInterval:   envDuration("TASK_POLL_INTERVAL", time.Second),
		MockASRDelay:       envMockASRDelay(&malformed),
		MockASRFailFirst:   envBool("MOCK_ASR_FAIL_FIRST", false),
		LLMBaseURL:         os.Getenv("LLM_BASE_URL"),
		LLMModel:           os.Getenv("LLM_MODEL"),
		LLMAPIKey:          os.Getenv("LLM_API_KEY"),
		LLMTimeout:         envDuration("LLM_TIMEOUT", 60*time.Second),
		DBQueryTimeout:     envDuration("DB_QUERY_TIMEOUT", 3*time.Second),
		ShutdownTimeout:    envDuration("SHUTDOWN_TIMEOUT", 20*time.Second),
		CleanupInterval:    envDuration("CLEANUP_INTERVAL", 30*time.Second),
		RecoveryMode:       envDefault("RECOVERY_MODE", "reset"),
	}

	if len(malformed) > 0 {
		return nil, errors.Join(malformed...)
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

	// 上传限制：非法值启动即失败（详设 §10）。负的磁盘阈值经 uint64 转换会变成
	// 天文数字，导致所有上传预检失败（503），必须在转换前拦截。
	switch {
	case uploadFileMB <= 0:
		return nil, fmt.Errorf("UPLOAD_MAX_FILE_MB 必须为正数，当前 %d", uploadFileMB)
	case uploadBodyMB <= 0:
		return nil, fmt.Errorf("UPLOAD_MAX_BODY_MB 必须为正数，当前 %d", uploadBodyMB)
	case uploadFreeMB < 0:
		return nil, fmt.Errorf("UPLOAD_MIN_FREE_DISK_MB 不能为负数，当前 %d", uploadFreeMB)
	case uploadBodyMB < uploadFileMB:
		return nil, fmt.Errorf("UPLOAD_MAX_BODY_MB（%d）不应小于 UPLOAD_MAX_FILE_MB（%d）：请求体包含文件字节与 multipart 开销", uploadBodyMB, uploadFileMB)
	case c.UploadReadTimeout <= 0:
		return nil, fmt.Errorf("UPLOAD_READ_TIMEOUT 必须为正时长，当前 %s", c.UploadReadTimeout)
	case c.UploadTotalTimeout <= 0:
		return nil, fmt.Errorf("UPLOAD_TOTAL_TIMEOUT 必须为正时长，当前 %s", c.UploadTotalTimeout)
	}
	return c, nil
}

func envDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// envMockASRDelay 读 MOCK_ASR_DELAY（duration 格式）：未设置 → -1（生产 Mock）；
// 设置且为负 → malformed（-1 保留给「未设置」，显式负值必是配置笔误，报错定位到变量名）。
func envMockASRDelay(malformed *[]error) time.Duration {
	v := os.Getenv("MOCK_ASR_DELAY")
	if v == "" {
		return -1
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		*malformed = append(*malformed, fmt.Errorf("环境变量 MOCK_ASR_DELAY=%q 不是合法时长（如 30s、150ms）", v))
		return -1
	}
	if d < 0 {
		*malformed = append(*malformed, fmt.Errorf("环境变量 MOCK_ASR_DELAY 不能为负数（未设置即用生产 Mock），当前 %q", v))
		return -1
	}
	return d
}

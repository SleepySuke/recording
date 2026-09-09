package unit

import (
	"strings"
	"testing"

	"recording-transcription/bootstrap"
)

// 测试依据：T04 修复轮 Minor #4——UPLOAD_* 限制与相关数值的启动即失败校验；
// 非法值（<=0 / 负数 / 非法格式 / body < file）不静默降级，报错可定位到环境变量名。

// setRequiredEnv 补齐必填项（与上传校验无关）。
func setRequiredEnv(t *testing.T) {
	t.Helper()
	t.Setenv("MYSQL_DSN", "user:pass@tcp(127.0.0.1:3306)/db")
	t.Setenv("LLM_BASE_URL", "http://localhost:8000")
	t.Setenv("LLM_MODEL", "test-model")
	t.Setenv("LLM_API_KEY", "test-key")
}

// TestLoadConfigUploadGuards：非法上传配置 → 报错含对应环境变量名；合法配置 → 通过。
func TestLoadConfigUploadGuards(t *testing.T) {
	cases := []struct {
		name    string
		env     map[string]string
		wantErr string // 期望报错包含的片段（空 = 应通过）
	}{
		{name: "默认值通过", wantErr: ""},
		{name: "显式合法值通过", env: map[string]string{
			"UPLOAD_MAX_FILE_MB": "10", "UPLOAD_MAX_BODY_MB": "12",
			"UPLOAD_MIN_FREE_DISK_MB": "64", "UPLOAD_READ_TIMEOUT": "5s", "UPLOAD_TOTAL_TIMEOUT": "1m",
		}, wantErr: ""},
		{name: "文件上限为零", env: map[string]string{"UPLOAD_MAX_FILE_MB": "0"},
			wantErr: "UPLOAD_MAX_FILE_MB"},
		{name: "文件上限为负", env: map[string]string{"UPLOAD_MAX_FILE_MB": "-50"},
			wantErr: "UPLOAD_MAX_FILE_MB"},
		{name: "请求体上限为零", env: map[string]string{"UPLOAD_MAX_BODY_MB": "0"},
			wantErr: "UPLOAD_MAX_BODY_MB"},
		{name: "请求体上限小于文件上限", env: map[string]string{
			"UPLOAD_MAX_FILE_MB": "50", "UPLOAD_MAX_BODY_MB": "49",
		}, wantErr: "UPLOAD_MAX_BODY_MB"},
		{name: "磁盘阈值为负（uint64 转换前拦截）", env: map[string]string{"UPLOAD_MIN_FREE_DISK_MB": "-1"},
			wantErr: "UPLOAD_MIN_FREE_DISK_MB"},
		{name: "读空闲超时为零", env: map[string]string{"UPLOAD_READ_TIMEOUT": "0"},
			wantErr: "UPLOAD_READ_TIMEOUT"},
		{name: "总超时为零", env: map[string]string{"UPLOAD_TOTAL_TIMEOUT": "0s"},
			wantErr: "UPLOAD_TOTAL_TIMEOUT"},
		{name: "数值非法格式", env: map[string]string{"UPLOAD_MAX_FILE_MB": "fifty"},
			wantErr: "UPLOAD_MAX_FILE_MB"},
		{name: "时长非法格式", env: map[string]string{"UPLOAD_READ_TIMEOUT": "soon"},
			wantErr: "UPLOAD_READ_TIMEOUT"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setRequiredEnv(t)
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			cfg, err := bootstrap.LoadConfig()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("合法配置不应报错: %v", err)
				}
				if cfg.UploadMaxFileBytes <= 0 || cfg.UploadMaxBodyBytes <= 0 {
					t.Errorf("上传上限不应为非正数: %+v", cfg)
				}
				return
			}
			if err == nil {
				t.Fatalf("非法配置应启动即失败，env=%v", tc.env)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("报错 %q 未包含 %q（应可定位到环境变量名）", err.Error(), tc.wantErr)
			}
		})
	}
}

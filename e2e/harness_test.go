// e2e 测试基建（T06E / 测试设计 §2、§5）：每用例一个 harness——TRUNCATE → 构造
// 真实 Config → bootstrap.NewServer → 真实 TCP 监听 + 真实 http.Client 驱动，
// 断言后经 t.Cleanup 关停（返回的 shutdown + 关闭监听）。
package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"recording-transcription/bootstrap"
	"recording-transcription/internal/infrastructure/llm"
)

// 上传限额取生产默认（50MiB/53MiB/512MiB，详设 §10），驱动层不另行放宽。
const (
	e2eMaxFileBytes = 50 * 1024 * 1024
	e2eMaxBodyBytes = 53 * 1024 * 1024
	e2eMinFreeBytes = 512 * 1024 * 1024
	e2eWorkers      = 3                     // 与生产默认一致（详设 §10 WORKER_CONCURRENCY=3）
	e2ePollInterval = 20 * time.Millisecond // 快轮询加速收敛，唤醒语义与生产一致
)

// e2eHarness 一台真实装配的服务：断言连接 + BaseURL + 数据/日志目录 + LLM 替身。
type e2eHarness struct {
	t        *testing.T
	DB       *gorm.DB
	BaseURL  string
	DataDir  string
	LogDir   string
	Fake     *llm.FakeLLM // 进程内假渠道（默认 normal；T08 的 E2E-03 切换挂起模式）
	client   *http.Client
	shutdown func() // NewServer 返回的停池函数（幂等）
}

// e2eOpts 装配选项（按用例注入替身形态，测试设计 §2；T08 起随用例增长）。
type e2eOpts struct {
	MockASRDelay     time.Duration // ≥0 = 确定性替身固定延迟（E2E 均用；-1 生产 Mock 不使用）
	MockASRFailFirst bool          // 确定性替身每个 task_id 首次转写失败（E2E-02，详设 §4.5）
	LLMTimeout       time.Duration // 0 = 默认 10s；E2E-03 缩短以快速触发 50001
}

// newE2E 装配并启动一台真实服务。opts 逐项透传 Config（对应 MOCK_ASR_* / LLM_TIMEOUT）。
func newE2E(t *testing.T, opts e2eOpts) *e2eHarness {
	t.Helper()
	gin.SetMode(gin.TestMode) // 只关路由注册噪音，不影响真实 HTTP 行为
	db := requireTestDB(t)
	dataDir := t.TempDir()
	logDir := t.TempDir()
	llmTimeout := opts.LLMTimeout
	if llmTimeout <= 0 {
		llmTimeout = 10 * time.Second
	}

	// 摘要段（T07）：真实 LLM 适配器指向进程内 FakeLLM（默认 normal 正常应答），
	// 服务经 cfg.LLM* 三项与其相连——摘要链路走真实 HTTP，只替身渠道本身。
	fake := llm.NewFake()
	fakeSrv := httptest.NewServer(fake)
	t.Cleanup(fakeSrv.Close)

	cfg := &bootstrap.Config{
		HTTPAddr:           "127.0.0.1:0",
		LogDir:             logDir,
		LogLevel:           "INFO",
		LogMaxSizeMB:       20,
		LogMaxBackups:      5,
		LogMaxAgeDays:      7,
		DataDir:            dataDir,
		MysqlDSN:           os.Getenv("TEST_MYSQL_DSN"),
		UploadMaxFileBytes: e2eMaxFileBytes,
		UploadMaxBodyBytes: e2eMaxBodyBytes,
		UploadMinFreeBytes: e2eMinFreeBytes,
		UploadReadTimeout:  30 * time.Second,
		UploadTotalTimeout: time.Minute,
		WorkerConcurrency:  e2eWorkers,
		TaskPollInterval:   e2ePollInterval,
		MockASRDelay:       opts.MockASRDelay,
		MockASRFailFirst:   opts.MockASRFailFirst,
		// LLM 指向进程内 FakeLLM（真实适配器走真实 HTTP 外呼）。
		LLMBaseURL:      fakeSrv.URL,
		LLMModel:        "fake-model",
		LLMAPIKey:       "fake-key",
		LLMTimeout:      llmTimeout,
		DBQueryTimeout:  3 * time.Second,
		ShutdownTimeout: 20 * time.Second,
		CleanupInterval: 30 * time.Second,
		RecoveryMode:    "reset",
	}

	srv, _, shutdown, err := bootstrap.NewServer(cfg)
	if err != nil {
		t.Fatalf("bootstrap.NewServer 失败: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("监听失败: %v", err)
	}
	go func() { _ = srv.Serve(ln) }() // 真实 TCP，非 httptest
	t.Cleanup(func() {
		shutdown() // 停 worker 池（幂等）
		_ = ln.Close()
		_ = srv.Close()
	})

	return &e2eHarness{
		t:        t,
		DB:       db,
		BaseURL:  "http://" + ln.Addr().String(),
		DataDir:  dataDir,
		LogDir:   logDir,
		Fake:     fake,
		client:   &http.Client{Timeout: 10 * time.Second},
		shutdown: shutdown,
	}
}

// stopWorkers 提前停掉 worker 池（E2E-08 直插种子前调用，防止池把 pending 种子
// 认领推进；HTTP 服务不受影响）。幂等，t.Cleanup 再调一次无害。
func (h *e2eHarness) stopWorkers() {
	h.t.Helper()
	h.shutdown()
}

// doGet 真实 HTTP GET，返回状态码与响应体。
func (h *e2eHarness) doGet(path string) (int, []byte) {
	h.t.Helper()
	resp, err := h.client.Get(h.BaseURL + path)
	if err != nil {
		h.t.Fatalf("GET %s 失败: %v", path, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		h.t.Fatalf("读取响应体失败: %v", err)
	}
	return resp.StatusCode, body
}

// doRetry 真实 HTTP POST /v1/tasks/:id/retry（详设 §8.1），返回状态码与响应体原文。
func (h *e2eHarness) doRetry(taskID string) (int, []byte) {
	h.t.Helper()
	req, err := http.NewRequest(http.MethodPost, h.BaseURL+"/v1/tasks/"+taskID+"/retry", nil)
	if err != nil {
		h.t.Fatalf("构造 retry 请求失败: %v", err)
	}
	resp, err := h.client.Do(req)
	if err != nil {
		h.t.Fatalf("POST retry 失败: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		h.t.Fatalf("读取 retry 响应体失败: %v", err)
	}
	return resp.StatusCode, body
}

// uploadResp 上传 202 响应体（与 handler 实际返回一致）。
type uploadResp struct {
	RecordingID string `json:"recording_id"`
	TaskID      string `json:"task_id"`
	Status      string `json:"status"`
}

// postUpload 真实 multipart 上传（每次调用独立构造请求体，并发安全）。
// 返回 (响应体, HTTP 状态码, 传输/解码错误)：状态码与响应体原样交 golden 比对，
// 只有传输失败/响应不可解码才返回错误（并发用例在子 goroutine 调用，不得 Fatal）。
func (h *e2eHarness) postUpload(filename string, content []byte) (uploadResp, int, error) {
	var out uploadResp
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	fw, err := w.CreateFormFile("file", filename)
	if err != nil {
		return out, 0, fmt.Errorf("构造 multipart 失败: %w", err)
	}
	if _, err := fw.Write(content); err != nil {
		return out, 0, fmt.Errorf("写入 multipart 内容失败: %w", err)
	}
	if err := w.Close(); err != nil {
		return out, 0, fmt.Errorf("关闭 multipart writer 失败: %w", err)
	}
	req, err := http.NewRequest(http.MethodPost, h.BaseURL+"/v1/recordings", &buf)
	if err != nil {
		return out, 0, fmt.Errorf("构造请求失败: %w", err)
	}
	req.Header.Set("Content-Type", w.FormDataContentType())
	resp, err := h.client.Do(req)
	if err != nil {
		return out, 0, fmt.Errorf("上传请求失败: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return out, resp.StatusCode, fmt.Errorf("读取上传响应失败: %w", err)
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return out, resp.StatusCode, fmt.Errorf("上传响应不是合法 JSON: %w, body=%q", err, body)
	}
	return out, resp.StatusCode, nil
}

// 状态推进序（用于轮询不回退检查；unknown 必然是异常值）。
// failed 排在 done 之后（T08 起 E2E-02/03 轮询到 failed）：单次 pollTask 会话内
// 单调即可；重试后新一轮从 pending 重新起步，跨轮比较无意义（每次调用独立序列）。
var statusRank = map[string]int{"pending": 0, "transcribing": 1, "summarizing": 2, "done": 3, "failed": 4}

// taskBody GET /v1/tasks/:id 响应体（与 handler 实际返回一致）。
type taskBody struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

// pollTask 轮询任务接口（间隔 e2ePollInterval）至 want 或期限：
// 返回按序观察到的状态列表（供「不得回退」检查）；期限已到仍未见 want 则 Fatal。
// 严格的状态序列断言走事件链（DB），轮询可能漏掉中间态，此处不做漏态强断言。
func (h *e2eHarness) pollTask(taskID, want string, timeout time.Duration) []string {
	h.t.Helper()
	var observed []string
	deadline := time.Now().Add(timeout)
	for {
		var tb taskBody
		code, body := h.doGet("/v1/tasks/" + taskID)
		if code != http.StatusOK {
			h.t.Fatalf("GET /v1/tasks/%s status = %d, want 200, body=%q", taskID, code, body)
		}
		if err := json.Unmarshal(body, &tb); err != nil {
			h.t.Fatalf("任务响应不是合法 JSON: %v, body=%q", err, body)
		}
		observed = append(observed, tb.Status)
		last, ok := statusRank[tb.Status]
		if !ok {
			h.t.Fatalf("未知状态 %q（task %s）", tb.Status, taskID)
		}
		if len(observed) > 1 {
			prev := statusRank[observed[len(observed)-2]]
			if prev > last {
				h.t.Fatalf("轮询观察到状态回退: %v（task %s）", observed, taskID)
			}
		}
		if tb.Status == want {
			return observed
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("任务 %s 未在 %s 内到达 %s，观察序列 = %v", taskID, timeout, want, observed)
		}
		time.Sleep(e2ePollInterval)
	}
}

// evtRow 事件链采集行（§5.1：event + attempt + error_code 按 event_seq），
// JSON 标签即 golden 中的事件序列形状。
type evtRow struct {
	EventSeq   int64   `json:"event_seq"`
	Event      string  `json:"event"`
	Attempt    int     `json:"attempt"`
	ErrorCode  *int    `json:"error_code"`
	FromStatus *string `json:"from_status"`
	ToStatus   string  `json:"to_status"`
}

// taskEventChain 采集某任务按 event_seq 排序的事件链（真实值，掩码交给 runGolden）。
func (h *e2eHarness) taskEventChain(taskID string) []evtRow {
	h.t.Helper()
	var evts []evtRow
	if err := h.DB.Raw(
		"SELECT event_seq, event, attempt, error_code, from_status, to_status FROM task_events WHERE task_id = ? ORDER BY event_seq",
		taskID).Scan(&evts).Error; err != nil {
		h.t.Fatalf("查询事件链失败: %v", err)
	}
	return evts
}

// tableCounts 三表行数采集（§5.1 DB 终态）。
func (h *e2eHarness) tableCounts() map[string]int64 {
	h.t.Helper()
	out := make(map[string]int64, 3)
	for _, table := range []string{"recordings", "tasks", "task_events"} {
		var n int64
		if err := h.DB.Raw("SELECT COUNT(*) FROM " + table).Scan(&n).Error; err != nil {
			h.t.Fatalf("统计 %s 失败: %v", table, err)
		}
		out[table] = n
	}
	return out
}

// mirroredEvents 解析 LogDir/app.jsonl，返回该 task_id 的镜像事件名集合（按出现序）。
// 镜像在事务提交后同步写出（详设 §7.4），与轮询可见之间有微秒级窗口，重试收敛。
func (h *e2eHarness) mirroredEvents(taskID string, wantCount int) []string {
	h.t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	var events []string
	for {
		events = events[:0]
		data, err := os.ReadFile(filepath.Join(h.LogDir, "app.jsonl"))
		if err != nil {
			h.t.Fatalf("读取 app.jsonl 失败: %v", err)
		}
		for _, line := range strings.Split(string(data), "\n") {
			if line == "" {
				continue
			}
			var rec struct {
				Event  string `json:"event"`
				TaskID string `json:"task_id"`
			}
			if json.Unmarshal([]byte(line), &rec) == nil && rec.TaskID == taskID && rec.Event != "" {
				events = append(events, rec.Event)
			}
		}
		if len(events) >= wantCount || time.Now().After(deadline) {
			return events
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// contentOf 生成 size 字节的可复现内容（与集成测试同模式）。
func contentOf(size int) []byte {
	pattern := []byte("abcdefghijklmnopqrstuvwxyz012345")
	out := bytes.Repeat(pattern, size/len(pattern))
	return append(out, pattern[:size%len(pattern)]...)
}

// checkInvariants 不变量巡检（测试设计 §6，R4）：每用例末尾对测试库断言——
// 无孤儿任务、无重复 (task_id, event_seq)、无半删除残留。
// wantDeletingRows：deleting_at 非空的 recordings 行数期望。当前无删除功能，
// 正常用例应为 0；E2E-08 直插 1 条 deleting 种子故传 1（T09 交付删除后放宽为
// 「清理完成后 deleting 行为 0」）。
func checkInvariants(t *testing.T, db *gorm.DB, wantDeletingRows int64) {
	t.Helper()
	var orphans int64
	if err := db.Raw(
		`SELECT COUNT(*) FROM tasks t LEFT JOIN recordings r ON r.id = t.recording_id WHERE r.id IS NULL`,
	).Scan(&orphans).Error; err != nil {
		t.Fatalf("孤儿巡检失败: %v", err)
	}
	if orphans != 0 {
		t.Errorf("不变量违规：存在 %d 个孤儿任务（tasks 无对应 recordings）", orphans)
	}

	var dupSeq int64
	if err := db.Raw(
		`SELECT COUNT(*) FROM (SELECT task_id, event_seq FROM task_events GROUP BY task_id, event_seq HAVING COUNT(*) > 1) d`,
	).Scan(&dupSeq).Error; err != nil {
		t.Fatalf("event_seq 重复巡检失败: %v", err)
	}
	if dupSeq != 0 {
		t.Errorf("不变量违规：%d 组 (task_id, event_seq) 重复", dupSeq)
	}

	var deleting int64
	if err := db.Raw(`SELECT COUNT(*) FROM recordings WHERE deleting_at IS NOT NULL`).Scan(&deleting).Error; err != nil {
		t.Fatalf("半删除巡检失败: %v", err)
	}
	if deleting != wantDeletingRows {
		t.Errorf("deleting_at 非空行数 = %d, want %d（当前无删除功能，正常应为 0；T09 后放宽语义）", deleting, wantDeletingRows)
	}
}

// insertPendingTask 直插一对 pending 录音+任务（含 task_created 事件，event_seq=1）；
// 写法参考 tests/integration/pipeline_test.go 的同名助手。deleting=true 时预置
// deleting_at（列表应过滤）。n 越小 created_at 越早。返回 (recordingID, taskID)。
func insertPendingTask(t *testing.T, db *gorm.DB, n int, deleting bool) (string, string) {
	t.Helper()
	recID := fmt.Sprintf("b0000000-0000-0000-0000-%012d", n)
	taskID := fmt.Sprintf("a0000000-0000-0000-0000-%012d", n)
	now := time.Now().UTC()
	createdAt := now.Add(-time.Duration(1000-n) * time.Second) // n 越小越早

	var deletingAt any
	if deleting {
		deletingAt = createdAt.Add(time.Second)
	}
	stmts := []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO recordings (id, original_filename, storage_path, extension, size_bytes, content_hash, deleting_at, created_at, updated_at)
		   VALUES (?, ?, ?, 'wav', 1024, ?, ?, ?, ?)`,
			[]any{recID, fmt.Sprintf("e2e-%d.wav", n), fmt.Sprintf("e2e/%d.wav", n), fmt.Sprintf("%064d", n), deletingAt, createdAt, createdAt}},
		{`INSERT INTO tasks (id, recording_id, status, attempt, event_seq, created_request_id, created_at, updated_at)
		   VALUES (?, ?, 'pending', 1, 1, 'req-e2e', ?, ?)`,
			[]any{taskID, recID, createdAt, createdAt}},
		{`INSERT INTO task_events (event_id, task_id, recording_id, event_seq, attempt, event, occurred_at, level, to_status, created_request_id, instance_id)
		   VALUES (?, ?, ?, 1, 1, 'task_created', ?, 'INFO', 'pending', 'req-e2e', ?)`,
			[]any{fmt.Sprintf("c0000000-0000-0000-0000-%012d", n), taskID, recID, createdAt, "11111111-1111-1111-1111-111111111111"}},
	}
	for _, s := range stmts {
		if err := db.Exec(s.sql, s.args...).Error; err != nil {
			t.Fatalf("预置数据失败: %v (sql=%s)", err, s.sql)
		}
	}
	return recID, taskID
}

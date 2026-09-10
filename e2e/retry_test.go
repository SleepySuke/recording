// E2E-02 / E2E-03 / E2E-04（测试设计 §5.2，T08；映射表 §5.0）：
// 自动退避重试 → 新一轮成功，以及最终失败后的手动重试冲突与
// 不存在（详设 §8.3）。E2E-02 失败注入 = MOCK_ASR_FAIL_FIRST（首轮转写失败 40001）；
// E2E-03 = FakeLLM 挂起 + 缩短的 LLM_TIMEOUT（首轮摘要超时 50001，详设 §9）；
// 两者共用同一采集形状与流程，仅注入方式不同，golden 各自手写。
package e2e

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"recording-transcription/internal/infrastructure/llm"
)

// taskDetailBody GET /v1/tasks/:id 响应体（重试用例只采集 status/attempt/error）。
type taskDetailBody struct {
	ID          string       `json:"id"`
	Status      string       `json:"status"`
	Attempt     int          `json:"attempt"`
	Error       *taskErrBody `json:"error"`
	NextRetryAt *time.Time   `json:"next_retry_at"`
}

// taskErrBody 任务异步执行错误（详设 §8.4：成功/处理中为 null）。
type taskErrBody struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// retryRespBody retry 202 响应体（详设 §8.1）。
type retryRespBody struct {
	TaskID  string `json:"task_id"`
	Status  string `json:"status"`
	Attempt int    `json:"attempt"`
}

// retryFlowActual E02/E03.json 的采集形状（§5.1：HTTP 响应、失败快照、重试受理、
// 终态产物、事件链、日志镜像、三表行数；标识/时间戳经 runGolden 掩码归一化）。
type retryFlowActual struct {
	Upload struct {
		HTTPStatus  int    `json:"http_status"`
		RecordingID string `json:"recording_id"`
		TaskID      string `json:"task_id"`
		Status      string `json:"status"`
	} `json:"upload"`
	AutoRetry struct {
		HTTPStatus int          `json:"http_status"`
		TaskID     string       `json:"task_id"`
		Status     string       `json:"status"`
		Attempt    int          `json:"attempt"`
		Error      *taskErrBody `json:"error"`
	} `json:"auto_retry"`
	TaskFinal struct {
		Status      string      `json:"status"`
		Attempt     int         `json:"attempt"`
		EventSeq    int64       `json:"event_seq"`
		Transcript  string      `json:"transcript"`
		SummaryJSON *resultBody `json:"summary_json"`
	} `json:"task_final"`
	// 状态序列 = 事件链推导（轮询序列可能漏态且重试后从头起步，只看不回退）。
	StatusSequence []string         `json:"status_sequence"`
	Events         []evtRow         `json:"events"`
	MirrorEvents   []string         `json:"mirror_events"`
	TableCounts    map[string]int64 `json:"table_counts"`
}

// runRetryFlow 首轮失败后观察持久化的自动重试，再恢复替身并等待下一轮成功。
func runRetryFlow(t *testing.T, caseName string, opts e2eOpts, inject, recover func(h *e2eHarness)) {
	t.Helper()
	h := newE2E(t, opts)
	inject(h)

	resp, upCode, err := h.postUpload(caseName+"-meeting.wav", contentOf(8*1024))
	if err != nil {
		t.Fatalf("上传失败: %v", err)
	}
	if resp.TaskID == "" || resp.RecordingID == "" {
		t.Fatalf("上传响应缺 recording_id/task_id: %+v (status=%d)", resp, upCode)
	}

	var a retryFlowActual
	a.Upload.HTTPStatus = upCode
	a.Upload.RecordingID = resp.RecordingID
	a.Upload.TaskID = resp.TaskID
	a.Upload.Status = resp.Status

	// 通过真实 HTTP 观察到首次失败已原子安排为 pending/attempt=2，且带退避时间。
	var auto taskDetailBody
	deadline := time.Now().Add(10 * time.Second)
	for {
		code, raw := h.doGet("/v1/tasks/" + resp.TaskID)
		if code != http.StatusOK {
			t.Fatalf("GET 任务 status = %d, want 200", code)
		}
		if err := json.Unmarshal(raw, &auto); err != nil {
			t.Fatalf("任务响应不是合法 JSON: %v", err)
		}
		if auto.Status == "pending" && auto.Attempt == 2 && auto.NextRetryAt != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("未观察到自动重试调度: %+v", auto)
		}
		time.Sleep(20 * time.Millisecond)
	}
	a.AutoRetry.HTTPStatus = http.StatusOK
	a.AutoRetry.TaskID, a.AutoRetry.Status, a.AutoRetry.Attempt, a.AutoRetry.Error = auto.ID, auto.Status, auto.Attempt, auto.Error

	// 恢复渠道后等待到期的第二轮自动执行成功；不发出人工 retry 请求。
	recover(h)
	h.pollTask(resp.TaskID, "done", 10*time.Second)

	var task struct {
		Status      string
		Attempt     int
		EventSeq    int64
		Transcript  string
		SummaryJSON *string
	}
	if err := h.DB.Raw(
		"SELECT status, attempt, event_seq, transcript, summary_json FROM tasks WHERE id = ?",
		resp.TaskID).Scan(&task).Error; err != nil {
		t.Fatalf("查询任务失败: %v", err)
	}
	a.TaskFinal.Status = task.Status
	a.TaskFinal.Attempt = task.Attempt
	a.TaskFinal.EventSeq = task.EventSeq
	a.TaskFinal.Transcript = task.Transcript
	if task.SummaryJSON != nil {
		var rb resultBody
		if err := json.Unmarshal([]byte(*task.SummaryJSON), &rb); err != nil {
			t.Fatalf("summary_json 不可解析: %v, raw=%q", err, *task.SummaryJSON)
		}
		a.TaskFinal.SummaryJSON = &rb
	}

	a.Events = h.taskEventChain(resp.TaskID)
	for _, e := range a.Events {
		a.StatusSequence = append(a.StatusSequence, e.ToStatus)
	}
	a.MirrorEvents = h.mirroredEvents(resp.TaskID, len(a.Events))
	a.TableCounts = h.tableCounts()

	runGolden(t, caseName, &a, map[string]string{
		resp.RecordingID: "<recording_id>",
		resp.TaskID:      "<task_id>",
	})
	checkInvariants(t, h.DB)
}

// TestE2E02_TranscribeFailThenRetryDone：转写失败→手动重试（测试设计 §5.2 E2E-02，
// 详设 §4.5）：首轮 failed/40001 → retry 202 → done 且 attempt=2；事件链含
// task_failed + task_retry_accepted + 新一轮 task_claimed（详设 §7.1）。
func TestE2E02_TranscribeFailThenRetryDone(t *testing.T) {
	runRetryFlow(t, "E02",
		e2eOpts{MockASRDelay: 100 * time.Millisecond, MockASRFailFirst: true},
		func(h *e2eHarness) {}, // 失败注入在配置（MOCK_ASR_FAIL_FIRST）
		func(h *e2eHarness) {}) // 替身有状态：同 task_id 第二次转写自然成功
}

// TestE2E03_LLMTimeoutThenRetryDone：摘要超时→手动重试（测试设计 §5.2 E2E-03，
// 详设 §9/§4.5）：FakeLLM 挂起 + 缩短 LLM_TIMEOUT → 首轮 failed/50001 →
// 恢复 normal → retry 202 → done 且 attempt=2。
func TestE2E03_LLMTimeoutThenRetryDone(t *testing.T) {
	runRetryFlow(t, "E03",
		e2eOpts{MockASRDelay: 20 * time.Millisecond, LLMTimeout: 400 * time.Millisecond},
		func(h *e2eHarness) { h.Fake.SetMode(llm.FakeModeHang) },
		func(h *e2eHarness) { h.Fake.SetMode(llm.FakeModeNormal) })
}

// e04Actual E04.json 的采集形状：上传主链路 + 两类重试拒绝（详设 §8.3）。
type e04Actual struct {
	Upload struct {
		HTTPStatus  int    `json:"http_status"`
		RecordingID string `json:"recording_id"`
		TaskID      string `json:"task_id"`
		Status      string `json:"status"`
	} `json:"upload"`
	RetryOnDone struct {
		HTTPStatus int          `json:"http_status"`
		Error      *taskErrBody `json:"error"`
	} `json:"retry_on_done"`
	RetryOnMissing struct {
		HTTPStatus int          `json:"http_status"`
		Error      *taskErrBody `json:"error"`
	} `json:"retry_on_missing"`
	TableCounts map[string]int64 `json:"table_counts"`
}

// TestE2E04_RetryConflictAndMissing（测试设计 §5.2 E2E-04，详设 §8.3）：
// 对 done 任务 retry → 409/30002；对随机 UUID retry → 404/30001。
// 任务不被推进、无新增事件。
func TestE2E04_RetryConflictAndMissing(t *testing.T) {
	h := newE2E(t, e2eOpts{MockASRDelay: 0})

	resp, upCode, err := h.postUpload("E04-meeting.wav", contentOf(8*1024))
	if err != nil {
		t.Fatalf("上传失败: %v", err)
	}
	if resp.TaskID == "" {
		t.Fatalf("上传响应缺 task_id: %+v (status=%d)", resp, upCode)
	}
	h.pollTask(resp.TaskID, "done", 10*time.Second)

	var a e04Actual
	a.Upload.HTTPStatus = upCode
	a.Upload.RecordingID = resp.RecordingID
	a.Upload.TaskID = resp.TaskID
	a.Upload.Status = resp.Status

	missingID := "4f4f4f4f-4f4f-4f4f-4f4f-4f4f4f4f4f4f"
	for _, tc := range []struct {
		name string
		id   string
		dst  *struct {
			HTTPStatus int          `json:"http_status"`
			Error      *taskErrBody `json:"error"`
		}
		wantHTTP int
	}{
		{"retry_on_done", resp.TaskID, &a.RetryOnDone, http.StatusConflict},
		{"retry_on_missing", missingID, &a.RetryOnMissing, http.StatusNotFound},
	} {
		code, raw := h.doRetry(tc.id)
		if code != tc.wantHTTP {
			t.Fatalf("%s status = %d, want %d, body=%q", tc.name, code, tc.wantHTTP, raw)
		}
		var body struct {
			Error taskErrBody `json:"error"`
		}
		if err := json.Unmarshal(raw, &body); err != nil {
			t.Fatalf("%s 响应不是合法 JSON: %v, body=%q", tc.name, err, raw)
		}
		tc.dst.HTTPStatus = code
		errBody := body.Error
		tc.dst.Error = &errBody
	}

	a.TableCounts = h.tableCounts()

	// 错误体采集 code/message（request_id 的存在性与 X-Request-ID 一致性由
	// 单元路由测试覆盖，不进 golden）。
	runGolden(t, "E04", &a, map[string]string{
		resp.RecordingID: "<recording_id>",
		resp.TaskID:      "<task_id>",
	})
	checkInvariants(t, h.DB)
}

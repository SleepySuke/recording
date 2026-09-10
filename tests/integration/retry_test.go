package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"recording-transcription/internal/application/errorcode"
	"recording-transcription/internal/application/ports"
	domain "recording-transcription/internal/domain/recording"
	"recording-transcription/internal/infrastructure/asr/mock"
	"recording-transcription/internal/infrastructure/llm"
	"recording-transcription/internal/infrastructure/persistence/mysql"
)

// 测试依据：T08 步骤 1；设计依据：详设 §4.5（重试协议：锁内复查 failed、条件更新
// attempt+1 回 pending、清空上轮产物、同事务 task_retry_accepted、COMMIT 后 Notify）、
// §4.4（条件更新与旧轮次隔离）、§8.1（retry 端点 202/409/404）、§8.3（30001/30002）。
// 用例编号 IT-06 / IT-07 及重试扩展（TestRetry_OldAttemptIsolated / TestRetry_FullFlow）。

// retryResp POST /v1/tasks/:id/retry 202 响应体（详设 §8.1）。
type retryResp struct {
	TaskID  string `json:"task_id"`
	Status  string `json:"status"`
	Attempt int    `json:"attempt"`
}

// doRetryRaw 发起一次 retry 请求，返回原始 recorder（并发用例在子 goroutine 调用，
// 不得 Fatal）。
func doRetryRaw(r *gin.Engine, taskID string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/v1/tasks/"+taskID+"/retry", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	return rec
}

// insertTaskState 直插一对录音+任务及事件链（task_created seq=1 + 末事件 seq=2），
// failed/done 形态预置上轮产物残留（transcript/summary/错误/时间），供重试用例验证
// 「清空上轮产物与错误」；transcribing 不预置产物。deleting=true 时预置 deleting_at。
// n 区分多行。返回 (recordingID, taskID)。
func insertTaskState(t *testing.T, db *gorm.DB, n int, status string, deleting bool) (string, string) {
	t.Helper()
	recID := fmt.Sprintf("b0000000-0000-0000-0000-%012d", n)
	taskID := fmt.Sprintf("a0000000-0000-0000-0000-%012d", n)
	createdAt := time.Now().UTC().Add(-time.Duration(1000-n) * time.Second)

	var deletingAt any
	if deleting {
		deletingAt = createdAt.Add(time.Second)
	}
	// 终态预置上轮执行痕迹；在途不预置（详设 §4.4：产物与状态分别原子提交）。
	var transcript any = ""
	var summaryJSON any
	var errorCode any
	var errorMessage any = ""
	var startedAt, finishedAt any
	lastEvent, fromStatus, toStatus := "task_claimed", "pending", "transcribing"
	switch status {
	case "summarizing": // T09 删除用例预置（详设 §4.1 transcribing→summarizing 出边形态）
		transcript = fmt.Sprintf("旧轮正文-%d", n)
		startedAt = createdAt.Add(2 * time.Second)
		lastEvent, fromStatus, toStatus = "transcription_completed", "transcribing", "summarizing"
	case "failed":
		transcript = fmt.Sprintf("旧轮正文-%d", n)
		summaryJSON = fmt.Sprintf(`{"summary":"旧轮摘要-%d","key_points":[],"todos":[]}`, n)
		errorCode = 40001
		errorMessage = "转写失败"
		startedAt = createdAt.Add(2 * time.Second)
		finishedAt = createdAt.Add(3 * time.Second)
		lastEvent, fromStatus, toStatus = "task_failed", "transcribing", "failed"
	case "done":
		transcript = fmt.Sprintf("旧轮正文-%d", n)
		summaryJSON = fmt.Sprintf(`{"summary":"旧轮摘要-%d","key_points":[],"todos":[]}`, n)
		startedAt = createdAt.Add(2 * time.Second)
		finishedAt = createdAt.Add(3 * time.Second)
		lastEvent, fromStatus, toStatus = "task_completed", "summarizing", "done"
	}

	stmts := []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO recordings (id, original_filename, storage_path, extension, size_bytes, content_hash, deleting_at, created_at, updated_at)
		   VALUES (?, ?, ?, 'wav', 1024, ?, ?, ?, ?)`,
			[]any{recID, fmt.Sprintf("it-retry-%d.wav", n), fmt.Sprintf("it/retry/%d.wav", n), fmt.Sprintf("%064d", n), deletingAt, createdAt, createdAt}},
		{`INSERT INTO tasks (id, recording_id, status, attempt, event_seq, transcript, summary_json, error_code, error_message, created_request_id, created_at, updated_at, started_at, finished_at)
		   VALUES (?, ?, ?, 1, 2, ?, ?, ?, ?, 'req-it', ?, ?, ?, ?)`,
			[]any{taskID, recID, status, transcript, summaryJSON, errorCode, errorMessage, createdAt, createdAt, startedAt, finishedAt}},
		{`INSERT INTO task_events (event_id, task_id, recording_id, event_seq, attempt, event, occurred_at, level, to_status, created_request_id, instance_id)
		   VALUES (?, ?, ?, 1, 1, 'task_created', ?, 'INFO', 'pending', 'req-it', ?)`,
			[]any{fmt.Sprintf("c0000000-0000-0000-0000-%012d", n), taskID, recID, createdAt, itInstanceID}},
		{`INSERT INTO task_events (event_id, task_id, recording_id, event_seq, attempt, event, occurred_at, level, from_status, to_status, created_request_id, instance_id)
		   VALUES (?, ?, ?, 2, 1, ?, ?, 'INFO', ?, ?, 'req-it', ?)`,
			[]any{fmt.Sprintf("e0000000-0000-0000-0000-%012d", n), taskID, recID, lastEvent, createdAt.Add(time.Second), fromStatus, toStatus, itInstanceID}},
	}
	for _, s := range stmts {
		if err := db.Exec(s.sql, s.args...).Error; err != nil {
			t.Fatalf("预置数据失败: %v (sql=%s)", err, s.sql)
		}
	}
	return recID, taskID
}

// TestIT06_ConcurrentRetry（IT-06，详设 §4.5）：同一 failed 任务两个并发 retry →
// 恰好一个 202、另一个 409/30002；attempt 恰好 +1（=2）；事件恰好一条
// task_retry_accepted；上轮 transcript/summary/错误/本轮时间被清空；不新增任务。
// 先停 worker 池：重试成功后任务停留 pending，终态断言不因立即认领而抖动。
func TestIT06_ConcurrentRetry(t *testing.T) {
	h := NewPipelineHarness(t)
	h.Pool.Stop() // 幂等；Notify 变为无人消费的信号（channel 不关闭），无副作用
	_, taskID := insertTaskState(t, h.DB, 1, "failed", false)

	const callers = 2
	results := make([]*httptest.ResponseRecorder, callers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i] = doRetryRaw(h.Router, taskID)
		}(i)
	}
	close(start)
	wg.Wait()

	// 行锁串行化 + 锁内复查：恰一个 202、一个 409（详设 §4.5「retry vs retry」）。
	codes := []int{results[0].Code, results[1].Code}
	sort.Ints(codes)
	if codes[0] != http.StatusAccepted || codes[1] != http.StatusConflict {
		t.Fatalf("并发 retry 状态码 = %v, want [202 409]", codes)
	}
	var accepted retryResp
	for _, r := range results {
		if r.Code == http.StatusAccepted {
			if err := json.Unmarshal(r.Body.Bytes(), &accepted); err != nil {
				t.Fatalf("202 响应不是合法 JSON: %v, body=%q", err, r.Body.String())
			}
		} else {
			if e := decodeAPIError(t, r); e.Error.Code != int(errorcode.CodeTaskNotRetryable) {
				t.Errorf("409 业务码 = %d, want 30002, body=%q", e.Error.Code, r.Body.String())
			}
		}
	}
	// 202 响应体契约（详设 §8.1）：task_id 复用、status=pending、attempt=2。
	if accepted.TaskID != taskID || accepted.Status != "pending" || accepted.Attempt != 2 {
		t.Errorf("202 响应 = %+v, want {task_id 复用, pending, attempt=2}", accepted)
	}

	// DB 终态：恰好 +1 轮、回 pending、上轮产物与错误清空、单条 retry 事件。
	var task struct {
		Status       string
		Attempt      int
		Transcript   string
		SummaryJSON  *string
		ErrorCode    *int
		ErrorMessage string
		StartedAt    *time.Time
		FinishedAt   *time.Time
	}
	if err := h.DB.Raw(
		"SELECT status, attempt, transcript, summary_json, error_code, error_message, started_at, finished_at FROM tasks WHERE id = ?",
		taskID).Scan(&task).Error; err != nil {
		t.Fatalf("查询任务失败: %v", err)
	}
	if task.Status != "pending" || task.Attempt != 2 {
		t.Errorf("任务 = %s/attempt %d, want pending/2（attempt 恰好 +1）", task.Status, task.Attempt)
	}
	if task.Transcript != "" || task.SummaryJSON != nil || task.ErrorCode != nil || task.ErrorMessage != "" {
		t.Errorf("上轮产物未清空: transcript=%q summary=%v error=%v/%q",
			task.Transcript, task.SummaryJSON, task.ErrorCode, task.ErrorMessage)
	}
	if task.StartedAt != nil || task.FinishedAt != nil {
		t.Errorf("本轮时间字段未清空: started=%v finished=%v", task.StartedAt, task.FinishedAt)
	}
	var retryEvents int64
	if err := h.DB.Raw(
		"SELECT COUNT(*) FROM task_events WHERE task_id = ? AND event = 'task_retry_accepted'",
		taskID).Scan(&retryEvents).Error; err != nil {
		t.Fatalf("查询事件失败: %v", err)
	}
	if retryEvents != 1 {
		t.Errorf("task_retry_accepted 事件 = %d 条, want 1", retryEvents)
	}
	if n := tableCount(t, h.DB, "tasks"); n != 1 {
		t.Errorf("tasks 行数 = %d, want 1（重复请求不新增任务，task_id 复用）", n)
	}
}

// TestIT07_RetryPreconditions（IT-07，详设 §8.3）：done / transcribing（在途）任务
// retry → 409/30002；不存在任务 → 404/30001；录音删除中的 failed 任务 → 404/30001
// （删除中不可见）；非法 task_id → 400/10001。失败请求不留痕迹（无新事件、不改状态）。
func TestIT07_RetryPreconditions(t *testing.T) {
	h := NewPipelineHarness(t)
	h.Pool.Stop()

	_, doneID := insertTaskState(t, h.DB, 1, "done", false)
	_, onflightID := insertTaskState(t, h.DB, 2, "transcribing", false)
	_, deletingID := insertTaskState(t, h.DB, 3, "failed", true)

	cases := []struct {
		name     string
		taskID   string
		raw      string // 路径参数原文（默认同 taskID）
		wantHTTP int
		wantCode int
	}{
		{"done任务409", doneID, "", http.StatusConflict, int(errorcode.CodeTaskNotRetryable)},
		{"在途任务409", onflightID, "", http.StatusConflict, int(errorcode.CodeTaskNotRetryable)},
		{"不存在任务404", "0f0f0f0f-0f0f-0f0f-0f0f-0f0f0f0f0f0f", "", http.StatusNotFound, int(errorcode.CodeTaskNotFound)},
		{"删除中404", deletingID, "", http.StatusNotFound, int(errorcode.CodeTaskNotFound)},
		{"非法task_id400", "", "not-an-uuid", http.StatusBadRequest, int(errorcode.CodeInvalidArgument)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			id := tc.taskID
			if tc.raw != "" {
				id = tc.raw
			}
			rec := doRetryRaw(h.Router, id)
			if rec.Code != tc.wantHTTP {
				t.Fatalf("status = %d, want %d, body=%q", rec.Code, tc.wantHTTP, rec.Body.String())
			}
			if e := decodeAPIError(t, rec); e.Error.Code != tc.wantCode {
				t.Errorf("业务码 = %d, want %d, body=%q", e.Error.Code, tc.wantCode, rec.Body.String())
			}
		})
	}

	// 前置校验失败不留任何痕迹：无新增事件、done 任务未被推进。
	if n := tableCount(t, h.DB, "task_events"); n != 6 { // 三组种子各 2 条
		t.Errorf("事件总数 = %d, want 6（前置校验失败不写事件）", n)
	}
	var doneStatus string
	if err := h.DB.Raw("SELECT status FROM tasks WHERE id = ?", doneID).Scan(&doneStatus).Error; err != nil {
		t.Fatalf("查询失败: %v", err)
	}
	if doneStatus != "done" {
		t.Errorf("done 任务被改写为 %q", doneStatus)
	}
}

// TestRetry_OldAttemptIsolated（IT-05 模式扩展到重试轮次，详设 §4.4/§4.5）：
// 首轮 attempt=1 失败落库 → RetryTask 触发新一轮（attempt=2）→ 持旧 attempt 的迟到
// 阶段写入（④/③/⑤）被条件更新拒绝（ErrStaleExecution），新一轮状态与产物不被
// 旧轮次覆盖（验收「旧 attempt 无法覆盖新 attempt 的状态和产物」）。
func TestRetry_OldAttemptIsolated(t *testing.T) {
	db, ptx := newClaimEnv(t)
	_, taskID := insertPendingTask(t, db, 1, false)
	exec, ok, err := ptx.ClaimNext(context.Background())
	if err != nil || !ok {
		t.Fatalf("认领失败: ok=%v err=%v", ok, err)
	}
	// 首轮转写失败 → failed/attempt=1（详设 §4.1）。
	if err := ptx.FailTask(context.Background(), exec.ExecutionKey, errorcode.CodeASRFailed, "转写失败"); err != nil {
		t.Fatalf("落 failed 失败: %v", err)
	}

	// 手动重试（端口层直调）→ 新一轮 attempt=2 回 pending。
	recTx := mysql.NewRecordingTx(db, itInstanceID, slog.New(slog.NewTextHandler(io.Discard, nil)))
	newAttempt, err := recTx.RetryTask(context.Background(), taskID)
	if err != nil {
		t.Fatalf("RetryTask 失败: %v", err)
	}
	if newAttempt != 2 {
		t.Fatalf("RetryTask 返回 attempt = %d, want 2", newAttempt)
	}

	// 旧 attempt（=1）的迟到写入：④保存转写 / ③落失败 / ⑤完成，全部被拒。
	oldKey := exec.ExecutionKey
	if err := ptx.SaveTranscription(context.Background(), oldKey, "旧轮迟到正文"); !errors.Is(err, ports.ErrStaleExecution) {
		t.Errorf("事务④旧轮写入错误 = %v, want ErrStaleExecution", err)
	}
	if err := ptx.FailTask(context.Background(), oldKey, errorcode.CodeASRFailed, "旧轮迟到失败"); !errors.Is(err, ports.ErrStaleExecution) {
		t.Errorf("事务③旧轮写入错误 = %v, want ErrStaleExecution", err)
	}
	if err := ptx.CompleteTask(context.Background(), oldKey, llm.FakeNormalSummary); !errors.Is(err, ports.ErrStaleExecution) {
		t.Errorf("事务⑤旧轮写入错误 = %v, want ErrStaleExecution", err)
	}

	// 新一轮状态不被旧轮次覆盖：pending / attempt=2 / 无产物无错误。
	var task struct {
		Status      string
		Attempt     int
		Transcript  string
		SummaryJSON *string
		ErrorCode   *int
		EventSeq    int64
	}
	if err := db.Raw(
		"SELECT status, attempt, transcript, summary_json, error_code, event_seq FROM tasks WHERE id = ?",
		taskID).Scan(&task).Error; err != nil {
		t.Fatalf("查询任务失败: %v", err)
	}
	if task.Status != "pending" || task.Attempt != 2 || task.Transcript != "" ||
		task.SummaryJSON != nil || task.ErrorCode != nil || task.EventSeq != 4 {
		t.Errorf("旧轮写入未全部隔离: %+v, want pending/attempt=2/无产物/seq=4（retry_accepted 占 seq=4）", task)
	}
	// 事件链：created → claimed → failed → retry_accepted，共 4 条，无旧轮新增。
	if n := tableCount(t, db, "task_events"); n != 4 {
		t.Errorf("事件数 = %d, want 4（created+claimed+failed+retry_accepted）", n)
	}
}

// TestRetry_FullFlow（T08 步骤 1 / 验收「失败任务 retry 后从头执行成功」）：
// FailFirst 替身首轮转写失败（40001）→ retry 202（attempt=2）→ 新一轮从头执行成功
// → done 且 attempt=2；事件链完整含 task_failed + task_retry_accepted + 新一轮
// task_claimed（详设 §4.5/§7.1 失败/重试链路）。
func TestRetry_FullFlow(t *testing.T) {
	h := NewPipelineHarness(t, WithFailFirstASR())

	w := doUpload(t, h.Router, formPart{field: "file", filename: "retry-meeting.wav", content: contentOf(2048)})
	if w.Code != 202 {
		t.Fatalf("上传 status = %d, want 202, body=%q", w.Code, w.Body.String())
	}
	resp := decodeUpload(t, w)

	// 首轮：转写失败 40001（详设 §4.1 transcribing→failed）。
	waitForTaskStatus(t, h.DB, resp.TaskID, "failed")
	var failed struct {
		Attempt     int
		ErrorCode   *int
		Transcript  string
		SummaryJSON *string
	}
	if err := h.DB.Raw("SELECT attempt, error_code, transcript, summary_json FROM tasks WHERE id = ?",
		resp.TaskID).Scan(&failed).Error; err != nil {
		t.Fatalf("查询任务失败: %v", err)
	}
	if failed.Attempt != 1 || failed.ErrorCode == nil || *failed.ErrorCode != int(errorcode.CodeASRFailed) {
		t.Fatalf("首轮失败形态 = %+v, want attempt=1 error_code=40001", failed)
	}
	if failed.Transcript != "" || failed.SummaryJSON != nil {
		t.Errorf("转写失败不应有产物: %+v", failed)
	}

	// 手动重试：202 {task_id 复用, status=pending, attempt=2}（详设 §8.1）。
	rec := doRetryRaw(h.Router, resp.TaskID)
	if rec.Code != 202 {
		t.Fatalf("retry status = %d, want 202, body=%q", rec.Code, rec.Body.String())
	}
	var body retryResp
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("202 响应不是合法 JSON: %v, body=%q", err, rec.Body.String())
	}
	if body.TaskID != resp.TaskID || body.Status != "pending" || body.Attempt != 2 {
		t.Fatalf("202 响应 = %+v, want {task_id 复用, pending, attempt=2}", body)
	}

	// 新一轮从头执行成功：done、attempt=2、种子正文、摘要内容、上轮错误清空。
	waitForTaskStatus(t, h.DB, resp.TaskID, "done")
	var task struct {
		Attempt     int
		EventSeq    int64
		Transcript  string
		SummaryJSON *string
		ErrorCode   *int
	}
	if err := h.DB.Raw(
		"SELECT attempt, event_seq, transcript, summary_json, error_code FROM tasks WHERE id = ?",
		resp.TaskID).Scan(&task).Error; err != nil {
		t.Fatalf("查询任务失败: %v", err)
	}
	if task.Attempt != 2 || task.EventSeq != 7 {
		t.Errorf("终态 = attempt %d/seq %d, want 2/7", task.Attempt, task.EventSeq)
	}
	if want := mock.SeedText(resp.TaskID); task.Transcript != want {
		t.Errorf("transcript = %q, want 种子文本 %q", task.Transcript, want)
	}
	if task.SummaryJSON == nil {
		t.Fatal("done 任务缺 summary_json")
	}
	sum, err := domain.ParseSummary([]byte(*task.SummaryJSON))
	if err != nil {
		t.Fatalf("summary_json 不可解析: %v", err)
	}
	if sum.Summary != llm.FakeNormalSummary.Summary || len(sum.KeyPoints) != len(llm.FakeNormalSummary.KeyPoints) {
		t.Errorf("summary = %+v, want %+v", sum, llm.FakeNormalSummary)
	}
	if task.ErrorCode != nil {
		t.Errorf("重试成功后 error_code = %d, want NULL（上轮错误清空）", *task.ErrorCode)
	}

	// 事件链（详设 §7.1 失败/重试）：created → claimed → failed → retry_accepted
	// → claimed → transcription_completed → completed，跨轮次 event_seq 连续。
	type evtRow struct {
		Event      string
		EventSeq   int64
		Attempt    int
		FromStatus *string
		ToStatus   string
		ErrorCode  *int
	}
	var evts []evtRow
	if err := h.DB.Raw(
		"SELECT event, event_seq, attempt, from_status, to_status, error_code FROM task_events WHERE task_id = ? ORDER BY event_seq",
		resp.TaskID).Scan(&evts).Error; err != nil {
		t.Fatalf("查询事件链失败: %v", err)
	}
	if len(evts) != 7 {
		t.Fatalf("事件链长度 = %d, want 7: %+v", len(evts), evts)
	}
	code40001 := int(errorcode.CodeASRFailed)
	want := []evtRow{
		{Event: "task_created", EventSeq: 1, Attempt: 1, ToStatus: "pending"},
		{Event: "task_claimed", EventSeq: 2, Attempt: 1, FromStatus: ptrStr("pending"), ToStatus: "transcribing"},
		{Event: "task_failed", EventSeq: 3, Attempt: 1, FromStatus: ptrStr("transcribing"), ToStatus: "failed", ErrorCode: &code40001},
		{Event: "task_retry_accepted", EventSeq: 4, Attempt: 2, FromStatus: ptrStr("failed"), ToStatus: "pending"},
		{Event: "task_claimed", EventSeq: 5, Attempt: 2, FromStatus: ptrStr("pending"), ToStatus: "transcribing"},
		{Event: "transcription_completed", EventSeq: 6, Attempt: 2, FromStatus: ptrStr("transcribing"), ToStatus: "summarizing"},
		{Event: "task_completed", EventSeq: 7, Attempt: 2, FromStatus: ptrStr("summarizing"), ToStatus: "done"},
	}
	for i, got := range evts {
		w := want[i]
		if got.Event != w.Event || got.EventSeq != w.EventSeq || got.Attempt != w.Attempt || got.ToStatus != w.ToStatus ||
			((got.FromStatus == nil) != (w.FromStatus == nil)) || (w.FromStatus != nil && *got.FromStatus != *w.FromStatus) ||
			((got.ErrorCode == nil) != (w.ErrorCode == nil)) || (w.ErrorCode != nil && *got.ErrorCode != *w.ErrorCode) {
			t.Errorf("事件[%d] = %+v, want %+v", i, got, w)
		}
	}
}

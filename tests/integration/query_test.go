package integration

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"recording-transcription/internal/infrastructure/persistence/mysql"
)

// 测试依据：T05 步骤 1；设计依据：详设 §8.1（端点与分页约定）、§8.3（404/400/90004）、
// §4.6（查询不用 INNER JOIN 静默隐藏损坏关联）；架构 §3（查询只读数据库）。
// 用例编号 IT-16 与三个补充用例（NotFound / DetailFields / MissingTaskIsolation）。

// ---- 响应解码 ----

type qTaskError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type qTaskBody struct {
	ID          string      `json:"id"`
	RecordingID string      `json:"recording_id"`
	Status      string      `json:"status"`
	Attempt     int         `json:"attempt"`
	Error       *qTaskError `json:"error"`
	CreatedAt   string      `json:"created_at"`
	StartedAt   *string     `json:"started_at"`
	FinishedAt  *string     `json:"finished_at"`
}

type qResultBody struct {
	Summary   string   `json:"summary"`
	KeyPoints []string `json:"key_points"`
	Todos     []string `json:"todos"`
}

type qDetailBody struct {
	ID               string       `json:"id"`
	OriginalFilename string       `json:"original_filename"`
	Extension        string       `json:"extension"`
	SizeBytes        int64        `json:"size_bytes"`
	CreatedAt        string       `json:"created_at"`
	Task             qTaskBody    `json:"task"`
	Transcript       *string      `json:"transcript"`
	Result           *qResultBody `json:"result"`
}

type qListItem struct {
	ID               string `json:"id"`
	OriginalFilename string `json:"original_filename"`
	SizeBytes        int64  `json:"size_bytes"`
	CreatedAt        string `json:"created_at"`
	TaskID           string `json:"task_id"`
	Status           string `json:"status"`
}

type qListBody struct {
	Items    []qListItem `json:"items"`
	Page     int         `json:"page"`
	PageSize int         `json:"page_size"`
	Total    int         `json:"total"`
}

// ---- 测试辅助 ----

func doGet(t *testing.T, r *gin.Engine, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

func decodeInto(t *testing.T, w *httptest.ResponseRecorder, v any) {
	t.Helper()
	if err := json.Unmarshal(w.Body.Bytes(), v); err != nil {
		t.Fatalf("响应不是合法 JSON: %v, body=%q", err, w.Body.String())
	}
}

// requireError 断言 HTTP 状态与业务码（详设 §8.2/§8.3）。
func requireError(t *testing.T, w *httptest.ResponseRecorder, wantStatus, wantCode int) {
	t.Helper()
	if w.Code != wantStatus {
		t.Fatalf("status = %d, want %d, body=%q", w.Code, wantStatus, w.Body.String())
	}
	if body := decodeAPIError(t, w); body.Error.Code != wantCode {
		t.Errorf("code = %d, want %d", body.Error.Code, wantCode)
	}
}

// requireRFC3339 断言时间字符串可按 RFC3339 解析（详设 §8.1：时间输出 RFC3339）。
func requireRFC3339(t *testing.T, field, s string) {
	t.Helper()
	if _, err := time.Parse(time.RFC3339, s); err != nil {
		t.Errorf("%s = %q 不是 RFC3339: %v", field, s, err)
	}
}

// seedID 生成确定性的合法 UUID 形态测试 ID（000…00N），保证 id 倒序可比。
func seedID(n int) string {
	return fmt.Sprintf("00000000-0000-4000-8000-%012d", n)
}

// seedOpts 直接落库构造 recordings（可选 tasks）行：绕过上传链以制造流水线
// 尚不能产生的状态（done/failed/deleting 等，T05 只读验证）。
type seedOpts struct {
	recordingID  string
	taskID       string // 空 = 只插录音（缺关联用例）
	createdAt    time.Time
	deletingAt   *time.Time
	filename     string
	status       string
	attempt      int
	transcript   string
	summaryJSON  *string
	errorCode    *int
	errorMessage string
	startedAt    *time.Time
	finishedAt   *time.Time
}

func seedPair(t *testing.T, db *gorm.DB, o seedOpts) {
	t.Helper()
	if o.filename == "" {
		o.filename = "seed.wav"
	}
	if o.status == "" {
		o.status = "pending"
	}
	if o.attempt == 0 {
		o.attempt = 1
	}
	rec := mysql.RecordingPO{
		ID:               o.recordingID,
		OriginalFilename: o.filename,
		StoragePath:      "seed/" + o.recordingID + ".wav",
		Extension:        "wav",
		SizeBytes:        4096,
		ContentHash:      strings.Repeat("ab", 32),
		DeletingAt:       o.deletingAt,
		CreatedAt:        o.createdAt,
		UpdatedAt:        o.createdAt,
	}
	if err := db.Create(&rec).Error; err != nil {
		t.Fatalf("插入 recordings 失败: %v", err)
	}
	if o.taskID == "" {
		return
	}
	tk := mysql.TaskPO{
		ID:               o.taskID,
		RecordingID:      o.recordingID,
		Status:           o.status,
		Attempt:          o.attempt,
		EventSeq:         1,
		Transcript:       o.transcript,
		SummaryJSON:      o.summaryJSON,
		ErrorCode:        o.errorCode,
		ErrorMessage:     o.errorMessage,
		CreatedRequestID: "seed",
		CreatedAt:        o.createdAt,
		UpdatedAt:        o.createdAt,
		StartedAt:        o.startedAt,
		FinishedAt:       o.finishedAt,
	}
	if err := db.Create(&tk).Error; err != nil {
		t.Fatalf("插入 tasks 失败: %v", err)
	}
}

// seedPage25 预置 25 对录音+任务，created_at 递增 1 秒（i 越大越新）。
func seedPage25(t *testing.T, db *gorm.DB) {
	t.Helper()
	base := time.Date(2026, 9, 9, 8, 0, 0, 0, time.UTC)
	for i := 1; i <= 25; i++ {
		seedPair(t, db, seedOpts{
			recordingID: seedID(i),
			taskID:      seedID(1000 + i),
			createdAt:   base.Add(time.Duration(i) * time.Second),
			filename:    fmt.Sprintf("it16-%02d.wav", i),
		})
	}
}

// TestIT16_Pagination：分页行为（IT-16 / 详设 §8.1）——默认 20、上限 100、非法值 400、
// 超总页数空列表、同 created_at 并列按 id 倒序稳定。
func TestIT16_Pagination(t *testing.T) {
	t.Run("默认page1_size20", func(t *testing.T) {
		r, db, _, _ := newUploadEnv(t)
		seedPage25(t, db)

		w := doGet(t, r, "/v1/recordings")
		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200, body=%q", w.Code, w.Body.String())
		}
		var list qListBody
		decodeInto(t, w, &list)
		if list.Page != 1 || list.PageSize != 20 || list.Total != 25 {
			t.Errorf("page/page_size/total = %d/%d/%d, want 1/20/25", list.Page, list.PageSize, list.Total)
		}
		if len(list.Items) != 20 {
			t.Fatalf("len(items) = %d, want 20", len(list.Items))
		}
		// 最新在前（created_at DESC）。
		if list.Items[0].ID != seedID(25) || list.Items[0].TaskID != seedID(1025) {
			t.Errorf("items[0] = %s/%s, want %s/%s", list.Items[0].ID, list.Items[0].TaskID, seedID(25), seedID(1025))
		}
		if list.Items[0].Status != "pending" || list.Items[0].OriginalFilename != "it16-25.wav" {
			t.Errorf("items[0] 状态/文件名不符: %+v", list.Items[0])
		}
		requireRFC3339(t, "items[0].created_at", list.Items[0].CreatedAt)

		// 第二页余 5 条，仍按时间倒序。
		w2 := doGet(t, r, "/v1/recordings?page=2&page_size=20")
		if w2.Code != http.StatusOK {
			t.Fatalf("page=2 status = %d, want 200", w2.Code)
		}
		var list2 qListBody
		decodeInto(t, w2, &list2)
		if len(list2.Items) != 5 || list2.Total != 25 || list2.Page != 2 {
			t.Fatalf("page=2: len=%d total=%d page=%d, want 5/25/2", len(list2.Items), list2.Total, list2.Page)
		}
		if list2.Items[0].ID != seedID(5) {
			t.Errorf("page=2 items[0].ID = %s, want %s", list2.Items[0].ID, seedID(5))
		}
	})

	t.Run("page_size上限100", func(t *testing.T) {
		r, db, _, _ := newUploadEnv(t)
		seedPage25(t, db)

		w := doGet(t, r, "/v1/recordings?page_size=100")
		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200, body=%q", w.Code, w.Body.String())
		}
		var list qListBody
		decodeInto(t, w, &list)
		if list.PageSize != 100 || len(list.Items) != 25 {
			t.Errorf("page_size=100: size=%d len=%d, want 100/25", list.PageSize, len(list.Items))
		}

		// 恰超上限 1 → 400/10001。
		requireError(t, doGet(t, r, "/v1/recordings?page_size=101"), http.StatusBadRequest, 10001)
	})

	t.Run("非法值400_10001", func(t *testing.T) {
		r, db, _, _ := newUploadEnv(t)
		seedPage25(t, db)

		for _, qs := range []string{
			"?page=0", "?page=-1", "?page=abc", "?page=1.5",
			"?page_size=0", "?page_size=-5", "?page_size=abc", "?page_size=1000000",
		} {
			requireError(t, doGet(t, r, "/v1/recordings"+qs), http.StatusBadRequest, 10001)
		}
	})

	t.Run("超总页数返回空数组", func(t *testing.T) {
		r, db, _, _ := newUploadEnv(t)
		seedPage25(t, db)

		w := doGet(t, r, "/v1/recordings?page=99&page_size=20")
		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200, body=%q", w.Code, w.Body.String())
		}
		var list qListBody
		decodeInto(t, w, &list)
		if list.Total != 25 {
			t.Errorf("total = %d, want 25", list.Total)
		}
		if list.Items == nil || len(list.Items) != 0 {
			t.Errorf("items = %#v, want 空数组（非 null）", list.Items)
		}
		if !strings.Contains(w.Body.String(), `"items":[]`) {
			t.Errorf("响应应含 \"items\":[]，body=%q", w.Body.String())
		}
	})

	t.Run("同created_at按id倒序稳定", func(t *testing.T) {
		r, db, _, _ := newUploadEnv(t)
		same := time.Date(2026, 9, 9, 9, 0, 0, 0, time.UTC)
		seedPair(t, db, seedOpts{recordingID: seedID(1001), taskID: seedID(2001), createdAt: same})
		seedPair(t, db, seedOpts{recordingID: seedID(1002), taskID: seedID(2002), createdAt: same})

		w := doGet(t, r, "/v1/recordings")
		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", w.Code)
		}
		var list qListBody
		decodeInto(t, w, &list)
		if len(list.Items) != 2 {
			t.Fatalf("len(items) = %d, want 2", len(list.Items))
		}
		if list.Items[0].ID != seedID(1002) || list.Items[1].ID != seedID(1001) {
			t.Errorf("并列排序 = [%s, %s], want id 倒序 [%s, %s]",
				list.Items[0].ID, list.Items[1].ID, seedID(1002), seedID(1001))
		}
	})
}

// TestQuery_NotFound：不存在与删除中资源的 404 语义（详设 §8.3：任务 30001、录音 20005、
// 删除中不可见；UUID 非法属参数错误 400/10001）。
func TestQuery_NotFound(t *testing.T) {
	t.Run("任务不存在404_30001", func(t *testing.T) {
		r, _, _, _ := newUploadEnv(t)
		requireError(t, doGet(t, r, "/v1/tasks/"+seedID(999)), http.StatusNotFound, 30001)
	})

	t.Run("录音不存在404_20005", func(t *testing.T) {
		r, _, _, _ := newUploadEnv(t)
		requireError(t, doGet(t, r, "/v1/recordings/"+seedID(999)), http.StatusNotFound, 20005)
	})

	t.Run("删除中资源同样404", func(t *testing.T) {
		r, db, _, _ := newUploadEnv(t)
		now := time.Now().UTC()
		seedPair(t, db, seedOpts{
			recordingID: seedID(1),
			taskID:      seedID(2),
			createdAt:   now,
			deletingAt:  &now,
		})

		requireError(t, doGet(t, r, "/v1/recordings/"+seedID(1)), http.StatusNotFound, 20005)
		requireError(t, doGet(t, r, "/v1/tasks/"+seedID(2)), http.StatusNotFound, 30001)

		// 删除中对列表同样不可见。
		w := doGet(t, r, "/v1/recordings")
		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", w.Code)
		}
		var list qListBody
		decodeInto(t, w, &list)
		if list.Total != 0 || len(list.Items) != 0 {
			t.Errorf("删除中资源应从列表隐藏: total=%d len=%d", list.Total, len(list.Items))
		}
	})

	t.Run("非法UUID路径参数400_10001", func(t *testing.T) {
		r, _, _, _ := newUploadEnv(t)
		requireError(t, doGet(t, r, "/v1/tasks/not-a-uuid"), http.StatusBadRequest, 10001)
		requireError(t, doGet(t, r, "/v1/recordings/zzz"), http.StatusBadRequest, 10001)
	})
}

// TestQuery_DetailFields：详情与任务查询的字段契约（详设 §8.1）——pending 产物为 null、
// done 后 transcript 与 result 三字段非 null、failed 仍 200 且 error 为落库值、
// 处理中状态直接体现阶段（完成标准第 2 条）。
func TestQuery_DetailFields(t *testing.T) {
	t.Run("pending详情transcript与result为null", func(t *testing.T) {
		r, db, _, _ := newUploadEnv(t)
		seedPair(t, db, seedOpts{recordingID: seedID(1), taskID: seedID(2), createdAt: time.Now().UTC()})

		w := doGet(t, r, "/v1/recordings/"+seedID(1))
		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200, body=%q", w.Code, w.Body.String())
		}
		var d qDetailBody
		decodeInto(t, w, &d)
		if d.ID != seedID(1) || d.OriginalFilename != "seed.wav" || d.Extension != "wav" || d.SizeBytes != 4096 {
			t.Errorf("录音元数据不符: %+v", d)
		}
		if d.Task.ID != seedID(2) || d.Task.RecordingID != seedID(1) || d.Task.Status != "pending" || d.Task.Attempt != 1 {
			t.Errorf("task 字段不符: %+v", d.Task)
		}
		if d.Task.Error != nil || d.Transcript != nil || d.Result != nil {
			t.Errorf("pending 详情 error/transcript/result 应为 null: %+v", d)
		}
		if d.Task.StartedAt != nil || d.Task.FinishedAt != nil {
			t.Errorf("pending 详情 started/finished 应为 null: %+v", d.Task)
		}
		requireRFC3339(t, "created_at", d.CreatedAt)
		requireRFC3339(t, "task.created_at", d.Task.CreatedAt)

		// 任务接口同样 error 为 null。
		w2 := doGet(t, r, "/v1/tasks/"+seedID(2))
		if w2.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", w2.Code)
		}
		var tv qTaskBody
		decodeInto(t, w2, &tv)
		if tv.Status != "pending" || tv.Error != nil {
			t.Errorf("pending 任务查询: status=%q error=%v, want pending/null", tv.Status, tv.Error)
		}
	})

	t.Run("处理中状态体现阶段", func(t *testing.T) {
		r, db, _, _ := newUploadEnv(t)
		now := time.Now().UTC()
		seedPair(t, db, seedOpts{
			recordingID: seedID(1), taskID: seedID(2), createdAt: now,
			status: "transcribing", startedAt: &now,
		})
		seedPair(t, db, seedOpts{
			recordingID: seedID(3), taskID: seedID(4), createdAt: now,
			status: "summarizing", transcript: "转写中已有文本", startedAt: &now,
		})

		var tv qTaskBody
		decodeInto(t, doGet(t, r, "/v1/tasks/"+seedID(2)), &tv)
		if tv.Status != "transcribing" || tv.Error != nil || tv.StartedAt == nil {
			t.Errorf("transcribing 任务查询不符: %+v", tv)
		}

		decodeInto(t, doGet(t, r, "/v1/tasks/"+seedID(4)), &tv)
		if tv.Status != "summarizing" {
			t.Errorf("summarizing 任务查询 status = %q", tv.Status)
		}
		var d qDetailBody
		decodeInto(t, doGet(t, r, "/v1/recordings/"+seedID(3)), &d)
		if d.Transcript == nil || *d.Transcript != "转写中已有文本" {
			t.Errorf("summarizing 详情 transcript 应已可用: %+v", d.Transcript)
		}
		if d.Result != nil {
			t.Errorf("summarizing 详情 result 应为 null: %+v", d.Result)
		}
	})

	t.Run("done后transcript与result非null", func(t *testing.T) {
		r, db, _, _ := newUploadEnv(t)
		now := time.Now().UTC()
		summary := `{"summary":"这是会议摘要","key_points":["要点A","要点B"],"todos":["待办一"]}`
		seedPair(t, db, seedOpts{
			recordingID: seedID(1), taskID: seedID(2), createdAt: now,
			status: "done", transcript: "完整转写文本", summaryJSON: &summary,
			startedAt: &now, finishedAt: &now,
		})

		w := doGet(t, r, "/v1/recordings/"+seedID(1))
		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200, body=%q", w.Code, w.Body.String())
		}
		var d qDetailBody
		decodeInto(t, w, &d)
		if d.Transcript == nil || *d.Transcript != "完整转写文本" {
			t.Errorf("done 详情 transcript = %v, want 完整转写文本", d.Transcript)
		}
		if d.Result == nil {
			t.Fatalf("done 详情 result 应非 null: %q", w.Body.String())
		}
		if d.Result.Summary != "这是会议摘要" || len(d.Result.KeyPoints) != 2 || d.Result.KeyPoints[0] != "要点A" ||
			len(d.Result.Todos) != 1 || d.Result.Todos[0] != "待办一" {
			t.Errorf("result 三字段不符: %+v", d.Result)
		}
		if d.Task.Status != "done" || d.Task.Error != nil || d.Task.FinishedAt == nil {
			t.Errorf("done task 字段不符: %+v", d.Task)
		}
	})

	t.Run("failed任务GET仍200且error为落库值", func(t *testing.T) {
		r, db, _, _ := newUploadEnv(t)
		now := time.Now().UTC()
		code := 50001
		seedPair(t, db, seedOpts{
			recordingID: seedID(1), taskID: seedID(2), createdAt: now,
			status: "failed", errorCode: &code, errorMessage: "摘要生成超时",
			startedAt: &now, finishedAt: &now,
		})

		w := doGet(t, r, "/v1/tasks/"+seedID(2))
		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200（failed 查询本身成功）, body=%q", w.Code, w.Body.String())
		}
		var tv qTaskBody
		decodeInto(t, w, &tv)
		if tv.Status != "failed" {
			t.Errorf("status = %q, want failed", tv.Status)
		}
		if tv.Error == nil || tv.Error.Code != 50001 || tv.Error.Message != "摘要生成超时" {
			t.Errorf("failed error = %+v, want code=50001/摘要生成超时", tv.Error)
		}

		var d qDetailBody
		decodeInto(t, doGet(t, r, "/v1/recordings/"+seedID(1)), &d)
		if d.Task.Error == nil || d.Task.Error.Code != 50001 {
			t.Errorf("详情 task.error = %+v, want code=50001", d.Task.Error)
		}
		if d.Result != nil {
			t.Errorf("failed 详情 result 应为 null: %+v", d.Result)
		}
	})
}

// TestQuery_MissingTaskIsolation：正常录音缺任务 → 500/90004，不用 JOIN 静默隐藏（详设 §4.6/§8.3）。
func TestQuery_MissingTaskIsolation(t *testing.T) {
	r, db, _, _ := newUploadEnv(t)
	seedPair(t, db, seedOpts{recordingID: seedID(1), createdAt: time.Now().UTC()}) // 只录音，无任务

	requireError(t, doGet(t, r, "/v1/recordings"), http.StatusInternalServerError, 90004)
	requireError(t, doGet(t, r, "/v1/recordings/"+seedID(1)), http.StatusInternalServerError, 90004)
}

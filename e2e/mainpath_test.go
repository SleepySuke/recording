// E2E-01 成功主链路（测试设计 §5.2 E2E-01，T07 扩展验收至 done，golden = §5.1 形状）：
// 上传 → 轮询至 done → 详情（result 三字段）→ DB 事件链/summary_json → 落盘文件 →
// 日志镜像 → 三表行数，采集进 actual 后与 expected/E01.json 深度比对。
// LLM 段 = 真实适配器 + 进程内 FakeLLM（normal，见 harness）。
package e2e

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// resultBody 详情响应的 result 字段（仅 done 非 null，详设 §8.1）。
type resultBody struct {
	Summary   string   `json:"summary"`
	KeyPoints []string `json:"key_points"`
	Todos     []string `json:"todos"`
}

// detailBody GET /v1/recordings/:id 响应体（与 handler 实际返回一致，嵌套 task 对象）。
type detailBody struct {
	ID               string `json:"id"`
	OriginalFilename string `json:"original_filename"`
	Extension        string `json:"extension"`
	SizeBytes        int64  `json:"size_bytes"`
	CreatedAt        string `json:"created_at"`
	Task             struct {
		ID      string `json:"id"`
		Status  string `json:"status"`
		Attempt int    `json:"attempt"`
	} `json:"task"`
	Transcript *string     `json:"transcript"`
	Result     *resultBody `json:"result"`
}

// e01Actual E01.json 的采集形状（§5.1：HTTP 响应、事件序列、终态产物、DB 终态、
// 文件存在性、日志镜像序列；标识/时间戳经 runGolden 掩码归一化）。
type e01Actual struct {
	Upload struct {
		HTTPStatus  int    `json:"http_status"`
		RecordingID string `json:"recording_id"`
		TaskID      string `json:"task_id"`
		Status      string `json:"status"`
	} `json:"upload"`
	RecordingDetail struct {
		HTTPStatus       int    `json:"http_status"`
		ID               string `json:"id"`
		OriginalFilename string `json:"original_filename"`
		Extension        string `json:"extension"`
		SizeBytes        int64  `json:"size_bytes"`
		CreatedAt        string `json:"created_at"`
		Task             struct {
			ID      string `json:"id"`
			Status  string `json:"status"`
			Attempt int    `json:"attempt"`
		} `json:"task"`
		Transcript string      `json:"transcript"`
		Result     *resultBody `json:"result"`
	} `json:"recording_detail"`
	TaskFinal struct {
		Status      string      `json:"status"`
		Attempt     int         `json:"attempt"`
		EventSeq    int64       `json:"event_seq"`
		Transcript  string      `json:"transcript"`
		SummaryJSON *resultBody `json:"summary_json"`
	} `json:"task_final"`
	// 状态序列 = 事件链推导的完整序列（轮询序列可能漏态，仅用于不回退检查，不进 golden）。
	StatusSequence []string `json:"status_sequence"`
	Events         []evtRow `json:"events"`
	StorageFile    struct {
		Exists    bool  `json:"exists"`
		SizeBytes int64 `json:"size_bytes"`
	} `json:"storage_file"`
	MirrorEvents []string         `json:"mirror_events"`
	TableCounts  map[string]int64 `json:"table_counts"`
}

// TestE2E01_MainPath：100ms 确定性转写延迟（给轮询机会观察到 transcribing，不强制）。
func TestE2E01_MainPath(t *testing.T) {
	h := newE2E(t, 100*time.Millisecond)

	content := contentOf(8 * 1024)
	resp, upCode, err := h.postUpload("e2e01-meeting.wav", content)
	if err != nil {
		t.Fatalf("上传失败: %v", err)
	}
	if resp.TaskID == "" || resp.RecordingID == "" {
		t.Fatalf("上传响应缺 recording_id/task_id: %+v (status=%d)", resp, upCode)
	}

	// 轮询至 done（期限 10s，含 LLM 段）；轮询可能漏掉中间态，「不回退」是唯一强断言，
	// 完整状态序列以事件链还原（进 golden）。
	h.pollTask(resp.TaskID, "done", 10*time.Second)

	var a e01Actual
	a.Upload.HTTPStatus = upCode
	a.Upload.RecordingID = resp.RecordingID
	a.Upload.TaskID = resp.TaskID
	a.Upload.Status = resp.Status

	// 详情响应采集（真实值；ID/时间戳由 runGolden 掩码）。
	dCode, dRaw := h.doGet("/v1/recordings/" + resp.RecordingID)
	var d detailBody
	if dCode != http.StatusOK {
		t.Fatalf("GET 详情 status = %d, want 200, body=%q", dCode, dRaw)
	}
	if err := json.Unmarshal(dRaw, &d); err != nil {
		t.Fatalf("详情响应不是合法 JSON: %v, body=%q", err, dRaw)
	}
	a.RecordingDetail.HTTPStatus = dCode
	a.RecordingDetail.ID = d.ID
	a.RecordingDetail.OriginalFilename = d.OriginalFilename
	a.RecordingDetail.Extension = d.Extension
	a.RecordingDetail.SizeBytes = d.SizeBytes
	a.RecordingDetail.CreatedAt = d.CreatedAt
	a.RecordingDetail.Task.ID = d.Task.ID
	a.RecordingDetail.Task.Status = d.Task.Status
	a.RecordingDetail.Task.Attempt = d.Task.Attempt
	if d.Transcript != nil {
		a.RecordingDetail.Transcript = *d.Transcript
	}
	a.RecordingDetail.Result = d.Result

	// DB 终态 + 事件链 + 状态序列（to_status 按 event_seq 推导）。
	var task struct {
		Status      string
		Attempt     int
		EventSeq    int64
		Transcript  string
		SummaryJSON *string
	}
	if err := h.DB.Raw("SELECT status, attempt, event_seq, transcript, summary_json FROM tasks WHERE id = ?",
		resp.TaskID).Scan(&task).Error; err != nil {
		t.Fatalf("查询任务失败: %v", err)
	}
	a.TaskFinal.Status = task.Status
	a.TaskFinal.Attempt = task.Attempt
	a.TaskFinal.EventSeq = task.EventSeq
	a.TaskFinal.Transcript = task.Transcript
	// summary_json 经解析后采集：MySQL JSON 列回读文本会按自身格式归一化（冒号/逗号后
	// 加空格），采集解析结果而非原文，golden 不依赖数据库序列化格式。
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

	// 落盘文件。
	var storagePath string
	if err := h.DB.Raw("SELECT storage_path FROM recordings WHERE id = ?",
		resp.RecordingID).Scan(&storagePath).Error; err != nil {
		t.Fatalf("查询录音失败: %v", err)
	}
	if fi, err := os.Stat(filepath.Join(h.DataDir, storagePath)); err == nil {
		a.StorageFile.Exists = true
		a.StorageFile.SizeBytes = fi.Size()
	}

	// 日志镜像事件名序列（与 DB 事件链同序）。
	a.MirrorEvents = h.mirroredEvents(resp.TaskID, 4)

	a.TableCounts = h.tableCounts()

	// golden 深度比对（§5.1）：该用例真实 ID → 占位符。
	runGolden(t, "E01", &a, map[string]string{
		resp.RecordingID: "<recording_id>",
		resp.TaskID:      "<task_id>",
	})

	// 不变量巡检（测试设计 §6，直接断言不进 golden）：当前无删除功能，deleting 行应为 0。
	checkInvariants(t, h.DB, 0)
}

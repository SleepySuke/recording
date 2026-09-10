// E2E-07 并发上限（测试设计 §5.2 E2E-07，T07 起验收至 done，golden = §5.1 形状）：
// worker=3 时并发上传 5 个（文件名序号 i 为键，与运行顺序无关）→ 真实排队 →
// 全部 done、每任务事件链完整（4 事件）、无重复认领、无残留，采集进 actual 后
// 与 expected/E07.json 深度比对。
package e2e

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// e07Task E07.json 中每个任务（键 = 上传文件名序号 i）的采集形状。
type e07Task struct {
	Status           string   `json:"status"`
	Attempt          int      `json:"attempt"`
	EventSeq         int64    `json:"event_seq"`
	TaskClaimedCount int64    `json:"task_claimed_count"`
	Events           []evtRow `json:"events"`
}

// e07Actual E07.json 的采集形状：uploads/tasks 以序号 i 为键，键序固定（map 序），
// 与上传完成顺序无关；全局项为三表行数与残留数。
type e07Actual struct {
	Uploads map[string]struct {
		HTTPStatus int    `json:"http_status"`
		Status     string `json:"status"`
	} `json:"uploads"`
	Tasks                         map[string]e07Task `json:"tasks"`
	ResidualPendingOrTranscribing int64              `json:"residual_pending_or_transcribing"`
	TableCounts                   map[string]int64   `json:"table_counts"`
}

// TestE2E07_Concurrency：MockASRDelay=50ms——转写真实占用 worker 槽位（3 并发上限
// 下 5 任务必然排队两轮），但总量可控（5×50ms ≈ 250ms，期限 15s 富余）。
func TestE2E07_Concurrency(t *testing.T) {
	h := newE2E(t, e2eOpts{MockASRDelay: 50 * time.Millisecond})

	// 并发 5 个上传：每个 goroutine 独立构造 multipart 请求体（postUpload 内部
	// 每次 new buffer/writer，无共享 writer）；采集留回主 goroutine。
	const uploads = 5
	type upResult struct {
		resp uploadResp
		code int
		err  error
	}
	results := make([]upResult, uploads)
	var wg sync.WaitGroup
	for i := 0; i < uploads; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r := upResult{}
			r.resp, r.code, r.err = h.postUpload(fmt.Sprintf("e2e-07-%d.wav", i), contentOf(4096))
			results[i] = r
		}(i)
	}
	wg.Wait()

	var a e07Actual
	a.Uploads = make(map[string]struct {
		HTTPStatus int    `json:"http_status"`
		Status     string `json:"status"`
	}, uploads)
	taskIDs := make([]string, uploads)
	for i, r := range results {
		if r.err != nil {
			t.Fatalf("并发上传[%d]失败: %v", i, r.err)
		}
		if r.resp.TaskID == "" {
			t.Fatalf("上传[%d]响应缺 task_id: %+v (status=%d)", i, r.resp, r.code)
		}
		key := fmt.Sprintf("%d", i)
		a.Uploads[key] = struct {
			HTTPStatus int    `json:"http_status"`
			Status     string `json:"status"`
		}{HTTPStatus: r.code, Status: r.resp.Status}
		taskIDs[i] = r.resp.TaskID
	}

	// 轮询 5 个任务全部至 done（期限 15s，含 LLM 段；轮询不回退检查在 pollTask 内）。
	for _, id := range taskIDs {
		h.pollTask(id, "done", 15*time.Second)
	}

	// 采集（按 i 键）：每任务终态、事件链（含 task_claimed 恰 1 条，由链长与计数共同体现）。
	a.Tasks = make(map[string]e07Task, uploads)
	for i, id := range taskIDs {
		var row struct {
			Status   string
			Attempt  int
			EventSeq int64
		}
		if err := h.DB.Raw("SELECT status, attempt, event_seq FROM tasks WHERE id = ?", id).
			Scan(&row).Error; err != nil {
			t.Fatalf("查询任务失败: %v", err)
		}
		var claimed int64
		if err := h.DB.Raw(
			"SELECT COUNT(*) FROM task_events WHERE task_id = ? AND event = 'task_claimed'", id).
			Scan(&claimed).Error; err != nil {
			t.Fatalf("统计 task_claimed 失败: %v", err)
		}
		a.Tasks[fmt.Sprintf("%d", i)] = e07Task{
			Status:           row.Status,
			Attempt:          row.Attempt,
			EventSeq:         row.EventSeq,
			TaskClaimedCount: claimed,
			Events:           h.taskEventChain(id),
		}
	}

	if err := h.DB.Raw(
		"SELECT COUNT(*) FROM tasks WHERE status IN ('pending', 'transcribing')").
		Scan(&a.ResidualPendingOrTranscribing).Error; err != nil {
		t.Fatalf("统计残留失败: %v", err)
	}
	a.TableCounts = h.tableCounts()

	// golden 深度比对（§5.1）。排队时序的定量断言（完成时间体现 3 并发排队）留给
	// T13 compose golden——种子延迟可控的环境才能做稳定时间断言，本用例不做（防 flaky）。
	runGolden(t, "E07", &a, nil)

	// 不变量巡检（测试设计 §6，直接断言不进 golden）。
	checkInvariants(t, h.DB)
}

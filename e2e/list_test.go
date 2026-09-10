// E2E-08 列表与状态汇总（测试设计 §5.2 E2E-08，T06E 当前可达成范围，golden = §5.1 形状）：
// 直插种子 + 真实上传混合 → 列表逐页采集（条目键序列按 created_at 降序、状态汇总、
// deleting 不可见、翻页无遗漏），采集进 actual 后与 expected/E08.json 深度比对。
// 条目以「键」组织（upload-1/2 = 上传序号、seed-N = 种子序号），与运行顺序无关。
package e2e

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"
)

// listBody GET /v1/recordings 响应体（与 handler 实际返回一致；列表项契约无 attempt，
// 详设 §8.1，故不采集 attempt——attempt 一致性由 E01 任务/详情 golden 覆盖）。
type listItem struct {
	ID               string `json:"id"`
	OriginalFilename string `json:"original_filename"`
	SizeBytes        int64  `json:"size_bytes"`
	CreatedAt        string `json:"created_at"`
	TaskID           string `json:"task_id"`
	Status           string `json:"status"`
}

type listResp struct {
	Items    []listItem `json:"items"`
	Page     int        `json:"page"`
	PageSize int        `json:"page_size"`
	Total    int        `json:"total"`
}

// e08Actual E08.json 的采集形状。
type e08Actual struct {
	Uploads map[string]struct {
		HTTPStatus int    `json:"http_status"`
		Status     string `json:"status"`
	} `json:"uploads"`
	DefaultPage struct {
		Page     int      `json:"page"`
		PageSize int      `json:"page_size"`
		Total    int      `json:"total"`
		ItemKeys []string `json:"item_keys"`
	} `json:"default_page"`
	Paged []struct {
		Page     int      `json:"page"`
		ItemKeys []string `json:"item_keys"`
	} `json:"paged"`
	ItemStatuses         map[string]string `json:"item_statuses"`
	DeletingItemVisible  bool              `json:"deleting_item_visible"`
	UniqueItemsCollected int               `json:"unique_items_collected"`
}

// TestE2E08_List：2 真实上传（至 done，T07 起流水线终点）+ 6 条 pending 种子 + 1 条 deleting 种子。
func TestE2E08_List(t *testing.T) {
	h := newE2E(t, e2eOpts{MockASRDelay: 0}) // 0 延迟确定性替身：两个上传最快到达 done

	// 真实上传 2 个至 done（顺序上传，created_at 严格递增：upload-1 早于 upload-2）。
	a := e08Actual{}
	a.Uploads = make(map[string]struct {
		HTTPStatus int    `json:"http_status"`
		Status     string `json:"status"`
	}, 2)
	keyOf := map[string]string{} // recordingID → 键（upload-N / seed-N）
	uploadKeys := make([]string, 2)
	for i := 1; i <= 2; i++ {
		resp, code, err := h.postUpload(fmt.Sprintf("e2e08-%d.wav", i), contentOf(4096))
		if err != nil {
			t.Fatalf("上传[%d]失败: %v", i, err)
		}
		if resp.TaskID == "" {
			t.Fatalf("上传[%d]响应缺 task_id: %+v (status=%d)", i, resp, code)
		}
		key := fmt.Sprintf("upload-%d", i)
		a.Uploads[key] = struct {
			HTTPStatus int    `json:"http_status"`
			Status     string `json:"status"`
		}{HTTPStatus: code, Status: resp.Status}
		keyOf[resp.RecordingID] = key
		uploadKeys[i-1] = key
		h.pollTask(resp.TaskID, "done", 10*time.Second)
	}

	// 停池后再直插种子：池以 20ms 轮询持续认领 pending，不停池则种子会被推进，
	//「pending 按时间倒序跟随」的采集对象就没了（HTTP 服务不受停池影响）。
	h.stopWorkers()
	seedKeys := make([]string, 0, 6)
	for n := 1; n <= 6; n++ {
		recID, _ := insertPendingTask(t, h.DB, n, false)
		key := fmt.Sprintf("seed-%d", n)
		keyOf[recID] = key
		seedKeys = append(seedKeys, key)
	}
	deletingRecID, deletingTaskID := insertPendingTask(t, h.DB, 7, true) // deleting 种子：任何页不可见
	keyOf[deletingRecID] = "deleting-7"

	getList := func(path string) listResp {
		code, body := h.doGet(path)
		if code != http.StatusOK {
			t.Fatalf("GET %s status = %d, want 200, body=%q", path, code, body)
		}
		var l listResp
		if err := json.Unmarshal(body, &l); err != nil {
			t.Fatalf("列表响应不是合法 JSON: %v, body=%q", err, body)
		}
		return l
	}
	keysOf := func(l listResp) []string {
		keys := make([]string, 0, len(l.Items))
		for _, it := range l.Items {
			keys = append(keys, keyOf[it.ID])
		}
		return keys
	}

	// 默认首页（page=1 / page_size=20）：全 8 条，键序按 created_at 降序。
	def := getList("/v1/recordings")
	a.DefaultPage.Page = def.Page
	a.DefaultPage.PageSize = def.PageSize
	a.DefaultPage.Total = def.Total
	a.DefaultPage.ItemKeys = keysOf(def)

	a.ItemStatuses = make(map[string]string, len(def.Items))
	deletingVisible := false
	for _, it := range def.Items {
		a.ItemStatuses[keyOf[it.ID]] = it.Status
		if it.TaskID == deletingTaskID {
			deletingVisible = true
		}
	}

	// 翻页取完（page_size=3 → 3 页 3/3/2）：键序逐页采集，去重核对无遗漏。
	seen := map[string]int{}
	for page := 1; page <= 3; page++ {
		pg := getList(fmt.Sprintf("/v1/recordings?page=%d&page_size=3", page))
		for _, it := range pg.Items {
			seen[keyOf[it.ID]]++
			if it.TaskID == deletingTaskID {
				deletingVisible = true
			}
		}
		a.Paged = append(a.Paged, struct {
			Page     int      `json:"page"`
			ItemKeys []string `json:"item_keys"`
		}{Page: pg.Page, ItemKeys: keysOf(pg)})
	}
	for key, n := range seen {
		if n != 1 {
			t.Errorf("录音 %s 出现 %d 次, want 1（无遗漏无重复）", key, n)
		}
	}
	a.DeletingItemVisible = deletingVisible
	a.UniqueItemsCollected = len(seen)

	// golden 深度比对（§5.1）：键化组织，无需 ID 掩码。
	runGolden(t, "E08", &a, nil)

	// 不变量巡检（测试设计 §6，直接断言不进 golden）：直插 1 条 deleting 种子 →
	// 期望 deleting 行数 = 1（当前无删除功能，正常用例为 0；T09 后放宽为「清理完成后为 0」）。
	checkInvariants(t, h.DB, 1)
}

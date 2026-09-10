// E2E-09 重复上传幂等：经真实 bootstrap、TCP、MySQL、文件存储和 worker 装配，
// 上传相同字节两次。golden 固定 API 复用标志、同一资源 ID、单次创建事件和文件收敛；
// 不将运行中任务的瞬态 status 放入 golden。
package e2e

import (
	"net/http"
	"os"
	"testing"
	"time"
)

type e09Actual struct {
	First struct {
		HTTPStatus int  `json:"http_status"`
		Reused     bool `json:"idempotent_reused"`
	} `json:"first"`
	Second struct {
		HTTPStatus int  `json:"http_status"`
		Reused     bool `json:"idempotent_reused"`
		SameIDs    bool `json:"same_ids"`
	} `json:"second"`
	TaskCreatedEvents int   `json:"task_created_events"`
	ActiveRecordings  int64 `json:"active_recordings"`
	StoredFiles       int   `json:"stored_files"`
}

func TestE2E09_DuplicateUploadReusesRecording(t *testing.T) {
	// 长延迟避免 worker 在两次上传之间推进状态；服务装配、通知与真实 HTTP 仍完整执行。
	h := newE2E(t, e2eOpts{MockASRDelay: 5 * time.Second})
	content := contentOf(8 * 1024)
	first, firstCode, err := h.postUpload("e09-original.wav", content)
	if err != nil {
		t.Fatalf("首次上传失败: %v", err)
	}
	second, secondCode, err := h.postUpload("e09-copy.wav", content)
	if err != nil {
		t.Fatalf("重复上传失败: %v", err)
	}

	var a e09Actual
	a.First.HTTPStatus = firstCode
	a.First.Reused = first.IdempotentReused
	a.Second.HTTPStatus = secondCode
	a.Second.Reused = second.IdempotentReused
	a.Second.SameIDs = first.RecordingID == second.RecordingID && first.TaskID == second.TaskID
	if err := h.DB.Raw("SELECT COUNT(*) FROM task_events WHERE event = 'task_created'").Scan(&a.TaskCreatedEvents).Error; err != nil {
		t.Fatalf("统计 task_created 失败: %v", err)
	}
	if err := h.DB.Raw("SELECT COUNT(*) FROM recordings WHERE deleting_at IS NULL").Scan(&a.ActiveRecordings).Error; err != nil {
		t.Fatalf("统计 active recordings 失败: %v", err)
	}
	entries, err := os.ReadDir(h.DataDir)
	if err != nil {
		t.Fatalf("读取数据目录失败: %v", err)
	}
	a.StoredFiles = len(entries)

	runGolden(t, "E09", &a, nil)
	if firstCode != http.StatusAccepted || secondCode != http.StatusAccepted || first.IdempotentReused || !second.IdempotentReused || !a.Second.SameIDs {
		t.Fatalf("幂等响应不符: first=%+v(%d) second=%+v(%d)", first, firstCode, second, secondCode)
	}
}

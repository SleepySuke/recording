// E2E-05 处理中删除（测试设计 §5.2 E2E-05，T09；详设 §5.3）：上传 → 推进至
// transcribing（确定性长延迟替身保证删除时转写在途）→ 立即 DELETE → 204；
// 随后 GET 任务/录音 404；文件不存在；三表无该 ID 残留；日志含 task_delete_requested
// （同事务事件镜像）与 task_deleted（三表删除提交后仅写文件日志，详设 §7.5）。
package e2e

import (
	"encoding/json"
	"net/http"
	"os"
	"testing"
	"time"
)

// e05Actual E05.json 的采集形状（§5.1）。
type e05Actual struct {
	Upload struct {
		HTTPStatus  int    `json:"http_status"`
		RecordingID string `json:"recording_id"`
		TaskID      string `json:"task_id"`
		Status      string `json:"status"`
	} `json:"upload"`
	StatusBeforeDelete string `json:"status_before_delete"`
	Delete             struct {
		HTTPStatus int `json:"http_status"`
	} `json:"delete"`
	TaskAfterDelete struct {
		HTTPStatus int          `json:"http_status"`
		Error      *taskErrBody `json:"error"`
	} `json:"task_after_delete"`
	RecordingAfterDelete struct {
		HTTPStatus int          `json:"http_status"`
		Error      *taskErrBody `json:"error"`
	} `json:"recording_after_delete"`
	StorageDirFiles int              `json:"storage_dir_files"`
	TableCounts     map[string]int64 `json:"table_counts"`
	MirrorEvents    []string         `json:"mirror_events"`
}

// TestE2E05_DeleteProcessing：5s 确定性转写延迟下推进至 transcribing 再删除——
// 转写在途时删除（详设 §5.3「处理中删除」），取消/迟到写入由集成 IT-08 覆盖，
// 此处验收用户可见契约：204 → 资源全消失 → 日志可还原删除链。
func TestE2E05_DeleteProcessing(t *testing.T) {
	h := newE2E(t, e2eOpts{MockASRDelay: 5 * time.Second})

	resp, upCode, err := h.postUpload("E05-meeting.wav", contentOf(8*1024))
	if err != nil {
		t.Fatalf("上传失败: %v", err)
	}
	if resp.TaskID == "" || resp.RecordingID == "" {
		t.Fatalf("上传响应缺 recording_id/task_id: %+v (status=%d)", resp, upCode)
	}

	// 推进至 transcribing 后删除：事件链确定为 created → claimed → delete_requested
	//（5s 延迟保证删除时转写在途，无后续阶段事件，golden 确定性）。
	h.pollTask(resp.TaskID, "transcribing", 10*time.Second)

	var a e05Actual
	a.Upload.HTTPStatus = upCode
	a.Upload.RecordingID = resp.RecordingID
	a.Upload.TaskID = resp.TaskID
	a.Upload.Status = resp.Status
	a.StatusBeforeDelete = "transcribing"

	dCode, _ := h.doDelete(resp.RecordingID)
	if dCode != http.StatusNoContent {
		t.Fatalf("DELETE status = %d, want 204", dCode)
	}
	a.Delete.HTTPStatus = dCode

	// 随后任务/录音 404（任务 30001、录音 20005，详设 §8.3）。
	tCode, tRaw := h.doGet("/v1/tasks/" + resp.TaskID)
	if tCode != http.StatusNotFound {
		t.Fatalf("GET 任务 status = %d, want 404, body=%q", tCode, tRaw)
	}
	var tBody struct {
		Error taskErrBody `json:"error"`
	}
	if err := json.Unmarshal(tRaw, &tBody); err != nil {
		t.Fatalf("任务 404 响应不是合法 JSON: %v, body=%q", err, tRaw)
	}
	a.TaskAfterDelete.HTTPStatus = tCode
	a.TaskAfterDelete.Error = &tBody.Error

	rCode, rRaw := h.doGet("/v1/recordings/" + resp.RecordingID)
	if rCode != http.StatusNotFound {
		t.Fatalf("GET 录音 status = %d, want 404, body=%q", rCode, rRaw)
	}
	var rBody struct {
		Error taskErrBody `json:"error"`
	}
	if err := json.Unmarshal(rRaw, &rBody); err != nil {
		t.Fatalf("录音 404 响应不是合法 JSON: %v, body=%q", err, rRaw)
	}
	a.RecordingAfterDelete.HTTPStatus = rCode
	a.RecordingAfterDelete.Error = &rBody.Error

	// 文件不存在：数据目录无残留（含 tmp-）。
	entries, err := os.ReadDir(h.DataDir)
	if err != nil {
		t.Fatalf("读数据目录失败: %v", err)
	}
	a.StorageDirFiles = len(entries)

	// 三表无该 ID 残留（本用例独库，全表为 0 即无残留，详设 §4.6）。
	a.TableCounts = h.tableCounts()

	// 日志镜像：created → claimed → delete_requested → deleted（最后者仅文件日志，§7.5）。
	a.MirrorEvents = h.mirroredEvents(resp.TaskID, 4)

	runGolden(t, "E05", &a, map[string]string{
		resp.RecordingID: "<recording_id>",
		resp.TaskID:      "<task_id>",
	})
	checkInvariants(t, h.DB)
}

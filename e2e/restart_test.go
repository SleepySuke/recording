// E2E-06 重启恢复（测试设计 §5.2 E2E-06，T11；详设 §6.1 崩溃窗口、§6.2 启动恢复
// 七步、§6.3 reset 基线）：上传 → 长延迟任务在途（transcribing）时超预算强停服务 A
// （§3.5 第 4 条：取消 runCtx，任务保持在途、无伪 failed）→ 同库同目录重启服务 B →
// 启动恢复重置在途（pending、attempt=2、task_recovered）→ 任务从头重做至 done；
// readyz 在恢复完成后就绪。任务简报步骤 6 的手动演练（kill 进程重启）由此进程内
// 重启路径覆盖。
package e2e

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"recording-transcription/internal/infrastructure/llm"
)

// e06Actual E06.json 的采集形状（§5.1）。
type e06Actual struct {
	Upload struct {
		HTTPStatus  int    `json:"http_status"`
		RecordingID string `json:"recording_id"`
		TaskID      string `json:"task_id"`
		Status      string `json:"status"`
	} `json:"upload"`
	StatusAtStop       string `json:"status_at_stop"`
	ReadyzAfterRestart int    `json:"readyz_after_restart"`
	TaskAfterRestart   struct {
		HTTPStatus int    `json:"http_status"`
		Status     string `json:"status"`
		Attempt    int    `json:"attempt"`
	} `json:"task_after_restart"`
	StorageDirFiles int              `json:"storage_dir_files"`
	TableCounts     map[string]int64 `json:"table_counts"`
	EventChain      []evtRow         `json:"event_chain"`
	MirrorEvents    []string         `json:"mirror_events"`
}

// TestE2E06_RecoverAfterRestart：A（10s 确定性转写延迟 + 300ms 停机预算）在途强停 →
// B（100ms 延迟，同 DataDir/LogDir/DB）重启恢复重做。事件链跨轮次可还原：
// created → claimed（第 1 轮）→ recovered → claimed → transcription_completed →
// task_completed（第 2 轮，详设 §7.1「重启恢复（基线）」）。
func TestE2E06_RecoverAfterRestart(t *testing.T) {
	db := requireTestDB(t)
	dataDir := t.TempDir()
	logDir := t.TempDir()

	// 服务 A：长延迟转写让任务稳定停在 transcribing；小停机预算保证 Shutdown 走
	// 超预算强停分支（在途保持在途，详设 §3.5/§6.1）。
	fakeA := llm.NewFake()
	fakeSrvA := httptest.NewServer(fakeA)
	t.Cleanup(fakeSrvA.Close)
	appA, urlA := bootApp(t, newE2EConfig(e2eOpts{MockASRDelay: 10 * time.Second},
		fakeSrvA.URL, dataDir, logDir, 300*time.Millisecond))
	hA := attach(t, db, appA, urlA, fakeA, dataDir, logDir)

	resp, upCode, err := hA.postUpload("E06-meeting.wav", contentOf(8*1024))
	if err != nil {
		t.Fatalf("上传失败: %v", err)
	}
	if resp.TaskID == "" || resp.RecordingID == "" {
		t.Fatalf("上传响应缺 recording_id/task_id: %+v (status=%d)", resp, upCode)
	}
	// 停机前任务处于在途（transcribing：10s 延迟保证强停时转写未完）。
	hA.pollTask(resp.TaskID, "transcribing", 10*time.Second)

	var a e06Actual
	a.Upload.HTTPStatus = upCode
	a.Upload.RecordingID = resp.RecordingID
	a.Upload.TaskID = resp.TaskID
	a.Upload.Status = resp.Status
	a.StatusAtStop = "transcribing"

	// 强停 A（超预算：取消 runCtx 中断在途外呼；无伪 failed，任务留在 transcribing）。
	appA.Lifecycle.Shutdown()

	// 服务 B：同库同目录重启——NewApp 装配内同步执行 §6.2 启动恢复，返回即完成；
	// readyz 门控在恢复后就绪（详设 §6.2 步骤 7）。
	fakeB := llm.NewFake()
	fakeSrvB := httptest.NewServer(fakeB)
	t.Cleanup(fakeSrvB.Close)
	appB, urlB := bootApp(t, newE2EConfig(e2eOpts{MockASRDelay: 100 * time.Millisecond},
		fakeSrvB.URL, dataDir, logDir, 20*time.Second))
	hB := attach(t, db, appB, urlB, fakeB, dataDir, logDir)

	a.ReadyzAfterRestart = hB.pollReadyz(5 * time.Second)

	// 恢复重做至 done、attempt=2（§6.1：重置 pending + attempt+1 从头重做）。
	hB.pollTask(resp.TaskID, "done", 30*time.Second)
	code, body := hB.doGet("/v1/tasks/" + resp.TaskID)
	if code != http.StatusOK {
		t.Fatalf("GET 任务 status = %d, want 200, body=%q", code, body)
	}
	var tb struct {
		Status  string `json:"status"`
		Attempt int    `json:"attempt"`
	}
	if err := json.Unmarshal(body, &tb); err != nil {
		t.Fatalf("任务响应不是合法 JSON: %v, body=%q", err, body)
	}
	a.TaskAfterRestart.HTTPStatus = code
	a.TaskAfterRestart.Status = tb.Status
	a.TaskAfterRestart.Attempt = tb.Attempt

	// 数据目录仅剩被引用音频（孤儿核对不误删，§5.2）。
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		t.Fatalf("读数据目录失败: %v", err)
	}
	a.StorageDirFiles = len(entries)
	a.TableCounts = hB.tableCounts()

	// 事件链跨轮次可还原（§7.1/§7.3，真实值采集）+ 文件日志镜像。
	a.EventChain = hB.taskEventChain(resp.TaskID)
	a.MirrorEvents = hB.mirroredEvents(resp.TaskID, 6)

	runGolden(t, "E06", &a, map[string]string{
		resp.RecordingID: "<recording_id>",
		resp.TaskID:      "<task_id>",
	})
	checkInvariants(t, db)
}

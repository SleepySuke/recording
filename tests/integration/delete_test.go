package integration

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"recording-transcription/internal/application/ports"
	domain "recording-transcription/internal/domain/recording"
	"recording-transcription/internal/infrastructure/llm"
	"recording-transcription/internal/infrastructure/persistence/mysql"
)

// 测试依据：T09 步骤 2；设计依据：详设 §5.3（删除流程：标记 → 取消 → 删文件 →
// 三表清理 → 204；失败 503/20006 保留标记由清理续做）、§3.4（取消表锁外调用）、
// §4.6（三表显式删除、任一步失败整体回滚）、§4.1（worker 落结果 vs DELETE）、
// §7.5（task_deleted 仅写文件日志）、§10（CLEANUP_INTERVAL 低频清理）。
// 用例编号 IT-08 / IT-09 / IT-19 及删除主链路（TestDelete_*）。

// lateSummary 迟到 ⑤ 落库用的合法摘要值。
func lateSummary() domain.Summary {
	return domain.Summary{Summary: "迟到摘要", KeyPoints: []string{}, Todos: []string{}}
}

// doDeleteRaw 发起 DELETE /v1/recordings/:id，返回原始 recorder。
func doDeleteRaw(r *gin.Engine, recordingID string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodDelete, "/v1/recordings/"+recordingID, nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	return rec
}

// doGetRaw 任意 GET（删除后 404 断言用）。
func doGetRaw(r *gin.Engine, path string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	return rec
}

// mirroredEventNames 解析 LogDir/app.jsonl，返回该 task_id 的镜像事件名集合（按出现序）。
func mirroredEventNames(t *testing.T, logDir, taskID string) []string {
	t.Helper()
	data, err := os.ReadFile(logDir + "/app.jsonl")
	if err != nil {
		t.Fatalf("读取 app.jsonl 失败: %v", err)
	}
	var events []string
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
	return events
}

// TestDelete_ProcessingRecording（T09 步骤 2 主链路，详设 §5.3）：上传后立即
// DELETE → 204；随后 GET 任务/录音 404；数据目录无该文件；三表无该 ID 残留；
// logs/app.jsonl 含 task_delete_requested 与 task_deleted（后者仅文件日志，§7.5）。
func TestDelete_ProcessingRecording(t *testing.T) {
	h := NewPipelineHarness(t, WithFileLogging())

	// 池在运行：上传与删除之间任务可能已被认领推进（任意状态可删，详设 §5.3）。
	w := doUpload(t, h.Router, formPart{field: "file", filename: "del-meeting.wav", content: contentOf(2048)})
	if w.Code != http.StatusAccepted {
		t.Fatalf("上传 status = %d, want 202, body=%q", w.Code, w.Body.String())
	}
	resp := decodeUpload(t, w)

	rec := doDeleteRaw(h.Router, resp.RecordingID)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("DELETE status = %d, want 204, body=%q", rec.Code, rec.Body.String())
	}

	// 随后任务/录音均 404（详设 §8.1：不存在/已删除；任务 30001、录音 20005）。
	taskRec := doGetRaw(h.Router, "/v1/tasks/"+resp.TaskID)
	if taskRec.Code != http.StatusNotFound {
		t.Errorf("GET 任务 status = %d, want 404, body=%q", taskRec.Code, taskRec.Body.String())
	} else if e := decodeAPIError(t, taskRec); e.Error.Code != 30001 {
		t.Errorf("GET 任务业务码 = %d, want 30001", e.Error.Code)
	}
	getRec := doGetRaw(h.Router, "/v1/recordings/"+resp.RecordingID)
	if getRec.Code != http.StatusNotFound {
		t.Errorf("GET 录音 status = %d, want 404, body=%q", getRec.Code, getRec.Body.String())
	} else if e := decodeAPIError(t, getRec); e.Error.Code != 20005 {
		t.Errorf("GET 录音业务码 = %d, want 20005", e.Error.Code)
	}

	// 文件已删：数据目录无残留（含 tmp-）。
	if names := dataFiles(t, h.DataDir); len(names) != 0 {
		t.Errorf("数据目录残留文件: %v, want 空", names)
	}
	// 三表无该 ID 残留（详设 §4.6 三表显式删除）。
	for _, table := range []string{"recordings", "tasks", "task_events"} {
		if n := tableCount(t, h.DB, table); n != 0 {
			t.Errorf("%s 残留 %d 行, want 0", table, n)
		}
	}
	// 文件日志：task_delete_requested（同事务事件镜像）与 task_deleted（三表删除提交后
	// 仅写文件日志，独立 event_id、最后 event_seq+1，详设 §7.5）。
	events := mirroredEventNames(t, h.LogDir, resp.TaskID)
	has := map[string]bool{}
	for _, e := range events {
		has[e] = true
	}
	if !has["task_delete_requested"] || !has["task_deleted"] {
		t.Errorf("镜像事件 = %v, want 同时含 task_delete_requested 与 task_deleted", events)
	}
}

// TestDelete_AnyStatus（T09 步骤 2，详设 §5.3「允许删除任意任务状态」）：
// pending / transcribing / summarizing / done / failed 预置状态后 DELETE → 204 且清理完成。
func TestDelete_AnyStatus(t *testing.T) {
	for i, status := range []string{"pending", "transcribing", "summarizing", "done", "failed"} {
		t.Run(status, func(t *testing.T) {
			h := NewPipelineHarness(t)
			h.Pool.Stop() // 冻结预置状态，不与流水线竞争
			recID, _ := insertTaskState(t, h.DB, i+1, status, false)

			rec := doDeleteRaw(h.Router, recID)
			if rec.Code != http.StatusNoContent {
				t.Fatalf("%s: DELETE status = %d, want 204, body=%q", status, rec.Code, rec.Body.String())
			}
			for _, table := range []string{"recordings", "tasks", "task_events"} {
				if n := tableCount(t, h.DB, table); n != 0 {
					t.Errorf("%s: %s 残留 %d 行, want 0", status, table, n)
				}
			}
		})
	}
}

// TestIT08_LateWriteRejected（IT-08，详设 §4.1「worker 落结果 vs DELETE」/§5.3）：
// deleting_at 提交后持原执行键的迟到落结果（④/⑤/③）全部被复查/条件更新拒绝
// （ErrStaleExecution），结果只记日志、资源不复活；随后三表清理完成，迟到写入依旧被拒。
func TestIT08_LateWriteRejected(t *testing.T) {
	db, ptx := newClaimEnv(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	recTx := mysql.NewRecordingTx(db, itInstanceID, logger)
	ctx := context.Background()

	recID, taskID := insertPendingTask(t, db, 1, false)
	exec, ok, err := ptx.ClaimNext(ctx) // → transcribing，执行键 {taskID, 1}
	if err != nil || !ok {
		t.Fatalf("认领失败: ok=%v err=%v", ok, err)
	}

	// 删除标记事务提交（详设 §5.3 步骤 1）。
	markedTaskID, marked, err := recTx.MarkDeleting(ctx, recID)
	if err != nil || !marked {
		t.Fatalf("MarkDeleting = (%q, %v, %v), want (taskID, true, nil)", markedTaskID, marked, err)
	}
	if markedTaskID != taskID {
		t.Fatalf("MarkDeleting 返回 taskID = %q, want %q", markedTaskID, taskID)
	}

	// 迟到落结果：④/③/⑤ 全部被拒（deleting_at 复查失败），不报硬错误
	//（硬错误会触发 §5.5 重试与停池）。
	key := exec.ExecutionKey
	if err := ptx.SaveTranscription(ctx, key, "迟到正文"); !errors.Is(err, ports.ErrStaleExecution) {
		t.Errorf("事务④迟到写入错误 = %v, want ErrStaleExecution", err)
	}
	if err := ptx.FailTask(ctx, key, 40001, "迟到失败"); !errors.Is(err, ports.ErrStaleExecution) {
		t.Errorf("事务③迟到写入错误 = %v, want ErrStaleExecution", err)
	}
	if err := ptx.CompleteTask(ctx, key, lateSummary()); !errors.Is(err, ports.ErrStaleExecution) {
		t.Errorf("事务⑤迟到写入错误 = %v, want ErrStaleExecution", err)
	}

	// 资源不复活：任务仍在途（transcribing）、deleting_at 保留、事件无迟到新增
	//（仅 created + claimed + delete_requested = 3 条）。
	var task struct {
		Status   string
		Attempt  int
		EventSeq int64
	}
	if err := db.Raw("SELECT status, attempt, event_seq FROM tasks WHERE id = ?", taskID).Scan(&task).Error; err != nil {
		t.Fatalf("查询任务失败: %v", err)
	}
	if task.Status != "transcribing" || task.Attempt != 1 || task.EventSeq != 3 {
		t.Errorf("任务 = %+v, want transcribing/attempt=1/seq=3（delete_requested 占 seq=3）", task)
	}
	if n := tableCount(t, db, "task_events"); n != 3 {
		t.Errorf("事件数 = %d, want 3（迟到写入不新增事件）", n)
	}
	var deletingAt *time.Time
	if err := db.Raw("SELECT deleting_at FROM recordings WHERE id = ?", recID).Scan(&deletingAt).Error; err != nil {
		t.Fatalf("查询录音失败: %v", err)
	}
	if deletingAt == nil {
		t.Error("deleting_at 未保留（迟到写入复活了资源）")
	}

	// 三表清理完成后，同键迟到写入依旧被拒（任务行已不存在 → 仍 ErrStaleExecution）。
	if err := recTx.PurgeRecording(ctx, recID); err != nil {
		t.Fatalf("PurgeRecording 失败: %v", err)
	}
	if err := ptx.SaveTranscription(ctx, key, "迟到正文"); !errors.Is(err, ports.ErrStaleExecution) {
		t.Errorf("清理后事务④错误 = %v, want ErrStaleExecution", err)
	}
	if err := ptx.FailTask(ctx, key, 40001, "迟到失败"); !errors.Is(err, ports.ErrStaleExecution) {
		t.Errorf("清理后事务③错误 = %v, want ErrStaleExecution", err)
	}
	if err := ptx.CompleteTask(ctx, key, lateSummary()); !errors.Is(err, ports.ErrStaleExecution) {
		t.Errorf("清理后事务⑤错误 = %v, want ErrStaleExecution", err)
	}
}

// TestDelete_LateWriteAfterPurge（T07 评审强修项，T09 一并交付）：录音行被清理后
// 迟到 ⑤/③ 不得把「录音行锁 NotFound」当硬错误（原实现会 §5.5 重试后停池）；
// 与事务④的既有处理（processing_tx.go SaveTranscription）对齐，返回 ErrStaleExecution
// 静默丢弃。
func TestDelete_LateWriteAfterPurge(t *testing.T) {
	ctx := context.Background()

	// ⑤：summarizing 下仅 recordings 行被清（任务行仍在）→ 事务⑤录音行锁 NotFound。
	db, ptx := newClaimEnv(t)
	recID, _ := insertPendingTask(t, db, 1, false)
	exec, ok, err := ptx.ClaimNext(ctx)
	if err != nil || !ok {
		t.Fatalf("认领失败: ok=%v err=%v", ok, err)
	}
	if err := ptx.SaveTranscription(ctx, exec.ExecutionKey, "正文"); err != nil {
		t.Fatalf("推进 summarizing 失败: %v", err)
	}
	if err := db.Exec("DELETE FROM recordings WHERE id = ?", recID).Error; err != nil {
		t.Fatalf("清理 recordings 行失败: %v", err)
	}
	if err := ptx.CompleteTask(ctx, exec.ExecutionKey, lateSummary()); !errors.Is(err, ports.ErrStaleExecution) {
		t.Errorf("事务⑤错误 = %v, want ErrStaleExecution（录音行已清，迟到结果静默丢弃）", err)
	}
	if n := tableCount(t, db, "task_events"); n != 3 { // created+claimed+transcription_completed
		t.Errorf("事务⑤事件数 = %d, want 3（无升级副作用）", n)
	}

	// ③：transcribing 下仅 recordings 行被清 → 事务③同样不得硬错误。
	db2, ptx2 := newClaimEnv(t)
	recID2, _ := insertPendingTask(t, db2, 1, false)
	exec2, ok2, err := ptx2.ClaimNext(ctx)
	if err != nil || !ok2 {
		t.Fatalf("认领失败: ok=%v err=%v", ok2, err)
	}
	if err := db2.Exec("DELETE FROM recordings WHERE id = ?", recID2).Error; err != nil {
		t.Fatalf("清理 recordings 行失败: %v", err)
	}
	if err := ptx2.FailTask(ctx, exec2.ExecutionKey, 40001, "迟到失败"); !errors.Is(err, ports.ErrStaleExecution) {
		t.Errorf("事务③错误 = %v, want ErrStaleExecution（录音行已清，迟到结果静默丢弃）", err)
	}
	if n := tableCount(t, db2, "task_events"); n != 2 { // created+claimed
		t.Errorf("事务③事件数 = %d, want 2（无升级副作用）", n)
	}
}

// TestIT09_ThreeTableDeleteRollback（IT-09，详设 §4.6/§5.3）：注入 task_events
// 删除失败（RENAME 隐藏表使三表清理首条 DELETE 真实报错）→ PurgeRecording 整体回滚，
// 无半删除（recordings/tasks/task_events 俱全、deleting_at 保留）；解除注入后
// 再次清理收敛完成。
func TestIT09_ThreeTableDeleteRollback(t *testing.T) {
	db := RequireTestDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	recTx := mysql.NewRecordingTx(db, itInstanceID, logger)
	ctx := context.Background()

	recID, _ := insertPendingTask(t, db, 1, false)
	if _, ok, err := recTx.MarkDeleting(ctx, recID); err != nil || !ok {
		t.Fatalf("MarkDeleting = (%v, %v), want (true, nil)", ok, err)
	}

	// 注入：隐藏 task_events，使三表清理的首条 DELETE 真实失败（详设 §5.3 DelTx
	// 「任一步失败」分支）；还原幂等（显式恢复后 t.Cleanup 再调为无操作），
	// 防失败路径遗留隐藏表影响后续用例。
	if err := db.Exec("RENAME TABLE task_events TO t09_events_hidden").Error; err != nil {
		t.Fatalf("注入（RENAME）失败: %v", err)
	}
	restore := func() {
		var hidden int64
		_ = db.Raw(
			"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = 't09_events_hidden'",
		).Scan(&hidden).Error
		if hidden == 1 {
			_ = db.Exec("RENAME TABLE t09_events_hidden TO task_events").Error
		}
	}
	t.Cleanup(restore)

	err := recTx.PurgeRecording(ctx, recID)
	if err == nil {
		t.Fatal("PurgeRecording 应因 events 删除失败而失败，实际成功")
	}
	if errors.Is(err, ports.ErrCommitUnknown) {
		t.Errorf("语句失败应表现为回滚错误而非提交结果未知: %v", err)
	}
	restore()

	// 整体回滚：三表俱全、deleting_at 保留（无半删除）。
	if n := tableCount(t, db, "recordings"); n != 1 {
		t.Errorf("recordings 行数 = %d, want 1（整体回滚）", n)
	}
	if n := tableCount(t, db, "tasks"); n != 1 {
		t.Errorf("tasks 行数 = %d, want 1（整体回滚）", n)
	}
	if n := tableCount(t, db, "task_events"); n != 2 { // created + delete_requested
		t.Errorf("task_events 行数 = %d, want 2（整体回滚）", n)
	}
	var deletingAt *time.Time
	if err := db.Raw("SELECT deleting_at FROM recordings WHERE id = ?", recID).Scan(&deletingAt).Error; err != nil {
		t.Fatalf("查询录音失败: %v", err)
	}
	if deletingAt == nil {
		t.Error("deleting_at 未保留（失败后应留标记供续做）")
	}

	// 解除注入后重试清理 → 收敛完成（详设 §5.3「供客户端再次 DELETE 或后台清理重试」）。
	if err := recTx.PurgeRecording(ctx, recID); err != nil {
		t.Fatalf("解除注入后 PurgeRecording 失败: %v", err)
	}
	for _, table := range []string{"recordings", "tasks", "task_events"} {
		if n := tableCount(t, db, table); n != 0 {
			t.Errorf("收敛后 %s 残留 %d 行, want 0", table, n)
		}
	}
}

// TestDelete_FileFailureRetriesViaCleanup（T09 步骤 2，详设 §5.3）：
// 文件删除失败（目录去写权限）→ 503/20006 保留标记；期间重复 DELETE 幂等续做、
// 不重复 task_delete_requested；恢复权限后低频清理循环收敛；已彻底删除的 ID → 404/20005。
func TestDelete_FileFailureRetriesViaCleanup(t *testing.T) {
	h := NewPipelineHarness(t)
	h.Pool.Stop()        // 冻结 pending，聚焦删除-清理链路
	h.StartCleanupLoop() // 本用例断言后台清理收敛，按需启动 50ms 循环（harness 默认不启动）

	w := doUpload(t, h.Router, formPart{field: "file", filename: "cleanup.wav", content: contentOf(1024)})
	if w.Code != http.StatusAccepted {
		t.Fatalf("上传 status = %d, want 202", w.Code)
	}
	resp := decodeUpload(t, w)

	// 目录去写权限：文件删除失败 → 503/20006（详设 §5.3 E503 分支）。
	if err := os.Chmod(h.DataDir, 0o500); err != nil {
		t.Fatalf("chmod 数据目录失败: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(h.DataDir, 0o700) })

	rec := doDeleteRaw(h.Router, resp.RecordingID)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("DELETE status = %d, want 503, body=%q", rec.Code, rec.Body.String())
	}
	if e := decodeAPIError(t, rec); e.Error.Code != 20006 {
		t.Errorf("业务码 = %d, want 20006", e.Error.Code)
	}

	// 标记保留 + 恰好一条 task_delete_requested。
	var marked int64
	if err := h.DB.Raw(
		"SELECT COUNT(*) FROM recordings WHERE id = ? AND deleting_at IS NOT NULL", resp.RecordingID,
	).Scan(&marked).Error; err != nil {
		t.Fatalf("查询失败: %v", err)
	}
	if marked != 1 {
		t.Fatalf("deleting_at 标记行 = %d, want 1（失败保留标记）", marked)
	}
	countReqEvents := func() int64 {
		var n int64
		if err := h.DB.Raw(
			"SELECT COUNT(*) FROM task_events WHERE task_id = ? AND event = 'task_delete_requested'", resp.TaskID,
		).Scan(&n).Error; err != nil {
			t.Fatalf("查询事件失败: %v", err)
		}
		return n
	}
	if n := countReqEvents(); n != 1 {
		t.Fatalf("task_delete_requested = %d 条, want 1", n)
	}

	// 期间重复 DELETE：幂等续做（仍 503/20006）且不重复追加事件。
	rec2 := doDeleteRaw(h.Router, resp.RecordingID)
	if rec2.Code != http.StatusServiceUnavailable {
		t.Fatalf("重复 DELETE status = %d, want 503", rec2.Code)
	}
	if e := decodeAPIError(t, rec2); e.Error.Code != 20006 {
		t.Errorf("重复 DELETE 业务码 = %d, want 20006", e.Error.Code)
	}
	if n := countReqEvents(); n != 1 {
		t.Errorf("重复 DELETE 后 task_delete_requested = %d 条, want 1（幂等续做不重复事件）", n)
	}

	// 恢复权限 → 低频清理循环收敛（harness 注入 50ms 间隔，详设 §10 CLEANUP_INTERVAL）。
	if err := os.Chmod(h.DataDir, 0o700); err != nil {
		t.Fatalf("恢复数据目录权限失败: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if tableCount(t, h.DB, "recordings") == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("清理循环未在 5 秒内收敛（recordings 仍在）")
		}
		time.Sleep(20 * time.Millisecond)
	}
	for _, table := range []string{"tasks", "task_events"} {
		if n := tableCount(t, h.DB, table); n != 0 {
			t.Errorf("清理收敛后 %s 残留 %d 行, want 0", table, n)
		}
	}
	if names := dataFiles(t, h.DataDir); len(names) != 0 {
		t.Errorf("清理收敛后数据目录残留: %v", names)
	}

	// 已彻底删除的 ID → 404/20005（详设 §5.3「已完全删除的 ID 返回 404」）。
	rec3 := doDeleteRaw(h.Router, resp.RecordingID)
	if rec3.Code != http.StatusNotFound {
		t.Fatalf("已删除 ID 的 DELETE status = %d, want 404", rec3.Code)
	}
	if e := decodeAPIError(t, rec3); e.Error.Code != 20005 {
		t.Errorf("已删除 ID 业务码 = %d, want 20005", e.Error.Code)
	}
}

// it19Row 事件链断言行（IT-19）。
type it19Row struct {
	Event    string
	EventSeq int64
	Attempt  int
}

// eventChain 按 event_seq 升序取该任务事件链（IT-19）。
func eventChain(t *testing.T, db *gorm.DB, taskID string) []it19Row {
	t.Helper()
	var rows []it19Row
	if err := db.Raw(
		"SELECT event, event_seq, attempt FROM task_events WHERE task_id = ? ORDER BY event_seq", taskID,
	).Scan(&rows).Error; err != nil {
		t.Fatalf("查询事件链失败: %v", err)
	}
	return rows
}

// assertChain 断言事件名序、event_seq 严格递增（1 起连续）与 attempt 匹配（IT-19）。
func assertChain(t *testing.T, name string, chain []it19Row, wantEvents []string, wantAttempts []int) {
	t.Helper()
	if len(chain) != len(wantEvents) {
		t.Fatalf("%s 事件链长度 = %d, want %d: %+v", name, len(chain), len(wantEvents), chain)
	}
	for i, row := range chain {
		if row.Event != wantEvents[i] {
			t.Errorf("%s 事件[%d] = %q, want %q", name, i, row.Event, wantEvents[i])
		}
		if row.EventSeq != int64(i+1) {
			t.Errorf("%s event_seq[%d] = %d, want %d（严格递增连续）", name, i, row.EventSeq, i+1)
		}
		if row.Attempt != wantAttempts[i] {
			t.Errorf("%s attempt[%d] = %d, want %d", name, i, row.Attempt, wantAttempts[i])
		}
	}
}

// TestIT19_EventChainReconstructable（IT-19，详设 §7.1/§7.2）：成功/失败/重试/删除
// 各跑一遍 → 按 task_id 查事件：event_seq 严格递增、attempt 正确、跨轮次完整；
// 删除场景验证到删除前为止（三表清理后事件随之消失，详设 §7.5）。
func TestIT19_EventChainReconstructable(t *testing.T) {
	h := NewPipelineHarness(t) // 确定性替身默认成功；失败经 FakeLLM 挂起注入（IT-14 模式）

	// A：成功链（created → claimed → transcription_completed → completed）。
	wa := doUpload(t, h.Router, formPart{field: "file", filename: "it19-a.wav", content: contentOf(1024)})
	if wa.Code != http.StatusAccepted {
		t.Fatalf("上传 A status = %d, want 202", wa.Code)
	}
	a := decodeUpload(t, wa)
	waitForTaskStatus(t, h.DB, a.TaskID, "done")

	// B：失败（LLM 挂起 → 50001）→ 手动重试 → 成功（7 事件跨两轮，详设 §7.1/§9）。
	h.Fake.SetMode(llm.FakeModeHang)
	wb := doUpload(t, h.Router, formPart{field: "file", filename: "it19-b.wav", content: contentOf(1024)})
	if wb.Code != http.StatusAccepted {
		t.Fatalf("上传 B status = %d, want 202", wb.Code)
	}
	b := decodeUpload(t, wb)
	waitForTaskStatus(t, h.DB, b.TaskID, "failed")
	h.Fake.SetMode(llm.FakeModeNormal)
	if rec := doRetryRaw(h.Router, b.TaskID); rec.Code != http.StatusAccepted {
		t.Fatalf("retry B status = %d, want 202, body=%q", rec.Code, rec.Body.String())
	}
	waitForTaskStatus(t, h.DB, b.TaskID, "done")

	// 删除前捕获 B 链（删除场景验证到删除前为止）。
	chainA := eventChain(t, h.DB, a.TaskID)
	chainB := eventChain(t, h.DB, b.TaskID)

	// 删除 A：三表清理后 A 事件消失，B 链不受影响。
	if rec := doDeleteRaw(h.Router, a.RecordingID); rec.Code != http.StatusNoContent {
		t.Fatalf("DELETE A status = %d, want 204", rec.Code)
	}
	if chain := eventChain(t, h.DB, a.TaskID); len(chain) != 0 {
		t.Errorf("删除后 A 事件仍存在: %v", chain)
	}
	if got := eventChain(t, h.DB, b.TaskID); len(got) != len(chainB) {
		t.Errorf("删除 A 影响 B 事件链: %d 条, want %d 条", len(got), len(chainB))
	}

	// A 链：严格递增 1..4，全为 attempt=1。
	assertChain(t, "A", chainA,
		[]string{"task_created", "task_claimed", "transcription_completed", "task_completed"},
		[]int{1, 1, 1, 1})
	// B 链：严格递增 1..8，attempt 首轮（含 summarizing 失败）1、新一轮 2。
	assertChain(t, "B", chainB,
		[]string{"task_created", "task_claimed", "transcription_completed", "task_failed",
			"task_retry_accepted", "task_claimed", "transcription_completed", "task_completed"},
		[]int{1, 1, 1, 1, 2, 2, 2, 2})
}

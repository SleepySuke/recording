// 测试依据：T11 步骤 2；设计依据：详设 §6.1（崩溃窗口：在途一律重置 pending、
// attempt+1、清本轮产物）、§6.2（启动恢复七步顺序与就绪门控）、§6.3（RECOVERY_MODE=
// interrupt 降级：failed/30003 + task_interrupted）、§4.6（LEFT JOIN 关联巡检，异常
// 90004 阻止 ready 不静默修复）、§5.2（孤儿文件核对：tmp- 直删、查询失败不清理）。
// 用例编号 IT-10 / IT-11 / IT-12 及恢复边界（TestRecovery_*）。
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime/multipart"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gorm.io/gorm"

	"recording-transcription/bootstrap"
	"recording-transcription/internal/application/ports"
	"recording-transcription/internal/application/processing"
	apprec "recording-transcription/internal/application/recording"
	"recording-transcription/internal/infrastructure/filestore/local"
	"recording-transcription/internal/infrastructure/llm"
	"recording-transcription/internal/infrastructure/persistence/mysql"
	"recording-transcription/internal/infrastructure/worker"
)

// recoverParts newRecoverService 的组装产物：RecoverService 供驱动，procTx / store
// 供查询失败注入（TestRecovery_OrphanFiles 的端口装饰）。
type recoverParts struct {
	rec    *processing.RecoverService
	procTx ports.RecoveryTx
	store  *local.Store
}

// newRecoverService 组装真实 RecoverService（MySQL RecoveryTx + 本地文件存储 +
// 删除用例 CleanupPending 复用为删除恢复）；mode 为 reset / interrupt（详设 §6.3）。
func newRecoverService(t *testing.T, db *gorm.DB, dataDir string, mode string) recoverParts {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := local.New(dataDir, itMaxFileBytes, 512*1024*1024)
	if err != nil {
		t.Fatalf("初始化测试数据目录失败: %v", err)
	}
	procTx := mysql.NewProcessingTx(db, itInstanceID, logger)
	recordingTx := mysql.NewRecordingTx(db, itInstanceID, logger)
	query := mysql.NewRecordingQuery(db)
	deleteSvc := apprec.NewDeleteService(recordingTx, query, store, worker.NewCancelTable(), logger, itInstanceID)
	return recoverParts{
		rec:    processing.NewRecoverService(procTx, deleteSvc, store, logger, mode == "interrupt"),
		procTx: procTx,
		store:  store,
	}
}

// failingPathsTx 端口装饰：ListStoragePaths 注入失败（§5.2「查询失败不清理」分支）。
type failingPathsTx struct{ ports.RecoveryTx }

func (f failingPathsTx) ListStoragePaths(context.Context) ([]string, error) {
	return nil, errors.New("注入：引用集查询失败")
}

// inflightRow 在途任务字段断言视图（IT-10 / 降级模式共用）。
type inflightRow struct {
	Status       string  `gorm:"column:status"`
	Attempt      int     `gorm:"column:attempt"`
	EventSeq     int64   `gorm:"column:event_seq"`
	Transcript   *string `gorm:"column:transcript"`
	SummaryJSON  *string `gorm:"column:summary_json"`
	ErrorCode    *int    `gorm:"column:error_code"`
	ErrorMessage *string `gorm:"column:error_message"`
	StartedAt    *string `gorm:"column:started_at"`
	FinishedAt   *string `gorm:"column:finished_at"`
}

// loadTask 取任务行（字段级断言用）。
func loadTask(t *testing.T, db *gorm.DB, taskID string) inflightRow {
	t.Helper()
	var row inflightRow
	if err := db.Raw(`SELECT status, attempt, event_seq, transcript, summary_json, error_code,
		error_message, started_at, finished_at FROM tasks WHERE id = ?`, taskID).Scan(&row).Error; err != nil {
		t.Fatalf("查询任务 %s 失败: %v", taskID, err)
	}
	return row
}

// recoveredDetails 解析 task_recovered / task_interrupted 的 details（详设 §7.1：
// details 记 previous_attempt；原状态一并入 details）。
func recoveredDetails(t *testing.T, db *gorm.DB, taskID, event string) map[string]any {
	t.Helper()
	var raw string
	if err := db.Raw(`SELECT details FROM task_events WHERE task_id = ? AND event = ?`,
		taskID, event).Scan(&raw).Error; err != nil {
		t.Fatalf("查询 %s details 失败: %v", event, err)
	}
	var d map[string]any
	if err := json.Unmarshal([]byte(raw), &d); err != nil {
		t.Fatalf("details 不是合法 JSON: %v, raw=%q", err, raw)
	}
	return d
}

// TestIT10_ResetInFlight（IT-10，详设 §6.1/§6.2 步骤 5）：预置 transcribing /
// summarizing（含崩溃残留产物与错误）→ 恢复 → pending、attempt+1、产物/错误/时间
// 清空、task_recovered 事件 details.previous_attempt 正确；pending 保留、done/failed
// 不动；重复执行幂等（第二次 0 行、事件不重复）。
func TestIT10_ResetInFlight(t *testing.T) {
	db := RequireTestDB(t)
	p := newRecoverService(t, db, t.TempDir(), "reset")

	_, transcribingID := insertTaskState(t, db, 1, "transcribing", false)
	_, summarizingID := insertTaskState(t, db, 2, "summarizing", false)
	_, pendingID := insertPendingTask(t, db, 3, false)
	_, doneID := insertTaskState(t, db, 4, "done", false)
	_, failedID := insertTaskState(t, db, 5, "failed", false)
	// 崩溃残留：在途行上的错误与时间残渣（§6.1「清空 transcript/summary/error/本轮时间字段」）。
	if err := db.Exec(`UPDATE tasks SET error_code = 40001, error_message = '崩溃前残留',
		finished_at = UTC_TIMESTAMP() WHERE id IN (?, ?)`, transcribingID, summarizingID).Error; err != nil {
		t.Fatalf("预置残留失败: %v", err)
	}

	n, err := p.rec.ResetInFlight(context.Background())
	if err != nil {
		t.Fatalf("ResetInFlight 失败: %v", err)
	}
	if n != 2 {
		t.Errorf("受影响任务数 = %d, want 2", n)
	}

	for _, tc := range []struct {
		taskID, previous string
	}{
		{transcribingID, "transcribing"},
		{summarizingID, "summarizing"},
	} {
		row := loadTask(t, db, tc.taskID)
		if row.Status != "pending" {
			t.Errorf("任务 %s status = %q, want pending", tc.taskID, row.Status)
		}
		if row.Attempt != 2 {
			t.Errorf("任务 %s attempt = %d, want 2（§6.2 步骤 5 attempt+1）", tc.taskID, row.Attempt)
		}
		if row.Transcript != nil && *row.Transcript != "" {
			t.Errorf("任务 %s transcript 未清空: %q", tc.taskID, *row.Transcript)
		}
		if row.SummaryJSON != nil {
			t.Errorf("任务 %s summary_json 未清空: %q", tc.taskID, *row.SummaryJSON)
		}
		if row.ErrorCode != nil {
			t.Errorf("任务 %s error_code 未清空: %d", tc.taskID, *row.ErrorCode)
		}
		if row.ErrorMessage != nil && *row.ErrorMessage != "" {
			t.Errorf("任务 %s error_message 未清空: %q", tc.taskID, *row.ErrorMessage)
		}
		if row.StartedAt != nil || row.FinishedAt != nil {
			t.Errorf("任务 %s started/finished 未清空: %v / %v", tc.taskID, row.StartedAt, row.FinishedAt)
		}
		// task_recovered：details 记 previous_attempt 与原状态（任务简报交付接口），
		// event_seq 相应递增（种子链止于 2 → 3）。
		d := recoveredDetails(t, db, tc.taskID, "task_recovered")
		if got, _ := d["previous_attempt"].(float64); got != 1 {
			t.Errorf("任务 %s details.previous_attempt = %v, want 1", tc.taskID, d["previous_attempt"])
		}
		if got, _ := d["previous_status"].(string); got != tc.previous {
			t.Errorf("任务 %s details.previous_status = %q, want %q", tc.taskID, got, tc.previous)
		}
		if row.EventSeq != 3 {
			t.Errorf("任务 %s event_seq = %d, want 3（task_recovered 递增）", tc.taskID, row.EventSeq)
		}
		var evt struct {
			FromStatus *string `gorm:"column:from_status"`
			ToStatus   *string `gorm:"column:to_status"`
			Attempt    int     `gorm:"column:attempt"`
			Level      string  `gorm:"column:level"`
		}
		if err := db.Raw(`SELECT from_status, to_status, attempt, level FROM task_events
			WHERE task_id = ? AND event = 'task_recovered'`, tc.taskID).Scan(&evt).Error; err != nil {
			t.Fatalf("查询 task_recovered 失败: %v", err)
		}
		if evt.FromStatus == nil || *evt.FromStatus != tc.previous || evt.ToStatus == nil || *evt.ToStatus != "pending" {
			t.Errorf("任务 %s task_recovered from/to = %v→%v, want %s→pending", tc.taskID, evt.FromStatus, evt.ToStatus, tc.previous)
		}
		if evt.Attempt != 2 {
			t.Errorf("任务 %s task_recovered attempt = %d, want 2（新轮次）", tc.taskID, evt.Attempt)
		}
		if evt.Level != "INFO" {
			t.Errorf("任务 %s task_recovered level = %q, want INFO", tc.taskID, evt.Level)
		}
	}

	// pending 保留、done/failed 不动（§6.4：pending 不因恢复丢失，终态不重跑）。
	for _, id := range []string{pendingID, doneID, failedID} {
		row := loadTask(t, db, id)
		if row.Attempt != 1 || row.EventSeq != 2 && row.EventSeq != 1 {
			t.Errorf("任务 %s 被恢复改动: attempt=%d event_seq=%d", id, row.Attempt, row.EventSeq)
		}
		var recovered int64
		if err := db.Raw(`SELECT COUNT(*) FROM task_events WHERE task_id = ? AND event IN ('task_recovered','task_interrupted')`,
			id).Scan(&recovered).Error; err != nil {
			t.Fatalf("查询恢复事件失败: %v", err)
		}
		if recovered != 0 {
			t.Errorf("非在途任务 %s 出现恢复事件 ×%d", id, recovered)
		}
	}
	pendingRow := loadTask(t, db, pendingID)
	if pendingRow.Status != "pending" {
		t.Errorf("pending 任务 status = %q, want pending", pendingRow.Status)
	}
	doneRow := loadTask(t, db, doneID)
	if doneRow.Status != "done" {
		t.Errorf("done 任务 status = %q, want done", doneRow.Status)
	}

	// 幂等（§6.2 防御性状态条件）：第二次执行无在途 → 0 行、事件数不变。
	n2, err := p.rec.ResetInFlight(context.Background())
	if err != nil {
		t.Fatalf("第二次 ResetInFlight 失败: %v", err)
	}
	if n2 != 0 {
		t.Errorf("第二次受影响任务数 = %d, want 0", n2)
	}
	var events int64
	if err := db.Raw(`SELECT COUNT(*) FROM task_events WHERE event = 'task_recovered'`).Scan(&events).Error; err != nil {
		t.Fatalf("统计恢复事件失败: %v", err)
	}
	if events != 2 {
		t.Errorf("task_recovered 事件数 = %d, want 2（重复执行不追加）", events)
	}
}

// TestRecovery_InterruptMode（详设 §6.3）：RECOVERY_MODE=interrupt → 在途任务不重置，
// 同事务落 failed/30003 + task_interrupted；attempt 不变（留给手动重试 +1）；
// pending/done 不动。
func TestRecovery_InterruptMode(t *testing.T) {
	db := RequireTestDB(t)
	p := newRecoverService(t, db, t.TempDir(), "interrupt")

	_, transcribingID := insertTaskState(t, db, 1, "transcribing", false)
	_, summarizingID := insertTaskState(t, db, 2, "summarizing", false)
	_, doneID := insertTaskState(t, db, 4, "done", false)

	n, err := p.rec.ResetInFlight(context.Background())
	if err != nil {
		t.Fatalf("ResetInFlight(interrupt) 失败: %v", err)
	}
	if n != 2 {
		t.Errorf("受影响任务数 = %d, want 2", n)
	}

	for _, id := range []string{transcribingID, summarizingID} {
		row := loadTask(t, db, id)
		if row.Status != "failed" {
			t.Errorf("任务 %s status = %q, want failed（§6.3 降级）", id, row.Status)
		}
		if row.ErrorCode == nil || *row.ErrorCode != 30003 {
			t.Errorf("任务 %s error_code = %v, want 30003（SERVICE_INTERRUPTED）", id, row.ErrorCode)
		}
		if row.ErrorMessage == nil || *row.ErrorMessage == "" {
			t.Errorf("任务 %s error_message 为空", id)
		}
		if row.Attempt != 1 {
			t.Errorf("任务 %s attempt = %d, want 1（interrupt 不递增，手动重试时 +1）", id, row.Attempt)
		}
		if row.FinishedAt == nil {
			t.Errorf("任务 %s finished_at 为空（终态应落时间）", id)
		}
		d := recoveredDetails(t, db, id, "task_interrupted")
		if got, _ := d["previous_attempt"].(float64); got != 1 {
			t.Errorf("任务 %s details.previous_attempt = %v, want 1", id, d["previous_attempt"])
		}
	}
	if row := loadTask(t, db, doneID); row.Status != "done" || row.Attempt != 1 {
		t.Errorf("done 任务被降级改动: status=%q attempt=%d", row.Status, row.Attempt)
	}
}

// TestIT11_ResumeDeletions（IT-11，详设 §6.1 删除两窗口/§6.2 步骤 4）：预置
// deleting_at + 残留文件 → 恢复后文件删除、三表清理完成。
func TestIT11_ResumeDeletions(t *testing.T) {
	db := RequireTestDB(t)
	dataDir := t.TempDir()
	p := newRecoverService(t, db, dataDir, "reset")

	recID, taskID := insertTaskState(t, db, 1, "summarizing", true)
	storagePath := "it/retry/1.wav"
	full := filepath.Join(dataDir, filepath.FromSlash(storagePath))
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatalf("建目录失败: %v", err)
	}
	if err := os.WriteFile(full, []byte("残留音频"), 0o644); err != nil {
		t.Fatalf("写残留文件失败: %v", err)
	}

	if err := p.rec.ResumeDeletions(context.Background()); err != nil {
		t.Fatalf("ResumeDeletions 失败: %v", err)
	}

	if _, err := os.Stat(full); !os.IsNotExist(err) {
		t.Errorf("残留文件未删除: %v", err)
	}
	for _, table := range []string{"recordings", "tasks", "task_events"} {
		col := "recording_id" // tasks / task_events 逻辑关联列
		if table == "recordings" {
			col = "id" // recordings 主键
		}
		var n int64
		if err := db.Raw("SELECT COUNT(*) FROM "+table+" WHERE "+col+" = ?", recID).Scan(&n).Error; err != nil {
			t.Fatalf("统计 %s 失败: %v", table, err)
		}
		if n != 0 {
			t.Errorf("%s 残留 %d 行, want 0（三表清理完成）", table, n)
		}
	}
	_ = taskID
}

// TestIT12_IntegrityInspect（IT-12，详设 §4.6/§6.2 步骤 3）：孤立任务 + 非删除中
// 缺任务录音 → Inspect 返回包装 90004 的错误（ErrDataInconsistent）、数据未被静默
// 修复；调用方（bootstrap）阻止 ready（boot 后 readyz 503、未启 worker）。
func TestIT12_IntegrityInspect(t *testing.T) {
	db := RequireTestDB(t)
	p := newRecoverService(t, db, t.TempDir(), "reset")

	// 孤立任务：recording_id 指向不存在的录音。
	orphanTask := "a0000000-0000-0000-0000-00000000dead"
	orphanRec := "b0000000-0000-0000-0000-00000000dead"
	now := time.Now().UTC()
	if err := db.Exec(`INSERT INTO tasks (id, recording_id, status, attempt, event_seq, created_request_id, created_at, updated_at)
		VALUES (?, ?, 'pending', 1, 1, 'req-it', ?, ?)`, orphanTask, orphanRec, now, now).Error; err != nil {
		t.Fatalf("预置孤立任务失败: %v", err)
	}
	// 缺任务录音：非删除中但无任务（§4.6 应用不变量违规）。
	missingRec := "b0000000-0000-0000-0000-00000000bad0"
	if err := db.Exec(`INSERT INTO recordings (id, original_filename, storage_path, extension, size_bytes, deleting_at, created_at, updated_at)
		VALUES (?, 'orphan.wav', 'orphan.wav', 'wav', 1024, NULL, ?, ?)`, missingRec, now, now).Error; err != nil {
		t.Fatalf("预置缺任务录音失败: %v", err)
	}

	err := p.rec.Inspect(context.Background())
	if !errors.Is(err, ports.ErrDataInconsistent) {
		t.Fatalf("Inspect 错误 = %v, want 包装 ErrDataInconsistent（90004）", err)
	}

	// 不静默修复：两行损坏关联原样保留（§4.6）。
	var orphanN, missingN int64
	if err := db.Raw(`SELECT COUNT(*) FROM tasks WHERE id = ?`, orphanTask).Scan(&orphanN).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Raw(`SELECT COUNT(*) FROM recordings WHERE id = ?`, missingRec).Scan(&missingN).Error; err != nil {
		t.Fatal(err)
	}
	if orphanN != 1 || missingN != 1 {
		t.Errorf("巡检静默修复了数据：orphan=%d missing=%d, want 1/1", orphanN, missingN)
	}

	// 调用方阻止 ready（§6.2 Block 分支）：boot 后不就绪、不启 worker、不接收上传。
	app, baseURL := bootAppOn(t, db, time.Second, 0)
	if app.Ready() {
		t.Error("巡检失败后仍就绪，want 阻止 ready（90004）")
	}
	if code := readyzCode(t, baseURL); code != http.StatusServiceUnavailable {
		t.Errorf("GET /readyz = %d, want 503", code)
	}
	// §6.2「全部完成前……不接收上传」：阻断态上传 → 503/90005，不产生录音/任务。
	var recordings, tasks int64
	if err := db.Raw(`SELECT COUNT(*) FROM recordings`).Scan(&recordings).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Raw(`SELECT COUNT(*) FROM tasks`).Scan(&tasks).Error; err != nil {
		t.Fatal(err)
	}
	upCode, upBody := postUploadOn(t, baseURL)
	if upCode != http.StatusServiceUnavailable {
		t.Errorf("阻断态上传 status = %d, want 503, body=%q", upCode, upBody)
	} else {
		var e struct {
			Error struct {
				Code int `json:"code"`
			} `json:"error"`
		}
		if err := json.Unmarshal(upBody, &e); err != nil || e.Error.Code != 90005 {
			t.Errorf("阻断态上传业务码 = %+v (err=%v), want 90005（SERVICE_NOT_READY）", e.Error.Code, err)
		}
	}
	if n := tableCount(t, db, "recordings"); n != recordings {
		t.Errorf("阻断态上传产生了录音行：%d → %d", recordings, n)
	}
	if n := tableCount(t, db, "tasks"); n != tasks {
		t.Errorf("阻断态上传产生了任务行：%d → %d", tasks, n)
	}
	start := time.Now()
	app.Lifecycle.Shutdown() // 未启池的停机应即刻收敛（不耗尽预算）
	if elapsed := time.Since(start); elapsed > 900*time.Millisecond {
		t.Errorf("未启 worker 的 Shutdown 耗时 %s（预算内未收敛）", elapsed)
	}
}

// TestRecovery_OrphanFiles（详设 §5.2/§6.2 步骤 6）：数据目录内未引用文件与 tmp-
// 临时文件被移除、被引用文件保留；引用集查询失败 → 不执行清理。
func TestRecovery_OrphanFiles(t *testing.T) {
	db := RequireTestDB(t)
	dataDir := t.TempDir()
	p := newRecoverService(t, db, dataDir, "reset")

	// 被引用文件：录音行 storage_path 指向（复用 insertPendingTask 的路径形态）。
	_, _ = insertPendingTask(t, db, 1, false)
	var refPath string
	if err := db.Raw(`SELECT storage_path FROM recordings LIMIT 1`).Scan(&refPath).Error; err != nil {
		t.Fatal(err)
	}
	write := func(name, body string) {
		t.Helper()
		full := filepath.Join(dataDir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write(refPath, "被引用音频")
	write("unreferenced.wav", "孤立文件")
	write("tmp-9f8e7d6c-0000-0000-0000-000000000000", "未完成 rename 的临时文件")

	n, err := p.rec.RemoveOrphanFiles(context.Background())
	if err != nil {
		t.Fatalf("RemoveOrphanFiles 失败: %v", err)
	}
	if n != 2 {
		t.Errorf("清理数 = %d, want 2（未引用 + tmp-）", n)
	}
	if _, err := os.Stat(filepath.Join(dataDir, filepath.FromSlash(refPath))); err != nil {
		t.Errorf("被引用文件被误删: %v", err)
	}
	for _, gone := range []string{"unreferenced.wav", "tmp-9f8e7d6c-0000-0000-0000-000000000000"} {
		if _, err := os.Stat(filepath.Join(dataDir, gone)); !os.IsNotExist(err) {
			t.Errorf("孤立文件 %s 未移除: %v", gone, err)
		}
	}

	// 查询失败不清理（§5.2）：注入 ListStoragePaths 失败 → 报错且文件不动。
	write("pending-sweep.wav", "待核对")
	failing := processing.NewRecoverService(failingPathsTx{p.procTx}, nil, p.store,
		slog.New(slog.NewTextHandler(io.Discard, nil)), false)
	n2, err := failing.RemoveOrphanFiles(context.Background())
	if err == nil {
		t.Error("查询失败未返回错误")
	}
	if n2 != 0 {
		t.Errorf("查询失败仍清理了 %d 个文件, want 0", n2)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "pending-sweep.wav")); err != nil {
		t.Errorf("查询失败路径仍执行了清理: %v", err)
	}
}

// TestRecovery_BootOrder（详设 §6.2 顺序的端到端断言）：恢复完成前不就绪、完成后
// 就绪——(a) 在途种子 → 装配返回即已重置且 readyz 200、worker 已启动认领；
// (b) 巡检失败 → readyz 503、worker 未启动（pending 不被认领）。
func TestRecovery_BootOrder(t *testing.T) {
	t.Run("恢复完成即就绪且在途已重置", func(t *testing.T) {
		db := RequireTestDB(t)
		_, taskID := insertTaskState(t, db, 1, "summarizing", false)

		app, baseURL := bootAppOn(t, db, 2*time.Second, 0)
		if !app.Ready() {
			t.Fatal("恢复完成后仍未就绪")
		}
		if code := readyzCode(t, baseURL); code != http.StatusOK {
			t.Errorf("GET /readyz = %d, want 200（恢复完成后就绪）", code)
		}
		// 就绪 ⇒ 恢复已作用：attempt=2 且 task_recovered 已写入。worker 在 NewApp 内
		// 已启动，断言时任务可能已被认领推进——不对瞬时 status 做时点断言（防 flake），
		// 不变量由 attempt 与事件表达，收敛到 done 在末尾断言。
		row := loadTask(t, db, taskID)
		if row.Attempt != 2 {
			t.Errorf("就绪时任务未完成重置: status=%q attempt=%d", row.Status, row.Attempt)
		}
		d := recoveredDetails(t, db, taskID, "task_recovered")
		if got, _ := d["previous_attempt"].(float64); got != 1 {
			t.Errorf("details.previous_attempt = %v, want 1", d["previous_attempt"])
		}
		waitForTaskStatus(t, db, taskID, "done") // 步骤 7：worker 启动后立即认领
	})

	t.Run("巡检失败不就绪且不启worker", func(t *testing.T) {
		db := RequireTestDB(t)
		// 孤立任务阻断巡检；另置 pending 种子观察「不启 worker」。
		now := time.Now().UTC()
		if err := db.Exec(`INSERT INTO tasks (id, recording_id, status, attempt, event_seq, created_request_id, created_at, updated_at)
			VALUES ('a0000000-0000-0000-0000-00000000dead', 'b0000000-0000-0000-0000-00000000dead',
			'pending', 1, 1, 'req-it', ?, ?)`, now, now).Error; err != nil {
			t.Fatal(err)
		}
		_, pendingID := insertPendingTask(t, db, 3, false)

		app, baseURL := bootAppOn(t, db, time.Second, 0)
		if app.Ready() {
			t.Error("巡检失败后仍就绪")
		}
		if code := readyzCode(t, baseURL); code != http.StatusServiceUnavailable {
			t.Errorf("GET /readyz = %d, want 503", code)
		}
		time.Sleep(200 * time.Millisecond) // 轮询 20ms：若误启 worker，pending 早被认领
		assertStatus(t, db, pendingID, "pending")
		app.Lifecycle.Shutdown()
	})
}

// bootAppOn 在已预置数据的测试库上装配真实服务（bootstrap.NewApp：T11 起装配内
// 同步执行 §6.2 启动恢复）并监听真实 TCP；返回 app 与 BaseURL。db 仅用于说明测试库
// 已由调用方准备（NewApp 自行开连接）。
func bootAppOn(t *testing.T, db *gorm.DB, shutdownTimeout, asrDelay time.Duration) (*bootstrap.App, string) {
	t.Helper()
	fake := llm.NewFake()
	fakeSrv := httptest.NewServer(fake)
	t.Cleanup(fakeSrv.Close)
	cfg := &bootstrap.Config{
		HTTPAddr:           "127.0.0.1:0",
		LogDir:             t.TempDir(),
		LogLevel:           "INFO",
		LogMaxSizeMB:       20,
		LogMaxBackups:      5,
		LogMaxAgeDays:      7,
		DataDir:            t.TempDir(),
		MysqlDSN:           os.Getenv("TEST_MYSQL_DSN"),
		UploadMaxFileBytes: itMaxFileBytes,
		UploadMaxBodyBytes: itMaxBodyBytes,
		UploadMinFreeBytes: 512 * 1024 * 1024,
		UploadReadTimeout:  10 * time.Second,
		UploadTotalTimeout: time.Minute,
		WorkerConcurrency:  1,
		TaskPollInterval:   20 * time.Millisecond,
		MockASRDelay:       asrDelay,
		LLMBaseURL:         fakeSrv.URL,
		LLMModel:           "it-model",
		LLMAPIKey:          "it-key",
		LLMTimeout:         10 * time.Second,
		DBQueryTimeout:     3 * time.Second,
		ShutdownTimeout:    shutdownTimeout,
		CleanupInterval:    time.Hour,
		RecoveryMode:       "reset",
	}
	app, err := bootstrap.NewApp(cfg)
	if err != nil {
		t.Fatalf("bootstrap.NewApp 失败: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("监听失败: %v", err)
	}
	go func() { _ = app.Server.Serve(ln) }()
	t.Cleanup(func() {
		app.Lifecycle.Shutdown() // 幂等
		_ = ln.Close()
		_ = app.Server.Close()
	})
	return app, "http://" + ln.Addr().String()
}

// readyzCode 真实 HTTP GET /readyz，返回状态码。
func readyzCode(t *testing.T, baseURL string) int {
	t.Helper()
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(baseURL + "/readyz")
	if err != nil {
		t.Fatalf("GET /readyz 失败: %v", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

// postUploadOn 真实 multipart POST /v1/recordings（阻断态上传断言用，IT-12），
// 返回状态码与响应体原文。
func postUploadOn(t *testing.T, baseURL string) (int, []byte) {
	t.Helper()
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	fw, err := w.CreateFormFile("file", "blocked.wav")
	if err != nil {
		t.Fatalf("构造 multipart 失败: %v", err)
	}
	if _, err := fw.Write(contentOf(1024)); err != nil {
		t.Fatalf("写入 multipart 内容失败: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("关闭 multipart writer 失败: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/recordings", &buf)
	if err != nil {
		t.Fatalf("构造请求失败: %v", err)
	}
	req.Header.Set("Content-Type", w.FormDataContentType())
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("上传请求失败: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("读取响应体失败: %v", err)
	}
	return resp.StatusCode, body
}

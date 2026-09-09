package integration

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"recording-transcription/internal/application/processing"
	apprec "recording-transcription/internal/application/recording"
	"recording-transcription/internal/infrastructure/asr/mock"
	"recording-transcription/internal/infrastructure/filestore/local"
	"recording-transcription/internal/infrastructure/persistence/mysql"
	"recording-transcription/internal/infrastructure/worker"
	httpapi "recording-transcription/internal/interfaces/http"
	"recording-transcription/internal/interfaces/http/handler"
)

// 测试依据：T06 步骤 3；设计依据：详设 §4.1/§4.3（认领互斥与 SKIP LOCKED 分流）、
// §4.4（条件更新与事件原子性）、§3.2（worker 控制流到 SaveT 分支）、§3.3（channel 唤醒）。
// 用例编号 IT-03 / IT-04 及流水线前半段（上传 → 认领 → Mock 转写 → 事务④）。

// PipelineHarness 真实全链路组装（上传 HTTP → 池唤醒 → 认领 → DeterministicTranscriber
// → 事务④）：T07/T08/T09 的流水线测试在此之上扩展（替换转写/摘要替身、追加桩）。
type PipelineHarness struct {
	DB      *gorm.DB
	Router  *gin.Engine
	Pool    *worker.Pool
	DataDir string
}

// NewPipelineHarness 组装并启动 3 worker 池（20ms 快轮询加速测试，唤醒语义与生产一致）；
// 转写用确定性替身（0 延迟、不失败，测试设计 §2「不等待、不碰运气」）。
// 生命周期挂 t.Cleanup：取消 runCtx 并 Stop 等待 worker 退出。
func NewPipelineHarness(t *testing.T) *PipelineHarness {
	t.Helper()
	gin.SetMode(gin.TestMode)
	db := RequireTestDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	dataDir := t.TempDir()
	store, err := local.New(dataDir, itMaxFileBytes, 512*1024*1024)
	if err != nil {
		t.Fatalf("初始化测试数据目录失败: %v", err)
	}

	procTx := mysql.NewProcessingTx(db, itInstanceID, logger)
	query := mysql.NewRecordingQuery(db)
	transcriber := &mock.DeterministicTranscriber{}
	processSvc := processing.NewProcessService(procTx, query, transcriber, worker.NewCancelTable(), logger)
	pool := worker.NewPool(3, 20*time.Millisecond, processSvc.Process)
	runCtx, cancelRun := context.WithCancel(context.Background())
	pool.Start(runCtx)
	t.Cleanup(func() {
		cancelRun()
		pool.Stop()
	})

	uploadSvc := apprec.NewUploadService(store, mysql.NewRecordingTx(db), pool, logger, itInstanceID)
	uploadHandler := handler.NewUploadHandler(uploadSvc, logger, handler.UploadLimits{
		MaxBodyBytes:    itMaxBodyBytes,
		ReadIdleTimeout: 10 * time.Second,
		TotalTimeout:    time.Minute,
	})
	queryHandler := handler.NewQueryHandler(apprec.NewQueryService(query, logger), logger)
	return &PipelineHarness{
		DB:      db,
		Router:  httpapi.New(logger, uploadHandler, queryHandler),
		Pool:    pool,
		DataDir: dataDir,
	}
}

// newClaimEnv 只测认领协议（不起 worker 池）：真实 MySQL + ProcessingTx 适配器。
func newClaimEnv(t *testing.T) (*gorm.DB, *mysql.ProcessingTxGORM) {
	t.Helper()
	db := RequireTestDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return db, mysql.NewProcessingTx(db, itInstanceID, logger)
}

// insertPendingTask 直插一对 pending 录音+任务（含 task_created 事件，event_seq=1）；
// deleting=true 时预置 deleting_at；n 区分多行（created_at 错开保证认领顺序稳定）。
// 返回 (recordingID, taskID)。
func insertPendingTask(t *testing.T, db *gorm.DB, n int, deleting bool) (string, string) {
	t.Helper()
	recID := fmt.Sprintf("b0000000-0000-0000-0000-%012d", n)
	taskID := fmt.Sprintf("a0000000-0000-0000-0000-%012d", n)
	now := time.Now().UTC()
	createdAt := now.Add(-time.Duration(100-n) * time.Second) // n 越小越早

	var deletingAt any
	if deleting {
		deletingAt = createdAt.Add(time.Second)
	}
	stmts := []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO recordings (id, original_filename, storage_path, extension, size_bytes, content_hash, deleting_at, created_at, updated_at)
		   VALUES (?, ?, ?, 'wav', 1024, ?, ?, ?, ?)`,
			[]any{recID, fmt.Sprintf("it-%d.wav", n), fmt.Sprintf("it/%d.wav", n), fmt.Sprintf("%064d", n), deletingAt, createdAt, createdAt}},
		{`INSERT INTO tasks (id, recording_id, status, attempt, event_seq, created_request_id, created_at, updated_at)
		   VALUES (?, ?, 'pending', 1, 1, 'req-it', ?, ?)`,
			[]any{taskID, recID, createdAt, createdAt}},
		{`INSERT INTO task_events (event_id, task_id, recording_id, event_seq, attempt, event, occurred_at, level, to_status, created_request_id, instance_id)
		   VALUES (?, ?, ?, 1, 1, 'task_created', ?, 'INFO', 'pending', 'req-it', ?)`,
			[]any{fmt.Sprintf("c0000000-0000-0000-0000-%012d", n), taskID, recID, createdAt, itInstanceID}},
	}
	for _, s := range stmts {
		if err := db.Exec(s.sql, s.args...).Error; err != nil {
			t.Fatalf("预置数据失败: %v (sql=%s)", err, s.sql)
		}
	}
	return recID, taskID
}

// waitForTaskStatus 轮询直到任务到达期望状态或超时。
func waitForTaskStatus(t *testing.T, db *gorm.DB, taskID, want string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var status string
		if err := db.Raw("SELECT status FROM tasks WHERE id = ?", taskID).Scan(&status).Error; err != nil {
			t.Fatalf("查询任务状态失败: %v", err)
		}
		if status == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("任务 %s 未在 5 秒内到达 %s，当前 %q", taskID, want, status)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestIT03_ConcurrentClaimMutex：2 个 worker 并发认领同一 pending → 恰好一个成功、
// 状态推进 transcribing、恰好一条 task_claimed（IT-03，详设 §4.1/§4.3）。
func TestIT03_ConcurrentClaimMutex(t *testing.T) {
	db, ptx := newClaimEnv(t)
	_, taskID := insertPendingTask(t, db, 1, false)

	const workers = 2
	start := make(chan struct{})
	results := make([]bool, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, ok, err := ptx.ClaimNext(context.Background())
			results[i] = ok && err == nil
		}(i)
	}
	close(start)
	wg.Wait()

	wins := 0
	for _, r := range results {
		if r {
			wins++
		}
	}
	if wins != 1 {
		t.Fatalf("并发认领成功数 = %d, want 1（结果 %v）", wins, results)
	}

	var status string
	if err := db.Raw("SELECT status FROM tasks WHERE id = ?", taskID).Scan(&status).Error; err != nil {
		t.Fatalf("查询任务失败: %v", err)
	}
	if status != "transcribing" {
		t.Errorf("status = %q, want transcribing", status)
	}
	if n := tableCount(t, db, "task_events"); n != 2 {
		t.Errorf("事件总数 = %d, want 2（task_created + 恰好一条 task_claimed）", n)
	}
	var claimed int64
	if err := db.Raw("SELECT COUNT(*) FROM task_events WHERE task_id = ? AND event = 'task_claimed'", taskID).Scan(&claimed).Error; err != nil {
		t.Fatalf("查询事件失败: %v", err)
	}
	if claimed != 1 {
		t.Errorf("task_claimed 事件 = %d 条, want 1", claimed)
	}
}

// TestIT04_SkipLockedDispatch：5 个 pending + 3 个并发认领者循环认领 →
// 全部被认领恰好一次、各自认领不同任务，无重复无遗漏（IT-04，详设 §4.3）。
func TestIT04_SkipLockedDispatch(t *testing.T) {
	db, ptx := newClaimEnv(t)
	want := make([]string, 0, 5)
	for n := 1; n <= 5; n++ {
		_, taskID := insertPendingTask(t, db, n, false)
		want = append(want, taskID)
	}

	const claimers = 3
	var mu sync.Mutex
	var claimed []string
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < claimers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for {
				exec, ok, err := ptx.ClaimNext(context.Background())
				if err != nil {
					t.Errorf("认领出错: %v", err)
					return
				}
				if !ok {
					return
				}
				mu.Lock()
				claimed = append(claimed, exec.ExecutionKey.TaskID)
				mu.Unlock()
			}
		}()
	}
	close(start)
	wg.Wait()

	if len(claimed) != len(want) {
		t.Fatalf("认领总数 = %d, want %d（无遗漏）: %v", len(claimed), len(want), claimed)
	}
	seen := make(map[string]int, len(claimed))
	for _, id := range claimed {
		seen[id]++
	}
	for _, id := range want {
		if seen[id] != 1 {
			t.Errorf("任务 %s 被认领 %d 次, want 1（无重复无遗漏）", id, seen[id])
		}
	}
	var pending int64
	if err := db.Raw("SELECT COUNT(*) FROM tasks WHERE status = 'pending'").Scan(&pending).Error; err != nil {
		t.Fatalf("查询失败: %v", err)
	}
	if pending != 0 {
		t.Errorf("残留 pending = %d, want 0", pending)
	}
	var events int64
	if err := db.Raw("SELECT COUNT(*) FROM task_events WHERE event = 'task_claimed'").Scan(&events).Error; err != nil {
		t.Fatalf("查询失败: %v", err)
	}
	if events != 5 {
		t.Errorf("task_claimed 总数 = %d, want 5", events)
	}
}

// TestPipeline_TranscribeToSummarizing：上传 → 池唤醒 → 认领 → 确定性转写 → 事务④：
// 任务到 summarizing、transcript 等于替身种子文本、事件链 task_created → task_claimed →
// transcription_completed（详设 §3.2 控制流到 SaveT 分支、§3.3 唤醒、§7.1 事件链）。
func TestPipeline_TranscribeToSummarizing(t *testing.T) {
	h := NewPipelineHarness(t)

	w := doUpload(t, h.Router, formPart{field: "file", filename: "pipe-meeting.wav", content: contentOf(2048)})
	if w.Code != 202 {
		t.Fatalf("status = %d, want 202, body=%q", w.Code, w.Body.String())
	}
	resp := decodeUpload(t, w)

	waitForTaskStatus(t, h.DB, resp.TaskID, "summarizing")

	var task struct {
		Transcript string
		Attempt    int
		EventSeq   int64
	}
	if err := h.DB.Raw("SELECT transcript, attempt, event_seq FROM tasks WHERE id = ?", resp.TaskID).Scan(&task).Error; err != nil {
		t.Fatalf("查询任务失败: %v", err)
	}
	if want := mock.SeedText(resp.TaskID); task.Transcript != want {
		t.Errorf("transcript = %q, want 种子文本 %q", task.Transcript, want)
	}
	if task.Attempt != 1 {
		t.Errorf("attempt = %d, want 1", task.Attempt)
	}
	if task.EventSeq != 3 {
		t.Errorf("event_seq = %d, want 3", task.EventSeq)
	}

	type evtRow struct {
		Event      string
		EventSeq   int64
		Attempt    int
		FromStatus *string
		ToStatus   string
	}
	var evts []evtRow
	if err := h.DB.Raw("SELECT event, event_seq, attempt, from_status, to_status FROM task_events WHERE task_id = ? ORDER BY event_seq", resp.TaskID).Scan(&evts).Error; err != nil {
		t.Fatalf("查询事件链失败: %v", err)
	}
	chain := []evtRow{
		{Event: "task_created", EventSeq: 1, ToStatus: "pending"},
		{Event: "task_claimed", EventSeq: 2, FromStatus: ptrStr("pending"), ToStatus: "transcribing"},
		{Event: "transcription_completed", EventSeq: 3, FromStatus: ptrStr("transcribing"), ToStatus: "summarizing"},
	}
	if len(evts) != len(chain) {
		t.Fatalf("事件链长度 = %d, want %d: %+v", len(evts), len(chain), evts)
	}
	for i, got := range evts {
		want := chain[i]
		if got.Event != want.Event || got.EventSeq != want.EventSeq || got.Attempt != 1 ||
			((want.FromStatus == nil) != (got.FromStatus == nil)) ||
			(want.FromStatus != nil && *got.FromStatus != *want.FromStatus) ||
			got.ToStatus != want.ToStatus {
			t.Errorf("事件[%d] = %+v, want %+v", i, got, want)
		}
	}
}

// TestPipeline_ClaimSkipsDeleting：预置 deleting_at 的 pending → 认领跳过：
// 不推进状态、不产生任何新事件（详设 §4.3 锁后复查、§4.1 DELETE vs 新认领）。
func TestPipeline_ClaimSkipsDeleting(t *testing.T) {
	db, ptx := newClaimEnv(t)
	_, taskID := insertPendingTask(t, db, 1, true)

	exec, ok, err := ptx.ClaimNext(context.Background())
	if ok || err != nil || exec != nil {
		t.Fatalf("ClaimNext = (%v, %v, %v), want (nil, false, nil)", exec, ok, err)
	}

	var status string
	if err := db.Raw("SELECT status FROM tasks WHERE id = ?", taskID).Scan(&status).Error; err != nil {
		t.Fatalf("查询任务失败: %v", err)
	}
	if status != "pending" {
		t.Errorf("status = %q, want pending（删除中任务留给清理）", status)
	}
	var events int64
	if err := db.Raw("SELECT COUNT(*) FROM task_events WHERE task_id = ?", taskID).Scan(&events).Error; err != nil {
		t.Fatalf("查询事件失败: %v", err)
	}
	if events != 1 {
		t.Errorf("事件数 = %d, want 1（仅预置 task_created，认领不产生事件）", events)
	}
}

// ptrStr 测试内字符串取址助手。
func ptrStr(s string) *string { return &s }

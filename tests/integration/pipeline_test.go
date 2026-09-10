package integration

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"recording-transcription/internal/application/errorcode"
	"recording-transcription/internal/application/ports"
	"recording-transcription/internal/application/processing"
	apprec "recording-transcription/internal/application/recording"
	domain "recording-transcription/internal/domain/recording"
	"recording-transcription/internal/infrastructure/asr/mock"
	"recording-transcription/internal/infrastructure/filestore/local"
	"recording-transcription/internal/infrastructure/llm"
	"recording-transcription/internal/infrastructure/logging"
	"recording-transcription/internal/infrastructure/persistence/mysql"
	"recording-transcription/internal/infrastructure/worker"
	httpapi "recording-transcription/internal/interfaces/http"
	"recording-transcription/internal/interfaces/http/handler"
)

// 测试依据：T06 步骤 3、T07 步骤 2；设计依据：详设 §4.1/§4.3（认领互斥与 SKIP LOCKED
// 分流）、§4.4（条件更新与事件原子性）、§3.2（worker 控制流）、§3.3（channel 唤醒）、
// §9（LLM 边界映射）、§7.3（事件失败整体回滚）。用例编号 IT-02 / IT-03 / IT-04 / IT-05 /
// IT-14 及流水线全链路（上传 → 认领 → 转写 → 事务④ → 摘要 → 事务⑤/③）。

// PipelineHarness 真实全链路组装（上传 HTTP → 池唤醒 → 认领 → DeterministicTranscriber
// → 事务④ → 真实 LLM 适配器 → 进程内 FakeLLM → 事务⑤/③）：T08/T09 的流水线测试在此
// 之上扩展（追加桩）。LLM 超时取 500ms：挂起分支快速触发 50001，正常分支不受影响。
type PipelineHarness struct {
	DB         *gorm.DB
	Router     *gin.Engine
	Pool       *worker.Pool
	ProcessSvc *processing.ProcessService // halted 等受控退出断言用（T10）
	DataDir    string
	LogDir     string       // WithFileLogging 时非空（app.jsonl 断言用，T09）
	Fake       *llm.FakeLLM // LLM 替身（默认 normal；IT-14 逐例切换模式）

	deleteSvc       *apprec.DeleteService
	cleanupCtx      context.Context
	cancelCleanup   context.CancelFunc
	cleanupWg       *sync.WaitGroup
	startCleanupRun sync.Once
}

// StartCleanupLoop 按需启动 50ms 低频清理循环（生产装配恒启动，详设 §10
// CLEANUP_INTERVAL 语义；harness 默认不启动——后台 Purge 会抢在预置 deleting_at 种子
// 的计数断言之前删行（TestIT07 竞态），只有真正断言清理收敛的用例显式调用）。
// 幂等；随 t.Cleanup 收口。
func (h *PipelineHarness) StartCleanupLoop() {
	h.startCleanupRun.Do(func() {
		worker.StartCleanup(h.cleanupCtx, h.cleanupWg, itCleanupInterval, h.deleteSvc.CleanupPending)
	})
}

// NewPipelineHarness 组装并启动 3 worker 池（20ms 快轮询加速测试，唤醒语义与生产一致）；
// 转写用确定性替身（0 延迟、不失败，测试设计 §2「不等待、不碰运气」），可用
// WithFailFirstASR() 切换为「每个 task_id 首次失败」（T08 重试用例）、WithFileLogging()
// 把日志写进临时目录（T09 删除用例断言文件日志镜像）。
// 生命周期挂 t.Cleanup：关闭 FakeLLM、取消 runCtx、Stop 池并等清理循环退出。
func NewPipelineHarness(t *testing.T, opts ...PipelineOpt) *PipelineHarness {
	t.Helper()
	var conf pipelineConf
	for _, o := range opts {
		o(&conf)
	}
	gin.SetMode(gin.TestMode)
	db := RequireTestDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	var logDir string
	if conf.fileLog {
		logDir = t.TempDir()
		fileLogger, err := logging.New(logging.Options{
			Dir: logDir, Level: "INFO", MaxSizeMB: 20, MaxBackups: 5, MaxAgeDays: 7,
		})
		if err != nil {
			t.Fatalf("初始化文件日志失败: %v", err)
		}
		logger = fileLogger
	}

	dataDir := t.TempDir()
	store, err := local.New(dataDir, itMaxFileBytes, 512*1024*1024)
	if err != nil {
		t.Fatalf("初始化测试数据目录失败: %v", err)
	}

	// 摘要段（T07）：真实适配器指向进程内 FakeLLM（测试设计 §2 替身设施）。
	fake := llm.NewFake()
	fakeSrv := httptest.NewServer(fake)
	t.Cleanup(fakeSrv.Close)
	summarizer := llm.New(fakeSrv.URL, "it-model", "it-key", 500*time.Millisecond, llm.DefaultMaxResponseBytes)

	var procTx ports.ProcessingTx = mysql.NewProcessingTx(db, itInstanceID, logger)
	if conf.txWrap != nil { // T10 §5.5 注入：端口边界装饰 ProcessingTx
		procTx = conf.txWrap(procTx)
	}
	query := mysql.NewRecordingQuery(db)
	transcriber := ports.Transcriber(&mock.DeterministicTranscriber{FailFirst: conf.failFirstASR})
	if conf.panicFirstASR { // T10 IT-17 注入：首个转写调用 panic
		transcriber = &panickingFirstTranscriber{inner: transcriber}
	}
	cancelTable := worker.NewCancelTable()
	processSvc := processing.NewProcessService(procTx, query, transcriber, summarizer, cancelTable, logger)
	if conf.onHalt != nil { // 须在 pool.Start 前赋值（goroutine 创建边覆盖写）
		processSvc.OnHalt = conf.onHalt
	}
	workers := conf.workers
	if workers <= 0 {
		workers = 3
	}
	pool := worker.NewPool(workers, 20*time.Millisecond, processSvc.Process)
	runCtx, cancelRun := context.WithCancel(context.Background())
	pool.Start(runCtx)

	recordingTx := mysql.NewRecordingTx(db, itInstanceID, logger)
	uploadSvc := apprec.NewUploadService(store, recordingTx, pool, logger, itInstanceID)
	uploadHandler := handler.NewUploadHandler(uploadSvc, logger, handler.UploadLimits{
		MaxBodyBytes:    itMaxBodyBytes,
		ReadIdleTimeout: 10 * time.Second,
		TotalTimeout:    time.Minute,
	})
	queryHandler := handler.NewQueryHandler(apprec.NewQueryService(query, logger), logger)
	// T08 起挂载重试接口（详设 §8.1），与生产装配同构。
	retryHandler := handler.NewRetryHandler(apprec.NewRetryService(recordingTx, pool, logger), logger)
	// T09：删除链（详设 §5.3）；低频清理循环（50ms 间隔加速收敛，语义同 §10
	// CLEANUP_INTERVAL 的生产循环）按需启动（StartCleanupLoop），ctx 随 t.Cleanup 收口。
	deleteSvc := apprec.NewDeleteService(recordingTx, query, store, cancelTable, logger, itInstanceID)
	deleteHandler := handler.NewDeleteHandler(deleteSvc, logger)
	cleanupCtx, cancelCleanup := context.WithCancel(runCtx)
	cleanupWg := &sync.WaitGroup{}
	t.Cleanup(func() {
		cancelRun()
		cancelCleanup()
		pool.Stop()
		cleanupWg.Wait()
	})
	return &PipelineHarness{
		DB:         db,
		Router:     httpapi.New(logger, uploadHandler, queryHandler, retryHandler, deleteHandler),
		Pool:       pool,
		ProcessSvc: processSvc,
		DataDir:    dataDir,
		LogDir:     logDir,
		Fake:       fake,

		deleteSvc:     deleteSvc,
		cleanupCtx:    cleanupCtx,
		cancelCleanup: cancelCleanup,
		cleanupWg:     cleanupWg,
	}
}

// itCleanupInterval 集成测试清理循环间隔（生产默认 30s，详设 §10；测试取 50ms 加速收敛）。
const itCleanupInterval = 50 * time.Millisecond

// pipelineConf / PipelineOpt 装配选项（T08 起按用例注入替身形态）。
type pipelineConf struct {
	failFirstASR  bool
	fileLog       bool
	workers       int                                         // T10 IT-17：单 worker 存活断言
	panicFirstASR bool                                        // T10 IT-17：首个转写调用 panic
	txWrap        func(ports.ProcessingTx) ports.ProcessingTx // T10 §5.5：ProcessingTx 装饰
	onHalt        func()                                      // T10 §5.5：受控退出观察器
}

// PipelineOpt 流水线测试装配选项。
type PipelineOpt func(*pipelineConf)

// WithFailFirstASR 确定性转写替身切换为「每个 task_id 首次失败、此后成功」
// （详设 §4.5：首轮 failed → 手动 retry → 新一轮成功）。
func WithFailFirstASR() PipelineOpt {
	return func(c *pipelineConf) { c.failFirstASR = true }
}

// WithFileLogging 日志写进临时目录（T09 删除用例断言 logs/app.jsonl 镜像事件）。
func WithFileLogging() PipelineOpt {
	return func(c *pipelineConf) { c.fileLog = true }
}

// WithWorkers 指定 worker 数（默认 3；IT-17 取 1：panic 击穿 worker 则后续任务无人认领）。
func WithWorkers(n int) PipelineOpt {
	return func(c *pipelineConf) { c.workers = n }
}

// WithPanickingFirstASR 转写替身首个调用 panic、此后委托确定性替身（IT-17 注入）。
func WithPanickingFirstASR() PipelineOpt {
	return func(c *pipelineConf) { c.panicFirstASR = true }
}

// WithProcessingTxWrap 在端口边界装饰 ProcessingTx（§5.5 持续落库失败注入）。
func WithProcessingTxWrap(wrap func(ports.ProcessingTx) ports.ProcessingTx) PipelineOpt {
	return func(c *pipelineConf) { c.txWrap = wrap }
}

// WithOnHalt 注入 §5.5 受控退出回调（观察 halted 触发）。
func WithOnHalt(f func()) PipelineOpt {
	return func(c *pipelineConf) { c.onHalt = f }
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

// TestPipeline_FullSuccess：确定性替身全链路（上传 → 认领 → 转写 → 事务④ → 真实适配器
// → FakeLLM normal → 事务⑤）：任务 done、transcript 为种子文本、summary_json 三字段
// 等于替身基准、事件链 4 条完整（IT-19 成功分支，详设 §3.2/§7.1/§4.4）。
// 替代 T06 的 TestPipeline_TranscribeToSummarizing：流水线接入摘要段后不再停在
// summarizing，中间态断言改由事件链还原。
func TestPipeline_FullSuccess(t *testing.T) {
	h := NewPipelineHarness(t)

	w := doUpload(t, h.Router, formPart{field: "file", filename: "pipe-meeting.wav", content: contentOf(2048)})
	if w.Code != 202 {
		t.Fatalf("status = %d, want 202, body=%q", w.Code, w.Body.String())
	}
	resp := decodeUpload(t, w)

	waitForTaskStatus(t, h.DB, resp.TaskID, "done")

	var task struct {
		Transcript  string
		SummaryJSON *string
		ErrorCode   *int
		Attempt     int
		EventSeq    int64
		FinishedAt  *time.Time
	}
	if err := h.DB.Raw(
		"SELECT transcript, summary_json, error_code, attempt, event_seq, finished_at FROM tasks WHERE id = ?",
		resp.TaskID).Scan(&task).Error; err != nil {
		t.Fatalf("查询任务失败: %v", err)
	}
	if want := mock.SeedText(resp.TaskID); task.Transcript != want {
		t.Errorf("transcript = %q, want 种子文本 %q", task.Transcript, want)
	}
	if task.SummaryJSON == nil {
		t.Fatal("done 任务缺 summary_json")
	}
	sum, err := domainParseSummary(*task.SummaryJSON)
	if err != nil {
		t.Fatalf("summary_json 不可解析: %v", err)
	}
	if !reflect.DeepEqual(sum, llm.FakeNormalSummary) {
		t.Errorf("summary = %+v, want %+v", sum, llm.FakeNormalSummary)
	}
	if task.ErrorCode != nil {
		t.Errorf("成功任务 error_code = %d, want NULL", *task.ErrorCode)
	}
	if task.Attempt != 1 {
		t.Errorf("attempt = %d, want 1", task.Attempt)
	}
	if task.EventSeq != 4 {
		t.Errorf("event_seq = %d, want 4", task.EventSeq)
	}
	if task.FinishedAt == nil {
		t.Error("done 任务 finished_at 应非空")
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
		{Event: "task_completed", EventSeq: 4, FromStatus: ptrStr("summarizing"), ToStatus: "done"},
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

// domainParseSummary 复用领域严格解析（详设 §9）解析 summary_json 列。
func domainParseSummary(raw string) (domain.Summary, error) {
	return domain.ParseSummary([]byte(raw))
}

// TestIT14_LLMBoundaryMapping（IT-14，详设 §9）：FakeLLM 依次返回挂起 / 500 / 坏 JSON /
// 缺字段 / 正常 → 任务依次 failed+50001 / failed+50002 / failed+50003 / failed+50003 /
// done+summary_json；task_failed 事件带对应 error_code。
func TestIT14_LLMBoundaryMapping(t *testing.T) {
	h := NewPipelineHarness(t)

	cases := []struct {
		name       string
		mode       string
		wantStatus string
		wantCode   int // 0 = done 无错误码
	}{
		{"挂起超时→50001", llm.FakeModeHang, "failed", int(errorcode.CodeLLMTimeout)},
		{"非2xx→50002", llm.FakeModeHTTPError, "failed", int(errorcode.CodeLLMUpstreamError)},
		{"坏JSON→50003", llm.FakeModeBadJSON, "failed", int(errorcode.CodeLLMInvalidOutput)},
		{"缺字段→50003", llm.FakeModeMissingField, "failed", int(errorcode.CodeLLMInvalidOutput)},
		{"正常→done", llm.FakeModeNormal, "done", 0},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h.Fake.SetMode(tc.mode)
			w := doUpload(t, h.Router, formPart{
				field: "file", filename: fmt.Sprintf("it14-%d.wav", i), content: contentOf(2048),
			})
			if w.Code != 202 {
				t.Fatalf("上传 status = %d, want 202, body=%q", w.Code, w.Body.String())
			}
			resp := decodeUpload(t, w)
			waitForTaskStatus(t, h.DB, resp.TaskID, tc.wantStatus)

			var task struct {
				SummaryJSON  *string
				ErrorCode    *int
				ErrorMessage string
			}
			if err := h.DB.Raw(
				"SELECT summary_json, error_code, error_message FROM tasks WHERE id = ?",
				resp.TaskID).Scan(&task).Error; err != nil {
				t.Fatalf("查询任务失败: %v", err)
			}
			if tc.wantStatus == "done" {
				if task.ErrorCode != nil {
					t.Fatalf("done 任务 error_code = %d, want NULL", *task.ErrorCode)
				}
				if task.SummaryJSON == nil {
					t.Fatal("done 任务缺 summary_json")
				}
				sum, err := domainParseSummary(*task.SummaryJSON)
				if err != nil {
					t.Fatalf("summary_json 不可解析: %v", err)
				}
				if !reflect.DeepEqual(sum, llm.FakeNormalSummary) {
					t.Errorf("summary = %+v, want %+v", sum, llm.FakeNormalSummary)
				}
				return
			}
			if task.ErrorCode == nil || *task.ErrorCode != tc.wantCode {
				t.Fatalf("error_code = %v, want %d", task.ErrorCode, tc.wantCode)
			}
			if task.SummaryJSON != nil {
				t.Errorf("failed 任务不应有 summary_json: %q", *task.SummaryJSON)
			}
			if task.ErrorMessage == "" {
				t.Error("failed 任务 error_message 不应为空")
			}
			var evt struct {
				Event     string
				ErrorCode *int
			}
			if err := h.DB.Raw(
				"SELECT event, error_code FROM task_events WHERE task_id = ? AND event_seq = (SELECT MAX(event_seq) FROM task_events WHERE task_id = ?)",
				resp.TaskID, resp.TaskID).Scan(&evt).Error; err != nil {
				t.Fatalf("查询末位事件失败: %v", err)
			}
			if evt.Event != "task_failed" || evt.ErrorCode == nil || *evt.ErrorCode != tc.wantCode {
				t.Errorf("末位事件 = %+v, want task_failed/%d", evt, tc.wantCode)
			}
		})
	}
}

// TestIT02_EventFailureRollsBack（IT-02，详设 §7.3/§4.4）：预置 (task_id, event_seq)
// 唯一键冲突事件 → 事务④/⑤事件插入失败，状态与产物全部回滚，无半条记录。
func TestIT02_EventFailureRollsBack(t *testing.T) {
	// 事务④分支：任务 transcribing（event_seq=2），事务④将分配 seq=3 → 预置冲突占位。
	db, ptx := newClaimEnv(t)
	_, taskID := insertPendingTask(t, db, 1, false)
	exec, ok, err := ptx.ClaimNext(context.Background())
	if err != nil || !ok {
		t.Fatalf("认领失败: ok=%v err=%v", ok, err)
	}
	insertConflictEvent(t, db, taskID, 3)

	if err := ptx.SaveTranscription(context.Background(), exec.ExecutionKey, "迟到正文"); err == nil {
		t.Fatal("事务④应因事件唯一键冲突失败，实际成功")
	} else if errors.Is(err, ports.ErrStaleExecution) {
		t.Fatalf("冲突应表现为事件插入错误，而非 stale: %v", err)
	}
	var task struct {
		Status     string
		Transcript string
		EventSeq   int64
	}
	if err := db.Raw("SELECT status, transcript, event_seq FROM tasks WHERE id = ?", taskID).Scan(&task).Error; err != nil {
		t.Fatalf("查询任务失败: %v", err)
	}
	if task.Status != "transcribing" || task.Transcript != "" || task.EventSeq != 2 {
		t.Errorf("事务④未整体回滚: %+v, want transcribing/空 transcript/seq=2", task)
	}
	if n := tableCount(t, db, "task_events"); n != 3 {
		t.Errorf("事件数 = %d, want 3（created+claimed+预置冲突行，无新增）", n)
	}

	// 事务⑤分支：任务推进 summarizing（event_seq=3），事务⑤将分配 seq=4 → 预置冲突占位。
	db2, ptx2 := newClaimEnv(t)
	_, taskID2 := insertPendingTask(t, db2, 1, false)
	exec2, ok, err := ptx2.ClaimNext(context.Background())
	if err != nil || !ok {
		t.Fatalf("认领失败: ok=%v err=%v", ok, err)
	}
	if err := ptx2.SaveTranscription(context.Background(), exec2.ExecutionKey, "正文"); err != nil {
		t.Fatalf("推进 summarizing 失败: %v", err)
	}
	insertConflictEvent(t, db2, taskID2, 4)

	if err := ptx2.CompleteTask(context.Background(), exec2.ExecutionKey, llm.FakeNormalSummary); err == nil {
		t.Fatal("事务⑤应因事件唯一键冲突失败，实际成功")
	} else if errors.Is(err, ports.ErrStaleExecution) {
		t.Fatalf("冲突应表现为事件插入错误，而非 stale: %v", err)
	}
	var task2 struct {
		Status      string
		Transcript  string
		SummaryJSON *string
		EventSeq    int64
	}
	if err := db2.Raw("SELECT status, transcript, summary_json, event_seq FROM tasks WHERE id = ?", taskID2).Scan(&task2).Error; err != nil {
		t.Fatalf("查询任务失败: %v", err)
	}
	if task2.Status != "summarizing" || task2.SummaryJSON != nil || task2.EventSeq != 3 || task2.Transcript != "正文" {
		t.Errorf("事务⑤未整体回滚: %+v, want summarizing/无 summary_json/seq=3/正文保留", task2)
	}
	if n := tableCount(t, db2, "task_events"); n != 4 {
		t.Errorf("事件数 = %d, want 4（created+claimed+transcription_completed+预置冲突行，无新增）", n)
	}
}

// insertConflictEvent 预置一条占用 (task_id, event_seq) 唯一键的事件行（event_id 另取，
// 不与既有行冲突），使后续同序号事件插入必然失败（IT-02 的约束触发手段）。
func insertConflictEvent(t *testing.T, db *gorm.DB, taskID string, seq int64) {
	t.Helper()
	if err := db.Exec(
		`INSERT INTO task_events (event_id, task_id, recording_id, event_seq, attempt, event, occurred_at, level, to_status, created_request_id, instance_id)
		 VALUES (?, ?, (SELECT recording_id FROM tasks WHERE id = ?), ?, 1, 'task_created', ?, 'INFO', 'pending', 'req-it', ?)`,
		fmt.Sprintf("d0000000-0000-0000-0000-%010d", seq), taskID, taskID, seq, time.Now().UTC(), itInstanceID,
	).Error; err != nil {
		t.Fatalf("预置冲突事件失败: %v", err)
	}
}

// TestIT05_StaleAttemptRejected（IT-05，详设 §4.4）：数据库侧 attempt+1（模拟重试后的
// 新一轮）后，持旧 attempt 调事务④/⑤/③ → 条件更新未命中返回 ErrStaleExecution，
// 结果丢弃不复活：状态、产物、事件均不变。
func TestIT05_StaleAttemptRejected(t *testing.T) {
	// ③/④：transcribing 下旧 attempt 写入被拒。
	db, ptx := newClaimEnv(t)
	_, taskID := insertPendingTask(t, db, 1, false)
	exec, ok, err := ptx.ClaimNext(context.Background())
	if err != nil || !ok {
		t.Fatalf("认领失败: ok=%v err=%v", ok, err)
	}
	if err := db.Exec("UPDATE tasks SET attempt = 2 WHERE id = ?", taskID).Error; err != nil {
		t.Fatalf("模拟新一轮 attempt 失败: %v", err)
	}

	if err := ptx.SaveTranscription(context.Background(), exec.ExecutionKey, "旧轮正文"); !errors.Is(err, ports.ErrStaleExecution) {
		t.Fatalf("事务④错误 = %v, want ErrStaleExecution", err)
	}
	if err := ptx.FailTask(context.Background(), exec.ExecutionKey, errorcode.CodeASRFailed, "转写失败"); !errors.Is(err, ports.ErrStaleExecution) {
		t.Fatalf("事务③错误 = %v, want ErrStaleExecution", err)
	}
	var task struct {
		Status     string
		Attempt    int
		Transcript string
		EventSeq   int64
	}
	if err := db.Raw("SELECT status, attempt, transcript, event_seq FROM tasks WHERE id = ?", taskID).Scan(&task).Error; err != nil {
		t.Fatalf("查询任务失败: %v", err)
	}
	if task.Status != "transcribing" || task.Attempt != 2 || task.Transcript != "" || task.EventSeq != 2 {
		t.Errorf("旧轮写入未静默丢弃: %+v, want transcribing/attempt=2/空 transcript/seq=2", task)
	}
	if n := tableCount(t, db, "task_events"); n != 2 {
		t.Errorf("事件数 = %d, want 2（created+claimed，无新增）", n)
	}

	// ⑤：summarizing 下旧 attempt 完成写入被拒。
	db2, ptx2 := newClaimEnv(t)
	_, taskID2 := insertPendingTask(t, db2, 1, false)
	exec2, ok, err := ptx2.ClaimNext(context.Background())
	if err != nil || !ok {
		t.Fatalf("认领失败: ok=%v err=%v", ok, err)
	}
	if err := ptx2.SaveTranscription(context.Background(), exec2.ExecutionKey, "正文"); err != nil {
		t.Fatalf("推进 summarizing 失败: %v", err)
	}
	if err := db2.Exec("UPDATE tasks SET attempt = 2 WHERE id = ?", taskID2).Error; err != nil {
		t.Fatalf("模拟新一轮 attempt 失败: %v", err)
	}

	if err := ptx2.CompleteTask(context.Background(), exec2.ExecutionKey, llm.FakeNormalSummary); !errors.Is(err, ports.ErrStaleExecution) {
		t.Fatalf("事务⑤错误 = %v, want ErrStaleExecution", err)
	}
	var task2 struct {
		Status      string
		SummaryJSON *string
		EventSeq    int64
	}
	if err := db2.Raw("SELECT status, summary_json, event_seq FROM tasks WHERE id = ?", taskID2).Scan(&task2).Error; err != nil {
		t.Fatalf("查询任务失败: %v", err)
	}
	if task2.Status != "summarizing" || task2.SummaryJSON != nil || task2.EventSeq != 3 {
		t.Errorf("旧轮完成写入未静默丢弃: %+v, want summarizing/无 summary_json/seq=3", task2)
	}
	if n := tableCount(t, db2, "task_events"); n != 3 {
		t.Errorf("事件数 = %d, want 3（created+claimed+transcription_completed，无新增）", n)
	}
}

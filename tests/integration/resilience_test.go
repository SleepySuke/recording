// T10 步骤 1（集成）：韧性用例。测试依据：IT-17、IT-18；设计依据：详设 §3.2（每轮
// 执行包裹 recover）、§3.5（两阶段退出）、§5.5（落库持续失败受控退出）。信号在进程
// 内不可靠，IT-18 经 Lifecycle.Drain/Shutdown 直接驱动两阶段转换；真实
// signal.NotifyContext 接线保持在 bootstrap.Run（main 薄层），不做进程内信号测试。
package integration

import (
	"context"
	"errors"
	"net"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"gorm.io/gorm"

	"recording-transcription/bootstrap"
	"recording-transcription/internal/application/errorcode"
	"recording-transcription/internal/application/ports"
	d "recording-transcription/internal/domain/recording"
	"recording-transcription/internal/infrastructure/llm"
)

// panickingFirstTranscriber IT-17 注入手段（测试设计 §2 替身设施）：首个 Transcribe
// 调用 panic（模拟转写阶段代码 panic），此后委托内层确定性替身——验证 recover 落
// failed/90001 后同一 worker 继续认领下一个任务。
type panickingFirstTranscriber struct {
	inner ports.Transcriber
	once  sync.Once
}

func (p *panickingFirstTranscriber) Transcribe(ctx context.Context, taskID string) (string, error) {
	p.once.Do(func() { panic("注入的转写阶段 panic（IT-17）") })
	return p.inner.Transcribe(ctx, taskID)
}

// persistFailingTx §5.5 注入手段（端口边界装饰器）：写事务③④⑤持续失败、认领只读
// 正常，模拟「外部成功但落库持续失败 → 数据库持续不可用」。
type persistFailingTx struct{ ports.ProcessingTx }

var errPersistUnavailable = errors.New("注入：数据库持续不可用（§5.5）")

func (f *persistFailingTx) SaveTranscription(context.Context, d.ExecutionKey, string) error {
	return errPersistUnavailable
}
func (f *persistFailingTx) CompleteTask(context.Context, d.ExecutionKey, d.Summary) error {
	return errPersistUnavailable
}
func (f *persistFailingTx) FailTask(context.Context, d.ExecutionKey, errorcode.ErrorCode, string) error {
	return errPersistUnavailable
}

// assertStatus 断言任务当前状态（并返回，供错误信息外排查）。
func assertStatus(t *testing.T, db *gorm.DB, taskID, want string) {
	t.Helper()
	var status string
	if err := db.Raw("SELECT status FROM tasks WHERE id = ?", taskID).Scan(&status).Error; err != nil {
		t.Fatalf("查询任务状态失败: %v", err)
	}
	if status != want {
		t.Errorf("任务 %s status = %q, want %q", taskID, status, want)
	}
}

// countStatus 统计指定状态任务数（无伪 failed 断言用）。
func countStatus(t *testing.T, db *gorm.DB, status string) int64 {
	t.Helper()
	var n int64
	if err := db.Raw("SELECT COUNT(*) FROM tasks WHERE status = ?", status).Scan(&n).Error; err != nil {
		t.Fatalf("统计 %s 失败: %v", status, err)
	}
	return n
}

// TestIT17_PanicRecovery（IT-17，详设 §3.2）：转写替身注入 panic → 任务落 failed/90001
// （含 task_failed 事件）；单 worker 存活断言——同一 worker 继续认领下一个任务并成功。
func TestIT17_PanicRecovery(t *testing.T) {
	h := NewPipelineHarness(t, WithWorkers(1), WithPanickingFirstASR())

	// 任务 A：转写阶段 panic → recover 尽力写 failed/90001。
	w := doUpload(t, h.Router, formPart{field: "file", filename: "it17-panic.wav", content: contentOf(2048)})
	if w.Code != 202 {
		t.Fatalf("上传 status = %d, want 202, body=%q", w.Code, w.Body.String())
	}
	panicTask := decodeUpload(t, w)
	waitForTaskStatus(t, h.DB, panicTask.TaskID, "failed")

	var task struct {
		ErrorCode    *int
		ErrorMessage string
	}
	if err := h.DB.Raw("SELECT error_code, error_message FROM tasks WHERE id = ?",
		panicTask.TaskID).Scan(&task).Error; err != nil {
		t.Fatalf("查询任务失败: %v", err)
	}
	if task.ErrorCode == nil || *task.ErrorCode != int(errorcode.CodeInternalError) {
		t.Errorf("error_code = %v, want 90001（未分类内部错误，§3.2）", task.ErrorCode)
	}
	if task.ErrorMessage == "" {
		t.Error("panic 任务 error_message 不应为空")
	}
	var evt struct {
		Event     string
		ErrorCode *int
	}
	if err := h.DB.Raw(
		"SELECT event, error_code FROM task_events WHERE task_id = ? AND event_seq = (SELECT MAX(event_seq) FROM task_events WHERE task_id = ?)",
		panicTask.TaskID, panicTask.TaskID).Scan(&evt).Error; err != nil {
		t.Fatalf("查询末位事件失败: %v", err)
	}
	if evt.Event != "task_failed" || evt.ErrorCode == nil || *evt.ErrorCode != int(errorcode.CodeInternalError) {
		t.Errorf("末位事件 = %+v, want task_failed/90001", evt)
	}

	// 任务 B：同一 worker（池仅 1 个）继续认领并成功——panic 未击穿 worker。
	w2 := doUpload(t, h.Router, formPart{field: "file", filename: "it17-next.wav", content: append(contentOf(2047), 'n')})
	if w2.Code != 202 {
		t.Fatalf("上传 status = %d, want 202, body=%q", w2.Code, w2.Body.String())
	}
	next := decodeUpload(t, w2)
	waitForTaskStatus(t, h.DB, next.TaskID, "done")
}

// lifecycleAppOpts IT-18 装配参数：停机预算与确定性转写延迟。
type lifecycleAppOpts struct {
	shutdownTimeout time.Duration
	asrDelay        time.Duration
}

// newLifecycleApp 真实全装配（bootstrap.NewApp：1 worker + 20ms 轮询 + 确定性转写 +
// FakeLLM 摘要），真实 TCP 监听；t.Cleanup 幂等收口。返回 app 与测试库连接。
func newLifecycleApp(t *testing.T, opts lifecycleAppOpts) (*bootstrap.App, *gorm.DB) {
	t.Helper()
	db := RequireTestDB(t)
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
		WorkerConcurrency:  1, // 单 worker：在途/未认领断言确定
		TaskPollInterval:   20 * time.Millisecond,
		MockASRDelay:       opts.asrDelay,
		LLMBaseURL:         fakeSrv.URL,
		LLMModel:           "it-model",
		LLMAPIKey:          "it-key",
		LLMTimeout:         10 * time.Second,
		DBQueryTimeout:     3 * time.Second,
		ShutdownTimeout:    opts.shutdownTimeout,
		CleanupInterval:    time.Hour, // 不让清理循环干扰断言
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
	return app, db
}

// TestIT18_GracefulShutdown（IT-18，详设 §3.5）：两阶段退出——停认领后新 pending 不被
// 领取；在途任务预算内完成或超预算强停后保持在途（无伪 failed）；退出等待有界；
// 数据库正常关闭。
func TestIT18_GracefulShutdown(t *testing.T) {
	t.Run("预算内在途完成", func(t *testing.T) {
		budget := 2 * time.Second
		app, db := newLifecycleApp(t, lifecycleAppOpts{
			shutdownTimeout: budget,
			asrDelay:        400 * time.Millisecond, // 在途转写 < 停机预算
		})
		_, taskA := insertPendingTask(t, db, 1, false)
		waitForTaskStatus(t, db, taskA, "transcribing") // 在途（转写延迟中）

		app.Lifecycle.Drain()                          // 阶段 1：停认领/停清理
		_, taskB := insertPendingTask(t, db, 2, false) // drain 后新 pending

		waitForTaskStatus(t, db, taskA, "done") // 在途任务 drain 期间继续执行至完成
		time.Sleep(150 * time.Millisecond)      // 轮询 20ms：若未停认领，B 早被领取
		assertStatus(t, db, taskB, "pending")   // 新 pending 不被认领，留待下次启动

		start := time.Now()
		app.Lifecycle.Shutdown()
		if elapsed := time.Since(start); elapsed > budget {
			t.Errorf("Shutdown 耗时 %s 超出预算 %s", elapsed, budget)
		}
		assertStatus(t, db, taskB, "pending")
		if n := countStatus(t, db, "failed"); n != 0 {
			t.Errorf("停机产生 %d 个伪 failed（§3.5 停机取消不伪造业务 failed）", n)
		}
		if !app.Lifecycle.DBClosed() {
			t.Error("停机后数据库未正常关闭")
		}
	})

	t.Run("超预算强停在途保持在途", func(t *testing.T) {
		budget := 400 * time.Millisecond
		app, db := newLifecycleApp(t, lifecycleAppOpts{
			shutdownTimeout: budget,
			asrDelay:        5 * time.Second, // 在途转写 >> 停机预算
		})
		_, taskA := insertPendingTask(t, db, 1, false)
		waitForTaskStatus(t, db, taskA, "transcribing")

		app.Lifecycle.Drain()
		start := time.Now()
		app.Lifecycle.Shutdown() // 预算耗尽 → 取消 runCtx 强停（§3.5 第 4 条）
		elapsed := time.Since(start)
		if elapsed > 3*time.Second {
			t.Errorf("Shutdown 耗时 %s，最终退出等待未收敛（预算 %s）", elapsed, budget)
		}
		// 强停取消不伪 failed：任务保持在途，留待重启恢复（T11）。
		assertStatus(t, db, taskA, "transcribing")
		if n := countStatus(t, db, "failed"); n != 0 {
			t.Errorf("强停产生 %d 个伪 failed", n)
		}
		if !app.Lifecycle.DBClosed() {
			t.Error("停机后数据库未正常关闭")
		}
	})
}

// TestResilience_DBUnavailableControlledExit（详设 §5.5）：写事务持续失败 → 有界重试
// 后停止认领并触发受控退出回调（bootstrap 接 Lifecycle）；任务保持在途、无伪状态、
// 事件链停在认领；停止认领后新 pending 不被领取。
func TestResilience_DBUnavailableControlledExit(t *testing.T) {
	halted := make(chan struct{})
	h := NewPipelineHarness(t,
		WithProcessingTxWrap(func(tx ports.ProcessingTx) ports.ProcessingTx {
			return &persistFailingTx{ProcessingTx: tx}
		}),
		WithOnHalt(func() { close(halted) }),
	)

	w := doUpload(t, h.Router, formPart{field: "file", filename: "db-down.wav", content: contentOf(2048)})
	if w.Code != 202 {
		t.Fatalf("上传 status = %d, want 202, body=%q", w.Code, w.Body.String())
	}
	resp := decodeUpload(t, w)

	select {
	case <-halted:
	case <-time.After(5 * time.Second):
		t.Fatal("落库持续失败未在重试预算内触发 §5.5 受控退出（OnHalt 未触发）")
	}
	if !h.ProcessSvc.Halted() {
		t.Error("Halted() = false, want true（落库持续失败后停止认领）")
	}

	// 不伪状态：任务保持在途、无错误码，事件链停在认领（created + claimed）。
	assertStatus(t, h.DB, resp.TaskID, "transcribing")
	var task struct{ ErrorCode *int }
	if err := h.DB.Raw("SELECT error_code FROM tasks WHERE id = ?", resp.TaskID).Scan(&task).Error; err != nil {
		t.Fatalf("查询任务失败: %v", err)
	}
	if task.ErrorCode != nil {
		t.Errorf("error_code = %d, want NULL（落库失败不得伪造失败终态）", *task.ErrorCode)
	}
	var events int64
	if err := h.DB.Raw("SELECT COUNT(*) FROM task_events WHERE task_id = ?", resp.TaskID).Scan(&events).Error; err != nil {
		t.Fatalf("查询事件失败: %v", err)
	}
	if events != 2 {
		t.Errorf("事件数 = %d, want 2（created+claimed，无伪 task_failed）", events)
	}

	// 停止认领：halt 后新 pending 不被领取（轮询 20ms，150ms 足以暴露未停）。
	_, taskB := insertPendingTask(t, h.DB, 9, false)
	time.Sleep(150 * time.Millisecond)
	assertStatus(t, h.DB, taskB, "pending")
}

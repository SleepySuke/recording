// T07 步骤 1（单元）：ProcessService 摘要阶段与完成/失败事务的应用层行为——
// 转写失败 40001 / LLM 失败分类落库 50001~50003 / 取消不落伪 failed / 成功走事务⑤ /
// 落库失败按 §5.5 有界重试并停止认领。设计依据：详设 §3.2 控制流、§3.4、§4.4、§5.5。
package unit

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"

	"recording-transcription/internal/application/errorcode"
	"recording-transcription/internal/application/ports"
	"recording-transcription/internal/application/processing"
	domain "recording-transcription/internal/domain/recording"
	"recording-transcription/internal/infrastructure/worker"
)

// ---- 测试替身（行为记录型，断言落库调用而非内部状态） ----

type stubTranscriber struct {
	text string
	err  error
}

func (s *stubTranscriber) Transcribe(context.Context, string) (string, error) {
	return s.text, s.err
}

type stubSummarizer struct {
	sum domain.Summary
	err error
}

func (s *stubSummarizer) Summarize(context.Context, string) (domain.Summary, error) {
	return s.sum, s.err
}

type stubQuery struct {
	view ports.TaskView
}

func (q *stubQuery) GetTask(context.Context, string) (ports.TaskView, error) { return q.view, nil }
func (q *stubQuery) GetRecording(context.Context, string) (ports.RecordingDetail, error) {
	return ports.RecordingDetail{}, nil
}
func (q *stubQuery) ListRecordings(context.Context, int, int) (ports.RecordingList, error) {
	return ports.RecordingList{}, nil
}

type fakeProcessingTx struct {
	mu           sync.Mutex
	claims       int // ClaimNext 成功认领次数
	saveErr      error
	completeErr  error
	saveCalls    int
	completeCnt  int
	failCnt      int
	lastFailKey  domain.ExecutionKey
	lastFailCode errorcode.ErrorCode
	lastFailMsg  string
	lastSum      domain.Summary
}

func (f *fakeProcessingTx) ClaimNext(context.Context) (*ports.ClaimedExecution, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.claims++
	if f.claims > 1 {
		return nil, false, nil
	}
	return &ports.ClaimedExecution{
		ExecutionKey: domain.ExecutionKey{TaskID: "t-0001", Attempt: 1},
		RecordingID:  "r-0001",
	}, true, nil
}

func (f *fakeProcessingTx) SaveTranscription(context.Context, domain.ExecutionKey, string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.saveCalls++
	return f.saveErr
}

func (f *fakeProcessingTx) CompleteTask(_ context.Context, _ domain.ExecutionKey, s domain.Summary) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.completeCnt++
	f.lastSum = s
	return f.completeErr
}

func (f *fakeProcessingTx) FailTask(_ context.Context, key domain.ExecutionKey, code errorcode.ErrorCode, msg string) error {
	_ = msg
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failCnt++
	f.lastFailKey = key
	f.lastFailCode = code
	f.lastFailMsg = msg
	return nil
}

// newProcessEnv 组装被测服务：固定认领 t-0001/attempt=1，复查视图匹配 transcribing。
func newProcessEnv(tr ports.Transcriber, su ports.Summarizer) (*processing.ProcessService, *fakeProcessingTx) {
	tx := &fakeProcessingTx{}
	q := &stubQuery{view: ports.TaskView{
		ID: "t-0001", RecordingID: "r-0001", Status: domain.StatusTranscribing, Attempt: 1,
	}}
	svc := processing.NewProcessService(tx, q, tr, su, worker.NewCancelTable(),
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	return svc, tx
}

// TestProcess_TranscribeFailFailsTask：转写失败 → 事务③落库 40001（详设 §4.1）。
func TestProcess_TranscribeFailFailsTask(t *testing.T) {
	svc, tx := newProcessEnv(
		&stubTranscriber{err: errors.New("mock transcription failed")},
		&stubSummarizer{},
	)
	svc.Process(context.Background())

	if tx.failCnt != 1 {
		t.Fatalf("FailTask 调用 %d 次, want 1", tx.failCnt)
	}
	if tx.lastFailCode != errorcode.CodeASRFailed {
		t.Errorf("FailTask code = %d, want 40001", tx.lastFailCode)
	}
	if tx.completeCnt != 0 {
		t.Errorf("CompleteTask 不应被调用，实际 %d 次", tx.completeCnt)
	}
	if tx.lastFailKey.Attempt != 1 {
		t.Errorf("FailTask key = %+v, want attempt 1", tx.lastFailKey)
	}
}

// TestProcess_LLMErrorMapping：LLM 失败按哨兵分类 → 50001/50002/50003，
// 未知错误归 50002（详设 §9）。
func TestProcess_LLMErrorMapping(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want errorcode.ErrorCode
	}{
		{"超时", ports.ErrLLMTimeout, errorcode.CodeLLMTimeout},
		{"非2xx", ports.ErrLLMUpstream, errorcode.CodeLLMUpstreamError},
		{"非法输出", ports.ErrLLMInvalidOutput, errorcode.CodeLLMInvalidOutput},
		{"未知错误", errors.New("boom"), errorcode.CodeLLMUpstreamError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc, tx := newProcessEnv(
				&stubTranscriber{text: "transcript"},
				&stubSummarizer{err: tc.err},
			)
			svc.Process(context.Background())

			if tx.failCnt != 1 {
				t.Fatalf("FailTask 调用 %d 次, want 1", tx.failCnt)
			}
			if tx.lastFailCode != tc.want {
				t.Errorf("FailTask code = %d, want %d", tx.lastFailCode, tc.want)
			}
		})
	}
}

// TestProcess_CancelDiscards：取消（删除/停机）不落伪 failed（详设 §3.4）。
func TestProcess_CancelDiscards(t *testing.T) {
	svc, tx := newProcessEnv(
		&stubTranscriber{text: "transcript"},
		&stubSummarizer{err: context.Canceled},
	)
	svc.Process(context.Background())

	if tx.failCnt != 0 || tx.completeCnt != 0 {
		t.Fatalf("取消后不应落库: fail=%d complete=%d", tx.failCnt, tx.completeCnt)
	}
}

// TestProcess_SuccessCompletes：全链路成功 → 事务⑤写 Summary（详设 §4.1 summarizing→done）。
func TestProcess_SuccessCompletes(t *testing.T) {
	want := domain.Summary{Summary: "s", KeyPoints: []string{"k1"}, Todos: []string{}}
	svc, tx := newProcessEnv(&stubTranscriber{text: "transcript"}, &stubSummarizer{sum: want})
	svc.Process(context.Background())

	if tx.completeCnt != 1 {
		t.Fatalf("CompleteTask 调用 %d 次, want 1", tx.completeCnt)
	}
	if tx.failCnt != 0 {
		t.Errorf("FailTask 不应被调用，实际 %d 次", tx.failCnt)
	}
	if tx.lastSum.Summary != want.Summary || len(tx.lastSum.KeyPoints) != 1 {
		t.Errorf("落库 Summary = %+v, want %+v", tx.lastSum, want)
	}
}

// TestProcess_StaleDiscarded：SaveTranscription 返回 ErrStaleExecution → 不重试、
// 不失败、不停止认领（详设 §4.4 stale 静默丢弃）。
func TestProcess_StaleDiscarded(t *testing.T) {
	svc, tx := newProcessEnv(
		&stubTranscriber{text: "transcript"},
		&stubSummarizer{},
	)
	tx.saveErr = ports.ErrStaleExecution
	svc.Process(context.Background())

	if tx.saveCalls != 1 {
		t.Fatalf("SaveTranscription 调用 %d 次, want 1（stale 不重试）", tx.saveCalls)
	}
	if tx.failCnt != 0 || tx.completeCnt != 0 {
		t.Errorf("stale 不应触发失败/完成落库: fail=%d complete=%d", tx.failCnt, tx.completeCnt)
	}
	// 未停止认领：下一轮 Process 仍会调用 ClaimNext。
	before := tx.claims
	svc.Process(context.Background())
	if tx.claims != before+1 {
		t.Errorf("stale 后不应停止认领：claims %d → %d", before, tx.claims)
	}
}

// TestProcess_PersistRetryBounds：完成落库持续失败 → 首次 + 200ms/500ms 重试共 3 次
// 后停止（§5.5 只重试写入、不重调模型），并停止认领（受控退出 T10 完善）。
func TestProcess_PersistRetryBounds(t *testing.T) {
	svc, tx := newProcessEnv(&stubTranscriber{text: "transcript"}, &stubSummarizer{})
	tx.completeErr = errors.New("db down")
	svc.Process(context.Background())

	if tx.completeCnt != 3 {
		t.Fatalf("CompleteTask 调用 %d 次, want 3（1 + 2 次重试）", tx.completeCnt)
	}
	if tx.failCnt != 0 {
		t.Errorf("落库失败不应转失败事务: fail=%d", tx.failCnt)
	}
	if !svc.Halted() {
		t.Fatal("持续落库失败后应处于停止认领状态")
	}
	before := tx.claims
	svc.Process(context.Background())
	if tx.claims != before {
		t.Errorf("停止认领后仍认领：claims %d → %d", before, tx.claims)
	}
}

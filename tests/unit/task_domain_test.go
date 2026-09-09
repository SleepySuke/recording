package unit

import (
	"errors"
	"sync"
	"testing"

	"recording-transcription/internal/domain/recording"
)

// TestUT01_LegalTransitions —— 测试依据：测试设计 UT-01；设计依据：详设 §4.1 矩阵。
// 逐条执行 11 条出边：CanTransition 覆盖 9 个 (from,to) 组合（transcribing/summarizing→pending
// 为启动恢复基线边，failed→pending 为手动重试）；另 2 条降级恢复边 transcribing/summarizing→failed
// 由 TransitionForRecovery 单独表达。每条断言 from/to/attempt 正确。
func TestUT01_LegalTransitions(t *testing.T) {
	regular := []struct{ from, to recording.TaskStatus }{
		{recording.StatusPending, recording.StatusTranscribing},
		{recording.StatusPending, recording.StatusFailed},
		{recording.StatusTranscribing, recording.StatusSummarizing},
		{recording.StatusTranscribing, recording.StatusFailed},
		{recording.StatusTranscribing, recording.StatusPending}, // 启动恢复基线
		{recording.StatusSummarizing, recording.StatusDone},
		{recording.StatusSummarizing, recording.StatusFailed},
		{recording.StatusSummarizing, recording.StatusPending}, // 启动恢复基线
		{recording.StatusFailed, recording.StatusPending},      // 手动重试
	}
	for _, tc := range regular {
		task := &recording.ProcessingTask{Status: tc.from, Attempt: 3}
		if !task.CanTransition(tc.to) {
			t.Errorf("CanTransition(%s→%s) = false, want true", tc.from, tc.to)
			continue
		}
		if err := task.Transition(3, tc.to); err != nil {
			t.Errorf("Transition(%s→%s) 返回错误: %v", tc.from, tc.to, err)
			continue
		}
		if task.Status != tc.to {
			t.Errorf("%s→%s 后 Status = %s, want %s", tc.from, tc.to, task.Status, tc.to)
		}
		if task.Attempt != 3 {
			t.Errorf("%s→%s 后 Attempt = %d, want 3（转换不改变轮次）", tc.from, tc.to, task.Attempt)
		}
	}

	// 降级恢复（12h 变体，详设 §4.1 恢复行 / §6.3）：在途状态直接判 failed
	for _, from := range []recording.TaskStatus{recording.StatusTranscribing, recording.StatusSummarizing} {
		task := &recording.ProcessingTask{Status: from, Attempt: 1}
		if err := task.TransitionForRecovery(recording.StatusFailed); err != nil {
			t.Errorf("TransitionForRecovery(%s→failed) 返回错误: %v", from, err)
			continue
		}
		if task.Status != recording.StatusFailed {
			t.Errorf("TransitionForRecovery(%s→failed) 后 Status = %s, want failed", from, task.Status)
		}
	}
}

// TestUT02_IllegalTransitions —— 测试依据：测试设计 UT-02；设计依据：详设 §4.1。
// 矩阵外出边（done 无出边、不可跨阶段、不可自环）返回 ErrInvalidTransition 且状态不变。
func TestUT02_IllegalTransitions(t *testing.T) {
	illegal := []struct{ from, to recording.TaskStatus }{
		{recording.StatusDone, recording.StatusPending},
		{recording.StatusPending, recording.StatusDone},
		{recording.StatusDone, recording.StatusFailed},
		{recording.StatusFailed, recording.StatusSummarizing},
		{recording.StatusPending, recording.StatusPending},
		{recording.StatusTranscribing, recording.StatusDone}, // 不可跨阶段
		{recording.StatusSummarizing, recording.StatusTranscribing},
	}
	for _, tc := range illegal {
		task := &recording.ProcessingTask{Status: tc.from, Attempt: 1}
		if task.CanTransition(tc.to) {
			t.Errorf("CanTransition(%s→%s) = true, want false", tc.from, tc.to)
		}
		err := task.Transition(1, tc.to)
		if !errors.Is(err, recording.ErrInvalidTransition) {
			t.Errorf("Transition(%s→%s) 错误 = %v, want ErrInvalidTransition", tc.from, tc.to, err)
		}
		if task.Status != tc.from {
			t.Errorf("被拒转换后 Status = %s, want 保持 %s", task.Status, tc.from)
		}
	}
}

// TestUT03_AttemptIsolation —— 测试依据：测试设计 UT-03；设计依据：详设 §4.4。
// 旧 attempt（N）对新 attempt（N+1）任务的转换请求被拒且状态不变；当前轮次不受误伤。
func TestUT03_AttemptIsolation(t *testing.T) {
	task := &recording.ProcessingTask{Status: recording.StatusTranscribing, Attempt: 2}

	if err := task.Transition(1, recording.StatusSummarizing); !errors.Is(err, recording.ErrAttemptMismatch) {
		t.Errorf("旧 attempt 转换错误 = %v, want ErrAttemptMismatch", err)
	}
	if task.Status != recording.StatusTranscribing || task.Attempt != 2 {
		t.Errorf("被拒后 Status=%s Attempt=%d, want transcribing/2", task.Status, task.Attempt)
	}

	if err := task.Transition(3, recording.StatusSummarizing); !errors.Is(err, recording.ErrAttemptMismatch) {
		t.Errorf("未来 attempt 转换错误 = %v, want ErrAttemptMismatch", err)
	}

	if err := task.Transition(2, recording.StatusSummarizing); err != nil || task.Status != recording.StatusSummarizing {
		t.Errorf("当前 attempt 转换失败: err=%v status=%s", err, task.Status)
	}
}

// TestUT10_EventSeqAllocation —— 测试依据：测试设计 UT-10；设计依据：详设 §7.2。
// 行锁（sync.Mutex 模拟）内并发 100 次分配：无重复、无跳号，序号恰为 1..100。
func TestUT10_EventSeqAllocation(t *testing.T) {
	const n = 100
	task := &recording.ProcessingTask{}
	var mu sync.Mutex // 模拟任务行锁：AllocateEventSeq 的契约要求在行锁内调用
	var wg sync.WaitGroup
	seqs := make([]int64, n)
	for i := range seqs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			mu.Lock()
			defer mu.Unlock()
			seqs[i] = task.AllocateEventSeq()
		}(i)
	}
	wg.Wait()

	seen := make(map[int64]bool, n)
	for _, s := range seqs {
		if s < 1 || s > n {
			t.Fatalf("event_seq %d 越界（合法区间 1..%d）", s, n)
		}
		if seen[s] {
			t.Errorf("event_seq %d 重复分配", s)
		}
		seen[s] = true
	}
	if len(seen) != n {
		t.Errorf("分配出 %d 个不同序号, want %d（存在跳号）", len(seen), n)
	}
	if task.EventSeq != n {
		t.Errorf("最终 EventSeq = %d, want %d", task.EventSeq, n)
	}
}

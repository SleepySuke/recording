package unit

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"recording-transcription/internal/infrastructure/asr/mock"
)

// 测试依据：T08 步骤 1（FailFirst 旋钮，测试设计 §2 替身设施）；设计依据：详设 §4.5
// （失败→手动重试→新一轮成功）。生产 Mock 保持 task_id 种子决定成败不变；
// FailFirst 仅作用于确定性替身：每个 task_id 首次调用失败（40001 根因）、此后恒成功，
// 支撑「首轮 failed → retry → done」用例（E2E-02 / TestRetry_FullFlow）。

// TestDeterministicTranscriber_FailFirst：FailFirst 打开后同一 task_id 首次失败
// （ErrTranscriptionFailed）、第二次成功且文本为种子值；关闭时恒成功。
func TestDeterministicTranscriber_FailFirst(t *testing.T) {
	taskA, taskB := "failfirst-task-a", "failfirst-task-b"

	d := &mock.DeterministicTranscriber{FailFirst: true}
	if _, err := d.Transcribe(context.Background(), taskA); !errors.Is(err, mock.ErrTranscriptionFailed) {
		t.Fatalf("首次转写错误 = %v, want ErrTranscriptionFailed", err)
	}
	txt, err := d.Transcribe(context.Background(), taskA)
	if err != nil {
		t.Fatalf("第二次转写不应失败: %v", err)
	}
	if txt != mock.SeedText(taskA) {
		t.Errorf("转写文本 = %q, want 种子文本 %q", txt, mock.SeedText(taskA))
	}
	// 第三次仍成功（不是「每隔一次失败」）。
	if _, err := d.Transcribe(context.Background(), taskA); err != nil {
		t.Fatalf("第三次转写不应失败: %v", err)
	}
	// 状态按 task_id 隔离：另一任务首次仍失败。
	if _, err := d.Transcribe(context.Background(), taskB); !errors.Is(err, mock.ErrTranscriptionFailed) {
		t.Fatalf("另一任务首次转写错误 = %v, want ErrTranscriptionFailed", err)
	}

	// 默认关闭：无 FailFirst 时恒成功（不影响既有用例）。
	d2 := &mock.DeterministicTranscriber{}
	if _, err := d2.Transcribe(context.Background(), taskA); err != nil {
		t.Fatalf("FailFirst 默认关闭，不应失败: %v", err)
	}
}

// TestDeterministicTranscriber_FailFirstConcurrent：多任务并发（worker 池真实并发形态，
// -race 下验证 per-task map 并发安全）：每个 task_id 恰好失败一次，其余全部成功。
func TestDeterministicTranscriber_FailFirstConcurrent(t *testing.T) {
	const tasks = 50
	const rounds = 4 // 每任务并发调用 4 次：1 次失败 + 3 次成功

	d := &mock.DeterministicTranscriber{FailFirst: true}
	failCounts := make([]int32, tasks)
	okCounts := make([]int32, tasks)

	var wg sync.WaitGroup
	for i := 0; i < tasks; i++ {
		taskID := fmt.Sprintf("concurrent-task-%d", i)
		for r := 0; r < rounds; r++ {
			wg.Add(1)
			go func(i int, taskID string) {
				defer wg.Done()
				if _, err := d.Transcribe(context.Background(), taskID); err != nil {
					if !errors.Is(err, mock.ErrTranscriptionFailed) {
						t.Errorf("非预期错误: %v", err)
						return
					}
					atomic.AddInt32(&failCounts[i], 1)
					return
				}
				atomic.AddInt32(&okCounts[i], 1)
			}(i, taskID)
		}
	}
	wg.Wait()

	for i := 0; i < tasks; i++ {
		if f, o := failCounts[i], okCounts[i]; f != 1 || o != rounds-1 {
			t.Errorf("task[%d] 失败 %d 次 / 成功 %d 次, want 1 / %d", i, f, o, rounds-1)
		}
	}
}

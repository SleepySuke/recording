package mock

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// DeterministicTranscriber 测试替身（测试设计 §2）：以 task_id 种子决定延迟/成败/文本，
// 同 task_id 结果可复现。Delay 默认 0（测试不等待）；Fail 默认 false（不碰运气），
// 需要覆盖失败分支的用例显式打开，成败仍由种子决定而非随机。
type DeterministicTranscriber struct {
	Delay     time.Duration // 每次转写前的可取消等待，默认 0
	Fail      bool          // true 时按种子 ~20% 失败；默认恒成功
	FailFirst bool          // true 时每个 task_id 首次调用失败、此后恒成功（详设 §4.5 重试用例）

	// FailFirst 的 per-task 状态（首次已失败集合）；池内多 worker 并发调用，须加锁。
	// 结构体经字面量构造（bootstrap/wire.go 与测试），无构造函数，map 惰性初始化。
	mu         sync.Mutex
	failedOnce map[string]bool
}

// Transcribe 执行一次确定性转写。
func (d *DeterministicTranscriber) Transcribe(ctx context.Context, taskID string) (string, error) {
	if err := wait(ctx, d.Delay); err != nil {
		return "", err
	}
	if d.failFirstNow(taskID) {
		return "", fmt.Errorf("%w: task %s", ErrTranscriptionFailed, taskID)
	}
	if d.Fail && seed(taskID)%5 == 0 {
		return "", fmt.Errorf("%w: task %s", ErrTranscriptionFailed, taskID)
	}
	return SeedText(taskID), nil
}

// failFirstNow FailFirst 旋钮（T08 / 测试设计 §2）：该 task_id 的首次调用返回 true
// （应失败，40001 根因），此后恒 false——支撑「首轮 failed → 手动 retry → 新一轮成功」
// 用例（E2E-02、TestRetry_FullFlow）。生产 Mock 的种子成败语义不受影响。
func (d *DeterministicTranscriber) failFirstNow(taskID string) bool {
	if !d.FailFirst {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.failedOnce == nil {
		d.failedOnce = make(map[string]bool)
	}
	if d.failedOnce[taskID] {
		return false
	}
	d.failedOnce[taskID] = true
	return true
}

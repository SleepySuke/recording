package mock

import (
	"context"
	"fmt"
	"time"
)

// DeterministicTranscriber 测试替身（测试设计 §2）：以 task_id 种子决定延迟/成败/文本，
// 同 task_id 结果可复现。Delay 默认 0（测试不等待）；Fail 默认 false（不碰运气），
// 需要覆盖失败分支的用例显式打开，成败仍由种子决定而非随机。
type DeterministicTranscriber struct {
	Delay time.Duration // 每次转写前的可取消等待，默认 0
	Fail  bool          // true 时按种子 ~20% 失败；默认恒成功
}

// Transcribe 执行一次确定性转写。
func (d *DeterministicTranscriber) Transcribe(ctx context.Context, taskID string) (string, error) {
	if err := wait(ctx, d.Delay); err != nil {
		return "", err
	}
	if d.Fail && seed(taskID)%5 == 0 {
		return "", fmt.Errorf("%w: task %s", ErrTranscriptionFailed, taskID)
	}
	return SeedText(taskID), nil
}

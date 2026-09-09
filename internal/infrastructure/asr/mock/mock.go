// Package mock 提供转写端口的 Mock 实现（详设 §3.2）：生产替身 5～15 秒种子延迟、
// 约 20% 种子失败、timer + select ctx.Done 可取消；测试确定性替身见 deterministic.go
// （测试设计 §2：同 task_id 结果可复现，测试不等待、不碰运气）。
package mock

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"time"
)

// ErrTranscriptionFailed Mock 转写失败（应用层映射 40001/ASR_FAILED，详设 §8.4）。
var ErrTranscriptionFailed = errors.New("mock transcription failed")

// seed 由 task_id 派生确定性种子：同 task_id 的延迟/成败/文本可复现（测试设计 §2）。
func seed(taskID string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(taskID))
	return h.Sum32()
}

// SeedText 该 task_id 的确定性转写文本：生产与测试替身共用，测试可预计算断言值。
func SeedText(taskID string) string {
	return fmt.Sprintf("[mock-asr] transcript for task %s (seed=%d)", taskID, seed(taskID))
}

// Transcriber 生产 Mock（详设 §3.2/§3.4）：5～15s 种子延迟、约 20% 种子失败；
// 等待用 timer + select ctx.Done，取消立即中断，不用不可取消的 Sleep。
type Transcriber struct{}

// New 构造生产 Mock 转写器。
func New() *Transcriber { return &Transcriber{} }

// Transcribe 模拟一次转写外呼。
func (m *Transcriber) Transcribe(ctx context.Context, taskID string) (string, error) {
	h := seed(taskID)
	delay := 5*time.Second + time.Duration(h%100)*100*time.Millisecond // 5.0s～14.9s
	if err := wait(ctx, delay); err != nil {
		return "", err
	}
	if h%5 == 0 { // 种子决定，约 20% 失败
		return "", fmt.Errorf("%w: task %s", ErrTranscriptionFailed, taskID)
	}
	return SeedText(taskID), nil
}

// wait 可取消等待（详设 §3.4：Mock 延迟必须能被 ctx 取消中断）。
func wait(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err() // nil when alive
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

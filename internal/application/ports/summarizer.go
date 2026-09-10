package ports

import (
	"context"
	"errors"

	domain "recording-transcription/internal/domain/recording"
)

// LLM 失败哨兵（详设 §9）：适配器错误必须可经 errors.Is 分类，应用层据此落库
// 50001/50002/50003；父级取消（删除/停机）不在此列——适配器应透传 context.Canceled，
// 由调用方丢弃结果而不落伪 failed（详设 §3.4）。
var (
	// ErrLLMTimeout 一次 LLM 请求或读响应超时（llmCtx 截止）→ 50001。
	ErrLLMTimeout = errors.New("llm timeout")
	// ErrLLMUpstream 网络错误或非 2xx（含响应体超上限）→ 50002。
	ErrLLMUpstream = errors.New("llm upstream error")
	// ErrLLMInvalidOutput 输出不符合结构：JSON 无法解析、缺字段、类型错误、
	// 围栏、多个 JSON 值、外层结构异常 → 50003。
	ErrLLMInvalidOutput = errors.New("llm invalid output")
)

// Summarizer 摘要外呼端口（详设 §9）：输入 transcript，返回经领域严格校验的 Summary。
// 实现须用 NewRequestWithContext、复用 http.Client、限制响应体大小并及时 Close；
// 不自动重试（超时或错误明确进入 failed，由手动重试恢复）。
type Summarizer interface {
	Summarize(ctx context.Context, transcript string) (domain.Summary, error)
}

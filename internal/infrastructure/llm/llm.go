// Package llm 真实 LLM 摘要适配器（详设 §9、§3.4 llmCtx）与测试替身 FakeLLM（fake.go）。
// 渠道为 T01 已定的小米 MiMo（OpenAI 兼容 chat/completions：Bearer Key + messages 数组，
// 见 docs/plan/tasks/T01-bootstrap.md 渠道记录）。超时/非 2xx/非法输出分别包装端口哨兵
// ErrLLMTimeout/ErrLLMUpstream/ErrLLMInvalidOutput，由应用层映射 50001/50002/50003；
// 错误消息只含状态码与解析原因，不含 API Key、请求头或堆栈（详设 §9）。
package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"recording-transcription/internal/application/ports"
	domain "recording-transcription/internal/domain/recording"
)

// 编译期保证 Client 实现 Summarizer 端口。
var _ ports.Summarizer = (*Client)(nil)

// DefaultMaxResponseBytes 响应体上限（详设 §3.4/§9）：摘要响应远小于 1MiB，
// 超限视为上游异常，截断读取后不再解析。
const DefaultMaxResponseBytes = 1 << 20

// summaryPrompt 提示词（详设 §9）：要求只输出 JSON 对象、无待办返回空数组、
// 内容忠于转写文本；转写正文作为 user 消息数据传入。
const summaryPrompt = "你是会议纪要助手。阅读用户消息中的会议转写文本，输出一个 JSON 对象，" +
	"包含且仅包含三个字段：summary（非空字符串，忠实概括转写内容）、key_points（字符串数组，" +
	"元素为非空字符串的要点）、todos（字符串数组，没有待办事项时为空数组）。" +
	"只输出该 JSON 对象本身，不要 Markdown 围栏，不要任何多余文本。"

// Client OpenAI 兼容 chat/completions 摘要客户端。
type Client struct {
	base    string // 形如 https://api.xiaomimimo.com/v1，构造时去尾斜杠
	model   string
	apiKey  string
	timeout time.Duration
	maxResp int64
	hc      *http.Client // 复用连接池（详设 §3.4）
}

// New 构造摘要适配器：timeout 为单次请求（含读响应）的 llmCtx 超时（详设 §3.4）；
// maxRespBytes 传 DefaultMaxResponseBytes 或测试显式缩短。
func New(baseURL, model, apiKey string, timeout time.Duration, maxRespBytes int64) *Client {
	return &Client{
		base:    strings.TrimSuffix(baseURL, "/"),
		model:   model,
		apiKey:  apiKey,
		timeout: timeout,
		maxResp: maxRespBytes,
		hc:      &http.Client{},
	}
}

// chatRequest OpenAI 兼容请求体。response_format JSON 模式未启用：渠道结构化输出
// 能力未经真实验证（T01 步骤 2 无 Key），本地严格校验（domain.ParseSummary）兜底（§9）。
type chatRequest struct {
	Model    string        `json:"model"`
	Messages []chatMessage `json:"messages"`
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// chatResponse 只取所需字段；其余忽略。
type chatResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
}

// Summarize 调一次 LLM 并严格校验输出（详设 §9）。llmCtx = 入参 ctx（taskCtx 派生）
// + WithTimeout（§3.4）；父级取消透传 context.Canceled 供调用方丢弃结果。
func (c *Client) Summarize(ctx context.Context, transcript string) (domain.Summary, error) {
	llmCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	payload, err := json.Marshal(chatRequest{
		Model: c.model,
		Messages: []chatMessage{
			{Role: "system", Content: summaryPrompt},
			{Role: "user", Content: transcript},
		},
	})
	if err != nil {
		return domain.Summary{}, fmt.Errorf("%w: 构造请求体失败", ports.ErrLLMUpstream)
	}

	req, err := http.NewRequestWithContext(llmCtx, http.MethodPost,
		c.base+"/chat/completions", bytes.NewReader(payload))
	if err != nil {
		return domain.Summary{}, fmt.Errorf("%w: 构造请求失败", ports.ErrLLMUpstream)
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.hc.Do(req)
	if err != nil {
		// §3.4：DeadlineExceeded = LLM 超时（50001）；Canceled = 删除/停机取消，
		// 透传原始错误让调用方丢弃结果、不落伪 failed。
		if llmCtx.Err() != nil {
			return domain.Summary{}, classifyCtxErr(llmCtx.Err())
		}
		return domain.Summary{}, fmt.Errorf("%w: %v", ports.ErrLLMUpstream, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		// 只带状态码，不回显响应体（避免向错误消息泄漏上游内容，§9）。
		return domain.Summary{}, fmt.Errorf("%w: HTTP %d", ports.ErrLLMUpstream, resp.StatusCode)
	}

	// 上限 + 1 字节读取：超限即截断放弃（连接随 Close 释放，§3.4）。
	body, err := io.ReadAll(io.LimitReader(resp.Body, c.maxResp+1))
	if err != nil {
		if llmCtx.Err() != nil {
			return domain.Summary{}, classifyCtxErr(llmCtx.Err())
		}
		return domain.Summary{}, fmt.Errorf("%w: 读取响应失败: %v", ports.ErrLLMUpstream, err)
	}
	if int64(len(body)) > c.maxResp {
		return domain.Summary{}, fmt.Errorf("%w: 响应体超过 %d 字节上限", ports.ErrLLMUpstream, c.maxResp)
	}

	var chat chatResponse
	if err := json.Unmarshal(body, &chat); err != nil || len(chat.Choices) == 0 {
		return domain.Summary{}, fmt.Errorf("%w: 外层结构异常（非 chat.completions 或无 choices）", ports.ErrLLMInvalidOutput)
	}
	s, err := domain.ParseSummary([]byte(chat.Choices[0].Message.Content))
	if err != nil {
		return domain.Summary{}, fmt.Errorf("%w: %v", ports.ErrLLMInvalidOutput, err)
	}
	return s, nil
}

// classifyCtxErr 区分超时与取消（详设 §3.4）：超时包装 ErrLLMTimeout（50001），
// 取消透传 context.Canceled（调用方丢弃结果）。
func classifyCtxErr(ctxErr error) error {
	if errors.Is(ctxErr, context.DeadlineExceeded) {
		return fmt.Errorf("%w: %v", ports.ErrLLMTimeout, ctxErr)
	}
	return ctxErr // context.Canceled
}

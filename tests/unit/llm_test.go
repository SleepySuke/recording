// T07 步骤 1（单元，详设 §9/§3.4）：真实 LLM 适配器指向 FakeLLM（httptest.Server，
// 无 MySQL）。断言三字段解析、超时/非 2xx/非法输出可经 errors.Is 分类、响应体上限
// 截断且连接复用不被破坏。测试依据：测试设计 §3 UT 系列与详设 §9 必须处理清单。
package unit

import (
	"context"
	"errors"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"recording-transcription/internal/application/ports"
	domain "recording-transcription/internal/domain/recording"
	"recording-transcription/internal/infrastructure/llm"
)

// newLLMEnv 起一个 FakeLLM httptest.Server 并构造指向它的真实适配器。
func newLLMEnv(t *testing.T, timeout time.Duration) (*llm.Client, *llm.FakeLLM) {
	t.Helper()
	fake := llm.NewFake()
	srv := httptest.NewServer(fake)
	t.Cleanup(srv.Close)
	return llm.New(srv.URL, "unit-model", "unit-key", timeout, llm.DefaultMaxResponseBytes), fake
}

// TestLLM_HappyPath：正常响应 → Summary 三字段正确（IT-14 正常分支的单元前置）。
func TestLLM_HappyPath(t *testing.T) {
	c, _ := newLLMEnv(t, 2*time.Second)

	got, err := c.Summarize(context.Background(), "[mock-asr] transcript for task t1")
	if err != nil {
		t.Fatalf("Summarize 出错: %v", err)
	}
	if !reflect.DeepEqual(got, llm.FakeNormalSummary) {
		t.Errorf("Summary = %+v, want %+v", got, llm.FakeNormalSummary)
	}
}

// TestLLM_Timeout：FakeLLM 挂起超过缩短后的超时 → ErrLLMTimeout（50001，详设 §9）。
func TestLLM_Timeout(t *testing.T) {
	c, fake := newLLMEnv(t, 50*time.Millisecond)
	fake.SetMode(llm.FakeModeHang)

	_, err := c.Summarize(context.Background(), "transcript")
	if !errors.Is(err, ports.ErrLLMTimeout) {
		t.Fatalf("错误 = %v, want ErrLLMTimeout", err)
	}
}

// TestLLM_Upstream：非 2xx（500）→ ErrLLMUpstream（50002），消息不带 Key（§9）。
func TestLLM_Upstream(t *testing.T) {
	c, fake := newLLMEnv(t, 2*time.Second)
	fake.SetMode(llm.FakeModeHTTPError)

	_, err := c.Summarize(context.Background(), "transcript")
	if !errors.Is(err, ports.ErrLLMUpstream) {
		t.Fatalf("错误 = %v, want ErrLLMUpstream", err)
	}
}

// TestLLM_InvalidOutput：坏 JSON、缺字段、类型错误、围栏、多个 JSON 值、
// 外层结构异常 → 全部 ErrLLMInvalidOutput（50003，详设 §9 严格校验清单）。
func TestLLM_InvalidOutput(t *testing.T) {
	cases := []struct {
		name    string
		mode    string
		content string // 非空时直接覆盖 message.content
	}{
		{name: "坏JSON", mode: llm.FakeModeBadJSON},
		{name: "缺字段", mode: llm.FakeModeMissingField},
		{name: "类型错误", mode: "", content: `{"summary":1,"key_points":["a"],"todos":[]}`},
		{name: "Markdown围栏", mode: "", content: "```json\n{\"summary\":\"a\",\"key_points\":[],\"todos\":[]}\n```"},
		{name: "多个JSON值", mode: "", content: `{"summary":"a","key_points":[],"todos":[]} {"summary":"b","key_points":[],"todos":[]}`},
		{name: "外层结构异常", mode: llm.FakeModeBadEnvelope},
		{name: "空响应", mode: llm.FakeModeEmptyContent},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, fake := newLLMEnv(t, 2*time.Second)
			if tc.mode != "" {
				fake.SetMode(tc.mode)
			} else {
				fake.SetContent(tc.content)
			}
			_, err := c.Summarize(context.Background(), "transcript")
			if !errors.Is(err, ports.ErrLLMInvalidOutput) {
				t.Fatalf("错误 = %v, want ErrLLMInvalidOutput", err)
			}
		})
	}
}

// TestLLM_BodyCap：响应体超上限 → 报错（不解析超限体），随后同 Client 正常请求
// 成功，证明及时 Close 没有破坏连接复用（详设 §3.4）。上限取 512：4KiB 超限体
// 触发截断，正常应答（~300B）不受影响。
func TestLLM_BodyCap(t *testing.T) {
	fake := llm.NewFake()
	srv := httptest.NewServer(fake)
	t.Cleanup(srv.Close)
	c := llm.New(srv.URL, "unit-model", "unit-key", 2*time.Second, 512)

	fake.SetMode(llm.FakeModeOversize)
	if _, err := c.Summarize(context.Background(), "transcript"); !errors.Is(err, ports.ErrLLMUpstream) {
		t.Fatalf("超限响应错误 = %v, want ErrLLMUpstream", err)
	}

	fake.SetMode(llm.FakeModeNormal)
	got, err := c.Summarize(context.Background(), "transcript")
	if err != nil {
		t.Fatalf("后续请求失败（连接复用被破坏）: %v", err)
	}
	if !reflect.DeepEqual(got, llm.FakeNormalSummary) {
		t.Errorf("后续请求 Summary = %+v, want %+v", got, llm.FakeNormalSummary)
	}
}

// TestLLM_ParentCancel：父 ctx 取消（删除/停机）透传 context.Canceled 而非伪超时，
// 供调用方区分「丢弃结果」与「落库 failed/50001」（详设 §3.4）。
func TestLLM_ParentCancel(t *testing.T) {
	c, fake := newLLMEnv(t, 10*time.Second)
	fake.SetMode(llm.FakeModeHang)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()
	_, err := c.Summarize(ctx, "transcript")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("错误 = %v, want context.Canceled", err)
	}
	if errors.Is(err, ports.ErrLLMTimeout) {
		t.Fatalf("取消被误分类为超时: %v", err)
	}
}

// 编译期确认：适配器实现 Summarizer 端口；FakeNormalSummary 与领域解析一致。
func TestLLM_PortCompliance(t *testing.T) {
	var _ ports.Summarizer = llm.New("http://127.0.0.1:1", "m", "k", time.Second, llm.DefaultMaxResponseBytes)
	if _, err := domain.ParseSummary([]byte(`{"summary":"a","key_points":[],"todos":[]}`)); err != nil {
		t.Fatalf("合法空数组应通过领域校验: %v", err)
	}
}

// FakeLLM 测试替身（测试设计 §2，与 asr/mock 的 DeterministicTranscriber 同类约定：
// 生产包内导出，供 tests/ 与 e2e/ 复用）：可编程 OpenAI 兼容 /chat/completions 端点，
// 挂到 httptest.Server 由真实适配器调用。模式覆盖详设 §9 必须处理清单与 IT-14
// 边界映射：正常 / 挂起至超时 / 非 2xx / 坏 JSON / 缺字段 / 外层结构异常 / 超限响应体。
package llm

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"

	domain "recording-transcription/internal/domain/recording"
)

// 模式取值（Fake.SetMode）。
const (
	FakeModeNormal       = "normal"        // 正常：FakeNormalSummary 的确定性 JSON
	FakeModeHang         = "hang"          // 挂起至请求取消（客户端超时触发 50001）
	FakeModeHTTPError    = "http_error"    // 非 2xx（500，→ 50002）
	FakeModeBadJSON      = "bad_json"      // message.content 为坏 JSON（→ 50003）
	FakeModeMissingField = "missing_field" // content 缺 todos 字段（→ 50003）
	FakeModeBadEnvelope  = "bad_envelope"  // 外层结构异常：200 但无 choices（→ 50003）
	FakeModeOversize     = "oversize"      // 响应体超上限（→ 50002，TestLLM_BodyCap）
	FakeModeEmptyContent = "empty_content" // message.content 为空串（→ 50003）
)

// FakeNormalSummary FakeModeNormal 的期望解析结果（断言基准，测试设计 §2 可预计算）。
var FakeNormalSummary = domain.Summary{
	Summary:   "确定性摘要：转写流水线已完成收尾",
	KeyPoints: []string{"完成 LLM 摘要接入", "验证完成与失败事务原子提交"},
	Todos:     []string{}, // 没有待办时返回空数组（详设 §9）
}

// fakeContentJSON 由 FakeNormalSummary 构造 LLM 输出原文，保证内容与断言基准一致。
func fakeContentJSON() string {
	b, err := json.Marshal(struct {
		Summary   string   `json:"summary"`
		KeyPoints []string `json:"key_points"`
		Todos     []string `json:"todos"`
	}{FakeNormalSummary.Summary, FakeNormalSummary.KeyPoints, FakeNormalSummary.Todos})
	if err != nil {
		return `{"summary":"确定性摘要","key_points":[],"todos":[]}`
	}
	return string(b)
}

// fakeEnvelope 假渠道应答的 chat.completions 外层。
type fakeEnvelope struct {
	Choices []fakeChoice `json:"choices"`
}

type fakeChoice struct {
	Message fakeChatMessage `json:"message"`
}

type fakeChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// FakeLLM 模式可切换的假渠道；实现 http.Handler，并发安全。
type FakeLLM struct {
	mu      sync.Mutex
	mode    string
	content string // 非空时覆盖模式指定的 message.content（单元级非法输出注入）
}

// NewFake 构造默认 normal 模式的替身。
func NewFake() *FakeLLM { return &FakeLLM{mode: FakeModeNormal} }

// SetMode 切换响应模式。
func (f *FakeLLM) SetMode(mode string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mode = mode
	f.content = ""
}

// SetContent 直接指定 message.content（200 + chat 外层 + 该原文），
// 空串恢复按模式生成。供单元测试注入类型错误/围栏/多值等长尾非法输出。
func (f *FakeLLM) SetContent(content string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.content = content
}

func (f *FakeLLM) snapshot() (mode, content string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.mode, f.content
}

// ServeHTTP 按当前模式应答一次 chat/completions 请求。
func (f *FakeLLM) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// 先读完请求体：POST 体未读完时 net/http 无法启动后台读，也就检测不到客户端
	// 断开——挂起模式的 r.Context() 将永不取消，拖死 httptest.Server.Close。
	_, _ = io.Copy(io.Discard, r.Body)

	mode, content := f.snapshot()
	switch mode {
	case FakeModeHang:
		// 挂起到客户端断开（超时/取消），不泄漏 goroutine。
		<-r.Context().Done()
		return
	case FakeModeHTTPError:
		http.Error(w, "fake upstream unavailable", http.StatusInternalServerError)
		return
	case FakeModeBadEnvelope:
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"object":"list","data":[]}`))
		return
	case FakeModeOversize:
		// 4KiB 填充：任何合理上限都会截断（TestLLM_BodyCap 用 512 字节上限，
		// 正常应答 ~300B 不受影响）。
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"padding":"` + strings.Repeat("x", 4096) + `"}`))
		return
	}

	switch mode {
	case FakeModeBadJSON:
		content = `{"summary":"未闭合`
	case FakeModeMissingField:
		content = `{"summary":"有摘要","key_points":["一个要点"]}` // 缺 todos
	case FakeModeEmptyContent:
		content = "" // 空响应：content 显式为空串
	default: // normal 或 SetContent 覆盖（覆盖值为空串时同空响应）
		if content == "" && mode == FakeModeNormal {
			content = fakeContentJSON()
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(fakeEnvelope{Choices: []fakeChoice{{
		Message: fakeChatMessage{Role: "assistant", Content: content},
	}}})
}

package unit

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"recording-transcription/internal/interfaces/http"
)

// 测试依据：T01 步骤 4；设计依据：详设 §8.6（统一错误渲染、NoRoute/NoMethod、panic 恢复、request_id 一致）。
func newTestRouter(t *testing.T) *gin.Engine {
	t.Helper()
	gin.SetMode(gin.TestMode)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return httpapi.New(logger, nil, nil, nil)
}

type errorBody struct {
	Error struct {
		Code      int    `json:"code"`
		Message   string `json:"message"`
		RequestID string `json:"request_id"`
	} `json:"error"`
}

func decodeError(t *testing.T, w *httptest.ResponseRecorder) errorBody {
	t.Helper()
	var body errorBody
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("错误响应不是合法 JSON: %v，body=%q", err, w.Body.String())
	}
	return body
}

func TestRouter_NoRoute(t *testing.T) {
	r := newTestRouter(t)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/nope", nil))

	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", w.Code)
	}
	if body := decodeError(t, w); body.Error.Code != 10002 {
		t.Errorf("code = %d, want 10002", body.Error.Code)
	}
}

func TestRouter_NoMethod(t *testing.T) {
	r := newTestRouter(t)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/healthz", nil))

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", w.Code)
	}
	if body := decodeError(t, w); body.Error.Code != 10003 {
		t.Errorf("code = %d, want 10003", body.Error.Code)
	}
	if allow := w.Header().Get("Allow"); !strings.Contains(allow, http.MethodGet) {
		t.Errorf("Allow = %q, want contains GET", allow)
	}
}

func TestRouter_PanicRecovery(t *testing.T) {
	r := newTestRouter(t)
	r.GET("/boom", func(c *gin.Context) { panic("boom detail") })

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/boom", nil))

	if w.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", w.Code)
	}
	if body := decodeError(t, w); body.Error.Code != 90001 {
		t.Errorf("code = %d, want 90001", body.Error.Code)
	}
	if strings.Contains(w.Body.String(), "boom detail") || strings.Contains(w.Body.String(), "goroutine") {
		t.Errorf("错误响应泄漏内部细节: %q", w.Body.String())
	}
}

func TestRouter_RequestID(t *testing.T) {
	r := newTestRouter(t)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/nope", nil))

	headerID := w.Header().Get("X-Request-ID")
	if headerID == "" {
		t.Fatal("响应缺少 X-Request-ID 头")
	}
	if body := decodeError(t, w); body.Error.RequestID != headerID {
		t.Errorf("body request_id = %q, header = %q, want 一致", body.Error.RequestID, headerID)
	}
}

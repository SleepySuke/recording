package unit

import (
	"testing"

	"recording-transcription/internal/application/errorcode"
)

// TestUT08_ErrorCodeRegistry —— 测试依据：测试设计 UT-08；设计依据：详设 §8.2～§8.3。
// 断言：编号全局唯一；同步码的 HTTPStatus 与 §8.3 表逐项相等；String/Message 非空；
// 异步码（40001/50001/50002/50003/30003）不映射 HTTP 状态（返回 0）。
func TestUT08_ErrorCodeRegistry(t *testing.T) {
	httpMapping := []struct {
		code errorcode.ErrorCode
		want int // §8.3 表中的 HTTP 状态码
	}{
		{errorcode.CodeInvalidArgument, 400},
		{errorcode.CodeRouteNotFound, 404},
		{errorcode.CodeMethodNotAllowed, 405},
		{errorcode.CodeFileRequired, 400},
		{errorcode.CodeEmptyFile, 400},
		{errorcode.CodeUnsupportedExtension, 400},
		{errorcode.CodeFileTooLarge, 413},
		{errorcode.CodeRecordingNotFound, 404},
		{errorcode.CodeRecordingDeletePending, 503},
		{errorcode.CodeTaskNotFound, 404},
		{errorcode.CodeTaskNotRetryable, 409},
		{errorcode.CodeDatabaseUnavailable, 503},
		{errorcode.CodeFileStorageUnavailable, 503},
		{errorcode.CodeInternalError, 500},
		{errorcode.CodeDataInconsistent, 500},
		{errorcode.CodeServiceNotReady, 503},
	}
	asyncCodes := []errorcode.ErrorCode{
		errorcode.CodeASRFailed,
		errorcode.CodeLLMTimeout,
		errorcode.CodeLLMUpstreamError,
		errorcode.CodeLLMInvalidOutput,
		errorcode.CodeServiceInterrupted,
	}

	seen := map[errorcode.ErrorCode]bool{}
	for _, tc := range httpMapping {
		checkCode(t, seen, tc.code)
		if got := tc.code.HTTPStatus(); got != tc.want {
			t.Errorf("%d HTTPStatus() = %d, want %d", tc.code, got, tc.want)
		}
	}
	for _, c := range asyncCodes {
		checkCode(t, seen, c)
		if got := c.HTTPStatus(); got != 0 {
			t.Errorf("async code %d HTTPStatus() = %d, want 0 (不直接映射)", c, got)
		}
	}

	if got := len(errorcode.All()); got != len(httpMapping)+len(asyncCodes) {
		t.Errorf("All() 返回 %d 个码，want %d（注册表与常量集不一致）", got, len(httpMapping)+len(asyncCodes))
	}
}

func checkCode(t *testing.T, seen map[errorcode.ErrorCode]bool, c errorcode.ErrorCode) {
	t.Helper()
	if seen[c] {
		t.Errorf("编号 %d 重复注册", c)
	}
	seen[c] = true
	if c.String() == "" {
		t.Errorf("编号 %d String() 为空", c)
	}
	if c.Message() == "" {
		t.Errorf("编号 %d Message() 为空", c)
	}
}

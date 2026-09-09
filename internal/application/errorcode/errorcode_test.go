package errorcode

import "testing"

// TestUT08_ErrorCodeRegistry —— 测试依据：测试设计 UT-08；设计依据：详设 §8.2～§8.3。
// 断言：编号全局唯一；同步码的 HTTPStatus 与 §8.3 表逐项相等；String/Message 非空；
// 异步码（40001/50001/50002/50003/30003）不映射 HTTP 状态（返回 0）。
func TestUT08_ErrorCodeRegistry(t *testing.T) {
	httpMapping := []struct {
		code ErrorCode
		want int // §8.3 表中的 HTTP 状态码
	}{
		{CodeInvalidArgument, 400},
		{CodeRouteNotFound, 404},
		{CodeMethodNotAllowed, 405},
		{CodeFileRequired, 400},
		{CodeEmptyFile, 400},
		{CodeUnsupportedExtension, 400},
		{CodeFileTooLarge, 413},
		{CodeRecordingNotFound, 404},
		{CodeRecordingDeletePending, 503},
		{CodeTaskNotFound, 404},
		{CodeTaskNotRetryable, 409},
		{CodeDatabaseUnavailable, 503},
		{CodeFileStorageUnavailable, 503},
		{CodeInternalError, 500},
		{CodeDataInconsistent, 500},
	}
	asyncCodes := []ErrorCode{
		CodeASRFailed,
		CodeLLMTimeout,
		CodeLLMUpstreamError,
		CodeLLMInvalidOutput,
		CodeServiceInterrupted,
	}

	seen := map[ErrorCode]bool{}
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

	if got := len(All()); got != len(httpMapping)+len(asyncCodes) {
		t.Errorf("All() 返回 %d 个码，want %d（注册表与常量集不一致）", got, len(httpMapping)+len(asyncCodes))
	}
}

func checkCode(t *testing.T, seen map[ErrorCode]bool, c ErrorCode) {
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

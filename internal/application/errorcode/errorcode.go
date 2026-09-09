// Package errorcode 定义全局数字业务错误码注册表，供 HTTP 适配层与 worker 共用（详设 §2.4、§8.2）。
// 编号为显式常量，禁止 iota：对外编号必须稳定，不随代码顺序漂移。
package errorcode

import "fmt"

// ErrorCode 具名整数业务码（详设 §8.2）。
type ErrorCode int

const (
	CodeInvalidArgument        ErrorCode = 10001 // 参数非法
	CodeRouteNotFound          ErrorCode = 10002 // 未定义路由
	CodeMethodNotAllowed       ErrorCode = 10003 // 方法不支持
	CodeFileRequired           ErrorCode = 20001 // 缺少 file 字段
	CodeEmptyFile              ErrorCode = 20002 // 文件为空
	CodeUnsupportedExtension   ErrorCode = 20003 // 扩展名不支持
	CodeFileTooLarge           ErrorCode = 20004 // 文件超限
	CodeRecordingNotFound      ErrorCode = 20005 // 录音不存在
	CodeRecordingDeletePending ErrorCode = 20006 // 已标记删除、清理未完成
	CodeTaskNotFound           ErrorCode = 30001 // 任务不存在
	CodeTaskNotRetryable       ErrorCode = 30002 // 任务并非 failed，不允许重试
	CodeServiceInterrupted     ErrorCode = 30003 // 12h 降级恢复：在途任务被中断
	CodeASRFailed              ErrorCode = 40001 // Mock 转写失败
	CodeLLMTimeout             ErrorCode = 50001 // 摘要生成超时
	CodeLLMUpstreamError       ErrorCode = 50002 // LLM 网络或非 2xx
	CodeLLMInvalidOutput       ErrorCode = 50003 // 摘要输出不符合结构
	CodeDatabaseUnavailable    ErrorCode = 90002 // 数据库暂不可用
	CodeFileStorageUnavailable ErrorCode = 90003 // 文件落盘暂不可用
	CodeInternalError          ErrorCode = 90001 // 未分类内部错误
	CodeDataInconsistent       ErrorCode = 90004 // 逻辑关联异常
)

// AppError 应用错误：携带业务码与可选原因，由接口层映射为统一 JSON 错误响应（详设 §8.2）。
type AppError struct {
	Code  ErrorCode
	Cause error
}

func (e *AppError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %v", e.Code, e.Cause)
	}
	return e.Code.String()
}

func (e *AppError) Unwrap() error { return e.Cause }

// New 构造 AppError。
func New(code ErrorCode, cause error) *AppError { return &AppError{Code: code, Cause: cause} }

type registryEntry struct {
	httpStatus int // 0 = 异步执行码，不直接映射 HTTP 状态（详设 §8.4）
	message    string
}

var registry = map[ErrorCode]registryEntry{
	// 同步 API 错误码 —— HTTP 映射与公开消息见详设 §8.3
	CodeInvalidArgument:        {400, "请求参数非法"},
	CodeRouteNotFound:          {404, "接口不存在"},
	CodeMethodNotAllowed:       {405, "HTTP 方法不支持"},
	CodeFileRequired:           {400, "缺少 file 文件字段"},
	CodeEmptyFile:              {400, "文件为空"},
	CodeUnsupportedExtension:   {400, "不支持的文件扩展名"},
	CodeFileTooLarge:           {413, "文件大小超过上限"},
	CodeRecordingNotFound:      {404, "录音不存在"},
	CodeRecordingDeletePending: {503, "录音删除尚未完成，请稍后重试"},
	CodeTaskNotFound:           {404, "任务不存在"},
	CodeTaskNotRetryable:       {409, "仅失败任务允许重试"},
	CodeDatabaseUnavailable:    {503, "数据库暂不可用"},
	CodeFileStorageUnavailable: {503, "文件存储暂不可用"},
	CodeInternalError:          {500, "内部错误"},
	CodeDataInconsistent:       {500, "数据关联异常"},

	// 异步执行错误码 —— 只进 tasks.error_code，不映射 HTTP（详设 §8.4）
	CodeServiceInterrupted: {0, "服务重启中断了任务，请手动重试"},
	CodeASRFailed:          {0, "转写失败"},
	CodeLLMTimeout:         {0, "摘要生成超时"},
	CodeLLMUpstreamError:   {0, "摘要服务暂不可用"},
	CodeLLMInvalidOutput:   {0, "摘要结果不符合预期格式"},
}

// All 返回注册表全部业务码（供唯一性与完整性断言，UT-08）。
func All() []ErrorCode {
	codes := make([]ErrorCode, 0, len(registry))
	for c := range registry {
		codes = append(codes, c)
	}
	return codes
}

// HTTPStatus 返回同步码的 HTTP 状态；异步码返回 0 表示不直接映射（详设 §8.3/§8.4）。
func (c ErrorCode) HTTPStatus() int { return registry[c].httpStatus }

// String 返回助记名（如 INVALID_ARGUMENT）。
func (c ErrorCode) String() string { return mnemonic[c] }

// Message 返回对外公开消息（中文，可本地化；客户端只按 code 分支）。
func (c ErrorCode) Message() string { return registry[c].message }

var mnemonic = map[ErrorCode]string{
	CodeInvalidArgument:        "INVALID_ARGUMENT",
	CodeRouteNotFound:          "ROUTE_NOT_FOUND",
	CodeMethodNotAllowed:       "METHOD_NOT_ALLOWED",
	CodeFileRequired:           "FILE_REQUIRED",
	CodeEmptyFile:              "EMPTY_FILE",
	CodeUnsupportedExtension:   "UNSUPPORTED_FILE_EXTENSION",
	CodeFileTooLarge:           "FILE_TOO_LARGE",
	CodeRecordingNotFound:      "RECORDING_NOT_FOUND",
	CodeRecordingDeletePending: "RECORDING_DELETE_PENDING",
	CodeTaskNotFound:           "TASK_NOT_FOUND",
	CodeTaskNotRetryable:       "TASK_NOT_RETRYABLE",
	CodeServiceInterrupted:     "SERVICE_INTERRUPTED",
	CodeASRFailed:              "ASR_FAILED",
	CodeLLMTimeout:             "LLM_TIMEOUT",
	CodeLLMUpstreamError:       "LLM_UPSTREAM_ERROR",
	CodeLLMInvalidOutput:       "LLM_INVALID_OUTPUT",
	CodeDatabaseUnavailable:    "DATABASE_UNAVAILABLE",
	CodeFileStorageUnavailable: "FILE_STORAGE_UNAVAILABLE",
	CodeInternalError:          "INTERNAL_ERROR",
	CodeDataInconsistent:       "DATA_INCONSISTENT",
}

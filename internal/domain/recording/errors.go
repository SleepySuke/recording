package recording

import "errors"

// 领域错误（详设 §2.1）：具名 error，由应用层分类映射数字业务码；
// 消息只描述规则本身，不含内部实现细节。
var (
	// ErrInvalidTransition 非法状态转换（§4.1 矩阵外的出边）。
	ErrInvalidTransition = errors.New("不合法的状态转换")
	// ErrAttemptMismatch 执行轮次不匹配（§4.4：旧轮次迟到写入被条件更新拒绝）。
	ErrAttemptMismatch = errors.New("执行轮次不匹配")
	// ErrSummaryInvalid 摘要结构不合法（§9）。
	ErrSummaryInvalid = errors.New("摘要结构不合法")
	// ErrUnsupportedExtension 不支持的文件扩展名（§5.1）。
	ErrUnsupportedExtension = errors.New("不支持的文件扩展名")
)

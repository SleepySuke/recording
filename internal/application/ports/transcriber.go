package ports

import "context"

// Transcriber 转写外呼端口（详设 §3.2/§3.4）：实现必须响应 ctx 取消以中断等待
// （timer + select，不用不可取消的 Sleep）；失败返回非 nil error，由调用方映射 40001。
type Transcriber interface {
	Transcribe(ctx context.Context, taskID string) (transcript string, err error)
}

// Package ports 定义应用层与基础设施之间的出站端口（详设 §2.3）：
// 接口属于应用层，实现属于基础设施层；端口不暴露 *gorm.DB、*os.File等实现细节。
package ports

import (
	"context"
	"errors"
	"io"
)

// 文件存储哨兵错误：应用层据此映射数字业务码（详设 §5.1 判定树）。
var (
	// ErrEmptyFile 空文件（→ 400/20002），临时文件已由实现清理。
	ErrEmptyFile = errors.New("empty file")
	// ErrFileTooLarge 超过单文件上限或请求体总上限（→ 413/20004）。
	// 请求体读端也可返回本哨兵（同一数字码，详设 §8.3）。
	ErrFileTooLarge = errors.New("file too large")
	// ErrStorageUnavailable 落盘不可用：磁盘预检不足或写中途失败（→ 503/90003）。
	ErrStorageUnavailable = errors.New("file storage unavailable")
)

// StoredFile 一次成功落盘的结果。
type StoredFile struct {
	StoragePath string // 数据目录内相对路径（服务生成 UUID 命名，详设 §5.1）
	SizeBytes   int64  // 流式计数的实际字节数
	ContentHash string // 流式计算的 SHA-256 十六进制（详设 §5.1 去重设计）
}

// FileStore 本地文件存储端口：磁盘预检 → tmp- 写入（流式 SHA-256 + 计数，读取
// 上限 = 限额 + 1 字节）→ 同文件系统 rename；任一失败自清理临时文件（详设 §5.1）。
// 读端错误（客户端断开、请求体超限等）原样返回，不套存储哨兵。
type FileStore interface {
	Save(ctx context.Context, src io.Reader, ext string) (StoredFile, error)
	// Delete 删除文件；不存在视为成功（删除幂等清理，详设 §5.3）。
	Delete(ctx context.Context, storagePath string) error
}

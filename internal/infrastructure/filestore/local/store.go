// Package local 实现本地磁盘文件存储端口（详设 §5.1）：数据目录 + tmp- 临时文件 +
// 同文件系统 rename；全程流式（字节计数 + SHA-256），音频字节不驻留内存。
package local

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"

	"recording-transcription/internal/application/ports"
	"recording-transcription/internal/pkg/uuid"
)

// copyBufSize 单次流式拷贝块大小；仅此缓冲驻留内存。
const copyBufSize = 32 * 1024

// Store 本地文件存储：所有文件平铺在 dir 下，最终名为 UUID.ext，临时名 tmp-UUID。
type Store struct {
	dir          string
	maxFileBytes int64  // 单文件上限；读取上限 = 限额 + 1 字节（详设 §5.1）
	minFreeBytes uint64 // 磁盘预检阈值（快检，写中途失败另行兜底）
}

// New 创建数据目录并返回存储。maxFileBytes 为单文件字节上限。
func New(dir string, maxFileBytes int64, minFreeBytes uint64) (*Store, error) {
	if maxFileBytes <= 0 {
		return nil, fmt.Errorf("maxFileBytes 必须为正数: %d", maxFileBytes)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("数据目录不可创建: %w", err)
	}
	return &Store{dir: dir, maxFileBytes: maxFileBytes, minFreeBytes: minFreeBytes}, nil
}

// Save 磁盘预检 → tmp- 写入（流式 SHA-256 + 计数）→ rename；任一失败清理临时文件。
// 读端错误（含 ctx 取消）原样返回；写端/预检失败包装 ports.ErrStorageUnavailable。
func (s *Store) Save(ctx context.Context, src io.Reader, ext string) (ports.StoredFile, error) {
	if err := s.checkFreeSpace(); err != nil {
		return ports.StoredFile{}, err
	}

	tmpPath := filepath.Join(s.dir, "tmp-"+uuid.New())
	f, err := os.Create(tmpPath)
	if err != nil {
		return ports.StoredFile{}, fmt.Errorf("%w: 创建临时文件失败: %v", ports.ErrStorageUnavailable, err)
	}

	stored, err := s.streamCopy(ctx, f, src)
	if cerr := f.Close(); err == nil && cerr != nil {
		err = fmt.Errorf("%w: 关闭临时文件失败: %v", ports.ErrStorageUnavailable, cerr)
	}
	if err != nil {
		_ = os.Remove(tmpPath) // 失败自清理（详设 §5.1：不留 tmp- 残留）
		return ports.StoredFile{}, err
	}

	finalName := uuid.New() + "." + ext
	finalPath := filepath.Join(s.dir, finalName)
	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		return ports.StoredFile{}, fmt.Errorf("%w: rename 失败: %v", ports.ErrStorageUnavailable, err)
	}
	stored.StoragePath = finalName
	return stored, nil
}

// streamCopy 逐块读取并写盘、更新哈希：显式区分读端错误（原样返回，可能是客户端
// 断开或请求体超限）与写端失败（包装存储哨兵）；空文件与超限在此判定。
func (s *Store) streamCopy(ctx context.Context, f *os.File, src io.Reader) (ports.StoredFile, error) {
	hash := sha256.New()
	buf := make([]byte, copyBufSize)
	var n int64
	for {
		if err := ctx.Err(); err != nil {
			return ports.StoredFile{}, err
		}
		m, readErr := src.Read(buf)
		if m > 0 {
			if n+int64(m) > s.maxFileBytes {
				return ports.StoredFile{}, fmt.Errorf("%w: %d 字节超过上限 %d", ports.ErrFileTooLarge, n+int64(m), s.maxFileBytes)
			}
			if _, werr := f.Write(buf[:m]); werr != nil {
				return ports.StoredFile{}, fmt.Errorf("%w: 写入临时文件失败: %v", ports.ErrStorageUnavailable, werr)
			}
			_, _ = hash.Write(buf[:m])
			n += int64(m)
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return ports.StoredFile{}, readErr
		}
	}
	if n == 0 {
		return ports.StoredFile{}, ports.ErrEmptyFile
	}
	return ports.StoredFile{StoragePath: "", SizeBytes: n, ContentHash: hex.EncodeToString(hash.Sum(nil))}, nil
}

// Delete 删除数据目录内文件；不存在视为成功（详设 §5.3 幂等清理）。
func (s *Store) Delete(_ context.Context, storagePath string) error {
	if storagePath == "" {
		return nil
	}
	err := os.Remove(filepath.Join(s.dir, storagePath))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("%w: 删除 %s 失败: %v", ports.ErrStorageUnavailable, storagePath, err)
	}
	return nil
}

// checkFreeSpace 写入前磁盘空间预检（详设 §5.1：不足直接 503/90003）。
func (s *Store) checkFreeSpace() error {
	var st unix.Statfs_t
	if err := unix.Statfs(s.dir, &st); err != nil {
		return fmt.Errorf("%w: 磁盘预检失败: %v", ports.ErrStorageUnavailable, err)
	}
	if free := uint64(st.Bsize) * st.Bavail; free < s.minFreeBytes {
		return fmt.Errorf("%w: 可用空间 %d 低于阈值 %d", ports.ErrStorageUnavailable, free, s.minFreeBytes)
	}
	return nil
}

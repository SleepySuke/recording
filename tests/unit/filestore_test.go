package unit

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"recording-transcription/internal/application/ports"
	"recording-transcription/internal/infrastructure/filestore/local"
)

// 测试依据：T04 步骤 2（IT-13 的核心断言下沉为单元测试）；设计依据：详设 §5.1
// （流式读取：字节计数 + SHA-256 + tmp- 临时文件 + 同文件系统 rename；失败自清理）。

const unitMaxFileBytes = 50 * 1024 * 1024

func newTestStore(t *testing.T) (*local.Store, string) {
	t.Helper()
	dir := t.TempDir()
	s, err := local.New(dir, unitMaxFileBytes, 512*1024*1024)
	if err != nil {
		t.Fatalf("创建本地文件存储失败: %v", err)
	}
	return s, dir
}

// dirFiles 列出目录下全部文件名（断言无 tmp- 残留）。
func dirFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("读目录 %s 失败: %v", dir, err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

// TestFileStore_SaveHashAndSize：已知字节流 → 返回的 ContentHash/SizeBytes 正确、
// 最终文件内容一致、扩展名保留；Delete 幂等（不存在视为成功）。
func TestFileStore_SaveHashAndSize(t *testing.T) {
	s, dir := newTestStore(t)

	// 大小跨过内部拷贝缓冲边界（非整块）。
	pattern := []byte("0123456789abcdefghijklmnopqrstuvwxyz")
	content := bytes.Repeat(pattern, 32*1024*7/len(pattern))
	content = append(content, pattern[:1234%len(pattern)]...)

	stored, err := s.Save(context.Background(), bytes.NewReader(content), "wav")
	if err != nil {
		t.Fatalf("Save 失败: %v", err)
	}

	sum := sha256.Sum256(content)
	if want := hex.EncodeToString(sum[:]); stored.ContentHash != want {
		t.Errorf("ContentHash = %q, want %q", stored.ContentHash, want)
	}
	if stored.SizeBytes != int64(len(content)) {
		t.Errorf("SizeBytes = %d, want %d", stored.SizeBytes, len(content))
	}

	// 最终路径：相对命名 + 保留扩展名，磁盘内容与源一致，且无 tmp- 残留。
	if filepath.Ext(stored.StoragePath) != ".wav" {
		t.Errorf("存储名 %q 缺少 .wav 扩展名", stored.StoragePath)
	}
	data, err := os.ReadFile(filepath.Join(dir, stored.StoragePath))
	if err != nil {
		t.Fatalf("读取落盘文件失败: %v", err)
	}
	if !bytes.Equal(data, content) {
		t.Error("落盘内容与源字节流不一致")
	}
	for _, name := range dirFiles(t, dir) {
		if len(name) >= 4 && name[:4] == "tmp-" {
			t.Errorf("成功路径残留临时文件: %s", name)
		}
	}

	if err := s.Delete(context.Background(), stored.StoragePath); err != nil {
		t.Fatalf("Delete 失败: %v", err)
	}
	if err := s.Delete(context.Background(), stored.StoragePath); err != nil {
		t.Errorf("Delete 不存在的文件应视为成功, got %v", err)
	}
}

// errBoom 模拟客户端上传中途断开（读端错误，非写端故障）。
var errBoom = errors.New("模拟客户端中途断开")

type failingReader struct {
	prefix []byte
	sent   bool
}

func (r *failingReader) Read(p []byte) (int, error) {
	if !r.sent {
		r.sent = true
		return copy(p, r.prefix), nil
	}
	return 0, errBoom
}

// TestFileStore_SaveCleansTmpOnFailure：写入中途读错误 → 错误原样传播（读端，
// 非存储不可用哨兵）、无 tmp- 残留、无最终文件。
func TestFileStore_SaveCleansTmpOnFailure(t *testing.T) {
	s, dir := newTestStore(t)

	_, err := s.Save(context.Background(), &failingReader{prefix: []byte("partial audio bytes")}, "mp3")
	if !errors.Is(err, errBoom) {
		t.Fatalf("错误 = %v, want 读端错误原样传播", err)
	}
	if errors.Is(err, ports.ErrStorageUnavailable) {
		t.Error("读端错误不应映射为存储不可用（90003）")
	}
	if names := dirFiles(t, dir); len(names) != 0 {
		t.Errorf("失败路径残留文件: %v", names)
	}
}

// TestFileStore_SaveRejectsEmptyAndOversize：空文件与超限（读取上限 = 限额 + 1 字节）
// 各自返回可区分哨兵且清理临时文件（详设 §5.1）。
func TestFileStore_SaveRejectsEmptyAndOversize(t *testing.T) {
	s, dir := newTestStore(t)

	if _, err := s.Save(context.Background(), bytes.NewReader(nil), "wav"); !errors.Is(err, ports.ErrEmptyFile) {
		t.Errorf("空文件错误 = %v, want ErrEmptyFile", err)
	}
	if names := dirFiles(t, dir); len(names) != 0 {
		t.Errorf("空文件路径残留: %v", names)
	}

	// 单独构造限额 10 字节的存储验证 +1 边界。
	limited, err := local.New(t.TempDir(), 10, 512*1024*1024)
	if err != nil {
		t.Fatalf("创建受限存储失败: %v", err)
	}
	if _, err := limited.Save(context.Background(), bytes.NewReader(bytes.Repeat([]byte("x"), 11)), "wav"); !errors.Is(err, ports.ErrFileTooLarge) {
		t.Errorf("超 1 字节错误 = %v, want ErrFileTooLarge", err)
	}
	if _, err := limited.Save(context.Background(), bytes.NewReader(bytes.Repeat([]byte("x"), 10)), "wav"); err != nil {
		t.Errorf("恰好等于限额应通过, got %v", err)
	}
}

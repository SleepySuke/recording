package unit

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"

	"recording-transcription/internal/application/errorcode"
	"recording-transcription/internal/application/ports"
	"recording-transcription/internal/application/recording"
	"recording-transcription/internal/infrastructure/filestore/local"
)

// 测试依据：T04 修复轮 Important #1——§5.2 不对称分支（明确回滚删文件 / 提交结果
// 未知保留文件）与 90003 落盘失败分支（磁盘预检不足、写中途失败）的服务级覆盖；
// 设计依据：详设 §5.1（失败分支清理）、§5.2（提交结果未知保守不删）。
// 事务①用桩注入结果，落盘用真实本地存储（临时目录），按分支断言磁盘上文件
// 存在性、错误分类（errors.Is 穿透 AppError 到端口哨兵）与无 tmp- 残留。

// stubRecordingTx 注入事务①结果：err 非 nil 时原样返回。
type stubRecordingTx struct {
	err    error
	called bool
}

func (s *stubRecordingTx) CreateWithTask(context.Context, ports.CreateInput) error {
	s.called = true
	return s.err
}

// RetryTask 满足 T08 起扩展的端口面（本文件只测上传链，不会被调用）。
func (s *stubRecordingTx) RetryTask(context.Context, string) (int, error) {
	return 0, nil
}

// newUploadEnvSvc 组装被测服务：真实本地存储 + 桩事务 + 丢弃日志。
func newUploadEnvSvc(t *testing.T, minFreeBytes uint64, tx *stubRecordingTx) (*recording.UploadService, string) {
	t.Helper()
	dir := t.TempDir()
	store, err := local.New(dir, unitMaxFileBytes, minFreeBytes)
	if err != nil {
		t.Fatalf("创建本地文件存储失败: %v", err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	// 本文件只测上传链：不驱动 worker 池，notifier 传 nil（Upload 内部判空）。
	return recording.NewUploadService(store, tx, nil, logger, "unit-instance"), dir
}

// appErrCode 断言错误为 *errorcode.AppError 并返回业务码。
func appErrCode(t *testing.T, err error) errorcode.ErrorCode {
	t.Helper()
	var appErr *errorcode.AppError
	if !errors.As(err, &appErr) {
		t.Fatalf("错误不是 *errorcode.AppError: %v", err)
	}
	return appErr.Code
}

// TestUploadService_TxRollbackDeletesFile：事务①返回明确回滚类错误 →
// 文件必须被删除、返回 90002（CodeDatabaseUnavailable）、无 tmp- 残留。
func TestUploadService_TxRollbackDeletesFile(t *testing.T) {
	errRollback := errors.New("模拟事务①明确回滚")
	tx := &stubRecordingTx{err: errRollback}
	svc, dir := newUploadEnvSvc(t, 512*1024*1024, tx)

	_, err := svc.Upload(context.Background(),
		recording.UploadRequest{FilePart: strings.NewReader("fake audio"), Filename: "svc.wav"})
	if !errors.Is(err, errRollback) {
		t.Fatalf("错误应穿透到回滚原因: %v", err)
	}
	if code := appErrCode(t, err); code != errorcode.CodeDatabaseUnavailable {
		t.Errorf("code = %d (%s), want 90002", code, code)
	}
	if !tx.called {
		t.Error("事务①应已被调用")
	}
	if names := dirFiles(t, dir); len(names) != 0 {
		t.Errorf("明确回滚必须删除已落盘文件，残留: %v", names)
	}
}

// TestUploadService_CommitUnknownKeepsFile：事务①返回包装 ErrCommitUnknown 的
// 提交结果未知错误 → 文件必须保留（§5.2 保守不删）、返回 90002。
func TestUploadService_CommitUnknownKeepsFile(t *testing.T) {
	tx := &stubRecordingTx{err: fmt.Errorf("COMMIT 应答丢失: %w", ports.ErrCommitUnknown)}
	svc, dir := newUploadEnvSvc(t, 512*1024*1024, tx)

	_, err := svc.Upload(context.Background(),
		recording.UploadRequest{FilePart: strings.NewReader("fake audio"), Filename: "svc.wav"})
	if !errors.Is(err, ports.ErrCommitUnknown) {
		t.Fatalf("错误应穿透到 ports.ErrCommitUnknown: %v", err)
	}
	if code := appErrCode(t, err); code != errorcode.CodeDatabaseUnavailable {
		t.Errorf("code = %d (%s), want 90002", code, code)
	}
	names := dirFiles(t, dir)
	if len(names) != 1 {
		t.Fatalf("提交结果未知必须保留文件（§5.2），目录文件数 = %d: %v", len(names), names)
	}
	if strings.HasPrefix(names[0], "tmp-") {
		t.Errorf("保留的应是 rename 后的最终文件而非临时文件: %s", names[0])
	}
}

// TestUploadService_DiskPrecheckFailure：磁盘预检不足 → 90003
// （CodeFileStorageUnavailable）、不进入事务①、无任何文件残留。
func TestUploadService_DiskPrecheckFailure(t *testing.T) {
	tx := &stubRecordingTx{}
	svc, dir := newUploadEnvSvc(t, math.MaxUint64, tx) // 预检阈值不可满足

	_, err := svc.Upload(context.Background(),
		recording.UploadRequest{FilePart: strings.NewReader("fake audio"), Filename: "svc.wav"})
	if !errors.Is(err, ports.ErrStorageUnavailable) {
		t.Fatalf("错误应穿透到 ports.ErrStorageUnavailable: %v", err)
	}
	if code := appErrCode(t, err); code != errorcode.CodeFileStorageUnavailable {
		t.Errorf("code = %d (%s), want 90003", code, code)
	}
	if tx.called {
		t.Error("落盘失败不应进入事务①")
	}
	if names := dirFiles(t, dir); len(names) != 0 {
		t.Errorf("预检失败路径残留文件: %v", names)
	}
}

// TestUploadService_MidWriteFailure：写中途失败（RLIMIT_FSIZE 注入 EFBIG，越过
// 64KiB 的写入报错）→ tmp- 清理、90003、不进入事务①。SIGXFSZ 被 signal.Notify
// 接管以免默认终止进程；限额与信号处理在测试结束时恢复。
func TestUploadService_MidWriteFailure(t *testing.T) {
	tx := &stubRecordingTx{}
	svc, dir := newUploadEnvSvc(t, 512*1024*1024, tx)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGXFSZ)
	var prev unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_FSIZE, &prev); err != nil {
		t.Skipf("无法读取 RLIMIT_FSIZE: %v", err)
	}
	limit := prev
	limit.Cur = 64 * 1024
	if err := unix.Setrlimit(unix.RLIMIT_FSIZE, &limit); err != nil {
		t.Skipf("无法设置 RLIMIT_FSIZE（%v），跳过写中途故障注入", err)
	}
	defer func() {
		_ = unix.Setrlimit(unix.RLIMIT_FSIZE, &prev)
		signal.Stop(sigCh)
	}()

	content := bytes.Repeat([]byte("x"), 1024*1024) // 1MiB：小于文件上限、大于写限额
	_, err := svc.Upload(context.Background(),
		recording.UploadRequest{FilePart: bytes.NewReader(content), Filename: "svc.wav"})
	if !errors.Is(err, ports.ErrStorageUnavailable) {
		t.Fatalf("错误应穿透到 ports.ErrStorageUnavailable: %v", err)
	}
	if code := appErrCode(t, err); code != errorcode.CodeFileStorageUnavailable {
		t.Errorf("code = %d (%s), want 90003", code, code)
	}
	if tx.called {
		t.Error("落盘失败不应进入事务①")
	}
	if names := dirFiles(t, dir); len(names) != 0 {
		t.Errorf("写中途失败必须清理 tmp-，残留: %v", names)
	}
}

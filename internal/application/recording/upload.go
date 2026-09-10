// Package recording 「录音处理」上下文的应用层用例（详设 §2.5 完整用例链）。
// UploadService 编排上传链：校验 → 落盘 → 事务① → 事件镜像；每个失败分支有清理动作。
package recording

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"time"

	"recording-transcription/internal/application/errorcode"
	"recording-transcription/internal/application/ports"
	domain "recording-transcription/internal/domain/recording"
	"recording-transcription/internal/infrastructure/logging"
	"recording-transcription/internal/pkg/uuid"
)

// UploadRequest 上传用例输入：FilePart 为第一个名为 file 的 multipart 文件部分（流式）。
type UploadRequest struct {
	FilePart  io.Reader
	Filename  string // 客户端原始文件名（仅元数据，净化后展示）
	RequestID string // 创建请求关联（task_events.created_request_id）
}

// UploadResult 上传用例输出：202 响应体所需字段。
type UploadResult struct {
	RecordingID      string
	TaskID           string
	Status           domain.TaskStatus
	IdempotentReused bool
}

// UploadService 上传用例（详设 §5.1 判定树逐分支）。
type UploadService struct {
	store      ports.FileStore
	tx         ports.RecordingTx
	notifier   ports.Notifier // 提交成功后非阻塞唤醒 worker 池（详设 §3.3）
	logger     *slog.Logger
	instanceID string // 事件产生实例标识（详设 §7.2 instance_id）
}

// NewUploadService 构造上传用例；notifier 为上传链收口的唤醒出口（详设 §2.5）。
func NewUploadService(store ports.FileStore, tx ports.RecordingTx, notifier ports.Notifier, logger *slog.Logger, instanceID string) *UploadService {
	return &UploadService{store: store, tx: tx, notifier: notifier, logger: logger, instanceID: instanceID}
}

// Upload 执行完整上传链，错误一律包装 *errorcode.AppError（详设 §8.2/§8.3 映射）；
// 唯一例外：context 取消/超时返回原始错误（客户端已不可达，接口层不渲染业务错误）。
func (s *UploadService) Upload(ctx context.Context, req UploadRequest) (UploadResult, error) {
	// 判定树第一分支：扩展名 = 最后一个点后缀转小写，仅白名单（详设 §5.1）。
	ext, err := domain.ParseExtension(req.Filename)
	if err != nil {
		return UploadResult{}, errorcode.New(errorcode.CodeUnsupportedExtension, err)
	}

	// 磁盘预检 → 流式写入 tmp-（计数 + SHA-256）→ rename（详设 §5.1）。
	stored, err := s.store.Save(ctx, req.FilePart, ext)
	switch {
	case err == nil:
	case errors.Is(err, ports.ErrEmptyFile):
		return UploadResult{}, errorcode.New(errorcode.CodeEmptyFile, err)
	case errors.Is(err, ports.ErrFileTooLarge):
		return UploadResult{}, errorcode.New(errorcode.CodeFileTooLarge, err)
	case errors.Is(err, ports.ErrStorageUnavailable):
		s.logger.Error("上传落盘失败", slog.String("component", "filestore"), slog.Any("err", err))
		return UploadResult{}, errorcode.New(errorcode.CodeFileStorageUnavailable, err)
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		// 客户端断开或上传总超时：tmp- 已由存储清理，无数据库记录（详设 §5.1）。
		return UploadResult{}, err
	default:
		// 读端错误：multipart 非法、连接中断等。
		s.logger.Warn("上传读取中断", slog.Any("err", err))
		return UploadResult{}, errorcode.New(errorcode.CodeInvalidArgument, err)
	}

	// 应用侧预生成全部 ID（详设 §7.3）；事件序号由任务分配（首事件 = 1）。
	now := time.Now().UTC()
	recordingID, taskID := uuid.New(), uuid.New()
	rec := domain.Recording{
		ID:               recordingID,
		OriginalFilename: domain.SanitizeFilename(req.Filename),
		StoragePath:      stored.StoragePath,
		Extension:        ext,
		SizeBytes:        stored.SizeBytes,
		ContentHash:      stored.ContentHash,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	task := domain.ProcessingTask{
		ID:               taskID,
		RecordingID:      recordingID,
		Status:           domain.StatusPending,
		Attempt:          1,
		CreatedRequestID: req.RequestID,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	seq := task.AllocateEventSeq()
	pending := domain.StatusPending
	event := domain.TaskEvent{
		EventID:          uuid.New(),
		TaskID:           taskID,
		RecordingID:      recordingID,
		EventSeq:         seq,
		Attempt:          1,
		Kind:             domain.EventTaskCreated,
		OccurredAt:       now,
		Level:            domain.LevelInfo,
		ToStatus:         &pending,
		CreatedRequestID: req.RequestID,
		InstanceID:       s.instanceID,
	}

	// rename 已完成 → 事务①（架构 §3）：三行同事务，任一失败整体回滚。
	txResult, err := s.tx.CreateOrReuseByContentHash(ctx, ports.CreateInput{Recording: rec, Task: task, Event: event})
	if err != nil {
		if errors.Is(err, ports.ErrCommitUnknown) {
			// 提交结果未知：保守保留文件（详设 §5.2），待核实，不删。
			s.logger.Error("事务①提交结果未知，保留文件待核对",
				slog.String("recording_id", recordingID), slog.Any("err", err))
			return UploadResult{}, errorcode.New(errorcode.CodeDatabaseUnavailable, err)
		}
		// 明确回滚：删除文件再返回错误（详设 §5.1 Rb 分支）。
		if derr := s.store.Delete(context.WithoutCancel(ctx), stored.StoragePath); derr != nil {
			s.logger.Warn("回滚后文件删除失败，留待启动核对",
				slog.String("recording_id", recordingID), slog.Any("err", derr))
		}
		s.logger.Error("事务①失败已回滚", slog.String("recording_id", recordingID), slog.Any("err", err))
		return UploadResult{}, errorcode.New(errorcode.CodeDatabaseUnavailable, err)
	}

	if txResult.Reused {
		// 复用已提交的聚合：当前请求的 UUID 文件从未被数据库引用，提交后再删。
		if derr := s.store.Delete(context.WithoutCancel(ctx), stored.StoragePath); derr != nil {
			s.logger.Error("复用上传的重复文件删除失败，留待启动核对",
				slog.String("storage_path", stored.StoragePath), slog.Any("err", derr))
			return UploadResult{}, errorcode.New(errorcode.CodeFileStorageUnavailable, derr)
		}
		s.logger.Info("上传内容复用已有录音",
			slog.String("recording_id", txResult.RecordingID),
			slog.String("task_id", txResult.TaskID), slog.String("content_hash", stored.ContentHash))
		return UploadResult{RecordingID: txResult.RecordingID, TaskID: txResult.TaskID,
			Status: txResult.Status, IdempotentReused: true}, nil
	}

	// 仅新建成功时唤醒 worker 与镜像 task_created；复用分支不产生新事件。
	if s.notifier != nil {
		s.notifier.Notify()
	}
	logging.MirrorEvent(s.logger, event)
	s.logger.Info("上传完成",
		slog.String("recording_id", recordingID),
		slog.String("task_id", taskID),
		slog.Int64("size_bytes", stored.SizeBytes),
		slog.String("content_hash", stored.ContentHash),
	)
	return UploadResult{RecordingID: recordingID, TaskID: taskID, Status: task.Status, IdempotentReused: false}, nil
}

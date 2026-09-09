package ports

import (
	"context"
	"errors"
	"time"

	domain "recording-transcription/internal/domain/recording"
)

// 查询哨兵错误：应用层据此映射数字业务码（详设 §8.3）。
var (
	// ErrTaskNotFound 任务不存在，或其录音已删除/删除中（→ 404/30001）。
	ErrTaskNotFound = errors.New("task not found")
	// ErrRecordingNotFound 录音不存在，或删除中不可见（→ 404/20005）。
	ErrRecordingNotFound = errors.New("recording not found")
	// ErrDataInconsistent 逻辑关联异常：正常录音缺任务、done 缺 summary 等损坏关联（→ 500/90004）。
	ErrDataInconsistent = errors.New("logical association broken")
)

// TaskError 任务异步执行错误（详设 §8.4）：成功/处理中任务为 nil。
type TaskError struct {
	Code    int
	Message string
}

// TaskView 任务查询视图（GET /v1/tasks/{task_id} 响应数据，详设 §8.1）。
type TaskView struct {
	ID          string
	RecordingID string
	Status      domain.TaskStatus
	Attempt     int
	Error       *TaskError
	CreatedAt   time.Time
	StartedAt   *time.Time
	FinishedAt  *time.Time
}

// RecordingDetail 录音详情视图：任务视图全部字段 + 录音元数据与产物；
// transcript 转写完成后非 nil，result 仅 done 非 nil。
type RecordingDetail struct {
	Task             TaskView
	OriginalFilename string
	Extension        string
	SizeBytes        int64
	CreatedAt        time.Time
	Transcript       *string         // 转写完成（summarizing/done 落库）后非 nil
	Result           *domain.Summary // 仅 done 非 nil（summary_json 解析结果）
}

// RecordingListItem 列表项：录音概要 + 最新任务状态。
type RecordingListItem struct {
	RecordingID      string
	OriginalFilename string
	SizeBytes        int64
	CreatedAt        time.Time
	TaskID           string
	Status           domain.TaskStatus
}

// RecordingList 分页列表视图（items 空页为空切片、非 nil）。
type RecordingList struct {
	Items    []RecordingListItem
	Page     int
	PageSize int
	Total    int
}

// RecordingQuery 只读查询端口（详设 §8.1；架构 §3 查询只走数据库）：
// 实现不加锁、不写库；所有查询过滤删除中资源，不静默隐藏损坏关联（§4.6）。
// 分页参数由应用层校验后再传入。
type RecordingQuery interface {
	// GetTask 按任务 ID 查询；不存在或其录音删除中 → ErrTaskNotFound。
	GetTask(ctx context.Context, taskID string) (TaskView, error)
	// GetRecording 按录音 ID 查询详情；不存在/删除中 → ErrRecordingNotFound，
	// 正常录音缺任务 → ErrDataInconsistent。
	GetRecording(ctx context.Context, id string) (RecordingDetail, error)
	// ListRecordings 分页列表：created_at DESC, id DESC；page 从 1 起，已校验合法。
	ListRecordings(ctx context.Context, page, pageSize int) (RecordingList, error)
}

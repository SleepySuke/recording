package recording

import "time"

// EventKind 任务生命周期事件枚举（详设 §7.1/§7.2），固定取值不随代码顺序漂移。
type EventKind string

const (
	EventTaskCreated            EventKind = "task_created"
	EventTaskClaimed            EventKind = "task_claimed"
	EventTranscriptionCompleted EventKind = "transcription_completed"
	EventTaskCompleted          EventKind = "task_completed"
	EventTaskFailed             EventKind = "task_failed"
	EventTaskRetryAccepted      EventKind = "task_retry_accepted"
	EventTaskRecovered          EventKind = "task_recovered"
	EventTaskInterrupted        EventKind = "task_interrupted"
	EventTaskDeleteRequested    EventKind = "task_delete_requested"
	EventTaskDeleted            EventKind = "task_deleted"
)

// TaskEvent 任务生命周期事件的领域值描述（详设 §7.2 全字段，去除数据库物理列）：
// 与状态变更同事务写入，写入后不修改；不随聚合整体加载，也不用于事件回放重建聚合（§2.1）。
type TaskEvent struct {
	EventID          string
	TaskID           string
	RecordingID      string
	EventSeq         int64
	Attempt          int
	Kind             EventKind
	OccurredAt       time.Time
	Level            string // INFO / WARN / ERROR
	FromStatus       *TaskStatus
	ToStatus         *TaskStatus
	Stage            *string // transcribing / summarizing
	RequestID        *string
	CreatedRequestID string
	InstanceID       string
	ErrorCode        *int
	ErrorMessage     string
	StageElapsedMs   *uint64
	AttemptElapsedMs *uint64
	TaskElapsedMs    *uint64
	Details          map[string]any // 白名单元数据（如 previous_attempt），不含转写正文
}

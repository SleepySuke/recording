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
	EventTaskAutoRetryScheduled EventKind = "task_auto_retry_scheduled"
	EventTaskRetryAccepted      EventKind = "task_retry_accepted"
	EventTaskRecovered          EventKind = "task_recovered"
	EventTaskInterrupted        EventKind = "task_interrupted"
	EventTaskDeleteRequested    EventKind = "task_delete_requested"
	EventTaskDeleted            EventKind = "task_deleted"
)

// EventLevel 事件级别枚举（详设 §7.2 level 列：INFO / WARN / ERROR）。
// 命名常量取代裸字符串（T03 评审遗留项）：新调用点一律用具名值。
type EventLevel string

const (
	LevelInfo  EventLevel = "INFO"
	LevelWarn  EventLevel = "WARN"
	LevelError EventLevel = "ERROR"
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
	Level            EventLevel // INFO / WARN / ERROR（详设 §7.2）
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

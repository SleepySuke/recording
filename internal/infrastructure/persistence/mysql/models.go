package mysql

import "time"

// 三张表的 GORM PO 模型，字段与 migrations/0001_init.sql 一一对应（架构 §4、详设 §7.2）。
// 逻辑关联无物理外键；关联完整性由应用事务维护（详设 §4.6）。

// RecordingPO 录音元数据（recordings 表）。
type RecordingPO struct {
	ID               string     `gorm:"column:id;size:36"`
	OriginalFilename string     `gorm:"column:original_filename;size:512"`
	StoragePath      string     `gorm:"column:storage_path;size:512;uniqueIndex:uq_recordings_storage_path"`
	Extension        string     `gorm:"column:extension;size:8"`
	SizeBytes        int64      `gorm:"column:size_bytes"`
	ContentHash      string     `gorm:"column:content_hash;size:64"`
	DeletingAt       *time.Time `gorm:"column:deleting_at"`
	CreatedAt        time.Time  `gorm:"column:created_at;autoCreateTime"`
	UpdatedAt        time.Time  `gorm:"column:updated_at;autoUpdateTime"`
}

func (RecordingPO) TableName() string { return "recordings" }

// RecordingHashLockPO 是内容哈希的持久互斥点，不是业务资源；删除 recording 时保留。
type RecordingHashLockPO struct {
	ContentHash string    `gorm:"column:content_hash;primaryKey;size:64"`
	CreatedAt   time.Time `gorm:"column:created_at"`
}

func (RecordingHashLockPO) TableName() string { return "recording_hash_locks" }

// TaskPO 处理任务（tasks 表）；recording_id 唯一逻辑关联，重试复用 id 递增 attempt。
type TaskPO struct {
	ID               string     `gorm:"column:id;size:36"`
	RecordingID      string     `gorm:"column:recording_id;size:36;uniqueIndex:uq_tasks_recording"`
	Status           string     `gorm:"column:status;size:32"`
	Attempt          int        `gorm:"column:attempt"`
	EventSeq         int64      `gorm:"column:event_seq"`
	Transcript       string     `gorm:"column:transcript;type:mediumtext"`
	SummaryJSON      *string    `gorm:"column:summary_json;type:json"` // 可空 JSON 列：空串不是合法 JSON，未完成时存 NULL
	ErrorCode        *int       `gorm:"column:error_code"`
	ErrorMessage     string     `gorm:"column:error_message;type:text"`
	CreatedRequestID string     `gorm:"column:created_request_id;size:64"`
	CreatedAt        time.Time  `gorm:"column:created_at;autoCreateTime"`
	UpdatedAt        time.Time  `gorm:"column:updated_at;autoUpdateTime"`
	StartedAt        *time.Time `gorm:"column:started_at"`
	FinishedAt       *time.Time `gorm:"column:finished_at"`
	NextRetryAt      *time.Time `gorm:"column:next_retry_at"`
}

func (TaskPO) TableName() string { return "tasks" }

// TaskEventPO 任务生命周期事件（task_events 表，详设 §7.2 全 21 列）；
// 与状态变更同事务写入，UNIQUE(task_id, event_seq) 保证顺序，写入后不修改。
type TaskEventPO struct {
	ID               int64     `gorm:"column:id;primaryKey;autoIncrement"`
	EventID          string    `gorm:"column:event_id;size:36;uniqueIndex:uq_task_events_event_id"`
	TaskID           string    `gorm:"column:task_id;size:36;uniqueIndex:uq_task_events_seq,priority:1"`
	RecordingID      string    `gorm:"column:recording_id;size:36"`
	EventSeq         int64     `gorm:"column:event_seq;uniqueIndex:uq_task_events_seq,priority:2"`
	Attempt          int       `gorm:"column:attempt"`
	Event            string    `gorm:"column:event;size:64"`
	OccurredAt       time.Time `gorm:"column:occurred_at"`
	Level            string    `gorm:"column:level;size:8"`
	FromStatus       *string   `gorm:"column:from_status;size:32"`
	ToStatus         *string   `gorm:"column:to_status;size:32"`
	Stage            *string   `gorm:"column:stage;size:32"`
	RequestID        *string   `gorm:"column:request_id;size:64"`
	CreatedRequestID string    `gorm:"column:created_request_id;size:64"`
	InstanceID       string    `gorm:"column:instance_id;size:36"`
	ErrorCode        *int      `gorm:"column:error_code"`
	ErrorMessage     *string   `gorm:"column:error_message;size:512"`
	StageElapsedMs   *int64    `gorm:"column:stage_elapsed_ms"`
	AttemptElapsedMs *int64    `gorm:"column:attempt_elapsed_ms"`
	TaskElapsedMs    *int64    `gorm:"column:task_elapsed_ms"`
	Details          string    `gorm:"column:details;type:json"`
}

func (TaskEventPO) TableName() string { return "task_events" }

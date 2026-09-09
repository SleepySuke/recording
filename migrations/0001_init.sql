-- 0001_init：三张业务表初始结构（架构 §4 数据模型；详设 §7.2 task_events 全字段）。
-- 约定：InnoDB；ID 为应用生成 UUID，CHAR(36) ASCII；时间 UTC DATETIME(6)；逻辑关联，无物理外键。

CREATE TABLE recordings (
    id                CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    original_filename VARCHAR(512) NOT NULL,
    storage_path      VARCHAR(512) NOT NULL,
    extension         VARCHAR(8)   NOT NULL,
    size_bytes        BIGINT       NOT NULL,
    content_hash      CHAR(64)     NULL,
    deleting_at       DATETIME(6)  NULL,
    created_at        DATETIME(6)  NOT NULL,
    updated_at        DATETIME(6)  NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uq_recordings_storage_path (storage_path),
    KEY idx_recordings_created (created_at DESC, id DESC),
    CONSTRAINT chk_recordings_extension CHECK (extension IN ('wav', 'mp3', 'm4a', 'aac')),
    CONSTRAINT chk_recordings_size CHECK (size_bytes > 0 AND size_bytes <= 52428800)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

CREATE TABLE tasks (
    id                CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    recording_id      CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    status            VARCHAR(32)  NOT NULL,
    attempt           INT          NOT NULL,
    event_seq         BIGINT UNSIGNED NOT NULL DEFAULT 1,
    transcript        MEDIUMTEXT   NULL,
    summary_json      JSON         NULL,
    error_code        INT          NULL,
    error_message     TEXT         NULL,
    created_request_id VARCHAR(64) NOT NULL,
    created_at        DATETIME(6)  NOT NULL,
    updated_at        DATETIME(6)  NOT NULL,
    started_at        DATETIME(6)  NULL,
    finished_at       DATETIME(6)  NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uq_tasks_recording (recording_id),
    KEY idx_tasks_queue (status, created_at, id),
    CONSTRAINT chk_tasks_status CHECK (status IN ('pending', 'transcribing', 'summarizing', 'done', 'failed')),
    CONSTRAINT chk_tasks_attempt CHECK (attempt > 0)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

CREATE TABLE task_events (
    id                 BIGINT UNSIGNED AUTO_INCREMENT,
    event_id           CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    task_id            CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    recording_id       CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    event_seq          BIGINT UNSIGNED NOT NULL,
    attempt            INT          NOT NULL,
    event              VARCHAR(64)  NOT NULL,
    occurred_at        DATETIME(6)  NOT NULL,
    level              VARCHAR(8)   NOT NULL,
    from_status        VARCHAR(32)  NULL,
    to_status          VARCHAR(32)  NULL,
    stage              VARCHAR(32)  NULL,
    request_id         VARCHAR(64)  NULL,
    created_request_id VARCHAR(64)  NOT NULL,
    instance_id        CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    error_code         INT          NULL,
    error_message      VARCHAR(512) NULL,
    stage_elapsed_ms   BIGINT UNSIGNED NULL,
    attempt_elapsed_ms BIGINT UNSIGNED NULL,
    task_elapsed_ms    BIGINT UNSIGNED NULL,
    details            JSON         NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uq_task_events_event_id (event_id),
    UNIQUE KEY uq_task_events_seq (task_id, event_seq)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

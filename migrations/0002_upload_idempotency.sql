-- 0002_upload_idempotency：同内容上传的持久串行化点。
-- 详见 docs/design/upload-idempotency.md；不对 recordings.content_hash 加唯一约束，
-- 因为 deleting_at 非空的旧录音不得阻止相同内容重新上传。

CREATE TABLE recording_hash_locks (
    content_hash CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    created_at   DATETIME(6) NOT NULL,
    PRIMARY KEY (content_hash)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

CREATE INDEX idx_recordings_content_hash ON recordings (content_hash);

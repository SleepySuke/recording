-- 0003_automatic_retry：持久化自动重试退避；详见 docs/design/retry-and-summary-stream.md。
ALTER TABLE tasks
    ADD COLUMN next_retry_at DATETIME(6) NULL AFTER finished_at,
    ADD KEY idx_tasks_ready (status, next_retry_at, created_at, id);

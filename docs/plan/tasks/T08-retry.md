# T08：失败重试

> 依赖：T07 · 预算：0.5h · 状态：**已完成（待提交）**

**设计依据**：详设 §4.5（重试协议）、§8.1（retry 端点）、§8.3（30001/30002）、§8.5（重复重试语义）；架构 §3 失败与重试段
**测试依据**：IT-06、IT-07；验收清单「重试、删除与恢复」第 1～2 条

## 目标

`POST /v1/tasks/{task_id}/retry`：仅 failed 可重试；条件更新 attempt+1 回 pending、清空上轮产物与错误；同轮并发重复请求一次 202 其余 409/30002；提交后 Notify。

## 涉及文件

- Create：`internal/application/recording/retry.go`（Retry 用例）
- Create/Modify：`internal/application/ports/recordingtx.go`（追加 RetryTask）
- Create/Modify：`internal/infrastructure/persistence/mysql/recording_tx.go`（实现）
- Create：`internal/interfaces/http/dto/retry.go`、`handler/retry.go`
- Modify：`internal/interfaces/http/router.go`
- Test：`tests/integration/retry_test.go`

## 交付接口

```go
// RecordingTx 追加：
RetryTask(ctx context.Context, taskID string) (attempt int, err error)
// 事务：锁 tasks→recordings 行 → 锁内复查 status=failed 且未删除 →
// 条件更新 status=pending、attempt=attempt+1、清空 transcript/summary_json/error_code/error_message/
// started_at/finished_at → 同事务 task_retry_accepted 事件 → 回读新 attempt
// 复查非 failed → 返回 *errorcode.AppError{Code: CodeTaskNotRetryable}（HTTP 409）；行锁保证并发下只有一个成功
```

响应契约（详设 §8.1）：202 `{"task_id":"…","status":"pending","attempt":N}`；非 failed → 409/30002；不存在/删除中 → 404/30001。

## 步骤

- [x] 1. 写失败测试（`tests/integration/retry_test.go`）：
  - `TestIT06_ConcurrentRetry`：同一 failed 任务两个并发 retry → 一个 202 一个 409/30002，attempt 恰好 +1，事件恰好一条 task_retry_accepted。
  - `TestIT07_RetryPreconditions`：对 done/processing 任务 retry → 409/30002；不存在任务 → 404/30001。
  - `TestRetry_OldAttemptIsolated`：重试触发新一轮后，用旧 attempt 模拟迟到阶段写入 → 被条件更新拒绝（覆盖验收「旧 attempt 无法覆盖新 attempt 的状态和产物」）。
  - `TestRetry_FullFlow`：确定性失败（40001）→ retry 202 → 替身新一轮成功 → done 且 attempt=2，事件链含 task_failed + task_retry_accepted + task_claimed。
- [x] 2. 运行确认失败：`TEST_MYSQL_DSN=… go test ./tests/integration/ -run 'TestIT06|TestIT07|TestRetry' -v`。
- [x] 3. 实现：RetryTask 事务（复用 T06/T07 的锁顺序与事件写入设施）、用例与 handler；COMMIT 后 `Notify()`。
- [x] 4. 运行确认通过：全绿。
- [ ] 5. 提交：`feat: support manual retry for failed tasks`。
- [x] 6. E2E：补 E2E-02 / E2E-03 / E2E-04（`e2e/`，映射表见测试设计 §5.0）。

## 完成标准

- [x] IT-06/IT-07 全绿；重复请求不新增任务（task_id 复用）。
- [x] 手动：失败任务 retry 后从头执行成功（attempt=2）。

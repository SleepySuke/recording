# T15：基于内容哈希的上传幂等

> 依赖：T14 · 预算：1.5h · 状态：**完成（2026-09-10）**

**设计依据**：[上传幂等设计](../../design/upload-idempotency.md)（唯一技术权威）；详设 §5.1、§5.2、§8.5 仅保留通用上传与结果未知规则。  
**测试依据**：IT-20～IT-23、E2E-09（均见[测试设计](../../design/test-design.md)）。

## 目标

按上传幂等设计实现内容哈希复用：相同完整字节内容命中未删除录音时返回既有 ID 与状态，成功响应显式标识 `idempotent_reused: true`；只有首次创建会产生任务、创建事件和 worker 唤醒。

## 涉及文件

- Create：`migrations/0002_upload_idempotency.sql`
- Modify：`internal/application/ports/recordingtx.go`、`internal/application/recording/upload.go`
- Modify：`internal/infrastructure/persistence/mysql/recording_tx.go`、删除标记所在的 MySQL 事务实现
- Modify：`internal/interfaces/http/dto/upload.go`、上传 handler 的响应映射
- Modify：测试库清理辅助、`tests/integration/upload_test.go`、`tests/integration/delete_test.go`
- Create/Modify：`e2e/idempotency_test.go`、`e2e/expected/E09.json`
- Modify：`README.md`、`HANDOFF.md`、本文件（仅在实现与验证结束后回写状态）

## 开发步骤

- [x] 1. 完成设计：新增 `docs/design/upload-idempotency.md`，定义哈希锁表、统一锁顺序、上传/删除数据流、补偿和接口契约；在架构、详设与测试设计建立引用。
- [x] 2. 编写 `0002_upload_idempotency.sql`：创建 `recording_hash_locks` 和 `idx_recordings_content_hash`；测试库前置清理哈希锁表，迁移幂等测试覆盖 0002。
- [x] 3. 先写 RED：实现 IT-20～IT-23 与 E2E-09 的预期结果，覆盖顺序复用、同名异内容、删除中重传和并发同内容；确认旧实现顺序重复建任务、八路并发建八任务、E2E-09 golden 均失败。
- [x] 4. 按设计改造端口与上传用例：事务返回“新建或复用”结果；复用分支清理当前请求的重复文件且不 Notify；响应增加 `idempotent_reused`。
- [x] 5. 按统一锁顺序改造 MySQL 上传和删除标记事务；实现提交未知与重复文件删除失败的补偿分支。
- [x] 6. GREEN：执行定向测试并处理首个哈希锁行的 InnoDB insert-intention 死锁；哈希锁行改为独立短语句确保存在，业务事务再 `FOR UPDATE` 串行化，IT-23 稳定通过。
- [x] 7. 运行完整门禁：unit、integration、过程级 E2E、`make lint`；回写 README 的接口示例与 HANDOFF 的加分项状态。
- [ ] 8. 提交：`feat: reuse active recording for duplicate upload content`。（用户手动提交）

## 完成标准

- [x] `migrations/0002_upload_idempotency.sql` 可在空库和既有 0001 数据库上前滚执行；不会为删除中录音施加唯一内容约束。
- [x] IT-20～IT-23 与 E2E-09 通过，且 IT-23 在 `-race` 下重复运行不出现重复 recording、task 或 `task_created`。
- [x] 原有上传、删除、重试和恢复测试保持通过；README、HANDOFF 与实现行为一致。

## 验收记录（2026-09-10）

```
$ TEST_MYSQL_DSN='recording:…/recording_test?…' GOTOOLCHAIN=local \
  go test ./tests/integration/ ./e2e/ \
  -run 'TestIT20|TestIT21|TestIT22|TestIT23|TestE2E09' -count=1 -v
ok  recording-transcription/tests/integration
ok  recording-transcription/e2e

$ TEST_MYSQL_DSN='recording:…/recording_test?…' GOTOOLCHAIN=local \
  go test ./... -race -count=1
ok  recording-transcription/e2e
ok  recording-transcription/tests/integration
ok  recording-transcription/tests/unit

$ make lint
GOTOOLCHAIN=local go vet ./...
OK     gofmt
```

过程级 E2E-09 经真实 `bootstrap.NewServer`、TCP、MySQL、本地文件存储和 worker 装配运行，golden 断言首次上传 `reused=false`、第二次 `reused=true`、两次 ID 相同、仅一条 `task_created`、一条活跃录音和一个最终文件。compose E2E 沿用既有单例 E-COMPOSE，未为本任务重建容器。

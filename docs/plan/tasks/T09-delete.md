# T09：删除与后台清理

> 依赖：T08 · 预算：1h · 状态：**未开始**

**设计依据**：详设 §5.3（删除流程与细则）、§3.4（取消表调用）、§4.6（三表显式删除）、§7.5（task_deleted 仅写文件日志）、§10（CLEANUP_INTERVAL）；架构 §3 删除段
**测试依据**：IT-08、IT-09、IT-19；验收清单「重试、删除与恢复」第 3～4 条

## 目标

`DELETE /v1/recordings/{id}` 任意状态可删：标记 deleting_at → 取消在途执行 → 删文件 → 三表事务清理 → 204。文件/清理失败 503/20006 保留标记，后台每 30s 低频清理续做；迟到 worker 结果被条件更新拒绝。

## 涉及文件

- Create：`internal/application/recording/delete.go`（Delete 用例 + 清理用例）
- Modify：`internal/application/ports/recordingtx.go`（追加 MarkDeleting / PurgeRecording）
- Modify：`internal/infrastructure/persistence/mysql/recording_tx.go`（实现）
- Create：`internal/infrastructure/worker/cleanup.go`（低频清理循环）
- Create：`internal/interfaces/http/handler/delete.go`
- Modify：`internal/interfaces/http/router.go`、`bootstrap/wire.go`（清理循环纳入生命周期）
- Test：`tests/integration/delete_test.go`

## 交付接口

```go
// RecordingTx 追加：
MarkDeleting(ctx context.Context, recordingID string) (taskID string, ok bool, err error)
// 事务：锁定 tasks→recordings → 不存在返回 ok=false（404）；已标记 deleting_at 也返回 ok=true（幂等续做，
// 不重复追加 task_delete_requested）→ 设置 deleting_at + task_delete_requested 事件
PurgeRecording(ctx context.Context, recordingID string) error
// 事务：DELETE task_events → tasks → recordings（显式顺序），任一步失败整体回滚

// worker/cleanup.go
func StartCleanup(runCtx context.Context, wg *sync.WaitGroup, interval time.Duration, cleanup func(ctx context.Context))
```

## 步骤

- [ ] 1. 通读设计：详设 §5.3 流程图与步骤细则、§7.5 删除时事件处理。
- [ ] 2. 写失败测试（`tests/integration/delete_test.go`）：
  - `TestDelete_ProcessingRecording`：上传后立即 DELETE → 204；随后 GET 任务/录音 404；数据目录无该文件；三表无该 ID 残留；`logs/app.jsonl` 含 task_delete_requested 与 task_deleted（文件日志）。
  - `TestDelete_AnyStatus`（子测试）：pending / transcribing / summarizing / done / failed 各状态均可删除（预置状态后执行）。
  - `TestIT08_LateWriteRejected`：deleting_at 提交后让 worker 迟到落结果 → 条件更新拒绝，结果只记日志，资源不复活。
  - `TestIT09_ThreeTableDeleteRollback`：注入 events 删除失败（如约束冲突包装适配器）→ 整体回滚，无半删除（recordings/tasks 仍在、deleting_at 保留）。
  - `TestDelete_FileFailureRetriesViaCleanup`：文件删除失败（chmod 目录模拟）→ 503/20006；恢复权限后等待清理循环 → 清理完成；期间重复 DELETE 幂等续做、不重复事件；已彻底删除的 ID → 404。
  - `TestIT19_EventChainReconstructable`：成功/失败/重试/删除各跑一遍 → 按 task_id 查事件，event_seq 严格递增、attempt 正确、跨轮次完整（删除场景验证到删除前为止）。
- [ ] 3. 运行确认失败：`TEST_MYSQL_DSN=… go test ./tests/integration/ -run 'TestDelete|TestIT08|TestIT09|TestIT19' -v`。
- [ ] 4. 实现要点：
  - 取消在途执行：MarkDeleting 提交后从取消表取 CancelFunc，**锁外**调用（§3.4）；即使取消未及时生效，条件更新兜底。
  - 删文件不存在视为成功；成功后 PurgeRecording；提交成功 → slog 写 task_deleted（独立 event_id，最后 event_seq+1，不写回已清理的事件表，§7.5）→ 204。
  - 清理循环：扫描 deleting_at 非空 → 删文件 → Purge；失败记日志不阻塞；「启动优先恢复删除」属 T11。
- [ ] 5. 运行确认通过：全绿（涉及取消表与清理循环的用例带 `-race`）。
- [ ] 6. 提交：`feat: implement safe deletion with background cleanup`。
- [ ] 7. E2E：补 E2E-05（`e2e/`，映射表见测试设计 §5.0）；E2E-08 与不变量巡检中 deleting 相关断言同步放宽。

## 完成标准

- [ ] 任意状态可删；删除后文件与三表数据消失；迟到写入不复活资源。
- [ ] 文件暂时删除失败可经 30s 清理或重复 DELETE 收敛。

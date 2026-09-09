# T06：Worker 池、任务认领与 Mock 转写

> 依赖：T04、T05 · 预算：1.5h · 状态：**未开始**

**设计依据**：详设 §3.1/§3.2（worker 控制流）、§3.3（channel 唤醒）、§3.4（context 树与取消表）、§4.2/§4.3（认领协议）、§4.4（条件更新）；架构 §5（状态机）
**测试依据**：UT-09；IT-03、IT-04；验收清单「异步与真实摘要」第 2～4 条

## 目标

异步流水线前半段：3 个 goroutine worker 短事务认领 pending（FOR UPDATE SKIP LOCKED + 锁后复查）、登记取消表、执行 Mock 转写（生产 5～15s/20% 失败，测试确定性替身）、事务④保存 transcript 并推进 summarizing。上传/重试提交后 channel 非阻塞唤醒，1s 轮询兜底。

## 涉及文件

- Create：`internal/application/processing/process.go`（Process 用例：认领→转写→保存）
- Create：`internal/application/ports/processingtx.go`、`transcriber.go`、`notifier.go`
- Create：`internal/infrastructure/worker/pool.go`（池 + 唤醒 + select 循环）、`canceltable.go`（内存取消表）
- Create：`internal/infrastructure/asr/mock/mock.go`（生产 Mock）、`deterministic.go`（测试替身，同包导出）
- Create：`internal/infrastructure/persistence/mysql/processing_tx.go`（认领与事务④实现）
- Modify：`internal/application/recording/upload.go`（提交成功后调用 `Notifier.Notify()`——本任务加入，之前上传不通知）
- Modify：`bootstrap/wire.go`（组装池与 runCtx 生命周期）
- Test：`internal/infrastructure/worker/pool_test.go`；`tests/integration/pipeline_test.go`

## 交付接口（后续任务依赖）

```go
// ports/processingtx.go
type ClaimedExecution struct{ ExecutionKey domain.ExecutionKey; RecordingID, StoragePath, Extension string; CreatedRequestID string }
type ProcessingTx interface {
    ClaimNext(ctx context.Context) (*ClaimedExecution, bool, error) // 事务②：SKIP LOCKED 选 pending（created_at,id 序）→
                                                                    // 锁 recording 复查 deleting_at → 条件更新 transcribing +
                                                                    // task_claimed 事件 → 同事务回读状态；无可认领返回 false
    SaveTranscription(ctx context.Context, key domain.ExecutionKey, transcript string) error // 事务④：transcript + summarizing +
                                                                    // transcription_completed，条件 id+attempt+expected_status
}
// ports/transcriber.go
type Transcriber interface {
    Transcribe(ctx context.Context, taskID string) (transcript string, err error) // ctx 取消必须中断等待
}
// ports/notifier.go
type Notifier interface{ Notify() } // 非阻塞；实现为池向所有 worker 的 cap-1 channel 投递

// worker 包
func NewPool(n int, poll time.Duration, process func(ctx context.Context)) *Pool
func (p *Pool) Start(runCtx context.Context) / (p *Pool) Stop()  // WaitGroup 管理；claimCtx 在 drain 时取消（§3.5 前置）
type CancelTable struct{ … } // Mutex + map[ExecutionKey]context.CancelFunc；Register/Unregister/Cancel（锁外调用）
```

认领 SQL 形态（详设 §4.3）：`tx.Clauses(clause.Locking{Strength:"UPDATE", Options:"SKIP LOCKED"}).Where("status = ?", pending).Order("created_at, id").First(&task)`，随后锁 recording 行、复查 `deleting_at IS NULL` 与关联存在性；孤儿触发 90004 停止认领（本任务先记录日志返回错误，巡检阻断在 T11）。

## 步骤

- [ ] 1. 通读设计：详设 §3.2 控制流图（本任务实现到 SaveT 分支）、§3.3 唤醒、§3.4 取消表、§4.3 时序图。
- [ ] 2. 写失败测试（单元）：`TestUT09_NotifyNeverBlocks`（channel 已满时连续 Notify 立即返回，无阻塞无 panic）；`TestCancelTable_RegisterCancelUnregister`（Cancel 后对应 taskCtx 取消；锁外调用路径不死锁）。
- [ ] 3. 写失败测试（集成，`tests/integration/pipeline_test.go`，DeterministicTranscriber + 桩 Summarizer 不触发）：
  - `TestIT03_ConcurrentClaimMutex`：2 个 worker 并发认领同一 pending → 恰好一个成功，恰好一条 task_claimed。
  - `TestIT04_SkipLockedDispatch`：5 个 pending + 3 worker 并发认领 → 各自认领不同任务，无重复无遗漏。
  - `TestPipeline_TranscribeToSummarizing`：上传后等待 → 任务到 summarizing、transcript 为替身种子文本、事件链 task_created→task_claimed→transcription_completed。
  - `TestPipeline_ClaimSkipsDeleting`：预置 deleting_at 的 pending → 认领跳过（不推进、不产生事件）。
- [ ] 4. 运行确认失败：`TEST_MYSQL_DSN=… go test ./tests/integration/ -run 'TestIT03|TestIT04|TestPipeline' -v`；`go test ./internal/infrastructure/worker/... -race -v`。
- [ ] 5. 实现要点：
  - worker 循环：select（wake chan / 1s ticker / 退出 ctx）→ 认领 → 无任务回 select；认领成功建 taskCtx 并登记取消表 → 复查轮次与 deleting_at（登记后外呼前，§3.4）→ 转写 → SaveTranscription；`RowsAffected=0` → stale，清理登记回 select（本任务 stub 阶段处理完即 Unregister）。
  - 生产 Mock：`time.NewTimer` + `select ctx.Done()`，不用不可取消的 Sleep；失败约 20%。
  - `Upload` 在事务①提交后调用 `Notify()`（§2.5 上传链收口）。
- [ ] 6. 运行确认通过：全部命令全绿（worker 包带 `-race`）。
- [ ] 7. 提交：`feat: implement worker pool with claim protocol and mock transcription`。

## 完成标准

- [ ] IT-03/IT-04 全绿：worker 并发不超配置，多 worker 不同认领同一轮次。
- [ ] 手动验证：上传小文件后任务在数秒内进入 summarizing；`docker compose up -d db && go run ./cmd/server` 可观察事件镜像。

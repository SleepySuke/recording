# T11：启动恢复与就绪门控

> 依赖：T10 · 预算：1h · 状态：**已完成（待提交）**

**设计依据**：详设 §6（崩溃窗口、启动恢复流程、12h 降级、恢复边界）、§4.6（关联巡检）、§5.2（孤儿文件核对）；架构 §6（启动顺序）
**测试依据**：IT-10、IT-11、IT-12；验收清单「重试、删除与恢复」第 5 条

## 目标

main 启动序列：健康检查 → 迁移 → 关联巡检（异常 90004 阻止 ready）→ 恢复未完成删除 → 重置在途任务（reset 基线 / interrupt 降级，`RECOVERY_MODE` 选择）→ 孤儿文件核对 → 启动 worker 与 HTTP → `/readyz` 就绪。完成 bootstrap 组装，形成完整可运行服务。

## 涉及文件

- Create：`internal/application/processing/recover.go`（Recover 用例：巡检 + 删除恢复 + 在途重置 + 孤儿核对）
- Modify：`internal/application/ports/processingtx.go`（追加恢复端口方法）
- Modify：`internal/infrastructure/persistence/mysql/processing_tx.go`（巡检 LEFT JOIN、批量重置事务、删除恢复）
- Modify：`internal/application/ports/filestore.go`（追加 ListStored / 存在性核对所需方法）
- Modify：`cmd/server/main.go`、`bootstrap/wire.go`（按 §6.2 顺序编排；readyz 接真实就绪状态，替换 T01 的占位）
- Test：`tests/integration/recovery_test.go`

## 交付接口

```go
// Recover 用例内部步骤（§6.2 顺序），对外暴露：
type Recoverer interface {
    Inspect(ctx context.Context) error                     // LEFT JOIN：孤立任务 / 非删除中缺任务 → 90004，不静默修复
    ResumeDeletions(ctx context.Context) error             // deleting_at 非空 → 删文件 → 三表清理；失败记日志不阻塞
    ResetInFlight(ctx context.Context) (int, error)        // 单事务批量：transcribing/summarizing → pending、attempt+1、
                                                           // 清空产物/错误/时间 + task_recovered（reset 模式）
                                                           // 或 → failed/30003 + task_interrupted（interrupt 模式）
    RemoveOrphanFiles(ctx context.Context) (int, error)    // tmp- 直接删；数据目录内未被 storage_path 引用的删除；查询失败不清理
}
```

批量重置 SQL 形态（详设 §6.2 第 5 步）：`Updates(map[string]any{"status":"pending","attempt":gorm.Expr("attempt+1"),"transcript":nil,"summary_json":nil,"error_code":nil,"error_message":nil,"started_at":nil,"finished_at":nil})`，带状态条件（防御性）；每个受影响任务同事务追加 task_recovered（details 记 previous_attempt 与原状态，event_seq 相应递增）。

## 步骤

- [x] 1. 通读设计：详设 §6.1 崩溃窗口表、§6.2 流程图与七步正文、§6.3 降级变体。
- [x] 2. 写失败测试（`tests/integration/recovery_test.go`，构造现场后执行 Recover 再断言）：
  - `TestIT10_ResetInFlight`：预置 transcribing / summarizing 记录（含残留产物与错误）→ 执行恢复 → pending、attempt+1、产物清空、task_recovered 事件且 details.previous_attempt 正确；pending 保留；done/failed 不动。
  - `TestRecovery_InterruptMode`：`RECOVERY_MODE=interrupt` → 在途任务 failed/30003 + task_interrupted。
  - `TestIT11_ResumeDeletions`：预置 deleting_at + 残留文件 → 恢复后文件与三表清理完成。
  - `TestIT12_IntegrityInspect`：构造孤儿任务 / 缺任务录音 → 返回 90004、调用方阻止 ready、数据未被静默修复。
  - `TestRecovery_OrphanFiles`：数据目录放置未引用文件与 `tmp-` 临时文件 → 均被移除；被引用文件保留；使 DB 查询失败 → 不执行清理。
  - `TestRecovery_BootOrder`：readyz 在恢复完成前非就绪、完成后就绪（对 §6.2 顺序的端到端断言）。
- [x] 3. 运行确认失败：`TEST_MYSQL_DSN=… go test ./tests/integration/ -run 'TestIT10|TestIT11|TestIT12|TestRecovery' -v`。
- [x] 4. 实现：四个恢复步骤 + main 编排（健康检查失败以失败退出交给容器重启；迁移失败阻止启动）；`/readyz` 门控替换 T01 占位。
- [x] 5. 运行确认通过：全绿。
- [x] 6. 手动演练（E2E-06 前置）：上传 → 长延迟任务在 summarizing 时 `kill` 进程 → 重启 → 任务重做至 done、attempt=2、事件含 task_recovered。
  - T11 执行注记：手动 kill 演练由 E2E-06 的进程内重启路径等价覆盖（`bootApp` A/B 同库同目录重启，A 以超预算强停等价 kill——§3.5 第 4 条取消 runCtx、任务保持在途），未另做进程级 kill。
- [ ] 7. 提交：`feat: implement startup recovery and readiness gating`。（本轮按编排约定不执行 git 操作，提交留给上层）
- [x] 8. E2E：补 E2E-06（`e2e/`，映射表见测试设计 §5.0）。

## 完成标准

- [x] IT-10/11/12 全绿；reset 与 interrupt 两种模式均按配置生效。
- [x] 重启后 pending 可被认领、在途从头执行、done/failed 不重跑；孤立文件被清理且不误删被引用文件。

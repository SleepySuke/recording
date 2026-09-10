# T10：panic 防护与优雅退出

> 依赖：T09 · 预算：0.5h · 状态：**未开始**

**设计依据**：详设 §3.2（每轮执行包裹 recover）、§3.5（两阶段退出）、§5.5（落库持续失败受控退出）；架构 §5 worker 韧性条目
**测试依据**：IT-17、IT-18；验收清单「异步与真实摘要」第 5 条（context 取消/退出与 WaitGroup 回收）

## 目标

单个任务 panic 不击穿 worker（recover → 记堆栈 → 尽力写 failed/90001 → 继续认领）；SIGTERM 两阶段退出（停认领 → drain 20s → 超预算强停），停机不伪造业务 failed；数据库持续不可用时受控退出。

## 涉及文件

- Modify：`internal/application/processing/process.go`（单轮执行 defer recover）
- Create：`bootstrap/lifecycle.go`（信号 → 协调器 → 两阶段退出；HTTP Shutdown 与后台 drain 共用 20s）
- Modify：`internal/infrastructure/worker/pool.go`（claimCtx/runCtx 分层；WaitGroup 收口）
- Test：`tests/integration/resilience_test.go`

## 交付接口

```go
// bootstrap/lifecycle.go
type Lifecycle struct{ … }
func Run(ctx context.Context, deps …) error
// 1) signal.NotifyContext 只通知开始退出，不直接当 runCtx（§3.5）
// 2) 就绪置 false → 取消 claimCtx（停认领/停清理）
// 3) 独立有效超时 context 调 Server.Shutdown（HTTP drain 与任务 drain 共用 SHUTDOWN_TIMEOUT=20s）
// 4) 超预算 → 取消 runCtx → 等 worker 收尾 → 关闭数据库；最终退出等待有上限
```

panic 防护语义（§3.2）：阶段代码 panic → 记堆栈与 task 关联字段 → 条件更新尽力写 failed/90001（任务已删除或轮次变化导致写不进则丢弃）→ worker 不退出，继续认领。

## 步骤

- [x] 1. 写失败测试（`tests/integration/resilience_test.go`）：
  - `TestIT17_PanicRecovery`：阶段钩子注入 panic（转写替身内 panic）→ 任务 failed/90001；同一 worker 继续认领下一个任务并成功。
  - `TestIT18_GracefulShutdown`：在途任务执行中发 SIGTERM → 停止认领（新 pending 不被领取）；在途任务要么完成要么保持在途状态（无伪 failed）；进程在超时预算内退出，数据库正常关闭。
  - `TestResilience_DBUnavailableControlledExit`：包装适配器注入持续落库失败 → 受控退出（不就地死循环、不伪状态）。
- [x] 2. 运行确认失败：`TEST_MYSQL_DSN=… go test ./tests/integration/ -run 'TestIT17|TestIT18|TestResilience' -race -v`。
- [x] 3. 实现：process 单轮 `defer func(){ if r := recover(); r != nil { … } }()`；lifecycle 两阶段退出接线（claimCtx/runCtx 在 T06 已建，本任务补齐信号与 Shutdown 编排）。
- [x] 4. 运行确认通过：全绿（`-race`）。
- [x] 5. 手动验证：启动 → 上传（确定性长延迟）→ Ctrl-C → 观察日志两阶段输出与退出码；重启后由 T11 的恢复处理在途任务。
- [ ] 6. 提交：`feat: add panic isolation and graceful shutdown`。

## 完成标准

- [x] IT-17/IT-18 全绿；panic 后 worker 存活，SIGTERM 后无伪 failed。
- [x] 退出等待有上限，WaitGroup 全部回收（`-race` 无泄漏告警）。

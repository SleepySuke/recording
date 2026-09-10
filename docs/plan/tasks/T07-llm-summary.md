# T07：LLM 摘要与完成 / 失败事务

> 依赖：T06 · 预算：1.5h · 状态：**未开始**

**设计依据**：详设 §9（LLM 适配与输出校验）、§3.4（llmCtx 与失败落库 context）、§4.4（条件更新与事件原子性）、§5.5（外部成功但落库失败）；架构 §3 事务③④⑤
**测试依据**：IT-02、IT-05、IT-14；验收清单「异步与真实摘要」第 8～10 条

## 目标

流水线后半段：真实 LLM 适配器（超时/非 2xx/非法输出分类）、事务⑤写 done、事务③写 failed（含 40001 转写失败路径）、条件更新拒绝旧轮次、事件插入失败整体回滚、落库失败受限重试。

## 涉及文件

- Create：`internal/application/ports/summarizer.go`
- Create：`internal/infrastructure/llm/llm.go`（真实适配器）
- Modify：`internal/application/processing/process.go`（摘要阶段与完成/失败落库）
- Modify：`internal/infrastructure/persistence/mysql/processing_tx.go`（新增事务③⑤方法）
- Test：`internal/infrastructure/llm/llm_test.go`（FakeLLM httptest.Server）；`tests/integration/pipeline_test.go`（追加用例）

## 交付接口（后续任务依赖）

```go
// ports/summarizer.go
type Summarizer interface {
    Summarize(ctx context.Context, transcript string) (domain.Summary, error)
    // 错误必须可分类：errors.Is 区分 ErrLLMTimeout(50001) / ErrLLMUpstream(50002) / ErrLLMInvalidOutput(50003)
}

// ProcessingTx 追加：
type ProcessingTx interface { // 完整接口（合并 T06 已有方法）
    ClaimNext(…) / SaveTranscription(…)
    CompleteTask(ctx context.Context, key domain.ExecutionKey, s domain.Summary) error  // 事务⑤：summary_json + done + task_completed
    FailTask(ctx context.Context, key domain.ExecutionKey, code errorcode.ErrorCode, msg string) error // 事务③：failed + 错误 + task_failed
}
```

FakeLLM（`internal/llm` 测试导出或 `tests` 内建，供 IT-14 与后续 E2E 复用）：可编程返回正常 JSON / 挂起至超时 / 非 2xx / 坏 JSON / 缺字段 / 外层结构异常。

## 步骤

- [ ] 1. 写失败测试（单元，FakeLLM + 真适配器指向 FakeLLM）：
  - `TestLLM_HappyPath`：正常响应 → `domain.Summary` 三字段正确。
  - `TestLLM_Timeout`：挂起超过（缩短后的）超时 → `ErrLLMTimeout`；`TestLLM_Upstream`：500 → `ErrLLMUpstream`。
  - `TestLLM_InvalidOutput`（子测试）：坏 JSON、缺字段、类型错误、围栏、多个 JSON 值 → `ErrLLMInvalidOutput`。
  - `TestLLM_BodyCap`：超限响应体被截断并 Close（连接复用不被破坏）。
- [ ] 2. 写失败测试（集成）：
  - `TestIT14_LLMBoundaryMapping`：FakeLLM 分别返回挂起/500/坏 JSON/缺字段/正常 → 任务依次 failed+50001 / failed+50002 / failed+50003 / failed+50003 / done+summary_json。
  - `TestIT02_EventFailureRollsBack`：预置重复 event_id 制约冲突 → 事务④/⑤整体回滚，无半条记录（状态、产物、事件都不变）。
  - `TestIT05_StaleAttemptRejected`：attempt+1 后用旧 attempt 条件更新 → RowsAffected=0，结果丢弃不复活。
  - `TestPipeline_FullSuccess`：确定性替身全链路 → done，事件链 4 条完整（IT-19 的成功分支）。
- [ ] 3. 运行确认失败：`go test ./internal/infrastructure/llm/... -v`；`TEST_MYSQL_DSN=… go test ./tests/integration/ -run 'TestIT02|TestIT05|TestIT14|TestPipeline' -v`。
- [ ] 4. 实现要点：
  - 适配器：T01 选定渠道的请求格式；复用 `http.Client`；`NewRequestWithContext(taskCtx+LLM_TIMEOUT)`；响应体上限 + 及时 Close；不向错误消息泄漏 Key 或堆栈（详设 §9）。
  - Process 流程扩展：SaveTranscription 成功 → `llmCtx` 调摘要 → 成功走事务⑤；任何失败（含转写失败 40001）走事务③。**失败落库用 runCtx 派生的新短期限 context，不用已超时的 llmCtx**（§3.4）。
  - 落库失败（DB 不可用）：保留产物，间隔 200ms/500ms 重试 ≤2 次，仍失败 → 记日志并受控停止该 worker 的推进（§5.5；受控退出机制 T10 完善，本任务先停止认领并 ERROR 日志）。
  - 转写失败路径（40001）同样经事务③落库，与 LLM 失败共用 FailTask。
- [ ] 5. 运行确认通过：全部命令全绿。
- [ ] 6. 真实 LLM 冒烟（P0，不留到交付）：配置真实渠道跑一次完整上传 → done，保存脱敏的请求/响应摘要到本文件末尾记录。
- [ ] 7. 提交：`feat: integrate llm summarization with atomic completion`。
- [ ] 8. E2E：扩展 `e2e/` 的 E2E-01 验收终点 summarizing → done（事件链、详情 result 三字段），并注入 LLM 替身覆盖边界（映射表见测试设计 §5.0）。

## 完成标准

- [ ] IT-02/05/14 全绿；超时、非 2xx、坏 JSON、缺字段、类型错误全部进入 failed 且错误码正确。
- [ ] 真实 LLM 至少一次成功全链路（done + 三摘要字段），记录在案。

## 真实调用冒烟记录（步骤 6 填写，脱敏）

- 时间 / 模型 / 耗时 / 摘要字段摘录（不含敏感内容）：

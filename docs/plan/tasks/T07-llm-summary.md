# T07：LLM 摘要与完成 / 失败事务

> 依赖：T06 · 预算：1.5h · 状态：**已完成（除真实渠道冒烟，见冒烟记录）**

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

> 实际落点（遵循测试目录约定）：单元测试在 `tests/unit/llm_test.go` 与
> `tests/unit/process_summary_test.go`（后者覆盖 §5.5 落库重试边界）；集成用例在
> `tests/integration/pipeline_test.go`。FakeLLM 导出在生产包
> `internal/infrastructure/llm/fake.go`（与 `asr/mock` 确定性替身同约定，供 tests/ 与 e2e/ 复用）。

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

- [x] 1. 写失败测试（单元，FakeLLM + 真适配器指向 FakeLLM）：
  - `TestLLM_HappyPath`：正常响应 → `domain.Summary` 三字段正确。
  - `TestLLM_Timeout`：挂起超过（缩短后的）超时 → `ErrLLMTimeout`；`TestLLM_Upstream`：500 → `ErrLLMUpstream`。
  - `TestLLM_InvalidOutput`（子测试）：坏 JSON、缺字段、类型错误、围栏、多个 JSON 值 → `ErrLLMInvalidOutput`。
  - `TestLLM_BodyCap`：超限响应体被截断并 Close（连接复用不被破坏）。
  - 追加：`TestLLM_ParentCancel`（父级取消透传 context.Canceled，§3.4）与
    `tests/unit/process_summary_test.go`（失败码映射 / 取消不落伪 failed / stale 丢弃 /
    §5.5 重试边界与停止认领）。
- [x] 2. 写失败测试（集成）：
  - `TestIT14_LLMBoundaryMapping`：FakeLLM 分别返回挂起/500/坏 JSON/缺字段/正常 → 任务依次 failed+50001 / failed+50002 / failed+50003 / failed+50003 / done+summary_json。
  - `TestIT02_EventFailureRollsBack`：预置重复 event_id 制约冲突 → 事务④/⑤整体回滚，无半条记录（状态、产物、事件都不变）。
  - `TestIT05_StaleAttemptRejected`：attempt+1 后用旧 attempt 条件更新 → RowsAffected=0，结果丢弃不复活。
  - `TestPipeline_FullSuccess`：确定性替身全链路 → done，事件链 4 条完整（IT-19 的成功分支）。
  - 说明：原 `TestPipeline_TranscribeToSummarizing` 由 `TestPipeline_FullSuccess` 替代——
    流水线接入摘要段后不再停在 summarizing，轮询中间态会 flaky，中间态断言改由事件链还原。
- [x] 3. 运行确认失败：`go test ./internal/infrastructure/llm/... -v`；`TEST_MYSQL_DSN=… go test ./tests/integration/ -run 'TestIT02|TestIT05|TestIT14|TestPipeline' -v`。
- [x] 4. 实现要点：
  - 适配器：T01 选定渠道的请求格式（小米 MiMo，OpenAI 兼容 chat/completions + Bearer）；复用 `http.Client`；`NewRequestWithContext(taskCtx+LLM_TIMEOUT)`；响应体上限 + 及时 Close；不向错误消息泄漏 Key 或堆栈（详设 §9）。
  - Process 流程扩展：SaveTranscription 成功 → `llmCtx` 调摘要 → 成功走事务⑤；任何失败（含转写失败 40001）走事务③。**失败落库用 runCtx 派生的新短期限 context，不用已超时的 llmCtx**（§3.4）。
  - 落库失败（DB 不可用）：保留产物，间隔 200ms/500ms 重试 ≤2 次，仍失败 → 记日志并受控停止该 worker 的推进（§5.5；受控退出机制 T10 完善，本任务先停止认领并 ERROR 日志）。
  - 转写失败路径（40001）同样经事务③落库，与 LLM 失败共用 FailTask。
- [x] 5. 运行确认通过：全部命令全绿。
- [ ] 6. 真实 LLM 冒烟（P0，不留到交付）：配置真实渠道跑一次完整上传 → done，保存脱敏的请求/响应摘要到本文件末尾记录。
- [ ] 7. 提交：`feat: integrate llm summarization with atomic completion`。（本次执行按指示不提交，改动留在工作区）
- [x] 8. E2E：扩展 `e2e/` 的 E2E-01 验收终点 summarizing → done（事件链、详情 result 三字段），并注入 LLM 替身覆盖边界（映射表见测试设计 §5.0）。
  - E2E-01/07/08 与三条 golden（E01/E07/E08.json）已扩展至 done；harness 的 LLM 三项指向
    进程内 FakeLLM（真实适配器 + 真实 HTTP）。边界注入用例随 T08（E2E-03）落地。

## 完成标准

- [x] IT-02/05/14 全绿；超时、非 2xx、坏 JSON、缺字段、类型错误全部进入 failed 且错误码正确。
- [ ] 真实 LLM 至少一次成功全链路（done + 三摘要字段），记录在案。

## 真实调用冒烟记录（步骤 6 填写，脱敏）

- 时间 / 模型 / 耗时 / 摘要字段摘录（不含敏感内容）：
- **无 Key，跳过，留缺口**（截至 2026-09-09 无 MiMo API Key；既有决定为跳过并记录缺口。
  Key 到位后补跑：配置 `LLM_BASE_URL/LLM_MODEL/LLM_API_KEY` 真实渠道，上传一次 → done，
  在此记录脱敏摘要即可）。

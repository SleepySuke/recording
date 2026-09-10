# T16：自动重试、摘要 SSE 与联调页

依赖：T15；预算：2.5h；状态：完成（2026-09-10）。

**设计依据**：[自动重试与摘要事件流设计](../../design/retry-and-summary-stream.md)；[详细技术设计](../../design/technical-design.md) §3～§9。
**测试依据**：IT-19 跨轮次事件链、E2E-02/03 自动退避流程、E2E-10 SSE 终态流。

## 目标

为可重试的 worker 失败提供持久化的最多三轮指数退避，并提供只读 SSE 任务阶段/终态摘要流；`/ui` 使用 SSE 展示摘要过程，保留轮询和人工最终重试作为降级路径。

## TDD 步骤

- [x] 1. RED/GREEN：迁移、自动调度和 E2E-02/03 的真实 HTTP golden 已覆盖；IT-19 同步验证跨轮次事件链。
- [x] 2. GREEN：实现 `next_retry_at`、到期认领、原子失败调度与事件；手动 retry 仅接受 final failed。
- [x] 3. GREEN：实现 SSE handler（status、心跳、done/failed/deleted），无新增依赖；E2E-10 经真实 TCP 验证 status → summary。
- [x] 4. 联调页：EventSource 连接、自动重试等待与最终摘要展示；最终失败才显示人工重试。
- [x] 5. 回写架构、测试设计、README、HANDOFF；门禁结果见验收记录。

建议提交：`feat: add persistent automatic retry and summary SSE`。

## 验收记录

- `go build ./...`、`go vet ./...`、`gofmt -l .` 通过。
- `go test ./tests/integration/ -race -run TestIT19_EventChainReconstructable`：通过，验证两次自动调度、第三轮失败和随后人工重试。
- `go test ./e2e/ -race -run 'TestE2E02|TestE2E03'`：通过，验证 Mock ASR 与 LLM 超时的自动退避后完成。
- `go test ./e2e/ -race -run TestE2E10_SummaryStream`：通过，验证真实 TCP SSE 的 `status → summary`。

# T01：工程骨架与错误码注册表

> 依赖：无（起点任务） · 预算：1h · 状态：**进行中**（仅剩步骤 2 MiMo 渠道验证，待 API Key；2026-09-09）

**设计依据**：详设 §1.6（框架与库）、§2.2（目录）、§2.3（分层依赖）、§7.4（日志文件与轮转）、§8.2～§8.4（错误码）、§8.6（中间件）；架构 §6（healthz / readyz）
**测试依据**：UT-08；§8.6 统一错误渲染与 404/405 行为

## 目标

建立可编译可启动的 HTTP 骨架与全局错误码体系：Gin 路由 + 四中间件 + 健康接口 + slog JSON 双写（stdout + logs/app.jsonl）+ 数字错误码注册表。同时解决 P0 阻塞项：选定真实 LLM 渠道并跑通最小调用。

## 涉及文件

- Create：`go.mod`、`cmd/server/main.go`
- Create：`bootstrap/config.go`（环境变量读取与校验）、`bootstrap/wire.go`（最小组装）
- Create：`internal/application/errorcode/errorcode.go`
- Create：`internal/interfaces/http/router.go`、`internal/interfaces/http/errorcode/render.go`
- Create：`internal/interfaces/http/middleware/requestid.go`、`accesslog.go`、`errorrender.go`、`panicrecovery.go`
- Create：`internal/infrastructure/logging/logging.go`（slog 初始化 + 文件轮转）
- Create：`.env.example`、`compose.yaml`（本任务只含 db 服务 + healthcheck，app 服务 T12 再加）
- Test：`tests/unit/errorcode_test.go`、`tests/unit/router_test.go`（2026-09-09 约定调整：测试统一放 `tests/` 目录——unit / integration 分层，不与实现代码同目录；实现只暴露可测的导出 API）

## 交付接口（后续任务依赖）

```go
// internal/application/errorcode —— 供 HTTP 与 worker 共用（详设 §2.4）
type ErrorCode int                       // 具名整数，编号为显式常量，禁止 iota
const (
    CodeInvalidArgument          ErrorCode = 10001
    CodeRouteNotFound            ErrorCode = 10002
    CodeMethodNotAllowed         ErrorCode = 10003
    CodeFileRequired             ErrorCode = 20001
    CodeEmptyFile                ErrorCode = 20002
    CodeUnsupportedExtension     ErrorCode = 20003
    CodeFileTooLarge             ErrorCode = 20004
    CodeRecordingNotFound        ErrorCode = 20005
    CodeRecordingDeletePending   ErrorCode = 20006
    CodeTaskNotFound             ErrorCode = 30001
    CodeTaskNotRetryable         ErrorCode = 30002
    CodeServiceInterrupted       ErrorCode = 30003
    CodeASRFailed                ErrorCode = 40001
    CodeLLMTimeout               ErrorCode = 50001
    CodeLLMUpstreamError         ErrorCode = 50002
    CodeLLMInvalidOutput         ErrorCode = 50003
    CodeInternalError            ErrorCode = 90001
    CodeDataInconsistent         ErrorCode = 90004
    CodeDatabaseUnavailable      ErrorCode = 90002
    CodeFileStorageUnavailable   ErrorCode = 90003
)
func (c ErrorCode) HTTPStatus() int  // 详设 §8.3 的 HTTP 映射；异步码（40001/50001/50002/50003/30003）返回 0 表示不直接映射
func (c ErrorCode) String() string   // 助记名
func (c ErrorCode) Message() string  // 公开消息（中文）
type AppError struct{ Code ErrorCode; Cause error }
```

统一错误响应结构（X-Request-ID 与响应体一致，详设 §8.2）：`{"error":{"code":…,"message":…,"request_id":"…"}}`

## 步骤

- [x] 1. 初始化仓库与模块：`git init`；确认 `.gitignore` 覆盖 `.env`、`logs/`、`data/`；首次提交全部现有文档（`chore: import design documents`）。`go mod init recording-transcription`，`go get` 固定 gin / gorm.io/gorm / gorm.io/driver/mysql / 轮转库（编码时验证版本）。（2026-09-09：gin v1.10.0 / gorm v1.31.2 / driver/mysql v1.6.0 / lumberjack v2.2.1；gin 最新 v1.12 需 go≥1.25 且引入 quic-go 重依赖，锁定 v1.10.0 + go 1.23）
- [ ] 2. 验证 LLM 渠道（P0，已定 **小米 MiMo**）：取得 API Key（[platform.xiaomimimo.com](https://platform.xiaomimimo.com/) 控制台「API Keys」页申请）写入本地 `.env`（不进 Git）；用 curl 跑通一次最小 chat 调用（OpenAI 兼容协议）；确定具体模型名并记入本文件末尾「渠道记录」小节，更新 `.env.example` 注释。（**待用户提供 API Key**）
- [x] 3. 写失败测试（UT-08）：`TestUT08_ErrorCodeRegistry` —— 断言编号全局唯一；每个同步码的 `HTTPStatus()` 与 §8.3 表逐项相等；`String()`/`Message()` 非空；异步码不映射 HTTP 状态。
- [x] 4. 写失败测试（路由）：`TestRouter_NoRoute`（未知路由 404 + code 10002）、`TestRouter_NoMethod`（405 + code 10003 + 正确 `Allow` 头）、`TestRouter_PanicRecovery`（handler panic → 500 + 90001，不泄漏堆栈）、`TestRouter_RequestID`（响应头与错误体 request_id 一致）。
- [x] 5. 运行确认失败：`go test ./internal/... -v`（包不存在 / 编译失败即为预期失败）。
- [x] 6. 实现要点：
  - `gin.New()` 显式装中间件，外→内：RequestID → AccessLog → ErrorRenderer → PanicRecovery（详设 §8.6）；NoRoute/NoMethod 走统一错误渲染。
  - `/healthz` 200；`/readyz` 本任务先返回 200（就绪门控在 T11 接入恢复流程后启用，加 `// T11 接入真实就绪门控` 注释）。
  - logging：slog JSONHandler 双写 stdout + `logs/app.jsonl`（`LOG_DIR`，默认 `./logs`），轮转 20MiB/5 备份/7 天（详设 §7.4）；`LOG_LEVEL` 只影响运行诊断，事件镜像不受级别过滤。
  - config：读取并校验详设 §10 的配置初值（HTTP_ADDR/LOG_DIR/LOG_LEVEL/WORKER_CONCURRENCY/TASK_POLL_INTERVAL/LLM_*/DB_*），缺失必填项启动即报错，不静默降级。
  - compose.yaml：仅 db（mysql:8.4 + healthcheck + 数据卷），供本任务起本地依赖。
- [x] 7. 运行确认通过：`go test ./internal/... -v` 全绿。（含 `-race`、`go vet`）
- [x] 8. 冒烟：`go run ./cmd/server` 后 `curl :8080/healthz` 200；`curl :8080/nope` 返回 404 与 10002；对 `/healthz` 发 POST 返回 405 且带 `Allow: GET`；`logs/app.jsonl` 有访问日志。（2026-09-09 全部通过；本地启动需 `set -a; source .env; set +a`，Makefile 封装在 T12）
- [ ] 9. 提交：`chore: bootstrap service skeleton and error registry`。（**待用户执行 commit**）

## 完成标准

- [x] UT-08 与四个路由测试通过；错误响应结构统一且 request_id 一致。
- [ ] LLM 渠道已定、最小调用成功、记录在案（去掉 Key）。（待 API Key）
- [x] 服务可启动，healthz/readyz/404/405/panic 五条路径行为符合 §8.6。

## 渠道记录（步骤 2 完成后补齐）

- 供应商：小米 MiMo 开放平台（已定）
- 模型：候选 mimo-v2.5-pro / mimo-v2-pro / mimo-v2-flash（步骤 2 确定并填写）：
- Base URL：`https://api.xiaomimimo.com/v1`（OpenAI 兼容；另有 Anthropic 兼容端点，本项目不用）
- 请求格式要点：标准 OpenAI chat/completions——`Authorization: Bearer $MIMO_API_KEY`、`messages` 数组、可选 `response_format` JSON 模式（步骤 2 验证后确认是否启用）
- 最小调用样例（步骤 2 验证后替换为实际可用形式）：

```bash
curl https://api.xiaomimimo.com/v1/chat/completions \
  -H "Authorization: Bearer $MIMO_API_KEY" -H "Content-Type: application/json" \
  -d '{"model":"mimo-v2-flash","messages":[{"role":"user","content":"回复一个 JSON 对象 {\"ok\":true}"}]}'
```

# T14：交付验收与文档回写

> 依赖：T13 · 预算：0.5h · 状态：**完成（真实 LLM 冒烟按既定决策留缺口，见步骤 2）**

**设计依据**：需求原文提交内容四项（README / 一键启动 / API 调试文件 / 完整 commit 历史）；测试设计附录 A（真实 LLM 人工验收）；HANDOFF §4（代码实现后必须回写 README）
**测试依据**：验收清单「交付」全条目；附录 A 三条

## 目标

交付四件材料齐备且据实：`api/recordings.http`、README 回写（功能状态与运行验证结果）、真实 LLM 人工验收记录、仓库卫生与 commit 历史检查。

## 涉及文件

- Create：`api/recordings.http`（覆盖六个接口）
- Modify：`README.md`（据实回写）
- Modify：`HANDOFF.md`（更新当前状态与剩余缺口，或标注完成）
- Modify：本文件（记录验收结果）

实际变更（含控制器补充项，2026-09-09 用户追加）：另 Create `static/index.html`（六接口联调页，`GET /ui` 同源托管）；Modify `internal/interfaces/http/router.go`（挂 /ui）、`Dockerfile`（static/ 入镜像）、`.dockerignore`（注释明示 static/ 不排除）、`cmd/server/main.go`（静默启动失败修复）、`e2e/compose_smoke_test.go`（T13 遗留两个 Minor 顺手修）。

## 步骤

- [x] 1. 编写 `api/recordings.http`：六个接口 + 变量化的 {{base_url}}/{{recording_id}}/{{task_id}} 占位；附注释说明「查看失败任务并重试」的操作路径（GET 任务 → 确认 failed → retry）。另附健康/就绪接口与 /ui 联调页说明。
- [ ] 2. 真实 LLM 人工验收（测试设计附录 A，逐条）——**按既定决策留缺口**（2026-09-09 用户裁定无 MiMo Key，不做真实调用；README「已知问题与缺口」已如实说明，HANDOFF §8 留档）：
  - [ ] 真实渠道上传 → done，三个摘要字段来自真实模型（脱敏样例记入 README）。**待 Key**。
  - [ ] 断网 / 改错 Key → failed/50002，响应不含 Key 与堆栈。**待 Key**；占位 Key 直连真实端点已实测：failed/50002（HTTP 401），错误消息仅含状态码——与预期一致（见验收记录）。
  - [ ] 核对调用次数与费用符合预期（重启恢复场景注意重复调用）。**待 Key**。
- [x] 3. README 回写：功能状态表改为实际状态；运行命令与验证结果据实更新（去掉"设计阶段/尚未实现"措辞）；补已知问题与未完成项（单实例约束、重启可能重复调用 LLM、无上传幂等、12h interrupt 默认关闭、真实渠道冒烟缺口、GOPROXY/镜像源备注）；表结构章节指向 migrations/0001_init.sql。
- [x] 4. 演示走查（原计划演示顺序）：启动展示迁移 → 上传 → 查询阶段 → 成功摘要 → 失败与手动重试 → 分页列表 → 删除与清理确认 → 核心测试运行。可脚本化部分全部实跑（见验收记录）；在途重启恢复与 Ctrl-C 两阶段退出引用既有证据（E2E-06 / T10 报告）。
- [x] 5. 仓库卫生：无 API Key / 真实录音 / 数据库文件 / 构建产物入库（git grep 秘密模式无命中、无音视频/二进制/运行产物入库）；commit 历史由用户逐任务补丁流保证（T00、T01～T06 已提交 7 个，T06E～T13 补丁齐备于 sdd/patches/ + commit-plan.md 顺序脚本，T14 补丁随后生成——合计 ≥14 个功能提交、无一次性大提交）。
- [ ] 6. 提交：`docs: finalize delivery docs and acceptance records`。（用户手动提交，本任务不代提交）

## 完成标准

- [x] 新环境按 README 两步启动成功（`make setup` → `make start` 路径 T12 已验；本任务 `make build && make start` 实跑至 readyz 就绪，/ui 200）；调试文件可直接导入使用。
- [x] README 与实际行为一致（六接口行为、make 语义、测试结果均实跑或引用真实证据）；需求四项提交材料全部就位。真实 LLM 脱敏样例为唯一缺口（无 Key，步骤 2 留档）。

## 验收记录（2026-09-10 实测）

### 五道门禁 + 附加

```
$ GOTOOLCHAIN=local go build ./... && go vet ./... && gofmt -l .
BUILD_VET_FMT_OK                                   # gofmt 输出为空

$ GOTOOLCHAIN=local go test ./tests/unit/ -race -count=1
ok  	recording-transcription/tests/unit	2.602s

$ TEST_MYSQL_DSN='recording:…@tcp(127.0.0.1:3306)/recording_test?…' GOTOOLCHAIN=local go test ./tests/integration/ -race -count=1
ok  	recording-transcription/tests/integration	14.839s

$ TEST_MYSQL_DSN='…' GOTOOLCHAIN=local go test ./e2e/ -race -count=1
ok  	recording-transcription/e2e	5.298s          # e2e/diff/ 条目数 = 0（golden 全部深度相等）

$ TEST_MYSQL_DSN='…' GOTOOLCHAIN=local go test ./... -race -count=1
ok  	recording-transcription/e2e	4.632s
ok  	recording-transcription/tests/integration	17.513s
ok  	recording-transcription/tests/unit	2.712s

$ make lint
GOTOOLCHAIN=local go vet ./...
OK     gofmt
提示：未安装 golangci-lint，已跳过（go vet + gofmt 已执行）
```

### 静默启动失败修复验证（控制器补充项 3）

错误 DSN 下 `go run ./cmd/server` 此前无任何输出即退出；修复后：

```
$ MYSQL_DSN='recording:wrongpass@tcp(127.0.0.1:3307)/nonexistent?…' go run ./cmd/server
启动失败: 连接 MySQL 失败: Error 1045 (28000): Access denied for user 'recording'@'…' (using password: YES)
exit status 1
```

同步修正 main.go 中「Run 已记日志」的不实注释（NewApp 在 logger 建立前失败时本就无日志可写）。

### 演示走查（make start 路径，真实容器 + 生产 Mock ASR + 真实 MiMo 端点）

```
$ make build && make start          # 镜像重建（router/Dockerfile 变更）→ up -d → 等待 readyz
 Container recording-transcription-app-1 Recreated / Started
等待 http://localhost:8080/readyz 就绪
$ curl -s http://localhost:8080/readyz        → {"status":"ready"}
$ curl -s -o /dev/null -w '%{http_code}\n' http://localhost:8080/ui   → 200（容器内命中 ./static/index.html）
$ docker logs recording-transcription-app-1 | head -1                  → {"…","msg":"db ready","dsn_database":"recording"}   # 迁移已执行

$ curl -F "file=@/tmp/t14-demo.wav;type=audio/wav" localhost:8080/v1/recordings
{"recording_id":"508493d2-…","task_id":"416f41fc-…","status":"pending"}    # HTTP 202

# 轮询（每次变化记录）：
00:04:30 transcribing 1
00:04:36 summarizing 1            # 生产 Mock 随机 5～15s 转写后进入摘要
00:04:37 failed 1 50002           # 真实 MiMo 端点 + 占位 Key → HTTP 401 → 50002
$ curl localhost:8080/v1/recordings/508493d2-…   # 详情：task.error={"code":50002,"message":"llm upstream error: HTTP 401"}，
                                                 # transcript=[mock-asr] transcript for task 416f41fc-…（seed=…），result=null

$ curl -X POST localhost:8080/v1/tasks/416f41fc-…/retry
{"task_id":"416f41fc-…","status":"pending","attempt":2}                    # HTTP 202；随后列表观察到 transcribing 重跑

$ curl 'localhost:8080/v1/recordings?page=1&page_size=5'                   # total=4，本任务行可见（failed→transcribing）
$ curl -X DELETE localhost:8080/v1/recordings/508493d2-…                   # HTTP 204
$ （30s 清理周期后）curl '…/v1/recordings?page=1&page_size=5'              # total=3，行已移除
```

成功摘要路径（done + result 三字段）：确定性替身下由 E2E golden E01 与 compose 冒烟 E-COMPOSE 全量验证（T13 报告，golden 深度相等）；真实模型样例待 Key（步骤 2 缺口）。在途重启恢复引用 E2E-06；Ctrl-C 两阶段退出引用 T10 报告（均在既有任务留档，未重复演练）。

### 仓库卫生

`git grep` 秘密模式（sk-/API Key 赋值/私钥头）在已跟踪文件无命中；无 .wav/.mp3/.exe/构建产物入库；logs/、data/、e2e/actual|diff 均被 .gitignore 覆盖；.env 不入库（占位 Key 仅存于本地 .env）。commit 历史要求由用户补丁流满足（见步骤 5）。

### 控制器补充项落实

1. 联调页：`static/index.html`（单文件、无框架、原生 JS、中文），`router.go` 一行 `r.StaticFile("/ui", "./static/index.html")`；Dockerfile 追加 `COPY /src/static/ /app/static/`（WORKDIR /app 与本地仓库根均命中）；.dockerignore 未排除 static/ 并注释明示。README 一句话注明开发辅助属性。
2. HANDOFF §8「需求对照与剩余缺口」：P0 全项对照、真实 LLM 冒烟缺口留档、加分项 3 达成（#2/#5/#6）4 未做（#1/#3/#4/#7，注明 #4 成本最低）及需求原文引句；全文无措辞红线词。
3. 静默启动失败：见上方验证。
4. T13 两个 Minor 顺手修：compose_smoke_test.go:228 格式串 fakeHost→lastStatus（语义回位）；复原失败 t.Log→t.Errorf。镜像名推导项未做（牵扯更大，维持 T13 记录）。

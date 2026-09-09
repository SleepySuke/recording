# T12：一键启动——Makefile / Dockerfile / Compose

> 依赖：T11 · 预算：0.5h · 状态：**未开始**

**设计依据**：架构 §6（部署拓扑、启动顺序、卷与日志挂载）；README「运行方式」的目标约定（Make 目标语义已在此定稿）；详设 §10（配置初值）
**测试依据**：验收清单「交付」第 1～6 条

## 目标

新环境两步启动：`make setup` → `make start` 至 `/readyz` 就绪。Makefile 十个目标、Dockerfile、最终 compose（app + db、健康检查、持久化卷、logs 绑定）、`.env.example` 补全。

## 涉及文件

- Create：`Makefile`、`Dockerfile`
- Modify：`compose.yaml`（补 app 服务：构建、依赖 db 健康检查、录音卷、`./logs` 绑定、`/readyz` 健康检查）
- Modify：`.env.example`（补全全部配置项与注释）
- Modify：`.gitignore`（确认覆盖构建产物）

## 交付接口

Make 目标语义（README 已定稿，逐条实现）：`check`（Docker/Compose/Go/.env 逐项 OK/缺失，可单独执行）、`setup`（复制 `.env.example` → `.env` 并提示）、`build`（显式构建镜像）、`start`（前置 check；复用已有镜像启动 app+db，等 `/readyz`）、`dev`（前置 check；只起 db，应用 `go run` 连本地）、`down`（移除容器，保留卷与 logs）、`logs`、`test`（单元恒跑；集成需 TEST_MYSQL_DSN 否则跳过；E2E 目标在 T13 接入）、`lint`（go vet + golangci-lint）、`clean`。

## 步骤

- [ ] 1. 实现 Dockerfile：多阶段构建（builder 缓存 go mod → 最小运行镜像），非 root 用户，EXPOSE 8080。
- [ ] 2. 实现 Makefile 与 compose 最终版（app 服务 `depends_on: db: condition: service_healthy`；两个数据卷；`./logs:/app/logs` 绑定；app healthcheck 打 `/readyz`）。
- [ ] 3. 验证清单（干净环境路径，逐项打勾）：
  - [ ] `make check`：逐项输出 OK/缺失，单独可执行。
  - [ ] `make setup && make start`：从零到 `/readyz` 200；迁移自动执行，无需手动建表。
  - [ ] `make start` 幂等：镜像已存在直接复用；`make build` 后才重建。
  - [ ] `make dev`：本地起 db + `go run`，日志到终端。
  - [ ] `make down && make start`：容器重建后录音文件与 MySQL 数据仍在（卷持久化）。
  - [ ] `make logs` 跟踪；宿主机 `./logs/app.jsonl` 可写。
  - [ ] 迁移失败注入（改坏 DSN）→ 应用不就绪（健康检查不过）。
  - [ ] `make lint` 通过。
- [ ] 4. 提交：`build: add makefile, dockerfile and compose stack`。

## 完成标准

- [ ] 新环境（清掉镜像/卷）两步启动至就绪，全程无手工 SQL。
- [ ] README「运行方式」表格中的每条命令都真实存在且行为一致（README 正文回写留 T14 统一做）。

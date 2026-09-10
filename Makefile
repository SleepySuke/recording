# 一键启动（架构 §6 部署拓扑 / README「运行方式」）：日常操作统一经 Makefile 进入，
# 底层封装 Docker Compose 与 Go 工具链。目标语义与 README 表格逐条对应。
#
# 本机 Go 为 1.23.x 且 GOTOOLCHAIN=local（gin v1.10.0 依赖锁定要求），
# 故所有 go 命令统一经 GO 变量带该前缀。
SHELL := /bin/bash

COMPOSE := docker compose
GO := GOTOOLCHAIN=local go
READYZ_URL := http://localhost:8080/readyz

.DEFAULT_GOAL := help
.PHONY: help check setup build start dev down logs test lint clean

help: ## 显示所有可用目标
	@awk -F':.*## ' '/^[a-zA-Z0-9_-]+:.*## / {printf "  \033[36m%-8s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

check: ## 环境自检：Docker/Compose/Go/.env 关键配置逐项输出 OK/缺失（start 与 dev 自动先行调用）
	@fail=0; \
	if docker version >/dev/null 2>&1; then \
		printf 'OK     docker（server %s）\n' "$$(docker version --format '{{.Server.Version}}')"; \
	else \
		printf '缺失   docker 守护进程（先启动 Docker Desktop）\n'; fail=1; \
	fi; \
	if $(COMPOSE) version >/dev/null 2>&1; then \
		printf 'OK     docker compose（%s）\n' "$$($(COMPOSE) version --short)"; \
	else \
		printf '缺失   docker compose 插件\n'; fail=1; \
	fi; \
	if $(GO) version >/dev/null 2>&1; then \
		printf 'OK     go（%s）\n' "$$($(GO) version | awk '{print $$3}')"; \
	else \
		printf '缺失   go 工具链\n'; fail=1; \
	fi; \
	if [ ! -f .env ]; then \
		printf '缺失   .env（先执行 make setup）\n'; exit 1; \
	fi; \
	printf 'OK     .env\n'; \
	if grep -Eq '^MYSQL_DSN=..*' .env; then \
		printf 'OK     .env MYSQL_DSN\n'; \
	else \
		printf '缺失   .env MYSQL_DSN\n'; fail=1; \
	fi; \
	if grep -Eq '^LLM_API_KEY=..*' .env; then \
		printf 'OK     .env LLM_API_KEY\n'; \
	else \
		printf '缺失   .env LLM_API_KEY（编辑 .env 填入 Key 后再启动）\n'; fail=1; \
	fi; \
	exit $$fail

setup: ## 初始化：复制 .env.example 为 .env（已存在则不覆盖），随后做环境自检
	@if [ -f .env ]; then \
		printf '.env 已存在，保持不变（如需重置：rm .env && make setup）\n'; \
	else \
		cp .env.example .env; \
		printf '已生成 .env：请编辑填写 LLM_API_KEY（platform.xiaomimimo.com 申请）\n'; \
	fi
	@$(MAKE) --no-print-directory check \
		|| printf '提示：补齐上述缺失项后再 make start / make dev\n'

build: ## 显式构建/更新 app 镜像（代码变更后执行；start 不自动重建已有镜像）
	$(COMPOSE) build app

start: ## 一键启动：先自检；up -d 起 app+db（复用已有镜像与容器，仅缺失时构建/拉取），等待 /readyz 就绪
	@$(MAKE) --no-print-directory check
	$(COMPOSE) up -d
	@printf '等待 %s' "$(READYZ_URL)"; \
	for i in $$(seq 1 60); do \
		code=$$(curl -s -o /dev/null -w '%{http_code}' $(READYZ_URL) 2>/dev/null || true); \
		if [ "$$code" = "200" ]; then printf ' 就绪\n'; exit 0; fi; \
		printf '.'; sleep 2; \
	done; \
	printf '\n超时未就绪：make logs 查看原因\n'; exit 1

dev: ## 本地开发：先自检；只以 Compose 起 db（已在运行则直接复用），应用 go run 连本地 .env，日志到终端
	@$(MAKE) --no-print-directory check
	$(COMPOSE) up -d --wait db
	@set -a; . ./.env; set +a; \
	printf 'go run ./cmd/server（Ctrl-C 退出应用；db 用 make down 停止）\n'; \
	$(GO) run ./cmd/server

down: ## 停止并移除容器（start 与 dev 通用）；保留数据卷与 ./logs
	$(COMPOSE) down

logs: ## 跟踪 app 与 db 容器日志（Ctrl-C 退出）
	$(COMPOSE) logs -f app db

test: ## 单元恒跑；集成需 TEST_MYSQL_DSN（未设则跳过并提示）
	$(GO) test ./tests/unit/ -race -count=1
	@if [ -n "$$TEST_MYSQL_DSN" ]; then \
		$(GO) test ./tests/integration/ -race -count=1; \
	else \
		printf 'TEST_MYSQL_DSN 未设置，跳过集成测试（DSN 示例见 .env.example 注释）\n'; \
	fi
	# E2E（make e2e：compose 临时起栈 + golden 逐字段比对）由 T13 接入，此处不重复起栈。

lint: ## go vet + gofmt；装有 golangci-lint 则一并执行，未装则一行提示后跳过
	$(GO) vet ./...
	@out=$$(gofmt -l .); \
	if [ -n "$$out" ]; then printf 'gofmt 未格式化文件：\n%s\n' "$$out"; exit 1; \
	else printf 'OK     gofmt\n'; fi
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run ./...; \
	else \
		printf '提示：未安装 golangci-lint，已跳过（go vet + gofmt 已执行）\n'; \
	fi

clean: ## 清理构建产物与本地 logs/（不动容器与数据卷；容器用 make down）
	rm -rf bin logs

# 多阶段构建（架构 §6 部署拓扑）：builder 产出静态二进制 → 最小运行镜像。
# 基础镜像名可用 build-arg 覆盖：docker.io 直连受限的网络在 .env 设镜像加速前缀
#（如 docker.m.daocloud.io/library/，见 .env.example GO_IMAGE / RUNTIME_IMAGE 注释）。
ARG GO_IMAGE=golang:1.23-alpine
ARG RUNTIME_IMAGE=alpine:3.20
# builder：go.mod/go.sum 单独一层先行下载，依赖不变时命中层缓存；
# 国内网络默认 goproxy.cn，可用 --build-arg GOPROXY=... 覆盖。
FROM ${GO_IMAGE} AS builder
ARG GOPROXY=https://goproxy.cn,direct
ENV GOPROXY=${GOPROXY}
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# CGO 关闭产出静态二进制，运行镜像无需工具链与 glibc；-s -w 去符号表减体积。
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags='-s -w' -o /out/server ./cmd/server

# 运行阶段：alpine 最小镜像；ca-certificates 供 LLM 出站 HTTPS（详设 §9）。
# 非运行时目录一律不带入；非 root 用户运行。
FROM ${RUNTIME_IMAGE}
RUN apk add --no-cache ca-certificates \
    && addgroup -S app && adduser -S app -G app \
    # 预建数据/日志目录并归属 app：命名卷首次挂载沿用镜像内目录属主，非 root 才能写入
    && mkdir -p /app/data/recordings /app/logs \
    && chown -R app:app /app
WORKDIR /app
COPY --from=builder --chown=app:app /out/server /app/server
# 启动时从相对目录 migrations/ 读 SQL 执行迁移（bootstrap.NewApp），必须随镜像交付。
COPY --from=builder --chown=app:app /src/migrations/ /app/migrations/
USER app
EXPOSE 8080
ENTRYPOINT ["/app/server"]

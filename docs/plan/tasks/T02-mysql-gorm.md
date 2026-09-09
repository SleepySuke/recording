# T02：MySQL 迁移与 GORM 基建

> 依赖：T01 · 预算：0.5h · 状态：**已完成**（2026-09-09；测试 3/3 绿，业务库冒烟通过）

**设计依据**：详设 §1.3（MySQL 使用约定）、§4.2（连接与事务参数）、§7.2（task_events 表结构）；架构 §4（数据模型、索引与 CHECK）
**测试依据**：测试设计 §2（测试库红线）；迁移幂等性

## 目标

提供三张表的编号 SQL 迁移与只前滚的迁移执行器，完成 GORM 初始化（READ COMMITTED、UTC、连接池），并搭好集成测试基建（测试库红线校验 + 每用例清表）。

## 涉及文件

- Create：`migrations/0001_init.sql`
- Create：`internal/infrastructure/persistence/mysql/mysql.go`（连接初始化）、`models.go`（三张表的 GORM PO 模型）、`migrate.go`（迁移执行器）
- Create：`tests/integration/main_test.go`（TestMain + 测试库守卫）、`tests/integration/migrate_test.go`
- Modify：`bootstrap/wire.go`（接入真实 DB 初始化，本任务后启动会连库执行迁移）

## 交付接口（后续任务依赖）

```go
// mysql.go
func Open(dsn string) (*gorm.DB, error) // DSN 结构化组装：parseTime=true、loc=UTC、time_zone='+00:00'；
                                        // 会话隔离级别 READ COMMITTED；MaxOpenConns=10、MaxIdleConns=5（详设 §4.2）
// migrate.go
func Migrate(db *gorm.DB, dir string) error // 创建 schema_migrations 表，按文件名编号只前滚未应用版本

// models.go —— 表名 recordings / tasks / task_events，字段与迁移 SQL 一一对应
type RecordingPO struct{ /* id, original_filename, storage_path, extension, size_bytes, content_hash, deleting_at, created_at, updated_at */ }
type TaskPO struct{ /* id, recording_id, status, attempt, event_seq, transcript, summary_json, error_code, error_message, created_request_id, created_at, updated_at, started_at, finished_at */ }
type TaskEventPO struct{ /* 详设 §7.2 全部 19 列 */ }

// tests/integration/main_test.go
func RequireTestDB(t *testing.T) *gorm.DB // TEST_MYSQL_DSN 未设 → t.Skip；DSN 不含 "test" → t.Fatal；返回前 TRUNCATE 三表
```

迁移 SQL 内容（0001_init.sql）：三张表按架构 §4 与详设 §7.2 全字段；ID 为 `CHAR(36) CHARACTER SET ascii COLLATE ascii_bin`；时间 `DATETIME(6)`；索引 `tasks(status, created_at, id)`、`recordings(created_at, id)`、`task_events UNIQUE(task_id, event_seq)`、`storage_path UNIQUE`、`recording_id UNIQUE`；CHECK（合法状态枚举、attempt > 0）；无物理外键。

## 步骤

- [x] 1. 通读设计：架构 §4、详设 §1.3/§4.2/§7.2。（task_events 实为 **21 列**——本文件"19 列"系笔误，以详设 §7.2 为准实现）
- [x] 2. 起库：`docker compose up -d db`；建测试库 `recording_test`。（2026-09-09：本机宿主 3306 被其他项目长期占用，本项目宿主端口改 **3307**，容器内仍 3306，compose 网络内 app 用 `db:3306` 不受影响；`.env`/`.env.example` 已同步）
- [x] 3. 写失败测试：`TestIT_MigrationIdempotent`（DROP 四表回空库 → Migrate 两次 → 无错误，schema_migrations 行数 = 迁移文件数，三表存在）；`TestIT_ConnectionSettings`（session `time_zone='+00:00'`、`transaction_isolation` READ COMMITTED）；`TestIT_TestDBGuard`（红线）。
- [x] 4. 运行确认失败：实现包不存在 / gorm 依赖未落，编译失败即预期 RED。
- [x] 5. 实现：0001_init.sql、Open（DSN 结构化组装 + Params 设 time_zone / transaction_isolation）、Migrate（按 ";" 切分语句、跳过注释块）、PO 模型、RequireTestDB（打开前先幂等迁移保证三表存在，再 TRUNCATE 三表）。
- [x] 6. 运行确认通过：`TEST_MYSQL_DSN='recording:recordingpass@tcp(127.0.0.1:3307)/recording_test?parseTime=true&loc=UTC' GOTOOLCHAIN=local go test ./tests/integration/ -v` 3/3 绿；mysql 客户端核对 idx_tasks_queue / uq_recordings_storage_path / idx_recordings_created DESC / CHECK（状态枚举、attempt>0、size 范围）/ uq_task_events_seq 均在。另冒烟：`go run ./cmd/server` 连业务库自动建表，healthz 200。
- [ ] 7. 提交：`feat: add mysql schema migration and gorm bootstrap`。（**待用户执行 commit**）

### 执行偏差记录（2026-09-09）

- 测试守卫断言方式：本文件原设想"子测试注入业务库 DSN + t.Run 返回值断言"不可行——现代 Go 中失败的子测试会把父测试一并标 FAIL，无法表达"子测试按预期失败而父测试通过"。改为把红线判定抽成 `checkTestDSN(dsn) error`（RequireTestDB 内部调用同一函数）直接断言。
- RequireTestDB 在 TRUNCATE 前先幂等 Migrate：否则全新空库上首个集成测试会因表不存在而误报失败。
- 镜像复用（2026-09-09，用户决策）：compose db 改用本机已有 `mysql:8.0` 镜像（删除新拉的 mysql:8.4，避免 1.1GB 重复占用）；文档选型仍为 MySQL 8.4（详设 §1.3），本项目用到的能力（SKIP LOCKED、JSON、DATETIME(6)、DESC 索引、8.0.16+ 强制 CHECK）8.0 均支持，集成测试 3/3 与业务库冒烟在 8.0 上复验通过。
- 共享 MySQL 实例（2026-09-09，用户决策）：本机日常开发**不起 compose db**，直连已有共享实例 `dataforge-mysql`（mysql 8.0.46，宿主 3306，卷 `dataforge-mysql-data`——其他服务共用该卷/实例），本项目在其内自建库 `recording` / `recording_test` 与专用用户 `recording`，`.env` DSN 指向 3306。compose 的 db 服务保留用于仓库自包含交付（T12 clone 即用，卷名回默认 `mysql-data` 项目前缀卷）。集成测试 3/3 与服务冒烟在共享实例上复验通过。
- 端口约定（2026-09-09，与用户对齐）：容器内恒 3306；compose db 宿主端口经 `.env` 的 `MYSQL_PORT` 注入（默认 3306，宿主被占时本机设 3307）。启动语义即 `docker compose up -d`：容器存在直接复用启动、不存在才创建、镜像缺失才拉取（T12 的 `make check/start/dev` 在此之上封装）。

## 完成标准

- [x] 空库 `Migrate` 幂等；重复执行不重复应用。
- [x] 连接会话验证过 `time_zone='+00:00'` 与 READ COMMITTED（测试断言 TestIT_ConnectionSettings；注：MySQL 按设置形式回显 `READ-COMMITTED`，测试归一化连字符后比较）。
- [x] 测试库红线生效：不含 "test" 的 DSN 直接失败。

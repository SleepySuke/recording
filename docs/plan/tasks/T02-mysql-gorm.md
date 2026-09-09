# T02：MySQL 迁移与 GORM 基建

> 依赖：T01 · 预算：0.5h · 状态：**未开始**

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

- [ ] 1. 通读设计：架构 §4、详设 §1.3/§4.2/§7.2。
- [ ] 2. 起库：`docker compose up -d db`；建测试库 `recording_test`（`CREATE DATABASE recording_test`）。
- [ ] 3. 写失败测试：`TestIT_MigrationIdempotent`（空库执行 Migrate 两次 → 无错误，schema_migrations 行数 = 迁移文件数）；`TestIT_TestDBGuard`（DSN 不含 "test" 时 RequireTestDB 必须 Fatal——用子测试注入业务库 DSN 断言失败）。
- [ ] 4. 运行确认失败：`TEST_MYSQL_DSN='root:<pwd>@tcp(127.0.0.1:3306)/recording_test?parseTime=true&loc=UTC' go test ./tests/integration/ -v`。
- [ ] 5. 实现：0001_init.sql、Open、Migrate、PO 模型、RequireTestDB。
- [ ] 6. 运行确认通过：同命令，全绿；另用 `mysql` 客户端核对三表索引与 CHECK 存在。
- [ ] 7. 提交：`feat: add mysql schema migration and gorm bootstrap`。

## 完成标准

- [ ] 空库 `Migrate` 幂等；重复执行不重复应用。
- [ ] 连接会话验证过 `time_zone='+00:00'` 与 READ COMMITTED（启动日志或测试断言）。
- [ ] 测试库红线生效：不含 "test" 的 DSN 直接失败。

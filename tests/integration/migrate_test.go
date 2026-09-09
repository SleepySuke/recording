package integration

import (
	"strings"
	"testing"

	"recording-transcription/internal/infrastructure/persistence/mysql"
)

// TestIT_MigrationIdempotent（T02）：空库连续执行两次 Migrate 均成功，
// 且 schema_migrations 行数等于迁移文件数（只前滚，不重复应用）。
func TestIT_MigrationIdempotent(t *testing.T) {
	db := RequireTestDB(t)

	// 清空到全新库状态：删四张表（含迁移账本），从零验证。
	for _, table := range []string{"task_events", "tasks", "recordings", "schema_migrations"} {
		if err := db.Exec("DROP TABLE IF EXISTS " + table).Error; err != nil {
			t.Fatalf("预清理 DROP %s 失败: %v", table, err)
		}
	}

	if err := mysql.Migrate(db, migrationsDir); err != nil {
		t.Fatalf("第一次迁移失败: %v", err)
	}
	if err := mysql.Migrate(db, migrationsDir); err != nil {
		t.Fatalf("第二次迁移失败（应幂等跳过）: %v", err)
	}

	files := migrationFiles(t)
	var applied int64
	if err := db.Raw("SELECT COUNT(*) FROM schema_migrations").Scan(&applied).Error; err != nil {
		t.Fatalf("查询 schema_migrations 失败: %v", err)
	}
	if applied != int64(len(files)) {
		t.Fatalf("schema_migrations 行数 %d != 迁移文件数 %d（重复应用）", applied, len(files))
	}

	// 三张业务表存在（迁移真的建了表）。
	for _, table := range []string{"recordings", "tasks", "task_events"} {
		if !db.Migrator().HasTable(table) {
			t.Fatalf("迁移后缺少表 %s", table)
		}
	}
}

// TestIT_ConnectionSettings（T02）：连接会话验证 time_zone='+00:00' 与 READ COMMITTED
// （详设 §1.3 / §4.2；完成标准要求以测试断言或启动日志验证）。
func TestIT_ConnectionSettings(t *testing.T) {
	db := RequireTestDB(t)

	var tz string
	if err := db.Raw("SELECT @@session.time_zone").Scan(&tz).Error; err != nil {
		t.Fatalf("查询 session.time_zone 失败: %v", err)
	}
	if tz != "+00:00" {
		t.Fatalf("session.time_zone = %q，期望 +00:00", tz)
	}

	var isolation string
	if err := db.Raw("SELECT @@session.transaction_isolation").Scan(&isolation).Error; err != nil {
		t.Fatalf("查询 transaction_isolation 失败: %v", err)
	}
	// MySQL 按设置形式回显（'READ-COMMITTED' 连字符拼写），归一化后比较。
	if got := strings.ReplaceAll(isolation, "-", " "); got != "READ COMMITTED" {
		t.Fatalf("transaction_isolation = %q，期望 READ COMMITTED", isolation)
	}
}

// TestIT_TestDBGuard（T02）：测试库红线——业务库 DSN 必须被拒绝。
// 注：任务文档原设想的"子测试注入 + t.Run 返回值"断言不可行——Go 现代版本中
// 失败的子测试会同时把父测试标 FAIL，无法表达"子测试按预期失败、父测试通过"，
// 故改为直接断言守卫函数（RequireTestDB 内部同样调用它）。
func TestIT_TestDBGuard(t *testing.T) {
	business := "recording:recordingpass@tcp(127.0.0.1:3307)/recording?parseTime=true&loc=UTC"
	if err := checkTestDSN(business); err == nil {
		t.Fatal("业务库 DSN 未被拒绝")
	}
	testdb := "recording:recordingpass@tcp(127.0.0.1:3307)/recording_test?parseTime=true&loc=UTC"
	if err := checkTestDSN(testdb); err != nil {
		t.Fatalf("测试库 DSN 被误拒: %v", err)
	}
}

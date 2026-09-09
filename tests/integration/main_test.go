// Package integration 提供数据库集成测试基建：测试库红线守卫 + 每用例清表（测试设计 §2）。
package integration

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gorm.io/gorm"

	"recording-transcription/internal/infrastructure/persistence/mysql"
)

// migrationsDir 迁移文件目录（测试从 tests/integration 运行，相对仓库根）。
const migrationsDir = "../../migrations"

// RequireTestDB 打开 TEST_MYSQL_DSN 指向的测试库：
// 未设置 → 跳过；DSN 不含 "test" → 直接失败（测试库红线，禁止指向业务数据）；
// 返回前执行幂等迁移并 TRUNCATE 三表，保证每用例干净起点。
func RequireTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := os.Getenv("TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("TEST_MYSQL_DSN 未设置，跳过集成测试")
	}
	if err := checkTestDSN(dsn); err != nil {
		t.Fatal(err)
	}
	db, err := mysql.Open(dsn)
	if err != nil {
		t.Fatalf("打开测试库失败: %v", err)
	}
	t.Cleanup(func() {
		if sqlDB, err := db.DB(); err == nil {
			sqlDB.Close()
		}
	})
	// 先迁移保证三表存在（首次运行时库为空），再清表。
	if err := mysql.Migrate(db, migrationsDir); err != nil {
		t.Fatalf("测试前置迁移失败: %v", err)
	}
	for _, table := range []string{"recordings", "tasks", "task_events"} {
		if err := db.Exec("TRUNCATE TABLE " + table).Error; err != nil {
			t.Fatalf("清空 %s 失败: %v", table, err)
		}
	}
	return db
}

// migrationFiles 返回迁移目录下按文件名排序的 .sql 文件基名。
func migrationFiles(t *testing.T) []string {
	t.Helper()
	entries, err := os.ReadDir(migrationsDir)
	if err != nil {
		t.Fatalf("读迁移目录失败: %v", err)
	}
	var names []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			names = append(names, strings.TrimSuffix(e.Name(), filepath.Ext(e.Name())))
		}
	}
	if len(names) == 0 {
		t.Fatal("迁移目录没有 .sql 文件")
	}
	return names
}

// checkTestDSN 测试库红线：DSN 必须含 "test"，防止误清业务数据（测试设计 §2）。
func checkTestDSN(dsn string) error {
	if !strings.Contains(dsn, "test") {
		return errors.New(`测试库红线：TEST_MYSQL_DSN 必须指向 test 库，当前 DSN 不含 "test"（为避免误清业务数据拒绝执行）`)
	}
	return nil
}

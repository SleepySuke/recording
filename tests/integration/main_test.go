// Package integration 提供数据库集成测试基建：测试库红线守卫 + 每用例清表（测试设计 §2）；
// 与 e2e 共用同一测试库，整包持有 MySQL 咨询锁互斥（见 TestMain）。
package integration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"gorm.io/gorm"

	"recording-transcription/internal/infrastructure/persistence/mysql"
)

// suiteLockName 包级互斥锁名：tests/integration 与 e2e 共用 recording_test 库，
// go test 多包并行时两包的「每用例 TRUNCATE 三表」会互相清空对方在途数据。
// 两包用同名 MySQL 咨询锁整包串行化；单实例/单测试库场景足够，不同 DSN 被一并
// 串行化也无害（安全侧）。
const suiteLockName = "recording_test_suite"

// TestMain 切到仓库根再运行（T10 起 bootstrap.NewApp 内 mysql.Migrate(db, "migrations")
// 依赖 cwd，与 e2e 同模式），取整包咨询锁后运行，结束后释放并还原。
// 未配置 TEST_MYSQL_DSN 时用例各自 t.Skip，无需互斥。
func TestMain(m *testing.M) {
	orig, err := os.Getwd()
	if err != nil {
		os.Stderr.WriteString("获取工作目录失败: " + err.Error() + "\n")
		os.Exit(1)
	}
	if err := os.Chdir("../.."); err != nil {
		os.Stderr.WriteString("切换到仓库根失败: " + err.Error() + "\n")
		os.Exit(1)
	}
	release, err := acquireSuiteLock()
	if err != nil {
		os.Stderr.WriteString("获取测试库包级互斥锁失败: " + err.Error() + "\n")
		os.Exit(1)
	}
	code := m.Run()
	release()
	if err := os.Chdir(orig); err != nil {
		os.Stderr.WriteString("还原工作目录失败: " + err.Error() + "\n")
		os.Exit(1)
	}
	os.Exit(code)
}

// acquireSuiteLock 打开专用连接执行 GET_LOCK（等待 300s，覆盖对方整包耗时，
// 集成包 ~10s 量级）；返回释放函数。GET_LOCK 为会话级锁，连接整包持有即整包互斥。
func acquireSuiteLock() (func(), error) {
	dsn := os.Getenv("TEST_MYSQL_DSN")
	if dsn == "" {
		return func() {}, nil
	}
	if !strings.Contains(dsn, "test") {
		return nil, errors.New(`测试库红线：TEST_MYSQL_DSN 必须指向 test 库，当前 DSN 不含 "test"`)
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("打开锁连接失败: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 310*time.Second)
	defer cancel()
	conn, err := db.Conn(ctx)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("获取锁连接失败: %w", err)
	}
	var got int
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, 300)", suiteLockName).Scan(&got); err != nil {
		_ = conn.Close()
		_ = db.Close()
		return nil, fmt.Errorf("执行 GET_LOCK 失败: %w", err)
	}
	if got != 1 {
		_ = conn.Close()
		_ = db.Close()
		return nil, fmt.Errorf("GET_LOCK(%q, 300) 未取得（返回 %d：另一测试包仍在运行或锁被占用）", suiteLockName, got)
	}
	return func() {
		_, _ = conn.ExecContext(context.Background(), "SELECT RELEASE_LOCK(?)", suiteLockName)
		_ = conn.Close()
		_ = db.Close()
	}, nil
}

// migrationsDir 迁移文件目录（TestMain 已切到仓库根，相对仓库根）。
const migrationsDir = "migrations"

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

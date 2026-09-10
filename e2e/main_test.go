// Package e2e 过程级端到端验收（测试设计 §5 两级 E2E 的第一级，T06E 建立）：
// 经 bootstrap.NewServer 真实装配——真实迁移、日志双写、本地文件存储、worker 池、
// 真实 TCP 监听 + 真实 http.Client 驱动；结果按 §5.1 golden 机制比对——
// 预期先行手写 expected/<case>.json，运行采集 actual/<case>.json（忽略清单以
// 占位符掩码归一化），深度相等才通过，差异写 diff/<case>.txt（§5.3 目录约定）。
// compose 全栈驱动（复用同一套 golden，只换驱动层）为第二级（T13）。
//
// 状态基准：流水线终点 done（T07 接入 LLM 摘要段：真实适配器 + 进程内 FakeLLM，
// 见 harness_test.go；LLM 边界注入用例随 T08 落地）。
package e2e

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"gorm.io/gorm"

	"recording-transcription/internal/infrastructure/persistence/mysql"
)

// suiteLockName 包级互斥锁名：与 tests/integration 同名——两包共用 recording_test 库，
// go test 多包并行时「每用例 TRUNCATE 三表」会互相清空对方在途数据；整包持有
// MySQL 咨询锁串行化两包（单实例/单测试库场景足够；不同 DSN 一并被串行化，无害）。
const suiteLockName = "recording_test_suite"

// TestMain 切到仓库根再运行（bootstrap.NewServer 内 mysql.Migrate(db, "migrations")
// 与 e2e/expected|actual|diff 相对路径都依赖 cwd），取整包咨询锁后运行，结束后释放
// 并还原。包内用例不并行（不使用 t.Parallel）。
func TestMain(m *testing.M) {
	orig, err := os.Getwd()
	if err != nil {
		os.Stderr.WriteString("获取工作目录失败: " + err.Error() + "\n")
		os.Exit(1)
	}
	if err := os.Chdir(".."); err != nil {
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

// acquireSuiteLock 打开专用连接执行 GET_LOCK（等待 300s，覆盖对方整包耗时），返回
// 释放函数。锁挂在专用连接上整包持有——不能复用用例的 gorm 连接（用例结束即关闭
// 释放，挡不住下一用例/另一包）。未配置 TEST_MYSQL_DSN 时用例各自 t.Skip，无需互斥。
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

// requireTestDB 测试库红线守卫（测试设计 §2）：与 tests/integration 同模式，
// 包间不可导入，接受少量重复。打开 TEST_MYSQL_DSN 断言连接：未设置 → 跳过；
// DSN 不含 "test" → 直接失败；返回前幂等迁移 + TRUNCATE 三表。
func requireTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := os.Getenv("TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("TEST_MYSQL_DSN 未设置，跳过 E2E 测试")
	}
	if !strings.Contains(dsn, "test") {
		t.Fatal(`测试库红线：TEST_MYSQL_DSN 必须指向 test 库，当前 DSN 不含 "test"（为避免误清业务数据拒绝执行）`)
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
	// 迁移幂等（schema_migrations 只前滚），与 NewServer 内的迁移共用一套。
	if err := mysql.Migrate(db, "migrations"); err != nil {
		t.Fatalf("测试前置迁移失败: %v", err)
	}
	for _, table := range []string{"recordings", "tasks", "task_events"} {
		if err := db.Exec("TRUNCATE TABLE " + table).Error; err != nil {
			t.Fatalf("清空 %s 失败: %v", table, err)
		}
	}
	return db
}

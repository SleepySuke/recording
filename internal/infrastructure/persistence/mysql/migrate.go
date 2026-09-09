package mysql

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gorm.io/gorm"
)

// Migrate 创建 schema_migrations 账本并按文件名编号只前滚应用 dir 下的 .sql 迁移
// （详设 §1.3：表结构变更用编号 SQL 迁移，AutoMigrate 不用于生产迁移）。
func Migrate(db *gorm.DB, dir string) error {
	if err := db.Exec(`CREATE TABLE IF NOT EXISTS schema_migrations (
		version    VARCHAR(64) PRIMARY KEY,
		applied_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
	) ENGINE = InnoDB`).Error; err != nil {
		return fmt.Errorf("创建 schema_migrations 失败: %w", err)
	}

	files, err := migrationFiles(dir)
	if err != nil {
		return err
	}
	applied := map[string]bool{}
	rows, err := db.Raw("SELECT version FROM schema_migrations").Rows()
	if err != nil {
		return fmt.Errorf("读取已应用迁移失败: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return err
		}
		applied[v] = true
	}

	for _, path := range files {
		version := strings.TrimSuffix(filepath.Base(path), ".sql")
		if applied[version] {
			continue
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("读取迁移 %s 失败: %w", path, err)
		}
		for _, stmt := range splitStatements(string(content)) {
			if err := db.Exec(stmt).Error; err != nil {
				return fmt.Errorf("执行迁移 %s 失败: %w", version, err)
			}
		}
		if err := db.Exec("INSERT INTO schema_migrations (version) VALUES (?)", version).Error; err != nil {
			return fmt.Errorf("登记迁移 %s 失败: %w", version, err)
		}
	}
	return nil
}

// migrationFiles 返回目录下按文件名排序的 .sql 文件（编号前缀保证顺序）。
func migrationFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("读迁移目录 %s 失败: %w", dir, err)
	}
	var files []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			files = append(files, filepath.Join(dir, e.Name()))
		}
	}
	sort.Strings(files)
	return files, nil
}

// splitStatements 按 ";" 切分并跳过注释/空白块；迁移文件为本仓库手写，字面量不含分号。
func splitStatements(sqlText string) []string {
	var stmts []string
	for _, chunk := range strings.Split(sqlText, ";") {
		var lines []string
		for _, line := range strings.Split(chunk, "\n") {
			trimmed := strings.TrimSpace(line)
			if trimmed == "" || strings.HasPrefix(trimmed, "--") {
				continue
			}
			lines = append(lines, line)
		}
		if len(lines) == 0 {
			continue
		}
		stmts = append(stmts, strings.TrimSpace(strings.Join(lines, "\n")))
	}
	return stmts
}

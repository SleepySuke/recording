// Package mysql 提供 GORM 连接初始化（详设 §1.3 MySQL 使用约定、§4.2 连接与事务参数）。
package mysql

import (
	"fmt"
	"time"

	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
	mysqldriver "github.com/go-sql-driver/mysql"
)

// Open 按详设约定初始化连接：parseTime=true、loc=UTC、会话 time_zone='+00:00'、
// 隔离级别 READ COMMITTED；连接池 MaxOpenConns=10、MaxIdleConns=5。
func Open(dsn string) (*gorm.DB, error) {
	cfg, err := mysqldriver.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("解析 MYSQL_DSN 失败: %w", err)
	}
	cfg.ParseTime = true
	cfg.Loc = time.UTC
	// loc 只影响驱动的时间解析，不等价于会话时区，显式设系统变量（详设 §1.3）。
	cfg.Params = map[string]string{
		"time_zone":             "'+00:00'",
		"transaction_isolation": "'READ-COMMITTED'",
	}

	db, err := gorm.Open(gormmysql.New(gormmysql.Config{DSN: cfg.FormatDSN()}), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("连接 MySQL 失败: %w", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}
	sqlDB.SetMaxOpenConns(10)
	sqlDB.SetMaxIdleConns(5)
	return db, nil
}

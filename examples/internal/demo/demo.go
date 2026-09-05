// Package demo 为 examples 提供零依赖的 SQLite 演示数据库支持：
// 内存库连接配置 + 方言 provider + 引擎全部工作流表的建表 DDL，
// 让示例无需安装 PostgreSQL/MySQL 即可 go run 直接运行。
//
// 已知限制：SQLite 没有 SELECT ... FOR UPDATE 行锁，多任务并发完成判定的
// 串行化语义只在生产库上成立，演示模式下单进程顺序审批不受影响。
package demo

import (
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"

	"github.com/rulego/gflow-engine/config"
)

// Driver SQLite 方言名，填 config.DatabaseConfig.Driver。
const Driver = "sqlite"

// DialectProvider 把 glebarez/sqlite 纯 Go 驱动桥接进引擎的 DialectProvider
// 接口（引擎默认只注册 postgres/mysql，见 service/default_dialects.go）。
type DialectProvider struct{}

func (DialectProvider) GetName() string               { return Driver }
func (DialectProvider) GetSupportedDrivers() []string { return []string{"sqlite", "sqlite3"} }
func (DialectProvider) CreateDialector(dsn string) (gorm.Dialector, error) {
	return sqlite.Open(dsn), nil
}

// NewDatabaseConfig 返回共享内存 SQLite 的连接配置：cache=shared 让连接池里的
// 多个连接看到同一份数据；MaxOpenConns=1 匹配 SQLite 单写串行语义
// （多连接并发 BEGIN 会触发驱动内部死锁）。进程退出数据即清空。
func NewDatabaseConfig() *config.DatabaseConfig {
	return &config.DatabaseConfig{
		Driver:       Driver,
		Dsn:          "file:gflow_demo?mode=memory&cache=shared&_busy_timeout=30000&_journal_mode=WAL",
		MaxOpenConns: 1,
		MaxIdleConns: 1,
	}
}

// CreateTables 建立引擎运行所需的全部工作流表，DDL 与 scripts/ 下的生产
// schema 及 test/e2e 的建表语句对齐。不用 AutoMigrate：model 结构体的
// `comment:` tag 在 SQLite 上不识别。
func CreateTables(db *gorm.DB) error {
	for _, ddl := range ddls {
		if err := db.Exec(ddl).Error; err != nil {
			return err
		}
	}
	return nil
}

var ddls = []string{
	`CREATE TABLE IF NOT EXISTS wf_process (
		id TEXT PRIMARY KEY,
		process_key TEXT NOT NULL,
		name TEXT NOT NULL,
		version INTEGER NOT NULL DEFAULT 1,
		category TEXT,
		description TEXT,
		definition_json TEXT NOT NULL,
		status TEXT NOT NULL DEFAULT 'active',
		publish_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		tenant_id TEXT NOT NULL,
		created_by TEXT NOT NULL,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_by TEXT,
		updated_at DATETIME,
		ext TEXT,
		process_type TEXT NOT NULL DEFAULT 'main',
		icon TEXT NOT NULL DEFAULT ''
	)`,
	`CREATE TABLE IF NOT EXISTS wf_instance (
		id TEXT PRIMARY KEY,
		process_id TEXT NOT NULL,
		business_key TEXT,
		name TEXT NOT NULL,
		start_user_id TEXT NOT NULL DEFAULT '',
		status TEXT NOT NULL,
		variables TEXT,
		current_activity TEXT,
		priority INTEGER NOT NULL DEFAULT 50,
		parent_id TEXT,
		tenant_id TEXT NOT NULL,
		created_by TEXT NOT NULL,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_by TEXT,
		updated_at DATETIME,
		end_reason TEXT,
		duration INTEGER,
		ended_at DATETIME,
		UNIQUE (tenant_id, business_key)
	)`,
	`CREATE TABLE IF NOT EXISTS wf_task (
		id TEXT PRIMARY KEY,
		process_instance_id TEXT,
		process_id TEXT,
		parent_id TEXT,
		task_def_key TEXT,
		task_type TEXT,
		name TEXT,
		description TEXT,
		status TEXT,
		assignee TEXT,
		owner TEXT,
		priority INTEGER DEFAULT 50,
		sequence_order INTEGER DEFAULT 0,
		due_date DATETIME,
		form_key TEXT,
		variables TEXT,
		claimed_at DATETIME,
		approval_type TEXT,
		approval_rule TEXT,
		delegate_from TEXT,
		delegate_reason TEXT,
		delegate_time DATETIME,
		ended_at DATETIME,
		comment TEXT,
		end_reason TEXT,
		duration INTEGER,
		tenant_id TEXT,
		created_by TEXT,
		created_at DATETIME,
		updated_by TEXT,
		updated_at DATETIME
	)`,
	`CREATE TABLE IF NOT EXISTS wf_task_assignee (
		id TEXT PRIMARY KEY,
		task_id TEXT NOT NULL,
		entity_type TEXT NOT NULL DEFAULT 'role',
		entity_id TEXT NOT NULL,
		tenant_id TEXT NOT NULL DEFAULT '',
		created_at DATETIME
	)`,
	`CREATE TABLE IF NOT EXISTS wf_hi_instance (
		id TEXT PRIMARY KEY,
		process_id TEXT NOT NULL,
		business_key TEXT,
		name TEXT NOT NULL,
		status TEXT NOT NULL,
		variables TEXT,
		current_activity TEXT,
		priority INTEGER NOT NULL DEFAULT 50,
		parent_id TEXT,
		tenant_id TEXT NOT NULL,
		created_by TEXT NOT NULL,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_by TEXT,
		updated_at DATETIME,
		end_reason TEXT,
		duration INTEGER,
		ended_at DATETIME,
		start_user_id TEXT NOT NULL DEFAULT ''
	)`,
	`CREATE TABLE IF NOT EXISTS wf_hi_task (
		id TEXT PRIMARY KEY,
		process_instance_id TEXT,
		process_id TEXT,
		task_def_key TEXT,
		task_type TEXT,
		name TEXT,
		description TEXT,
		parent_id TEXT,
		status TEXT,
		assignee TEXT,
		owner TEXT,
		priority INTEGER DEFAULT 50,
		due_date DATETIME,
		form_key TEXT,
		variables TEXT,
		claimed_at DATETIME,
		sequence_order INTEGER DEFAULT 0,
		approval_type TEXT,
		approval_rule TEXT,
		delegate_from TEXT,
		delegate_reason TEXT,
		delegate_time DATETIME,
		ended_at DATETIME,
		comment TEXT,
		end_reason TEXT,
		duration INTEGER,
		tenant_id TEXT,
		created_by TEXT,
		created_at DATETIME,
		updated_by TEXT,
		updated_at DATETIME
	)`,
	`CREATE TABLE IF NOT EXISTS wf_task_comment (
		id TEXT PRIMARY KEY,
		task_id TEXT NOT NULL,
		process_instance_id TEXT NOT NULL DEFAULT '',
		tenant_id TEXT NOT NULL DEFAULT '',
		user_id TEXT NOT NULL,
		user_name TEXT NOT NULL DEFAULT '',
		content TEXT NOT NULL,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	)`,
}

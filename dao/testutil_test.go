package dao

import (
	"fmt"
	"testing"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/query"
)

// 内存 SQLite 建表语句（列与 model 生成代码保持一致）。
const (
	ddlWfTask = `CREATE TABLE IF NOT EXISTS wf_task (
		id TEXT PRIMARY KEY,
		process_instance_id TEXT,
		process_id TEXT NOT NULL DEFAULT '',
		parent_id TEXT,
		task_def_key TEXT NOT NULL DEFAULT '',
		name TEXT NOT NULL DEFAULT '',
		task_type TEXT NOT NULL DEFAULT 'user_task',
		description TEXT,
		status TEXT NOT NULL DEFAULT 'created',
		assignee TEXT,
		owner TEXT,
		due_date DATETIME,
		priority INTEGER NOT NULL DEFAULT 50,
		form_key TEXT,
		variables TEXT,
		claimed_at DATETIME,
		sequence_order INTEGER NOT NULL DEFAULT 0,
		approval_type TEXT NOT NULL DEFAULT 'single',
		approval_rule TEXT,
		delegate_from TEXT,
		delegate_reason TEXT,
		delegate_time DATETIME,
		ended_at DATETIME,
		comment TEXT,
		end_reason TEXT,
		duration INTEGER,
		tenant_id TEXT NOT NULL DEFAULT '',
		created_by TEXT NOT NULL DEFAULT '',
		created_at DATETIME,
		updated_by TEXT,
		updated_at DATETIME
	)`

	ddlWfTaskAssignee = `CREATE TABLE IF NOT EXISTS wf_task_assignee (
		id TEXT PRIMARY KEY,
		task_id TEXT NOT NULL,
		entity_type TEXT NOT NULL DEFAULT 'role',
		entity_id TEXT NOT NULL,
		tenant_id TEXT NOT NULL DEFAULT '',
		created_at DATETIME
	)`

	ddlWfHiTask = `CREATE TABLE IF NOT EXISTS wf_hi_task (
		id TEXT PRIMARY KEY,
		process_instance_id TEXT,
		process_id TEXT NOT NULL DEFAULT '',
		task_def_key TEXT,
		task_type TEXT NOT NULL DEFAULT 'user_task',
		name TEXT NOT NULL DEFAULT '',
		description TEXT,
		parent_id TEXT,
		status TEXT NOT NULL DEFAULT '',
		assignee TEXT,
		owner TEXT,
		due_date DATETIME,
		priority INTEGER NOT NULL DEFAULT 50,
		form_key TEXT,
		variables TEXT,
		claimed_at DATETIME,
		sequence_order INTEGER NOT NULL DEFAULT 0,
		approval_type TEXT NOT NULL DEFAULT 'single',
		approval_rule TEXT,
		delegate_from TEXT,
		delegate_reason TEXT,
		delegate_time DATETIME,
		ended_at DATETIME,
		comment TEXT,
		end_reason TEXT,
		duration INTEGER,
		tenant_id TEXT NOT NULL DEFAULT '',
		created_by TEXT NOT NULL DEFAULT '',
		created_at DATETIME,
		updated_by TEXT,
		updated_at DATETIME
	)`

	ddlWfHiInstance = `CREATE TABLE IF NOT EXISTS wf_hi_instance (
		id TEXT PRIMARY KEY,
		process_id TEXT NOT NULL DEFAULT '',
		business_key TEXT,
		name TEXT NOT NULL DEFAULT '',
		status TEXT NOT NULL DEFAULT 'running',
		variables TEXT,
		current_activity TEXT,
		priority INTEGER NOT NULL DEFAULT 50,
		parent_id TEXT,
		tenant_id TEXT NOT NULL DEFAULT '',
		created_by TEXT NOT NULL DEFAULT '',
		created_at DATETIME,
		updated_by TEXT,
		updated_at DATETIME,
		end_reason TEXT,
		duration INTEGER,
		ended_at DATETIME,
		start_user_id TEXT NOT NULL DEFAULT ''
	)`

	ddlWfInstance = `CREATE TABLE IF NOT EXISTS wf_instance (
		id TEXT PRIMARY KEY,
		process_id TEXT NOT NULL DEFAULT '',
		business_key TEXT,
		name TEXT NOT NULL DEFAULT '',
		status TEXT NOT NULL DEFAULT 'running',
		variables TEXT,
		current_activity TEXT,
		priority INTEGER NOT NULL DEFAULT 50,
		parent_id TEXT,
		tenant_id TEXT NOT NULL DEFAULT '',
		created_by TEXT NOT NULL DEFAULT '',
		created_at DATETIME,
		updated_by TEXT,
		updated_at DATETIME,
		end_reason TEXT,
		duration INTEGER,
		ended_at DATETIME,
		start_user_id TEXT NOT NULL DEFAULT ''
	)`

	ddlWfProcess = `CREATE TABLE IF NOT EXISTS wf_process (
		id TEXT PRIMARY KEY,
		process_key TEXT NOT NULL,
		name TEXT NOT NULL,
		version INTEGER NOT NULL DEFAULT 1,
		category TEXT,
		description TEXT,
		definition_json TEXT,
		status TEXT NOT NULL DEFAULT 'active',
		publish_time DATETIME,
		tenant_id TEXT NOT NULL DEFAULT '',
		created_by TEXT,
		created_at DATETIME,
		updated_by TEXT,
		updated_at DATETIME,
		ext TEXT,
		process_type TEXT,
		icon TEXT
	)`
)

// newTestQuery 打开按用例名隔离的内存 SQLite 并建表，返回 gen Query。
func newTestQuery(t *testing.T, ddls ...string) *query.Query {
	t.Helper()
	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared", t.Name())
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() {
		if c, e := db.DB(); e == nil {
			_ = c.Close()
		}
	})
	for _, ddl := range ddls {
		if err := db.Exec(ddl).Error; err != nil {
			t.Fatalf("create table: %v", err)
		}
	}
	return query.Use(db)
}

// taskIDSet 把任务列表转成 ID 集合，便于断言过滤结果。
func taskIDSet(tasks []*model.WfTask) map[string]bool {
	ids := make(map[string]bool, len(tasks))
	for _, tk := range tasks {
		ids[tk.ID] = true
	}
	return ids
}

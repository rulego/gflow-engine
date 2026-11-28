// subProcess 目标解析的单元测试：DB 回退必须取 active 最新版本（无排序 First 会
// 在多版本历史行里按随机主键抽中已停用旧版本），内存映射指向旧版本时命中自愈。
package service

import (
	"context"
	"testing"

	"github.com/glebarez/sqlite"
	"github.com/rulego/rulego"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/rulego/gflow-engine/dao"
	"github.com/rulego/gflow-engine/query"
	"github.com/rulego/gflow-engine/types/enums"
)

// newSubProcessResolveDB 建内存 wf_process 表，返回 gorm.DB 与 query.Query。
func newSubProcessResolveDB(t *testing.T) (*gorm.DB, *query.Query) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open("file:subprocess_resolve_test?mode=memory&cache=shared&_busy_timeout=30000"),
		&gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	t.Cleanup(func() {
		if c, e := db.DB(); e == nil {
			_ = c.Close()
		}
	})
	require.NoError(t, db.Exec(`CREATE TABLE IF NOT EXISTS wf_process (
		id TEXT PRIMARY KEY, process_key TEXT NOT NULL, name TEXT NOT NULL,
		version INTEGER NOT NULL DEFAULT 1, category TEXT, description TEXT,
		definition_json TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'active',
		publish_time DATETIME, tenant_id TEXT NOT NULL, created_by TEXT,
		created_at DATETIME, updated_by TEXT, updated_at DATETIME, ext TEXT,
		process_type TEXT, icon TEXT
	)`).Error, "create table")
	require.NoError(t, db.Exec("DELETE FROM wf_process").Error)
	return db, query.Use(db)
}

// seedProcessRow 插入一条流程定义行（按 key/version/status 定制）。
// 与生产同构：processKey 即 ruleChain.id（subProcess targetId 寻址键）。
func seedProcessRow(t *testing.T, db *gorm.DB, id, key string, version int32, status string) {
	t.Helper()
	require.NoError(t, db.Exec(
		`INSERT INTO wf_process (id, process_key, name, version, definition_json, status, tenant_id, created_by, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)`,
		id, key, "流程"+id, version, `{"ruleChain":{"id":"`+key+`"}}`, status, "t1", "sys").Error, "seed", id)
}

// GetActiveByKey：多版本历史行并存时取版本最高的 active 行——状态过滤 + 版本降序
// 缺一不可（无排序 First 按随机主键返回，会抽中已停用版本）。
func TestGetActiveByKey_PicksNewestActive(t *testing.T) {
	db, q := newSubProcessResolveDB(t)
	seedProcessRow(t, db, "p-act-v1", "child", 1, string(enums.ProcessStatusActive))
	seedProcessRow(t, db, "p-ret-v2", "child", 2, string(enums.ProcessStatusRetired))
	seedProcessRow(t, db, "p-act-v3", "child", 3, string(enums.ProcessStatusActive))

	proc, err := dao.NewProcessDAOWithQuery(q).GetActiveByKey(context.Background(), "t1", "child")
	require.NoError(t, err)
	require.NotNil(t, proc)
	assert.Equal(t, "p-act-v3", proc.ID, "多个 active 行时取版本最高者")
}

// GetActiveByKey：全部停用 / 无此 key 返回 ErrRecordNotFound。
func TestGetActiveByKey_NoneActive(t *testing.T) {
	db, q := newSubProcessResolveDB(t)
	seedProcessRow(t, db, "p-ret-v1", "child", 1, string(enums.ProcessStatusRetired))

	_, err := dao.NewProcessDAOWithQuery(q).GetActiveByKey(context.Background(), "t1", "child")
	require.ErrorIs(t, err, gorm.ErrRecordNotFound)

	_, err = dao.NewProcessDAOWithQuery(q).GetActiveByKey(context.Background(), "t1", "ghost")
	require.ErrorIs(t, err, gorm.ErrRecordNotFound)
}

// ResolveSubProcessTarget：内存映射指向已停用旧版本（另一副本重新部署后），
// 命中校验判失效 → DB 重解析到 active 新版本并回填映射。
func TestResolveSubProcessTarget_StaleMappingSelfHeals(t *testing.T) {
	db, q := newSubProcessResolveDB(t)
	seedProcessRow(t, db, "p-ret-v1", "child", 1, string(enums.ProcessStatusRetired))
	seedProcessRow(t, db, "p-act-v2", "child", 2, string(enums.ProcessStatusActive))

	s := &RuntimeServiceImpl{
		processDAO: dao.NewProcessDAOWithQuery(q),
		enginePool: rulego.NewRuleGo(),
	}
	s.subProcessTargets.Store("t1|child", "p-ret-v1")

	id, ok := s.ResolveSubProcessTarget(context.Background(), "t1", "child")
	require.True(t, ok)
	assert.Equal(t, "p-act-v2", id, "旧版本映射必须自愈到 active 新版本")
	v, loaded := s.subProcessTargets.Load("t1|child")
	require.True(t, loaded)
	assert.Equal(t, "p-act-v2", v, "自愈后回填映射")
}

// ResolveSubProcessTarget：映射命中且仍是 active 版本时直接返回（不重解析）。
func TestResolveSubProcessTarget_FreshHitKept(t *testing.T) {
	db, q := newSubProcessResolveDB(t)
	seedProcessRow(t, db, "p-act-v2", "child", 2, string(enums.ProcessStatusActive))
	seedProcessRow(t, db, "p-ret-v1", "child", 1, string(enums.ProcessStatusRetired))

	s := &RuntimeServiceImpl{
		processDAO: dao.NewProcessDAOWithQuery(q),
		enginePool: rulego.NewRuleGo(),
	}
	s.subProcessTargets.Store("t1|child", "p-act-v2")

	id, ok := s.ResolveSubProcessTarget(context.Background(), "t1", "child")
	require.True(t, ok)
	assert.Equal(t, "p-act-v2", id)
}

// ResolveSubProcessTarget：映射未命中回落 DB 取 active 最新版本（多版本并存）。
func TestResolveSubProcessTarget_FallbackPicksActive(t *testing.T) {
	db, q := newSubProcessResolveDB(t)
	seedProcessRow(t, db, "p-ret-v1", "child", 1, string(enums.ProcessStatusRetired))
	seedProcessRow(t, db, "p-ret-v2", "child", 2, string(enums.ProcessStatusRetired))
	seedProcessRow(t, db, "p-act-v3", "child", 3, string(enums.ProcessStatusActive))

	s := &RuntimeServiceImpl{
		processDAO: dao.NewProcessDAOWithQuery(q),
		enginePool: rulego.NewRuleGo(),
	}
	id, ok := s.ResolveSubProcessTarget(context.Background(), "t1", "child")
	require.True(t, ok)
	assert.Equal(t, "p-act-v3", id, "DB 回退必须取 active 最新版本，而非主键序随机行")
}

// ResolveSubProcessTarget：全部停用 / 无此 key 返回 false（子流程未部署是正常
// 业务态，调用方报「未注册」）。
func TestResolveSubProcessTarget_NoneActiveFalse(t *testing.T) {
	db, q := newSubProcessResolveDB(t)
	seedProcessRow(t, db, "p-ret-v1", "child", 1, string(enums.ProcessStatusRetired))

	s := &RuntimeServiceImpl{
		processDAO: dao.NewProcessDAOWithQuery(q),
		enginePool: rulego.NewRuleGo(),
	}
	_, ok := s.ResolveSubProcessTarget(context.Background(), "t1", "child")
	assert.False(t, ok)

	_, ok = s.ResolveSubProcessTarget(context.Background(), "t1", "ghost-key")
	assert.False(t, ok)
}

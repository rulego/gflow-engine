package service

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/rulego/gflow-engine/dao"
	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/query"
	"github.com/rulego/gflow-engine/types/dto"
)

// resolveProcessIDFilter 的 key→全版本 ID 解析与 AND 交集语义。
type fakeProcessStore struct {
	defs []model.WfProcess
	err  error
}

func (f *fakeProcessStore) Underlying() *query.Query { return nil }
func (f *fakeProcessStore) List(ctx context.Context, request *dto.ProcessQueryRequest) ([]*model.WfProcess, int64, error) {
	if f.err != nil {
		return nil, 0, f.err
	}
	var out []*model.WfProcess
	for i := range f.defs {
		d := &f.defs[i]
		if request.ProcessKey != "" && d.ProcessKey != request.ProcessKey {
			continue
		}
		if request.TenantID != "" && d.TenantID != request.TenantID {
			continue
		}
		out = append(out, d)
	}
	return out, int64(len(out)), nil
}
func (f *fakeProcessStore) Get(ctx context.Context, id string) (*model.WfProcess, error) {
	return nil, errors.New("not implemented")
}
func (f *fakeProcessStore) GetByIDs(ctx context.Context, ids []string) ([]*model.WfProcess, error) {
	return nil, errors.New("not implemented")
}
func (f *fakeProcessStore) GetByKeyAndVersion(ctx context.Context, tenantID, processKey string, version int32) (*model.WfProcess, error) {
	return nil, errors.New("not implemented")
}
func (f *fakeProcessStore) GetLatestByKey(ctx context.Context, tenantID, processKey string) (*model.WfProcess, error) {
	return nil, errors.New("not implemented")
}
func (f *fakeProcessStore) Update(ctx context.Context, entity *model.WfProcess) error {
	return errors.New("not implemented")
}
func (f *fakeProcessStore) Delete(ctx context.Context, tenantID, id string) error {
	return errors.New("not implemented")
}
func (f *fakeProcessStore) CountActiveReferencingForm(ctx context.Context, tenantID, formKey string) (int64, error) {
	return 0, errors.New("not implemented")
}

func TestResolveProcessIDFilter(t *testing.T) {
	store := &fakeProcessStore{defs: []model.WfProcess{
		{ID: "p1", ProcessKey: "k", Version: 1, TenantID: "t1"},
		{ID: "p2", ProcessKey: "k", Version: 2, TenantID: "t1"},
		{ID: "q1", ProcessKey: "other", Version: 1, TenantID: "t1"},
	}}
	ctx := context.Background()

	// key 与 ID 都为空：不按流程定义过滤
	ids, noMatch, err := resolveProcessIDFilter(ctx, store, "t1", "", "")
	if err != nil || noMatch || ids != nil {
		t.Errorf("empty filter: ids=%v noMatch=%v err=%v, want nil/false/nil", ids, noMatch, err)
	}

	// 仅 ID：单元素集合
	ids, noMatch, err = resolveProcessIDFilter(ctx, store, "t1", "", "p9")
	if err != nil || noMatch || len(ids) != 1 || ids[0] != "p9" {
		t.Errorf("id only: ids=%v noMatch=%v err=%v", ids, noMatch, err)
	}

	// 仅 key：该 key 全部版本 ID
	ids, noMatch, err = resolveProcessIDFilter(ctx, store, "t1", "k", "")
	if err != nil || noMatch || len(ids) != 2 {
		t.Errorf("key only: ids=%v noMatch=%v err=%v, want 2 version IDs", ids, noMatch, err)
	}

	// key + 属于该 key 的 ID：AND 命中，返回全集
	ids, noMatch, err = resolveProcessIDFilter(ctx, store, "t1", "k", "p1")
	if err != nil || noMatch || len(ids) != 2 {
		t.Errorf("key+member id: ids=%v noMatch=%v err=%v, want full set", ids, noMatch, err)
	}

	// key + 不属于该 key 的 ID：交集为空
	ids, noMatch, err = resolveProcessIDFilter(ctx, store, "t1", "k", "q1")
	if err != nil || !noMatch {
		t.Errorf("key+foreign id: noMatch=%v err=%v, want noMatch=true", noMatch, err)
	}

	// key 无任何版本：空结果
	ids, noMatch, err = resolveProcessIDFilter(ctx, store, "t1", "ghost", "")
	if err != nil || !noMatch {
		t.Errorf("unknown key: noMatch=%v err=%v, want noMatch=true", noMatch, err)
	}

	// 存储查询失败：错误冒泡
	store.err = errors.New("db down")
	if _, _, err = resolveProcessIDFilter(ctx, store, "t1", "k", ""); err == nil {
		t.Errorf("store failure: want error bubble-up")
	}
}

// union 列表的查询条件透传：发起人 startUserId 与创建时间窗须同时作用于
// 运行表和历史归档表，服务层丢弃任一条件都会让管理端筛选形同虚设。
func TestGetProcessInstanceUnionList_PassesFilters(t *testing.T) {
	q := newUnionListTestDB(t)
	svc := &RuntimeServiceImpl{
		instanceDAO: dao.NewInstanceDAOWithQuery(q),
		processDAO:  dao.NewProcessDAOWithQuery(q),
	}
	base := time.Date(2026, 11, 13, 8, 0, 0, 0, time.Local)
	seedUnionInstance(t, q, "i-u1-hist", "u1", base)                  // 08:00 归档
	seedUnionInstance(t, q, "i-u2-hist", "u2", base.Add(time.Hour))   // 09:00 归档
	seedUnionInstance(t, q, "i-u1-live", "u1", base.Add(2*time.Hour)) // 10:00 运行
	seedUnionInstance(t, q, "i-u2-live", "u2", base.Add(3*time.Hour)) // 11:00 运行
	actor := Actor{UserID: "admin", TenantID: "t1"}
	ctx := context.Background()
	ids := func(list []*model.WfInstance) map[string]bool {
		out := make(map[string]bool, len(list))
		for _, in := range list {
			out[in.ID] = true
		}
		return out
	}

	// 不带条件：活表+归档表 4 行全量
	got, total, err := svc.GetProcessInstanceUnionList(ctx, actor, &dto.ProcessInstanceQueryDTO{PageRequest: dto.PageRequest{Page: 1, PageSize: 10}})
	require.NoError(t, err)
	require.EqualValues(t, 4, total)
	require.Len(t, got, 4)

	// 按发起人过滤：两表的 u1 行都命中，u2 行排除
	afterHalf := base.Add(90 * time.Minute) // 09:30
	got, total, err = svc.GetProcessInstanceUnionList(ctx, actor, &dto.ProcessInstanceQueryDTO{
		PageRequest: dto.PageRequest{Page: 1, PageSize: 10}, StartUserID: "u1",
	})
	require.NoError(t, err)
	require.EqualValues(t, 2, total)
	require.Equal(t, map[string]bool{"i-u1-live": true, "i-u1-hist": true}, ids(got))

	// 创建时间窗：仅 09:30 之后的行，两表同样生效
	got, total, err = svc.GetProcessInstanceUnionList(ctx, actor, &dto.ProcessInstanceQueryDTO{
		PageRequest: dto.PageRequest{Page: 1, PageSize: 10}, CreatedAfter: &afterHalf,
	})
	require.NoError(t, err)
	require.EqualValues(t, 2, total)
	require.Equal(t, map[string]bool{"i-u1-live": true, "i-u2-live": true}, ids(got))

	// 发起人+时间窗组合：仅剩 u2 的运行行
	got, total, err = svc.GetProcessInstanceUnionList(ctx, actor, &dto.ProcessInstanceQueryDTO{
		PageRequest: dto.PageRequest{Page: 1, PageSize: 10}, StartUserID: "u2", CreatedAfter: &afterHalf,
	})
	require.NoError(t, err)
	require.EqualValues(t, 1, total)
	require.Equal(t, map[string]bool{"i-u2-live": true}, ids(got))
}

// seedUnionInstance 按测试数据需要分侧落行：ID 以 hist 结尾的落历史归档表，
// 其余落运行表。两侧各插一半，能同时暴露"过滤只作用单表"的漏传。
func seedUnionInstance(t *testing.T, q *query.Query, id, startUser string, createdAt time.Time) {
	t.Helper()
	row := &model.WfInstance{
		ID: id, ProcessID: "proc-u", Name: "union-" + id, Status: "completed",
		TenantID: "t1", CreatedBy: "system", StartUserID: startUser, CreatedAt: createdAt,
	}
	if id[len(id)-4:] == "hist" {
		require.NoError(t, q.WfHiInstance.Create(&model.WfHiInstance{
			ID: row.ID, ProcessID: row.ProcessID, Name: row.Name, Status: row.Status,
			TenantID: row.TenantID, CreatedBy: row.CreatedBy, StartUserID: row.StartUserID, CreatedAt: row.CreatedAt,
		}))
		return
	}
	require.NoError(t, q.WfInstance.Create(row))
}

func newUnionListTestDB(t *testing.T) *query.Query {
	t.Helper()
	db, err := gorm.Open(sqlite.Open("file:union_list_svc_test?mode=memory&cache=shared&_busy_timeout=30000"),
		&gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	t.Cleanup(func() {
		if c, e := db.DB(); e == nil {
			_ = c.Close()
		}
	})
	instanceDDL := `CREATE TABLE IF NOT EXISTS %s (
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
	for _, table := range []string{"wf_instance", "wf_hi_instance"} {
		require.NoError(t, db.Exec(fmt.Sprintf(instanceDDL, table)).Error)
	}
	require.NoError(t, db.Exec(`CREATE TABLE IF NOT EXISTS wf_process (
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
	)`).Error)
	return query.Use(db)
}

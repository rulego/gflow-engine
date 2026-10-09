package service

import (
	"context"
	"errors"
	"testing"

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

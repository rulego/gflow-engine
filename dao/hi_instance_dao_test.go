package dao

import (
	"context"
	"testing"
	"time"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/types/dto"
	"github.com/rulego/gflow-engine/types/enums"
)

// 历史列表与单行读取都必须遵守"deleted 对用户不可见"：终态实例归档后再被
// 删除的行（status=deleted）不允许从历史侧漏出；物理清除路径例外，走
// GetIncludingDeleted。
func TestHiInstanceDAO_DeletedInvisible(t *testing.T) {
	q := newTestQuery(t, ddlWfHiInstance)
	d := NewHiInstanceDAOWithQuery(q)
	ctx := context.Background()
	now := time.Now()

	seed := []*model.WfHiInstance{
		{ID: "hi-done", ProcessID: "p1", Name: "completed", Status: string(enums.InstanceStatusCompleted), TenantID: "t1", StartUserID: "u1", CreatedAt: now},
		{ID: "hi-del", ProcessID: "p1", Name: "deleted", Status: string(enums.InstanceStatusDeleted), TenantID: "t1", StartUserID: "u1", CreatedAt: now},
	}
	for _, hi := range seed {
		if err := q.WfHiInstance.WithContext(ctx).Create(hi); err != nil {
			t.Fatalf("seed hi instance %s: %v", hi.ID, err)
		}
	}

	// 未指定状态：deleted 不带出
	list, total, err := d.List(ctx, &dto.ProcessInstanceQueryDTO{
		PageRequest: dto.PageRequest{TenantID: "t1"},
	})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if total != 1 || len(list) != 1 || list[0].ID != "hi-done" {
		t.Errorf("default list = total %d rows %v, want only hi-done", total, ids(list))
	}

	// 显式按状态过滤：按传入口径命中
	list, total, err = d.List(ctx, &dto.ProcessInstanceQueryDTO{
		PageRequest: dto.PageRequest{
			TenantID: "t1",
			Status:   []string{string(enums.InstanceStatusDeleted)},
		},
	})
	if err != nil {
		t.Fatalf("list by status: %v", err)
	}
	if total != 1 || len(list) != 1 || list[0].ID != "hi-del" {
		t.Errorf("explicit deleted filter = total %d rows %v, want hi-del", total, ids(list))
	}

	// 单行读取：deleted 按未命中返回
	got, err := d.Get(ctx, "hi-del")
	if err != nil {
		t.Fatalf("get deleted: %v", err)
	}
	if got != nil {
		t.Errorf("get deleted = %v, want nil", got)
	}
	got, err = d.Get(ctx, "hi-done")
	if err != nil || got == nil || got.ID != "hi-done" {
		t.Errorf("get completed = (%v, %v), want hi-done", got, err)
	}

	// 物理清除路径仍能读到原始行
	raw, err := d.GetIncludingDeleted(ctx, "hi-del")
	if err != nil || raw == nil || raw.ID != "hi-del" {
		t.Errorf("get including deleted = (%v, %v), want hi-del", raw, err)
	}
}

func ids(list []*model.WfInstance) []string {
	out := make([]string, 0, len(list))
	for _, in := range list {
		out = append(out, in.ID)
	}
	return out
}

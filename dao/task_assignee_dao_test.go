package dao

import (
	"context"
	"testing"

	"github.com/rulego/gflow-engine/model"
)

// 零值 DAO（Query==nil），只覆盖参数校验/nil 输入分支，
// 真实 DB 集成测试在 test/e2e 下跑。

func TestTaskAssigneeDAO_Create_NilEntity(t *testing.T) {
	d := &TaskAssigneeDAO{}
	if err := d.Create(context.Background(), nil); err == nil {
		t.Error("expected error for nil entity")
	}
}

func TestTaskAssigneeDAO_CreateBatch_Empty(t *testing.T) {
	d := &TaskAssigneeDAO{}
	if err := d.CreateBatch(context.Background(), nil); err != nil {
		t.Errorf("expected nil for empty batch, got %v", err)
	}
	if err := d.CreateBatch(context.Background(), []*model.WfTaskAssignee{}); err != nil {
		t.Errorf("expected nil for empty batch, got %v", err)
	}
}

func TestTaskAssigneeDAO_GetByTaskID_EmptyTaskID(t *testing.T) {
	d := &TaskAssigneeDAO{}
	if _, err := d.GetByTaskID(context.Background(), "tenant1", ""); err == nil {
		t.Error("expected error for empty taskID")
	}
}

func TestTaskAssigneeDAO_GetByInstanceAndDefKey_EmptyArgs(t *testing.T) {
	d := &TaskAssigneeDAO{}
	cases := []struct{ inst, key string }{
		{"", "node1"},
		{"inst1", ""},
		{"", ""},
	}
	for _, c := range cases {
		if _, err := d.GetByInstanceAndDefKey(context.Background(), "tenant1", c.inst, c.key); err == nil {
			t.Errorf("expected error for inst=%q key=%q", c.inst, c.key)
		}
	}
}

func TestTaskAssigneeDAO_DeleteByTaskAndEntity_EmptyArgs(t *testing.T) {
	d := &TaskAssigneeDAO{}
	if err := d.DeleteByTaskAndEntity(context.Background(), "tenant1", "", "role", "r1"); err == nil {
		t.Error("expected error for empty taskID")
	}
	if err := d.DeleteByTaskAndEntity(context.Background(), "tenant1", "t1", "", "r1"); err == nil {
		t.Error("expected error for empty entityType")
	}
	if err := d.DeleteByTaskAndEntity(context.Background(), "tenant1", "t1", "role", ""); err == nil {
		t.Error("expected error for empty entityID")
	}
}

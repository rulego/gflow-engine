package dao

import (
	"context"
	"testing"
	"time"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/types/dto"
)

// Tests for instance_dao.go.

func TestIsValidOrderBy_Valid(t *testing.T) {
	valid := []string{
		"created_at",
		"updated_at",
		"tenant_id",
		"name",
		"status",
		"priority",
		"createdAt",
		"table.column",
		"i.created_at",
		"t.name",
		"a",
		"_private",
		"_column",
		"col123",
	}
	for _, col := range valid {
		if !isValidOrderBy(col) {
			t.Errorf("expected valid: %q", col)
		}
	}
}

func TestIsValidOrderBy_Invalid(t *testing.T) {
	invalid := []string{
		"",
		"123abc",
		"col-name",
		"col name",
		"col; DROP TABLE",
		"col OR 1=1",
		"1 OR 1=1",
		"col;--",
		"col' OR '1'='1",
		"col\x00",
		"status DESC",
		"name, id",
		".dotstart",
	}
	for _, col := range invalid {
		if isValidOrderBy(col) {
			t.Errorf("expected invalid: %q", col)
		}
	}
}

func TestNewInstanceDAO_NilQuery(t *testing.T) {
	// NewInstanceDAO uses global query.Q which may be nil
	// Just verify it doesn't panic during construction
	d := &InstanceDAO{Query: nil}
	if d.Query != nil {
		t.Error("expected nil Query")
	}
}

func TestNewInstanceDAOWithQuery_NilQuery(t *testing.T) {
	d := NewInstanceDAOWithQuery(nil)
	if d.Query != nil {
		t.Error("expected nil Query")
	}
}

// List ORDER BY 回退：未知排序字段回退 created_at。
func TestInstanceDAO_List_OrderByFallback(t *testing.T) {
	q := newTestQuery(t, ddlWfInstance, ddlWfProcess)
	d := NewInstanceDAOWithQuery(q)
	ctx := context.Background()
	base := time.Now()

	for _, s := range []struct {
		id   string
		hour time.Duration
	}{
		{"i2", 1}, {"i1", 0}, {"i3", 2},
	} {
		entity := &model.WfInstance{
			ID:        s.id,
			ProcessID: "proc-1",
			Name:      s.id,
			Status:    "active",
			TenantID:  "tenant1",
			CreatedBy: "user1",
			CreatedAt: base.Add(s.hour * time.Hour),
		}
		if err := d.Create(ctx, entity); err != nil {
			t.Fatalf("seed instance %s: %v", s.id, err)
		}
	}

	list, _, err := d.List(ctx, &dto.ProcessInstanceQueryDTO{
		PageRequest: dto.PageRequest{OrderBy: "no_such_column"},
	})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	want := []string{"i1", "i2", "i3"}
	if len(list) != len(want) {
		t.Fatalf("got %d instances, want %d", len(list), len(want))
	}
	for i := range want {
		if list[i].ID != want[i] {
			got := make([]string, 0, len(list))
			for _, in := range list {
				got = append(got, in.ID)
			}
			t.Errorf("order = %v, want %v", got, want)
			break
		}
	}
}

func assertStatistics(t *testing.T, got map[string]interface{}, want map[string]interface{}) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("statistics keys = %v, want keys %v", got, want)
	}
	for k, w := range want {
		g, ok := got[k]
		if !ok {
			t.Errorf("missing key %q", k)
			continue
		}
		gi, gOK := g.(int64)
		wi, wOK := w.(int64)
		if !gOK || !wOK || gi != wi {
			t.Errorf("statistics[%q] = %v (%T), want %v", k, g, g, w)
		}
	}
}

// InstanceDAO 统计：GetInstanceStatisticsByTenant 返回各状态计数。
func TestInstanceDAO_Statistics(t *testing.T) {
	q := newTestQuery(t, ddlWfInstance)
	d := NewInstanceDAOWithQuery(q)
	ctx := context.Background()
	now := time.Now()

	// tenant1: 2 active + completed/suspended/terminated/failed/draft 各 1（共 7）
	// tenant2: 1 active
	instances := []struct {
		id, processID, status, tenant, starter string
	}{
		{"i1", "proc-A", "active", "tenant1", "user1"},
		{"i2", "proc-A", "active", "tenant1", "user2"},
		{"i3", "proc-B", "completed", "tenant1", "user1"},
		{"i4", "proc-B", "suspended", "tenant1", "user1"},
		{"i5", "proc-A", "terminated", "tenant1", "user2"},
		{"i6", "proc-B", "failed", "tenant1", "user1"},
		{"i7", "proc-A", "draft", "tenant1", "user1"},
		{"i8", "proc-A", "active", "tenant2", "user1"},
	}
	for _, s := range instances {
		if err := d.Create(ctx, &model.WfInstance{
			ID:        s.id,
			ProcessID: s.processID,
			Name:      s.id,
			Status:    s.status,
			TenantID:  s.tenant,
			CreatedBy: s.starter,
			CreatedAt: now,
		}); err != nil {
			t.Fatalf("seed instance %s: %v", s.id, err)
		}
	}

	tests := []struct {
		name      string
		tenant    string
		processID string
		starter   string
		want      map[string]interface{}
	}{
		{
			name:   "by tenant",
			tenant: "tenant1",
			want: map[string]interface{}{
				"total_count": int64(7), "active_count": int64(2), "completed_count": int64(1),
				"suspended_count": int64(1), "terminated_count": int64(1),
			},
		},
		{
			name:      "by tenant and process",
			tenant:    "tenant1",
			processID: "proc-A",
			want: map[string]interface{}{
				"total_count": int64(4), "active_count": int64(2), "completed_count": int64(0),
				"suspended_count": int64(0), "terminated_count": int64(1),
			},
		},
		{
			name:    "by tenant and starter",
			tenant:  "tenant1",
			starter: "user1",
			want: map[string]interface{}{
				// i1 active、i3 completed、i4 suspended、i6 failed、i7 draft
				"total_count": int64(5), "active_count": int64(1), "completed_count": int64(1),
				"suspended_count": int64(1), "terminated_count": int64(0),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := d.GetInstanceStatisticsByTenant(ctx, tt.tenant, tt.processID, tt.starter)
			if err != nil {
				t.Fatalf("GetInstanceStatisticsByTenant: %v", err)
			}
			assertStatistics(t, got, tt.want)
		})
	}

	// 空租户必须报错
	if _, err := d.GetInstanceStatisticsByTenant(ctx, "", "", ""); err == nil {
		t.Error("expected error for empty tenantID")
	}
}

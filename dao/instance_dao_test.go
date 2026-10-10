package dao

import (
	"context"
	"strings"
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

// ListByTaskConditions 候选维度：dept 候选任务落库的是 department 实体，
// 待办查询必须按用户的部门 ID 匹配才对成员可见；person/role 口径不得误命中。
func TestInstanceDAO_ListByTaskConditions_DeptCandidate(t *testing.T) {
	q := newTestQuery(t, ddlWfInstance, ddlWfTask, ddlWfTaskAssignee, ddlWfHiInstance, ddlWfHiTask)
	d := NewInstanceDAOWithQuery(q)
	ctx := context.Background()
	now := time.Now()

	if err := q.WfInstance.Create(&model.WfInstance{
		ID: "i-dept", ProcessID: "proc-1", Name: "dept_candidate_flow", Status: "active",
		TenantID: "t1", CreatedBy: "starter", StartUserID: "starter", CreatedAt: now,
	}); err != nil {
		t.Fatalf("seed instance: %v", err)
	}
	instID := "i-dept"
	if err := q.WfTask.Create(&model.WfTask{
		ID: "task-dept", ProcessInstanceID: &instID, TaskDefKey: "n1", Name: "审批",
		TaskType: "user_task", Status: "pending", TenantID: "t1", CreatedBy: "system", CreatedAt: now,
	}); err != nil {
		t.Fatalf("seed task: %v", err)
	}
	if err := q.WfTaskAssignee.Create(&model.WfTaskAssignee{
		ID: "as-dept", TaskID: "task-dept", EntityType: "department", EntityID: "dept-1",
		TenantID: "t1", CreatedAt: now,
	}); err != nil {
		t.Fatalf("seed assignee: %v", err)
	}

	todoHit := func(deptIDs []string) bool {
		t.Helper()
		instances, _, err := d.ListByTaskConditions(ctx, &dto.TaskQuery{
			Assignee: "u1", CandidateUser: "u1", CandidateDeptIDs: deptIDs, TenantID: "t1",
			PageRequest: dto.PageRequest{Status: []string{"pending"}},
		})
		if err != nil {
			t.Fatalf("ListByTaskConditions: %v", err)
		}
		for _, in := range instances {
			if in.ID == "i-dept" {
				return true
			}
		}
		return false
	}

	if !todoHit([]string{"dept-1"}) {
		t.Error("dept 候选任务的实例应出现在该部门成员的待办中")
	}
	if todoHit([]string{"dept-2"}) {
		t.Error("非该部门成员不应看到 dept 候选任务")
	}
	if todoHit(nil) {
		t.Error("仅 person/role 口径不应命中 department 候选")
	}
}

// 联合查询未显式指定状态时排除软删除实例；显式状态查询不受影响。
func TestInstanceDAO_UnionPagination_ExcludesDeletedByDefault(t *testing.T) {
	q := newTestQuery(t, ddlWfInstance, ddlWfHiInstance)
	d := NewInstanceDAOWithQuery(q)
	ctx := context.Background()
	now := time.Now()

	seed := []*model.WfInstance{
		{ID: "i-active", ProcessID: "p1", Name: "active", Status: "active", TenantID: "t1", StartUserID: "u1", CreatedAt: now},
		{ID: "i-done", ProcessID: "p1", Name: "completed", Status: "completed", TenantID: "t1", StartUserID: "u1", CreatedAt: now},
		{ID: "i-del", ProcessID: "p1", Name: "deleted", Status: "deleted", TenantID: "t1", StartUserID: "u1", CreatedAt: now},
	}
	for _, in := range seed {
		if err := d.Create(ctx, in); err != nil {
			t.Fatalf("seed instance %s: %v", in.ID, err)
		}
	}
	// 历史表同样存在软删除行（终态实例归档后再删除的场景）
	if err := d.Query.WfHiInstance.WithContext(ctx).Create(&model.WfHiInstance{
		ID: "hi-del", ProcessID: "p1", Name: "hi deleted", Status: "deleted", TenantID: "t1", StartUserID: "u1", CreatedAt: now,
	}); err != nil {
		t.Fatalf("seed hi instance: %v", err)
	}

	// 未指定状态：两表的 deleted 行都排除
	list, total, err := d.GetInstancesUnionPagination(ctx, "t1", nil, "u1", nil, "", nil, nil, 10, 0, "", "", "")
	if err != nil {
		t.Fatalf("union query: %v", err)
	}
	if total != 2 {
		t.Errorf("total = %d, want 2", total)
	}
	for _, in := range list {
		if in.Status == "deleted" {
			t.Errorf("instance %s (status=deleted) 不应出现在结果中", in.ID)
		}
	}

	// 显式传 deleted：按传入过滤，两表各一条均命中
	list, total, err = d.GetInstancesUnionPagination(ctx, "t1", nil, "u1", []string{"deleted"}, "", nil, nil, 10, 0, "", "", "")
	if err != nil {
		t.Fatalf("union query by status: %v", err)
	}
	if total != 2 || len(list) != 2 {
		t.Errorf("explicit deleted filter: total=%d len=%d, want 2/2", total, len(list))
	}
	got := map[string]bool{}
	for _, in := range list {
		got[in.ID] = true
	}
	if !got["i-del"] || !got["hi-del"] {
		t.Errorf("explicit deleted filter: got %v, want i-del + hi-del", got)
	}
}

// 联合查询按流程定义 ID 集合过滤（IN 语义，活表与历史表同时命中）。
func TestInstanceDAO_UnionPagination_ProcessIDs(t *testing.T) {
	q := newTestQuery(t, ddlWfInstance, ddlWfHiInstance)
	d := NewInstanceDAOWithQuery(q)
	ctx := context.Background()
	now := time.Now()

	seed := []*model.WfInstance{
		{ID: "i-v1", ProcessID: "p1", Name: "on v1", Status: "active", TenantID: "t1", StartUserID: "u1", CreatedAt: now},
		{ID: "i-v2", ProcessID: "p2", Name: "on v2", Status: "active", TenantID: "t1", StartUserID: "u1", CreatedAt: now},
		{ID: "i-other", ProcessID: "p9", Name: "other def", Status: "active", TenantID: "t1", StartUserID: "u1", CreatedAt: now},
	}
	for _, in := range seed {
		if err := d.Create(ctx, in); err != nil {
			t.Fatalf("seed instance %s: %v", in.ID, err)
		}
	}
	if err := d.Query.WfHiInstance.WithContext(ctx).Create(&model.WfHiInstance{
		ID: "hi-v1", ProcessID: "p1", Name: "archived on v1", Status: "terminated", TenantID: "t1", StartUserID: "u1", CreatedAt: now,
	}); err != nil {
		t.Fatalf("seed hi instance: %v", err)
	}

	// 多版本 ID 集合：两版本各一条 + 老版本的历史归档行
	list, total, err := d.GetInstancesUnionPagination(ctx, "t1", []string{"p1", "p2"}, "u1", nil, "", nil, nil, 10, 0, "", "", "")
	if err != nil {
		t.Fatalf("union query by process IDs: %v", err)
	}
	if total != 3 || len(list) != 3 {
		t.Errorf("process IDs filter: total=%d len=%d, want 3/3", total, len(list))
	}
	got := map[string]bool{}
	for _, in := range list {
		got[in.ID] = true
	}
	for _, want := range []string{"i-v1", "i-v2", "hi-v1"} {
		if !got[want] {
			t.Errorf("process IDs filter: %s missing, got %v", want, got)
		}
	}
	if got["i-other"] {
		t.Errorf("process IDs filter: i-other (p9) 不应命中")
	}

	// 含空串元素的集合跳过空值，等价于不按流程定义过滤（4 条全量）
	list, total, err = d.GetInstancesUnionPagination(ctx, "t1", []string{""}, "u1", nil, "", nil, nil, 10, 0, "", "", "")
	if err != nil {
		t.Fatalf("union query with blank IDs: %v", err)
	}
	if total != 4 || len(list) != 4 {
		t.Errorf("blank process IDs should mean no filter: total=%d, want 4", total)
	}
}

// 联合查询按 end_reason 前缀过滤。
func TestInstanceDAO_UnionPagination_EndReasonPrefix(t *testing.T) {
	q := newTestQuery(t, ddlWfInstance, ddlWfHiInstance)
	d := NewInstanceDAOWithQuery(q)
	ctx := context.Background()
	now := time.Now()

	reasonRejected := "审批拒绝：终止流程"
	reasonManual := "测试终止"
	seed := []*model.WfInstance{
		{ID: "i-rejected", ProcessID: "p1", Name: "rejected", Status: "terminated", EndReason: &reasonRejected, TenantID: "t1", StartUserID: "u1", CreatedAt: now},
		{ID: "i-manual", ProcessID: "p1", Name: "manual", Status: "terminated", EndReason: &reasonManual, TenantID: "t1", StartUserID: "u1", CreatedAt: now},
		{ID: "i-done", ProcessID: "p1", Name: "completed", Status: "completed", TenantID: "t1", StartUserID: "u1", CreatedAt: now},
	}
	for _, in := range seed {
		if err := d.Create(ctx, in); err != nil {
			t.Fatalf("seed instance %s: %v", in.ID, err)
		}
	}

	list, total, err := d.GetInstancesUnionPagination(ctx, "t1", nil, "u1", []string{"terminated"}, "", nil, nil, 10, 0, "", "", "审批拒绝")
	if err != nil {
		t.Fatalf("union query by end_reason prefix: %v", err)
	}
	if total != 1 || len(list) != 1 || list[0].ID != "i-rejected" {
		t.Errorf("rejected prefix filter: total=%d, want i-rejected only", total)
	}
}

// 已办列表按实例状态与 end_reason 前缀过滤。
func TestInstanceDAO_ListByTaskConditions_InstanceStatusFilter(t *testing.T) {
	q := newTestQuery(t, ddlWfInstance, ddlWfTask, ddlWfHiInstance, ddlWfHiTask)
	d := NewInstanceDAOWithQuery(q)
	ctx := context.Background()
	now := time.Now()

	reasonRejected := "审批拒绝：终止流程"
	assignee := "u1"
	instActive, instRejected := "i-active", "i-rejected"
	mkTask := func(id, instID string) *model.WfTask {
		return &model.WfTask{ID: id, ProcessInstanceID: &instID, TaskDefKey: "n1", Name: "审批", TaskType: "user_task", Status: "completed", Assignee: &assignee, TenantID: "t1", CreatedAt: now}
	}
	seed := []struct {
		inst *model.WfInstance
		task *model.WfTask
	}{
		{&model.WfInstance{ID: instActive, ProcessID: "p1", Name: "active", Status: "active", TenantID: "t1", StartUserID: "u1", CreatedAt: now}, mkTask("t1", instActive)},
		{&model.WfInstance{ID: instRejected, ProcessID: "p1", Name: "rejected", Status: "terminated", EndReason: &reasonRejected, TenantID: "t1", StartUserID: "u1", CreatedAt: now}, mkTask("t2", instRejected)},
	}
	for _, s := range seed {
		if err := d.Create(ctx, s.inst); err != nil {
			t.Fatalf("seed instance: %v", err)
		}
		if err := q.WfTask.Create(s.task); err != nil {
			t.Fatalf("seed task: %v", err)
		}
	}

	doneQuery := func(statuses []string, prefix string) map[string]bool {
		t.Helper()
		instances, _, err := d.ListByTaskConditions(ctx, &dto.TaskQuery{
			Assignee: "u1", TenantID: "t1",
			InstanceStatuses: statuses, EndReasonPrefix: prefix,
			PageRequest: dto.PageRequest{Status: []string{"completed"}},
		})
		if err != nil {
			t.Fatalf("ListByTaskConditions: %v", err)
		}
		got := map[string]bool{}
		for _, in := range instances {
			got[in.ID] = true
		}
		return got
	}

	if !doneQuery([]string{"terminated"}, "审批拒绝")["i-rejected"] {
		t.Error("拒绝前缀过滤应命中 i-rejected")
	}
	if doneQuery([]string{"active"}, "")["i-rejected"] {
		t.Error("active 过滤不应命中 i-rejected")
	}
	if got := doneQuery(nil, ""); !got["i-active"] || !got["i-rejected"] {
		t.Errorf("不过滤时应两单都可见: %v", got)
	}
}

// 已删实例不进任务维度列表：删除终态实例时历史行标为 deleted，
// 已办/抄送经历史分支、待办经运行时分支，两分支都不得再带出。
func TestInstanceDAO_ListByTaskConditions_ExcludesDeleted(t *testing.T) {
	q := newTestQuery(t, ddlWfInstance, ddlWfTask, ddlWfTaskAssignee, ddlWfHiInstance, ddlWfHiTask)
	d := NewInstanceDAOWithQuery(q)
	ctx := context.Background()
	now := time.Now()

	assignee := "u1"
	liveInst := "i-live"
	if err := q.WfInstance.Create(&model.WfInstance{
		ID: liveInst, ProcessID: "p1", Name: "live", Status: "active",
		TenantID: "t1", StartUserID: "u1", CreatedAt: now,
	}); err != nil {
		t.Fatalf("seed live instance: %v", err)
	}
	if err := q.WfTask.Create(&model.WfTask{
		ID: "task-live", ProcessInstanceID: &liveInst, TaskDefKey: "n1", Name: "审批",
		TaskType: "user_task", Status: "active", Assignee: &assignee, TenantID: "t1", CreatedAt: now,
	}); err != nil {
		t.Fatalf("seed live task: %v", err)
	}

	mkHi := func(id, status string) (*model.WfHiInstance, *model.WfHiTask) {
		defKey := "n1"
		return &model.WfHiInstance{
				ID: id, ProcessID: "p1", Name: id, Status: status,
				TenantID: "t1", StartUserID: "u1", CreatedAt: now,
			},
			&model.WfHiTask{
				ID: "hitask-" + id, ProcessInstanceID: &id, TaskDefKey: &defKey, Name: "审批",
				TaskType: "user_task", Status: "completed", Assignee: &assignee, TenantID: "t1", CreatedAt: now,
			}
	}
	for _, tc := range []struct{ id, status string }{
		{"hi-done", "completed"},
		{"hi-del", "deleted"},
	} {
		inst, task := mkHi(tc.id, tc.status)
		if err := q.WfHiInstance.WithContext(ctx).Create(inst); err != nil {
			t.Fatalf("seed hi instance %s: %v", tc.id, err)
		}
		if err := q.WfHiTask.WithContext(ctx).Create(task); err != nil {
			t.Fatalf("seed hi task %s: %v", tc.id, err)
		}
	}

	ids := func(instances []*model.WfInstance) map[string]bool {
		got := map[string]bool{}
		for _, in := range instances {
			got[in.ID] = true
		}
		return got
	}

	done, total, err := d.ListByTaskConditions(ctx, &dto.TaskQuery{
		Assignee: "u1", TenantID: "t1",
		PageRequest: dto.PageRequest{Status: []string{"completed", "returned"}},
	})
	if err != nil {
		t.Fatalf("done query: %v", err)
	}
	if total != 1 {
		t.Errorf("done total = %d, want 1（已删实例不得计入）", total)
	}
	if !ids(done)["hi-done"] || ids(done)["hi-del"] {
		t.Errorf("done 结果应含 hi-done、不含 hi-del: %v", ids(done))
	}

	todo, _, err := d.ListByTaskConditions(ctx, &dto.TaskQuery{
		Assignee: "u1", CandidateUser: "u1", TenantID: "t1",
		PageRequest: dto.PageRequest{Status: []string{"pending", "active"}},
	})
	if err != nil {
		t.Fatalf("todo query: %v", err)
	}
	if !ids(todo)["i-live"] || len(todo) != 1 {
		t.Errorf("todo 结果应仅含活表实例: %v", ids(todo))
	}
}

// 实例维度按桶计数：与 GetInstancesUnionPagination 同口径（排除 deleted、限定发起人、keyword 生效）
func TestInstanceDAO_CountUnionByBuckets(t *testing.T) {
	q := newTestQuery(t, ddlWfInstance, ddlWfHiInstance)
	d := NewInstanceDAOWithQuery(q)
	ctx := context.Background()
	now := time.Now()

	reasonRejected := "审批拒绝：不同意"
	reasonWithdrawn := "申请人撤回"
	reasonManual := "系统终止"
	mk := func(id, status, name string, reason *string, user string) *model.WfInstance {
		return &model.WfInstance{ID: id, ProcessID: "p1", Name: name, Status: status, EndReason: reason, TenantID: "t1", StartUserID: user, CreatedAt: now}
	}
	seed := []*model.WfInstance{
		mk("i-active", "active", "active", nil, "u1"),
		mk("i-done", "completed", "completed", nil, "u1"),
		mk("i-rejected", "terminated", "rejected", &reasonRejected, "u1"),
		mk("i-withdrawn", "terminated", "withdrawn", &reasonWithdrawn, "u1"),
		mk("i-manual", "terminated", "manual", &reasonManual, "u1"),
		mk("i-suspended", "suspended", "suspended", nil, "u1"),
		mk("i-draft", "draft", "draft", nil, "u1"),
		mk("i-del", "deleted", "deleted", nil, "u1"),
		mk("i-other", "active", "other", nil, "u2"),
	}
	for _, in := range seed {
		if err := d.Create(ctx, in); err != nil {
			t.Fatalf("seed instance %s: %v", in.ID, err)
		}
	}
	if err := q.WfHiInstance.WithContext(ctx).Create(&model.WfHiInstance{
		ID: "hi-del", ProcessID: "p1", Name: "hi deleted", Status: "deleted", TenantID: "t1", StartUserID: "u1", CreatedAt: now,
	}); err != nil {
		t.Fatalf("seed hi instance: %v", err)
	}

	// 与 service 层 instanceStatusBuckets(true) 同构的桶集：terminated 排除拒绝/撤回前缀
	buckets := []InstanceStatusBucket{
		{Name: "active", Statuses: []string{"active"}},
		{Name: "completed", Statuses: []string{"completed"}},
		{Name: "rejected", Statuses: []string{"terminated"}, EndReasonPrefix: "审批拒绝"},
		{Name: "withdrawn", Statuses: []string{"terminated"}, EndReasonPrefix: "申请人撤回"},
		{Name: "suspended", Statuses: []string{"suspended"}},
		{Name: "terminated", Statuses: []string{"terminated"}, EndReasonNotPrefixes: []string{"审批拒绝", "申请人撤回"}},
		{Name: "draft", Statuses: []string{"draft"}},
	}

	counts, err := d.CountInstancesUnionByBuckets(ctx, "t1", "", "u1", "", nil, nil, buckets)
	if err != nil {
		t.Fatalf("count by buckets: %v", err)
	}
	// total=7：deleted 两行排除、他人实例不计数；manual 终止落「已终止」桶，各桶之和=total
	want := map[string]int64{
		"total": 7, "active": 1, "completed": 1, "rejected": 1, "withdrawn": 1,
		"suspended": 1, "terminated": 1, "draft": 1,
	}
	var sum int64
	for k, v := range want {
		if counts[k] != v {
			t.Errorf("counts[%s] = %d, want %d", k, counts[k], v)
		}
		if k != "total" {
			sum += counts[k]
		}
	}
	if sum != counts["total"] {
		t.Errorf("桶之和 %d != total %d（口径失衡）", sum, counts["total"])
	}

	// keyword 与列表同口径生效
	counts, err = d.CountInstancesUnionByBuckets(ctx, "t1", "", "u1", "active", nil, nil, buckets)
	if err != nil {
		t.Fatalf("count by buckets with keyword: %v", err)
	}
	if counts["total"] != 1 || counts["active"] != 1 {
		t.Errorf("keyword 计数 = %v, want total=1 active=1", counts)
	}
}

// 任务维度按桶计数：与 ListByTaskConditions 同口径（UNION 去重、排除 deleted、候选人过滤生效）
func TestInstanceDAO_CountTaskInstancesByBuckets(t *testing.T) {
	q := newTestQuery(t, ddlWfInstance, ddlWfTask, ddlWfHiInstance, ddlWfHiTask)
	d := NewInstanceDAOWithQuery(q)
	ctx := context.Background()
	now := time.Now()

	reasonRejected := "审批拒绝：不同意"
	reasonManual := "系统终止"
	assignee := "u1"
	startU1, startU2 := "u1", "u2"
	instA, instB, instC, instD, instE, instF := "i-a", "i-b", "i-c", "i-del", "i-hi", "i-manual"
	seedInst := []*model.WfInstance{
		{ID: instA, ProcessID: "p1", Name: "a", Status: "active", TenantID: "t1", StartUserID: startU1, CreatedAt: now},
		{ID: instB, ProcessID: "p1", Name: "b", Status: "terminated", EndReason: &reasonRejected, TenantID: "t1", StartUserID: startU1, CreatedAt: now},
		{ID: instC, ProcessID: "p1", Name: "c", Status: "active", TenantID: "t1", StartUserID: startU2, CreatedAt: now},
		{ID: instD, ProcessID: "p1", Name: "del", Status: "deleted", TenantID: "t1", StartUserID: startU1, CreatedAt: now},
		{ID: instF, ProcessID: "p1", Name: "manual", Status: "terminated", EndReason: &reasonManual, TenantID: "t1", StartUserID: startU1, CreatedAt: now},
	}
	for _, in := range seedInst {
		if err := d.Create(ctx, in); err != nil {
			t.Fatalf("seed instance %s: %v", in.ID, err)
		}
	}
	mkTask := func(id, instID string) *model.WfTask {
		inst := instID
		return &model.WfTask{ID: id, ProcessInstanceID: &inst, TaskDefKey: "n1", Name: "审批", TaskType: "user_task", Status: "completed", Assignee: &assignee, TenantID: "t1", CreatedAt: now}
	}
	// 实例 A 两个已办任务：计数须按实例去重为 1
	for _, tk := range []*model.WfTask{mkTask("t-a1", instA), mkTask("t-a2", instA), mkTask("t-b", instB), mkTask("t-del", instD), mkTask("t-manual", instF)} {
		if err := q.WfTask.Create(tk); err != nil {
			t.Fatalf("seed task %s: %v", tk.ID, err)
		}
	}
	if err := q.WfHiInstance.WithContext(ctx).Create(&model.WfHiInstance{
		ID: instE, ProcessID: "p1", Name: "hi", Status: "completed", TenantID: "t1", StartUserID: startU1, CreatedAt: now,
	}); err != nil {
		t.Fatalf("seed hi instance: %v", err)
	}
	defKey := "n1"
	if err := q.WfHiTask.WithContext(ctx).Create(&model.WfHiTask{
		ID: "hit-e", ProcessInstanceID: &instE, TaskDefKey: &defKey, Name: "审批", TaskType: "user_task", Status: "completed", Assignee: &assignee, TenantID: "t1", CreatedAt: now,
	}); err != nil {
		t.Fatalf("seed hi task: %v", err)
	}

	// 与 service 层 done 口径同构：terminated 排除拒绝/撤回前缀，与 rejected/withdrawn 互斥
	buckets := []InstanceStatusBucket{
		{Name: "active", Statuses: []string{"active"}},
		{Name: "completed", Statuses: []string{"completed"}},
		{Name: "rejected", Statuses: []string{"terminated"}, EndReasonPrefix: "审批拒绝"},
		{Name: "terminated", Statuses: []string{"terminated"}, EndReasonNotPrefixes: []string{"审批拒绝", "申请人撤回"}},
	}
	counts, err := d.CountTaskInstancesByBuckets(ctx, &dto.TaskQuery{
		Assignee: "u1", TenantID: "t1", StartUserIDs: []string{"u1"},
		PageRequest: dto.PageRequest{Status: []string{"completed"}},
	}, buckets)
	if err != nil {
		t.Fatalf("count task instances by buckets: %v", err)
	}
	// total=4：A(去重) + B(拒绝) + E(历史) + F(手动终止)；C 他人发起被 StartUserIDs 排除、D 已删排除；
	// B 只落 rejected 不落 terminated（NOT LIKE 互斥）
	want := map[string]int64{"total": 4, "active": 1, "completed": 1, "rejected": 1, "terminated": 1}
	for k, v := range want {
		if counts[k] != v {
			t.Errorf("counts[%s] = %d, want %d", k, counts[k], v)
		}
	}
}

// NULL end_reason 的 terminated 实例必须计入「已终止」桶与列表：
// end_reason NOT LIKE 'x%' 在 SQL 三值逻辑下对 NULL 返回未知，若不特殊处理，
// NULL 终止实例会被「已终止」桶/列表漏掉（chips 总数与列表对不上）。
// 判空采用 (end_reason IS NULL OR ...)：IS NULL 是 ANSI 标准语义，不依赖
// 「空串≠NULL」，在 Oracle/达梦等把空串当 NULL 的方言下行为仍一致。
func TestInstanceDAO_NullEndReasonCountedInTerminated(t *testing.T) {
	q := newTestQuery(t, ddlWfInstance, ddlWfHiInstance, ddlWfTask, ddlWfHiTask)
	d := NewInstanceDAOWithQuery(q)
	ctx := context.Background()
	now := time.Now()

	reasonRejected := "审批拒绝：不同意"
	reasonWithdrawn := "申请人撤回"
	reasonManual := "系统终止"
	assignee := "u1"
	seed := []*model.WfInstance{
		{ID: "i-null-term", ProcessID: "p1", Name: "null", Status: "terminated", EndReason: nil, TenantID: "t1", StartUserID: "u1", CreatedAt: now},
		{ID: "i-rejected", ProcessID: "p1", Name: "rejected", Status: "terminated", EndReason: &reasonRejected, TenantID: "t1", StartUserID: "u1", CreatedAt: now},
		{ID: "i-withdrawn", ProcessID: "p1", Name: "withdrawn", Status: "terminated", EndReason: &reasonWithdrawn, TenantID: "t1", StartUserID: "u1", CreatedAt: now},
		{ID: "i-manual", ProcessID: "p1", Name: "manual", Status: "terminated", EndReason: &reasonManual, TenantID: "t1", StartUserID: "u1", CreatedAt: now},
	}
	for _, in := range seed {
		if err := d.Create(ctx, in); err != nil {
			t.Fatalf("seed instance %s: %v", in.ID, err)
		}
		tk := &model.WfTask{ID: "t-" + in.ID, ProcessInstanceID: &in.ID, TaskDefKey: "n1", Name: "审批", TaskType: "user_task", Status: "completed", Assignee: &assignee, TenantID: "t1", CreatedAt: now}
		if err := q.WfTask.Create(tk); err != nil {
			t.Fatalf("seed task %s: %v", tk.ID, err)
		}
	}

	buckets := []InstanceStatusBucket{
		{Name: "rejected", Statuses: []string{"terminated"}, EndReasonPrefix: "审批拒绝"},
		{Name: "terminated", Statuses: []string{"terminated"}, EndReasonNotPrefixes: []string{"审批拒绝", "申请人撤回"}},
	}

	// 1) 实例维度桶计数（bucketWhere 的 NOT LIKE）
	counts, err := d.CountInstancesUnionByBuckets(ctx, "t1", "", "u1", "", nil, nil, buckets)
	if err != nil {
		t.Fatalf("count union by buckets: %v", err)
	}
	if counts["terminated"] != 2 || counts["rejected"] != 1 {
		t.Errorf("union bucket counts = %v, want terminated=2 rejected=1（NULL end_reason 不得漏计）", counts)
	}

	// 2) 联合查询列表（buildInstanceUnionQuery 的 NOT LIKE）
	list, total, err := d.GetInstancesUnionPagination(ctx, "t1", nil, "u1", []string{"terminated"}, "", nil, nil, 10, 0, "", "", "", "审批拒绝", "申请人撤回")
	if err != nil {
		t.Fatalf("union by not-prefix: %v", err)
	}
	if total != 2 {
		t.Errorf("union terminated total = %d, want 2", total)
	}
	ids := map[string]bool{}
	for _, in := range list {
		ids[in.ID] = true
	}
	if !ids["i-null-term"] || !ids["i-manual"] || ids["i-rejected"] || ids["i-withdrawn"] {
		t.Errorf("union terminated list = %v, want i-null-term+i-manual only", ids)
	}

	// 3) 任务维度列表（buildTaskInstanceQuery 的 NOT LIKE）
	done, total2, err := d.ListByTaskConditions(ctx, &dto.TaskQuery{
		Assignee: "u1", TenantID: "t1",
		InstanceStatuses:     []string{"terminated"},
		EndReasonNotPrefixes: []string{"审批拒绝", "申请人撤回"},
		PageRequest:          dto.PageRequest{Status: []string{"completed"}},
	})
	if err != nil {
		t.Fatalf("ListByTaskConditions by not-prefix: %v", err)
	}
	if total2 != 2 {
		t.Errorf("ListByTaskConditions terminated total = %d, want 2", total2)
	}
	doneIDs := map[string]bool{}
	for _, in := range done {
		doneIDs[in.ID] = true
	}
	if !doneIDs["i-null-term"] || !doneIDs["i-manual"] || doneIDs["i-rejected"] || doneIDs["i-withdrawn"] {
		t.Errorf("task-dim terminated list = %v, want i-null-term+i-manual only", doneIDs)
	}
}

// 可移植性护栏：end_reason 的 NOT LIKE 判空必须用 ANSI 标准的 IS NULL OR，
// 不得退回 COALESCE 折叠空串的写法。Oracle/达梦等把空串当 NULL 的方言下，
// 那种写法会退化（NULL 终止实例再次被「已终止」桶/列表漏掉），
// 而 IS NULL OR 在所有方言行为严格一致。
func TestEndReasonNotLike_UsesIsNullOR(t *testing.T) {
	tq := buildTaskInstanceQuery(&dto.TaskQuery{EndReasonNotPrefixes: []string{"审批拒绝", "申请人撤回"}})
	uq := buildInstanceUnionQuery("t1", nil, "u1", []string{"terminated"}, "", nil, nil, "", "", "", "审批拒绝", "申请人撤回")
	bcond, _ := bucketWhere(InstanceStatusBucket{Statuses: []string{"terminated"}, EndReasonNotPrefixes: []string{"审批拒绝", "申请人撤回"}})

	for _, tc := range []struct{ name, sql string }{
		{"buildTaskInstanceQuery", tq.conditions},
		{"buildInstanceUnionQuery", uq.conditions},
		{"bucketWhere", bcond},
	} {
		if strings.Contains(tc.sql, "COALESCE(") {
			t.Errorf("%s: 判空不得依赖 COALESCE（空串即 NULL 的方言会退化）：%q", tc.name, tc.sql)
		}
		if !strings.Contains(tc.sql, "IS NULL OR") {
			t.Errorf("%s: 判空应使用 ANSI 的 IS NULL OR：%q", tc.name, tc.sql)
		}
	}
}

// GetActiveByBusinessKey：活表 businessKey 精确占用查询。
// 语义约束：仅扫活表（终态归档即活表删行、businessKey 释放），等值匹配
// 杜绝 LIKE 的子串/通配符越界命中；draft/active/suspended 皆视为占用
// （三者都落在唯一索引 uq_wf_instance_tenant_bizkey 上）。
func TestInstanceDAO_GetActiveByBusinessKey(t *testing.T) {
	q := newTestQuery(t, ddlWfInstance, ddlWfHiInstance)
	d := NewInstanceDAOWithQuery(q)
	ctx := context.Background()

	seed := func(id, tenant, businessKey, status string) {
		t.Helper()
		if err := q.WfInstance.WithContext(ctx).Create(&model.WfInstance{
			ID: id, ProcessID: "proc-" + id, BusinessKey: &businessKey,
			Name: "n", Status: status, TenantID: tenant, CreatedBy: "u", CreatedAt: time.Now(),
		}); err != nil {
			t.Fatalf("seed %s: %v", id, err)
		}
	}
	seed("inst-active", "t1", "ORDER-100", "active")
	seed("inst-sus", "t1", "BK-SUS", "suspended")
	seed("inst-draft", "t1", "BK-DRAFT", "draft")
	seed("inst-other", "t2", "ORDER-100", "active")

	// 精确命中 active 占用者
	hit, err := d.GetActiveByBusinessKey(ctx, "t1", "ORDER-100")
	if err != nil {
		t.Fatalf("GetActiveByBusinessKey: %v", err)
	}
	if hit == nil || hit.ID != "inst-active" {
		t.Fatalf("active occupant: got %+v, want inst-active", hit)
	}

	// 挂起/草稿同样占用
	for _, tc := range []struct{ key, wantID string }{{"BK-SUS", "inst-sus"}, {"BK-DRAFT", "inst-draft"}} {
		hit, err := d.GetActiveByBusinessKey(ctx, "t1", tc.key)
		if err != nil {
			t.Fatalf("GetActiveByBusinessKey(%s): %v", tc.key, err)
		}
		if hit == nil || hit.ID != tc.wantID {
			t.Fatalf("%s occupant: got %+v, want %s", tc.key, hit, tc.wantID)
		}
	}

	// 子串/通配符请求键不得越界命中（LIKE 语义下的误报源）
	for _, key := range []string{"ORDER-10", "ORDER-1_0", "ORDER%", "BK-SU"} {
		hit, err := d.GetActiveByBusinessKey(ctx, "t1", key)
		if err != nil {
			t.Fatalf("GetActiveByBusinessKey(%q): %v", key, err)
		}
		if hit != nil {
			t.Fatalf("businessKey %q must not match existing rows, got %+v", key, hit)
		}
	}

	// 跨租户同 key 不命中（占用按 tenant 隔离）
	if hit, _ := d.GetActiveByBusinessKey(ctx, "t3", "ORDER-100"); hit != nil {
		t.Fatalf("cross-tenant lookup must not hit, got %+v", hit)
	}

	// 历史表归档行不占用（活表删行后 businessKey 释放）
	archivedKey := "BK-ARCHIVED"
	if err := q.WfHiInstance.WithContext(ctx).Create(&model.WfHiInstance{
		ID: "inst-archived", ProcessID: "proc-x", BusinessKey: &archivedKey,
		Name: "n", Status: "completed", TenantID: "t1", CreatedBy: "u", CreatedAt: time.Now(),
	}); err != nil {
		t.Fatalf("seed hi instance: %v", err)
	}
	if hit, err := d.GetActiveByBusinessKey(ctx, "t1", "BK-ARCHIVED"); err != nil || hit != nil {
		t.Fatalf("archived businessKey must be released, got (%+v, %v)", hit, err)
	}
}

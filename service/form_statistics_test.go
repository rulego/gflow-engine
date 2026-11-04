package service

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/query"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func ptrJSON(s string) *string { return &s }

func TestCompileAndResolveFormStatsPath(t *testing.T) {
	segs, err := compileFormStatsPath("days")
	if err != nil || len(segs) != 1 || segs[0].key != "days" || segs[0].slice {
		t.Fatalf("plain path compile failed: %+v err=%v", segs, err)
	}

	segs, err = compileFormStatsPath("detail[].hours")
	if err != nil || len(segs) != 2 || !segs[0].slice || segs[1].key != "hours" {
		t.Fatalf("subform path compile failed: %+v err=%v", segs, err)
	}

	if _, err := compileFormStatsPath(""); err == nil {
		t.Fatal("empty path should fail")
	}
	if _, err := compileFormStatsPath("a..b"); err == nil {
		t.Fatal("malformed path should fail")
	}

	vars := map[string]interface{}{
		"days":   float64(3),
		"detail": []interface{}{map[string]interface{}{"hours": float64(8)}, map[string]interface{}{"hours": "4"}},
		"nested": map[string]interface{}{"inner": map[string]interface{}{"v": float64(1)}},
	}
	if got := resolveFormStatsPath(vars, mustSegs(t, "days")); len(got) != 1 {
		t.Fatalf("plain resolve want 1 value, got %v", got)
	}
	// 子表展开：两行明细各出一个值，字符串数字由 coerce 兜底
	if got := resolveFormStatsPath(vars, mustSegs(t, "detail[].hours")); len(got) != 2 {
		t.Fatalf("subform resolve want 2 values, got %v", got)
	}
	if got := resolveFormStatsPath(vars, mustSegs(t, "nested.inner.v")); len(got) != 1 {
		t.Fatalf("nested resolve want 1 value, got %v", got)
	}
	if got := resolveFormStatsPath(vars, mustSegs(t, "missing")); got != nil {
		t.Fatalf("missing path want nil, got %v", got)
	}
	// 数组段后跟非对象元素：只保留可继续下钻的行
	mixed := map[string]interface{}{"rows": []interface{}{map[string]interface{}{"v": 1}, "scalar"}}
	if got := resolveFormStatsPath(mixed, mustSegs(t, "rows[].v")); len(got) != 1 {
		t.Fatalf("mixed rows want 1 value, got %v", got)
	}
}

func mustSegs(t *testing.T, path string) []formStatsSegment {
	t.Helper()
	segs, err := compileFormStatsPath(path)
	if err != nil {
		t.Fatalf("compile %s: %v", path, err)
	}
	return segs
}

func TestCoerceFormStatsFloat(t *testing.T) {
	if f, ok := coerceFormStatsFloat(float64(2.5)); !ok || f != 2.5 {
		t.Fatalf("float64 coerce failed: %v %v", f, ok)
	}
	if f, ok := coerceFormStatsFloat(" 12 "); !ok || f != 12 {
		t.Fatalf("string number coerce failed: %v %v", f, ok)
	}
	if _, ok := coerceFormStatsFloat(""); ok {
		t.Fatal("empty string should not coerce")
	}
	if _, ok := coerceFormStatsFloat(true); ok {
		t.Fatal("bool should not coerce")
	}
	if _, ok := coerceFormStatsFloat([]interface{}{1}); ok {
		t.Fatal("slice should not coerce")
	}
}

func TestCoerceFormStatsKeys(t *testing.T) {
	if keys := coerceFormStatsKeys([]interface{}{"年假", "调休"}); len(keys) != 2 || keys[0] != "年假" {
		t.Fatalf("multi value keys failed: %v", keys)
	}
	if keys := coerceFormStatsKeys(true); len(keys) != 1 || keys[0] != "true" {
		t.Fatalf("bool key failed: %v", keys)
	}
	if keys := coerceFormStatsKeys(float64(3)); len(keys) != 1 || keys[0] != "3" {
		t.Fatalf("int-valued float key failed: %v", keys)
	}
	if keys := coerceFormStatsKeys(""); len(keys) != 0 {
		t.Fatalf("empty string should yield no key: %v", keys)
	}
}

func TestMetricAccFeedAndResult(t *testing.T) {
	acc := &metricAcc{}
	acc.feed([]interface{}{float64(3), float64(7)})
	acc.feed([]interface{}{float64(5)})
	acc.feed(nil) // 缺字段
	acc.feed([]interface{}{"not-a-number"})

	sum := acc.result(FormStatsMetricRequest{Agg: FormStatsAggSum})
	if sum.Value != 15 || sum.Count != 3 || sum.Skipped != 2 {
		t.Fatalf("sum result failed: %+v", sum)
	}
	avg := acc.result(FormStatsMetricRequest{Agg: FormStatsAggAvg})
	if avg.Value != 5 {
		t.Fatalf("avg want 5, got %v", avg.Value)
	}
	min := acc.result(FormStatsMetricRequest{Agg: FormStatsAggMin})
	max := acc.result(FormStatsMetricRequest{Agg: FormStatsAggMax})
	if min.Value != 3 || max.Value != 7 {
		t.Fatalf("min/max failed: %v %v", min.Value, max.Value)
	}

	empty := (&metricAcc{}).result(FormStatsMetricRequest{Agg: FormStatsAggSum})
	if empty.Value != 0 || empty.Count != 0 {
		t.Fatalf("empty acc should be zero: %+v", empty)
	}
}

func TestDistAccOptionsAlignmentAndMultiPick(t *testing.T) {
	acc := &distAcc{counts: make(map[string]int64)}
	acc.feed([]interface{}{"年假"})
	acc.feed([]interface{}{"病假"})
	acc.feed([]interface{}{[]interface{}{"年假", "事假"}}) // 多选拆票：一票两桶
	acc.feed(nil)

	r := acc.result(FormStatsDistributionRequest{Options: []string{"年假", "病假", "事假", "调休"}})
	if r.Total != 4 || r.Skipped != 1 {
		t.Fatalf("total/skipped want 4/1, got %v/%v", r.Total, r.Skipped)
	}
	byKey := map[string]FormStatsBucket{}
	for _, b := range r.Buckets {
		byKey[b.Key] = b
	}
	if byKey["年假"].Count != 2 || byKey["调休"].Count != 0 {
		t.Fatalf("bucket counts failed: %+v", byKey)
	}
	// 全集对齐：零票选项也出桶
	if len(r.Buckets) != 4 {
		t.Fatalf("options alignment want 4 buckets, got %d", len(r.Buckets))
	}
	// 分母固定为实例数：年假 2/4=50%
	if byKey["年假"].Percentage != 50 {
		t.Fatalf("percentage want 50, got %v", byKey["年假"].Percentage)
	}
}

func newStatsAcc(t *testing.T) *formStatsAccumulator {
	t.Helper()
	acc, err := newFormStatsAccumulator(&FormStatsQuery{
		Metrics:       []FormStatsMetricRequest{{Path: "days", Agg: FormStatsAggSum}},
		Distributions: []FormStatsDistributionRequest{{Path: "leaveType"}},
		GroupBy: []FormStatsGroupByRequest{
			{Kind: FormStatsGroupStartUser},
			{Kind: FormStatsGroupField, Path: "leaveType"},
		},
	})
	if err != nil {
		t.Fatalf("newFormStatsAccumulator: %v", err)
	}
	return acc
}

func TestAccumulatorFeedGroups(t *testing.T) {
	acc := newStatsAcc(t)
	base := time.Date(2026, 11, 3, 10, 0, 0, 0, time.Local)
	acc.feed(map[string]interface{}{"days": float64(3), "leaveType": "年假"}, base, "u1")
	acc.feed(map[string]interface{}{"days": float64(5), "leaveType": "病假"}, base, "u1")
	acc.feed(map[string]interface{}{"days": float64(2)}, base, "u2") // 无类型：field 维度不进桶

	r := acc.snapshot()
	if r.Total != 3 {
		t.Fatalf("total want 3, got %v", r.Total)
	}
	if len(r.Metrics) != 1 || r.Metrics[0].Value != 10 || r.Metrics[0].Skipped != 0 {
		t.Fatalf("root metric failed: %+v", r.Metrics)
	}

	byGroup := map[string]FormStatsGroupResult{}
	for _, g := range r.Groups {
		byGroup[string(g.Kind)+"|"+g.Key] = g
	}
	u1 := byGroup["startUser|u1"]
	if len(u1.Metrics) != 1 || u1.Metrics[0].Value != 8 {
		t.Fatalf("startUser u1 group want sum 8: %+v", u1)
	}
	u2 := byGroup["startUser|u2"]
	if len(u2.Metrics) != 1 || u2.Metrics[0].Value != 2 {
		t.Fatalf("startUser u2 group want sum 2: %+v", u2)
	}
	annual := byGroup["field|年假"]
	if len(annual.Metrics) != 1 || annual.Metrics[0].Value != 3 {
		t.Fatalf("field 年假 group want sum 3: %+v", annual)
	}
	if _, exists := byGroup["field|病假"]; !exists {
		t.Fatal("field 病假 group missing")
	}
	// u2 的单没有 leaveType：field 维度只有两个桶，startUser 维度两个桶
	if len(r.Groups) != 4 {
		t.Fatalf("groups want 4 rows, got %d: %+v", len(r.Groups), r.Groups)
	}
}

func TestAccumulatorTimeBucketGroup(t *testing.T) {
	acc, err := newFormStatsAccumulator(&FormStatsQuery{
		Metrics: []FormStatsMetricRequest{{Path: "days", Agg: FormStatsAggSum}},
		GroupBy: []FormStatsGroupByRequest{{Kind: FormStatsGroupTime, Bucket: "month"}},
	})
	if err != nil {
		t.Fatalf("newFormStatsAccumulator: %v", err)
	}
	acc.feed(map[string]interface{}{"days": 1}, time.Date(2026, 10, 31, 23, 59, 0, 0, time.Local), "u1")
	acc.feed(map[string]interface{}{"days": 2}, time.Date(2026, 11, 1, 0, 1, 0, 0, time.Local), "u1")

	r := acc.snapshot()
	if len(r.Groups) != 2 {
		t.Fatalf("time bucket groups want 2, got %+v", r.Groups)
	}
	if r.Groups[0].Key != "2026-10" || r.Groups[1].Key != "2026-11" {
		t.Fatalf("month keys wrong: %+v", r.Groups)
	}
}

// ============================================================================
// Aggregate 集成：sqlite 内存库走 DAO 全链路
// ============================================================================

func newFormStatsTestDB(t *testing.T) *query.Query {
	t.Helper()
	// 唯一库名隔离：固定 URI 的 cache=shared 会让同包其他测试的残留数据串库
	dbName := fmt.Sprintf("file:formstat_%d?mode=memory&cache=shared&_busy_timeout=5000", time.Now().UnixNano())
	db, err := gorm.Open(sqlite.Open(dbName), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() {
		if sqlDB, e := db.DB(); e == nil {
			_ = sqlDB.Close()
		}
	})
	ddls := []string{
		`CREATE TABLE IF NOT EXISTS wf_process (
			id TEXT PRIMARY KEY,
			process_key TEXT NOT NULL,
			name TEXT NOT NULL,
			version INTEGER NOT NULL DEFAULT 1,
			category TEXT,
			description TEXT,
			definition_json TEXT NOT NULL DEFAULT '{}',
			status TEXT NOT NULL DEFAULT 'active',
			publish_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			icon TEXT NOT NULL DEFAULT '',
			process_type TEXT NOT NULL DEFAULT 'main',
			tenant_id TEXT NOT NULL,
			created_by TEXT NOT NULL DEFAULT '',
			created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_by TEXT,
			updated_at DATETIME,
			ext TEXT
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
			created_at DATETIME NOT NULL,
			updated_by TEXT,
			updated_at DATETIME,
			end_reason TEXT,
			duration INTEGER,
			ended_at DATETIME,
			start_user_id TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE TABLE IF NOT EXISTS wf_instance (
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
			created_at DATETIME NOT NULL,
			updated_by TEXT,
			updated_at DATETIME,
			end_reason TEXT,
			duration INTEGER,
			ended_at DATETIME,
			start_user_id TEXT NOT NULL DEFAULT ''
		)`,
	}
	for _, ddl := range ddls {
		if err := db.Exec(ddl).Error; err != nil {
			t.Fatalf("ddl: %v", err)
		}
	}
	return query.Use(db)
}

// seedLeaveProcess 写入统计查询 join 依赖的流程定义行（各用例显式调用，
// 不藏在实例种子里形成对调用顺序的隐式依赖）。
func seedLeaveProcess(t *testing.T, q *query.Query, createdAt time.Time) {
	t.Helper()
	proc := model.WfProcess{
		ID: "proc-1", ProcessKey: "leave", Name: "请假", Version: 1,
		DefinitionJSON: "{}", Status: "active", Icon: "", ProcessType: "main",
		TenantID: "t1", CreatedBy: "seeder", CreatedAt: createdAt, PublishTime: createdAt,
	}
	if err := q.WfProcess.Save(&proc); err != nil {
		t.Fatalf("seed process: %v", err)
	}
}

func seedHiInstance(t *testing.T, q *query.Query, id, tenant, status, vars, startUser string, createdAt time.Time) {
	t.Helper()
	inst := model.WfHiInstance{
		ID: id, ProcessID: "proc-1", Name: "n", Status: status,
		Variables: ptrJSON(vars), TenantID: tenant, CreatedBy: startUser,
		CreatedAt: createdAt, StartUserID: startUser,
	}
	if err := q.WfHiInstance.Save(&inst); err != nil {
		t.Fatalf("seed instance %s: %v", id, err)
	}
}

func TestAggregateFullPipeline(t *testing.T) {
	q := newFormStatsTestDB(t)
	svc := NewFormStatisticsService(q)
	ctx := context.Background()
	now := time.Now()
	seedLeaveProcess(t, q, now)

	seedHiInstance(t, q, "i1", "t1", "completed", `{"days":3,"leaveType":"年假"}`, "u1", now)
	seedHiInstance(t, q, "i2", "t1", "completed", `{"days":5,"leaveType":"病假"}`, "u2", now)
	seedHiInstance(t, q, "i3", "t1", "terminated", `{"days":2}`, "u1", now)
	seedHiInstance(t, q, "i4", "t1", "active", `{"days":99}`, "u1", now) // 运行中：默认口径不含
	seedHiInstance(t, q, "i5", "other", "completed", `{"days":7}`, "u9", now)

	res, err := svc.Aggregate(ctx, Actor{UserID: "admin", TenantID: "t1"}, &FormStatsQuery{
		ProcessKey: "leave",
		Start:      now.Add(-24 * time.Hour),
		End:        now.Add(time.Hour),
		Metrics:    []FormStatsMetricRequest{{Path: "days", Agg: FormStatsAggSum}},
		Distributions: []FormStatsDistributionRequest{
			{Path: "leaveType", Options: []string{"年假", "病假", "事假"}},
		},
		GroupBy: []FormStatsGroupByRequest{{Kind: FormStatsGroupStartUser}},
	})
	if err != nil {
		t.Fatalf("aggregate: %v", err)
	}
	if res.Total != 3 {
		t.Fatalf("total want 3 (terminal + same tenant), got %d", res.Total)
	}
	if res.Metrics[0].Value != 10 || res.Metrics[0].Skipped != 0 {
		t.Fatalf("sum want 10, got %+v", res.Metrics[0])
	}
	if len(res.Groups) != 2 {
		t.Fatalf("groups want u1/u2, got %+v", res.Groups)
	}
	dist := res.Distributions[0]
	if dist.Total != 3 || dist.Skipped != 1 {
		t.Fatalf("dist total/skipped want 3/1, got %v/%v", dist.Total, dist.Skipped)
	}
}

// 多批次 keyset 遍历正确性：1100 行跨 2 个拉取批次（formStatsBatch+余量），
// 游标推进不得丢行/重行，按用户分组的分组和与逐行累加的期望值对账。
func TestAggregateKeysetMultiBatch(t *testing.T) {
	q := newFormStatsTestDB(t)
	svc := NewFormStatisticsService(q)
	ctx := context.Background()
	now := time.Now()
	seedLeaveProcess(t, q, now)

	const rows = formStatsBatch + 100
	wantSum := 0
	wantByUser := map[string]int{}
	batch := make([]*model.WfHiInstance, 0, rows)
	for i := 0; i < rows; i++ {
		days := 1 + i%5
		user := fmt.Sprintf("u%d", i%3)
		wantSum += days
		wantByUser[user] += days
		batch = append(batch, &model.WfHiInstance{
			ID: fmt.Sprintf("k%04d", i), ProcessID: "proc-1", Name: "n", Status: "completed",
			Variables: ptrJSON(fmt.Sprintf(`{"days":%d}`, days)), TenantID: "t1",
			CreatedBy: user, CreatedAt: now, StartUserID: user,
		})
	}
	// 500 行一批：单条 INSERT 变量数不超 sqlite 上限
	if err := q.WfHiInstance.WithContext(ctx).CreateInBatches(batch, 500); err != nil {
		t.Fatalf("seed instances: %v", err)
	}

	res, err := svc.Aggregate(ctx, Actor{UserID: "admin", TenantID: "t1"}, &FormStatsQuery{
		ProcessKey: "leave",
		Start:      now.Add(-24 * time.Hour),
		End:        now.Add(time.Hour),
		Metrics:    []FormStatsMetricRequest{{Path: "days", Agg: FormStatsAggSum}},
		GroupBy:    []FormStatsGroupByRequest{{Kind: FormStatsGroupStartUser}},
	})
	if err != nil {
		t.Fatalf("aggregate: %v", err)
	}
	if res.Total != rows {
		t.Fatalf("total want %d, got %d", rows, res.Total)
	}
	if res.Metrics[0].Value != float64(wantSum) || res.Metrics[0].Skipped != 0 {
		t.Fatalf("sum want %d, got %+v", wantSum, res.Metrics[0])
	}
	if len(res.Groups) != len(wantByUser) {
		t.Fatalf("groups want %d, got %d", len(wantByUser), len(res.Groups))
	}
	for _, g := range res.Groups {
		sum := 0.0
		for _, m := range g.Metrics {
			if m.Agg == FormStatsAggSum {
				sum = m.Value
			}
		}
		if sum != float64(wantByUser[g.Key]) {
			t.Fatalf("group %s sum want %d, got %v", g.Key, wantByUser[g.Key], sum)
		}
	}
}

// 分片规划正确性：行数超过 formStatsChunkTarget 时窗口会被对半再切，
// 聚在同一时刻的行（极端聚集）与分片边界时刻的行都只计一次。
func TestAggregateChunkBoundaryDedup(t *testing.T) {
	q := newFormStatsTestDB(t)
	svc := NewFormStatisticsService(q)
	ctx := context.Background()
	now := time.Now()
	seedLeaveProcess(t, q, now)

	// 全部行挤在窗口中段的同一时刻，且行数超分片阈值，强制走递归再切
	const rows = formStatsChunkTarget + 500
	burstAt := now.Add(-10 * time.Hour)
	batch := make([]*model.WfHiInstance, 0, rows)
	for i := 0; i < rows; i++ {
		batch = append(batch, &model.WfHiInstance{
			ID: fmt.Sprintf("b%05d", i), ProcessID: "proc-1", Name: "n", Status: "completed",
			Variables: ptrJSON(`{"days":2}`), TenantID: "t1",
			CreatedBy: "u1", CreatedAt: burstAt, StartUserID: "u1",
		})
	}
	// 500 行一批：单条 INSERT 变量数不超 sqlite 上限
	if err := q.WfHiInstance.WithContext(ctx).CreateInBatches(batch, 500); err != nil {
		t.Fatalf("seed instances: %v", err)
	}

	res, err := svc.Aggregate(ctx, Actor{UserID: "admin", TenantID: "t1"}, &FormStatsQuery{
		ProcessKey: "leave",
		Start:      now.Add(-24 * time.Hour),
		End:        now.Add(time.Hour),
		Metrics:    []FormStatsMetricRequest{{Path: "days", Agg: FormStatsAggSum}},
	})
	if err != nil {
		t.Fatalf("aggregate: %v", err)
	}
	if res.Total != rows {
		t.Fatalf("total want %d, got %d", rows, res.Total)
	}
	if res.Metrics[0].Value != float64(2*rows) {
		t.Fatalf("sum want %d, got %v", 2*rows, res.Metrics[0].Value)
	}
}

func TestAggregateIncludeActive(t *testing.T) {
	q := newFormStatsTestDB(t)
	svc := NewFormStatisticsService(q)
	ctx := context.Background()
	now := time.Now()

	seedLeaveProcess(t, q, now)
	seedHiInstance(t, q, "a1", "t1", "completed", `{"days":3}`, "u1", now)
	// 运行表两行：active 实例 + 已完成未归档实例（归档窗口内）
	seedLeaveProcess(t, q, now)
	rtActive := model.WfInstance{
		ID: "a2", ProcessID: "proc-1", Name: "n", Status: "active",
		Variables: ptrJSON(`{"days":4}`), TenantID: "t1", CreatedBy: "u1",
		CreatedAt: now, StartUserID: "u1",
	}
	rtDone := model.WfInstance{
		ID: "a3", ProcessID: "proc-1", Name: "n", Status: "completed",
		Variables: ptrJSON(`{"days":10}`), TenantID: "t1", CreatedBy: "u1",
		CreatedAt: now, StartUserID: "u1",
	}
	if err := q.WfInstance.Save(&rtActive); err != nil {
		t.Fatalf("seed runtime active: %v", err)
	}
	if err := q.WfInstance.Save(&rtDone); err != nil {
		t.Fatalf("seed runtime done: %v", err)
	}

	// IncludeActive：历史 3 + 运行表 active 4 + 运行表未归档 completed 10
	res, err := svc.Aggregate(ctx, Actor{UserID: "admin", TenantID: "t1"}, &FormStatsQuery{
		ProcessKey:    "leave",
		Start:         now.Add(-24 * time.Hour),
		End:           now.Add(time.Hour),
		IncludeActive: true,
		Metrics:       []FormStatsMetricRequest{{Path: "days", Agg: FormStatsAggSum}},
	})
	if err != nil {
		t.Fatalf("aggregate: %v", err)
	}
	if res.Total != 3 || res.Metrics[0].Value != 17 {
		t.Fatalf("include active want total 3 sum 17, got total %d sum %v", res.Total, res.Metrics[0].Value)
	}

	// 默认口径（仅终态）：未归档 completed 也必须计入（归档窗口不漏单）
	resTerm, err := svc.Aggregate(ctx, Actor{UserID: "admin", TenantID: "t1"}, &FormStatsQuery{
		ProcessKey: "leave",
		Start:      now.Add(-24 * time.Hour),
		End:        now.Add(time.Hour),
		Metrics:    []FormStatsMetricRequest{{Path: "days", Agg: FormStatsAggSum}},
	})
	if err != nil {
		t.Fatalf("aggregate terminal: %v", err)
	}
	if resTerm.Total != 2 || resTerm.Metrics[0].Value != 13 {
		t.Fatalf("terminal incl unarchived want total 2 sum 13, got total %d sum %v", resTerm.Total, resTerm.Metrics[0].Value)
	}
}

func TestAggregateScopeGuard(t *testing.T) {
	q := newFormStatsTestDB(t)
	svc := NewFormStatisticsService(q)
	ctx := context.Background()
	now := time.Now()
	seedLeaveProcess(t, q, now)
	for i := 0; i < 3; i++ {
		seedHiInstance(t, q, fmt.Sprintf("g%d", i), "t1", "completed", `{"days":1}`, "u1", now)
	}
	err := func() error {
		_, err := svc.Aggregate(ctx, Actor{UserID: "admin", TenantID: "t1"}, &FormStatsQuery{
			ProcessKey: "leave",
			Start:      now.Add(-24 * time.Hour),
			End:        now.Add(time.Hour),
			Metrics:    []FormStatsMetricRequest{{Path: "days", Agg: FormStatsAggSum}},
			// 护栏压到 2：3 条终态应被拒绝
			MaxInstances: 2,
		})
		return err
	}()
	if err == nil || !errors.Is(err, ErrFormStatsScopeTooLarge) {
		t.Fatalf("scope guard should reject 3 instances with limit 2, got %v", err)
	}
}

func TestAggregateInputValidation(t *testing.T) {
	q := newFormStatsTestDB(t)
	svc := NewFormStatisticsService(q)
	ctx := context.Background()
	actor := Actor{UserID: "admin", TenantID: "t1"}

	if _, err := svc.Aggregate(ctx, Actor{UserID: "x"}, &FormStatsQuery{ProcessKey: "p"}); err == nil {
		t.Fatal("missing tenant should fail")
	}
	if _, err := svc.Aggregate(ctx, actor, &FormStatsQuery{}); err == nil {
		t.Fatal("missing process key should fail")
	}
	if _, err := svc.Aggregate(ctx, actor, &FormStatsQuery{
		ProcessKey: "p", Start: time.Now(), End: time.Now().Add(-time.Hour),
		Metrics: []FormStatsMetricRequest{{Path: "d", Agg: FormStatsAggSum}},
	}); err == nil {
		t.Fatal("reversed time range should fail")
	}
	if _, err := svc.Aggregate(ctx, actor, &FormStatsQuery{
		ProcessKey: "p", Start: time.Now(), End: time.Now().Add(time.Hour),
	}); err == nil {
		t.Fatal("empty plan should fail")
	}
}

// TestAggregatePaginationBeyondDaoLimit 回归：iterate 的批大小若超过 DAO
// 分页硬上限（dto.MaxPageSize=200，超限被静默截断），首页会被误判为末页，
// 200 行之后的实例全部漏统计。
func TestAggregatePaginationBeyondDaoLimit(t *testing.T) {
	q := newFormStatsTestDB(t)
	svc := NewFormStatisticsService(q)
	ctx := context.Background()
	now := time.Now()
	seedLeaveProcess(t, q, now)
	const rows = 205
	for i := 0; i < rows; i++ {
		seedHiInstance(t, q, fmt.Sprintf("p%03d", i), "t1", "completed", `{"days":1}`, "u1", now)
	}
	res, err := svc.Aggregate(ctx, Actor{UserID: "admin", TenantID: "t1"}, &FormStatsQuery{
		ProcessKey: "leave",
		Start:      now.Add(-24 * time.Hour),
		End:        now.Add(time.Hour),
		Metrics:    []FormStatsMetricRequest{{Path: "days", Agg: FormStatsAggSum}},
	})
	if err != nil {
		t.Fatalf("aggregate: %v", err)
	}
	if res.Total != rows || res.Metrics[0].Value != rows {
		t.Fatalf("pagination must cover all %d rows, got total %d sum %v", rows, res.Total, res.Metrics[0].Value)
	}
}

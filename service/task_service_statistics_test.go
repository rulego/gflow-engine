package service

import (
	"testing"
	"time"

	"github.com/rulego/gflow-engine/model"
)

// 纯逻辑测试：countOverdueFromTasks / deriveMonthStats 不依赖 DAO，
// 覆盖 GetApprovalStatistics 新增的 overdueCount / monthApproved / monthRejected / avgDurationMonth 语义。

func statStrPtr(s string) *string        { return &s }
func statIntPtr(i int64) *int64          { return &i }
func statTimePtr(t time.Time) *time.Time { return &t }

func TestCountOverdueFromTasks_Empty(t *testing.T) {
	if got := countOverdueFromTasks(nil, time.Now()); got != 0 {
		t.Errorf("expected 0, got %d", got)
	}
}

func TestCountOverdueFromTasks_NoDueDate(t *testing.T) {
	now := time.Date(2026, 6, 26, 12, 0, 0, 0, time.UTC)
	tasks := []*model.WfTask{
		{DueDate: nil},
		{DueDate: nil},
	}
	if got := countOverdueFromTasks(tasks, now); got != 0 {
		t.Errorf("expected 0 when no dueDate set, got %d", got)
	}
}

func TestCountOverdueFromTasks_Mixed(t *testing.T) {
	now := time.Date(2026, 6, 26, 12, 0, 0, 0, time.UTC)
	tasks := []*model.WfTask{
		{DueDate: statTimePtr(now.Add(-1 * time.Hour))}, // 已过期
		{DueDate: statTimePtr(now.Add(1 * time.Hour))},  // 未到期
		{DueDate: nil}, // 无到期时间
		{DueDate: statTimePtr(now.Add(-2 * time.Hour))}, // 已过期
	}
	if got := countOverdueFromTasks(tasks, now); got != 2 {
		t.Errorf("expected 2 overdue, got %d", got)
	}
}

func TestDeriveMonthStats_Empty(t *testing.T) {
	approved, rejected, total, avg := deriveMonthStats(nil)
	if approved != 0 || rejected != 0 || total != 0 || avg != 0 {
		t.Errorf("expected all zero, got approved=%d rejected=%d total=%d avg=%d", approved, rejected, total, avg)
	}
}

func TestDeriveMonthStats_Mixed(t *testing.T) {
	tasks := []*model.WfTask{
		{EndReason: statStrPtr("approved"), Duration: statIntPtr(1000)},
		{EndReason: statStrPtr("approved"), Duration: statIntPtr(3000)},
		{EndReason: statStrPtr("rejected"), Duration: statIntPtr(2000)},
		{EndReason: statStrPtr("withdrawn"), Duration: statIntPtr(4000)}, // 非 approve/reject，计入 total 但不计通过/拒绝
		{EndReason: statStrPtr("approved"), Duration: nil},               // 无 duration，不参与均值
	}
	approved, rejected, total, avg := deriveMonthStats(tasks)
	if approved != 3 {
		t.Errorf("expected 3 approved, got %d", approved)
	}
	if rejected != 1 {
		t.Errorf("expected 1 rejected, got %d", rejected)
	}
	if total != 5 {
		t.Errorf("expected total 5, got %d", total)
	}
	// 均值只算 duration>0 的 4 个：(1000+3000+2000+4000)/4 = 2500
	if avg != 2500 {
		t.Errorf("expected avg 2500, got %d", avg)
	}
}

func TestDeriveMonthStats_ApprovedPlusRejectedLeTotal(t *testing.T) {
	// withdrawn/cancelled 等既非 approved 也非 rejected，因此通过+拒绝 ≤ 总完成数。
	tasks := []*model.WfTask{
		{EndReason: statStrPtr("approved"), Duration: statIntPtr(100)},
		{EndReason: statStrPtr("rejected"), Duration: statIntPtr(200)},
		{EndReason: statStrPtr("withdrawn"), Duration: statIntPtr(300)},
		{EndReason: nil, Duration: statIntPtr(400)},
	}
	approved, rejected, total, _ := deriveMonthStats(tasks)
	if approved+rejected > total {
		t.Errorf("approved+rejected (%d) must not exceed total (%d)", approved+rejected, total)
	}
	if approved+rejected != 2 || total != 4 {
		t.Errorf("expected approved+rejected=2 total=4, got ar=%d total=%d", approved+rejected, total)
	}
}

func TestClampSub(t *testing.T) {
	cases := []struct{ a, b, want int64 }{
		{10, 3, 7},
		{3, 10, 0},
		{5, 5, 0},
		{0, 0, 0},
	}
	for _, c := range cases {
		if got := clampSub(c.a, c.b); got != c.want {
			t.Errorf("clampSub(%d,%d)=%d want %d", c.a, c.b, got, c.want)
		}
	}
}

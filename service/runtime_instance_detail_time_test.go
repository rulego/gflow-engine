package service

import (
	"testing"
	"time"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/utils"
)

// formatTransferTime 对无区布局文本必须按本地时区锚定：time.Parse 默认得 UTC，
// 规整成 RFC3339 后浏览器按偏移换算会整体偏移一个时区。断言时刻等值（与运行
// 机器的本地时区无关）。
func TestFormatTransferTimeAnchorsLocalWall(t *testing.T) {
	got := formatTransferTime("2026-11-13 10:49:26")
	parsed, err := time.Parse(time.RFC3339, got)
	if err != nil {
		t.Fatalf("规整结果应为合法 RFC3339：%v（got %s）", err, got)
	}
	want := time.Date(2026, 11, 13, 10, 49, 26, 0, time.Local)
	if !parsed.Equal(want) {
		t.Fatalf("本地墙钟被时区漂移：got %s want %s", got, want.Format(time.RFC3339))
	}
}

// 旧数据是带偏移的 RFC3339 文本：规整按原时刻透传，不得按本地重锚。
func TestFormatTransferTimeKeepsLegacyOffset(t *testing.T) {
	got := formatTransferTime("2026-11-13T02:49:26Z")
	parsed, err := time.Parse(time.RFC3339, got)
	if err != nil {
		t.Fatalf("规整结果应为合法 RFC3339：%v（got %s）", err, got)
	}
	want := time.Date(2026, 11, 13, 2, 49, 26, 0, time.UTC)
	if !parsed.Equal(want) {
		t.Fatalf("旧 RFC3339 数据时刻被改动：got %s want %s", got, want.Format(time.RFC3339))
	}
}

// 无法识别的文本原样透出，不得吞字段。
func TestFormatTransferTimePassesUnknownThrough(t *testing.T) {
	if got := formatTransferTime("not-a-time"); got != "not-a-time" {
		t.Fatalf("未知文本应原样返回：got %s", got)
	}
}

// 时间线 endedAt/claimedAt 必须以 RFC3339 带偏移透出。
func TestTask2ExecutionInfoEmitsRFC3339(t *testing.T) {
	cst := time.FixedZone("CST", 8*3600)
	ended := time.Date(2026, 11, 13, 10, 49, 50, 0, cst)
	assignee := "u1"
	task := &model.WfTask{
		ID:        "t1",
		TaskType:  "userTask",
		Status:    "completed",
		Assignee:  &assignee,
		ClaimedAt: &ended,
		EndedAt:   &ended,
	}
	info := Task2ExecutionInfo(task, nil)
	if got, want := info.EndedAt, "2026-11-13T10:49:50+08:00"; got != want {
		t.Fatalf("endedAt 应为 RFC3339 带偏移：got %s want %s", got, want)
	}
	if got, want := info.ClaimedAt, "2026-11-13T10:49:50+08:00"; got != want {
		t.Fatalf("claimedAt 应为 RFC3339 带偏移：got %s want %s", got, want)
	}
	if info.EndedAt != utils.FormatTime(ended) {
		t.Fatalf("endedAt 与 utils.FormatTime 口径不一致：got %s", info.EndedAt)
	}
}

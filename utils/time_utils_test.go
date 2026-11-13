package utils

import (
	"testing"
	"time"
)

// 对外 DTO 时间必须带时区偏移：偏移随值透出后，前端按偏移换算本地时间，
// 服务器进程时区不再影响展示（审批时间 ±8 小时的根因是无区裸字符串）。
func TestFormatTime(t *testing.T) {
	cst := time.FixedZone("CST", 8*3600)
	if got, want := FormatTime(time.Date(2026, 11, 13, 10, 49, 50, 0, cst)), "2026-11-13T10:49:50+08:00"; got != want {
		t.Errorf("FormatTime = %q, want %q", got, want)
	}
	if got, want := FormatTime(time.Date(2026, 11, 13, 2, 49, 50, 0, time.UTC)), "2026-11-13T02:49:50Z"; got != want {
		t.Errorf("FormatTime UTC = %q, want %q", got, want)
	}
}

func TestFormatTimePtr(t *testing.T) {
	if got := FormatTimePtr(nil); got != nil {
		t.Errorf("FormatTimePtr(nil) = %v, want nil", *got)
	}
	cst := time.FixedZone("CST", 8*3600)
	tm := time.Date(2026, 11, 13, 10, 49, 50, 0, cst)
	if got := FormatTimePtr(&tm); got == nil || *got != FormatTime(tm) {
		t.Errorf("FormatTimePtr mismatch: %v", got)
	}
}

package utils

import (
	"time"
)

// FormatTime 引擎对外 DTO 的统一时间格式：RFC3339，偏移随值透出
// （如 2026-01-02T15:04:05+08:00），前端/调用方按偏移换算查看者本地时间，
// 服务器进程时区不再影响展示。落库与任务变量的存储布局仍为
// constants.TimeFormatLayout（进程本地墙钟、不带时区），两者不可混用。
func FormatTime(tm time.Time) string {
	return tm.Format(time.RFC3339)
}

// FormatTimePtr 格式化时间指针；nil 返回 nil（用于可空时间字段的 DTO 装配）。
func FormatTimePtr(tm *time.Time) *string {
	if tm == nil {
		return nil
	}
	formatted := FormatTime(*tm)
	return &formatted
}

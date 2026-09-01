package utils

import (
	"time"

	"github.com/rulego/gflow-engine/types/constants"
)

// FormatTime 按引擎统一的时间格式化（constants.TimeFormatLayout）。
func FormatTime(tm time.Time) string {
	return tm.Format(constants.TimeFormatLayout)
}

// FormatTimePtr 格式化时间指针；nil 返回 nil（用于可空时间字段的 DTO 装配）。
func FormatTimePtr(tm *time.Time) *string {
	if tm == nil {
		return nil
	}
	formatted := FormatTime(*tm)
	return &formatted
}

package utils

import (
	"testing"
	"time"

	"github.com/rulego/gflow-engine/types/constants"
)

func TestFormatTime(t *testing.T) {
	tm := time.Date(2026, 8, 30, 10, 20, 30, 0, time.UTC)
	if got, want := FormatTime(tm), tm.Format(constants.TimeFormatLayout); got != want {
		t.Errorf("FormatTime = %q, want %q", got, want)
	}
}

func TestFormatTimePtr(t *testing.T) {
	if got := FormatTimePtr(nil); got != nil {
		t.Errorf("FormatTimePtr(nil) = %v, want nil", *got)
	}
	tm := time.Date(2026, 8, 30, 10, 20, 30, 0, time.UTC)
	if got := FormatTimePtr(&tm); got == nil || *got != FormatTime(tm) {
		t.Errorf("FormatTimePtr mismatch: %v", got)
	}
}

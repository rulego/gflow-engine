package service

import (
	"testing"
)

func TestTaskEventTypeConstants(t *testing.T) {
	// 验证事件类型常量值正确
	tests := []struct {
		name     string
		got      TaskEventType
		expected string
	}{
		{"assigned", TaskEventAssigned, "assigned"},
		{"forwarded", TaskEventForwarded, "forwarded"},
		{"rejected", TaskEventRejected, "rejected"},
		{"terminated", TaskEventTerminated, "terminated"},
		{"completed", TaskEventCompleted, "completed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if string(tt.got) != tt.expected {
				t.Errorf("TaskEventType %s = %q, want %q", tt.name, tt.got, tt.expected)
			}
		})
	}
}

func TestTaskEventStruct(t *testing.T) {
	evt := TaskEvent{
		Type:       TaskEventAssigned,
		TaskID:     "t1",
		InstanceID: "inst1",
		ToUsers:    []string{"user1", "user2"},
	}
	if evt.Type != TaskEventAssigned {
		t.Errorf("Type = %q, want assigned", evt.Type)
	}
	if len(evt.ToUsers) != 2 {
		t.Errorf("ToUsers len = %d, want 2", len(evt.ToUsers))
	}
}

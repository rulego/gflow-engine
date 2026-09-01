package components

import (
	"testing"

	"github.com/rulego/rulego/api/types"
)

// Tests for start_node.go

func TestStartTaskNode_Type(t *testing.T) {
	node := &StartTaskNode{}
	if node.Type() != StartTaskNodeType {
		t.Errorf("Type = %q, want %q", node.Type(), StartTaskNodeType)
	}
}

func TestStartTaskNode_New(t *testing.T) {
	node := &StartTaskNode{}
	newNode := node.New()
	if newNode == nil {
		t.Fatal("expected non-nil node")
	}
	if newNode.Type() != StartTaskNodeType {
		t.Errorf("Type = %q, want %q", newNode.Type(), StartTaskNodeType)
	}
}

func TestStartTaskNode_Init_EmptyConfig(t *testing.T) {
	node := &StartTaskNode{}
	err := node.Init(types.Config{}, nil)
	if err != nil {
		t.Errorf("Init with empty config should not error: %v", err)
	}
}

func TestStartTaskNode_Destroy(t *testing.T) {
	node := &StartTaskNode{}
	// Should not panic
	node.Destroy()
}

func TestStartTaskNodeType_Constant(t *testing.T) {
	if StartTaskNodeType != "startTask" {
		t.Errorf("StartTaskNodeType = %q, want 'startTask'", StartTaskNodeType)
	}
}

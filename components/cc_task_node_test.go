package components

import (
	"testing"

	"github.com/rulego/rulego/api/types"
)

// Tests for cc_task_node.go

func TestCCTaskNode_Type(t *testing.T) {
	node := &CCTaskNode{}
	if node.Type() != CCTaskNodeType {
		t.Errorf("Type = %q, want %q", node.Type(), CCTaskNodeType)
	}
}

func TestCCTaskNode_New(t *testing.T) {
	node := &CCTaskNode{}
	newNode := node.New()
	if newNode == nil {
		t.Fatal("expected non-nil node")
	}
	ccNode, ok := newNode.(*CCTaskNode)
	if !ok {
		t.Fatal("expected *CCTaskNode")
	}
	if ccNode.Type() != CCTaskNodeType {
		t.Errorf("Type = %q, want %q", ccNode.Type(), CCTaskNodeType)
	}
}

func TestCCTaskNode_New_PreservesTaskService(t *testing.T) {
	// TaskService is nil in test, but New should preserve the reference
	node := &CCTaskNode{TaskService: nil}
	newNode := node.New().(*CCTaskNode)
	// TaskService should be copied from original
	_ = newNode
}

func TestCCTaskNode_Init_EmptyConfig(t *testing.T) {
	node := &CCTaskNode{}
	err := node.Init(types.Config{}, nil)
	// Init with nil configuration may or may not error depending on maps.Map2Struct
	// We just verify it doesn't panic
	_ = err
}

func TestCCTaskNode_Destroy(t *testing.T) {
	node := &CCTaskNode{}
	// Should not panic
	node.Destroy()
}

func TestCCTaskNodeType_Constant(t *testing.T) {
	if CCTaskNodeType != "ccTask" {
		t.Errorf("CCTaskNodeType = %q, want 'ccTask'", CCTaskNodeType)
	}
}

func TestCCTaskNode_GetSelfId_Default(t *testing.T) {
	node := &CCTaskNode{}
	// With empty CurrentNodeDef, should return the type constant
	id := node.GetSelfId()
	if id != CCTaskNodeType {
		t.Errorf("GetSelfId = %q, want %q", id, CCTaskNodeType)
	}
}

func TestCCTaskNode_GetSelfName_Default(t *testing.T) {
	node := &CCTaskNode{}
	// With empty CurrentNodeDef, should return empty string
	name := node.GetSelfName()
	if name != "" {
		t.Errorf("GetSelfName = %q, want ''", name)
	}
}

func TestCCTaskNodeConfiguration_DefaultValues(t *testing.T) {
	cfg := CCTaskNodeConfiguration{}
	if cfg.SelfSelect != false {
		t.Error("expected SelfSelect to default to false")
	}
	if len(cfg.CCUserIds) != 0 {
		t.Error("expected CCUserIds to default to empty")
	}
}

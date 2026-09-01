package dao

import (
	"context"
	"testing"

	"github.com/rulego/gflow-engine/model"
)

// Tests for process_dao.go parameter validation.

func TestProcessDAO_Create_NilEntity(t *testing.T) {
	d := &ProcessDAO{}
	err := d.Create(context.Background(), nil)
	if err == nil {
		t.Error("expected error for nil entity")
	}
}

func TestProcessDAO_CreateBatch_EmptyList(t *testing.T) {
	d := &ProcessDAO{}
	err := d.CreateBatch(context.Background(), nil)
	if err != nil {
		t.Errorf("unexpected error for nil list: %v", err)
	}
}

func TestProcessDAO_CreateBatch_EmptySlice(t *testing.T) {
	d := &ProcessDAO{}
	err := d.CreateBatch(context.Background(), []*model.WfProcess{})
	if err != nil {
		t.Errorf("unexpected error for empty slice: %v", err)
	}
}

func TestProcessDAO_Get_EmptyID(t *testing.T) {
	d := &ProcessDAO{}
	_, err := d.Get(context.Background(), "")
	if err == nil {
		t.Error("expected error for empty ID")
	}
}

func TestProcessDAO_Get_NilQuery_Panics(t *testing.T) {
	d := &ProcessDAO{}
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic from nil Query")
		}
	}()
	d.Get(context.Background(), "proc-1")
}

func TestProcessDAO_GetLatestByKey_EmptyKey(t *testing.T) {
	d := &ProcessDAO{}
	_, err := d.GetLatestByKey(context.Background(), "tenant1", "")
	if err == nil {
		t.Error("expected error for empty key")
	}
}

func TestProcessDAO_GetLatestByKey_NilQuery_Panics(t *testing.T) {
	d := &ProcessDAO{}
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic from nil Query")
		}
	}()
	d.GetLatestByKey(context.Background(), "tenant1", "key1")
}

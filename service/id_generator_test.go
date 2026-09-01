package service

import (
	"strings"
	"testing"
)

func TestNewIDGenerator(t *testing.T) {
	gen := NewIDGenerator()
	if gen == nil {
		t.Fatal("expected non-nil generator")
	}
}

func TestNewIDGenerator_EmptyPrefix(t *testing.T) {
	gen := NewIDGenerator()
	if gen == nil {
		t.Fatal("expected non-nil generator with empty prefix")
	}
}

func TestGenerateID(t *testing.T) {
	gen := NewIDGenerator()
	id := gen.GenerateID()
	if id == "" {
		t.Error("GenerateID returned empty string")
	}
	if len(id) != 36 { // UUID format: 8-4-4-4-12
		t.Errorf("GenerateID = %q, expected UUID length 36, got %d", id, len(id))
	}
}

func TestGenerateID_Uniqueness(t *testing.T) {
	gen := NewIDGenerator()
	ids := map[string]bool{}
	for i := 0; i < 100; i++ {
		id := gen.GenerateID()
		if ids[id] {
			t.Errorf("duplicate ID generated: %q", id)
		}
		ids[id] = true
	}
}

func TestGenerateInstanceID(t *testing.T) {
	gen := NewIDGenerator()
	id := gen.GenerateInstanceID()
	if id == "" {
		t.Error("GenerateInstanceID returned empty")
	}
	if len(id) != 36 {
		t.Errorf("GenerateInstanceID = %q, length %d", id, len(id))
	}
}

func TestGenerateTaskID(t *testing.T) {
	gen := NewIDGenerator()
	id := gen.GenerateTaskID()
	if id == "" {
		t.Error("GenerateTaskID returned empty")
	}
}

func TestGenerateProcessID(t *testing.T) {
	gen := NewIDGenerator()
	id := gen.GenerateProcessID()
	if id == "" {
		t.Error("GenerateProcessID returned empty")
	}
}

func TestGenerateBusinessKey(t *testing.T) {
	gen := NewIDGenerator()
	key := gen.GenerateBusinessKey()
	if !strings.HasPrefix(key, "BIZ_") {
		t.Errorf("GenerateBusinessKey = %q, expected BIZ_ prefix", key)
	}
	uuid := strings.TrimPrefix(key, "BIZ_")
	if len(uuid) != 36 {
		t.Errorf("business key UUID part = %q, expected length 36", uuid)
	}
}

func TestAllGenerateMethodsAreUnique(t *testing.T) {
	gen := NewIDGenerator()
	ids := []string{
		gen.GenerateID(),
		gen.GenerateInstanceID(),
		gen.GenerateTaskID(),
		gen.GenerateProcessID(),
		gen.GenerateBusinessKey(),
	}
	seen := map[string]bool{}
	for _, id := range ids {
		if seen[id] {
			t.Errorf("duplicate across methods: %q", id)
		}
		seen[id] = true
	}
}

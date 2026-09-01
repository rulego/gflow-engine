package service

import (
	"context"
	"testing"

	"github.com/rulego/gflow-engine/types/constants"
)

// ──────────────────────────────────────────────
// DialectRegistry
// ──────────────────────────────────────────────

func TestGetGlobalRegistry(t *testing.T) {
	r1 := GetGlobalRegistry()
	r2 := GetGlobalRegistry()
	if r1 != r2 {
		t.Error("GetGlobalRegistry should return the same instance")
	}
}

func TestDialectRegistry_RegisterAndGet(t *testing.T) {
	r := &DialectRegistry{providers: make(map[string]DialectProvider)}

	provider := &PostgresDialectProvider{}
	err := r.RegisterDialectProvider(provider)
	if err != nil {
		t.Fatalf("RegisterDialectProvider failed: %v", err)
	}

	got, ok := r.GetDialectProvider("postgres")
	if !ok {
		t.Error("GetDialectProvider should find postgres")
	}
	if got.GetName() != "postgres" {
		t.Errorf("GetName = %q", got.GetName())
	}
}

func TestDialectRegistry_RegisterNil(t *testing.T) {
	r := &DialectRegistry{}
	err := r.RegisterDialectProvider(nil)
	if err == nil {
		t.Error("nil provider should return error")
	}
}

func TestDialectRegistry_DuplicateName(t *testing.T) {
	r := &DialectRegistry{providers: make(map[string]DialectProvider)}
	p1 := &PostgresDialectProvider{}
	p2 := &PostgresDialectProvider{}
	_ = r.RegisterDialectProvider(p1)
	err := r.RegisterDialectProvider(p2)
	if err == nil {
		t.Error("duplicate name should return error")
	}
}

func TestDialectRegistry_GetNotFound(t *testing.T) {
	r := &DialectRegistry{}
	_, ok := r.GetDialectProvider("nonexistent")
	if ok {
		t.Error("unregistered name should return false")
	}
}

func TestDialectRegistry_GetRegisteredProviders(t *testing.T) {
	r := &DialectRegistry{providers: make(map[string]DialectProvider)}
	_ = r.RegisterDialectProvider(&PostgresDialectProvider{})
	_ = r.RegisterDialectProvider(&MySQLDialectProvider{})

	names := r.GetRegisteredProviders()
	if len(names) != 2 {
		t.Errorf("expected 2 providers, got %d", len(names))
	}
}

func TestDialectRegistry_GetSupportedDrivers(t *testing.T) {
	r := &DialectRegistry{providers: make(map[string]DialectProvider)}
	_ = r.RegisterDialectProvider(&PostgresDialectProvider{})
	_ = r.RegisterDialectProvider(&MySQLDialectProvider{})

	drivers := r.GetSupportedDrivers()
	if len(drivers) < 3 { // postgres, postgresql, mysql
		t.Errorf("expected at least 3 drivers, got %d: %v", len(drivers), drivers)
	}
}

// ──────────────────────────────────────────────
// Dialect providers
// ──────────────────────────────────────────────

func TestPostgresDialectProvider(t *testing.T) {
	p := &PostgresDialectProvider{}
	if p.GetName() != "postgres" {
		t.Errorf("GetName = %q", p.GetName())
	}
	drivers := p.GetSupportedDrivers()
	if len(drivers) != 2 {
		t.Errorf("GetSupportedDrivers = %v", drivers)
	}
}

func TestMySQLDialectProvider(t *testing.T) {
	m := &MySQLDialectProvider{}
	if m.GetName() != "mysql" {
		t.Errorf("GetName = %q", m.GetName())
	}
	drivers := m.GetSupportedDrivers()
	if len(drivers) != 1 {
		t.Errorf("GetSupportedDrivers = %v", drivers)
	}
}

// ──────────────────────────────────────────────
// Package-level functions
// ──────────────────────────────────────────────

func TestGlobalRegistryHasDefaultProviders(t *testing.T) {
	r := GetGlobalRegistry()
	p, ok := r.GetDialectProvider("postgres")
	if !ok {
		t.Error("postgres should be registered in global registry")
	}
	if p.GetName() != "postgres" {
		t.Errorf("GetName = %q", p.GetName())
	}
}

// ──────────────────────────────────────────────
// Actor context helpers
// ──────────────────────────────────────────────

func TestSetAndGetUserFromCtx(t *testing.T) {
	ctx := context.Background()

	if u := GetUserFromCtx(ctx); u != nil {
		t.Error("expected nil before setting")
	}

	user := &Actor{UserID: "user1", UserName: "Test", TenantID: "t1"}
	ctx = SetUserToCtx(ctx, user)

	got := GetUserFromCtx(ctx)
	if got == nil {
		t.Fatal("expected non-nil after setting")
	}
	if got.UserID != "user1" {
		t.Errorf("UserId = %q", got.UserID)
	}
	if got.UserName != "Test" {
		t.Errorf("UserName = %q", got.UserName)
	}
	if got.TenantID != "t1" {
		t.Errorf("TenantId = %q", got.TenantID)
	}
}

func TestSetUserToCtx_Nil(t *testing.T) {
	ctx := context.Background()
	ctx = SetUserToCtx(ctx, nil)
	if u := GetUserFromCtx(ctx); u != nil {
		t.Error("setting nil should produce nil on get")
	}
}

func TestGetUserFromCtx_WrongType(t *testing.T) {
	ctx := context.Background()
	ctx = context.WithValue(ctx, constants.KeyCurrentUser, "not-an-identity")
	if u := GetUserFromCtx(ctx); u != nil {
		t.Error("wrong type should return nil")
	}
}

package components

import (
	"testing"

	"github.com/rulego/rulego/api/types"
)

// noOpFn 测试用函数实现，符合 action.Functions.Register 的签名
func noOpFn(ctx types.RuleContext, msg types.RuleMsg) {}

func newTestRegistry() *ServiceRegistry {
	return &ServiceRegistry{visibility: AllowAllVisibility{}}
}

func TestServiceRegistry_RegisterAndGet(t *testing.T) {
	r := newTestRegistry()
	r.Register(ServiceFuncDef{Name: "a", Label: "A", Fields: []ServiceFuncField{{Name: "p1"}}}, noOpFn)

	def, ok := r.Get("a")
	if !ok {
		t.Fatal("expected to find registered function 'a'")
	}
	if def.Label != "A" {
		t.Errorf("Label = %q, want %q", def.Label, "A")
	}

	if _, ok := r.Get("not-exist"); ok {
		t.Fatal("expected miss for unregistered function")
	}
}

func TestServiceRegistry_List_KeepsOrder(t *testing.T) {
	r := newTestRegistry()
	r.Register(ServiceFuncDef{Name: "b"}, noOpFn)
	r.Register(ServiceFuncDef{Name: "a"}, noOpFn)
	r.Register(ServiceFuncDef{Name: "c"}, noOpFn)

	got := r.List()
	if len(got) != 3 {
		t.Fatalf("List len = %d, want 3", len(got))
	}
	// 保持注册顺序，非字母序
	wantOrder := []string{"b", "a", "c"}
	for i, w := range wantOrder {
		if got[i].Name != w {
			t.Errorf("List[%d].Name = %q, want %q", i, got[i].Name, w)
		}
	}
}

func TestServiceRegistry_Register_OverwritesExisting(t *testing.T) {
	r := newTestRegistry()
	r.Register(ServiceFuncDef{Name: "a", Label: "old"}, noOpFn)
	r.Register(ServiceFuncDef{Name: "a", Label: "new"}, noOpFn)

	got := r.List()
	if len(got) != 1 {
		t.Fatalf("after overwrite, List len = %d, want 1", len(got))
	}
	if got[0].Label != "new" {
		t.Errorf("Label = %q, want %q (should be overwritten)", got[0].Label, "new")
	}
}

// 默认 AllowAll：任意租户可见全部函数。
func TestServiceRegistry_ListForTenant_AllVisibleByDefault(t *testing.T) {
	r := newTestRegistry()
	r.Register(ServiceFuncDef{Name: "a"}, noOpFn)
	r.Register(ServiceFuncDef{Name: "b"}, noOpFn)

	for _, tenant := range []string{"t1", "t2", ""} {
		got := r.ListForTenant(tenant)
		if len(got) != 2 {
			t.Errorf("tenant %q: ListForTenant len = %d, want 2", tenant, len(got))
		}
	}
}

// 未注入 provider 时，按 def.Visible 白名单过滤。
func TestServiceRegistry_ListForTenant_VisibleWhitelistFallback(t *testing.T) {
	r := newTestRegistry()
	r.Register(ServiceFuncDef{Name: "public"}, noOpFn)
	r.Register(ServiceFuncDef{Name: "private", Visible: []string{"t1"}}, noOpFn)

	if t1 := r.ListForTenant("t1"); len(t1) != 2 {
		t.Errorf("t1: len = %d, want 2", len(t1))
	}
	if t2 := r.ListForTenant("t2"); len(t2) != 1 || t2[0].Name != "public" {
		t.Errorf("t2: got %+v, want only [public]", t2)
	}
}

// 注入 provider 后以其为准，忽略 def.Visible。
func TestServiceRegistry_ListForTenant_WithProvider(t *testing.T) {
	provider := allowSet{"a": true}
	r := &ServiceRegistry{visibility: provider}
	r.Register(ServiceFuncDef{Name: "a", Visible: []string{"t1"}}, noOpFn)
	r.Register(ServiceFuncDef{Name: "b"}, noOpFn)

	got := r.ListForTenant("any-tenant")
	if len(got) != 1 || got[0].Name != "a" {
		t.Errorf("with provider: got %+v, want only [a]", names(got))
	}
}

// SetVisibilityProvider(nil) 应回退到 AllowAll。
func TestServiceRegistry_SetVisibilityProvider_NilResetsDefault(t *testing.T) {
	r := &ServiceRegistry{visibility: allowSet{}}
	r.SetVisibilityProvider(nil)

	r.Register(ServiceFuncDef{Name: "a"}, noOpFn)
	if len(r.ListForTenant("any")) != 1 {
		t.Error("after SetVisibilityProvider(nil), expected all visible again")
	}
}

// allowSet 测试用 provider：仅 map 中 true 的函数可见。
type allowSet map[string]bool

func (s allowSet) Visible(_ string, funcName string) bool { return s[funcName] }

func names(defs []ServiceFuncDef) []string {
	out := make([]string, 0, len(defs))
	for _, d := range defs {
		out = append(out, d.Name)
	}
	return out
}

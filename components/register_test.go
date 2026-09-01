package components

import (
	"testing"

	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/components/action"
	"github.com/stretchr/testify/require"
)

// RegisterServiceFuncs 批量注册：元数据进 catalog、实现进 action.Functions，
// nil 实现跳过。不经过 Register 全量引导，避免测试抢占节点原型绑定。
func TestRegisterServiceFuncs(t *testing.T) {
	const fnName = "test:register:servicefuncs"
	defer Services.Unregister(fnName)

	called := false
	RegisterServiceFuncs([]ServiceFunc{
		{
			Def: ServiceFuncDef{Name: fnName, Label: "批量注册"},
			Fn: func(ctx types.RuleContext, msg types.RuleMsg) {
				called = true
				ctx.TellSuccess(msg)
			},
		},
		// nil 实现：元数据先行、实现后补的场景，跳过不报错
		{Def: ServiceFuncDef{Name: "test:register:nilfn"}, Fn: nil},
	})

	def, ok := Services.Get(fnName)
	require.True(t, ok, "metadata must land in catalog")
	require.Equal(t, "批量注册", def.Label)

	_, ok = Services.Get("test:register:nilfn")
	require.False(t, ok, "nil-Fn entry must be skipped")

	fn, ok := action.Functions.Get(fnName)
	require.True(t, ok, "implementation must be reachable by runtime lookup")
	require.NotNil(t, fn)
	require.False(t, called, "registration must not invoke the function")
}

// Unregister 同步清除 catalog 元数据与 action.Functions 运行时实现；注销不存在的名称是 no-op。
func TestServiceRegistry_Unregister(t *testing.T) {
	r := newTestRegistry()
	r.Register(ServiceFuncDef{Name: "x"}, noOpFn)
	r.Register(ServiceFuncDef{Name: "y"}, noOpFn)

	r.Unregister("x")

	_, ok := r.Get("x")
	require.False(t, ok, "unregistered def must be gone from catalog")
	_, ok = action.Functions.Get("x")
	require.False(t, ok, "unregistered impl must be gone from runtime registry")

	got := r.List()
	require.Len(t, got, 1, "order slice must be compacted")
	require.Equal(t, "y", got[0].Name)

	// 注销不存在的名称：静默 no-op
	r.Unregister("not-exist")
	require.Len(t, r.List(), 1)
}

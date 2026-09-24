package components

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// emptyApproverPolicy 取值合法性

func TestIsValidEmptyApproverPolicy(t *testing.T) {
	require.True(t, isValidEmptyApproverPolicy(""))
	require.True(t, isValidEmptyApproverPolicy(EmptyApproverPolicyTenantAdmin))
	require.True(t, isValidEmptyApproverPolicy(EmptyApproverPolicyAutoApprove))
	require.True(t, isValidEmptyApproverPolicy(EmptyApproverPolicyPark))
	require.False(t, isValidEmptyApproverPolicy("autoApprove"))
	require.False(t, isValidEmptyApproverPolicy("admin"))
}

// Normalize 缺省落定

func TestNormalizeEmptyApproverPolicyDefault(t *testing.T) {
	c := UserTaskNodeConfiguration{}
	c.Normalize()
	require.Equal(t, EmptyApproverPolicyTenantAdmin, c.EmptyApproverPolicy)

	c = UserTaskNodeConfiguration{EmptyApproverPolicy: " " + EmptyApproverPolicyPark + " "}
	c.Normalize()
	require.Equal(t, EmptyApproverPolicyPark, c.EmptyApproverPolicy)

	c = UserTaskNodeConfiguration{EmptyApproverPolicy: EmptyApproverPolicyAutoApprove}
	c.Normalize()
	require.Equal(t, EmptyApproverPolicyAutoApprove, c.EmptyApproverPolicy)
}

// Validate 拒绝未知取值

func TestValidateEmptyApproverPolicy(t *testing.T) {
	valid := UserTaskNodeConfiguration{
		Approver:            ApproverConfig{Type: "role", RoleIds: []string{"r1"}},
		ApproveMode:         "single",
		EmptyApproverPolicy: EmptyApproverPolicyPark,
	}
	valid.Normalize()
	require.Empty(t, valid.Validate())

	invalid := valid
	invalid.EmptyApproverPolicy = "nobody"
	issues := invalid.Validate()
	require.Len(t, issues, 1)
	require.Contains(t, issues[0], "emptyApproverPolicy")
}

// approverSummary 摘要形态

func TestApproverSummary(t *testing.T) {
	require.Equal(t, "role:r1,r2", approverSummary(&ApproverConfig{Type: "role", RoleIds: []string{"r1", "r2"}}))
	require.Equal(t, "dept:d1", approverSummary(&ApproverConfig{Type: "dept", DeptIds: []string{"d1"}}))
	require.Equal(t, "user:u1", approverSummary(&ApproverConfig{Type: "user", UserIds: []string{"u1"}}))
	// 无 ID 的类型只报类型名
	require.Equal(t, "initiatorSelect", approverSummary(&ApproverConfig{Type: "initiatorSelect"}))
}

// excludeID 兜底审批人剔除发起人

func TestExcludeID(t *testing.T) {
	require.Equal(t, []string{"a", "b"}, excludeID([]string{"a", "b"}, ""))
	require.Equal(t, []string{"a", "b"}, excludeID([]string{"a", "b"}, "c"))
	require.Equal(t, []string{"b"}, excludeID([]string{"a", "b"}, "a"))
	require.Empty(t, excludeID([]string{"a"}, "a"))
	require.Nil(t, excludeID(nil, "a"))
}

// applyFallbackVars 兜底留痕写入任务变量

func TestApplyFallbackVars(t *testing.T) {
	vars := map[string]interface{}{"existing": 1}
	applyFallbackVars(vars, EmptyApproverPolicyPark, "role:r9", "审批人为空，任务挂起待指派")
	require.Equal(t, 1, vars["existing"])
	require.Equal(t, EmptyApproverPolicyPark, vars["fallback_policy"])
	require.Equal(t, "role:r9", vars["fallback_from"])
	require.Equal(t, "审批人为空，任务挂起待指派", vars["fallback_reason"])
	require.NotNil(t, vars["fallback_time"])

	// nil map 不 panic
	require.NotPanics(t, func() { applyFallbackVars(nil, "x", "y", "z") })
}

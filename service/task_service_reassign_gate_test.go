// 保留标记流转剥离与系统改投豁免开关的回归测试。
package service

import (
	"context"
	"strings"
	"testing"

	"github.com/rulego/gflow-engine/types/constants"
	"github.com/rulego/gflow-engine/types/enums"
	"github.com/stretchr/testify/require"
)

// 兜底标记不随流转下传，任务行留痕保留。
func TestFallback_MarkNotLeakedDownstream(t *testing.T) {
	q := proxyAuditDB(t)
	proxySeedInstance(t, q, "fb-inst-1")
	proxySeedTask(t, q, "t-fb1", "fb-inst-1", "a", "yi",
		`{"amount":100,"fallback_policy":"tenant_admin","fallback_from":"role:r1","fallback_reason":"审批人为空，已转交租户管理员","fallback_time":"2026-09-24 10:00:00"}`)

	svc, eng := newRecallSvc(q, proxyDefinition(true), nil)
	require.NoError(t, svc.CompleteWithApproval(context.Background(), Actor{UserID: "yi", TenantID: "t1"}, &ApprovalRequest{
		TaskID:         "t-fb1",
		ApprovalResult: enums.ApprovalResultApproved,
		Comment:        "同意",
	}))

	row, err := q.WfTask.WithContext(context.Background()).Where(q.WfTask.ID.Eq("t-fb1")).First()
	require.NoError(t, err)
	require.Equal(t, string(enums.TaskStatusCompleted), row.Status)
	require.True(t, strings.Contains(*row.Variables, constants.VarsFallbackReason), "任务行保留兜底留痕")

	require.NotNil(t, eng.internal.execNextVars)
	require.NotContains(t, eng.internal.execNextVars, constants.VarsFallbackPolicy)
	require.NotContains(t, eng.internal.execNextVars, constants.VarsFallbackFrom)
	require.NotContains(t, eng.internal.execNextVars, constants.VarsFallbackReason)
	require.NotContains(t, eng.internal.execNextVars, constants.VarsFallbackTime)
	require.Equal(t, float64(100), eng.internal.execNextVars["amount"], "业务变量照常下传")
}

// 内部模式 + 系统身份才豁免，真实用户不豁免。
func TestSystemInitiatedReassign(t *testing.T) {
	require.False(t, systemInitiatedReassign(context.Background()), "未标记调用模式不豁免")
	require.True(t, systemInitiatedReassign(WithInternalCallingMode(context.Background())), "内部模式无用户=系统改投")

	internalRealUser := SetUserToCtx(WithInternalCallingMode(context.Background()),
		&Actor{UserID: "admin", TenantID: "t1", WorkflowAdmin: true})
	require.False(t, systemInitiatedReassign(internalRealUser), "内部模式带真实用户=手动干预，不豁免")

	internalSystem := SetUserToCtx(WithInternalCallingMode(context.Background()),
		&Actor{UserID: constants.UserSystem, TenantID: "t1", WorkflowAdmin: true})
	require.True(t, systemInitiatedReassign(internalSystem))
}

// 设计器禁用 reassign 的流程定义（actionPermissions 挂节点 additionalInfo）。
func reassignDisabledDefinition() string {
	return `{"ruleChain":{"additionalInfo":{}},` +
		`"metadata":{"nodes":[{"id":"a","type":"userTask","additionalInfo":{"actionPermissions":{"reassign":false}}}],"connections":[]}}`
}

// 设计器关闭改派：手动改派被拒，系统自动改投不受限。
func TestReassign_SystemRedirectBypassesDesignerSwitch(t *testing.T) {
	q := proxyAuditDB(t)
	proxySeedInstance(t, q, "ra-inst-1")
	proxySeedTask(t, q, "t-ra1", "ra-inst-1", "a", "yi", `{}`)

	svc, _ := newRecallSvc(q, reassignDisabledDefinition(), nil)

	_, err := svc.Reassign(context.Background(), Actor{UserID: "admin", TenantID: "t1", WorkflowAdmin: true},
		"t-ra1", "jia", "管理员手动改派")
	require.Error(t, err, "设计器禁用改派时管理员手动改派应被拒")

	_, err = svc.Reassign(WithInternalCallingMode(context.Background()),
		Actor{UserID: constants.UserSystem, TenantID: "t1", WorkflowAdmin: true},
		"t-ra1", "jia", "离岗代审：张三 的代理规则生效")
	require.NoError(t, err, "系统级自动改投不受设计器开关约束")

	row, err := q.WfTask.WithContext(context.Background()).Where(q.WfTask.ID.Eq("t-ra1")).First()
	require.NoError(t, err)
	require.Equal(t, "jia", *row.Assignee)
	require.Equal(t, string(enums.TaskStatusActive), row.Status)
}

// 无开关配置时手动改派照常可用。
func TestReassign_ManualReassignStillWorksWithoutDesignerSwitch(t *testing.T) {
	q := proxyAuditDB(t)
	proxySeedInstance(t, q, "ra-inst-2")
	proxySeedTask(t, q, "t-ra2", "ra-inst-2", "a", "yi", `{}`)

	svc, _ := newRecallSvc(q, proxyDefinition(true), nil)
	_, err := svc.Reassign(context.Background(), Actor{UserID: "admin", TenantID: "t1", WorkflowAdmin: true},
		"t-ra2", "jia", "管理员手动改派")
	require.NoError(t, err)

	row, err := q.WfTask.WithContext(context.Background()).Where(q.WfTask.ID.Eq("t-ra2")).First()
	require.NoError(t, err)
	require.Equal(t, "jia", *row.Assignee)
}

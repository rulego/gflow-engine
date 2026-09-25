// 部署期直派审批人校验测试：DSL 形状抽取、能力探测跳过、幽灵/停用拦截、
// create() 部署入口的拦截与放行（nil DAO panic = 守卫放行后走到 DAO）。
package service

import (
	"context"
	"strings"
	"testing"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/types/enums"
	"github.com/rulego/rulego/api/types"
	"github.com/stretchr/testify/require"
)

// fakeActiveChecker 只实现 ActiveUserChecker 的身份服务桩。
type fakeActiveChecker struct {
	IdentityService
	active map[string]bool
	err    error
}

func (f *fakeActiveChecker) AreActiveUsers(_ context.Context, _ string, userIDs []string) (map[string]bool, error) {
	if f.err != nil {
		return nil, f.err
	}
	out := make(map[string]bool, len(userIDs))
	for _, id := range userIDs {
		out[id] = f.active[id]
	}
	return out, nil
}

// fakeEngineIdentity 只暴露 GetIdentityService 的引擎桩。
type fakeEngineIdentity struct {
	WorkflowEngine
	identity IdentityService
}

func (f *fakeEngineIdentity) GetIdentityService() IdentityService { return f.identity }

func directAssigneeDefinition(userIDs ...string) string {
	quoted := make([]string, len(userIDs))
	for i, u := range userIDs {
		quoted[i] = `"` + u + `"`
	}
	return `{"ruleChain":{"additionalInfo":{}},` +
		`"metadata":{"nodes":[{"id":"a","type":"userTask","configuration":{"approver":{"type":"user","userIds":[` +
		strings.Join(quoted, ",") + `]}}}],"connections":[]}}`
}

func TestDeployDirectAssignees_Extraction(t *testing.T) {
	chain := &types.RuleChain{Metadata: types.RuleMetadata{Nodes: []*types.RuleNode{
		{Id: "direct", Type: "userTask", Configuration: types.Configuration{
			"approver": map[string]interface{}{"type": "user", "userIds": []interface{}{"u1", "u2"}},
		}},
		// role 直派不校验（候选池运行期展开，有停用过滤）
		{Id: "role", Type: "userTask", Configuration: types.Configuration{
			"approver": map[string]interface{}{"type": "role", "userIds": []interface{}{"r1"}},
		}},
		// 非 userTask 节点忽略
		{Id: "sw", Type: "switch"},
	}}}
	got := deployDirectAssignees(chain)
	require.Equal(t, map[string][]string{"direct": {"u1", "u2"}}, got)
}

func TestValidateDirectAssignees(t *testing.T) {
	ctx := context.Background()
	chain := &types.RuleChain{Metadata: types.RuleMetadata{Nodes: []*types.RuleNode{
		{Id: "a", Type: "userTask", Configuration: types.Configuration{
			"approver": map[string]interface{}{"type": "user", "userIds": []interface{}{"u1", "ghost"}},
		}},
	}}}

	// 幽灵/停用直派拒绝，错误定位节点与 userId
	err := ValidateDirectAssignees(ctx, &fakeActiveChecker{active: map[string]bool{"u1": true}}, "t1", chain)
	require.ErrorIs(t, err, ErrValidation)
	require.Contains(t, err.Error(), "ghost")
	require.Contains(t, err.Error(), "node a")

	// 全部存在且启用：放行
	require.NoError(t, ValidateDirectAssignees(ctx, &fakeActiveChecker{active: map[string]bool{"u1": true, "ghost": true}}, "t1", chain))

	// 身份服务查询失败：降级放行（部署不被身份服务抖动卡死）
	require.NoError(t, ValidateDirectAssignees(ctx, &fakeActiveChecker{err: context.DeadlineExceeded}, "t1", chain))

	// 未实现 ActiveUserChecker：跳过
	require.NoError(t, ValidateDirectAssignees(ctx, nil, "t1", chain))

	// 租户为空：跳过
	require.NoError(t, ValidateDirectAssignees(ctx, &fakeActiveChecker{}, "", chain))
}

// create() 部署入口：幽灵直派在校验块被拦（ErrValidation）；直派全部有效则
// 守卫放行，流程继续走到 DAO（nil DAO panic 证明守卫未误拦）。
func TestCreate_DirectAssigneeGateAtDeploy(t *testing.T) {
	newProc := func(def string) *model.WfProcess {
		return &model.WfProcess{Name: "直派校验", ProcessKey: "dag-1", TenantID: "t1", DefinitionJSON: def}
	}
	ghost := &ProcessServiceImpl{engine: &fakeEngineIdentity{identity: &fakeActiveChecker{active: map[string]bool{"u1": true}}}}
	_, err := ghost.create(context.Background(), newProc(directAssigneeDefinition("u1", "ghost")), false, string(enums.ProcessStatusActive))
	require.ErrorIs(t, err, ErrValidation)
	require.Contains(t, err.Error(), "ghost")

	onlyActive := &ProcessServiceImpl{engine: &fakeEngineIdentity{identity: &fakeActiveChecker{active: map[string]bool{"u1": true}}}}
	require.Panics(t, func() {
		_, _ = onlyActive.create(context.Background(), newProc(directAssigneeDefinition("u1")), false, string(enums.ProcessStatusActive))
	}, "直派全部有效应通过守卫（panic=nil DAO 到达）")
}

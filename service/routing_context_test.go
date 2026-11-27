package service

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/types/constants"
	"github.com/rulego/gflow-engine/types/enums"
	"github.com/rulego/rulego/utils/el"
	"github.com/stretchr/testify/require"
)

const starterTestChainDef = `{"ruleChain":{"id":"starter_chain","name":"starter","root":true},"metadata":{"firstNodeIndex":0,"nodes":[{"id":"route","type":"jsSwitch","name":"route","configuration":{"jsScript":"if (msg._starter.deptPathIds.includes('dept')) { return ['Default']; } return ['Default'];"}}],"connections":[]}}`

type starterProviderFunc func(ctx context.Context, tenantID, userID string) (map[string]interface{}, error)

func (f starterProviderFunc) BuildStarterContext(ctx context.Context, tenantID, userID string) (map[string]interface{}, error) {
	return f(ctx, tenantID, userID)
}

func TestStarterContextIsReservedAtInstanceAndTaskBoundaries(t *testing.T) {
	require.True(t, isReservedInstanceVariableKey("_starter"))
	require.True(t, isEngineReservedVarKey("_starter"))
}

func TestStarterContextSurvivesTaskVariableLifecycle(t *testing.T) {
	vars := map[string]interface{}{
		constants.VarsStarter:        map[string]interface{}{"deptPathIds": []string{"root", "dept"}},
		constants.VarsFallbackPolicy: "tenant_admin",
		"days":                       2,
	}
	out := stripReservedTaskVars(vars)
	require.Contains(t, out, constants.VarsStarter)
	require.NotContains(t, out, constants.VarsFallbackPolicy)

	submitted := map[string]interface{}{constants.VarsStarter: map[string]interface{}{"deptPathIds": []string{"spoof"}}}
	stripEngineReservedVars(submitted)
	require.NotContains(t, submitted, constants.VarsStarter)
}

func TestResolveDriveVariablesOverlaysTrustedStarter(t *testing.T) {
	instanceVars := `{"days":1,"_starter":{"deptPathIds":["root","company","dept"],"deptPathNames":["Root","Company","Dept"]}}`
	taskVars := `{"days":2,"_starter":{"deptPathIds":["spoof"],"deptPathNames":["Spoof"]}}`

	payload := resolveDriveVariables(&taskVars, &instanceVars)
	var got map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(payload), &got))
	require.Equal(t, float64(2), got["days"])
	starter := got["_starter"].(map[string]interface{})
	require.Equal(t, []interface{}{"root", "company", "dept"}, starter["deptPathIds"])
	require.Equal(t, []interface{}{"Root", "Company", "Dept"}, starter["deptPathNames"])
}

func TestForecastVariablesUseTrustedStarter(t *testing.T) {
	instanceVars := `{"days":1,"_starter":{"deptPathIds":["root","dept"],"deptPathNames":["Root","Dept"]}}`
	taskVars := map[string]interface{}{
		"days":                2,
		constants.VarsStarter: map[string]interface{}{"deptPathIds": []string{"spoof"}},
	}

	got := resolveForecastVariables(taskVars, &instanceVars)
	starter := got[constants.VarsStarter].(map[string]interface{})
	require.Equal(t, []interface{}{"root", "dept"}, starter["deptPathIds"])
	require.Equal(t, 2, got["days"])
}

func TestResolveDriveVariablesStripsStarterWithoutTrustedSnapshot(t *testing.T) {
	instanceVars := `{"days":1}`
	taskVars := `{"days":2,"_starter":{"deptPathIds":["spoof"],"deptPathNames":["Spoof"]}}`

	payload := resolveDriveVariables(&taskVars, &instanceVars)
	var got map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(payload), &got))
	require.NotContains(t, got, "_starter")
	require.Equal(t, float64(2), got["days"])
}

func TestResolveDriveVariablesLeavesLegacyPayloadUntouched(t *testing.T) {
	instanceVars := `{"days":1}`
	taskVars := `{"days":2,"note":"unchanged"}`

	require.Equal(t, taskVars, resolveDriveVariables(&taskVars, &instanceVars))
	require.Equal(t, instanceVars, resolveDriveVariables(nil, &instanceVars))
}

func TestBuilderInjectsRoutingContextProvider(t *testing.T) {
	called := false
	provider := starterProviderFunc(func(context.Context, string, string) (map[string]interface{}, error) {
		called = true
		return map[string]interface{}{"deptPathIds": []string{}, "deptPathNames": []string{}}, nil
	})
	engine, err := NewWorkflowEngineBuilder().SetRoutingContextProvider(provider).Build()
	require.NoError(t, err)
	runtime := NewRuntimeService(engine).(*RuntimeServiceImpl)
	_, err = runtime.getRoutingContextProvider().BuildStarterContext(context.Background(), "t1", "u1")
	require.NoError(t, err)
	require.True(t, called)
}

func TestStartInstanceCoreStripsUntrustedStarter(t *testing.T) {
	rs, _ := newPoolTestRS(t)
	rs.idGenerator = &testSeqIDGen{}
	processDef := &model.WfProcess{
		ID: "proc-starter-strip", ProcessKey: "starter_strip", Name: "starter strip",
		TenantID: "t1", DefinitionJSON: poolTestChainDef("starter_strip"),
	}
	input := map[string]interface{}{
		"days":                2,
		constants.VarsStarter: map[string]interface{}{"deptPathIds": []string{"spoof"}},
	}

	instanceID, _, msg, err := rs.startInstanceCore(context.Background(), processDef,
		Actor{UserID: "u1", TenantID: "t1"}, "", input, false, "")
	require.NoError(t, err)
	var msgData map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(msg.GetData()), &msgData))
	require.NotContains(t, msgData, constants.VarsStarter)

	instance, err := rs.instanceDAO.Get(context.Background(), instanceID)
	require.NoError(t, err)
	var stored map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(*instance.Variables), &stored))
	require.NotContains(t, stored, constants.VarsStarter)
	require.Equal(t, float64(2), stored["days"])
}

func TestStartInstanceCoreRequiresProviderForStarterExpressions(t *testing.T) {
	rs, db := newPoolTestRS(t)
	rs.idGenerator = &testSeqIDGen{}
	processDef := &model.WfProcess{
		ID: "proc-starter-required", ProcessKey: "starter_required", Name: "starter required",
		TenantID: "t1", DefinitionJSON: starterTestChainDef,
	}

	_, _, _, err := rs.startInstanceCore(context.Background(), processDef,
		Actor{UserID: "u1", TenantID: "t1"}, "", nil, false, "")
	require.ErrorContains(t, err, "routing context provider")
	var stored int64
	require.NoError(t, db.Table("wf_instance").Select("count(*)").Scan(&stored).Error)
	require.Zero(t, stored)
}

func TestStartInstanceCoreBuildsTrustedStarterSnapshot(t *testing.T) {
	rs, _ := newPoolTestRS(t)
	rs.idGenerator = &testSeqIDGen{}
	var gotTenant, gotUser string
	rs.starterProvider = starterProviderFunc(func(_ context.Context, tenantID, userID string) (map[string]interface{}, error) {
		gotTenant, gotUser = tenantID, userID
		return map[string]interface{}{
			"deptPathIds":   []string{"root", "company", "dept"},
			"deptPathNames": []string{"Root", "Company", "Dept"},
		}, nil
	})
	processDef := &model.WfProcess{
		ID: "proc-starter-build", ProcessKey: "starter_build", Name: "starter build",
		TenantID: "t1", DefinitionJSON: starterTestChainDef,
	}
	input := map[string]interface{}{
		"days":                2,
		constants.VarsStarter: map[string]interface{}{"deptPathIds": []string{"spoof"}},
	}

	instanceID, _, msg, err := rs.startInstanceCore(context.Background(), processDef,
		Actor{UserID: "u1", TenantID: "t1"}, "", input, false, "")
	require.NoError(t, err)
	require.Equal(t, "t1", gotTenant)
	require.Equal(t, "u1", gotUser)
	var msgData map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(msg.GetData()), &msgData))
	starter := msgData[constants.VarsStarter].(map[string]interface{})
	require.Equal(t, []interface{}{"root", "company", "dept"}, starter["deptPathIds"])

	instance, err := rs.instanceDAO.Get(context.Background(), instanceID)
	require.NoError(t, err)
	var stored map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(*instance.Variables), &stored))
	require.Equal(t, msgData[constants.VarsStarter], stored[constants.VarsStarter])
}

func TestStartInstanceCoreRejectsInvalidStarterSnapshot(t *testing.T) {
	rs, db := newPoolTestRS(t)
	rs.idGenerator = &testSeqIDGen{}
	rs.starterProvider = starterProviderFunc(func(context.Context, string, string) (map[string]interface{}, error) {
		return map[string]interface{}{
			"deptPathIds":   []string{"root", "dept"},
			"deptPathNames": []string{"Root"},
		}, nil
	})
	processDef := &model.WfProcess{
		ID: "proc-starter-invalid", ProcessKey: "starter_invalid", Name: "starter invalid",
		TenantID: "t1", DefinitionJSON: starterTestChainDef,
	}

	_, _, _, err := rs.startInstanceCore(context.Background(), processDef,
		Actor{UserID: "u1", TenantID: "t1"}, "", nil, false, "")
	require.ErrorContains(t, err, "deptPath")
	var stored int64
	require.NoError(t, db.Table("wf_instance").Select("count(*)").Scan(&stored).Error)
	require.Zero(t, stored)
}

func TestStartInstanceCoreProviderFailureLeavesNoInstance(t *testing.T) {
	rs, db := newPoolTestRS(t)
	rs.idGenerator = &testSeqIDGen{}
	rs.starterProvider = starterProviderFunc(func(context.Context, string, string) (map[string]interface{}, error) {
		return nil, errors.New("organization unavailable")
	})
	processDef := &model.WfProcess{
		ID: "proc-starter-error", ProcessKey: "starter_error", Name: "starter error",
		TenantID: "t1", DefinitionJSON: starterTestChainDef,
	}

	_, _, _, err := rs.startInstanceCore(context.Background(), processDef,
		Actor{UserID: "u1", TenantID: "t1"}, "", nil, false, "")
	require.ErrorContains(t, err, "organization unavailable")
	var stored int64
	require.NoError(t, db.Table("wf_instance").Select("count(*)").Scan(&stored).Error)
	require.Zero(t, stored)
}

func TestDraftActivationBuildsStarterSnapshot(t *testing.T) {
	rs, db := newPoolTestRS(t)
	rs.workflowEngine = activateTestEngine{scopeIdentityEngine{identity: newMockIdentity()}}
	rs.starterProvider = starterProviderFunc(func(context.Context, string, string) (map[string]interface{}, error) {
		return map[string]interface{}{
			"deptPathIds": []string{"root", "dept"}, "deptPathNames": []string{"Root", "Dept"},
		}, nil
	})
	require.NoError(t, db.Exec(`INSERT INTO wf_process (id, process_key, name, version, definition_json, status, tenant_id, created_by, process_type, icon) VALUES ('p_starter_draft','starter_draft','starter draft',1,?, 'active', 't1', 'tester', 'main', '')`, starterTestChainDef).Error)
	seedDraftInstance(t, db, "d_starter", "p_starter_draft", "t1", "u1")
	spoof := `{"days":2,"_starter":{"deptPathIds":["spoof"],"deptPathNames":["Spoof"]}}`
	require.NoError(t, db.Table("wf_instance").Where("id = ?", "d_starter").Update("variables", spoof).Error)
	ctx := bindActor(context.Background(), Actor{UserID: "u1", TenantID: "t1"})

	require.NoError(t, WithInstanceTx(ctx, rs.instanceDAO.Underlying(), "d_starter", func(scope *InstanceScope) error {
		_, started, err := rs.activateProcessInstanceInternal(ctx, scope, "d_starter")
		require.True(t, started)
		return err
	}))
	instance, err := rs.instanceDAO.Get(ctx, "d_starter")
	require.NoError(t, err)
	var stored map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(*instance.Variables), &stored))
	starter := stored[constants.VarsStarter].(map[string]interface{})
	require.Equal(t, []interface{}{"root", "dept"}, starter["deptPathIds"])
	require.Equal(t, float64(2), stored["days"])
}

func TestDraftActivationProviderFailureKeepsDraft(t *testing.T) {
	rs, db := newPoolTestRS(t)
	rs.workflowEngine = activateTestEngine{scopeIdentityEngine{identity: newMockIdentity()}}
	rs.starterProvider = starterProviderFunc(func(context.Context, string, string) (map[string]interface{}, error) {
		return nil, errors.New("organization unavailable")
	})
	require.NoError(t, db.Exec(`INSERT INTO wf_process (id, process_key, name, version, definition_json, status, tenant_id, created_by, process_type, icon) VALUES ('p_starter_draft_error','starter_draft_error','starter draft error',1,?, 'active', 't1', 'tester', 'main', '')`, starterTestChainDef).Error)
	seedDraftInstance(t, db, "d_starter_error", "p_starter_draft_error", "t1", "u1")
	ctx := bindActor(context.Background(), Actor{UserID: "u1", TenantID: "t1"})

	err := WithInstanceTx(ctx, rs.instanceDAO.Underlying(), "d_starter_error", func(scope *InstanceScope) error {
		_, _, err := rs.activateProcessInstanceInternal(ctx, scope, "d_starter_error")
		return err
	})
	require.ErrorContains(t, err, "organization unavailable")
	instance, getErr := rs.instanceDAO.Get(ctx, "d_starter_error")
	require.NoError(t, getErr)
	require.Equal(t, string(enums.InstanceStatusDraft), instance.Status)
}

func TestSubProcessInheritsParentStarterSnapshot(t *testing.T) {
	rs, _ := newPoolTestRS(t)
	rs.idGenerator = &testSeqIDGen{}
	rs.starterProvider = starterProviderFunc(func(context.Context, string, string) (map[string]interface{}, error) {
		return nil, errors.New("provider must not be called for child snapshot")
	})
	processDef := &model.WfProcess{
		ID: "proc-starter-child", ProcessKey: "starter_child", Name: "starter child",
		TenantID: "t1", DefinitionJSON: starterTestChainDef,
	}
	input := map[string]interface{}{constants.VarsStarter: map[string]interface{}{
		"deptPathIds": []string{"root", "dept"}, "deptPathNames": []string{"Root", "Dept"},
	}}

	_, _, msg, err := rs.startInstanceCore(context.Background(), processDef,
		Actor{UserID: "u1", TenantID: "t1"}, "", input, false, "parent-1")
	require.NoError(t, err)
	var data map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(msg.GetData()), &data))
	starter := data[constants.VarsStarter].(map[string]interface{})
	require.Equal(t, []interface{}{"root", "dept"}, starter["deptPathIds"])
}

func TestStarterExpressionRuntimeSemantics(t *testing.T) {
	variables := map[string]interface{}{
		constants.VarsStarter: map[string]interface{}{
			"deptPathIds":   []string{"root", "company", "dept"},
			"deptPathNames": []string{"测试集团", "子公司甲", "测试部门"},
		},
	}
	expressions := []string{
		`"company" in msg._starter.deptPathIds`,
		`"other" not in msg._starter.deptPathIds`,
		`"子公司甲" in msg._starter.deptPathNames`,
		`last(msg._starter.deptPathIds) == "dept"`,
	}
	for _, expression := range expressions {
		tpl, err := el.NewExprTemplate(expression)
		require.NoError(t, err)
		actual, err := tpl.Execute(map[string]interface{}{"msg": variables})
		require.NoError(t, err)
		require.Equal(t, true, actual, expression)
	}
}

// 检测只认条件载体：节点名、脚本注释里出现同字样的无关流程不该在发起时被拦。
func TestRequiresStarterContextOnlyScansConditionCarriers(t *testing.T) {
	casesDef := `{"metadata":{"nodes":[{"id":"n1","type":"switch","name":"含msg._starter的节点名","configuration":{"cases":[{"case":"'dept' in msg._starter.deptPathIds","then":"a"}]}}]}}`
	jsDef := `{"metadata":{"nodes":[{"id":"n2","type":"jsSwitch","name":"脚本路由","configuration":{"jsScript":"if (msg._starter.deptPathIds.length > 0) { return ['a']; } return ['b'];"}}]}}`
	nameOnlyDef := `{"metadata":{"nodes":[{"id":"n3","type":"switch","name":"msg._starter字样","configuration":{"cases":[{"case":"msg.days > 1","then":"a"}]}}]}}`
	scriptCommentDef := `{"metadata":{"nodes":[{"id":"n4","type":"jsSwitch","name":"脚本路由","configuration":{"jsScript":"// 参考 msg._starter 写法\nreturn ['b'];"}}]}}`
	otherNodeDef := `{"metadata":{"nodes":[{"id":"n5","type":"userTask","name":"msg._starter","configuration":{}}]}}`

	require.True(t, requiresStarterContext(casesDef), "switch case 引用应命中")
	require.True(t, requiresStarterContext(jsDef), "jsSwitch 脚本引用应命中")
	require.False(t, requiresStarterContext(nameOnlyDef), "仅节点名含字样不应命中")
	require.False(t, requiresStarterContext(scriptCommentDef), "脚本注释里的字样不应命中")
	require.False(t, requiresStarterContext(otherNodeDef), "非条件节点不应命中")
	require.True(t, requiresStarterContext("not-json-but-has-msg._starter"), "解析失败保守按命中处理")

	// rulego 原生嵌套形态（ruleChain.metadata.nodes）同样覆盖
	nestedDef := `{"ruleChain":{"metadata":{"nodes":[{"id":"n6","type":"switch","configuration":{"cases":[{"case":"last(msg._starter.deptPathIds) == 'd1'","then":"a"}]}}]}}}`
	require.True(t, requiresStarterContext(nestedDef), "嵌套形态的 case 引用应命中")
}

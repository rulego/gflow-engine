package service

// 鉴权 fail-closed 边界用例（覆盖 Delegate/Transfer/AddSign/ReduceSign/
// requireActionEnabled 五条路径）：
//  1. 未分配（assignee=nil）任务不可被委派/转办
//  2. 空 UserID 操作者不可加签/减签
//  3. requireActionEnabled 解析失败时拒绝动作（fail-closed）
//  4. （锁内复校见 addSignInternal/reduceSignInternal/deleteTaskInternal 实现）

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rulego/gflow-engine/dao"
	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/types/enums"
)

// newFailClosedSvc 构造绑定 secFixDB（wf_task 表）的 TaskServiceImpl。
func newFailClosedSvc(t *testing.T) *TaskServiceImpl {
	t.Helper()
	q := secFixDB(t)
	return &TaskServiceImpl{
		taskDAO:        dao.NewTaskDAOWithQuery(q),
		hiTaskDAO:      dao.NewHiTaskDAOWithQuery(q),
		workflowEngine: &testEngineDouble{},
	}
}

// seedFailClosedTask 写入一条 active 任务。assignee=="" 表示未分配（nil），
// instanceID=="" 表示 orphan/draft（无实例）。
func seedFailClosedTask(t *testing.T, svc *TaskServiceImpl, id, tenant, assignee, instanceID string) {
	t.Helper()
	task := &model.WfTask{
		ID:         id,
		TaskDefKey: "approve",
		Name:       "审批",
		TaskType:   "user_task",
		Status:     string(enums.TaskStatusActive),
		TenantID:   tenant,
		CreatedBy:  "system",
		CreatedAt:  time.Now(),
	}
	if assignee != "" {
		task.Assignee = secFixStrPtr(assignee)
	}
	if instanceID != "" {
		task.ProcessInstanceID = secFixStrPtr(instanceID)
	}
	require.NoError(t, svc.taskDAO.Create(context.Background(), task))
}

// TestFailClosed_DelegateUnassignedTaskRejected 验证：未分配（assignee=nil）任务不可被委派。
func TestFailClosed_DelegateUnassignedTaskRejected(t *testing.T) {
	svc := newFailClosedSvc(t)
	seedFailClosedTask(t, svc, "task-del-unassigned", "t1", "", "")

	err := svc.Delegate(context.Background(), Actor{UserID: "userA", UserName: "A", TenantID: "t1"}, "task-del-unassigned", "userB", "帮我办")
	require.Error(t, err, "未分配任务不应允许委派")
	require.True(t, errors.Is(err, ErrPermissionDenied), "期望 ErrPermissionDenied，got %v", err)
}

// TestFailClosed_TransferUnassignedTaskRejected 验证：未分配（assignee=nil）任务不可被转办。
func TestFailClosed_TransferUnassignedTaskRejected(t *testing.T) {
	svc := newFailClosedSvc(t)
	seedFailClosedTask(t, svc, "task-tr-unassigned", "t1", "", "")

	err := svc.Transfer(context.Background(), Actor{UserID: "userA", UserName: "A", TenantID: "t1"}, "task-tr-unassigned", "userC", "转给你")
	require.Error(t, err, "未分配任务不应允许转办")
	require.True(t, errors.Is(err, ErrPermissionDenied), "期望 ErrPermissionDenied，got %v", err)
}

// TestFailClosed_AddSignEmptyOperatorRejected 验证：空 UserID 的操作者无法加签。
func TestFailClosed_AddSignEmptyOperatorRejected(t *testing.T) {
	svc := newFailClosedSvc(t)
	seedFailClosedTask(t, svc, "task-sign", "t1", "userA", "")

	err := svc.AddSign(context.Background(), Actor{UserID: "", UserName: "", TenantID: "t1"}, "task-sign", []string{"userB"}, "加签")
	require.Error(t, err, "空操作者身份不应允许加签")
	require.True(t, errors.Is(err, ErrPermissionDenied), "期望 ErrPermissionDenied，got %v", err)
}

// TestFailClosed_ReduceSignEmptyOperatorRejected 验证：空 UserID 的操作者无法减签。
func TestFailClosed_ReduceSignEmptyOperatorRejected(t *testing.T) {
	svc := newFailClosedSvc(t)
	seedFailClosedTask(t, svc, "task-rsign", "t1", "userA", "")

	err := svc.ReduceSign(context.Background(), Actor{UserID: "", UserName: "", TenantID: "t1"}, "task-rsign", []string{"userB"}, "减签")
	require.Error(t, err, "空操作者身份不应允许减签")
	require.True(t, errors.Is(err, ErrPermissionDenied), "期望 ErrPermissionDenied，got %v", err)
}

// TestFailClosed_ActionPermissionsResolutionFailureRejected 验证：任务关联了实例但
// 设计器配置无法解析（这里 workflowEngine 未注入运行时服务）时，requireActionEnabled
// 必须 fail-closed 拒绝。
func TestFailClosed_ActionPermissionsResolutionFailureRejected(t *testing.T) {
	svc := newFailClosedSvc(t)
	seedFailClosedTask(t, svc, "task-ap", "t1", "userA", "inst-x")

	// userA 是合法 assignee（满足 assignee 校验），但 actionPermissions 解析失败 → 拒绝
	err := svc.Transfer(context.Background(), Actor{UserID: "userA", UserName: "A", TenantID: "t1"}, "task-ap", "userC", "转办")
	require.Error(t, err, "设计器配置解析失败时动作应被拒绝（fail-closed）")
	require.True(t, errors.Is(err, ErrPermissionDenied), "期望 ErrPermissionDenied，got %v", err)
}

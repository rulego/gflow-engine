// 管理员代审：代替在途任务当前办理人出票。assignee 不变、票记原办理人名下，
// 任务变量写 proxy_operator/proxy_time 留痕，被代审的票不可再收回。

package service

import (
	"context"
	"fmt"

	"github.com/rulego/gflow-engine/types/constants"
	"github.com/rulego/gflow-engine/types/enums"
)

// ProxyAudit 管理员代审在途任务。
// 前置校验（锁外）：管理员/系统身份、任务已签收、非委派中、流程级 proxyAudit
// 开关未显式关闭（缺省即开）。
// 出票走 completeWithApprovalInternal 的 OnBehalfOf 分支（锁内还有委派兜底）。
func (s *TaskServiceImpl) ProxyAudit(ctx context.Context, actor Actor, taskID string, approved bool, comment string) error {
	ctx = bindActorAPI(ctx, actor)
	if taskID == "" {
		return fmt.Errorf("task ID cannot be empty")
	}
	// 管理操作鉴权：代审替人签字，必须 WorkflowAdmin/系统（同 Reassign 口径），
	// 权限粒度由宿主 workflow:task:proxy 权限位控制
	if err := requireAdminIdentity(&actor); err != nil {
		return err
	}

	task, err := s.taskDAO.Get(ctx, taskID)
	if err != nil {
		return fmt.Errorf("failed to get task: %w", err)
	}
	if task == nil {
		return fmt.Errorf("%w: task", ErrNotFound)
	}
	if u := GetUserFromCtx(ctx); u != nil && task.TenantID != u.TenantID {
		return fmt.Errorf("%w: task", ErrNotFound)
	}
	if task.Assignee == nil || *task.Assignee == "" {
		return fmt.Errorf("%w: 任务尚未签收办理人，无法代审（可先认领或改派）", ErrValidation)
	}
	// 仅审批任务可代审：ccTask 等信息类任务被出票会污染审批历史
	if task.TaskType != constants.TaskTypeUserTask {
		return fmt.Errorf("%w: 仅审批任务可以代审", ErrValidation)
	}
	if task.Owner != nil && *task.Owner != "" {
		return fmt.Errorf("%w: 任务已委派，请等被委派人办理或归还后再代审", ErrValidation)
	}
	// 流程级开关缺省即开（opt-out）：代审是管理员兜底动作，不要求逐流程显式
	// 开启；不开放的流程在设计器显式关闭。存量 DSL 无此键同按开处理，与详情
	// 按钮位同口径。
	// 定义解析要读流程表，放事务外避免拉长行锁持有时间。
	ap, _, err := resolveProcessActionPermissions(ctx, s.workflowEngine, task.ProcessID)
	if err != nil {
		return fmt.Errorf("%w: 流程定义不可用，无法代审", ErrValidation)
	}
	if designerDisabled(ap, "proxyAudit") {
		return fmt.Errorf("%w: 该流程已关闭管理员代审", ErrPermissionDenied)
	}

	result := enums.ApprovalResultApproved
	if !approved {
		result = enums.ApprovalResultRejected
	}
	ctx = WithOnBehalfOf(ctx, *task.Assignee)
	ctx = WithEventSource(ctx, EventSourceProxy)

	instanceID := ""
	if task.ProcessInstanceID != nil {
		instanceID = *task.ProcessInstanceID
	}
	request := &ApprovalRequest{TaskID: taskID, ApprovalResult: result, Comment: comment}
	if instanceID == "" {
		return s.completeWithApprovalInternal(ctx, bareScope(s.taskDAO.Query), request)
	}
	return WithInstanceTx(ctx, s.taskDAO.Query, instanceID, func(scope *InstanceScope) error {
		return s.completeWithApprovalInternal(ctx, scope, request)
	})
}

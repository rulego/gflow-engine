/*
 * Copyright 2025 The RuleGo Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package components

import (
	"context"
	"fmt"
	"time"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/service"
	"github.com/rulego/gflow-engine/types/constants"
	"github.com/rulego/gflow-engine/types/enums"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/components/base"
	"github.com/rulego/rulego/utils/el"
	"github.com/rulego/rulego/utils/maps"
	"github.com/sirupsen/logrus"
)

// CCTaskNodeType 抄送任务节点类型
const CCTaskNodeType = "ccTask"

// CCTaskNodeConfiguration 抄送任务节点配置
type CCTaskNodeConfiguration struct {
	// CCUserIds 抄送人列表：静态 userId 或 "${msg.xxx}" 表达式模板项，
	// 模板项运行时以流程变量求值，结果为字符串取单值、为数组则逐项摊平
	CCUserIds []string `json:"ccUserIds"`
}

type CCTaskNode struct {
	Config         CCTaskNodeConfiguration
	TaskService    service.TaskServiceInternal
	CurrentNodeDef types.RuleNode
	// TenantGuard 租户归属鉴权守卫：校验抄送人属于实例租户
	// （TenantMembershipChecker）。由 Register 注入；未实现该接口的宿主跳过校验。
	TenantGuard service.TenantMembershipGuard

	// ccUserTemplates 名单逐项预编译的模板（Init 期构建）
	ccUserTemplates []el.Template

	// OnCCTaskCreated 由 Builder.SetCCTaskCreatedListener 经
	// Register 注入。每条 CC 任务创建成功后调用一次。
	// nil 时仅写 wf_task，不发出事件。
	OnCCTaskCreated service.CCTaskCreatedListener
}

func (x *CCTaskNode) Type() string {
	return CCTaskNodeType
}

// New 创建新实例
func (x *CCTaskNode) New() types.Node {
	return &CCTaskNode{
		Config:      CCTaskNodeConfiguration{},
		TaskService: x.TaskService,
		// 监听器从注册原型拷贝到每条链的执行实例，漏掉会导致 CC 事件被静默丢弃
		OnCCTaskCreated: x.OnCCTaskCreated,
		TenantGuard:     x.TenantGuard,
	}
}

// Init 初始化组件
func (x *CCTaskNode) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &x.Config)
	if err != nil {
		return err
	}
	// 保存当前节点信息
	x.CurrentNodeDef = base.NodeUtils.GetSelfDefinition(configuration)
	// 名单逐项预编译：含 "${...}" 的项按模板求值，静态项原样返回
	x.ccUserTemplates = make([]el.Template, 0, len(x.Config.CCUserIds))
	for _, item := range x.Config.CCUserIds {
		tpl, err := el.NewTemplate(item)
		if err != nil {
			return fmt.Errorf("failed to compile ccUserIds item %q: %w", item, err)
		}
		x.ccUserTemplates = append(x.ccUserTemplates, tpl)
	}
	return nil
}

// OnMsg 处理消息
//
// CC（抄送）任务的设计：
//   - CC 任务是"知会型"任务：抄送对象只需被告知，无需审批操作
//   - 因此创建时即标记 Status=Completed + EndedAt=now + EndReason=EndReasonCC("cc")，等同历史记录
//   - 流程不会因 CC 任务而暂停，OnMsg 末尾直接 TellSuccess 推进下游
func (x *CCTaskNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
	defer recoverNodePanic(ctx, msg, CCTaskNodeType, x.GetSelfId())
	// 获取抄送用户：模板项以流程变量求值（变量在 env 信封的 "msg" 下，见 extractVariables）。
	// 求值结果为字符串取单值、为数组逐项摊平；变量缺失的模板项求值为空，不视为错误。
	// 节点实例跨消息复用，名单先复制再追加，避免并发 OnMsg 写共享底层数组。
	vars := extractVariables(ctx, msg)
	ccUserIds := make([]string, 0, len(x.ccUserTemplates))
	for _, tpl := range x.ccUserTemplates {
		v, err := tpl.Execute(map[string]interface{}{types.MsgKey: vars})
		if err != nil {
			logrus.WithError(err).WithField("nodeId", x.GetSelfId()).Warn("ccUserIds template execute failed")
			continue
		}
		switch val := v.(type) {
		case string:
			if val != "" {
				ccUserIds = append(ccUserIds, val)
			}
		case []string:
			ccUserIds = append(ccUserIds, val...)
		case []interface{}:
			for _, item := range val {
				if s := fmt.Sprintf("%v", item); s != "" {
					ccUserIds = append(ccUserIds, s)
				}
			}
		}
	}

	// 去重
	uniqueUserIds := make(map[string]bool)
	var finalUserIds []string
	for _, id := range ccUserIds {
		if id != "" && !uniqueUserIds[id] {
			uniqueUserIds[id] = true
			finalUserIds = append(finalUserIds, id)
		}
	}

	if len(finalUserIds) > 0 {
		processInstanceID := metaValue(msg, constants.KeyInstanceID)
		processID := metaValue(msg, constants.KeyProcessID)
		tenantID := metaValue(msg, constants.KeyTenantID)
		// 创建抄送任务（创建即完成，等同历史记录）
		now := time.Now()
		endReason := string(enums.EndReasonCC) // 标记为抄送任务，便于历史检索
		// 抄送人必须属于实例租户：拦截业务变量/静态名单里夹带的跨租户 userId（越租户抄送）。
		// 批量探测一次（宿主实现 TenantMembershipBatchChecker 时整单名单单次查询，无逐人 N+1）。
		membership := x.TenantGuard.CheckUsersInTenant(ctx.GetContext(), tenantID, finalUserIds)
		// 非阻塞语义：个别失败只计数继续循环，至少一个成功即推进下游（全部失败才 TellFailure）
		failedCount := 0
		firstErr := error(nil)
		for _, userId := range finalUserIds {
			if err := membership[userId]; err != nil {
				failedCount++
				if firstErr == nil {
					firstErr = fmt.Errorf("cc user %q not in tenant %q: %w", userId, tenantID, err)
				}
				continue
			}
			task := &model.WfTask{
				ProcessInstanceID: &processInstanceID,
				ProcessID:         processID,
				TaskDefKey:        x.GetSelfId(),
				TaskType:          CCTaskNodeType,
				Name:              x.GetSelfName(),
				Description:       nil,
				Status:            string(enums.TaskStatusCompleted), // 创建即完成
				Assignee:          &userId,
				ApprovalType:      string(enums.ApprovalTypeCC),
				CreatedBy:         constants.UserSystem,
				CreatedAt:         now,
				UpdatedAt:         &now,
				EndedAt:           &now, // 抄送任务不需要处理，直接结束
				EndReason:         &endReason,
				TenantID:          tenantID,
			}
			if _, err := x.TaskService.CreateTask(ctx.GetContext(), service.SystemActor(), task); err != nil {
				// 记录首个错误用于诊断；不 return，让剩余用户的抄送任务仍被创建。
				failedCount++
				if firstErr == nil {
					firstErr = err
				}
				// 不派发事件（任务没创建成功），直接跳过此用户。
				continue
			}
			// 派发 CC 事件给已注册监听器（如 gflow 的 notifications 表），失败不阻塞流程
			x.fireCCTaskCreated(ctx.GetContext(), service.CCEvent{
				TaskID:            task.ID,
				ProcessInstanceID: processInstanceID,
				ProcessID:         processID,
				TenantID:          tenantID,
				AssigneeUserID:    userId,
				TaskName:          x.GetSelfName(),
				CreatedAt:         now,
			})
		}

		// 仅当所有抄送任务都失败时才让节点失败；至少有一个成功就视为成功（CC 是非阻塞语义）。
		if failedCount == len(finalUserIds) && firstErr != nil {
			ctx.TellFailure(msg, fmt.Errorf("all cc tasks failed; first error: %w", firstErr))
			return
		}
		if failedCount > 0 {
			// 部分失败也记录到 metadata 便于排查，但仍 TellSuccess 推进下游
			if msg.Metadata != nil {
				msg.Metadata.PutValue("ccFailedCount", fmt.Sprintf("%d", failedCount))
			}
		}
	}

	ctx.TellSuccess(msg)
}

func (x *CCTaskNode) Destroy() {
}

// fireCCTaskCreated 异步派发 CC 事件，监听器为空或 panic 不影响节点主流程。
func (x *CCTaskNode) fireCCTaskCreated(ctx context.Context, evt service.CCEvent) {
	if x.OnCCTaskCreated == nil {
		// 抄送任务已建但事件无人接收，上层通知会丢失；监听器经 Builder/ComponentDeps 注入，
		// 装配说明见 register.go。
		logrus.WithField("taskId", evt.TaskID).WithField("assignee", evt.AssigneeUserID).
			Warn("CC event dropped: OnCCTaskCreated listener is nil")
		return
	}
	service.DispatchCCEvent(x.OnCCTaskCreated, evt, ctx)
}

// GetSelfId 获取当前节点ID
func (x *CCTaskNode) GetSelfId() string {
	return selfID(x.CurrentNodeDef, CCTaskNodeType)
}

// GetSelfName 获取当前节点Name
func (x *CCTaskNode) GetSelfName() string {
	return selfName(x.CurrentNodeDef)
}

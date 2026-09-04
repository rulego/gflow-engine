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

package service

import (
	"context"
	"fmt"

	"github.com/rulego/gflow-engine/types/constants"
)

// Actor 操作人：引擎公共 API 的变更类操作全部以显式 actor 参数传入。
type Actor struct {
	UserID   string `json:"userId"`
	UserName string `json:"userName"`
	TenantID string `json:"tenantId"`
	// SuperAdmin：工作流管理员（持 workflow:instance:view 的运营/管理角色）。
	// 实例详情 IDOR 校验对其放行（管理侧需要查看所有实例）；普通审批用户不设此标记。
	// 仅由宿主服务端按角色判定设置，不带 json tag：Actor 不得从请求体反序列化，
	// 防止客户端伪造管理员标记。
	SuperAdmin bool
}

// SystemActor 引擎内部机制（节点自动推进/巡检等）代替用户执行时的操作人，
// 用户 ID 取 constants.UserSystem。
func SystemActor() Actor {
	return Actor{UserID: constants.UserSystem, UserName: constants.UserSystem}
}

// IsSystemActor 判断操作人是否为系统身份：系统代表平台自身操作，不冒充任何用户，
// 不受"仅本人可操作"类限制（租户校验等照常执行）。注意 ActivateProcessInstance
// 等入口经 bindActor 把 actor 写进 ctx，系统上下文里 GetUserFromCtx 永远非 nil，
// 判断系统身份必须用本方法，不能依赖"ctx 无用户"。
func IsSystemActor(a *Actor) bool {
	return a != nil && a.UserID == constants.UserSystem
}

// requireAdminIdentity 校验操作人为工作流管理员（Actor.SuperAdmin）或系统身份。
// 用于跳过 assignee/候选人校验的强制改派类管理操作（Reassign/SetAssignee/SetOwner）：
// 这些操作绕过"仅本人可操作"语义，引擎内部无其他鉴权点，必须在入口强制校验，
// 否则任意同租户用户可拿到 taskID 即劫持他人任务。
//
// 系统身份（IsSystemActor）放行，供定时巡检/跨服务级联等引擎内部机制使用；
// 管理员身份由宿主服务端按角色判定后置 SuperAdmin 标记（Actor 无 json tag，
// 不能从请求体反序列化伪造）。
func requireAdminIdentity(actor *Actor) error {
	if actor == nil || actor.UserID == "" {
		return fmt.Errorf("admin identity required: %w", ErrAuthenticationRequired)
	}
	if actor.SuperAdmin || IsSystemActor(actor) {
		return nil
	}
	return fmt.Errorf("operation requires admin or system identity: %w", ErrPermissionDenied)
}

// ActorFromCtx 取 ctx 已绑定操作人，未绑定返回 SystemActor。
// 供引擎内部回调（aspect/节点/级联）使用，保留原身份可维持租户校验与事件归属。
func ActorFromCtx(ctx context.Context) Actor {
	if u := GetUserFromCtx(ctx); u != nil {
		return *u
	}
	return SystemActor()
}

// bindActor 把显式 actor 绑进 ctx 并标记调用模式。
// ctx 已带 CallingModeInternal 时保留内部模式，避免节点自动推进被误判为 API 入口。
func bindActor(ctx context.Context, actor Actor) context.Context {
	ctx = SetUserToCtx(ctx, &actor)
	if GetCallingMode(ctx) == CallingModeUnknown {
		ctx = WithAPICallingMode(ctx)
	}
	return ctx
}

// ensureTenantAccess 校验 ctx 操作人与资源属同租户。resourceDesc 用于错误信息
// （如 "process instance"）。跨租户按 ErrPermissionDenied 拒绝；需要隐藏资源
// 存在性的路径（claim/withdraw 等）仍应各自按 ErrNotFound 处理，不走本方法。
//
// 放行三类：资源侧租户为空（单租户部署/历史数据）；显式系统身份（IsSystemActor，
// 平台自身跨租户操作是设计内的，如定时巡检、级联清理）；ctx 无 actor——引擎
// 内部级联（aspect/节点回调）不带 actor，与 ActorFromCtx 的"无用户视为系统"
// 约定一致。API 入口都经 bindActor 绑定操作人，真正要拦的是"半构造 actor"：
// UserID 非空但租户为空（无租户 claim 的旧 token、漏传租户的调用方）。
func ensureTenantAccess(ctx context.Context, resourceDesc, resourceTenantID string) error {
	if resourceTenantID == "" {
		return nil
	}
	u := GetUserFromCtx(ctx)
	if u == nil || IsSystemActor(u) {
		return nil
	}
	if u.TenantID == resourceTenantID {
		return nil
	}
	return fmt.Errorf("%s belongs to another tenant: %w", resourceDesc, ErrPermissionDenied)
}

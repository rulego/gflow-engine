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
	"fmt"
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/rulego/gflow-engine/types/enums"
	"github.com/rulego/rulego/utils/el"
)

// 内置审批人类型策略。身份服务查询报错向上抛：身份服务故障不是"审批人为空"，
// 吞掉会静默走兜底策略，auto_approve 下等于无审批放行。组织顶端的发起人
// 没有上级是数据常态，按空成员落节点策略。}
func init() {
	builtins := map[enums.CandidateType]ApproverResolver{
		enums.CandidateTypeUser:              userResolver{},
		enums.CandidateTypeRole:              roleResolver{},
		enums.CandidateTypeDirectManager:     directManagerResolver{},
		enums.CandidateTypeInitiatorSelect:   initiatorSelectResolver{},
		enums.CandidateTypeInitiatorSelf:     initiatorSelfResolver{},
		enums.CandidateTypeMultiLevelManager: multiLevelManagerResolver{},
		enums.CandidateTypeDept:              deptResolver{},
	}
	for t, r := range builtins {
		// 同类型仅 init 期注册一次，失败属编程错误，静默忽略与方言注册口径一致
		_ = RegisterApproverResolver(t, r)
	}
}

// dedupeIDs 去重保序。
func dedupeIDs(ids []string) []string {
	var out []string
	set := map[string]bool{}
	for _, id := range ids {
		out = addUnique(out, set, id)
	}
	return out
}

// nonEmptyIDs 剔除空串。
func nonEmptyIDs(ids []string) []string {
	filtered := make([]string, 0, len(ids))
	for _, id := range ids {
		if id != "" {
			filtered = append(filtered, id)
		}
	}
	return filtered
}

// cfgPoolGroups 按 cfg 内 role/department 两组实体产候选池。组类型（role/dept）
// 的类型字段不互斥——手写 DSL 可能同时携带两组 ID，落池顺序与 wf_task_assignee
// 写入顺序一致：先 role 后 department。
func cfgPoolGroups(cfg *ApproverConfig) []PoolCandidateGroup {
	var groups []PoolCandidateGroup
	if ids := nonEmptyIDs(cfg.RoleIds); len(ids) > 0 {
		groups = append(groups, PoolCandidateGroup{EntityType: string(enums.EntityTypeRole), IDs: ids})
	}
	if ids := nonEmptyIDs(cfg.DeptIds); len(ids) > 0 {
		groups = append(groups, PoolCandidateGroup{EntityType: string(enums.EntityTypeDepartment), IDs: ids})
	}
	return groups
}

type userResolver struct{}

func (userResolver) Resolve(in ApproverResolveInput) ([]string, error) {
	return dedupeIDs(in.Cfg.UserIds), nil
}

func (userResolver) Validate(cfg *ApproverConfig) []string {
	if len(cfg.UserIds) == 0 {
		return []string{"approver.userIds is empty"}
	}
	return nil
}

func (userResolver) PoolCandidates(*ApproverConfig) []PoolCandidateGroup { return nil }

type roleResolver struct{}

func (roleResolver) Resolve(in ApproverResolveInput) ([]string, error) {
	if in.Identity == nil {
		return nil, fmt.Errorf("identity service not configured for role-based candidate resolution")
	}
	var assignees []string
	set := map[string]bool{}
	for _, rid := range in.Cfg.RoleIds {
		if rid == "" {
			continue
		}
		members, err := in.Identity.GetUserIDsByRoleID(in.Ctx, in.TenantID, rid)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve role members of %s: %w", rid, err)
		}
		for _, m := range members {
			assignees = addUnique(assignees, set, m)
		}
	}
	return assignees, nil
}

func (roleResolver) Validate(cfg *ApproverConfig) []string {
	if len(cfg.RoleIds) == 0 {
		return []string{"approver.roleIds is empty"}
	}
	return nil
}

func (roleResolver) PoolCandidates(cfg *ApproverConfig) []PoolCandidateGroup {
	return cfgPoolGroups(cfg)
}

type directManagerResolver struct{}

func (directManagerResolver) Resolve(in ApproverResolveInput) ([]string, error) {
	if in.Owner == "" {
		return nil, fmt.Errorf("direct_manager candidate requires process owner in metadata")
	}
	if in.Identity == nil {
		return nil, fmt.Errorf("identity service not configured for direct_manager candidate resolution")
	}
	// levels>1 表示取第 N 级主管（设计器"发起人的第 N 级主管"），逐级向上只保留终点
	levels := in.Cfg.Levels
	if levels <= 0 {
		levels = 1
	}
	current := in.Owner
	managerID := ""
	for i := 0; i < levels; i++ {
		mgr, err := in.Identity.GetUserManagerID(in.Ctx, in.TenantID, current)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve manager of %s: %w", current, err)
		}
		if mgr == "" {
			// 组织顶端的发起人没有上级是数据常态而非故障
			logrus.WithFields(logrus.Fields{"node": in.NodeID, "owner": in.Owner, "level": i + 1}).
				Info("no manager found; empty approvers fall back to node policy")
			return nil, nil
		}
		managerID = mgr
		current = mgr
	}
	return []string{managerID}, nil
}

func (directManagerResolver) Validate(*ApproverConfig) []string { return nil }

func (directManagerResolver) PoolCandidates(*ApproverConfig) []PoolCandidateGroup { return nil }

type initiatorSelectResolver struct{}

func (initiatorSelectResolver) Resolve(in ApproverResolveInput) ([]string, error) {
	if in.Expression == nil {
		logrus.Warn("initiatorSelectedTemplate is nil")
		return nil, nil
	}
	v, err := in.Expression(in.Vars)
	if err != nil {
		// 模板求值报错（变量缺失/类型不符）向上抛，不落空静默走兜底
		return nil, fmt.Errorf("failed to execute initiator selected template: %w", err)
	}
	return dedupeIDs(toStringSlice(v)), nil
}

func (initiatorSelectResolver) Validate(cfg *ApproverConfig) []string {
	if strings.TrimSpace(cfg.Expression) == "" {
		return []string{"approver.expression is empty"}
	}
	if _, err := el.NewTemplate(strings.TrimSpace(cfg.Expression)); err != nil {
		return []string{fmt.Sprintf("approver.expression compile: %v", err)}
	}
	return nil
}

func (initiatorSelectResolver) PoolCandidates(*ApproverConfig) []PoolCandidateGroup { return nil }

type initiatorSelfResolver struct{}

func (initiatorSelfResolver) Resolve(in ApproverResolveInput) ([]string, error) {
	if in.Owner == "" {
		return nil, nil
	}
	return []string{in.Owner}, nil
}

func (initiatorSelfResolver) Validate(*ApproverConfig) []string { return nil }

func (initiatorSelfResolver) PoolCandidates(*ApproverConfig) []PoolCandidateGroup { return nil }

type multiLevelManagerResolver struct{}

func (multiLevelManagerResolver) Resolve(in ApproverResolveInput) ([]string, error) {
	if in.Owner == "" {
		return nil, fmt.Errorf("multi_level_manager candidate requires process owner in metadata")
	}
	if in.Identity == nil {
		return nil, fmt.Errorf("identity service not configured for multi_level_manager candidate resolution")
	}
	// levels>0：固定审批到第 N 级；levels<0：直到最上层（设计器 directorMode=0），
	// 组织关系中没有更上级时自然停止。
	// visited 防组织关系成环（A 的上级是 B、B 的上级是 A）导致死循环
	levels := in.Cfg.Levels
	current := in.Owner
	visited := map[string]bool{in.Owner: true}
	var assignees []string
	set := map[string]bool{}
	for i := 0; levels < 0 || i < levels; i++ {
		mgr, err := in.Identity.GetUserManagerID(in.Ctx, in.TenantID, current)
		if err != nil || mgr == "" {
			break
		}
		if visited[mgr] {
			logrus.WithFields(logrus.Fields{
				"node": in.NodeID, "user": current, "cycleTo": mgr,
			}).Warn("manager chain has a cycle; treating as top of organization")
			break
		}
		visited[mgr] = true
		assignees = addUnique(assignees, set, mgr)
		current = mgr
	}
	return assignees, nil
}

func (multiLevelManagerResolver) Validate(*ApproverConfig) []string { return nil }

func (multiLevelManagerResolver) PoolCandidates(*ApproverConfig) []PoolCandidateGroup {
	return nil
}

type deptResolver struct{}

func (deptResolver) Resolve(in ApproverResolveInput) ([]string, error) {
	if in.Identity == nil {
		return nil, fmt.Errorf("identity service not configured for dept-based candidate resolution")
	}
	var assignees []string
	set := map[string]bool{}
	for _, did := range in.Cfg.DeptIds {
		if did == "" {
			continue
		}
		members, err := in.Identity.GetUserIDsByDepartmentID(in.Ctx, in.TenantID, did)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve dept members of %s: %w", did, err)
		}
		for _, m := range members {
			assignees = addUnique(assignees, set, m)
		}
	}
	return assignees, nil
}

func (deptResolver) Validate(cfg *ApproverConfig) []string {
	if len(cfg.DeptIds) == 0 {
		return []string{"approver.deptIds is empty"}
	}
	return nil
}

func (deptResolver) PoolCandidates(cfg *ApproverConfig) []PoolCandidateGroup {
	return cfgPoolGroups(cfg)
}

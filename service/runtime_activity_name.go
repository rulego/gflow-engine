package service

import (
	"context"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/rulego/api/types"
)

// decorateCurrentActivityNames 为列表/单条实例装配非持久化字段 CurrentActivityName：
// 按 instance.CurrentActivity 节点 ID 从流程定义解析节点名。
//
// Process 关联缺失时（raw SQL 列表路径不 Preload）先按 processId 一次批量补齐
// （GetByIDs），避免逐实例查询；ToRuleChain 无缓存，逐实例解析 JSON 的开销
// 与宿主原实现同复杂度。解析失败或节点缺失回退节点 ID，无 CurrentActivity 置空。
func (s *RuntimeServiceImpl) decorateCurrentActivityNames(ctx context.Context, instances []*model.WfInstance) {
	if len(instances) == 0 {
		return
	}
	missingIDs := make(map[string]struct{})
	for _, inst := range instances {
		if inst != nil && inst.Process == nil && inst.ProcessID != "" {
			missingIDs[inst.ProcessID] = struct{}{}
		}
	}
	// 半装配场景（部分单测只填个别 DAO）：processDAO 缺失时跳过批量补齐，
	// 未 preload 的实例回退节点 ID，不 panic。
	if s.processDAO != nil && len(missingIDs) > 0 {
		ids := make([]string, 0, len(missingIDs))
		for id := range missingIDs {
			ids = append(ids, id)
		}
		if procs, err := s.processDAO.GetByIDs(ctx, ids); err == nil {
			procMap := make(map[string]*model.WfProcess, len(procs))
			for _, p := range procs {
				if p != nil {
					procMap[p.ID] = p
				}
			}
			for _, inst := range instances {
				if inst != nil && inst.Process == nil {
					inst.Process = procMap[inst.ProcessID]
				}
			}
		}
		// 补齐失败（DB 故障）不阻塞列表：下方解析对 Process==nil 回退节点 ID
	}
	// 同页同定义的行共享一次链解析：ToRuleChain 是全量 definition_json 反序列化，
	// 按 processID memoize 消除逐实例重复解析（分页 200 行共享 10 定义时的纯 CPU 放大）
	chainCache := make(map[string]*types.RuleChain, len(instances))
	for _, inst := range instances {
		inst.CurrentActivityName = resolveInstanceActivityNameCached(inst, chainCache)
	}
}

// resolveInstanceActivityNameCached 按 memoize 缓存解析当前节点名；
// cache 为 nil 时等价于直接解析。
func resolveInstanceActivityNameCached(instance *model.WfInstance, cache map[string]*types.RuleChain) string {
	if instance == nil || instance.CurrentActivity == nil || *instance.CurrentActivity == "" {
		return ""
	}
	nodeID := *instance.CurrentActivity
	if instance.Process == nil {
		return nodeID
	}
	var rc *types.RuleChain
	if cache != nil {
		rc = cache[instance.ProcessID]
	}
	if rc == nil {
		parsed, err := instance.Process.ToRuleChain()
		if err != nil || parsed == nil {
			return nodeID
		}
		rc = parsed
		if cache != nil {
			cache[instance.ProcessID] = rc
		}
	}
	if node, ok := rc.GetNode(nodeID); ok && node.Name != "" {
		return node.Name
	}
	return nodeID
}

// resolveInstanceActivityName 按实例 CurrentActivity 节点 ID 从关联流程定义解析节点名；
// 解析失败、节点缺失或无流程定义时回退节点 ID，保证列表"当前节点"列不空。
func resolveInstanceActivityName(instance *model.WfInstance) string {
	if instance == nil || instance.CurrentActivity == nil || *instance.CurrentActivity == "" {
		return ""
	}
	nodeID := *instance.CurrentActivity
	if instance.Process == nil {
		return nodeID
	}
	if rc, err := instance.Process.ToRuleChain(); err == nil && rc != nil {
		if node, ok := rc.GetNode(nodeID); ok && node.Name != "" {
			return node.Name
		}
	}
	return nodeID
}

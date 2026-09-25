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
	"errors"
	"fmt"
	"sync"

	"github.com/rulego/gflow-engine/service"
	"github.com/rulego/gflow-engine/types/enums"
)

// ErrApproverResolverAlreadyRegistered 审批人解析策略重复注册。
// 幂等注册方应以 errors.Is 判定此哨兵，而非匹配错误文本。
var ErrApproverResolverAlreadyRegistered = errors.New("approver resolver already registered")

// ApproverResolveInput 审批人解析入参。Identity 为宿主注入的身份服务，未注入
// 时为 nil（依赖身份服务的策略应返回 error 而非空成员）。Expression 为节点
// 预编译的表达式求值入口，仅发起人自选类策略消费，其余策略可忽略。
type ApproverResolveInput struct {
	Ctx        context.Context
	Identity   service.IdentityService
	TenantID   string
	Owner      string
	NodeID     string
	Cfg        *ApproverConfig
	Vars       map[string]interface{}
	Expression func(vars map[string]interface{}) (interface{}, error)
}

// ApproverResolver 审批人类型（approver.type）的解析策略。运行期解析、部署期
// 配置校验、组池语义三件事由同一实现提供；内置类型在包 init 注册，宿主经
// RegisterApproverResolver 注册自定义类型（岗位、外部通讯录、动态规则选人等）。
type ApproverResolver interface {
	// Resolve 解析成员用户 ID。身份服务故障、配置非法返回 error；空成员返回
	// (nil, nil)——两者语义不同：前者按节点失败处理，后者落 emptyApproverPolicy。
	Resolve(input ApproverResolveInput) ([]string, error)
	// Validate 部署期校验该类型必填配置，返回问题列表（空=通过）。
	Validate(cfg *ApproverConfig) []string
	// PoolCandidates 组池候选：非空时 single/any 模式建认领任务并按组落候选池
	// （role/dept 类）；非组池类型返回 nil。IDs 须已剔除空串。
	PoolCandidates(cfg *ApproverConfig) []PoolCandidateGroup
}

// PoolCandidateGroup 一组候选池实体，按 EntityType 落 wf_task_assignee。
type PoolCandidateGroup struct {
	EntityType string
	IDs        []string
}

var (
	approverResolverMu sync.RWMutex
	approverResolvers  = map[enums.CandidateType]ApproverResolver{}
)

// RegisterApproverResolver 注册审批人类型解析策略。同类型已注册（含内置类型）
// 返回 ErrApproverResolverAlreadyRegistered——先注册者生效，覆盖内置策略须在
// 自定义类型上做而不是复用其类型名。
func RegisterApproverResolver(t enums.CandidateType, r ApproverResolver) error {
	if r == nil {
		return fmt.Errorf("approver resolver for %s cannot be nil", t)
	}
	approverResolverMu.Lock()
	defer approverResolverMu.Unlock()
	if _, ok := approverResolvers[t]; ok {
		return fmt.Errorf("%w: %s", ErrApproverResolverAlreadyRegistered, t)
	}
	approverResolvers[t] = r
	return nil
}

// LookupApproverResolver 取类型的解析策略；未注册返回 nil（解析按不分配
// 审批人处理，部署期校验按类型不支持报错）。
func LookupApproverResolver(t enums.CandidateType) ApproverResolver {
	approverResolverMu.RLock()
	defer approverResolverMu.RUnlock()
	return approverResolvers[t]
}

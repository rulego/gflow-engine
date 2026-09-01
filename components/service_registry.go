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
	"sync"

	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/components/action"
	"github.com/sirupsen/logrus"
)

// ServiceFuncField 服务函数参数字段，字段名与取值对齐 rulego 的 ComponentFormField，
// 供前端动态字段渲染器直接消费。
type ServiceFuncField struct {
	Name         string         `json:"name"`  // 字段名
	Label        string         `json:"label"` // 显示名
	Desc         string         `json:"desc"`  // 描述
	Type         string         `json:"type"`  // string/int/bool/json
	DefaultValue any            `json:"defaultValue,omitempty"`
	Required     bool           `json:"required"`
	Component    map[string]any `json:"component,omitempty"` // UI 提示，如 {"type":"select","options":[...]}
}

// ServiceFuncDef 服务函数定义（元数据，不含实现）。
type ServiceFuncDef struct {
	Name     string             `json:"name"`     // 函数唯一名 = action.Functions key = 节点 functionName 取值
	Label    string             `json:"label"`    // 显示名
	Desc     string             `json:"desc"`     // 描述
	Category string             `json:"category"` // 分组，用于设计器分组展示
	Fields   []ServiceFuncField `json:"fields"`   // 参数 schema
	// Visible 租户可见性白名单：空=全部租户可见，非空=仅列出租户可见。
	// 仅在未注入 VisibilityProvider 时生效。
	Visible []string `json:"-"`
}

// VisibilityProvider 租户可见性策略（弱隔离）。
// 上层应用可注入对接自身权限系统的实现；不注入时回退到 Visible 白名单或全可见。
type VisibilityProvider interface {
	Visible(tenantID, funcName string) bool
}

// AllowAllVisibility 默认可见性策略：任何租户可见任何函数。
type AllowAllVisibility struct{}

func (AllowAllVisibility) Visible(string, string) bool { return true }

// ServiceFunc 服务任务函数注册项：元数据 + 实现。
// 宿主（含外部集成方）在启动期经 ComponentDeps.ServiceFuncs 一次性注册，
// 或逐个调 Services.Register；两条路径最终等价。
type ServiceFunc struct {
	Def ServiceFuncDef
	Fn  func(ctx types.RuleContext, msg types.RuleMsg)
}

// ServiceRegistry 服务函数注册表。
// 函数实现委托上游 action.Functions（运行时由 FunctionsNode 查找调用），
// 本地 catalog 只存元数据供设计器消费，不持久化。
type ServiceRegistry struct {
	catalog    map[string]ServiceFuncDef
	order      []string // 保持注册顺序
	visibility VisibilityProvider
	mu         sync.RWMutex
}

// Services 进程级服务函数注册表。
var Services = &ServiceRegistry{visibility: AllowAllVisibility{}}

// SetVisibilityProvider 注入租户可见性策略（弱隔离）。
// 传 nil 等价于恢复默认 AllowAllVisibility。线程安全，可在运行期切换。
func (r *ServiceRegistry) SetVisibilityProvider(p VisibilityProvider) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if p == nil {
		r.visibility = AllowAllVisibility{}
	} else {
		r.visibility = p
	}
}

// Register 注册服务函数：实现委托 action.Functions，元数据存入 catalog。
// 同名重复注册会覆盖（打 Warn 留痕，不视为错误——支持宿主启动期幂等重注册）。
func (r *ServiceRegistry) Register(def ServiceFuncDef, fn func(ctx types.RuleContext, msg types.RuleMsg)) {
	action.Functions.Register(def.Name, fn, def.Label, def.Desc)

	r.mu.Lock()
	if r.catalog == nil {
		r.catalog = make(map[string]ServiceFuncDef)
	}
	_, exists := r.catalog[def.Name]
	if !exists {
		r.order = append(r.order, def.Name)
	}
	r.catalog[def.Name] = def
	r.mu.Unlock()

	if exists {
		// 解锁后再打日志，避免持锁做 IO
		logrus.WithField("funcName", def.Name).Warn("service function re-registered, overwriting previous definition")
	}
}

// Unregister 注销服务函数：同步清除 catalog 元数据与 action.Functions 运行时实现。
// 主要供测试隔离与宿主动态下线使用；注销不存在的名称是静默 no-op。
func (r *ServiceRegistry) Unregister(name string) {
	action.Functions.UnRegister(name)

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.catalog[name]; !exists {
		return
	}
	delete(r.catalog, name)
	for i, n := range r.order {
		if n == name {
			r.order = append(r.order[:i], r.order[i+1:]...)
			break
		}
	}
}

// List 返回全部已注册函数定义（按注册顺序，不做租户过滤）。
func (r *ServiceRegistry) List() []ServiceFuncDef {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]ServiceFuncDef, 0, len(r.order))
	for _, name := range r.order {
		if def, ok := r.catalog[name]; ok {
			out = append(out, def)
		}
	}
	return out
}

// ListForTenant 返回对指定租户可见的函数定义（按注册顺序）。
// 注入 VisibilityProvider 时以其为准；否则用 Visible 白名单（空=全可见）。
// 必须在锁内把定义值拷出再过滤：Register 会并发写 catalog，
// 锁外读 map 会触发 Go runtime 的并发读写 panic。
func (r *ServiceRegistry) ListForTenant(tenantID string) []ServiceFuncDef {
	r.mu.RLock()
	visibility := r.visibility
	snapshot := make([]ServiceFuncDef, 0, len(r.order))
	for _, name := range r.order {
		if def, ok := r.catalog[name]; ok {
			snapshot = append(snapshot, def)
		}
	}
	r.mu.RUnlock()

	out := make([]ServiceFuncDef, 0, len(snapshot))
	for _, def := range snapshot {
		if !r.visibleFor(visibility, tenantID, def) {
			continue
		}
		out = append(out, def)
	}
	return out
}

// visibleFor 判定单个函数对单个租户是否可见。
// 读锁外调用，visibility/def 按值传入，无竞态。
func (r *ServiceRegistry) visibleFor(visibility VisibilityProvider, tenantID string, def ServiceFuncDef) bool {
	if visibility == nil {
		visibility = AllowAllVisibility{}
	}
	// provider 注入时以此为准，忽略 def.Visible 字段
	if _, isDefault := visibility.(AllowAllVisibility); !isDefault {
		return visibility.Visible(tenantID, def.Name)
	}
	// 回退到 def.Visible：空=全可见
	if len(def.Visible) == 0 {
		return true
	}
	for _, t := range def.Visible {
		if t == tenantID {
			return true
		}
	}
	return false
}

// Get 按 name 查找函数定义（不做租户过滤），未找到返回 false。
func (r *ServiceRegistry) Get(name string) (ServiceFuncDef, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	def, ok := r.catalog[name]
	return def, ok
}

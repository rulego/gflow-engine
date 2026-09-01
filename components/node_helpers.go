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
	"encoding/json"
	"fmt"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"

	"github.com/rulego/gflow-engine/service"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/components/base"
	"github.com/sirupsen/logrus"
)

// metaValue 从消息 metadata 读取 key；metadata 为 nil 时返回空串。
// rulego 的 GetMetadata() 原样返回 m.Metadata（可能为 nil），直接链式
// GetValue 会因 nil 接收者 panic——组件内统一经此函数读取。
func metaValue(msg types.RuleMsg, key string) string {
	if msg.Metadata == nil {
		return ""
	}
	return msg.Metadata.GetValue(key)
}

// recoverNodePanic 把节点 OnMsg 内的 panic 转为 Failure 边并带堆栈记日志。
// rulego 引擎对节点 OnMsg 是裸调用（无 recover），宿主经 WithServiceFuncs 注入的
// 服务函数一旦 panic 会击穿引擎所在进程——可嵌入引擎必须自行兜底。
// 各 BPM 节点必须在 OnMsg 首行 `defer recoverNodePanic(ctx, msg, <type>, <id>)`。
func recoverNodePanic(ctx types.RuleContext, msg types.RuleMsg, nodeType, nodeID string) {
	if r := recover(); r != nil {
		logrus.WithFields(logrus.Fields{
			"node_type": nodeType,
			"node_id":   nodeID,
			"panic":     r,
			"stack":     string(debug.Stack()),
		}).Error("BPM node panic recovered; routed to Failure")
		ctx.TellFailure(msg, fmt.Errorf("%s node %s panicked: %v", nodeType, nodeID, r))
	}
}

// selfID 返回节点定义 ID，为空时回退 fallback（通常为节点类型名）。
func selfID(def types.RuleNode, fallback string) string {
	if def.Id != "" {
		return def.Id
	}
	return fallback
}

// selfName 返回节点定义名称，为空时回退节点 ID。
func selfName(def types.RuleNode) string {
	if def.Name != "" {
		return def.Name
	}
	return def.Id
}

// extractVariables 提取流程业务变量（userTask/aiAgent 等节点共用）。
// 业务变量在 env["msg"]（解析后的 msg.Data）；只取扁平业务变量，不含 id/ts/metadata
// 信封（信封里有租户/用户等内部信息，不应进入任务变量或 LLM 提示词）。
// 返回浅拷贝：env 的 msg map 在节点间共享，调用方写入的缓存键
// （如 _sequentialAssignees）不得污染共享 map。
func extractVariables(ctx types.RuleContext, msg types.RuleMsg) map[string]interface{} {
	env := base.NodeUtils.GetEvn(ctx, msg)
	business, ok := env[types.MsgKey].(map[string]interface{})
	if !ok {
		return map[string]interface{}{}
	}
	copied := make(map[string]interface{}, len(business))
	for k, v := range business {
		copied[k] = v
	}
	return copied
}

// keyedMutex 按 key 串行化的互斥锁组，无持锁者时自动回收条目。
type keyedMutex struct {
	mu    sync.Mutex
	locks map[string]*keyedLockEntry
}

type keyedLockEntry struct {
	mu      sync.Mutex
	waiters int
}

// Lock 获取 key 对应的锁，返回解锁函数。
func (k *keyedMutex) Lock(key string) func() {
	k.mu.Lock()
	if k.locks == nil {
		k.locks = make(map[string]*keyedLockEntry)
	}
	e := k.locks[key]
	if e == nil {
		e = &keyedLockEntry{}
		k.locks[key] = e
	}
	e.waiters++
	k.mu.Unlock()

	e.mu.Lock()
	return func() {
		k.mu.Lock()
		e.waiters--
		if e.waiters == 0 {
			delete(k.locks, key)
		}
		k.mu.Unlock()
		e.mu.Unlock()
	}
}

// taskOpMutex 按实例ID+节点ID 串行化任务创建路径，防止并发重入重复建任务。
// 仅进程内生效；多实例部署需上层注入分布式锁（见 README 扩展点）。
var taskOpMutex keyedMutex

// serializeVariables 把变量 map 序列化为 JSON 字符串（剔除 data 信封键，不修改原 map）。
// 空 map 或序列化失败返回 "{}"。
func serializeVariables(variables map[string]interface{}) string {
	if len(variables) == 0 {
		return "{}"
	}
	// Copy map to avoid mutating the caller's map
	copied := make(map[string]interface{}, len(variables))
	for k, v := range variables {
		copied[k] = v
	}
	delete(copied, types.DataKey)
	data, err := json.Marshal(copied)
	if err != nil {
		return "{}"
	}
	return string(data)
}

// formKeyPtr 返回 formKey 的指针；空字符串返回 nil（对应 wf_task.form_key 可空列）。
func formKeyPtr(formKey string) *string {
	if formKey == "" {
		return nil
	}
	return &formKey
}

// toStringSlice 将任意 interface{} 转换为 []string
func toStringSlice(v interface{}) []string {
	switch vv := v.(type) {
	case nil:
		return nil
	case []string:
		return vv
	case []interface{}:
		res := make([]string, 0, len(vv))
		for _, it := range vv {
			res = append(res, fmt.Sprintf("%v", it))
		}
		return res
	case string:
		if vv == "" {
			return nil
		}
		// 逗号分隔
		parts := strings.Split(vv, ",")
		res := make([]string, 0, len(parts))
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p != "" {
				res = append(res, p)
			}
		}
		return res
	default:
		return nil
	}
}

// toInt 将任意 interface{} 转换为 int，失败返回默认值
func toInt(v interface{}, def int) int {
	switch vv := v.(type) {
	case nil:
		return def
	case int:
		return vv
	case int32:
		return int(vv)
	case int64:
		return int(vv)
	case float64:
		return int(vv)
	case float32:
		return int(vv)
	case string:
		if vv == "" {
			return def
		}
		if i, err := strconv.Atoi(vv); err == nil {
			return i
		}
		return def
	default:
		return def
	}
}

// addUnique 当 v 非空且未出现在 set 中时追加到 list，并同步去重集合。
func addUnique(list []string, set map[string]bool, v string) []string {
	if v == "" || set[v] {
		return list
	}
	set[v] = true
	return append(list, v)
}

// operatorFromCtx 取当前操作人 userId，ctx 无 Actor 时为空。
func operatorFromCtx(ctx context.Context) string {
	if u := service.GetUserFromCtx(ctx); u != nil {
		return u.UserID
	}
	return ""
}

// systemActorForTenant 构造带任务租户的系统操作人（节点自动创建任务/落库候选人用）：
// AddCandidates 等接口按 actor.TenantID 取租户，SystemActor 本身不带租户。
func systemActorForTenant(tenantID string) service.Actor {
	actor := service.SystemActor()
	actor.TenantID = tenantID
	return actor
}

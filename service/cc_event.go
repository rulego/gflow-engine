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
	"time"
)

// CCEvent 描述一次抄送事件。
//
// 引擎在成功创建一条 CC 任务（task_type=ccTask, approval_type=cc）后构造此事件
// 并派发给已注册的 CCTaskCreatedListener。上层应用（如 gflow）通过
// WorkflowEngineBuilder.SetCCTaskCreatedListener 注册监听器，
// 把它转译成 notifications 等用户视图层行为。
type CCEvent struct {
	TaskID            string    // wf_task.id
	ProcessInstanceID string    // 流程实例 ID
	ProcessID         string    // 流程定义 ID（应用层可借此回查流程名）
	TenantID          string    // 租户 ID
	AssigneeUserID    string    // 抄送对象用户 ID
	TaskName          string    // 抄送节点名称（用于通知展示）
	CreatedAt         time.Time // 抄送时间
}

// CCTaskCreatedListener 抄送任务创建监听器。
//
// 实现要求：
//   - 快速返回（长 IO 应自行 goroutine 化）
//   - 内部吞掉错误并打日志（错误不会冒泡到引擎主流程，CCTaskNode 会对 panic 做 recover）
type CCTaskCreatedListener func(ctx context.Context, evt CCEvent)

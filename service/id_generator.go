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
	"github.com/google/uuid"
	"sync"
)

// IDGenerator ID生成器接口
type IDGenerator interface {
	// GenerateID 生成唯一ID
	GenerateID() string
	// GenerateInstanceID 生成流程实例ID
	GenerateInstanceID() string
	// GenerateTaskID 生成任务ID
	GenerateTaskID() string
	// GenerateProcessID 生成流程定义ID
	GenerateProcessID() string
	// GenerateBusinessKey 生成业务键
	GenerateBusinessKey() string
}

// idGenerator ID生成器实现（全部基于 UUID）
type idGenerator struct {
}

// NewIDGenerator 创建新的ID生成器，所有 ID 均为纯 UUID。
func NewIDGenerator() IDGenerator {
	return &idGenerator{}
}

// GenerateID 生成唯一ID
func (g *idGenerator) GenerateID() string {
	return uuid.New().String()
}

// GenerateInstanceID 生成流程实例ID
func (g *idGenerator) GenerateInstanceID() string {
	return uuid.New().String()
}

// GenerateTaskID 生成任务ID
func (g *idGenerator) GenerateTaskID() string {
	return uuid.New().String()
}

// GenerateProcessID 生成流程定义ID
func (g *idGenerator) GenerateProcessID() string {
	return uuid.New().String()
}

// GenerateBusinessKey 生成业务键，固定携带 "BIZ_" 前缀 + UUID，
// 供业务系统将自有单据号以外的场景做唯一关联。
func (g *idGenerator) GenerateBusinessKey() string {
	return "BIZ_" + uuid.New().String()
}

// DefaultIDGenerator 进程级默认 ID 生成器，仅作为无引擎引用处的兜底
// （如事件派发的 EventID 生成）。构建期不会被隐式替换；宿主若希望事件 ID
// 也跟随自定义生成器，可显式调用 SetDefaultIDGenerator。并发安全。
var DefaultIDGenerator = NewIDGenerator()

var defaultIDGenMu sync.RWMutex

// SetDefaultIDGenerator 显式替换进程级默认 ID 生成器（供宿主可选调用；
// Builder.SetIDGenerator 不会触碰该全局值）。
func SetDefaultIDGenerator(generator IDGenerator) {
	defaultIDGenMu.Lock()
	defer defaultIDGenMu.Unlock()
	DefaultIDGenerator = generator
}

// getDefaultIDGenerator 并发安全地读取当前默认生成器。
func getDefaultIDGenerator() IDGenerator {
	defaultIDGenMu.RLock()
	defer defaultIDGenMu.RUnlock()
	return DefaultIDGenerator
}

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
	"errors"
	"fmt"
	"gorm.io/gorm"
	"sync"
)

// ErrDialectAlreadyRegistered 重复注册同名方言提供者。幂等注册方应以此哨兵
// 判定（errors.Is），而非匹配错误文本。
var ErrDialectAlreadyRegistered = errors.New("dialect provider already registered")

// DialectProvider 数据库方言提供者接口
// 用于创建特定数据库的 GORM Dialector
type DialectProvider interface {
	// GetName 获取方言名称
	GetName() string

	// CreateDialector 创建数据库方言处理器
	CreateDialector(dsn string) (gorm.Dialector, error)

	// GetSupportedDrivers 获取支持的驱动名称列表
	GetSupportedDrivers() []string
}

// DialectRegistry 方言注册表
// 管理所有已注册的数据库方言提供者
type DialectRegistry struct {
	providers map[string]DialectProvider
	mutex     sync.RWMutex
}

var (
	// 全局方言注册表实例
	globalRegistry = &DialectRegistry{
		providers: make(map[string]DialectProvider),
	}
)

// GetGlobalRegistry 获取全局方言注册表
func GetGlobalRegistry() *DialectRegistry {
	return globalRegistry
}

// RegisterDialectProvider 注册方言提供者
func (r *DialectRegistry) RegisterDialectProvider(provider DialectProvider) error {
	if provider == nil {
		return fmt.Errorf("dialect provider cannot be nil")
	}

	name := provider.GetName()
	if name == "" {
		return fmt.Errorf("dialect provider name cannot be empty")
	}

	r.mutex.Lock()
	defer r.mutex.Unlock()

	if _, exists := r.providers[name]; exists {
		return fmt.Errorf("%w: %s", ErrDialectAlreadyRegistered, name)
	}

	r.providers[name] = provider
	return nil
}

// GetDialectProvider 获取方言提供者
func (r *DialectRegistry) GetDialectProvider(name string) (DialectProvider, bool) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	provider, exists := r.providers[name]
	return provider, exists
}

// CreateDialector 创建数据库方言处理器
func (r *DialectRegistry) CreateDialector(driverName, dsn string) (gorm.Dialector, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	for _, provider := range r.providers {
		supportedDrivers := provider.GetSupportedDrivers()
		for _, supportedDriver := range supportedDrivers {
			if supportedDriver == driverName {
				return provider.CreateDialector(dsn)
			}
		}
	}

	return nil, fmt.Errorf("unsupported database driver: %s", driverName)
}

// GetRegisteredProviders 获取所有已注册的方言提供者
func (r *DialectRegistry) GetRegisteredProviders() []string {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	names := make([]string, 0, len(r.providers))
	for name := range r.providers {
		names = append(names, name)
	}
	return names
}

// GetSupportedDrivers 获取所有支持的驱动名称
func (r *DialectRegistry) GetSupportedDrivers() []string {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	driverSet := make(map[string]bool)
	for _, provider := range r.providers {
		for _, driver := range provider.GetSupportedDrivers() {
			driverSet[driver] = true
		}
	}

	drivers := make([]string, 0, len(driverSet))
	for driver := range driverSet {
		drivers = append(drivers, driver)
	}
	return drivers
}

// RegisterDialectProvider 注册方言提供者到全局注册表
func RegisterDialectProvider(provider DialectProvider) error {
	return globalRegistry.RegisterDialectProvider(provider)
}

// CreateDialector 使用全局注册表创建数据库方言处理器
func CreateDialector(driverName, dsn string) (gorm.Dialector, error) {
	return globalRegistry.CreateDialector(driverName, dsn)
}

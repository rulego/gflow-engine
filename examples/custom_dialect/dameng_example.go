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

// 本示例演示如何为引擎注册自定义数据库方言（达梦 / 人大金仓）。
// 实际接入步骤：
//  1. 引入对应 GORM 驱动（如 dm.Open(dsn)、kingbase 的官方驱动包）；
//  2. 在 CreateDialector 中返回真实 Dialector，替换示例中的错误返回；
//  3. 通过 Builder.SetDialectProvider 或 service.RegisterDialectProvider 注册。
package main

import (
	"context"
	"fmt"

	"github.com/rulego/gflow-engine/config"
	"github.com/rulego/gflow-engine/service"
	"gorm.io/gorm"
)

// DamengDialectProvider 达梦数据库方言提供者示例
type DamengDialectProvider struct{}

// GetName 返回方言名称 "dameng"，与配置中的 driver 字段对应
func (d *DamengDialectProvider) GetName() string {
	return "dameng"
}

// CreateDialector 创建达梦数据库的 GORM Dialector；dsn 为数据源名称。
// 示例未引入真实驱动，接入时改为 return dm.Open(dsn), nil。
func (d *DamengDialectProvider) CreateDialector(dsn string) (gorm.Dialector, error) {
	return nil, fmt.Errorf("dameng driver not implemented in this example")
}

// GetSupportedDrivers 返回可识别的驱动别名列表
func (d *DamengDialectProvider) GetSupportedDrivers() []string {
	return []string{"dm", "dameng"}
}

// KingbaseDialectProvider 人大金仓数据库方言提供者示例
type KingbaseDialectProvider struct{}

// GetName 返回方言名称 "kingbase"，与配置中的 driver 字段对应
func (k *KingbaseDialectProvider) GetName() string {
	return "kingbase"
}

// CreateDialector 创建人大金仓数据库的 GORM Dialector；dsn 为数据源名称。
// 示例未引入真实驱动，接入时替换为驱动提供的 Open 实现。
func (k *KingbaseDialectProvider) CreateDialector(dsn string) (gorm.Dialector, error) {
	return nil, fmt.Errorf("kingbase driver not implemented in this example")
}

// GetSupportedDrivers 返回可识别的驱动别名列表
func (k *KingbaseDialectProvider) GetSupportedDrivers() []string {
	return []string{"kingbase", "kdb"}
}

// 示例1：使用构建器注册自定义方言
func exampleWithBuilder() {
	fmt.Println("=== 示例1：使用构建器注册自定义方言 ===")

	cfg := &config.Config{
		Database: &config.DatabaseConfig{
			Driver: "dameng",
			Dsn:    "dm://username:password@localhost:5236/database",
		},
	}

	engine, err := service.NewWorkflowEngineBuilder().
		SetName("DamengWorkflowEngine").
		SetConfig(cfg).
		SetDialectProvider(&DamengDialectProvider{}).   // 注册达梦数据库方言
		SetDialectProvider(&KingbaseDialectProvider{}). // 注册人大金仓方言
		Build()

	if err != nil {
		fmt.Printf("构建引擎失败: %v\n", err)
		return
	}

	fmt.Printf("成功创建工作流引擎: %s\n", engine.GetName())

	// 未注册真实达梦/金仓驱动，Start 预期失败，仅演示方言装配路径
	if err := engine.Start(context.Background()); err != nil {
		fmt.Printf("启动引擎失败（预期的）: %v\n", err)
	}
}

// 示例2：直接注册到全局注册表
func exampleWithGlobalRegistry() {
	fmt.Println("\n=== 示例2：直接注册到全局注册表 ===")

	if err := service.RegisterDialectProvider(&DamengDialectProvider{}); err != nil {
		fmt.Printf("注册达梦方言失败: %v\n", err)
	} else {
		fmt.Println("成功注册达梦数据库方言")
	}

	if err := service.RegisterDialectProvider(&KingbaseDialectProvider{}); err != nil {
		fmt.Printf("注册人大金仓方言失败: %v\n", err)
	} else {
		fmt.Println("成功注册人大金仓数据库方言")
	}

	registry := service.GetGlobalRegistry()
	providers := registry.GetRegisteredProviders()
	fmt.Printf("已注册的方言提供者: %v\n", providers)

	drivers := registry.GetSupportedDrivers()
	fmt.Printf("支持的数据库驱动: %v\n", drivers)
}

// 示例3：测试方言创建
func exampleTestDialectCreation() {
	fmt.Println("\n=== 示例3：测试方言创建 ===")

	registry := service.GetGlobalRegistry()

	testCases := []struct {
		driver string
		dsn    string
	}{
		{"mysql", "user:password@tcp(localhost:3306)/dbname"},
		{"postgres", "host=localhost user=username password=password dbname=mydb port=5432"},
		{"dameng", "dm://username:password@localhost:5236/database"},
		{"kingbase", "kingbase://username:password@localhost:54321/database"},
		{"unsupported", "some://dsn"},
	}

	for _, tc := range testCases {
		fmt.Printf("测试驱动 '%s':\n", tc.driver)
		dialector, err := registry.CreateDialector(tc.driver, tc.dsn)
		if err != nil {
			fmt.Printf("  失败: %v\n", err)
		} else {
			fmt.Printf("  成功创建方言处理器: %T\n", dialector)
		}
	}
}

func main() {
	fmt.Println("自定义数据库方言示例")
	fmt.Println("====================")

	exampleWithBuilder()
	exampleWithGlobalRegistry()
	exampleTestDialectCreation()

	fmt.Println("\n注意：")
	fmt.Println("1. 本示例中的达梦和人大金仓驱动是模拟实现，实际使用时需要引入真实的驱动包")
	fmt.Println("2. 需要根据具体数据库的 GORM 驱动文档来实现 CreateDialector 方法")
	fmt.Println("3. 建议在 init() 函数中注册自定义方言提供者")
}

# 自定义数据库方言示例

本示例展示如何为工作流引擎添加自定义数据库方言支持，例如国产数据库（达梦、人大金仓等）。

## 使用场景

当您需要使用本库不直接支持的数据库时，可以通过实现 `DialectProvider` 接口来添加自定义数据库支持。

## 实现步骤

### 1. 实现 DialectProvider 接口

```go
package main

import (
    "github.com/rulego/gflow-engine/service"
    "gorm.io/gorm"
    // 假设这是达梦数据库的 GORM 驱动
    "dm.gorm.io/driver/dm"
)

// DamengDialectProvider 达梦数据库方言提供者
type DamengDialectProvider struct{}

// GetName 获取方言名称
func (d *DamengDialectProvider) GetName() string {
    return "dameng"
}

// CreateDialector 创建达梦数据库方言处理器
func (d *DamengDialectProvider) CreateDialector(dsn string) (gorm.Dialector, error) {
    return dm.Open(dsn), nil
}

// GetSupportedDrivers 获取支持的驱动名称列表
func (d *DamengDialectProvider) GetSupportedDrivers() []string {
    return []string{"dm", "dameng"}
}
```

### 2. 注册自定义方言提供者

#### 方式一：使用构建器注册

```go
package main

import (
    "context"
    "github.com/rulego/gflow-engine/config"
    "github.com/rulego/gflow-engine/service"
)

func main() {
    // 创建配置
    cfg := &config.Config{
        Database: &config.DatabaseConfig{
            Driver: "dameng",
            Dsn:    "dm://username:password@localhost:5236/database",
        },
    }

    // 使用构建器创建工作流引擎
    engine, err := service.NewWorkflowEngineBuilder().
        SetName("MyWorkflowEngine").
        SetConfig(cfg).
        SetDialectProvider(&DamengDialectProvider{}). // 注册自定义方言
        Build()
    
    if err != nil {
        panic(err)
    }

    // 启动引擎
    if err := engine.Start(context.Background()); err != nil {
        panic(err)
    }

    // 使用引擎...
}
```

#### 方式二：直接注册到全局注册表

```go
package main

import (
    "context"
    "github.com/rulego/gflow-engine/config"
    "github.com/rulego/gflow-engine/service"
)

func init() {
    // 在 init 函数中注册自定义方言
    service.RegisterDialectProvider(&DamengDialectProvider{})
}

func main() {
    // 创建配置
    cfg := &config.Config{
        Database: &config.DatabaseConfig{
            Driver: "dameng",
            Dsn:    "dm://username:password@localhost:5236/database",
        },
    }

    // 创建工作流引擎
    engine := service.NewWorkflowEngine("MyWorkflowEngine")
    engine.(*service.WorkflowEngineImpl).SetConfig(cfg)

    // 启动引擎
    if err := engine.Start(context.Background()); err != nil {
        panic(err)
    }

    // 使用引擎...
}
```

## 支持的数据库

### 内置支持

- PostgreSQL (`postgres`, `postgresql`)
- MySQL (`mysql`)

SQLite 未内置（测试场景请自行注册纯 Go 方言，如 `github.com/glebarez/sqlite`；
示例侧的现成实现见 [examples/internal/demo](../internal/demo/demo.go)）。

### 可扩展支持

通过实现 `DialectProvider` 接口，您可以添加对任何数据库的支持，包括但不限于：

- 达梦数据库 (DM)
- 人大金仓 (KingbaseES)
- 神通数据库 (Oscar)
- 南大通用 (GBase)
- 华为 GaussDB
- 阿里云 PolarDB
- 腾讯云 TDSQL
- 等等...

## 注意事项

1. 确保您的自定义数据库驱动与 GORM 兼容
2. 方言提供者的名称应该是唯一的
3. 支持的驱动名称列表应该包含所有可能的驱动别名
4. 建议在 `init()` 函数中注册方言提供者，以确保在使用前已经注册

## 错误处理

如果指定的数据库驱动不被支持，系统会返回类似以下的错误：

```
failed to create dialector for driver 'unsupported_driver': unsupported database driver: unsupported_driver
```

此时您需要：

1. 检查驱动名称是否正确
2. 确认相应的方言提供者是否已注册
3. 验证方言提供者的 `GetSupportedDrivers()` 方法是否返回了正确的驱动名称
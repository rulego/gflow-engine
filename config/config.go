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

package config

import (
	"fmt"
	"time"
)

// Config 应用配置。
// 引擎以库形态嵌入宿主应用：配置结构由宿主构造后经
// service.WorkflowEngineBuilder.SetConfig 注入；从文件/环境变量加载配置
// 属宿主职责，引擎不做：内置的配置加载对未绑定的环境变量会静默失效，
// 且失败时 panic，不适合库形态的引擎。

type Config struct {
	Database *DatabaseConfig `json:"database" yaml:"database"`
	Server   *ServerConfig   `json:"server" yaml:"server"`
	Logging  *LoggingConfig  `json:"logging" yaml:"logging"`
}

// DatabaseConfig 数据库配置
type DatabaseConfig struct {
	Driver string `json:"driver" yaml:"driver"`
	Dsn    string `json:"dsn" yaml:"dsn"`
	// MaxOpenConns 最大打开连接数；<=0 时由引擎取驱动默认值
	MaxOpenConns int `json:"max_open_conns" yaml:"max_open_conns"`
	// MaxIdleConns 最大空闲连接数；<=0 时由引擎取驱动默认值
	MaxIdleConns int `json:"max_idle_conns" yaml:"max_idle_conns"`
	// ConnMaxLifetime 连接最长复用时长；0 表示不限制。云数据库/代理会在服务端
	// 静默掐断长连接，生产环境建议显式设置（如 30m）。
	ConnMaxLifetime time.Duration `json:"conn_max_lifetime" yaml:"conn_max_lifetime"`
}

// ServerConfig 服务器配置（引擎本体不启 HTTP 服务；供宿主复用同一份配置文件）
type ServerConfig struct {
	Host         string        `json:"host" yaml:"host"`
	Port         int           `json:"port" yaml:"port"`
	ReadTimeout  time.Duration `json:"read_timeout" yaml:"read_timeout"`
	WriteTimeout time.Duration `json:"write_timeout" yaml:"write_timeout"`
}

// LoggingConfig 日志配置（引擎仅消费 Level；其余字段供宿主日志设施使用）
type LoggingConfig struct {
	Level      string `json:"level" yaml:"level"`
	Format     string `json:"format" yaml:"format"`
	Output     string `json:"output" yaml:"output"`
	FilePath   string `json:"file_path" yaml:"file_path"`
	MaxSize    int    `json:"max_size" yaml:"max_size"`
	MaxBackups int    `json:"max_backups" yaml:"max_backups"`
	MaxAge     int    `json:"max_age" yaml:"max_age"`
	Compress   bool   `json:"compress" yaml:"compress"`
}

// DefaultConfig 返回一份可用的默认配置（本地 PostgreSQL）。
// 仅作开发起点：生产部署必须替换 DSN 并按需调整连接池参数。
func DefaultConfig() *Config {
	return &Config{
		Database: &DatabaseConfig{
			Driver:          "postgres",
			Dsn:             "host=127.0.0.1 user=postgres password=postgres dbname=gflow port=5432 sslmode=disable TimeZone=UTC",
			MaxOpenConns:    25,
			MaxIdleConns:    5,
			ConnMaxLifetime: 30 * time.Minute,
		},
		Server: &ServerConfig{
			Host:         "0.0.0.0",
			Port:         8080,
			ReadTimeout:  time.Second * 30,
			WriteTimeout: time.Second * 30,
		},
		Logging: &LoggingConfig{
			Level:      "info",
			Format:     "json",
			Output:     "stdout",
			FilePath:   "./logs/app.log",
			MaxSize:    100,
			MaxBackups: 3,
			MaxAge:     7,
			Compress:   true,
		},
	}
}

// Validate 验证配置：引擎启动的最小集合是数据库三要素。
func (c *Config) Validate() error {
	if c == nil {
		return fmt.Errorf("config is nil")
	}
	if c.Database == nil {
		return fmt.Errorf("database config is required")
	}
	if c.Database.Driver == "" {
		return fmt.Errorf("database driver is required")
	}
	if c.Database.Dsn == "" {
		return fmt.Errorf("database dsn is required")
	}
	return nil
}

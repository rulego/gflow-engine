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
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// PostgresDialectProvider PostgreSQL方言提供者
type PostgresDialectProvider struct{}

func (p *PostgresDialectProvider) GetName() string {
	return "postgres"
}

func (p *PostgresDialectProvider) CreateDialector(dsn string) (gorm.Dialector, error) {
	return postgres.Open(dsn), nil
}

func (p *PostgresDialectProvider) GetSupportedDrivers() []string {
	return []string{"postgres", "postgresql"}
}

// MySQLDialectProvider MySQL方言提供者
type MySQLDialectProvider struct{}

func (m *MySQLDialectProvider) GetName() string {
	return "mysql"
}

func (m *MySQLDialectProvider) CreateDialector(dsn string) (gorm.Dialector, error) {
	return mysql.Open(dsn), nil
}

func (m *MySQLDialectProvider) GetSupportedDrivers() []string {
	return []string{"mysql"}
}

// SQLite 方言当前不提供；需要时按 DialectProvider 接口自行实现并注册。

// init 自动注册默认的方言提供者（postgres / mysql）
func init() {
	_ = RegisterDialectProvider(&PostgresDialectProvider{})
	_ = RegisterDialectProvider(&MySQLDialectProvider{})
}

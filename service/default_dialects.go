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
	"fmt"

	gomysql "github.com/go-sql-driver/mysql"
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
	cfg, err := mysqlDSNWithFoundRows(dsn)
	if err != nil {
		return nil, err
	}
	return mysql.Open(cfg.FormatDSN()), nil
}

func (m *MySQLDialectProvider) GetSupportedDrivers() []string {
	return []string{"mysql"}
}

// mysqlDSNWithFoundRows 解析 DSN 并强制 clientFoundRows。MySQL 默认 affected
// rows 只计值发生变化的行，而 DAO 层按 matched rows 语义用 RowsAffected==0
// 判定行不存在——不开此参数，幂等重试/重复置同一状态会被误报 not found
// （PG 天然计 matched rows，无此差异）。
func mysqlDSNWithFoundRows(dsn string) (*gomysql.Config, error) {
	cfg, err := gomysql.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse mysql dsn: %w", err)
	}
	cfg.ClientFoundRows = true
	return cfg, nil
}

// SQLite 方言当前不提供；需要时按 DialectProvider 接口自行实现并注册。

// init 自动注册默认的方言提供者（postgres / mysql）
func init() {
	_ = RegisterDialectProvider(&PostgresDialectProvider{})
	_ = RegisterDialectProvider(&MySQLDialectProvider{})
}

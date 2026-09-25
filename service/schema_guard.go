// 启动期校验引擎按毫秒比较的时间列精度。收回守卫按 ended_at 分序、按
// created_at>=ended_at 圈前沿终止集，列精度低于毫秒时同秒两票无法分序——
// 秒级列是早期 DDL 快照的中间态，规范 DDL 为 DATETIME(3)，探到即拒绝启动。

package service

import (
	"fmt"

	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

// schemaTimePrecisionTargets 引擎按毫秒比较的列全集（表 → 时间列）。
// 与引擎建表脚本的 DATETIME(3) 基准对应；新增毫秒时序依赖时同步补这里。
var schemaTimePrecisionTargets = map[string][]string{
	"wf_task":    {"created_at", "ended_at", "updated_at"},
	"wf_hi_task": {"created_at", "ended_at", "updated_at"},
}

// schemaPrecisionProbe information_schema 单列探测结果。Precision 用指针：
// 国产兼容实现对 DATETIME_PRECISION 可能返回 NULL，扫成 int 会变 0 被误判
// 成秒级，读不到精度按缺失（告警）处理而非拦截。
type schemaPrecisionProbe struct {
	Table     string `gorm:"column:table_name"`
	Column    string `gorm:"column:column_name"`
	Precision *int   `gorm:"column:datetime_precision"`
}

// SchemaPrecisionGuard 启动期列精度探测守卫。
type SchemaPrecisionGuard struct {
	db *gorm.DB
}

// NewSchemaPrecisionGuard 创建守卫；db 为空（如单测装配无库引擎）时探测跳过。
func NewSchemaPrecisionGuard(db *gorm.DB) *SchemaPrecisionGuard {
	return &SchemaPrecisionGuard{db: db}
}

// Validate 执行探测。低精度列返回错误拒绝启动（存量库先跑毫秒精度补丁再发版）；
// 列缺失只告警，存在性由启动后首个查询自然暴露，本守卫只盯精度漂移；探测失败
// （方言/权限差异）也只告警，不因无法取证阻断启动。
func (g *SchemaPrecisionGuard) Validate() error {
	if g == nil || g.db == nil {
		return nil
	}
	var (
		probes []schemaPrecisionProbe
		err    error
	)
	switch g.db.Dialector.Name() {
	case "mysql":
		probes, err = probeTimePrecision(g.db, `
SELECT TABLE_NAME AS table_name, COLUMN_NAME AS column_name, DATETIME_PRECISION AS datetime_precision
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME IN ('wf_task','wf_hi_task')
  AND COLUMN_NAME IN ('created_at','ended_at','updated_at')`)
	case "postgres":
		probes, err = probeTimePrecision(g.db, `
SELECT table_name, column_name, datetime_precision
FROM information_schema.columns
WHERE table_name IN ('wf_task','wf_hi_task')
  AND column_name IN ('created_at','ended_at','updated_at')`)
	default:
		// sqlite 及国产库专有方言（达梦等）：无对应 information_schema 精度语义
		// 或不经此守卫，跳过；MySQL/PG 协议兼容库走上方分支，探测失败只告警
		return nil
	}
	if err != nil {
		logrus.WithError(err).Warn("schema precision probe failed, skipping (advisory check)")
		return nil
	}
	low, missing := evaluateSchemaPrecision(schemaTimePrecisionTargets, probes)
	for _, m := range missing {
		logrus.WithField("column", m).Warn("SCHEMA_TIME_COLUMN_MISSING: 引擎时序守卫依赖的列不存在（全新库未 init 或表名漂移），精度探测跳过该列")
	}
	if len(low) == 0 {
		return nil
	}
	return fmt.Errorf("引擎时序依赖的时间列精度低于毫秒（收回守卫按 ended_at 毫秒分序，秒级列上同秒两票无法分序）: %v；存量库请先执行毫秒精度补丁（对齐引擎 scripts 建表脚本的 DATETIME(3) 基准）再启动", low)
}

func probeTimePrecision(db *gorm.DB, query string) ([]schemaPrecisionProbe, error) {
	var probes []schemaPrecisionProbe
	if err := db.Raw(query).Scan(&probes).Error; err != nil {
		return nil, fmt.Errorf("query information_schema: %w", err)
	}
	return probes, nil
}

// evaluateSchemaPrecision 归类探测结果（纯逻辑可单测）：low=精度不足
// （毫秒时序依赖被打破），missing=列未建或精度读不到（NULL 视同缺失）。
func evaluateSchemaPrecision(expected map[string][]string, probes []schemaPrecisionProbe) (low, missing []string) {
	seen := make(map[string]int, len(probes))
	for _, p := range probes {
		if p.Precision == nil {
			continue
		}
		seen[p.Table+"."+p.Column] = *p.Precision
	}
	for table, cols := range expected {
		for _, col := range cols {
			key := table + "." + col
			precision, ok := seen[key]
			if !ok {
				missing = append(missing, key)
				continue
			}
			if precision < 3 {
				low = append(low, fmt.Sprintf("%s(%d)", key, precision))
			}
		}
	}
	return low, missing
}

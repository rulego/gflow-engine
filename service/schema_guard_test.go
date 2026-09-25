// 启动期 schema 精度校验测试：判定逻辑（纯函数）+ 跳过路径（sqlite/nil db）。
package service

import (
	"testing"

	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func intPtr(i int) *int { return &i }

func TestEvaluateSchemaPrecision(t *testing.T) {
	expected := map[string][]string{
		"wf_task":    {"created_at", "ended_at"},
		"wf_hi_task": {"ended_at"},
	}
	allMs := []schemaPrecisionProbe{
		{Table: "wf_task", Column: "created_at", Precision: intPtr(3)},
		{Table: "wf_task", Column: "ended_at", Precision: intPtr(3)},
		{Table: "wf_hi_task", Column: "ended_at", Precision: intPtr(3)},
	}
	low, missing := evaluateSchemaPrecision(expected, allMs)
	require.Empty(t, low)
	require.Empty(t, missing)

	// 秒级漂移（DATETIME 无精度）：precision=0
	second := append([]schemaPrecisionProbe{}, allMs...)
	second[1].Precision = intPtr(0)
	low, missing = evaluateSchemaPrecision(expected, second)
	require.Equal(t, []string{"wf_task.ended_at(0)"}, low)
	require.Empty(t, missing)

	// 列缺失（全新库未 init / 表名漂移）：只进 missing，不误报低精度
	low, missing = evaluateSchemaPrecision(expected, allMs[:1])
	require.Empty(t, low)
	require.Equal(t, []string{"wf_task.ended_at", "wf_hi_task.ended_at"}, missing)

	// 精度读不到（国产兼容实现返回 NULL）：按缺失告警，不误判秒级
	nullPrecision := []schemaPrecisionProbe{
		{Table: "wf_task", Column: "created_at", Precision: intPtr(3)},
		{Table: "wf_task", Column: "ended_at"},
		{Table: "wf_hi_task", Column: "ended_at", Precision: intPtr(3)},
	}
	low, missing = evaluateSchemaPrecision(expected, nullPrecision)
	require.Empty(t, low)
	require.Equal(t, []string{"wf_task.ended_at"}, missing)
}

// sqlite 等无精度概念的方言直接跳过（引擎自测环境就是 sqlite）。
func TestSchemaPrecisionGuard_SQLiteSkipped(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, NewSchemaPrecisionGuard(db).Validate())
}

// db 为空（单测装配无库引擎）时探测跳过。
func TestSchemaPrecisionGuard_NilDB(t *testing.T) {
	require.NoError(t, NewSchemaPrecisionGuard(nil).Validate())
}

package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ============================================================================
// 表单数据统计：查询计划与结果契约
//
// 引擎只按声明式查询计划对实例 variables 做聚合，不感知表单语义：
// 哪个字段是数字、哪些选项合法由宿主从表单 schema 解析后随计划传入。
// 计划是纯数据（无闭包），为后续把聚合下推到 SQL 预留同一形状。
// ============================================================================

// FormStatsAgg 数值聚合算子
type FormStatsAgg string

const (
	FormStatsAggSum FormStatsAgg = "sum"
	FormStatsAggAvg FormStatsAgg = "avg"
	FormStatsAggMin FormStatsAgg = "min"
	FormStatsAggMax FormStatsAgg = "max"
)

// FormStatsGroupKind 分组维度类型
type FormStatsGroupKind string

const (
	// FormStatsGroupField 按表单字段值分组（如请假类型）
	FormStatsGroupField FormStatsGroupKind = "field"
	// FormStatsGroupTime 按时间桶分组；Path 为空时用实例创建时间，
	// 指向日期字段时用字段值
	FormStatsGroupTime FormStatsGroupKind = "time"
	// FormStatsGroupStartUser 按发起人分组，引擎输出原始用户 ID，由宿主翻译姓名/部门
	FormStatsGroupStartUser FormStatsGroupKind = "startUser"
)

// FormStatsMetricRequest 单个数值聚合项
type FormStatsMetricRequest struct {
	Path string       `json:"path"` // variables 内取值路径，支持 a.b 与 a[].b（子表展开）
	Agg  FormStatsAgg `json:"agg"`
}

// FormStatsDistributionRequest 选项占比项
type FormStatsDistributionRequest struct {
	Path    string   `json:"path"`
	Options []string `json:"options,omitempty"` // 桶全集（宿主从 schema 选项下发），缺省动态收桶
}

// FormStatsGroupByRequest 分组维度
type FormStatsGroupByRequest struct {
	Kind   FormStatsGroupKind `json:"kind"`
	Path   string             `json:"path,omitempty"`
	Bucket string             `json:"bucket,omitempty"` // day|month，time 维度必填
}

// FormStatsQuery 统计查询计划
type FormStatsQuery struct {
	ProcessKey string    `json:"processKey"`
	Start      time.Time `json:"start"`
	End        time.Time `json:"end"`

	// IncludeActive 默认只统计历史表终态实例（月报语义）；
	// 置 true 时合并运行中实例（运行表通常远小于历史表）
	IncludeActive bool `json:"includeActive"`

	Metrics       []FormStatsMetricRequest       `json:"metrics,omitempty"`
	Distributions []FormStatsDistributionRequest `json:"distributions,omitempty"`
	GroupBy       []FormStatsGroupByRequest      `json:"groupBy,omitempty"`

	// MaxInstances 实时聚合护栏上限，<=0 时取 DefaultFormStatsMaxInstances
	MaxInstances int64 `json:"-"`
}

// DefaultFormStatsMaxInstances 实时聚合的实例数上限。
// 上限对应"传输+解析约百兆、秒级完成"的交互忍耐边界，越线应缩小时间范围
// 或切换事实表预聚合路径（宿主侧）。
const DefaultFormStatsMaxInstances int64 = 50000

// FormStatsMetricResult 单指标聚合结果。Count 为有效样本数，Skipped 为
// 字段缺失或值无法数值化的实例数，二者之和等于参与统计的实例总数。
type FormStatsMetricResult struct {
	Path    string       `json:"path"`
	Agg     FormStatsAgg `json:"agg"`
	Value   float64      `json:"value"`
	Count   int64        `json:"count"`
	Skipped int64        `json:"skipped"`
}

// FormStatsBucket 占比桶
type FormStatsBucket struct {
	Key        string  `json:"key"`
	Count      int64   `json:"count"`
	Percentage float64 `json:"percentage"`
}

// FormStatsDistributionResult 占比结果。多选字段一票可命中多桶，
// 各桶 Count 之和可大于 Total，分母固定为实例数。
type FormStatsDistributionResult struct {
	Path    string            `json:"path"`
	Total   int64             `json:"total"`
	Skipped int64             `json:"skipped"`
	Buckets []FormStatsBucket `json:"buckets"`
}

// FormStatsGroupResult 分组行：Key 为维度原始值（startUser 维度是用户 ID），
// 指标为该组内的聚合值。
type FormStatsGroupResult struct {
	Kind    FormStatsGroupKind      `json:"kind"`
	Path    string                  `json:"path,omitempty"`
	Key     string                  `json:"key"`
	Metrics []FormStatsMetricResult `json:"metrics"`
}

// FormStatsResult 聚合结果
type FormStatsResult struct {
	Total         int64                         `json:"total"` // 参与统计的实例总数
	Metrics       []FormStatsMetricResult       `json:"metrics,omitempty"`
	Distributions []FormStatsDistributionResult `json:"distributions,omitempty"`
	Groups        []FormStatsGroupResult        `json:"groups,omitempty"`
}

// FormStatisticsService 表单数据统计服务：按查询计划对实例 variables 聚合。
type FormStatisticsService interface {
	// Aggregate 执行聚合。actor 租户必须非空，统计范围强制限定在该租户内；
	// 参与实例数超过护栏上限时返回 ErrFormStatsScopeTooLarge。
	Aggregate(ctx context.Context, actor Actor, q *FormStatsQuery) (*FormStatsResult, error)
}

// ErrFormStatsScopeTooLarge 统计范围超过实时聚合护栏上限。
// 调用方应引导用户缩小时间范围，而不是静默截断数据。
var ErrFormStatsScopeTooLarge = fmt.Errorf("form statistics scope exceeds limit")

// ============================================================================
// 路径解析与取值
// ============================================================================

// formStatsSegment 预编译的路径段：slice 为 true 表示展开数组（子表单）。
// 路径在计划阶段编译一次，逐实例取值时不再重复切分字符串。
type formStatsSegment struct {
	key   string
	slice bool
}

// compileFormStatsPath 把 "a.b" / "a[].b" 编译为段序列。
func compileFormStatsPath(path string) ([]formStatsSegment, error) {
	if path == "" {
		return nil, fmt.Errorf("empty field path")
	}
	parts := strings.Split(path, ".")
	segs := make([]formStatsSegment, 0, len(parts))
	for _, p := range parts {
		if p == "" {
			return nil, fmt.Errorf("invalid field path %q", path)
		}
		if strings.HasSuffix(p, "[]") {
			segs = append(segs, formStatsSegment{key: strings.TrimSuffix(p, "[]"), slice: true})
		} else {
			segs = append(segs, formStatsSegment{key: p})
		}
	}
	return segs, nil
}

// resolveFormStatsPath 按段序列取值，数组段展开为多值（子表每行一条）。
// 字段缺失或中途类型不符时返回空切片，由调用方计入 skipped。
func resolveFormStatsPath(vars map[string]interface{}, segs []formStatsSegment) []interface{} {
	current := []interface{}{vars}
	for _, seg := range segs {
		next := make([]interface{}, 0, len(current))
		for _, node := range current {
			m, ok := node.(map[string]interface{})
			if !ok {
				continue
			}
			v, exists := m[seg.key]
			if !exists || v == nil {
				continue
			}
			if seg.slice {
				arr, ok := v.([]interface{})
				if !ok {
					continue
				}
				next = append(next, arr...)
			} else {
				next = append(next, v)
			}
		}
		current = next
		if len(current) == 0 {
			return nil
		}
	}
	return current
}

// coerceFormStatsFloat 把变量值转 float64。JSON 反序列化的数字恒为 float64；
// 字符串数字（外部 API 写入的脏数据）做兜底转换，失败返回 false。
func coerceFormStatsFloat(v interface{}) (float64, bool) {
	switch t := v.(type) {
	case float64:
		return t, true
	case json.Number:
		f, err := t.Float64()
		return f, err == nil
	case string:
		s := strings.TrimSpace(t)
		if s == "" {
			return 0, false
		}
		f, err := strconv.ParseFloat(s, 64)
		return f, err == nil
	default:
		return 0, false
	}
}

// coerceFormStatsKey 把变量值转为分组/占比桶键：多值数组展开为多键
// （多选一票多桶），标量统一按字符串呈现。
func coerceFormStatsKeys(v interface{}) []string {
	switch t := v.(type) {
	case []interface{}:
		keys := make([]string, 0, len(t))
		for _, item := range t {
			if s, ok := coerceFormStatsScalarKey(item); ok {
				keys = append(keys, s)
			}
		}
		return keys
	default:
		if s, ok := coerceFormStatsScalarKey(v); ok {
			return []string{s}
		}
		return nil
	}
}

func coerceFormStatsScalarKey(v interface{}) (string, bool) {
	switch t := v.(type) {
	case nil:
		return "", false
	case string:
		if t == "" {
			return "", false
		}
		return t, true
	case bool:
		if t {
			return "true", true
		}
		return "false", true
	case float64:
		// 整数值选项（checkbox 存 1/2/3）去掉小数尾巴，与 schema 选项字符串对齐
		if t == float64(int64(t)) {
			return strconv.FormatInt(int64(t), 10), true
		}
		return strconv.FormatFloat(t, 'g', -1, 64), true
	case json.Number:
		return t.String(), true
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return "", false
		}
		return string(b), true
	}
}

// formStatsTimeBucket 按本地时区取时间桶键。审批日期字段无时区语义，
// 与列表查询的本地时区口径一致，避免 UTC 边界把单日拆到两个桶。
func formStatsTimeBucket(t time.Time, bucket string) string {
	switch bucket {
	case "month":
		return t.Format("2006-01")
	default:
		return t.Format("2006-01-02")
	}
}

package service

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/rulego/gflow-engine/dao"
	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/query"
	"github.com/rulego/gflow-engine/types/dto"
	"github.com/rulego/gflow-engine/types/enums"
)

// ============================================================================
// 表单数据统计：内存聚合实现
//
// 执行管线：护栏校验（先 COUNT，超限直接拒绝，不白拉数据）
//   → 时间分片 + keyset 流式拉实例（片内 WHERE id > 游标，逐批解析 variables）
//   → 按预编译路径取值，喂给指标/占比/分组三类累加器
// 单次聚合的内存占用 = 累加器状态 + 实例去重集（护栏为上界，数 MB）
//   + 当前批实例，不随总实例数增长。
// ============================================================================

// formStatisticsServiceImpl 内存聚合实现。依赖两个实例 DAO 复用既有的
// 租户隔离与过滤口径；独立于 WorkflowEngine 接口装配（宿主持 engine.GetDB()
// 构造 query 后创建），不侵入引擎接口面。
type formStatisticsServiceImpl struct {
	hiInstanceDAO *dao.HiInstanceDAO
	instanceDAO   *dao.InstanceDAO
}

// NewFormStatisticsService 创建表单数据统计服务。
func NewFormStatisticsService(q *query.Query) FormStatisticsService {
	return &formStatisticsServiceImpl{
		hiInstanceDAO: dao.NewHiInstanceDAOWithQuery(q),
		instanceDAO:   dao.NewInstanceDAOWithQuery(q),
	}
}

// formStatsBatch 流式拉取的批大小。id 排序在带 JOIN 的过滤集上无法完全走
// 索引，每批的排序输入是剩余过滤集，总排序量约 N²/(2×批大小)：受控内部
// 遍历取远大于列表页的大批，压掉深遍历的平方项。1000 行 × 常见表单
// variables（2-8KB）单批内存约 2-8MB，仍在"累加器+当前批"的内存承诺内；
// 不受列表页 MaxPageSize=200 约束（该上限防的是前端一页拉全表）。
const formStatsBatch = 1000

// Aggregate 执行统计主流程。
func (s *formStatisticsServiceImpl) Aggregate(ctx context.Context, actor Actor, q *FormStatsQuery) (*FormStatsResult, error) {
	if actor.TenantID == "" {
		return nil, fmt.Errorf("tenant ID cannot be empty")
	}
	if q == nil || q.ProcessKey == "" {
		return nil, fmt.Errorf("process key cannot be empty")
	}
	if q.Start.IsZero() || q.End.IsZero() || q.End.Before(q.Start) {
		return nil, fmt.Errorf("invalid time range")
	}
	if len(q.Metrics) == 0 && len(q.Distributions) == 0 && len(q.GroupBy) == 0 {
		return nil, fmt.Errorf("empty statistics plan")
	}

	acc, err := newFormStatsAccumulator(q)
	if err != nil {
		return nil, err
	}

	maxInstances := q.MaxInstances
	if maxInstances <= 0 {
		maxInstances = DefaultFormStatsMaxInstances
	}

	// 终态实例可能仍在运行表：完成到归档（迁入 wf_hi_instance）之间有窗口，
	// 只扫历史表会漏掉刚完结的单。运行表按同终态集补扫；IncludeActive
	// 再追加 active。运行表通常远小于历史表，补扫开销可忽略。
	rtStatuses := formStatsTerminalStatuses
	if q.IncludeActive {
		rtStatuses = append(rtStatuses, string(enums.InstanceStatusActive))
	}

	// 护栏前置：先 COUNT 再决定是否拉数据，超限直接拒绝；
	// 条件可走 tenant+status+created_at 或 process+created_at 索引。
	// 两表行数同时充当各趟遍历的分片规划提示
	var hiTotal, rtTotal int64
	hiReq := s.buildCountQuery(actor, q, formStatsTerminalStatuses)
	if _, total, err := s.hiInstanceDAO.List(ctx, hiReq); err != nil {
		return nil, fmt.Errorf("failed to count history instances: %w", err)
	} else {
		hiTotal = total
	}
	rtReq := s.buildCountQuery(actor, q, rtStatuses)
	if _, total, err := s.instanceDAO.List(ctx, rtReq); err != nil {
		return nil, fmt.Errorf("failed to count runtime instances: %w", err)
	} else {
		rtTotal = total
	}
	if hiTotal+rtTotal > maxInstances {
		return nil, fmt.Errorf("%w: %d instances (limit %d), narrow the time range",
			ErrFormStatsScopeTooLarge, hiTotal+rtTotal, maxInstances)
	}

	// 分片流式拉取：时间窗先切成行数有界的分片，片内按 id 游标推进。
	// 迭代非事务快照，游标推进期间新归档的终态行会被后续批次自然带上；
	// 两表并存的竞态行与分片边界共享时刻的行统一按实例 ID 去重。
	// 先扫运行表补归档窗口，再扫历史表
	seen := make(map[string]struct{})
	if err := s.iterate(ctx, actor, q, rtStatuses, false, rtTotal, func(inst *model.WfInstance) error {
		return feedInstanceDedup(acc, inst, seen)
	}); err != nil {
		return nil, err
	}
	if err := s.iterate(ctx, actor, q, formStatsTerminalStatuses, true, hiTotal, func(inst *model.WfInstance) error {
		return feedInstanceDedup(acc, inst, seen)
	}); err != nil {
		return nil, err
	}

	return acc.snapshot(), nil
}

// feedInstanceDedup 同实例 ID 只喂一次：覆盖两类重复——历史表与运行表在
// 归档竞态窗口内同时持有终态行；分片遍历相邻片共享边界时刻的行。统计
// 口径按单据数而非表行数。
func feedInstanceDedup(acc *formStatsAccumulator, inst *model.WfInstance, seen map[string]struct{}) error {
	if _, dup := seen[inst.ID]; dup {
		return nil
	}
	seen[inst.ID] = struct{}{}
	return feedInstance(acc, inst)
}

// feedInstance 解析单个实例的 variables 并喂给累加器。
// variables 是引擎自己序列化的 JSON，正常不会解析失败；外部 API 写坏
// 的情况下该实例无法访问任何字段，整条跳过（不计入总数）。
func feedInstance(acc *formStatsAccumulator, inst *model.WfInstance) error {
	vars, err := ParseVariablesJSON(inst.Variables)
	if err != nil {
		return nil
	}
	acc.feed(vars, inst.CreatedAt, inst.StartUserID)
	return nil
}

// iterate 分片流式遍历实例表：先把时间窗切成行数有界的分片，片内以最后
// 一批末行 id 为游标逐批拉取，单批行数不足 formStatsBatch 视为片尾。
// 分片把每批排序输入从"剩余全集"压到"片内剩余"（id 排序在带 JOIN 的过滤
// 集上无法完全走索引，排序开销随输入平方增长），深遍历成本回到与总行数
// 线性。相邻分片共享边界时刻（闭区间相接），边界行按实例 ID 去重。
// hint 为护栏阶段数得的该趟行数，仅作规划提示，片内完成条件仍是短批。
func (s *formStatisticsServiceImpl) iterate(ctx context.Context, actor Actor, q *FormStatsQuery,
	statuses []string, history bool, hint int64, fn func(*model.WfInstance) error) error {
	fetch := func(lo, hi time.Time, afterID string) ([]*model.WfInstance, error) {
		if history {
			return s.hiInstanceDAO.ListAfterID(ctx, actor.TenantID, q.ProcessKey, statuses, &lo, &hi, afterID, formStatsBatch)
		}
		return s.instanceDAO.ListAfterID(ctx, actor.TenantID, q.ProcessKey, statuses, &lo, &hi, afterID, formStatsBatch)
	}
	chunks, err := s.planChunks(ctx, actor, q, statuses, history, q.Start, q.End, hint)
	if err != nil {
		return err
	}
	for _, ch := range chunks {
		afterID := ""
		for {
			list, err := fetch(ch.lo, ch.hi, afterID)
			if err != nil {
				return fmt.Errorf("failed to load instances batch after %q: %w", afterID, err)
			}
			for _, inst := range list {
				if err := fn(inst); err != nil {
					return err
				}
			}
			if len(list) < formStatsBatch {
				break
			}
			afterID = list[len(list)-1].ID
		}
	}
	return nil
}

// formStatsChunk 时间分片，lo/hi 均为闭区间边界。
type formStatsChunk struct {
	lo, hi time.Time
}

// formStatsChunkTarget 分片行数目标：单批排序输入的上界，也是分片规划的
// 再切阈值。
const formStatsChunkTarget = 2500

// planChunks 把 [lo, hi] 递归对半切成行数不超过 formStatsChunkTarget 的分
// 片（除窗口过窄切不动的尾部）。行数超过目标时数左半推右半（hint 减法），
// 少一次 COUNT；行数为零的片直接剪掉。
func (s *formStatisticsServiceImpl) planChunks(ctx context.Context, actor Actor, q *FormStatsQuery,
	statuses []string, history bool, lo, hi time.Time, hint int64) ([]formStatsChunk, error) {
	if hint <= 0 {
		return nil, nil
	}
	if hint <= formStatsChunkTarget {
		return []formStatsChunk{{lo: lo, hi: hi}}, nil
	}
	mid := lo.Add(hi.Sub(lo) / 2)
	if !mid.After(lo) || !mid.Before(hi) {
		// 窗口窄到时间精度切不出新边界（极端聚集的时刻），保留整片
		return []formStatsChunk{{lo: lo, hi: hi}}, nil
	}
	leftHint, err := s.countRange(ctx, actor, q, statuses, history, lo, mid)
	if err != nil {
		return nil, err
	}
	left, err := s.planChunks(ctx, actor, q, statuses, history, lo, mid, leftHint)
	if err != nil {
		return nil, err
	}
	right, err := s.planChunks(ctx, actor, q, statuses, history, mid, hi, hint-leftHint)
	if err != nil {
		return nil, err
	}
	return append(left, right...), nil
}

// countRange 数当前趟（历史表或运行表）在 [lo, hi] 内的行数。
func (s *formStatisticsServiceImpl) countRange(ctx context.Context, actor Actor, q *FormStatsQuery,
	statuses []string, history bool, lo, hi time.Time) (int64, error) {
	if history {
		return s.hiInstanceDAO.CountInRange(ctx, actor.TenantID, q.ProcessKey, statuses, lo, hi)
	}
	return s.instanceDAO.CountInRange(ctx, actor.TenantID, q.ProcessKey, statuses, lo, hi)
}

// buildCountQuery 组装护栏 COUNT 用的 DAO 查询：租户强制 actor 口径，
// 时间窗闭区间，状态集由调用方给定（历史表取终态集，运行表取补扫集）。
func (s *formStatisticsServiceImpl) buildCountQuery(actor Actor, q *FormStatsQuery,
	statuses []string) *dto.ProcessInstanceQueryDTO {
	return &dto.ProcessInstanceQueryDTO{
		PageRequest: dto.PageRequest{
			Page:     1,
			PageSize: 1,
			TenantID: actor.TenantID,
			Status:   statuses,
		},
		ProcessKey:    q.ProcessKey,
		CreatedAfter:  &q.Start,
		CreatedBefore: &q.End,
	}
}

// formStatsTerminalStatuses 统计默认口径的终态集合，与实例冻结口径同源
// （terminalInstanceStatuses）：撤回/驳回级联的实例也落在这些状态上，
// 月报语义下都属于"有结果的单"。排序保证 SQL IN 列表稳定，便于缓存对比。
var formStatsTerminalStatuses = func() []string {
	out := make([]string, 0, len(terminalInstanceStatuses))
	for s := range terminalInstanceStatuses {
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}()

// ============================================================================
// 累加器
// ============================================================================

// formStatsAccumulator 单实例喂入、跨实例累加的聚合器组。
type formStatsAccumulator struct {
	metricReqs  []FormStatsMetricRequest
	metricSegs  [][]formStatsSegment
	distReqs    []FormStatsDistributionRequest
	distSegs    [][]formStatsSegment
	groupReqs   []compiledGroupBy
	result      FormStatsResult
	rootMetrics []*metricAcc
	rootDists   []*distAcc
	groups      map[string]*groupAcc
}

type compiledGroupBy struct {
	req  FormStatsGroupByRequest
	segs []formStatsSegment // field/time 维度的字段路径；startUser 维度为空
}

// metricAcc 数值累加器：sum/avg/min/max 共用一次遍历。
type metricAcc struct {
	sum     float64
	min     float64
	max     float64
	count   int64
	skipped int64
}

func (a *metricAcc) feed(values []interface{}) {
	if len(values) == 0 {
		a.skipped++
		return
	}
	fed := false
	for _, v := range values {
		f, ok := coerceFormStatsFloat(v)
		if !ok {
			continue
		}
		// min/max 只在首个有效样本初始化（count==0），跨 feed 保持累计
		if a.count == 0 {
			a.min, a.max = f, f
		} else {
			if f < a.min {
				a.min = f
			}
			if f > a.max {
				a.max = f
			}
		}
		a.sum += f
		a.count++
		fed = true
	}
	if !fed {
		a.skipped++
	}
}

func (a *metricAcc) result(req FormStatsMetricRequest) FormStatsMetricResult {
	r := FormStatsMetricResult{Path: req.Path, Agg: req.Agg, Count: a.count, Skipped: a.skipped}
	if a.count == 0 {
		return r
	}
	switch req.Agg {
	case FormStatsAggSum:
		r.Value = a.sum
	case FormStatsAggAvg:
		r.Value = a.sum / float64(a.count)
	case FormStatsAggMin:
		r.Value = a.min
	case FormStatsAggMax:
		r.Value = a.max
	}
	return r
}

// distAcc 占比累加器：counts 收桶，total 按实例计（多选一票多桶时分母不变）。
type distAcc struct {
	counts  map[string]int64
	order   []string // 桶输出顺序稳定：配置全集时按全集，否则按首现顺序
	total   int64
	skipped int64
}

func (a *distAcc) feed(values []interface{}) {
	a.total++
	if len(values) == 0 {
		a.skipped++
		return
	}
	fed := false
	for _, v := range values {
		// 每个子表行/字段值独立拆键：多选字段的数组值在此展开为一票多桶
		for _, key := range coerceFormStatsKeys(v) {
			if _, exists := a.counts[key]; !exists {
				a.order = append(a.order, key)
			}
			a.counts[key]++
			fed = true
		}
	}
	if !fed {
		a.skipped++
	}
}

func (a *distAcc) result(req FormStatsDistributionRequest) FormStatsDistributionResult {
	r := FormStatsDistributionResult{Path: req.Path, Total: a.total, Skipped: a.skipped}
	if len(req.Options) > 0 {
		// 全集对齐：零票选项也出 0%，前端不需要为缺失桶兜底
		r.Buckets = make([]FormStatsBucket, 0, len(req.Options))
		for _, opt := range req.Options {
			r.Buckets = append(r.Buckets, bucketOf(opt, a.counts[opt], a.total))
		}
		return r
	}
	r.Buckets = make([]FormStatsBucket, 0, len(a.order))
	for _, key := range a.order {
		r.Buckets = append(r.Buckets, bucketOf(key, a.counts[key], a.total))
	}
	return r
}

func bucketOf(key string, count, total int64) FormStatsBucket {
	p := 0.0
	if total > 0 {
		p = float64(count) / float64(total) * 100
	}
	return FormStatsBucket{Key: key, Count: count, Percentage: round2(p)}
}

func round2(v float64) float64 {
	return float64(int64(v*100+0.5)) / 100
}

// groupAcc 分组桶：各组独立持有整套指标累加器。
type groupAcc struct {
	metrics []*metricAcc
}

func newFormStatsAccumulator(q *FormStatsQuery) (*formStatsAccumulator, error) {
	acc := &formStatsAccumulator{
		metricReqs: q.Metrics,
		distReqs:   q.Distributions,
		groups:     make(map[string]*groupAcc),
	}
	acc.metricSegs = make([][]formStatsSegment, len(q.Metrics))
	for i, m := range q.Metrics {
		segs, err := compileFormStatsPath(m.Path)
		if err != nil {
			return nil, err
		}
		acc.metricSegs[i] = segs
		acc.rootMetrics = append(acc.rootMetrics, &metricAcc{})
	}
	acc.distSegs = make([][]formStatsSegment, len(q.Distributions))
	for i, d := range q.Distributions {
		segs, err := compileFormStatsPath(d.Path)
		if err != nil {
			return nil, err
		}
		acc.distSegs[i] = segs
		acc.rootDists = append(acc.rootDists, &distAcc{counts: make(map[string]int64)})
	}
	for _, g := range q.GroupBy {
		cg := compiledGroupBy{req: g}
		if g.Kind == FormStatsGroupField || (g.Kind == FormStatsGroupTime && g.Path != "") {
			segs, err := compileFormStatsPath(g.Path)
			if err != nil {
				return nil, err
			}
			cg.segs = segs
		}
		acc.groupReqs = append(acc.groupReqs, cg)
	}
	return acc, nil
}

// feed 消费单个实例。createdAt 供 time 维度（Path 为空）分桶。
func (a *formStatsAccumulator) feed(vars map[string]interface{}, createdAt time.Time, startUserID string) {
	a.result.Total++
	for i := range a.metricReqs {
		a.rootMetrics[i].feed(resolveFormStatsPath(vars, a.metricSegs[i]))
	}
	for i := range a.distReqs {
		a.rootDists[i].feed(resolveFormStatsPath(vars, a.distSegs[i]))
	}
	// 分组：一个实例在多个维度上各归一个桶；维度值缺失时该维度不进桶
	for gi := range a.groupReqs {
		cg := &a.groupReqs[gi]
		var key string
		switch cg.req.Kind {
		case FormStatsGroupStartUser:
			key = startUserID
		case FormStatsGroupTime:
			if len(cg.segs) > 0 {
				values := resolveFormStatsPath(vars, cg.segs)
				if len(values) == 0 {
					continue
				}
				t, ok := parseFormStatsDate(values[0])
				if !ok {
					continue
				}
				key = formStatsTimeBucket(t, cg.req.Bucket)
			} else {
				key = formStatsTimeBucket(createdAt, cg.req.Bucket)
			}
		default: // field
			values := resolveFormStatsPath(vars, cg.segs)
			if len(values) == 0 {
				continue
			}
			keys := coerceFormStatsKeys(values[0])
			if len(keys) == 0 {
				continue
			}
			key = keys[0]
		}
		if key == "" {
			continue
		}
		// 分组键带维度序号隔离：同实例在多维度分组时不串桶
		accKey := fmt.Sprintf("%d\x00%s", gi, key)
		g, exists := a.groups[accKey]
		if !exists {
			g = &groupAcc{metrics: make([]*metricAcc, len(a.metricReqs))}
			for mi := range g.metrics {
				g.metrics[mi] = &metricAcc{}
			}
			a.groups[accKey] = g
		}
		for mi := range a.metricReqs {
			g.metrics[mi].feed(resolveFormStatsPath(vars, a.metricSegs[mi]))
		}
	}
}

// snapshot 输出聚合结果。分组行按（维度序号，键）排序，响应顺序稳定，
// 便于缓存对比与前端翻页渲染。
func (a *formStatsAccumulator) snapshot() *FormStatsResult {
	r := a.result
	r.Metrics = make([]FormStatsMetricResult, 0, len(a.metricReqs))
	for i, req := range a.metricReqs {
		r.Metrics = append(r.Metrics, a.rootMetrics[i].result(req))
	}
	r.Distributions = make([]FormStatsDistributionResult, 0, len(a.distReqs))
	for i, req := range a.distReqs {
		r.Distributions = append(r.Distributions, a.rootDists[i].result(req))
	}
	if len(a.groupReqs) > 0 {
		keys := make([]string, 0, len(a.groups))
		for k := range a.groups {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool {
			giI, keyI := splitGroupKey(keys[i])
			giJ, keyJ := splitGroupKey(keys[j])
			if giI != giJ {
				return giI < giJ
			}
			return keyI < keyJ
		})
		r.Groups = make([]FormStatsGroupResult, 0, len(keys))
		for _, k := range keys {
			gi, key := splitGroupKey(k)
			g := a.groups[k]
			row := FormStatsGroupResult{
				Kind:    a.groupReqs[gi].req.Kind,
				Path:    a.groupReqs[gi].req.Path,
				Key:     key,
				Metrics: make([]FormStatsMetricResult, 0, len(a.metricReqs)),
			}
			for mi, req := range a.metricReqs {
				row.Metrics = append(row.Metrics, g.metrics[mi].result(req))
			}
			r.Groups = append(r.Groups, row)
		}
	}
	return &r
}

// splitGroupKey 拆出维度序号与业务键（feed 里以 NUL 分隔拼接）。
func splitGroupKey(k string) (int, string) {
	idx := strings.IndexByte(k, 0)
	if idx < 0 {
		return 0, k
	}
	var gi int
	if _, err := fmt.Sscanf(k[:idx], "%d", &gi); err != nil {
		return 0, k
	}
	return gi, k[idx+1:]
}

// parseFormStatsDate 解析日期字段值，容忍表单日期控件常见格式；
// 按本地时区解释（表单日期无时区语义，与列表查询口径一致）。
func parseFormStatsDate(v interface{}) (time.Time, bool) {
	s, ok := v.(string)
	if !ok || s == "" {
		return time.Time{}, false
	}
	for _, layout := range []string{"2006-01-02 15:04:05", "2006-01-02T15:04:05", "2006-01-02", "2006/01/02"} {
		if t, err := time.ParseInLocation(layout, s, time.Local); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

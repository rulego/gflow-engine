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

package components

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/rulego/gflow-engine/types/constants"
	"github.com/rulego/rulego/api/types"
)

// 节点瞬时故障自动重试的共享策略：可重试判定、指数退避、幂等键生成。
// httpCall（发送段同步循环）与 serviceTask（包装 RuleContext 拦截失败路由）
// 共用本文件的判定口径，重试语义必须一致。
//
// 量级边界：重试在节点 OnMsg 内同步进行，占住实例执行闸的时长≈退避累计
// （1s+4s+16s）+ 重试次数×timeoutMs，配置侧应控制两者乘积不超出业务可容忍窗口。

// nodeRetryBackoffUnit 指数退避时基，生产 1s（1s/4s/16s）；测试替换为毫秒级提速。
var nodeRetryBackoffUnit = time.Second

// RetryAbortHook 实例终态哨兵：重试在节点 OnMsg 内同步进行，感知不到实例被并发
// 终止/完成（终止只改库归档，不取消在途 OnMsg），若不干预会对已终态实例继续发起
// 请求/副作用直到预算耗尽。组件装配（Register）时经 ComponentDeps.RuntimeService
// 接线：按消息元数据的 instance_id 查一次运行态，实例已离开可执行状态返回 true，
// 重试循环立即中止。挂起不算离开（挂起可恢复，中止会把本可恢复的实例提前推向
// 终态）；查询失败放行（退化为仅按预算兜底）。未接线（未注册组件的嵌入式用法/
// 单测）时为 nil，跳过检查。
var RetryAbortHook func(msg types.RuleMsg) bool

// retryAbortError 实例终态哨兵命中后的终止错误：不再重试，节点走失败路由收尾
// （对已终态实例的失败路由由运行时守卫兜住，无害）。
func retryAbortError() error { return errors.New("实例已离开执行中状态，中止重试") }

// retryAbortRequested 哨兵命中判定。
func retryAbortRequested(msg types.RuleMsg) bool {
	return RetryAbortHook != nil && RetryAbortHook(msg)
}

// retryBackoffDelay 第 retryIdx 次（0 起）重试前的等待时长：1x/4x/16x 指数，16x 封顶
// （retryCount 上限 5，后续重试不再放大等待）。
func retryBackoffDelay(retryIdx int) time.Duration {
	if retryIdx > 2 {
		retryIdx = 2
	}
	if retryIdx < 0 {
		retryIdx = 0
	}
	return nodeRetryBackoffUnit << (2 * retryIdx)
}

// waitRetryBackoff 等待第 retryIdx 次重试的退避；引擎取消立即停，返回取消错误
// 且不再发起下一次请求。
func waitRetryBackoff(ctx context.Context, retryIdx int) error {
	t := time.NewTimer(retryBackoffDelay(retryIdx))
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// httpStatusError HTTP 状态码错误。保留原始状态码供重试判定（5xx 瞬时可重试、
// 408/429 瞬时可重试、其余 4xx 业务失败终态），错误串格式与历史实现一致
// （"http %d: %s"）。
type httpStatusError struct {
	code int
	body string
}

func (e *httpStatusError) Error() string { return fmt.Sprintf("http %d: %s", e.code, e.body) }

// transient 5xx=服务端瞬时故障；408 请求超时/429 限流虽属 4xx，但同属瞬时过载，
// 重试有意义（429 的 Retry-After 不单独解析，统一按退避节奏重试）；其余
// 4xx=请求本身有问题，重试无意义。
func (e *httpStatusError) transient() bool {
	return e.code >= 500 || e.code == http.StatusRequestTimeout || e.code == http.StatusTooManyRequests
}

// transientMarker 可重试错误的自证接口：错误类型直接声明自己是否瞬时故障，
// 免去按错误串猜。
type transientMarker interface{ transient() bool }

// http5xxPattern serviceTask 注册函数把上游 5xx 格式化成普通错误串（丢失状态码
// 类型）时的兜底识别，要求 http 前缀锚定（"http 502"、"HTTP 502 Bad Gateway"、
// "http503"），裸数字（金额、计数里的 500）不误判；原始状态行 HTTP/1.1 5xx 不在
// 识别面内——注册方应报语义化错误而非透传状态行。httpCall 自身走 httpStatusError
// 不经此路径。
var http5xxPattern = regexp.MustCompile(`http[ /]?5\d\d`)

// isTransientNodeError 判断节点执行错误是否瞬时技术故障（值得重试）：
// 连接/读超时、连接拒绝、连接重置、断管、5xx、408/429。其余 4xx 业务失败、
// 解析失败、SSRF 拦截、引擎取消（context.Canceled）一律不可重试。
func isTransientNodeError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	var tm transientMarker
	if errors.As(err, &tm) {
		return tm.transient()
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return true
	}
	// 底层 syscall 错误跨平台判定（Windows 上 Go 把 ECONNREFUSED 映射到 WSA 同值）
	if errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) {
		return true
	}
	// 字符串兜底：serviceTask 注册函数常把底层错误格式化成普通错误串，类型化判定
	// 到不了这里。覆盖 Windows 拨号文案（"actively refused"=WSAECONNREFUSED、
	// "forcibly closed"=WSAECONNRESET、"pipe has been ended"=EPIPE）
	s := strings.ToLower(err.Error())
	for _, p := range []string{
		"connection refused", "actively refused",
		"connection reset", "forcibly closed",
		"broken pipe", "pipe has been ended",
		"timeout", "deadline exceeded",
	} {
		if strings.Contains(s, p) {
			return true
		}
	}
	return http5xxPattern.MatchString(s)
}

// retryCountFromConfig 从节点 configuration 提取 retryCount：缺省给 def（httpCall=3、
// serviceTask=2），显式 0=关闭，负数与超出上限的值按 [0, NodeRetryMax] 裁剪。
// JSON 数字经 map 反序列化是 float64，同时兼容 int 注入。
func retryCountFromConfig(configuration types.Configuration, def int) int {
	raw, ok := configuration["retryCount"]
	if !ok || raw == nil {
		return def
	}
	var n int
	switch v := raw.(type) {
	case float64:
		n = int(v)
	case int:
		n = v
	default:
		return def
	}
	if n < 0 {
		return 0
	}
	if n > constants.NodeRetryMax {
		return constants.NodeRetryMax
	}
	return n
}

// newNodeExecutionID 生成节点执行实例的幂等键（16 字节随机 hex）：同一节点执行的
// 全部重试共用一个键，下游接口据此去重。随机源失效时退回纳秒时间戳（进程内仍
// 满足"同一执行一致"）。
func newNodeExecutionID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("idem-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

// retryExhaustedError 预算耗尽后的失败包装：错误串带「重试 N 次后失败」，
// 节点执行日志与实例终止 reason 据此可见重试事实。
func retryExhaustedError(retries int, err error) error {
	if retries <= 0 {
		return err
	}
	return fmt.Errorf("重试 %d 次后失败: %w", retries, err)
}

// retryServiceContext 包装 RuleContext 拦截失败路由：瞬时故障在预算内重试
// （重新执行 invoke），预算耗尽或业务失败原样交真实下游。
// 嵌入接口委托 TellSuccess/TellNext 等其余方法；只拦截 TellFailure——函数经
// TellNext(Failure) 路由失败时不带错误对象，无从判定瞬时性，不拦。
type retryServiceContext struct {
	types.RuleContext
	st *serviceRetryState
}

func (c *retryServiceContext) TellFailure(msg types.RuleMsg, err error) {
	st := c.st
	if st.attempt < st.max && isTransientNodeError(err) {
		// 实例终态哨兵：重试期间实例被并发终止/完成时立即停，不再重跑副作用
		if retryAbortRequested(msg) {
			st.real.TellFailure(msg, retryAbortError())
			return
		}
		idx := st.attempt
		delay := retryBackoffDelay(idx)
		st.attempt++
		logNodeRetry(ServiceTaskNodeType, st.real.GetSelfId(), st.attempt, st.max, delay, err)
		if cerr := waitRetryBackoff(st.real.GetContext(), idx); cerr != nil {
			st.real.TellFailure(msg, fmt.Errorf("serviceTask 重试退避被取消: %w", cerr))
			return
		}
		st.invoke()
		return
	}
	st.real.TellFailure(msg, retryExhaustedError(st.attempt, err))
}

// serviceRetryState 一次节点执行的重试状态：attempt/max 跨重试共享，invoke 重新
// 执行节点（serviceTask=重跑 FunctionsNode.OnMsg，重解析函数名+重渲染 param）。
// attempt 无锁：依赖 FunctionsNode 契约——注册函数必须同步调用 Tell* 完成路由；
// 异步 goroutine 里回调 TellFailure 的宿主函数不在重试保障范围（会竞态+并发
// 重入 invoke），注册方需自行保证同步回调。
type serviceRetryState struct {
	max     int
	attempt int
	real    types.RuleContext
	invoke  func()
}

// logNodeRetry 节点重试留痕：第几次/退避多久/错误，进节点执行日志。
func logNodeRetry(nodeType, nodeID string, attempt, max int, delay time.Duration, cause error) {
	logrus.WithFields(logrus.Fields{
		"nodeType":  nodeType,
		"nodeId":    nodeID,
		"attempt":   fmt.Sprintf("%d/%d", attempt, max),
		"backoffMs": delay.Milliseconds(),
		"cause":     cause.Error(),
	}).Warn("transient failure, auto retrying")
}

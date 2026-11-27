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
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/components/action"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// speedUpRetryBackoff 把重试退避时基换到毫秒级（生产为 1s），返回还原函数。
func speedUpRetryBackoff(t *testing.T) func() {
	t.Helper()
	prev := nodeRetryBackoffUnit
	nodeRetryBackoffUnit = time.Millisecond
	return func() { nodeRetryBackoffUnit = prev }
}

// buildServiceEngine 构造一条仅含 serviceTask 节点的引擎。
func buildServiceEngine(t *testing.T, chainID, configJSON string) types.RuleEngine {
	t.Helper()
	def := `{
		"ruleChain": {"id": "` + chainID + `", "name": "main", "root": true},
		"metadata": {
			"firstNodeIndex": 0,
			"nodes": [{"id": "s", "type": "serviceTask", "name": "服务", "configuration": ` + configJSON + `}],
			"connections": []
		}
	}`
	_ = rulego.Registry.Register(&ServiceTaskNode{}) // 幂等
	engine, err := rulego.New(chainID, []byte(def), rulego.WithConfig(rulego.NewConfig()))
	require.NoError(t, err)
	return engine
}

// retryCountFromConfig 边界：缺省、显式 0、负数裁剪、超上限裁剪、float64/int 兼容、
// 非法类型回缺省。
func TestRetryCountFromConfig(t *testing.T) {
	cases := []struct {
		name string
		raw  any
		def  int
		want int
	}{
		{"absent uses default", nil, 3, 3},
		{"explicit zero disables", float64(0), 3, 0},
		{"negative clipped to zero", float64(-2), 3, 0},
		{"over max clipped", float64(9), 3, 5},
		{"max boundary kept", float64(5), 3, 5},
		{"int injection", 2, 3, 2},
		{"invalid type falls back", "3", 3, 3},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := types.Configuration{}
			if c.raw != nil {
				cfg["retryCount"] = c.raw
			}
			assert.Equal(t, c.want, retryCountFromConfig(cfg, c.def))
		})
	}
}

// http5xxPattern 识别面：http 前缀锚定的 5xx 文本；裸数字与非 5xx 状态码不误判。
func TestHttp5xxPattern(t *testing.T) {
	transient := []string{"http 500: boom", "http 502 bad gateway", "上游 http503"}
	for _, s := range transient {
		assert.True(t, http5xxPattern.MatchString(strings.ToLower(s)), s)
	}
	notTransient := []string{"http 400: bad request", "http 404: not found", "金额500元", " http50"}
	for _, s := range notTransient {
		assert.False(t, http5xxPattern.MatchString(strings.ToLower(s)), s)
	}
}

// ---- httpCall 瞬时故障自动重试 ----

// 前 2 次返 500、第 3 次返 200：节点最终 Success、总请求=3、三次带同一 Idempotency-Key。
func TestHttpCall_RetryTransient5xxThenSuccess(t *testing.T) {
	registerHttpCallForTest(t)
	defer speedUpRetryBackoff(t)()

	var mu sync.Mutex
	var count int
	var keys []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		count++
		keys = append(keys, r.Header.Get("Idempotency-Key"))
		n := count
		mu.Unlock()
		if n <= 2 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	cfg := `{"url":"` + srv.URL + `","method":"GET","retryCount":3}`
	engine := buildHttpEngine(t, "t_http_retry_ok", cfg)
	_, rel, err := runChain(t, engine, types.NewMsgWithJsonData(`{}`))
	require.NoError(t, err)
	assert.Equal(t, types.Success, rel)
	assert.Equal(t, 3, count, "两次 500 后第三次应成功")
	require.Len(t, keys, 3)
	assert.NotEmpty(t, keys[0], "必须携带 Idempotency-Key")
	assert.Equal(t, keys[0], keys[1], "重试必须复用同一 Idempotency-Key")
	assert.Equal(t, keys[1], keys[2], "重试必须复用同一 Idempotency-Key")
}

// 恒返 400：业务失败重试无意义，总请求=1，直接 Failure。
func TestHttpCall_RetryNeverFor4xx(t *testing.T) {
	registerHttpCallForTest(t)
	defer speedUpRetryBackoff(t)()

	var count int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&count, 1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	cfg := `{"url":"` + srv.URL + `","method":"GET","retryCount":3}`
	engine := buildHttpEngine(t, "t_http_retry_4xx", cfg)
	_, rel, err := runChain(t, engine, types.NewMsgWithJsonData(`{}`))
	require.Error(t, err)
	assert.Equal(t, types.Failure, rel)
	assert.Equal(t, int32(1), atomic.LoadInt32(&count), "4xx 不得重试")
}

// 恒返 500 且 retryCount=2：预算耗尽走 Failure，错误串带「重试 2 次后失败」，总请求=3。
func TestHttpCall_RetryExhaustedReportsCount(t *testing.T) {
	registerHttpCallForTest(t)
	defer speedUpRetryBackoff(t)()

	var count int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&count, 1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	cfg := `{"url":"` + srv.URL + `","method":"GET","retryCount":2}`
	engine := buildHttpEngine(t, "t_http_retry_exhaust", cfg)
	_, rel, err := runChain(t, engine, types.NewMsgWithJsonData(`{}`))
	require.Error(t, err)
	assert.Equal(t, types.Failure, rel)
	assert.Equal(t, int32(3), atomic.LoadInt32(&count), "首试+2 次重试=3 次请求")
	assert.Contains(t, err.Error(), "重试 2 次后失败")
}

// retryCount=0 显式关闭：恒 500 只打一次。
func TestHttpCall_RetryDisabledWhenZero(t *testing.T) {
	registerHttpCallForTest(t)
	defer speedUpRetryBackoff(t)()

	var count int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&count, 1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	cfg := `{"url":"` + srv.URL + `","method":"GET","retryCount":0}`
	engine := buildHttpEngine(t, "t_http_retry_off", cfg)
	_, rel, err := runChain(t, engine, types.NewMsgWithJsonData(`{}`))
	require.Error(t, err)
	assert.Equal(t, types.Failure, rel)
	assert.Equal(t, int32(1), atomic.LoadInt32(&count), "retryCount=0 必须关闭重试")
}

// 连接拒绝（服务没起）：瞬时技术故障，预算内重试。
func TestHttpCall_RetryOnConnectionRefused(t *testing.T) {
	registerHttpCallForTest(t)
	defer speedUpRetryBackoff(t)()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	u := srv.URL
	srv.Close() // 先拿到端口再关掉，制造连接拒绝

	cfg := `{"url":"` + u + `","method":"GET","retryCount":1}`
	engine := buildHttpEngine(t, "t_http_retry_refused", cfg)
	_, rel, err := runChain(t, engine, types.NewMsgWithJsonData(`{}`))
	require.Error(t, err)
	assert.Equal(t, types.Failure, rel)
	assert.Contains(t, err.Error(), "重试 1 次后失败")
}

// ---- serviceTask 瞬时故障自动重试 ----

// 前 2 次报瞬时故障、第 3 次成功：节点最终 Success、函数共执行 3 次。
func TestServiceTask_RetryTransientThenSuccess(t *testing.T) {
	defer speedUpRetryBackoff(t)()
	const fn = "test:svc:retry:flaky"
	action.Functions.UnRegister(fn)
	defer action.Functions.UnRegister(fn)

	var calls int32
	action.Functions.Register(fn, func(ctx types.RuleContext, msg types.RuleMsg) {
		if atomic.AddInt32(&calls, 1) <= 2 {
			ctx.TellFailure(msg, errors.New("dial tcp 10.0.0.9:8080: connection refused"))
			return
		}
		ctx.TellSuccess(msg)
	})

	engine := buildServiceEngine(t, "t_svc_retry_ok",
		`{"functionName":"`+fn+`","retryCount":2}`)
	_, rel, err := runChain(t, engine, types.NewMsgWithJsonData(`{}`))
	require.NoError(t, err)
	assert.Equal(t, types.Success, rel)
	assert.Equal(t, int32(3), atomic.LoadInt32(&calls), "两次瞬时失败后第三次应成功")
}

// 业务错误（非瞬时）不重试：函数只执行 1 次，直接 Failure。
func TestServiceTask_NoRetryForBusinessError(t *testing.T) {
	defer speedUpRetryBackoff(t)()
	const fn = "test:svc:retry:business"
	action.Functions.UnRegister(fn)
	defer action.Functions.UnRegister(fn)

	var calls int32
	action.Functions.Register(fn, func(ctx types.RuleContext, msg types.RuleMsg) {
		atomic.AddInt32(&calls, 1)
		ctx.TellFailure(msg, errors.New("参数校验失败：金额必须大于 0"))
	})

	engine := buildServiceEngine(t, "t_svc_retry_biz",
		`{"functionName":"`+fn+`","retryCount":2}`)
	_, rel, err := runChain(t, engine, types.NewMsgWithJsonData(`{}`))
	require.Error(t, err)
	assert.Equal(t, types.Failure, rel)
	assert.Equal(t, int32(1), atomic.LoadInt32(&calls), "业务失败不得重试")
}

// 预算耗尽：错误串带「重试 N 次后失败」。
func TestServiceTask_RetryExhaustedReportsCount(t *testing.T) {
	defer speedUpRetryBackoff(t)()
	const fn = "test:svc:retry:exhaust"
	action.Functions.UnRegister(fn)
	defer action.Functions.UnRegister(fn)

	var calls int32
	action.Functions.Register(fn, func(ctx types.RuleContext, msg types.RuleMsg) {
		atomic.AddInt32(&calls, 1)
		ctx.TellFailure(msg, errors.New("read timeout"))
	})

	engine := buildServiceEngine(t, "t_svc_retry_exhaust",
		`{"functionName":"`+fn+`","retryCount":2}`)
	_, rel, err := runChain(t, engine, types.NewMsgWithJsonData(`{}`))
	require.Error(t, err)
	assert.Equal(t, types.Failure, rel)
	assert.Equal(t, int32(3), atomic.LoadInt32(&calls), "首试+2 次重试=3 次执行")
	assert.Contains(t, err.Error(), "重试 2 次后失败")
}

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
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var httpRegisterOnce sync.Once

// registerHttpCallForTest 注册 HttpCallNode 原型(全局 registry 只注册一次,幂等)。
func registerHttpCallForTest(t *testing.T) {
	t.Helper()
	httpRegisterOnce.Do(func() {
		if err := rulego.Registry.Register(&HttpCallNode{}); err != nil {
			if !strings.Contains(err.Error(), "already exists") {
				require.NoError(t, err)
			}
		}
	})
}

// buildHttpEngine 构造一条仅含 httpCall 节点的引擎。
func buildHttpEngine(t *testing.T, chainID, configJSON string) types.RuleEngine {
	t.Helper()
	def := `{
		"ruleChain": {"id": "` + chainID + `", "name": "main", "root": true},
		"metadata": {
			"firstNodeIndex": 0,
			"nodes": [{"id": "h", "type": "httpCall", "name": "HTTP", "configuration": ` + configJSON + `}],
			"connections": []
		}
	}`
	engine, err := rulego.New(chainID, []byte(def), rulego.WithConfig(rulego.NewConfig()))
	require.NoError(t, err)
	return engine
}

func parseData(t *testing.T, s string) map[string]interface{} {
	t.Helper()
	m := map[string]interface{}{}
	require.NoError(t, json.Unmarshal([]byte(s), &m))
	return m
}

func TestHttpCallNode_Type(t *testing.T) {
	assert.Equal(t, "httpCall", (&HttpCallNode{}).Type())
}

func TestHttpCallNode_MappingsExtract(t *testing.T) {
	registerHttpCallForTest(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"code":0,"data":{"score":85}}`))
	}))
	defer srv.Close()

	cfg := `{"url":"` + srv.URL + `","method":"GET","outputMappings":[{"from":"$.data.score","to":"score"}]}`
	engine := buildHttpEngine(t, "t_http_mappings", cfg)
	msg := types.NewMsgWithJsonData(`{"days":3}`)

	endMsg, rel, err := runChain(t, engine, msg)
	require.NoError(t, err)
	assert.Equal(t, types.Success, rel)

	m := parseData(t, endMsg.GetData())
	assert.Equal(t, float64(85), m["score"])
	assert.Equal(t, float64(3), m["days"]) // 表单保留
}

func TestHttpCallNode_FlattenDefault(t *testing.T) {
	registerHttpCallForTest(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"score":85,"level":"A"}`))
	}))
	defer srv.Close()

	cfg := `{"url":"` + srv.URL + `","method":"GET"}`
	engine := buildHttpEngine(t, "t_http_flatten", cfg)
	msg := types.NewMsgWithJsonData(`{"days":3,"name":"li"}`)

	endMsg, rel, err := runChain(t, engine, msg)
	require.NoError(t, err)
	assert.Equal(t, types.Success, rel)

	m := parseData(t, endMsg.GetData())
	assert.Equal(t, float64(85), m["score"]) // 默认全平铺
	assert.Equal(t, "A", m["level"])
	assert.Equal(t, float64(3), m["days"]) // 表单保留
	assert.Equal(t, "li", m["name"])
}

func TestHttpCallNode_VariableSubstitution(t *testing.T) {
	registerHttpCallForTest(t)
	var gotPath, gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	// url 路径 + header value 都用 ${msg.xxx} 变量替换
	cfg := `{"url":"` + srv.URL + `/${msg.userId}","method":"GET","headers":{"Authorization":"Bearer ${msg.token}"}}`
	engine := buildHttpEngine(t, "t_http_vars", cfg)
	msg := types.NewMsgWithJsonData(`{"userId":"u123","token":"xyz"}`)

	_, rel, err := runChain(t, engine, msg)
	require.NoError(t, err)
	assert.Equal(t, types.Success, rel)
	assert.Equal(t, "/u123", gotPath)
	assert.Equal(t, "Bearer xyz", gotAuth)
}

// 统一输出模型（与 aiAgent 同一套三规则）：默认平铺模式下完整响应也始终在 msg._http。
func TestHttpCallNode_OutputAlwaysReservedWhenFlatten(t *testing.T) {
	registerHttpCallForTest(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"score":85,"level":"A"}`))
	}))
	defer srv.Close()

	cfg := `{"url":"` + srv.URL + `","method":"GET"}`
	engine := buildHttpEngine(t, "t_http_reserved", cfg)
	msg := types.NewMsgWithJsonData(`{"days":3}`)

	endMsg, rel, err := runChain(t, engine, msg)
	require.NoError(t, err)
	assert.Equal(t, types.Success, rel)

	m := parseData(t, endMsg.GetData())
	assert.Equal(t, float64(85), m["score"]) // 平铺默认开
	raw, ok := m["_http"].(map[string]interface{})
	require.True(t, ok, "full response must always be kept under _http")
	assert.Equal(t, float64(85), raw["score"])
	assert.Equal(t, float64(3), m["days"])
}

// 隔离模式：响应不碰表单顶层，完整响应只在 _http；映射仍生效（两种模式下都执行）。
func TestHttpCallNode_IsolationMode(t *testing.T) {
	registerHttpCallForTest(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"days":99,"score":85}`))
	}))
	defer srv.Close()

	cfg := `{"url":"` + srv.URL + `","method":"GET","flattenOutput":false,"outputMappings":[{"from":"score","to":"riskScore"}]}`
	engine := buildHttpEngine(t, "t_http_isolation", cfg)
	msg := types.NewMsgWithJsonData(`{"days":3}`)

	endMsg, rel, err := runChain(t, engine, msg)
	require.NoError(t, err)
	assert.Equal(t, types.Success, rel)

	m := parseData(t, endMsg.GetData())
	assert.Equal(t, float64(3), m["days"], "isolation must not overwrite form field")
	_, polluted := m["score"]
	assert.False(t, polluted, "isolation: response fields stay inside _http")
	assert.Equal(t, float64(85), m["riskScore"], "mappings still execute in isolation mode")
	raw, ok := m["_http"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, float64(99), raw["days"])
}

func TestHttpCallNode_NonJsonReserved(t *testing.T) {
	registerHttpCallForTest(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`plain text response`))
	}))
	defer srv.Close()

	cfg := `{"url":"` + srv.URL + `","method":"GET"}`
	engine := buildHttpEngine(t, "t_http_text", cfg)
	msg := types.NewMsgWithJsonData(`{"days":3}`)

	endMsg, rel, err := runChain(t, engine, msg)
	require.NoError(t, err)
	assert.Equal(t, types.Success, rel)

	m := parseData(t, endMsg.GetData())
	assert.Equal(t, "plain text response", m["_http"]) // 非 JSON 整体挂保留 key
	assert.Equal(t, float64(3), m["days"])             // 表单保留
}

func TestHttpCallNode_4xxFailure(t *testing.T) {
	registerHttpCallForTest(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(`upstream down`))
	}))
	defer srv.Close()

	cfg := `{"url":"` + srv.URL + `","method":"GET"}`
	engine := buildHttpEngine(t, "t_http_4xx", cfg)
	msg := types.NewMsgWithJsonData(`{"days":3}`)

	_, rel, err := runChain(t, engine, msg)
	require.Error(t, err)
	assert.Equal(t, types.Failure, rel)
}

func TestHttpCallNode_PostBody(t *testing.T) {
	registerHttpCallForTest(t)
	var gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, 1024)
		n, _ := r.Body.Read(buf)
		gotBody = string(buf[:n])
		_, _ = w.Write([]byte(`{"received":true}`))
	}))
	defer srv.Close()

	// body 模板用 ${msg.amount}
	cfg := `{"url":"` + srv.URL + `","method":"POST","headers":{"Content-Type":"application/json"},"body":"{\"amount\":${msg.amount}}"}`
	engine := buildHttpEngine(t, "t_http_post", cfg)
	msg := types.NewMsgWithJsonData(`{"amount":500}`)

	_, rel, err := runChain(t, engine, msg)
	require.NoError(t, err)
	assert.Equal(t, types.Success, rel)
	assert.Equal(t, `{"amount":500}`, gotBody)
}

// SSRF 防护：非 http/https scheme（file/gopher/ftp/dict 等）经 ${msg.xxx} 注入或直接配置时，
// 必须在发请求前被拦截 → TellFailure，杜绝 SSRF（读本地文件/打内网非 HTTP 服务等）。
func TestHttpCall_SchemeBlocked(t *testing.T) {
	registerHttpCallForTest(t)
	for i, badURL := range []string{"file:///etc/passwd", "gopher://x:1", "ftp://host", "dict://x:1"} {
		cfg := `{"url":"` + badURL + `","method":"GET"}`
		engine := buildHttpEngine(t, fmt.Sprintf("t_ssrf_%d", i), cfg)
		msg := types.NewMsgWithJsonData(`{}`)
		_, rel, runErr := runChain(t, engine, msg)
		require.Error(t, runErr, "scheme %s 应被拦截", badURL)
		assert.Equal(t, types.Failure, rel, "scheme %s 应 Failure", badURL)
	}
}

// 超时：响应慢于 timeoutMs → TellFailure（不永久挂起）。
func TestHttpCall_Timeout(t *testing.T) {
	registerHttpCallForTest(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(500 * time.Millisecond)
		_, _ = w.Write([]byte(`{"score":1}`))
	}))
	defer srv.Close()

	cfg := `{"url":"` + srv.URL + `","method":"GET","timeoutMs":100}`
	engine := buildHttpEngine(t, "t_http_timeout", cfg)
	msg := types.NewMsgWithJsonData(`{"days":3}`)

	_, rel, runErr := runChain(t, engine, msg)
	require.Error(t, runErr)
	assert.Equal(t, types.Failure, rel)
}

// TestHttpCallNode_SSRFSchemeRejected: 非 http/https scheme（经 ${msg.xxx} 注入的 file:// 等）
// 在发请求前被 SSRF 基线防护拦截 → TellFailure，不发起任何网络调用。
func TestHttpCallNode_SSRFSchemeRejected(t *testing.T) {
	registerHttpCallForTest(t)
	cfg := `{"url":"${msg.url}","method":"GET"}`
	engine := buildHttpEngine(t, "t_http_ssrf", cfg)
	msg := types.NewMsgWithJsonData(`{"url":"file:///etc/passwd"}`)

	_, rel, err := runChain(t, engine, msg)
	require.Error(t, err)
	assert.Equal(t, types.Failure, rel)
}

// SSRF 主机防护：白名单 + 动态主机危险地址拦截 + 重定向复校验

func TestUrlHostIsDynamic(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want bool
	}{
		{"fully dynamic url", "${msg.url}", true},
		{"dynamic host", "http://${msg.host}/api", true},
		{"dynamic host with port", "https://${metadata.host}:8080/x", true},
		{"static host dynamic path", "http://api.internal/${msg.path}", false},
		{"static host dynamic query", "http://api.internal/x?id=${msg.id}", false},
		{"static url", "http://api.internal/x", false},
		{"userinfo dynamic host", "http://u:p@${msg.host}/x", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := urlHostIsDynamic(c.in); got != c.want {
				t.Errorf("urlHostIsDynamic(%q) = %v, want %v", c.in, got, c.want)
			}
		})
	}
}

func TestIsBlockedSSRFIP(t *testing.T) {
	cases := []struct {
		name         string
		ip           string
		blockPrivate bool
		want         bool
	}{
		{"loopback v4", "127.0.0.1", false, true},
		{"loopback v6", "::1", false, true},
		{"cloud metadata link-local", "169.254.169.254", false, true},
		{"link-local v6", "fe80::1", false, true},
		{"unspecified", "0.0.0.0", false, true},
		{"public", "8.8.8.8", false, false},
		{"private 10 default allowed", "10.1.2.3", false, false},
		{"private 172 default allowed", "172.16.0.9", false, false},
		{"private 192 default allowed", "192.168.1.5", false, false},
		{"private 10 blocked when opt-in", "10.1.2.3", true, true},
		{"private 192 blocked when opt-in", "192.168.1.5", true, true},
		{"public unaffected by opt-in", "8.8.8.8", true, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ip := net.ParseIP(c.ip)
			require.NotNil(t, ip)
			if got := isBlockedSSRFIP(ip, c.blockPrivate); got != c.want {
				t.Errorf("isBlockedSSRFIP(%s, %v) = %v, want %v", c.ip, c.blockPrivate, got, c.want)
			}
		})
	}
}

// 白名单：未命中主机的请求直接 TellFailure，不发起网络调用。
func TestHttpCall_AllowedHostsBlock(t *testing.T) {
	registerHttpCallForTest(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("request must not reach the server when host not allowed")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	cfg := `{"url":"` + srv.URL + `","method":"GET","allowedHosts":["example.com"]}`
	engine := buildHttpEngine(t, "t_http_allow_block", cfg)
	_, rel, err := runChain(t, engine, types.NewMsgWithJsonData(`{}`))
	require.Error(t, err)
	assert.Equal(t, types.Failure, rel)
	assert.Contains(t, err.Error(), "allowedHosts")
}

// 白名单：命中时正常放行（显式信任，不叠加地址拦截，内网/回环地址也可用）。
func TestHttpCall_AllowedHostsAllow(t *testing.T) {
	registerHttpCallForTest(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	u, err := url.Parse(srv.URL)
	require.NoError(t, err)
	cfg := `{"url":"` + srv.URL + `","method":"GET","allowedHosts":["` + u.Host + `"]}`
	engine := buildHttpEngine(t, "t_http_allow_pass", cfg)
	_, rel, runErr := runChain(t, engine, types.NewMsgWithJsonData(`{}`))
	require.NoError(t, runErr)
	assert.Equal(t, types.Success, rel)
}

// 动态主机（${msg.url} 整体注入）：回环地址在发请求前被拦截。
func TestHttpCall_DynamicHostLoopbackBlocked(t *testing.T) {
	registerHttpCallForTest(t)
	cfg := `{"url":"${msg.url}","method":"GET"}`
	engine := buildHttpEngine(t, "t_http_dyn_loopback", cfg)
	msg := types.NewMsgWithJsonData(`{"url":"http://127.0.0.1:9/secret"}`)

	_, rel, err := runChain(t, engine, msg)
	require.Error(t, err)
	assert.Equal(t, types.Failure, rel)
	assert.Contains(t, err.Error(), "blocked address")
}

// 动态主机：云元数据地址（169.254.169.254）同样被拦截。
func TestHttpCall_DynamicHostMetadataBlocked(t *testing.T) {
	registerHttpCallForTest(t)
	cfg := `{"url":"http://${msg.host}/latest/meta-data/","method":"GET"}`
	engine := buildHttpEngine(t, "t_http_dyn_meta", cfg)
	msg := types.NewMsgWithJsonData(`{"host":"169.254.169.254"}`)

	_, rel, err := runChain(t, engine, msg)
	require.Error(t, err)
	assert.Equal(t, types.Failure, rel)
	assert.Contains(t, err.Error(), "blocked address")
}

// 重定向复校验：配置白名单后，30x 跳转目标未命中白名单 → Failure。
func TestHttpCall_RedirectBlockedByAllowedHosts(t *testing.T) {
	registerHttpCallForTest(t)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("redirect target must not be reached when not allowed")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer target.Close()
	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	defer redirector.Close()

	u, err := url.Parse(redirector.URL)
	require.NoError(t, err)
	cfg := `{"url":"` + redirector.URL + `","method":"GET","allowedHosts":["` + u.Host + `"]}`
	engine := buildHttpEngine(t, "t_http_redirect_block", cfg)
	_, rel, runErr := runChain(t, engine, types.NewMsgWithJsonData(`{}`))
	require.Error(t, runErr)
	assert.Equal(t, types.Failure, rel)
	assert.Contains(t, runErr.Error(), "allowedHosts")
}

// 重定向复校验：跳转目标也在白名单内时正常跟随。
func TestHttpCall_RedirectAllowedByAllowedHosts(t *testing.T) {
	registerHttpCallForTest(t)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer target.Close()
	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	defer redirector.Close()

	ru, err := url.Parse(redirector.URL)
	require.NoError(t, err)
	tu, err := url.Parse(target.URL)
	require.NoError(t, err)
	cfg := `{"url":"` + redirector.URL + `","method":"GET","allowedHosts":["` + ru.Host + `","` + tu.Host + `"]}`
	engine := buildHttpEngine(t, "t_http_redirect_pass", cfg)
	_, rel, runErr := runChain(t, engine, types.NewMsgWithJsonData(`{}`))
	require.NoError(t, runErr)
	assert.Equal(t, types.Success, rel)
}

// httpCall + switch 协作：查询外部接口拿字段 → 写入 msg.Data → switch 按该字段分支。

// TestHttpCallThenSwitch_RoutesByQueriedField 验证核心端到端路径:
// httpCall 查询外部接口拿 score → 写入 msg.Data → switch 按 msg.score 分支 → 表单字段(days)保留不被冲掉。
//
// 用真实 rulego engine 跑 httpCall+switch 节点协作。
func TestHttpCallThenSwitch_RoutesByQueriedField(t *testing.T) {
	registerHttpCallForTest(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"score":85}`)) // 模拟外部接口返回
	}))
	defer srv.Close()

	def := `{
		"ruleChain": {"id": "t_http_switch", "name": "main", "root": true},
		"metadata": {
			"firstNodeIndex": 0,
			"nodes": [
				{"id": "h", "type": "httpCall", "name": "query", "configuration": {"url": "` + srv.URL + `", "method": "GET", "outputMappings": [{"from": "$.score", "to": "score"}]}},
				{"id": "s", "type": "switch", "name": "byScore", "configuration": {"cases": [{"case": "msg.score > 80", "then": "high"}, {"case": "msg.score <= 80", "then": "low"}]}}
			],
			"connections": [
				{"fromId": "h", "toId": "s", "type": "Success"}
			]
		}
	}`
	engine, err := rulego.New("t_http_switch", []byte(def), rulego.WithConfig(rulego.NewConfig()))
	require.NoError(t, err)

	// msg.Data 带表单字段 days,验证它不会被 httpCall 输出冲掉
	msg := types.NewMsgWithJsonData(`{"days":3,"reason":"vacation"}`)

	endMsg, rel, err := runChain(t, engine, msg)
	require.NoError(t, err)
	assert.Equal(t, "high", rel) // score=85 > 80 → 走 high 分支

	m := parseData(t, endMsg.GetData())
	assert.Equal(t, float64(85), m["score"]) // httpCall 查回的字段
	assert.Equal(t, float64(3), m["days"])   // 表单字段保留
	assert.Equal(t, "vacation", m["reason"]) // 表单字段保留
}

// TestHttpCallThenSwitch_LowBranch 对照组:score 低走 low 分支。
func TestHttpCallThenSwitch_LowBranch(t *testing.T) {
	registerHttpCallForTest(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"score":50}`))
	}))
	defer srv.Close()

	def := `{
		"ruleChain": {"id": "t_http_switch_low", "root": true},
		"metadata": {
			"firstNodeIndex": 0,
			"nodes": [
				{"id": "h", "type": "httpCall", "configuration": {"url": "` + srv.URL + `", "method": "GET", "outputMappings": [{"from": "$.score", "to": "score"}]}},
				{"id": "s", "type": "switch", "configuration": {"cases": [{"case": "msg.score > 80", "then": "high"}, {"case": "msg.score <= 80", "then": "low"}]}}
			],
			"connections": [{"fromId": "h", "toId": "s", "type": "Success"}]
		}
	}`
	engine, err := rulego.New("t_http_switch_low", []byte(def), rulego.WithConfig(rulego.NewConfig()))
	require.NoError(t, err)

	msg := types.NewMsgWithJsonData(`{"days":3}`)
	_, rel, err := runChain(t, engine, msg)
	require.NoError(t, err)
	assert.Equal(t, "low", rel) // score=50 ≤ 80 → 走 low
}

// 白名单按字面 IP 信任：显式指定地址时拨号期不复验，回环回调可用。
// （与 TestHttpCall_AllowedHostsAllow 同场景，验证字面 IP 白名单的语义。）
func TestHttpCall_AllowedHostsLiteralIP_DialAllowed(t *testing.T) {
	registerHttpCallForTest(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	u, err := url.Parse(srv.URL)
	require.NoError(t, err)
	require.NotNil(t, net.ParseIP(u.Hostname()), "httptest 地址应为 IP 字面量")
	cfg := `{"url":"` + srv.URL + `","method":"GET","allowedHosts":["` + u.Hostname() + `"]}`
	engine := buildHttpEngine(t, "t_http_allow_literal", cfg)
	_, rel, runErr := runChain(t, engine, types.NewMsgWithJsonData(`{}`))
	require.NoError(t, runErr, "literal-IP whitelist must bypass the dial-time guard")
	assert.Equal(t, types.Success, rel)
}

// 白名单按域名信任：DNS 被劫持到回环地址时，拨号期守卫必须拦截
// （字面 IP 白名单不受影响，见上一个用例）。
func TestHttpCall_AllowedHostsByName_LoopbackBlockedAtDial(t *testing.T) {
	registerHttpCallForTest(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("whitelisted-by-name host must not reach loopback via hijacked DNS")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	u, err := url.Parse(srv.URL)
	require.NoError(t, err)
	// "localhost" 解析到 127.0.0.1——模拟"白名单域名被 DNS 劫持指向回环/元数据"的形态
	cfg := `{"url":"http://localhost:` + u.Port() + `","method":"GET","allowedHosts":["localhost"]}`
	engine := buildHttpEngine(t, "t_http_allow_name_hijack", cfg)
	_, rel, runErr := runChain(t, engine, types.NewMsgWithJsonData(`{}`))
	require.Error(t, runErr, "name-whitelisted host resolving to loopback must be blocked at dial time")
	assert.Equal(t, types.Failure, rel)
	assert.Contains(t, runErr.Error(), "blocked address")
}

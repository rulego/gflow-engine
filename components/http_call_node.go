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
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/rulego/gflow-engine/types/constants"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/components/base"
	"github.com/rulego/rulego/utils/el"
	"github.com/rulego/rulego/utils/maps"
)

// HttpCallNodeType HTTP 调用节点类型。
// 响应经 MergeAgentOutput 三规则合并：完整响应始终在 msg._http，平铺模式（默认）
// 顶层字段并入 msg.Data，映射最后执行；原有表单字段保留（同名被显式覆盖除外）。
const HttpCallNodeType = "httpCall"

// maxHTTPResponseBytes 限制 HTTP 响应体读取大小，防恶意/异常大响应导致内存暴涨/OOM。
const maxHTTPResponseBytes = 10 << 20 // 10MB

// defaultHTTPTimeoutMs 未配置 timeoutMs 时的默认超时（毫秒）
const defaultHTTPTimeoutMs = 10000

// SSRF 基线防护：仅允许的 URL scheme（拦截 file://、gopher:// 等经 ${msg.xxx} 注入的 scheme）。
const (
	schemeHTTP  = "http"
	schemeHTTPS = "https"
)

// HttpCallNodeConfiguration HTTP 调用节点配置。
type HttpCallNodeConfiguration struct {
	// Url 请求地址（必填），支持 ${msg.xxx}/${metadata.xxx} 变量替换；
	// 仅允许 http/https scheme
	Url string `json:"url"`
	// Method HTTP 方法，默认 GET；自动转为大写，不做方法白名单
	Method string `json:"method"`
	// Headers 请求头,value 支持 ${msg.xxx} 变量替换
	Headers map[string]string `json:"headers"`
	// Body 请求体模板,支持 ${msg.xxx} 变量替换;空则不发 body
	Body string `json:"body"`
	// TimeoutMs 超时毫秒,默认 10000
	TimeoutMs int `json:"timeoutMs"`
	// OutputMappings 响应字段映射；在输出模式与平铺之后最后执行（优先级最高），见 MergeAgentOutput
	OutputMappings []OutputMapping `json:"outputMappings"`
	// FlattenOutput 输出模式：true=平铺（响应对象顶层字段并入 msg.Data 顶层，同名覆盖表单，
	// httpCall 主用途是查接口补全数据，默认平铺）；false=隔离（完整响应只放 ReservedKey 下，不碰表单）。
	// 两种模式下完整响应都会保留在 msg.<ReservedKey>。
	FlattenOutput *bool `json:"flattenOutput"`
	// ReservedKey 完整响应写入的 msg.Data key（对象存对象、非对象存原文），默认 "_http"。
	// 存量配置可自定义；设计器不再暴露此字段。
	ReservedKey string `json:"reservedKey"`

	// SSRF 防护（可选）
	// AllowedHosts 允许访问的主机白名单。为空时不做主机限制。
	// 非空时:渲染后的 URL 主机必须命中白名单(条目支持 "host" 或 "host:port",不区分大小写),
	// 且每一跳 30x 重定向的目标也会重新校验(CheckRedirect),未命中即 TellFailure。
	// 命中白名单的主机视为设计者显式信任,跳过动态主机危险地址拦截。
	AllowedHosts []string `json:"allowedHosts"`
	// BlockPrivateNetworks 是否拦截 RFC1918 私有网段(10/8、172.16/12、192.168/16)。默认 false:
	// BPM 流程常需调用内网服务,默认放行最不意外;显式置 true 才拦截。
	// 回环(127.0.0.0/8、::1)、链路本地/云元数据(169.254.0.0/16)、未指定与组播地址
	// 在 URL 主机含动态变量(${...})时始终拦截,不受本开关影响。
	BlockPrivateNetworks bool `json:"blockPrivateNetworks"`

	// 危险项：默认关闭，仅支持通过 DSL 配置
	// InsecureSkipVerify 跳过 HTTPS 证书校验，默认 false
	InsecureSkipVerify bool `json:"insecureSkipVerify"`
	// ProxyUrl http/https 代理地址（仅支持这两种代理协议，不支持 SOCKS5）
	ProxyUrl string `json:"proxyUrl"`
}

// HttpCallNode BPM 内联 HTTP 调用节点。
//
// 执行流程：渲染 url/headers/body（rulego el 模板）→ 发送请求（见 http_client.go）
// → 状态码 ≥400 走 Failure → MergeAgentOutput 把响应合并进 msg.Data → TellSuccess。
// 合并语义与 aiAgent 同一套三规则：完整响应始终在 msg._http（审计/兜底可取）→
// 平铺模式（默认）顶层字段并入 msg.Data → 映射最后执行（优先级最高）。
type HttpCallNode struct {
	Config HttpCallNodeConfiguration
	// CurrentNodeDef 当前节点定义快照，供审计日志取节点 ID
	CurrentNodeDef types.RuleNode
	client         *http.Client
	urlTmpl        el.Template
	bodyTmpl       el.Template
	headerTmpl     map[string]el.Template

	// allowedHostSet 小写化的主机白名单（含 "host" 与 "host:port" 两种条目形态）
	allowedHostSet map[string]bool
	// hostIsDynamic URL 模板的主机部分是否含 ${...} 变量；仅主机动态时才启用危险地址拦截
	hostIsDynamic bool
}

// GetSelfId 当前节点 ID（CurrentNodeDef 缺失时退回节点类型）
func (n *HttpCallNode) GetSelfId() string {
	return selfID(n.CurrentNodeDef, n.Type())
}

// Type 组件类型,前端 NODE_TYPES.HTTP 对齐
func (n *HttpCallNode) Type() string { return HttpCallNodeType }

// New 创建实例
func (n *HttpCallNode) New() types.Node { return &HttpCallNode{} }

// Init 初始化:校验配置、预编译 el 模板、构造 http client
func (n *HttpCallNode) Init(_ types.Config, cfg types.Configuration) error {
	if err := maps.Map2Struct(cfg, &n.Config); err != nil {
		return err
	}
	n.CurrentNodeDef = base.NodeUtils.GetSelfDefinition(cfg)
	if strings.TrimSpace(n.Config.Url) == "" {
		return fmt.Errorf("httpCall node url is required")
	}
	if strings.TrimSpace(n.Config.Method) == "" {
		n.Config.Method = "GET"
	}
	n.Config.Method = strings.ToUpper(n.Config.Method)
	if n.Config.TimeoutMs <= 0 {
		n.Config.TimeoutMs = defaultHTTPTimeoutMs
	}
	if strings.TrimSpace(n.Config.ReservedKey) == "" {
		n.Config.ReservedKey = HTTPReservedKey
	}

	// 预编译模板(支持 ${msg.xxx}/${metadata.xxx} 变量替换)
	t, err := el.NewTemplate(n.Config.Url)
	if err != nil {
		return fmt.Errorf("invalid url template: %w", err)
	}
	n.urlTmpl = t

	if strings.TrimSpace(n.Config.Body) != "" {
		bt, err := el.NewTemplate(n.Config.Body)
		if err != nil {
			return fmt.Errorf("invalid body template: %w", err)
		}
		n.bodyTmpl = bt
	}

	n.headerTmpl = make(map[string]el.Template, len(n.Config.Headers))
	for k, v := range n.Config.Headers {
		ht, err := el.NewTemplate(v)
		if err != nil {
			return fmt.Errorf("invalid header %q template: %w", k, err)
		}
		n.headerTmpl[k] = ht
	}

	// SSRF 防护初始化：主机白名单（空=不限制）+ URL 主机是否由动态变量渲染
	n.allowedHostSet = make(map[string]bool, len(n.Config.AllowedHosts))
	for _, h := range n.Config.AllowedHosts {
		if h = strings.TrimSpace(h); h != "" {
			n.allowedHostSet[strings.ToLower(h)] = true
		}
	}
	n.hostIsDynamic = urlHostIsDynamic(n.Config.Url)

	c, err := newHTTPClient(n.Config.TimeoutMs, n.Config.InsecureSkipVerify, n.Config.ProxyUrl)
	if err != nil {
		return err
	}
	// 重定向目标与初始 URL 同规则校验，防 30x 跳转绕过 SSRF 防护
	c.CheckRedirect = n.checkRedirect
	// DNS rebinding 防护：解析结果与实际拨号之间可能被换址（TTL/攻击），拨号时
	// 解析并直连首个放行 IP（TLS 的 SNI/证书校验仍按原主机名，不受影响）。
	// 触发条件：动态主机，或配置了白名单——白名单按【域名】信任时，DNS 仍可能被
	// 劫持到 169.254.169.254（云元数据）/回环地址，拨号期保留回环/链路本地/元数据
	// 段的兜底拦截；白名单按【字面 IP（可带端口）】信任时视为显式指定地址，跳过
	// 拨号期复验（内网/回环回调是合法用法，由配置者显式负责）。
	// 纯静态 URL 且未配置白名单：DSL 作者显式写死的目标视为完全可信，不安装拨号守卫。
	if n.hostIsDynamic || len(n.allowedHostSet) > 0 {
		if tr, ok := c.Transport.(*http.Transport); ok {
			baseDial := tr.DialContext
			if baseDial == nil {
				baseDial = http.DefaultTransport.(*http.Transport).DialContext
			}
			blockPrivate := n.Config.BlockPrivateNetworks
			tr.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
				host, port, err := net.SplitHostPort(addr)
				if err != nil {
					return nil, fmt.Errorf("httpCall dial address %q: %w", addr, err)
				}
				// 字面地址显式信任：白名单命中该 IP（含 host:port 形态）时不复验
				if n.hostAllowedAddr(host, port) {
					return baseDial(ctx, network, addr)
				}
				var ips []net.IP
				if ip := net.ParseIP(host); ip != nil {
					ips = []net.IP{ip}
				} else if resolved, rerr := net.DefaultResolver.LookupIPAddr(ctx, host); rerr == nil {
					for _, ia := range resolved {
						ips = append(ips, ia.IP)
					}
				}
				var approved net.IP
				for _, ip := range ips {
					if !isBlockedSSRFIP(ip, blockPrivate) {
						approved = ip
						break
					}
				}
				if approved == nil {
					return nil, fmt.Errorf("httpCall host %q resolves to blocked address(es) at dial time", host)
				}
				return baseDial(ctx, network, net.JoinHostPort(approved.String(), port))
			}
		}
	}
	n.client = c
	return nil
}

// OnMsg 渲染请求 → 发送 → 合并响应到 msg → TellSuccess/Failure
func (n *HttpCallNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
	defer recoverNodePanic(ctx, msg, HttpCallNodeType, n.GetSelfId())
	start := time.Now()
	evn := base.NodeUtils.GetEvnAndMetadata(ctx, msg)

	endpoint := n.urlTmpl.ExecuteAsString(evn)

	// 仅允许 http/https scheme（SSRF 基线防护）。
	u, perr := url.Parse(endpoint)
	if perr != nil || (u.Scheme != schemeHTTP && u.Scheme != schemeHTTPS) {
		n.auditLog(msg, endpoint, 0, start, fmt.Errorf("scheme not allowed"))
		ctx.TellFailure(msg, fmt.Errorf("httpCall url scheme not allowed: %q", endpoint))
		return
	}
	// 主机校验：白名单（如配置）+ 动态主机危险地址拦截
	if err := n.validateURLHost(ctx.GetContext(), u); err != nil {
		n.auditLog(msg, endpoint, 0, start, err)
		ctx.TellFailure(msg, err)
		return
	}

	var bodyReader io.Reader
	if n.bodyTmpl != nil {
		bodyReader = bytes.NewReader([]byte(n.bodyTmpl.ExecuteAsString(evn)))
	}

	req, err := http.NewRequestWithContext(ctx.GetContext(), n.Config.Method, endpoint, bodyReader)
	if err != nil {
		n.auditLog(msg, endpoint, 0, start, fmt.Errorf("build http request: %w", err))
		ctx.TellFailure(msg, fmt.Errorf("build http request: %w", err))
		return
	}
	for k, t := range n.headerTmpl {
		req.Header.Set(k, t.ExecuteAsString(evn))
	}

	resp, err := n.client.Do(req)
	if err != nil {
		err = fmt.Errorf("http call failed: %w", err)
		n.auditLog(msg, endpoint, 0, start, err)
		ctx.TellFailure(msg, err)
		return
	}
	defer resp.Body.Close()
	// 多读 1 字节判断截断：静默截断的 JSON 会以"纯文本"形态混入 msg，宁可显式失败
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxHTTPResponseBytes+1))
	if err != nil {
		n.auditLog(msg, endpoint, resp.StatusCode, start, fmt.Errorf("read http response: %w", err))
		ctx.TellFailure(msg, fmt.Errorf("read http response: %w", err))
		return
	}
	if len(body) > maxHTTPResponseBytes {
		err := fmt.Errorf("http response exceeds %d bytes limit", maxHTTPResponseBytes)
		n.auditLog(msg, endpoint, resp.StatusCode, start, err)
		ctx.TellFailure(msg, err)
		return
	}

	if resp.StatusCode >= 400 {
		// 响应体可能很大（上限 10MB），截断进错误串避免撑爆实例终止 reason
		snippet := string(body)
		if len(snippet) > 1024 {
			snippet = snippet[:1024] + "...(truncated)"
		}
		err := fmt.Errorf("http %d: %s", resp.StatusCode, snippet)
		n.auditLog(msg, endpoint, resp.StatusCode, start, err)
		ctx.TellFailure(msg, err)
		return
	}

	// 输出合并与 aiAgent 同一套三规则（MergeAgentOutput）：完整响应始终写 ReservedKey(默认 _http)，
	// 平铺模式(默认)再把对象顶层字段并入 msg.Data，映射最后执行（优先级最高）。
	flatten := n.Config.FlattenOutput == nil || *n.Config.FlattenOutput
	if err := MergeAgentOutput(&msg, body, n.Config.OutputMappings, n.Config.ReservedKey, flatten); err != nil {
		ctx.TellFailure(msg, fmt.Errorf("merge http output: %w", err))
		return
	}
	n.auditLog(msg, endpoint, resp.StatusCode, start, nil)
	ctx.TellSuccess(msg)
}

// auditLog 结构化审计日志：线上排障主要靠它（实例终止 reason 只有一条）
func (n *HttpCallNode) auditLog(msg types.RuleMsg, endpoint string, statusCode int, start time.Time, err error) {
	fields := logrus.Fields{
		"nodeType":   n.Type(),
		"nodeId":     n.GetSelfId(),
		"method":     n.Config.Method,
		"endpoint":   endpoint,
		"instanceId": metaValue(msg, constants.KeyInstanceID),
		"taskId":     metaValue(msg, constants.KeyTaskID),
		"statusCode": statusCode,
		"durationMs": time.Since(start).Milliseconds(),
		"timeoutMs":  n.Config.TimeoutMs,
	}
	if err != nil {
		fields["error"] = err.Error()
		logrus.WithFields(fields).Warn("HttpCallNode execution failed")
		return
	}
	logrus.WithFields(fields).Info("HttpCallNode execution")
}

func (n *HttpCallNode) Destroy() {}

// validateURLHost 校验渲染后的 URL 主机：
//   - 配置了 AllowedHosts 时主机必须命中白名单（命中即视为显式信任，跳过地址拦截）
//   - 未配置白名单且主机由动态变量渲染时，拦截回环/链路本地/元数据等危险地址
func (n *HttpCallNode) validateURLHost(ctx context.Context, u *url.URL) error {
	if strings.TrimSpace(u.Host) == "" {
		return fmt.Errorf("httpCall url host is empty: %q", u.String())
	}
	if len(n.allowedHostSet) > 0 {
		if !n.hostAllowed(u) {
			return fmt.Errorf("httpCall url host %q not in allowedHosts", u.Host)
		}
		return nil
	}
	if n.hostIsDynamic {
		return n.checkHostIPs(ctx, u.Hostname())
	}
	return nil
}

// checkRedirect 重定向目标校验：与初始 URL 同一套防护（scheme + 白名单 + 动态主机危险地址），
// 防止经 30x 跳转绕过。静态 URL 且未配置白名单时不做限制。
func (n *HttpCallNode) checkRedirect(req *http.Request, _ []*http.Request) error {
	u := req.URL
	if u.Scheme != schemeHTTP && u.Scheme != schemeHTTPS {
		return fmt.Errorf("httpCall redirect scheme not allowed: %q", u.String())
	}
	if len(n.allowedHostSet) > 0 {
		if !n.hostAllowed(u) {
			return fmt.Errorf("httpCall redirect host %q not in allowedHosts", u.Host)
		}
		return nil
	}
	if n.hostIsDynamic {
		return n.checkHostIPs(req.Context(), u.Hostname())
	}
	return nil
}

// hostAllowed 判断 URL 主机是否命中白名单；白名单条目支持 "host" 或 "host:port"（不区分大小写）。
func (n *HttpCallNode) hostAllowed(u *url.URL) bool {
	if n.allowedHostSet[strings.ToLower(u.Host)] {
		return true
	}
	return n.allowedHostSet[strings.ToLower(u.Hostname())]
}

// hostAllowedAddr 判断拨号地址（无端口 host + port）是否命中白名单的【字面 IP】。
// 仅当 host 本身是 IP 字面量且在白名单内才算字面信任（显式指定地址，跳过拨号期复验，
// 内网/回环回调由配置者显式负责）；白名单按域名信任时返回 false，拨号期守卫照常
// 拦截回环/元数据段（防 DNS 劫持）。
func (n *HttpCallNode) hostAllowedAddr(host, port string) bool {
	if net.ParseIP(host) == nil {
		return false
	}
	h := strings.ToLower(host)
	if n.allowedHostSet[h] {
		return true
	}
	if port != "" {
		return n.allowedHostSet[strings.ToLower(net.JoinHostPort(host, port))]
	}
	return false
}

// checkHostIPs 解析主机并拦截 SSRF 危险地址。
// IP 字面量直接判定；域名先 DNS 解析再逐个判定（任一命中即拒绝，解析失败按拒绝处理）。
// ctx 用于 DNS 解析超时控制（3s）：慢 DNS 不能挂住工作流 goroutine。
func (n *HttpCallNode) checkHostIPs(ctx context.Context, host string) error {
	if host == "" {
		return fmt.Errorf("httpCall url host is empty")
	}
	var ips []net.IP
	if ip := net.ParseIP(host); ip != nil {
		ips = []net.IP{ip}
	} else {
		resolveCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		resolved, err := net.DefaultResolver.LookupIPAddr(resolveCtx, host)
		if err != nil {
			return fmt.Errorf("httpCall resolve host %q failed: %w", host, err)
		}
		for _, addr := range resolved {
			ips = append(ips, addr.IP)
		}
	}
	for _, ip := range ips {
		if isBlockedSSRFIP(ip, n.Config.BlockPrivateNetworks) {
			return fmt.Errorf("httpCall url host %q resolves to blocked address %s", host, ip)
		}
	}
	return nil
}

// isBlockedSSRFIP 判断 IP 是否属于 SSRF 危险地址段。
// 始终拦截：回环(127.0.0.0/8、::1)、链路本地/云元数据(169.254.0.0/16、fe80::/10)、未指定、组播。
// RFC1918 私有网段仅在 blockPrivate=true 时拦截（默认放行，避免误伤内网服务调用）。
func isBlockedSSRFIP(ip net.IP, blockPrivate bool) bool {
	if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
		ip.IsUnspecified() || ip.IsMulticast() {
		return true
	}
	return blockPrivate && ip.IsPrivate()
}

// urlHostIsDynamic 判断 URL 模板的主机部分是否含 ${...} 动态变量。
// 仅主机本身可能来自 msg/metadata 时才存在 SSRF 跳转风险，需要启用危险地址拦截；
// 主机固定、仅路径/查询含变量时不触发（如 http://api.internal/${msg.path}）。
// 无 scheme 的完整动态 URL（如 "${msg.url}"）视为主机动态。
func urlHostIsDynamic(urlTemplate string) bool {
	rest := urlTemplate
	if i := strings.Index(rest, "://"); i >= 0 {
		rest = rest[i+3:]
	}
	// 截取主机段：到首个 / ? # 为止
	if i := strings.IndexAny(rest, "/?#"); i >= 0 {
		rest = rest[:i]
	}
	// 去掉 user:pass@ 用户信息（少见，防御性处理）
	if i := strings.IndexByte(rest, '@'); i >= 0 {
		rest = rest[i+1:]
	}
	return strings.Contains(rest, "${")
}

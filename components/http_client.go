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
	"crypto/tls"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

// newHTTPClient 构造带请求超时、TLS 校验开关与代理的 HTTP 客户端。
// 不支持 SSE 流式响应；proxyUrl 仅做 URL 解析，scheme（http/https/socks5）
// 由 net/http 处理。proxyUrl 为空时沿用环境变量代理（ProxyFromEnvironment）。
func newHTTPClient(timeoutMs int, insecureSkipVerify bool, proxyURL string) (*http.Client, error) {
	// Timeout=0 表示无限等待，会把 httpCall 节点挂死；helper 层自行兜底，
	// 不依赖调用方 Init 的默认值
	if timeoutMs <= 0 {
		timeoutMs = defaultHTTPTimeoutMs
	}
	baseTransport, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		baseTransport = &http.Transport{} // 回退新建，避免 panic
	}
	transport := baseTransport.Clone()
	transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: insecureSkipVerify}
	if proxyURL != "" {
		u, err := url.Parse(proxyURL)
		if err != nil {
			return nil, fmt.Errorf("invalid proxyUrl %q: %w", proxyURL, err)
		}
		transport.Proxy = http.ProxyURL(u)
	}
	return &http.Client{Transport: transport, Timeout: time.Duration(timeoutMs) * time.Millisecond}, nil
}

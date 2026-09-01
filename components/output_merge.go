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
	"strings"

	"github.com/bytedance/sonic"
	"github.com/rulego/rulego/api/types"
	"github.com/tidwall/gjson"
)

// 非 object 输出整体写入 msg.Data 的保留 key 默认值。
const (
	DefaultReservedKey = "_reserved"
	HTTPReservedKey    = "_http"
	AIAgentReservedKey = "_ai"
)

// OutputMapping 单条字段映射:把节点输出里的某个值,写到流程上下文(msg.Data / metadata)。
//
// From:gjson 路径,如 "$.data.score" 或 "data.score"(两种写法 gjson 均原生支持)。
// To:目标位置——
//   - "score"        → 写 msg.Data 顶层 key(后续 switch 边表达式可 msg.score 读)
//   - "metadata.k"   → 写 msg.Metadata 的 k(metadata 只存 string,非字符串值会 JSON 序列化)
type OutputMapping struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// MergeAgentOutput 节点输出合并的统一模型（aiAgent / httpCall 共用，固定三条规则，无互斥分支）：
//  1. 完整输出始终写入 msg.Data[reservedKey]——能提取出 JSON 对象则存对象
//     （下游可 msg._ai.field / msg._http.field 读取），否则存原文字符串（审计原始记录永远在）
//  2. flatten 且输出为对象时：对象顶层字段并入 msg.Data 顶层（同名覆盖表单字段）
//  3. mappings 永远最后执行——设计者显式配置的改写优先级最高
//
// 路由裁决不经过本函数（aiAgent 按 AI_DECISION 标记路由，与 JSON 无关）。
// 原 msg.Data 的表单字段保留（除被规则 2/3 显式覆盖）。msg.DataType 最终置为 JSON。
func MergeAgentOutput(msg *types.RuleMsg, output []byte, mappings []OutputMapping, reservedKey string, flatten bool) error {
	if msg == nil {
		return nil
	}
	if reservedKey == "" {
		reservedKey = AIAgentReservedKey
	}

	// 读出原 msg.Data(表单字段)作为合并基座。解析失败(非 JSON)或为 "null" 则用空 map
	// （sonic.Unmarshal "null" 会置 nil，后续写入 panic，必须防护）。
	dataMap := map[string]interface{}{}
	if orig := msg.GetData(); len(orig) > 0 {
		_ = sonic.Unmarshal([]byte(orig), &dataMap)
		if dataMap == nil {
			dataMap = map[string]interface{}{}
		}
	}

	if len(output) > 0 {
		// 映射取值来源：能提取出 JSON 对象时用对象字节（剥掉了围栏/标记行/说明文字），
		// 否则用原始输出（gjson 对非 JSON 取不到值，映射自然全部跳过）。
		mappingSource := output
		if objBytes, ok := ExtractJSONObject(output); ok {
			var obj map[string]interface{}
			if err := sonic.Unmarshal(objBytes, &obj); err == nil && obj != nil {
				dataMap[reservedKey] = obj
				if flatten {
					for k, v := range obj {
						dataMap[k] = v
					}
				}
				mappingSource = objBytes
			} else {
				dataMap[reservedKey] = strings.TrimSpace(string(output))
			}
		} else {
			dataMap[reservedKey] = strings.TrimSpace(string(output))
		}

		for _, m := range mappings {
			if m.From == "" || m.To == "" {
				continue
			}
			r := gjson.GetBytes(mappingSource, normalizeGjsonPath(m.From))
			if !r.Exists() {
				continue
			}
			assignOutputValue(dataMap, msg, m.To, r.Value())
		}
	}

	b, err := sonic.Marshal(dataMap)
	if err != nil {
		return err
	}
	msg.SetData(string(b))
	msg.DataType = types.JSON
	return nil
}

// ExtractJSONObject 从原始输出中提取第一个 JSON 对象。
// 支持三种形态：整体即对象、markdown 围栏包裹（```json ... ```）、前后混有
// 说明文字/裁决标记行。实现上从首个 '{' 起做括号配对扫描（跳过字符串字面量），
// 截取后校验；首个不合法则继续尝试下一个 '{'。
// 提取不到返回 false（调用方整体按原文处理）。
func ExtractJSONObject(b []byte) ([]byte, bool) {
	s := strings.TrimSpace(string(b))
	if s == "" {
		return nil, false
	}
	start := strings.IndexByte(s, '{')
	for start >= 0 {
		if end, ok := matchJSONObject(s, start); ok {
			cand := s[start : end+1]
			var m map[string]interface{}
			if err := sonic.Unmarshal([]byte(cand), &m); err == nil {
				return []byte(cand), true
			}
		}
		next := strings.IndexByte(s[start+1:], '{')
		if next < 0 {
			break
		}
		start = start + 1 + next
	}
	return nil, false
}

// matchJSONObject 从 s[start]（必须是 '{'）起找配对的 '}'，
// 字符串字面量内的括号不计。返回结束下标。
func matchJSONObject(s string, start int) (int, bool) {
	depth := 0
	inStr := false
	esc := false
	for i := start; i < len(s); i++ {
		c := s[i]
		if inStr {
			if esc {
				esc = false
				continue
			}
			if c == '\\' {
				esc = true
				continue
			}
			if c == '"' {
				inStr = false
			}
			continue
		}
		switch c {
		case '"':
			inStr = true
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return i, true
			}
		}
	}
	return 0, false
}

// normalizeGjsonPath 去掉标准 JSONPath 的 $ 前缀:gjson 路径不以 $ 开头。
// 把 "$.a.b"→"a.b"、"$"→""(根)、"$a"→"a",使 from 既能写标准 JSONPath 也能写 gjson 原生路径。
func normalizeGjsonPath(path string) string {
	if path == "" {
		return ""
	}
	if strings.HasPrefix(path, "$.") {
		return path[2:]
	}
	if path == "$" {
		return ""
	}
	if strings.HasPrefix(path, "$") {
		return path[1:]
	}
	return path
}

// assignOutputValue 把值写到 to 指定位置:"metadata.xxx"→metadata;否则→dataMap 顶层 key。
func assignOutputValue(dataMap map[string]interface{}, msg *types.RuleMsg, to string, value interface{}) {
	if strings.HasPrefix(to, "metadata.") {
		key := strings.TrimPrefix(to, "metadata.")
		if msg.Metadata == nil {
			msg.Metadata = types.NewMetadata()
		}
		msg.Metadata.PutValue(key, toMetadataString(value))
		return
	}
	dataMap[to] = value
}

// toMetadataString 把任意值转成 metadata 可存的字符串:
// string/nil 原样;其余类型(number/bool/object/array)JSON 序列化。
func toMetadataString(value interface{}) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return v
	default:
		b, _ := sonic.Marshal(v)
		return string(b)
	}
}

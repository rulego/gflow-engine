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
	"testing"

	"github.com/rulego/rulego/api/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mustJSON 把 msg.Data 解析成 map,便于断言。
func mustJSON(t *testing.T, s string) map[string]interface{} {
	t.Helper()
	m := map[string]interface{}{}
	require.NoError(t, json.Unmarshal([]byte(s), &m))
	return m
}

// 迁移自旧 MergeOutputToMsg 的行为契约：映射写 metadata、from 取不到跳过、
// 空数组等非对象输出挂保留 key、空 msg.Data 安全。

func TestMergeAgentOutput_MappingToMetadata(t *testing.T) {
	msg := types.NewMsgWithJsonData(`{"days":3}`)
	output := []byte(`{"token":"abc"}`)
	err := MergeAgentOutput(&msg, output, []OutputMapping{{From: "$.token", To: "metadata.authToken"}}, "_http", false)
	require.NoError(t, err)
	assert.Equal(t, "abc", msg.GetMetadata().GetValue("authToken"))

	m := mustJSON(t, msg.GetData())
	assert.Equal(t, float64(3), m["days"]) // 表单保留
	_, hasToken := m["token"]
	assert.False(t, hasToken, "token mapped into metadata, not data top-level")
}

func TestMergeAgentOutput_MappingFromMissingSkipped(t *testing.T) {
	msg := types.NewMsgWithJsonData(`{"days":3}`)
	output := []byte(`{"a":1}`)
	err := MergeAgentOutput(&msg, output, []OutputMapping{
		{From: "$.notExist", To: "score"},
		{From: "$.a", To: "a2"},
	}, "_http", false)
	require.NoError(t, err)
	m := mustJSON(t, msg.GetData())
	assert.Equal(t, float64(1), m["a2"])
	_, hasScore := m["score"]
	assert.False(t, hasScore, "from missing must be skipped, no null pollution")
}

// JSON 数组响应（httpCall 常见）：非对象无法平铺，整体原文挂保留 key。
func TestMergeAgentOutput_JSONArrayOutput(t *testing.T) {
	msg := types.NewMsgWithJsonData(`{"days":3}`)
	output := []byte(`[1,2,3]`)
	err := MergeAgentOutput(&msg, output, nil, "_http", true)
	require.NoError(t, err)
	m := mustJSON(t, msg.GetData())
	assert.Equal(t, "[1,2,3]", m["_http"])
	assert.Equal(t, float64(3), m["days"])
}

// msg.Data 为空串：以空 map 为基座，不 panic。
func TestMergeAgentOutput_EmptyMsgData(t *testing.T) {
	msg := types.NewMsgWithJsonData(``)
	err := MergeAgentOutput(&msg, []byte(`{"score":85}`), nil, "_http", true)
	require.NoError(t, err)
	m := mustJSON(t, msg.GetData())
	assert.Equal(t, float64(85), m["score"])
}

// 规则 1：默认（不平铺、无映射）输出对象整体挂 _ai，表单字段保留。
func TestMergeAgentOutput_NamespaceOnly(t *testing.T) {
	msg := types.NewMsgWithJsonData(`{"contractName":"c1"}`)
	output := []byte(`{"approved":true,"reason":"ok"}`)
	err := MergeAgentOutput(&msg, output, nil, "_ai", false)
	require.NoError(t, err)
	m := mustJSON(t, msg.GetData())
	assert.Equal(t, "c1", m["contractName"])
	ai, ok := m["_ai"].(map[string]interface{})
	assert.True(t, ok, "_ai must hold the object")
	assert.Equal(t, true, ai["approved"])
	_, flattened := m["approved"]
	assert.False(t, flattened, "no flatten: approved must stay inside _ai")
}

// 规则 1+围栏+标记行：能从围栏/说明文字中提取 JSON 对象。
func TestMergeAgentOutput_FencedOutputWithMarker(t *testing.T) {
	msg := types.NewMsgWithJsonData(`{}`)
	output := []byte("```json\n{\"approved\":false,\"reason\":\"超限\"}\n```\nAI_DECISION: REJECT")
	err := MergeAgentOutput(&msg, output, []OutputMapping{{From: "reason", To: "aiReason"}}, "_ai", false)
	require.NoError(t, err)
	m := mustJSON(t, msg.GetData())
	ai, ok := m["_ai"].(map[string]interface{})
	assert.True(t, ok, "fenced JSON must be extracted into _ai")
	assert.Equal(t, false, ai["approved"])
	assert.Equal(t, "超限", m["aiReason"], "mapping must read from extracted object")
}

// 规则 2：flatten 平铺顶层字段，同名覆盖表单值。
func TestMergeAgentOutput_FlattenOverwritesForm(t *testing.T) {
	msg := types.NewMsgWithJsonData(`{"approved":true,"formOnly":"keep"}`)
	output := []byte(`{"approved":false}`)
	err := MergeAgentOutput(&msg, output, nil, "_ai", true)
	require.NoError(t, err)
	m := mustJSON(t, msg.GetData())
	assert.Equal(t, false, m["approved"], "flatten must overwrite same-name form field")
	assert.Equal(t, "keep", m["formOnly"])
	ai := m["_ai"].(map[string]interface{})
	assert.Equal(t, false, ai["approved"], "raw record always kept under _ai")
}

// 规则 3：mappings 在 flatten 之后执行（显式改写优先级最高，用于救回冲突字段）。
func TestMergeAgentOutput_MappingsAfterFlatten(t *testing.T) {
	msg := types.NewMsgWithJsonData(`{"approved":true}`)
	output := []byte(`{"approved":false}`)
	err := MergeAgentOutput(&msg, output, []OutputMapping{{From: "approved", To: "approved"}}, "_ai", true)
	require.NoError(t, err)
	m := mustJSON(t, msg.GetData())
	// flatten 先写 approved=false，映射 approved→approved 再写一次（来源同为对象，演示优先级）
	assert.Equal(t, false, m["approved"])
	// 改名映射在 flatten 之上叠加：两个字段并存
	msg2 := types.NewMsgWithJsonData(`{}`)
	err = MergeAgentOutput(&msg2, output, []OutputMapping{{From: "approved", To: "aiApproved"}}, "_ai", true)
	require.NoError(t, err)
	m2 := mustJSON(t, msg2.GetData())
	assert.Equal(t, false, m2["approved"], "flattened field present")
	assert.Equal(t, false, m2["aiApproved"], "renamed mapping present")
}

// 非 JSON 输出：原文存 _ai 字符串，映射自然跳过，不 panic。
func TestMergeAgentOutput_PlainText(t *testing.T) {
	msg := types.NewMsgWithJsonData(`{"formOnly":"keep"}`)
	output := []byte("这份合同看起来没什么问题。")
	err := MergeAgentOutput(&msg, output, []OutputMapping{{From: "approved", To: "aiApproved"}}, "_ai", true)
	require.NoError(t, err)
	m := mustJSON(t, msg.GetData())
	assert.Equal(t, "这份合同看起来没什么问题。", m["_ai"])
	_, ok := m["aiApproved"]
	assert.False(t, ok, "mapping must skip for non-JSON output")
	assert.Equal(t, "keep", m["formOnly"])
}

// msg.Data 为 "null" 防护：不 panic。
func TestMergeAgentOutput_NullMsgData(t *testing.T) {
	msg := types.NewMsgWithJsonData(`null`)
	err := MergeAgentOutput(&msg, []byte(`{"a":1}`), nil, "_ai", false)
	require.NoError(t, err)
	m := mustJSON(t, msg.GetData())
	ai := m["_ai"].(map[string]interface{})
	assert.Equal(t, float64(1), ai["a"])
}

// ExtractJSONObject：整体对象 / 围栏 / 混排 / 无对象。
func TestExtractJSONObject(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
		ok   bool
	}{
		{"plain object", `{"a":1}`, `{"a":1}`, true},
		{"fenced", "```json\n{\"a\":1}\n```", `{"a":1}`, true},
		{"prose then object", "结论如下：\n{\"a\":1}\n以上。", `{"a":1}`, true},
		{"object then marker", `{"a":1}` + "\nAI_DECISION: PASS", `{"a":1}`, true},
		{"nested braces in string", `{"a":"}{ text"}`, `{"a":"}{ text"}`, true},
		{"two objects takes first", `{"a":1} {"b":2}`, `{"a":1}`, true},
		{"no object", "plain text only", "", false},
		{"empty", "", "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := ExtractJSONObject([]byte(c.in))
			assert.Equal(t, c.ok, ok)
			if c.ok {
				assert.JSONEq(t, c.want, string(got))
			}
		})
	}
}

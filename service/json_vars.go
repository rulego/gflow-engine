// This file hosts the shared JSON-variables parsing helper used by instance /
// task services and (exported) by components nodes that read task variables.

package service

import (
	"encoding/json"
	"fmt"
)

// ParseVariablesJSON 把任务/实例的 variables JSON 字符串指针解析为 map。
// nil 指针或空串返回空 map（非 nil），解析失败返回错误。
// 空输入返回非 nil 空 map：多数调用方随后会向 map 合并键或整体下传。
func ParseVariablesJSON(p *string) (map[string]interface{}, error) {
	if p == nil || *p == "" {
		return map[string]interface{}{}, nil
	}
	var v map[string]interface{}
	if err := json.Unmarshal([]byte(*p), &v); err != nil {
		return nil, fmt.Errorf("failed to parse variables: %w", err)
	}
	return v, nil
}

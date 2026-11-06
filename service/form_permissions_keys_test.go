package service

// 权限表键归一测试：设计器权限表以字段 id 为键，提交变量以数据键为键，
// resolveNodeFormPermissions 出口须按 form schema 把两套键对齐。

import (
	"testing"

	"github.com/rulego/rulego/api/types"
	"github.com/stretchr/testify/require"
)

func formSchemaChain(form map[string]interface{}) *types.RuleChain {
	rc := &types.RuleChain{}
	rc.RuleChain.AdditionalInfo = map[string]interface{}{"form": form}
	return rc
}

// 字段树含顶层字段、卡片子字段、栅格列、页签、子表列：全部进 id→数据键映射。
func TestFormSchemaFieldKeyMap_ContainersFlattened(t *testing.T) {
	form := map[string]interface{}{
		"fields": []interface{}{
			map[string]interface{}{"id": "f_year", "field": "year"},
			map[string]interface{}{
				"id": "card_1", "type": "card",
				"fields": []interface{}{
					map[string]interface{}{"id": "f_port", "field": "port"},
				},
			},
			map[string]interface{}{
				"id": "grid_1", "type": "grid",
				"columns": []interface{}{
					map[string]interface{}{
						"fields": []interface{}{
							map[string]interface{}{"id": "f_remark", "field": "remark"},
						},
					},
				},
			},
			map[string]interface{}{
				"id": "tabs_1", "type": "tabs",
				"tabs": []interface{}{
					map[string]interface{}{
						"fields": []interface{}{
							map[string]interface{}{"id": "f_tab", "field": "tabField"},
						},
					},
				},
			},
			map[string]interface{}{
				"id": "sub_1", "field": "subTable", "type": "subform",
				"subFields": []interface{}{
					map[string]interface{}{"id": "f_col", "field": "colName"},
				},
			},
		},
	}
	m := formSchemaFieldKeyMap(formSchemaChain(form))
	require.Equal(t, map[string]string{
		"f_year": "year", "f_port": "port", "f_remark": "remark",
		"f_tab": "tabField", "f_col": "colName",
		"sub_1": "subTable", // 子表单本体也是数据字段，与设计器展平口径一致
	}, m)

	// 无 form / 空 schema：空映射，归一退化为原样透传
	require.Empty(t, formSchemaFieldKeyMap(&types.RuleChain{}))
	require.Empty(t, formSchemaFieldKeyMap(nil))
}

// id 条目换算为数据键并覆盖同名存量数据键条目（id 层为准）；未命中键原样保留。
func TestNormalizeFormPermissionKeys_IdLayerWins(t *testing.T) {
	out := normalizeFormPermissionKeys(map[string]interface{}{
		"f_year": "w", "year": "w", // id 条目与数据键条目并存
		"f_port": "r", "port": "w", // id 只读覆盖寄生数据键可写
		"legacy": "h", // 映射未命中：视为已是数据键，原样保留
	}, map[string]string{"f_year": "year", "f_port": "port"})
	require.Equal(t, map[string]interface{}{
		"year": "w", "port": "r", "legacy": "h",
	}, out)

	// 空 map / 空映射：原样返回（同一引用语义，内容相等即可）
	in := map[string]interface{}{"year": "w"}
	require.Equal(t, in, normalizeFormPermissionKeys(in, nil))
	require.Empty(t, normalizeFormPermissionKeys(map[string]interface{}{}, map[string]string{"f": "f"}))
}

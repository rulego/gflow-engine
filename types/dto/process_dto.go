package dto

import (
	"github.com/rulego/gflow-engine/types/enums"
)

type ProcessQueryRequest struct {
	PageRequest
	ProcessKey string `json:"processKey" form:"processKey"`
	Category   string `json:"category" form:"category"`
	//是否查询所有版本
	AllVersion  bool   `json:"allVersion" form:"allVersion"`
	ProcessType string `json:"processType" form:"processType"` //main sub
}

// CreateProcessDefinitionRequest 创建流程定义请求
// 用于创建新的流程定义
type CreateProcessDefinitionRequest struct {
	ProcessKey     string            `json:"processKey"`     // 流程键（业务唯一标识）
	Name           string            `json:"name"`           // 流程名称
	Category       *string           `json:"category"`       // 分类
	Description    *string           `json:"description"`    // 流程描述
	DefinitionJSON string            `json:"definitionJson"` // 流程定义DSL
	ProcessType    string            `json:"processType"`    //main sub
	Icon           string            `json:"icon"`           //流程图标
	Ext            map[string]string `json:"ext"`            // 结构化扩展字段
	Duplicate      bool              `json:"duplicate"`      //是否允许重复部署（false: 如果processKey已存在则返回错误，true: 创建新版本）
}

// ProcessDefinitionStatusRequest 流程定义状态更新请求
// 用于更新流程定义状态（激活、挂起、停用等）
type ProcessDefinitionStatusRequest struct {
	ProcessID string              `json:"processId"` // 流程定义ID
	Status    enums.ProcessStatus `json:"status"`    // 目标状态
	Reason    string              `json:"reason"`    // 状态变更原因
}

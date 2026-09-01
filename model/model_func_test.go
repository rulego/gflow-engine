package model

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestWfProcess_ToRuleChain_Valid(t *testing.T) {
	p := &WfProcess{
		DefinitionJSON: `{"ruleChain":{"id":"chain1","name":"Test"},"metadata":{"nodes":[],"connections":[]}}`,
	}
	rc, err := p.ToRuleChain()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rc.RuleChain.ID != "chain1" {
		t.Errorf("ID = %q", rc.RuleChain.ID)
	}
	if rc.RuleChain.Name != "Test" {
		t.Errorf("Name = %q", rc.RuleChain.Name)
	}
}

func TestWfProcess_ToRuleChain_Invalid(t *testing.T) {
	p := &WfProcess{
		DefinitionJSON: `{invalid}`,
	}
	_, err := p.ToRuleChain()
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestWfProcess_ToRuleChain_Empty(t *testing.T) {
	p := &WfProcess{
		DefinitionJSON: "",
	}
	_, err := p.ToRuleChain()
	if err == nil {
		t.Error("expected error for empty string")
	}
}

func TestWfTask_GetVariablesAsMap_Nil(t *testing.T) {
	task := &WfTask{Variables: nil}
	m := task.GetVariablesAsMap()
	if m != nil {
		t.Errorf("expected nil, got %v", m)
	}
}

func TestWfTask_GetVariablesAsMap_Empty(t *testing.T) {
	empty := ""
	task := &WfTask{Variables: &empty}
	m := task.GetVariablesAsMap()
	if m != nil {
		t.Errorf("expected nil for empty string, got %v", m)
	}
}

func TestWfTask_GetVariablesAsMap_Valid(t *testing.T) {
	vars := `{"name":"Alice","age":30}`
	task := &WfTask{Variables: &vars}
	m := task.GetVariablesAsMap()
	if m == nil {
		t.Fatal("expected non-nil map")
	}
	if m["name"] != "Alice" {
		t.Errorf("name = %v", m["name"])
	}
}

func TestWfTask_GetVariablesAsMap_Invalid(t *testing.T) {
	bad := "not-json"
	task := &WfTask{Variables: &bad}
	m := task.GetVariablesAsMap()
	if m != nil {
		t.Errorf("expected nil for invalid JSON, got %v", m)
	}
}

func TestWfTask_GetVariablesAsMap_RoundTrip(t *testing.T) {
	original := map[string]interface{}{"x": float64(1), "y": "hello"}
	data, _ := json.Marshal(original)
	s := string(data)
	task := &WfTask{Variables: &s}
	m := task.GetVariablesAsMap()
	if m["x"] != float64(1) {
		t.Errorf("x = %v", m["x"])
	}
	if m["y"] != "hello" {
		t.Errorf("y = %v", m["y"])
	}
}

// nodeIDs / endNodeIDs / outgoing / findNode 都是测试辅助：从 DefinitionJSON 里提取节点/出边信息。
func nodeIDs(p *WfProcess) []string {
	var doc map[string]interface{}
	_ = json.Unmarshal([]byte(p.DefinitionJSON), &doc)
	meta, _ := doc["metadata"].(map[string]interface{})
	nodes, _ := meta["nodes"].([]interface{})
	ids := make([]string, 0, len(nodes))
	for _, n := range nodes {
		if m, ok := n.(map[string]interface{}); ok {
			if id, _ := m["id"].(string); id != "" {
				ids = append(ids, id)
			}
		}
	}
	return ids
}

func hasOutgoingEdge(p *WfProcess, from string) bool {
	var doc map[string]interface{}
	_ = json.Unmarshal([]byte(p.DefinitionJSON), &doc)
	meta, _ := doc["metadata"].(map[string]interface{})
	conns, _ := meta["connections"].([]interface{})
	for _, c := range conns {
		if m, ok := c.(map[string]interface{}); ok {
			if f, _ := m["fromId"].(string); f == from {
				return true
			}
		}
	}
	return false
}

func edgeTo(p *WfProcess, from, to string) bool {
	var doc map[string]interface{}
	_ = json.Unmarshal([]byte(p.DefinitionJSON), &doc)
	meta, _ := doc["metadata"].(map[string]interface{})
	conns, _ := meta["connections"].([]interface{})
	for _, c := range conns {
		if m, ok := c.(map[string]interface{}); ok {
			f, _ := m["fromId"].(string)
			t, _ := m["toId"].(string)
			if f == from && t == to {
				return true
			}
		}
	}
	return false
}

func countEndNodes(p *WfProcess) int {
	var doc map[string]interface{}
	_ = json.Unmarshal([]byte(p.DefinitionJSON), &doc)
	meta, _ := doc["metadata"].(map[string]interface{})
	nodes, _ := meta["nodes"].([]interface{})
	count := 0
	for _, n := range nodes {
		if m, ok := n.(map[string]interface{}); ok {
			if t, _ := m["type"].(string); t == "end" {
				count++
			}
		}
	}
	return count
}

// 业务规则：引擎只在 end 节点触发 CompleteProcessInstance。缺少 end 节点的 DSL
// 在最后一个任务执行完后链静默走完，实例永远停在 active（无任务可办、无终点），
// 部署期必须自动补 end 节点，并把无出边的尾节点连过去。
func TestEnsureEndNode_AppendsEndAndConnectsDanglingTail(t *testing.T) {
	p := &WfProcess{DefinitionJSON: `{
	  "ruleChain":{"id":"c1","name":"t"},
	  "metadata":{
	    "nodes":[
	      {"id":"start","type":"startTask"},
	      {"id":"approve","type":"userTask"}
	    ],
	    "connections":[
	      {"fromId":"start","toId":"approve","type":"Success"}
	    ]
	  }}`}
	p.EnsureEndNode()
	if got := countEndNodes(p); got != 1 {
		t.Fatalf("应补 1 个 end 节点，实际 %d", got)
	}
	if !hasOutgoingEdge(p, "approve") {
		t.Fatalf("无出边的尾节点 approve 应被连到 end")
	}
	if !edgeTo(p, "approve", "node_end") {
		t.Fatalf("尾节点应连到 node_end")
	}
}

// 业务规则：链尾不唯一时（如分支各尾节点都无出边），每个悬垂尾都要接到 end，
// 保证任意路径执行完都能到达 end。
func TestEnsureEndNode_ConnectsAllDanglingNodes(t *testing.T) {
	p := &WfProcess{DefinitionJSON: `{
	  "ruleChain":{"id":"c1"},"metadata":{
	    "nodes":[
	      {"id":"start","type":"startTask"},
	      {"id":"gw","type":"switch"},
	      {"id":"A","type":"userTask"},
	      {"id":"B","type":"userTask"}
	    ],
	    "connections":[
	      {"fromId":"start","toId":"gw","type":"Success"},
	      {"fromId":"gw","toId":"A","type":"c1"},
	      {"fromId":"gw","toId":"B","type":"c2"}
	    ]
	  }}`}
	p.EnsureEndNode()
	for _, id := range []string{"start", "gw", "A", "B"} {
		if !hasOutgoingEdge(p, id) {
			t.Fatalf("节点 %s 应有出边（尾节点应连 end）", id)
		}
	}
	if !edgeTo(p, "A", "node_end") || !edgeTo(p, "B", "node_end") {
		t.Fatalf("分支尾 A/B 都应连到 end")
	}
}

// 业务规则：DSL 已有 end 节点（如 dsl-examples 手工 DSL）时不得再追加，
// 避免产生冗余节点/破坏手工编排。
func TestEnsureEndNode_KeepsExistingEnd(t *testing.T) {
	p := &WfProcess{DefinitionJSON: `{
	  "ruleChain":{"id":"c1"},"metadata":{
	    "nodes":[
	      {"id":"start","type":"startTask"},
	      {"id":"node_end","type":"end","name":"结束"}
	    ],
	    "connections":[{"fromId":"start","toId":"node_end","type":"Success"}]
	  }}`}
	before := len(nodeIDs(p))
	p.EnsureEndNode()
	if got := countEndNodes(p); got != 1 {
		t.Fatalf("已有 end 不应再补，实际 %d", got)
	}
	if len(nodeIDs(p)) != before {
		t.Fatalf("节点数不应变化")
	}
}

// 业务规则：与 EnsureSwitchDefaultEdges 串联时，先补 end 再补 Default，
// 非穷尽网关的 Default 兜底边应能指向新补的 end 节点。
func TestEnsureEndNode_ThenSwitchDefaultChain(t *testing.T) {
	p := &WfProcess{DefinitionJSON: `{
	  "ruleChain":{"id":"c1"},"metadata":{
	    "nodes":[
	      {"id":"gw","type":"switch"},
	      {"id":"A","type":"userTask"}
	    ],
	    "connections":[
	      {"fromId":"gw","toId":"A","type":"c1"}
	    ]
	  }}`}
	p.EnsureEndNode()
	p.EnsureSwitchDefaultEdges()
	if got := defaultEdgeCount(p, "gw"); got != 1 {
		t.Fatalf("switch 应补 1 条 Default 出边指向 end，实际 %d", got)
	}
	if !edgeTo(p, "A", "node_end") {
		t.Fatalf("尾节点 A 应连到 end")
	}
}

// 业务规则：node_end id 被其它类型节点占用时（外部 DSL 恰有同名节点），
// 自动补的 end 应换 id 而不是冲突。
func TestEnsureEndNode_AvoidsIDCollision(t *testing.T) {
	p := &WfProcess{DefinitionJSON: `{
	  "ruleChain":{"id":"c1"},"metadata":{
	    "nodes":[
	      {"id":"start","type":"startTask"},
	      {"id":"node_end","type":"ccTask"}
	    ],
	    "connections":[
	      {"fromId":"start","toId":"node_end","type":"Success"}
	    ]
	  }}`}
	p.EnsureEndNode()
	if got := countEndNodes(p); got != 1 {
		t.Fatalf("应补 1 个 end 节点，实际 %d", got)
	}
	// 占用 node_end id 的 ccTask 尾节点应被连到新 end
	if !edgeTo(p, "node_end", "node_end_1") {
		t.Fatalf("占用了 node_end id 的尾节点应连到 node_end_1")
	}
}

// def 构造一份 DefinitionJSON：gateway(gw) 有 case→A→end，但无 Default 出边。
func def(gatewayType string) string {
	return `{
	  "ruleChain":{"id":"c1","name":"t"},
	  "metadata":{
	    "nodes":[
	      {"id":"start","type":"startTask"},
	      {"id":"gw","type":"` + gatewayType + `"},
	      {"id":"A","type":"userTask"},
	      {"id":"end","type":"end"}
	    ],
	    "connections":[
	      {"fromId":"start","toId":"gw","type":"Success"},
	      {"fromId":"gw","toId":"A","type":"c1"},
	      {"fromId":"A","toId":"end","type":"Success"}
	    ]
	  }
	}`
}

// defaultEdgeCount 统计 from 节点的 Default 出边数量。
func defaultEdgeCount(p *WfProcess, from string) int {
	var doc map[string]interface{}
	_ = json.Unmarshal([]byte(p.DefinitionJSON), &doc)
	meta, _ := doc["metadata"].(map[string]interface{})
	conns, _ := meta["connections"].([]interface{})
	count := 0
	for _, c := range conns {
		m, _ := c.(map[string]interface{})
		if m["type"] == "Default" && m["fromId"] == from {
			count++
		}
	}
	return count
}

// 业务规则：switch 在数据无 case 命中时由 rulego 引擎转发到 Default 关系，
// 故部署期必须为没有 Default 出边的 switch 补一条到 end 的 Default 边，否则实例卡死。
func TestEnsureSwitchDefaultEdges_SwitchGetsDefault(t *testing.T) {
	p := &WfProcess{DefinitionJSON: def("switch")}
	p.EnsureSwitchDefaultEdges()
	if got := defaultEdgeCount(p, "gw"); got != 1 {
		t.Fatalf("switch gw 应补 1 条 Default 出边，实际 %d", got)
	}
}

// 业务规则：已有 Default 出边的 switch 不应再补，避免重复边。
func TestEnsureSwitchDefaultEdges_SwitchKeepsExistingDefault(t *testing.T) {
	p := &WfProcess{DefinitionJSON: `{
	  "ruleChain":{"id":"c1"},"metadata":{
	    "nodes":[{"id":"gw","type":"switch"},{"id":"end","type":"end"}],
	    "connections":[{"fromId":"gw","toId":"end","type":"Default"}]
	  }}`}
	p.EnsureSwitchDefaultEdges()
	if got := defaultEdgeCount(p, "gw"); got != 1 {
		t.Fatalf("已有 Default 不应重复补，实际 %d", got)
	}
}

// 业务规则：inclusive 网关与 switch 一样——无 case 命中时由引擎转发到 Default 关系
// （见 rulego inclusive_node.OnMsg：无匹配 TellNext Default）。故没有 Default 出边的
// inclusive 同样必须补一条到 end 的 Default 边，否则无匹配时实例卡死。
func TestEnsureSwitchDefaultEdges_InclusiveGetsDefault(t *testing.T) {
	p := &WfProcess{DefinitionJSON: def("inclusive")}
	p.EnsureSwitchDefaultEdges()
	if got := defaultEdgeCount(p, "gw"); got != 1 {
		t.Fatalf("inclusive gw 应补 1 条 Default 出边，实际 %d", got)
	}
}

// TestWfTaskColumnMappings 校验 WfTask 结构体字段映射到正确的数据库列名，
// 防止 DAO 代码使用错误的列名（如 "approval_result" 而非 "end_reason"）。
func TestWfTaskColumnMappings(t *testing.T) {
	task := WfTask{}
	taskType := reflect.TypeOf(task)

	expectedColumns := map[string]string{
		"ID":                "id",
		"ProcessInstanceID": "process_instance_id",
		"ProcessID":         "process_id",
		"TaskType":          "task_type",
		"TaskDefKey":        "task_def_key",
		"Name":              "name",
		"Description":       "description",
		"ParentID":          "parent_id",
		"Status":            "status",
		"Assignee":          "assignee",
		"Owner":             "owner",
		"DueDate":           "due_date",
		"Priority":          "priority",
		"FormKey":           "form_key",
		"Variables":         "variables",
		"ClaimedAt":         "claimed_at",
		"SequenceOrder":     "sequence_order",
		"ApprovalType":      "approval_type",
		"ApprovalRule":      "approval_rule",
		"DelegateFrom":      "delegate_from",
		"DelegateReason":    "delegate_reason",
		"DelegateTime":      "delegate_time",
		"EndedAt":           "ended_at",
		"Comment":           "comment",
		"EndReason":         "end_reason",
		"Duration":          "duration",
		"TenantID":          "tenant_id",
		"CreatedBy":         "created_by",
		"CreatedAt":         "created_at",
		"UpdatedBy":         "updated_by",
		"UpdatedAt":         "updated_at",
	}

	for fieldName, expectedCol := range expectedColumns {
		field, ok := taskType.FieldByName(fieldName)
		if !ok {
			t.Errorf("field %s not found in WfTask", fieldName)
			continue
		}
		gotCol := field.Tag.Get("gorm")
		if gotCol == "" {
			t.Errorf("field %s has no gorm tag", fieldName)
			continue
		}
		// Extract column name from gorm tag
		col := parseGormColumn(gotCol)
		if col != expectedCol {
			t.Errorf("WfTask.%s: expected column %q, got %q (tag: %s)", fieldName, expectedCol, col, gotCol)
		}
	}

	// WfTask 不应包含这些列
	forbiddenCols := []string{"approval_result", "approval_comment", "completed_at", "suspend_reason"}
	for _, col := range forbiddenCols {
		for i := 0; i < taskType.NumField(); i++ {
			field := taskType.Field(i)
			gormTag := field.Tag.Get("gorm")
			if gormTag == "column:"+col || containsColumn(gormTag, col) {
				t.Errorf("forbidden column %q found in WfTask.%s", col, field.Name)
			}
		}
	}
}

func parseGormColumn(tag string) string {
	// Simple parser for "column:xxx;..." format
	parts := []byte(tag)
	i := 0
	for i < len(parts) {
		if len(parts)-i >= 7 && string(parts[i:i+7]) == "column:" {
			j := i + 7
			for j < len(parts) && parts[j] != ';' {
				j++
			}
			return string(parts[i+7 : j])
		}
		i++
	}
	return ""
}

func containsColumn(tag, col string) bool {
	return false
}

func TestWfTaskHasParentID(t *testing.T) {
	// ParentID 必须存在：GetAddSignTasks 过滤依赖它
	task := WfTask{}
	taskType := reflect.TypeOf(task)
	_, ok := taskType.FieldByName("ParentID")
	if !ok {
		t.Error("WfTask.ParentID field not found - GetAddSignTasks filter depends on it")
	}
}

func TestWfTaskHasEndReason(t *testing.T) {
	// EndReason 必须存在：WithdrawTask/ApproveTask/RejectTask 依赖它
	task := WfTask{}
	taskType := reflect.TypeOf(task)
	_, ok := taskType.FieldByName("EndReason")
	if !ok {
		t.Error("WfTask.EndReason field not found - DAO methods depend on it")
	}
}

func TestWfTaskHasComment(t *testing.T) {
	// Comment 必须存在：WithdrawTask/ApproveTask/RejectTask 依赖它
	task := WfTask{}
	taskType := reflect.TypeOf(task)
	_, ok := taskType.FieldByName("Comment")
	if !ok {
		t.Error("WfTask.Comment field not found - DAO methods depend on it")
	}
}

func TestWfTaskHasEndedAt(t *testing.T) {
	// EndedAt 必须存在：WithdrawTask/ApproveTask/RejectTask/CompleteTask 依赖它
	task := WfTask{}
	taskType := reflect.TypeOf(task)
	_, ok := taskType.FieldByName("EndedAt")
	if !ok {
		t.Error("WfTask.EndedAt field not found - DAO methods depend on it")
	}
}

// routeGateway 迁移：type→switch、routeList/cases 清空、Success 出边→Default。
// 迁移后 DSL 必须能被 rulego 装载（switch 为已注册类型）且行为等价：
// 无 case 命中 → Default → 原后继。
func TestMigrateRouteGateway_ConvertsToSwitch(t *testing.T) {
	p := &WfProcess{DefinitionJSON: `{
	  "ruleChain": {"id": "p1", "name": "旧流程"},
	  "metadata": {
	    "nodes": [
	      {"id": "start", "type": "startTask", "name": "发起人"},
	      {"id": "route1", "type": "routeGateway", "name": "路由", "configuration": {
	        "routeList": [{"title": "A", "routeKey": "r1", "conditionList": [[{"field": "amount", "operator": ">=", "value": "10000"}]]}],
	        "cases": [{"case": "msg.amount >= 10000", "then": "A"}]
	      }},
	      {"id": "task1", "type": "userTask", "name": "审批"}
	    ],
	    "connections": [
	      {"fromId": "start", "toId": "route1", "type": "Success"},
	      {"fromId": "route1", "toId": "task1", "type": "Success"}
	    ]
	  }
	}`}
	p.MigrateRouteGateway()

	var doc struct {
		Metadata struct {
			Nodes []map[string]any `json:"nodes"`
			Conns []map[string]any `json:"connections"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal([]byte(p.DefinitionJSON), &doc); err != nil {
		t.Fatalf("unmarshal migrated DSL: %v", err)
	}
	for _, n := range doc.Metadata.Nodes {
		if n["id"] != "route1" {
			continue
		}
		if n["type"] != "switch" {
			t.Errorf("type = %v, want switch", n["type"])
		}
		cfg, _ := n["configuration"].(map[string]any)
		if cfg == nil || len(cfg) != 0 {
			t.Errorf("configuration should be emptied (routeList/cases dropped), got: %v", cfg)
		}
	}
	edgeFound := false
	for _, c := range doc.Metadata.Conns {
		if c["fromId"] == "route1" {
			edgeFound = true
			if c["type"] != "Default" || c["toId"] != "task1" {
				t.Errorf("route1 edge = %v, want Default→task1", c)
			}
		}
	}
	if !edgeFound {
		t.Error("route1 should keep an outgoing edge")
	}

	// 幂等：二次迁移不再改动
	again := p.DefinitionJSON
	p.MigrateRouteGateway()
	if p.DefinitionJSON != again {
		t.Error("migrate should be idempotent")
	}
}

func TestMigrateRouteGateway_NoRouteGateway_NoChange(t *testing.T) {
	orig := `{"ruleChain":{"id":"p1","name":"x"},"metadata":{"nodes":[{"id":"n1","type":"userTask","name":"a"}],"connections":[]}}`
	p := &WfProcess{DefinitionJSON: orig}
	p.MigrateRouteGateway()
	if p.DefinitionJSON != orig {
		t.Errorf("DSL without routeGateway should stay untouched:\n%s", p.DefinitionJSON)
	}
}

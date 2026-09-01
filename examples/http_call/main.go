// Package main 演示 httpCall 节点与 switch 条件分支的组合使用：
// 查询外部 HTTP 接口 → 响应字段映射进流程变量 → switch 按查询结果路由到不同分支。
//
// 流程（见 dsl.json）：
//
//	startTask → httpCall(查询风险评估接口) → switch(msg.riskLevel)
//	  ├─ case "high" → node_manual_review（serviceTask：高风险转人工）
//	  └─ Default     → node_auto_pass（serviceTask：低风险自动通过）→ end
//
// 运行前准备：
//  1. 默认使用内存 SQLite，零依赖直接 go run 即可（进程退出数据即清空）
//  2. 连接外部数据库：设置 GFLOW_DSN（driver 用 GFLOW_DRIVER 指定，默认 postgres），
//     并先创建 gflow 库、执行 scripts/00.init_bpm_pg.sql 建表（与 leave_approval 示例一致）。
//
// 运行：go run .
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/rulego/rulego/api/types"

	"github.com/rulego/gflow-engine/components"
	"github.com/rulego/gflow-engine/config"
	"github.com/rulego/gflow-engine/examples/internal/demo"
	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/service"
)

func main() {
	ctx := context.Background()

	// 模拟的外部风险评估系统：按 days 返回 riskLevel/score
	const addr = "127.0.0.1:18080"
	mux := http.NewServeMux()
	mux.HandleFunc("/api/risk", func(w http.ResponseWriter, r *http.Request) {
		days := 0
		fmt.Sscan(r.URL.Query().Get("days"), &days)
		resp := map[string]interface{}{"riskLevel": "low", "score": 12}
		if days > 3 {
			resp = map[string]interface{}{"riskLevel": "high", "score": 88}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	})
	go func() {
		if err := http.ListenAndServe(addr, mux); err != nil {
			log.Fatalf("启动模拟 HTTP 服务失败: %v", err)
		}
	}()

	// switch 两个分支的目标节点都是 serviceTask，打印各自拿到的流程变量。
	// 函数经 WithServiceFuncs 随组件引导注册：实现进运行时注册表，元数据进设计器目录。
	serviceFuncs := []components.ServiceFunc{
		{
			Def: components.ServiceFuncDef{
				Name:  "RiskService.autoPass",
				Label: "低风险自动通过",
				Desc:  "打印低风险分支拿到的流程变量",
			},
			Fn: func(ctx types.RuleContext, msg types.RuleMsg) {
				fmt.Printf("  [低风险] 自动通过：riskLevel=%v riskScore=%v\n", varOf(msg, "riskLevel"), varOf(msg, "riskScore"))
				ctx.TellSuccess(msg)
			},
		},
		{
			Def: components.ServiceFuncDef{
				Name:  "RiskService.manualReview",
				Label: "高风险转人工",
				Desc:  "打印高风险分支拿到的流程变量",
			},
			Fn: func(ctx types.RuleContext, msg types.RuleMsg) {
				fmt.Printf("  [高风险] 转人工审批：riskLevel=%v riskScore=%v\n", varOf(msg, "riskLevel"), varOf(msg, "riskScore"))
				ctx.TellSuccess(msg)
			},
		},
	}

	cfg := &config.Config{Database: databaseConfig()}
	builder := service.NewWorkflowEngineBuilder().
		SetName("HttpCallDemo").
		SetConfig(cfg).
		SetIDGenerator(service.NewIDGenerator())
	if cfg.Database.Driver == demo.Driver {
		// 引擎默认不带 SQLite 方言，演示模式注入 examples/internal/demo 的 provider
		builder = builder.SetDialectProvider(demo.DialectProvider{})
	}
	engine, err := builder.Build()
	if err != nil {
		log.Fatalf("构建工作流引擎失败: %v", err)
	}
	if err := engine.Start(ctx); err != nil {
		log.Fatalf("启动工作流引擎失败: %v", err)
	}
	defer engine.Stop(ctx)
	if cfg.Database.Driver == demo.Driver {
		// SQLite 演示模式自建全部工作流表；PG/MySQL 用 scripts/ 初始化脚本建表
		if err := demo.CreateTables(engine.GetDB()); err != nil {
			log.Fatalf("SQLite 建表失败: %v", err)
		}
	}
	// 注册全部 BPM 节点：依赖由引擎自取，宿主服务函数随引导一并注册
	if err := components.RegisterFromEngine(engine, components.WithServiceFuncs(serviceFuncs)); err != nil {
		log.Fatalf("注册工作流组件失败: %v", err)
	}

	if err := deploy(ctx, engine, addr); err != nil {
		log.Fatalf("部署流程失败: %v", err)
	}
	fmt.Println("风险评估流程部署成功（processKey=risk_check）")

	// days=2 → 低风险走 Default 分支；days=5 → 高风险命中 case 分支
	for _, days := range []int{2, 5} {
		fmt.Printf("=== 发起风险评估：days=%d ===\n", days)
		id, err := engine.GetRuntimeService().StartProcessInstanceByKey(ctx,
			service.Actor{UserID: "emp001", UserName: "张三", TenantID: "default"},
			"risk_check", fmt.Sprintf("risk_%d_%d", days, time.Now().UnixNano()),
			map[string]interface{}{"employeeId": "emp001", "days": days})
		if err != nil {
			log.Fatalf("发起流程失败: %v", err)
		}
		waitCompleted(ctx, engine, id)
	}
}

// deploy 加载 dsl.json 并注入模拟服务地址后部署。
// httpCall 节点的 url 写死为本机地址（静态主机，无 SSRF 拦截）；
// 若目标服务地址运行时才确定，可改用 ${metadata.xxx} 变量并配置 allowedHosts。
func deploy(ctx context.Context, engine service.WorkflowEngine, addr string) error {
	dsl, err := os.ReadFile(dslPath())
	if err != nil {
		return err
	}
	var def map[string]interface{}
	if err := json.Unmarshal(dsl, &def); err != nil {
		return fmt.Errorf("解析 dsl.json 失败: %w", err)
	}
	meta := def["metadata"].(map[string]interface{})
	for _, n := range meta["nodes"].([]interface{}) {
		node := n.(map[string]interface{})
		if node["type"] == "httpCall" {
			cfg := node["configuration"].(map[string]interface{})
			cfg["url"] = fmt.Sprintf("http://%s/api/risk?employeeId=${msg.employeeId}&days=${msg.days}", addr)
		}
	}
	b, _ := json.Marshal(def)

	_, err = engine.GetProcessService().Deploy(ctx, service.Actor{UserID: "admin", UserName: "admin", TenantID: "default"}, &model.WfProcess{
		ProcessKey:     "risk_check",
		Name:           "风险评估流程",
		DefinitionJSON: string(b),
		CreatedBy:      "admin",
		TenantID:       "default",
	}, true)
	return err
}

// waitCompleted 轮询等待实例到终态（节点执行与实例完结都是异步的）。
func waitCompleted(ctx context.Context, engine service.WorkflowEngine, instanceID string) {
	deadline := time.Now().Add(10 * time.Second)
	for {
		inst, err := engine.GetRuntimeService().GetProcessInstance(ctx, service.ActorFromCtx(ctx), instanceID)
		if err != nil {
			log.Fatalf("查询流程状态失败: %v", err)
		}
		switch inst.Status {
		case "completed", "terminated", "failed":
			fmt.Printf("流程实例 %s 最终状态: %s\n\n", instanceID[:8], inst.Status)
			return
		}
		if time.Now().After(deadline) {
			fmt.Printf("流程实例 %s 当前状态: %s（等待终态超时）\n\n", instanceID[:8], inst.Status)
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// varOf 从 msg.Data 取顶层字段（httpCall 按 outputMappings 合并进来的值）。
func varOf(msg types.RuleMsg, key string) interface{} {
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(msg.GetData()), &m); err != nil {
		return nil
	}
	return m[key]
}

func dslPath() string {
	for _, p := range []string{"dsl.json", "examples/http_call/dsl.json"} {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	log.Fatal("找不到 dsl.json，请在 examples/http_call 目录或仓库根目录运行")
	return ""
}

// databaseConfig 默认使用内存 SQLite（零依赖开箱即跑，进程退出数据即清空）；
// 设置 GFLOW_DSN 时切换到外部数据库（driver 用 GFLOW_DRIVER 指定，默认 postgres），
// 此时需先按 scripts/00.init_bpm_pg.sql / 00.init_bpm_mysql.sql 初始化 gflow 库。
func databaseConfig() *config.DatabaseConfig {
	if dsn := os.Getenv("GFLOW_DSN"); dsn != "" {
		driver := os.Getenv("GFLOW_DRIVER")
		if driver == "" {
			driver = "postgres"
		}
		return &config.DatabaseConfig{Driver: driver, Dsn: dsn}
	}
	return demo.NewDatabaseConfig()
}

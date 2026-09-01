// Package main 演示用 GFlow Engine 实现完整的请假审批流程：
// 部署流程定义（DSL 从 dsl.json 加载）→ 启动实例 → 查询待办 → 审批 → 查询实例状态。
//
// 三条审批路径由 switch 条件分支节点按请假天数路由（与 GFlow 设计器的条件分支同款 DSL）：
//   - ≤3 天：经理单签（approvalType=single，cases 命中）
//   - 3~7 天：经理 + HR 并行会签（countersign，all，cases 命中）
//   - >7 天：三人顺序会签（countersign，isSequential + majority，走 Default 默认分支）
//
// 运行前准备：
//  1. 默认使用内存 SQLite，零依赖直接 go run 即可（进程退出数据即清空）
//  2. 连接外部数据库：设置 GFLOW_DSN（driver 用 GFLOW_DRIVER 指定，默认 postgres），
//     并先创建 gflow 库、执行 scripts/00.init_bpm_pg.sql（或 00.init_bpm_mysql.sql）建表
//     （若已用 gflow 平台的 00.init_pg.sql 初始化过则已包含）
//
// 运行：
//
//	go run .           # 依次演示三条路径
//	go run . short     # 只演示 ≤3 天：经理单签
//	go run . long      # 只演示 3~7 天：并行会签
//	go run . sequential # 只演示 >7 天：顺序会签
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/rulego/gflow-engine/components"
	"github.com/rulego/gflow-engine/config"
	"github.com/rulego/gflow-engine/examples/internal/demo"
	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/service"
	"github.com/rulego/gflow-engine/types/constants"
	"github.com/rulego/gflow-engine/types/dto"
	"github.com/rulego/gflow-engine/types/enums"
	"github.com/rulego/rulego/api/types"
)

// LeaveRequest 请假申请结构
type LeaveRequest struct {
	EmployeeID   string    `json:"employeeId"`   // 员工ID
	EmployeeName string    `json:"employeeName"` // 员工姓名
	LeaveType    string    `json:"leaveType"`    // 请假类型：annual(年假), sick(病假), personal(事假)
	StartDate    time.Time `json:"startDate"`    // 开始日期
	EndDate      time.Time `json:"endDate"`      // 结束日期
	Days         int       `json:"days"`         // 请假天数
	Reason       string    `json:"reason"`       // 请假原因
	ManagerID    string    `json:"managerId"`    // 直属经理ID
	HrID         string    `json:"hrId"`         // HR ID
}

// LeaveApprovalWorkflow 请假审批工作流，持有引擎的三个核心服务。
type LeaveApprovalWorkflow struct {
	processService service.ProcessService
	runtimeService service.RuntimeService
	taskService    service.TaskService
}

// NewLeaveApprovalWorkflow 从引擎实例创建工作流封装。
func NewLeaveApprovalWorkflow(workflowEngine service.WorkflowEngine) *LeaveApprovalWorkflow {
	return &LeaveApprovalWorkflow{
		processService: workflowEngine.GetProcessService(),
		runtimeService: workflowEngine.GetRuntimeService(),
		taskService:    workflowEngine.GetTaskService(),
	}
}

// DeployLeaveApprovalProcess 从 dsl.json 加载流程 DSL 并部署为新版本。
func (w *LeaveApprovalWorkflow) DeployLeaveApprovalProcess(ctx context.Context) error {
	dsl, err := loadDSL()
	if err != nil {
		return err
	}

	// 部署操作需要操作人身份（审计字段）：显式 actor 传参
	admin := service.Actor{
		UserID:   "admin",
		UserName: "系统管理员",
		TenantID: "default",
	}

	category := "HR"
	description := "员工请假审批工作流程"
	_, err = w.processService.Deploy(ctx, admin, &model.WfProcess{
		ProcessKey:     "leave_approval",
		Name:           "请假审批流程",
		Category:       &category,
		Description:    &description,
		DefinitionJSON: dsl,
		CreatedBy:      "admin",
		TenantID:       "default",
	}, true) // duplicate=true：已存在时创建新版本
	return err
}

// loadDSL 读取 dsl.json，兼容从示例目录或仓库根目录运行。
func loadDSL() (string, error) {
	for _, p := range []string{"dsl.json", "examples/leave_approval/dsl.json"} {
		if b, err := os.ReadFile(p); err == nil {
			return string(b), nil
		}
	}
	return "", fmt.Errorf("找不到 dsl.json，请在 examples/leave_approval 目录或仓库根目录运行")
}

// StartLeaveApprovalProcess 以员工身份发起请假流程，返回流程实例 ID。
func (w *LeaveApprovalWorkflow) StartLeaveApprovalProcess(ctx context.Context, leaveRequest *LeaveRequest) (string, error) {
	variables := map[string]interface{}{
		"employeeId":   leaveRequest.EmployeeID,
		"employeeName": leaveRequest.EmployeeName,
		"leaveType":    leaveRequest.LeaveType,
		"startDate":    leaveRequest.StartDate.Format("2006-01-02"),
		"endDate":      leaveRequest.EndDate.Format("2006-01-02"),
		"days":         leaveRequest.Days,
		"reason":       leaveRequest.Reason,
		"managerId":    leaveRequest.ManagerID,
		"hrId":         leaveRequest.HrID,
		"status":       "pending",
		"submittedAt":  time.Now().Format(constants.TimeFormatLayout),
	}

	// 发起人身份必须带 UserID/TenantID，缺租户会被权限校验拒绝
	return w.runtimeService.StartProcessInstanceByKey(ctx, service.Actor{
		UserID:   leaveRequest.EmployeeID,
		UserName: leaveRequest.EmployeeName,
		TenantID: "default",
	}, "leave_approval",
		fmt.Sprintf("leave_%s_%d", leaveRequest.EmployeeID, time.Now().Unix()),
		variables)
}

// ApproveLeaveRequest 审批任务：approved=true 同意，false 拒绝（触发节点的驳回策略）。
func (w *LeaveApprovalWorkflow) ApproveLeaveRequest(ctx context.Context, taskID, userID string, approved bool, comment string) error {
	approvalResult := enums.ApprovalResultApproved
	if !approved {
		approvalResult = enums.ApprovalResultRejected
	}
	// actor 必须带 TenantID：UserID 非空而租户为空的"半构造 actor"会被租户校验拒绝
	return w.taskService.CompleteWithApproval(ctx, service.Actor{UserID: userID, TenantID: "default"}, &service.ApprovalRequest{
		TaskID:         taskID,
		ApprovalResult: approvalResult,
		Comment:        comment,
		Variables: map[string]interface{}{
			"approved":   approved,
			"comment":    comment,
			"approvedBy": userID,
			"approvedAt": time.Now().Format(constants.TimeFormatLayout),
			"status":     map[bool]string{true: "approved", false: "rejected"}[approved],
		},
	})
}

// GetUserTasks 查询用户的待办任务（pending/active）。
func (w *LeaveApprovalWorkflow) GetUserTasks(ctx context.Context, userID string) ([]*model.WfTask, error) {
	tasks, _, err := w.taskService.GetTaskList(ctx, service.ActorFromCtx(ctx), &dto.TaskQuery{
		Assignee: userID,
		PageRequest: dto.PageRequest{
			Status:   []string{string(enums.TaskStatusPending), string(enums.TaskStatusActive)},
			PageSize: 100,
		},
	})
	return tasks, err
}

// GetProcessInstanceStatus 查询流程实例状态（含流程变量）。
func (w *LeaveApprovalWorkflow) GetProcessInstanceStatus(ctx context.Context, processInstanceID string) (*dto.ProcessInstanceResponse, error) {
	instance, err := w.runtimeService.GetProcessInstance(ctx, service.ActorFromCtx(ctx), processInstanceID)
	if err != nil {
		return nil, err
	}

	response := &dto.ProcessInstanceResponse{
		ID:                  instance.ID,
		ProcessDefinitionID: instance.ProcessID,
		BusinessKey:         instance.BusinessKey,
		Name:                instance.Name,
		Status:              instance.Status,
		StartTime:           instance.CreatedAt,
		TenantID:            instance.TenantID,
		StartedBy:           instance.CreatedBy,
		Priority:            instance.Priority,
		ParentID:            instance.ParentID,
	}
	if instance.Variables != nil && *instance.Variables != "" {
		var variables map[string]interface{}
		if err := json.Unmarshal([]byte(*instance.Variables), &variables); err == nil {
			response.Variables = variables
		}
	}
	return response, nil
}

func main() {
	ctx := context.Background()

	// serviceTask 节点引用的通知函数；真实系统里对接邮件/IM 即可。
	// 经 WithServiceFuncs 随组件引导注册：实现进运行时注册表，元数据进设计器目录。
	serviceFuncs := []components.ServiceFunc{
		{
			Def: components.ServiceFuncDef{
				Name:  "NotificationService.sendApprovalNotification",
				Label: "发送审批结果通知",
				Desc:  "审批流程结束时向发起人发送结果通知",
			},
			Fn: func(ctx types.RuleContext, msg types.RuleMsg) {
				fmt.Println("  [通知] 审批流程结束，向发起人发送结果通知")
				ctx.TellSuccess(msg)
			},
		},
	}

	cfg := &config.Config{Database: databaseConfig()}

	// NewIDGenerator 为流程/实例/任务生成 UUID 主键
	builder := service.NewWorkflowEngineBuilder().
		SetName("LeaveApprovalEngine").
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

	// 注册全部 BPM 节点（userTask/serviceTask/ccTask/automation/subProcess/aiAgent/httpCall）：
	// 依赖由引擎自取，宿主服务函数随引导一并注册
	if err := components.RegisterFromEngine(engine, components.WithServiceFuncs(serviceFuncs)); err != nil {
		log.Fatalf("注册工作流组件失败: %v", err)
	}

	leaveWorkflow := NewLeaveApprovalWorkflow(engine)
	if err := leaveWorkflow.DeployLeaveApprovalProcess(ctx); err != nil {
		log.Fatalf("部署流程失败: %v", err)
	}
	fmt.Println("请假审批流程部署成功（processKey=leave_approval）")

	scenario := "all"
	if len(os.Args) > 1 {
		scenario = os.Args[1]
	}
	switch scenario {
	case "short":
		shortLeaveDemo(ctx, leaveWorkflow)
	case "long":
		longLeaveDemo(ctx, leaveWorkflow)
	case "sequential":
		sequentialLeaveDemo(ctx, leaveWorkflow)
	case "all":
		shortLeaveDemo(ctx, leaveWorkflow)
		longLeaveDemo(ctx, leaveWorkflow)
		sequentialLeaveDemo(ctx, leaveWorkflow)
	default:
		log.Fatalf("未知场景 %q，可选: short | long | sequential | all", scenario)
	}
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
		return &config.DatabaseConfig{
			Driver:       driver,
			Dsn:          dsn,
			MaxOpenConns: 10,
			MaxIdleConns: 5,
		}
	}
	return demo.NewDatabaseConfig()
}

// startLeave 发起请假并打印实例 ID。
func startLeave(ctx context.Context, w *LeaveApprovalWorkflow, req *LeaveRequest) string {
	instanceID, err := w.StartLeaveApprovalProcess(ctx, req)
	if err != nil {
		log.Fatalf("发起流程失败: %v", err)
	}
	fmt.Printf("  员工 %s 发起 %d 天请假，流程实例: %s\n", req.EmployeeName, req.Days, instanceID)
	return instanceID
}

// waitAndApprove 轮询等待 userID 的待办出现并审批通过；
// 实例先到终态（如会签达阈值提前完成）或超时返回 false。
// 任务创建与实例完结都是异步的（节点在独立 goroutine 中执行），
// 审批驱动的后续推进是同步的（CompleteWithApproval 返回时下一步已完成）；
// 生产系统可改用事件监听器推送，避免轮询。
func waitAndApprove(ctx context.Context, w *LeaveApprovalWorkflow, instanceID, userID, comment string) bool {
	deadline := time.Now().Add(10 * time.Second)
	for {
		if instanceFinished(ctx, w, instanceID) {
			fmt.Printf("  流程已到终态，%s 不再有待办\n", userID)
			return false
		}
		tasks, err := w.GetUserTasks(ctx, userID)
		if err != nil {
			log.Fatalf("查询 %s 的待办失败: %v", userID, err)
		}
		if len(tasks) > 0 {
			taskID := tasks[0].ID
			if err := w.ApproveLeaveRequest(ctx, taskID, userID, true, comment); err != nil {
				log.Fatalf("审批失败(task=%s): %v", taskID, err)
			}
			fmt.Printf("  %s 审批通过任务 %s：%s\n", userID, taskID, comment)
			return true
		}
		if time.Now().After(deadline) {
			fmt.Printf("  %s 在 10 秒内未出现待办任务\n", userID)
			return false
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// printStatus 等待实例到达终态（完结是异步归档的）并打印最终状态。
func printStatus(ctx context.Context, w *LeaveApprovalWorkflow, instanceID string) {
	deadline := time.Now().Add(10 * time.Second)
	for {
		status, err := w.GetProcessInstanceStatus(ctx, instanceID)
		if err != nil {
			log.Fatalf("查询流程状态失败: %v", err)
		}
		switch status.Status {
		case "completed", "terminated", "failed":
			fmt.Printf("  流程实例 %s 最终状态: %s\n\n", instanceID, status.Status)
			return
		}
		if time.Now().After(deadline) {
			fmt.Printf("  流程实例 %s 当前状态: %s（等待终态超时）\n\n", instanceID, status.Status)
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// shortLeaveDemo ≤3 天：经理单签（node_manager_approval，approvalType=single）。
func shortLeaveDemo(ctx context.Context, w *LeaveApprovalWorkflow) {
	fmt.Println("=== 场景一：短期请假（≤3 天，经理单签） ===")
	instanceID := startLeave(ctx, w, &LeaveRequest{
		EmployeeID:   "emp001",
		EmployeeName: "张三",
		LeaveType:    "annual",
		StartDate:    time.Now().AddDate(0, 0, 7),
		EndDate:      time.Now().AddDate(0, 0, 9),
		Days:         3,
		Reason:       "家庭事务",
		ManagerID:    "mgr001",
		HrID:         "hr001",
	})
	waitAndApprove(ctx, w, instanceID, "mgr001", "同意请假，注意工作交接")
	printStatus(ctx, w, instanceID)
}

// longLeaveDemo 3~7 天：经理 + HR 并行会签（node_hr_approval，countersign + all）。
// 两人同时收到待办，全部同意后流程继续。
func longLeaveDemo(ctx context.Context, w *LeaveApprovalWorkflow) {
	fmt.Println("=== 场景二：中长期请假（3~7 天，经理+HR 并行会签） ===")
	instanceID := startLeave(ctx, w, &LeaveRequest{
		EmployeeID:   "emp002",
		EmployeeName: "李四",
		LeaveType:    "personal",
		StartDate:    time.Now().AddDate(0, 0, 14),
		EndDate:      time.Now().AddDate(0, 0, 21),
		Days:         7,
		Reason:       "个人事务处理",
		ManagerID:    "mgr001",
		HrID:         "hr001",
	})
	waitAndApprove(ctx, w, instanceID, "mgr001", "经理同意长期请假申请")
	waitAndApprove(ctx, w, instanceID, "hr001", "长期请假已批准，请做好工作安排")
	printStatus(ctx, w, instanceID)
}

// sequentialLeaveDemo >7 天：三人顺序会签（node_sequential_approval，isSequential + majority）。
// 任务按 user001 → user002 → user003 依次生成，严格过半（2 票）即通过，剩余任务自动终止。
func sequentialLeaveDemo(ctx context.Context, w *LeaveApprovalWorkflow) {
	fmt.Println("=== 场景三：超长期请假（>7 天，三人顺序会签） ===")
	instanceID := startLeave(ctx, w, &LeaveRequest{
		EmployeeID:   "emp003",
		EmployeeName: "王五",
		LeaveType:    "sick",
		StartDate:    time.Now().AddDate(0, 0, 30),
		EndDate:      time.Now().AddDate(0, 0, 45),
		Days:         15,
		Reason:       "手术康复需要长期休养",
		ManagerID:    "mgr001",
		HrID:         "hr001",
	})

	comments := map[string]string{
		"user001": "张三同意，员工确实需要康复时间",
		"user002": "李四同意，支持员工康复",
		"user003": "王五同意，按多数同意原则通过",
	}
	for _, userID := range []string{"user001", "user002", "user003"} {
		waitAndApprove(ctx, w, instanceID, userID, comments[userID])
	}
	printStatus(ctx, w, instanceID)
}

// instanceFinished 判断实例是否已到终态（completed/terminated/failed）。
func instanceFinished(ctx context.Context, w *LeaveApprovalWorkflow, instanceID string) bool {
	status, err := w.GetProcessInstanceStatus(ctx, instanceID)
	if err != nil {
		return false
	}
	switch status.Status {
	case "completed", "terminated", "failed":
		return true
	}
	return false
}

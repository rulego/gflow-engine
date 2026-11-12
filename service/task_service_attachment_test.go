package service

// Tests for task_service_attachment.go: 附件引用校验链、意见行附件落库、
// 回退补写意见行，以及时间线意见源的账本行合并。

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/query"
	"github.com/rulego/gflow-engine/types/constants"
	"github.com/rulego/gflow-engine/types/dto"
	"github.com/rulego/gflow-engine/types/enums"
)

// ---- 夹具 ----

// attachmentUploadDefinition 节点 a 的 uploadAttachment 开关可开可关；
// 节点 a→b 均为 userTask，供回退用例构造上游节点。
func attachmentUploadDefinition(uploadOn bool) string {
	on := "false"
	if uploadOn {
		on = "true"
	}
	return fmt.Sprintf(`{"ruleChain":{},`+
		`"metadata":{"nodes":[{"id":"a","type":"userTask","additionalInfo":{"actionPermissions":{"uploadAttachment":%s}}},`+
		`{"id":"b","type":"userTask","additionalInfo":{"actionPermissions":{"return":true,"uploadAttachment":%s}}}],`+
		`"connections":[{"fromId":"a","toId":"b","type":"Success"}]}}`, on, on)
}

func attachmentSeedProcess(t *testing.T, q *query.Query, def string) {
	t.Helper()
	require.NoError(t, q.WfProcess.Create(&model.WfProcess{
		ID: "proc-att", ProcessKey: "att-test", Name: "附件测试", Status: "active",
		DefinitionJSON: def, TenantID: "t1", CreatedBy: "system",
	}))
}

func attachmentSeedInstance(t *testing.T, q *query.Query, id string) {
	t.Helper()
	require.NoError(t, q.WfInstance.Create(&model.WfInstance{
		ID: id, ProcessID: "proc-att", Name: "附件测试实例", Status: string(enums.InstanceStatusActive),
		TenantID: "t1", CreatedBy: "system", StartUserID: "starter", CreatedAt: time.Now(),
	}))
}

// attachmentSeedTask 建在办任务；defKey 决定落在哪个节点（开关与回退上游不同）。
func attachmentSeedTask(t *testing.T, q *query.Query, id, instID, defKey, status, assignee string) {
	t.Helper()
	task := &model.WfTask{
		ID: id, ProcessInstanceID: secFixStrPtr(instID), ProcessID: "proc-att",
		TaskDefKey: defKey, Name: defKey, TaskType: constants.TaskTypeUserTask,
		Status: status, TenantID: "t1", CreatedBy: "system",
		CreatedAt: time.Now().Add(-time.Hour),
	}
	if assignee != "" {
		task.Assignee = secFixStrPtr(assignee)
	}
	require.NoError(t, q.WfTask.Create(task))
}

// attachmentApprove 走审批人本人的 CompleteWithApproval（approve 语义）。
func attachmentApprove(svc *TaskServiceImpl, taskID, comment string, atts []dto.ApprovalAttachment) error {
	return svc.CompleteWithApproval(
		SetUserToCtx(context.Background(), &Actor{UserID: "yi", UserName: "老幺", TenantID: "t1"}),
		Actor{UserID: "yi", UserName: "老幺", TenantID: "t1"},
		&ApprovalRequest{TaskID: taskID, ApprovalResult: enums.ApprovalResultApproved, Comment: comment, Attachments: atts})
}

func attachmentCommentsOf(t *testing.T, q *query.Query, taskID string) []*model.WfTaskComment {
	t.Helper()
	rows, err := q.WfTaskComment.WithContext(context.Background()).
		Where(q.WfTaskComment.TaskID.Eq(taskID)).Order(q.WfTaskComment.CreatedAt).Find()
	require.NoError(t, err)
	return rows
}

func attRef(filename, path string, size int64) dto.ApprovalAttachment {
	return dto.ApprovalAttachment{Filename: filename, Path: path, Size: size}
}

// ---- 用例 ----

// path 校验纯函数表驱动：租户前缀、穿越、绝对路径、空段全拒绝。
func TestValidateAttachmentPath(t *testing.T) {
	cases := []struct {
		name    string
		path    string
		wantErr bool
	}{
		{"正常", "t1/approval/2026-11-11/a.pdf", false},
		{"空路径", "", true},
		{"绝对路径", "/t1/approval/a.pdf", true},
		{"反斜杠", "t1\\approval\\a.pdf", true},
		{"跨租户前缀", "t2/approval/a.pdf", true},
		{"父目录穿越", "t1/approval/../a.pdf", true},
		{"当前段", "t1/./a.pdf", true},
		{"空段", "t1//a.pdf", true},
		{"尾斜杠", "t1/approval/", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := validateAttachmentPath(c.path, "t1")
			if c.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// 开关关（未显式开启）时带附件审批必须拒绝：uploadAttachment 默认关、fail-closed。
func TestApproveAttachments_SwitchOff(t *testing.T) {
	q := proxyAuditDB(t)
	attachmentSeedProcess(t, q, attachmentUploadDefinition(false))
	attachmentSeedInstance(t, q, "att-inst-1")
	attachmentSeedTask(t, q, "t-att1", "att-inst-1", "a", string(enums.TaskStatusActive), "yi")

	svc, _ := newRecallSvc(q, attachmentUploadDefinition(false), nil)
	err := attachmentApprove(svc, "t-att1", "同意", []dto.ApprovalAttachment{attRef("a.pdf", "t1/approval/2026-11-11/a.pdf", 1024)})
	require.ErrorIs(t, err, ErrPermissionDenied)
	// 任务不得被完成
	row, gerr := q.WfTask.WithContext(context.Background()).Where(q.WfTask.ID.Eq("t-att1")).First()
	require.NoError(t, gerr)
	require.Equal(t, string(enums.TaskStatusActive), row.Status)
	require.Empty(t, attachmentCommentsOf(t, q, "t-att1"), "被拒动作不得残留意见行")
}

// 开关开：附件随意见同事务落行，JSON 引用完整。
func TestApproveAttachments_SwitchOn(t *testing.T) {
	q := proxyAuditDB(t)
	def := attachmentUploadDefinition(true)
	attachmentSeedProcess(t, q, def)
	attachmentSeedInstance(t, q, "att-inst-2")
	attachmentSeedTask(t, q, "t-att2", "att-inst-2", "a", string(enums.TaskStatusActive), "yi")

	svc, _ := newRecallSvc(q, def, nil)
	atts := []dto.ApprovalAttachment{
		attRef("凭证.pdf", "t1/approval/2026-11-11/凭证.pdf", 2048),
		attRef("现场.jpg", "t1/approval/2026-11-11/现场.jpg", 4096),
	}
	require.NoError(t, attachmentApprove(svc, "t-att2", "材料属实", atts))

	rows := attachmentCommentsOf(t, q, "t-att2")
	require.Len(t, rows, 1)
	require.Equal(t, "材料属实", rows[0].Content)
	require.Equal(t, "yi", rows[0].UserID)
	require.NotNil(t, rows[0].Attachments)
	var stored []dto.ApprovalAttachment
	require.NoError(t, json.Unmarshal([]byte(*rows[0].Attachments), &stored))
	require.Equal(t, atts, stored)
}

// 空意见纯凭证是合法动作：意见行落库（content 空、attachments 非空）。
func TestApproveAttachments_EmptyCommentAllowed(t *testing.T) {
	q := proxyAuditDB(t)
	def := attachmentUploadDefinition(true)
	attachmentSeedProcess(t, q, def)
	attachmentSeedInstance(t, q, "att-inst-3")
	attachmentSeedTask(t, q, "t-att3", "att-inst-3", "a", string(enums.TaskStatusActive), "yi")

	svc, _ := newRecallSvc(q, def, nil)
	require.NoError(t, attachmentApprove(svc, "t-att3", "", []dto.ApprovalAttachment{attRef("a.pdf", "t1/approval/d/a.pdf", 10)}))

	rows := attachmentCommentsOf(t, q, "t-att3")
	require.Len(t, rows, 1, "只传凭证不写意见也须落意见行")
	require.Equal(t, "", rows[0].Content)
	require.NotNil(t, rows[0].Attachments)
}

// 意见与附件均空：不落行（语义与存量"意见为空跳过"一致）。
func TestApproveAttachments_BothEmptySkipsRow(t *testing.T) {
	q := proxyAuditDB(t)
	def := attachmentUploadDefinition(true)
	attachmentSeedProcess(t, q, def)
	attachmentSeedInstance(t, q, "att-inst-4")
	attachmentSeedTask(t, q, "t-att4", "att-inst-4", "a", string(enums.TaskStatusActive), "yi")

	svc, _ := newRecallSvc(q, def, nil)
	require.NoError(t, attachmentApprove(svc, "t-att4", "", nil))
	require.Empty(t, attachmentCommentsOf(t, q, "t-att4"))
}

// 引用合法性表驱动：数量/大小/文件名/path 逐项拒绝，权威校验在引擎。
func TestApproveAttachments_RejectsInvalidRefs(t *testing.T) {
	big := int64(50)<<20 + 1
	var eleven []dto.ApprovalAttachment
	for i := 0; i < 11; i++ {
		eleven = append(eleven, attRef(fmt.Sprintf("f%d.pdf", i), fmt.Sprintf("t1/approval/d/f%d.pdf", i), 100))
	}
	cases := []struct {
		name string
		atts []dto.ApprovalAttachment
	}{
		{"超数量上限", eleven},
		{"单文件超大小", []dto.ApprovalAttachment{attRef("big.bin", "t1/approval/d/big.bin", big)}},
		{"大小为零", []dto.ApprovalAttachment{attRef("zero.bin", "t1/approval/d/zero.bin", 0)}},
		{"大小为负", []dto.ApprovalAttachment{attRef("neg.bin", "t1/approval/d/neg.bin", -1)}},
		{"文件名空白", []dto.ApprovalAttachment{attRef("  ", "t1/approval/d/a.pdf", 10)}},
		{"path 跨租户", []dto.ApprovalAttachment{attRef("a.pdf", "t2/approval/d/a.pdf", 10)}},
		{"path 穿越", []dto.ApprovalAttachment{attRef("a.pdf", "t1/approval/../a.pdf", 10)}},
	}
	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			q := proxyAuditDB(t)
			def := attachmentUploadDefinition(true)
			attachmentSeedProcess(t, q, def)
			instID := fmt.Sprintf("att-inst-r%d", i)
			taskID := fmt.Sprintf("t-att-r%d", i)
			attachmentSeedInstance(t, q, instID)
			attachmentSeedTask(t, q, taskID, instID, "a", string(enums.TaskStatusActive), "yi")
			svc, _ := newRecallSvc(q, def, nil)
			err := attachmentApprove(svc, taskID, "意见", c.atts)
			require.ErrorIs(t, err, ErrValidation)
			require.Empty(t, attachmentCommentsOf(t, q, taskID))
		})
	}
}

// 回退补写意见行：原因随退回动作落账本，时间线意见源升级后回退说明不丢。
func TestReturn_RecordsApprovalComment(t *testing.T) {
	q := proxyAuditDB(t)
	def := attachmentUploadDefinition(true)
	attachmentSeedProcess(t, q, def)
	attachmentSeedInstance(t, q, "att-inst-5")
	// 节点 a 的已完成票 + 节点 b 的在办票：b 退回 a
	attachmentSeedTask(t, q, "t-att5a", "att-inst-5", "a", string(enums.TaskStatusCompleted), "yi")
	attachmentSeedTask(t, q, "t-att5b", "att-inst-5", "b", string(enums.TaskStatusActive), "er")

	svc, _ := newRecallSvc(q, def, nil)
	err := svc.Return(
		SetUserToCtx(context.Background(), &Actor{UserID: "er", UserName: "老二", TenantID: "t1"}),
		Actor{UserID: "er", TenantID: "t1"}, "t-att5b", "a", "材料不齐", nil)
	require.NoError(t, err)

	rows := attachmentCommentsOf(t, q, "t-att5b")
	require.Len(t, rows, 1)
	require.Equal(t, "材料不齐", rows[0].Content)
	require.Equal(t, "er", rows[0].UserID)
	require.Nil(t, rows[0].Attachments)
}

// 回退带附件：附件引用随回退意见行落库（同一开关）。
func TestReturn_AttachmentsRideCommentRow(t *testing.T) {
	q := proxyAuditDB(t)
	def := attachmentUploadDefinition(true)
	attachmentSeedProcess(t, q, def)
	attachmentSeedInstance(t, q, "att-inst-6")
	attachmentSeedTask(t, q, "t-att6a", "att-inst-6", "a", string(enums.TaskStatusCompleted), "yi")
	attachmentSeedTask(t, q, "t-att6b", "att-inst-6", "b", string(enums.TaskStatusActive), "er")

	svc, _ := newRecallSvc(q, def, nil)
	atts := []dto.ApprovalAttachment{attRef("退回说明.pdf", "t1/approval/d/退回说明.pdf", 512)}
	err := svc.Return(
		SetUserToCtx(context.Background(), &Actor{UserID: "er", UserName: "老二", TenantID: "t1"}),
		Actor{UserID: "er", TenantID: "t1"}, "t-att6b", "a", "", atts)
	require.NoError(t, err)

	rows := attachmentCommentsOf(t, q, "t-att6b")
	require.Len(t, rows, 1)
	require.Equal(t, "", rows[0].Content)
	require.NotNil(t, rows[0].Attachments)
}

// 回退意见与附件均空：不落行，end_reason 也不带说明。
func TestReturn_EmptyReasonSkipsCommentRow(t *testing.T) {
	q := proxyAuditDB(t)
	def := attachmentUploadDefinition(true)
	attachmentSeedProcess(t, q, def)
	attachmentSeedInstance(t, q, "att-inst-7")
	attachmentSeedTask(t, q, "t-att7a", "att-inst-7", "a", string(enums.TaskStatusCompleted), "yi")
	attachmentSeedTask(t, q, "t-att7b", "att-inst-7", "b", string(enums.TaskStatusActive), "er")

	svc, _ := newRecallSvc(q, def, nil)
	err := svc.Return(
		SetUserToCtx(context.Background(), &Actor{UserID: "er", TenantID: "t1"}),
		Actor{UserID: "er", TenantID: "t1"}, "t-att7b", "a", "", nil)
	require.NoError(t, err)
	require.Empty(t, attachmentCommentsOf(t, q, "t-att7b"))
}

// marshalAttachments 空清单保持 NULL 列（nil）。
func TestMarshalAttachments_EmptyNil(t *testing.T) {
	col, err := marshalAttachments(nil)
	require.NoError(t, err)
	require.Nil(t, col)
	col, err = marshalAttachments([]dto.ApprovalAttachment{})
	require.NoError(t, err)
	require.Nil(t, col)
}

// 时间线意见源合并：账本行优先（首行文本+全行附件并集）、快照兜底、
// 子任务递归、损坏 JSON 跳过。
func TestMergeTimelineComments(t *testing.T) {
	lead := "同意"
	attsJSON := `[{"filename":"a.pdf","path":"t1/approval/d/a.pdf","size":10}]`
	broken := "not-json"
	snapshot := "快照意见"
	subSnapshot := "子任务快照"

	execs := []dto.ExecutionInfo{
		{TaskID: "task-with-ledger", Comment: &snapshot},
		{TaskID: "task-snapshot-only", Comment: &snapshot},
		{TaskID: "task-empty-ledger"}, // 账本行内容为空：显式空意见，不回退快照
		{
			TaskID: "task-parent",
			SubExecutions: []dto.ExecutionInfo{
				{TaskID: "task-sub", Comment: &subSnapshot},
			},
		},
	}
	byTask := map[string][]*model.WfTaskComment{
		"task-with-ledger": {
			{TaskID: "task-with-ledger", Content: lead, Attachments: &attsJSON},
			{TaskID: "task-with-ledger", Content: "代审：管理员 代 老幺 通过该审批"},
		},
		"task-empty-ledger": {
			{TaskID: "task-empty-ledger", Content: "", Attachments: &broken},
		},
		"task-sub": {
			{TaskID: "task-sub", Content: "子任务意见", Attachments: &attsJSON},
		},
	}

	mergeTimelineComments(execs, byTask)

	// 账本行优先：文本取首行，附件取并集，快照被替换
	require.Equal(t, lead, *execs[0].Comment)
	require.Len(t, execs[0].Attachments, 1)
	require.Equal(t, "a.pdf", execs[0].Attachments[0].Filename)

	// 无账本行：快照兜底
	require.Equal(t, snapshot, *execs[1].Comment)
	require.Empty(t, execs[1].Attachments)

	// 空内容账本行：显式空意见（不回退快照），损坏 JSON 附件跳过
	require.Empty(t, *execs[2].Comment)
	require.Empty(t, execs[2].Attachments)

	// 子任务递归同口径
	sub := execs[3].SubExecutions[0]
	require.Equal(t, "子任务意见", *sub.Comment)
	require.Len(t, sub.Attachments, 1)
}

// 代审携带附件：与普通审批同一节点开关；附件随本人意见行落库、系统行不带。
func TestProxyAudit_Attachments(t *testing.T) {
	q := proxyAuditDB(t)
	def := attachmentUploadDefinition(true)
	attachmentSeedProcess(t, q, def)
	attachmentSeedInstance(t, q, "att-inst-px")
	proxySeedTask(t, q, "t-att-px", "att-inst-px", "a", "yi", `{"amount":1}`)

	svc, _ := newRecallSvc(q, def, nil)
	admin := Actor{UserID: "admin", TenantID: "t1", UserName: "管理员", WorkflowAdmin: true}
	ctx := SetUserToCtx(context.Background(), &Actor{UserID: "admin", TenantID: "t1", UserName: "管理员", WorkflowAdmin: true})
	atts := []dto.ApprovalAttachment{attRef("凭证.pdf", "t1/approval/d/凭证.pdf", 2048)}
	require.NoError(t, svc.ProxyAudit(ctx, admin, "t-att-px", true, "材料属实", atts))

	rows := attachmentCommentsOf(t, q, "t-att-px")
	require.Len(t, rows, 2, "代审票应有本人意见行+系统行")
	require.Equal(t, "材料属实", rows[0].Content)
	require.NotNil(t, rows[0].Attachments, "附件随本人意见行落库")
	require.Nil(t, rows[1].Attachments, "系统行不带附件")
	// 系统行落行时间晚于意见行：时间线取首行时系统行不得反超
	require.True(t, rows[1].CreatedAt.After(rows[0].CreatedAt), "系统行时间须显式晚于本人意见行")
}

// 代审节点开关关闭：带附件的代审被拒，票面不动。
func TestProxyAudit_AttachmentsSwitchOff(t *testing.T) {
	q := proxyAuditDB(t)
	def := attachmentUploadDefinition(false)
	attachmentSeedProcess(t, q, def)
	attachmentSeedInstance(t, q, "att-inst-px2")
	proxySeedTask(t, q, "t-att-px2", "att-inst-px2", "a", "yi", `{"amount":1}`)

	svc, _ := newRecallSvc(q, def, nil)
	admin := Actor{UserID: "admin", TenantID: "t1", WorkflowAdmin: true}
	ctx := SetUserToCtx(context.Background(), &Actor{UserID: "admin", TenantID: "t1", WorkflowAdmin: true})
	err := svc.ProxyAudit(ctx, admin, "t-att-px2", true, "越级确认",
		[]dto.ApprovalAttachment{attRef("a.pdf", "t1/approval/d/a.pdf", 10)})
	require.ErrorIs(t, err, ErrPermissionDenied)
	require.Empty(t, attachmentCommentsOf(t, q, "t-att-px2"))
}

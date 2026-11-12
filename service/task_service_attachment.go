// 审批动作附件的校验链与序列化：文件本体先落宿主 /files 存储，动作请求只带
// 引用元数据，引擎按节点开关 + 引用合法性拒绝非法载荷，引用随意见同事务落库。

package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/types/dto"
)

// 动作附件上限：单动作最多 10 个、单文件最大 50MB，与宿主 /files/upload 的
// 单文件上限一致。上传口已拦文件实体，这里的 size 校验针对的是引用谎报
// （防把超大引用写进意见行绕过展示端的预期）。导出供宿主浅校验引用同一份值。
const (
	MaxActionAttachments = 10
	MaxAttachmentBytes   = int64(50) << 20
)

// validateActionAttachments 审批动作附件校验链（任一不过返回错误，事务回滚）：
//  1. 节点 actionPermissions.uploadAttachment 显式开启（默认关，fail-closed）；
//  2. 数量 ≤10；
//  3. 引用字段完整、单文件 size ≤50MB；
//  4. path 首段=任务租户 ID（防把别家租户的文件挂进自家意见行）、无穿越段。
//
// attachments 为空直接放行——不带附件的既有调用方零行为变化。
// 须在实例事务内调用（流程定义读取走 scope 连接，避免 tx 逃逸）。
func (s *TaskServiceImpl) validateActionAttachments(ctx context.Context, scope *InstanceScope, task *model.WfTask, attachments []dto.ApprovalAttachment) error {
	if len(attachments) == 0 {
		return nil
	}
	if err := s.requireUploadAttachmentEnabled(ctx, scope, task); err != nil {
		return err
	}
	if len(attachments) > MaxActionAttachments {
		return fmt.Errorf("attachments exceed limit %d per action: %w", MaxActionAttachments, ErrValidation)
	}
	for i, a := range attachments {
		if strings.TrimSpace(a.Filename) == "" {
			return fmt.Errorf("attachment #%d filename cannot be empty: %w", i+1, ErrValidation)
		}
		if a.Size <= 0 || a.Size > MaxAttachmentBytes {
			return fmt.Errorf("attachment %q size %d out of range: %w", a.Filename, a.Size, ErrValidation)
		}
		if err := validateAttachmentPath(a.Path, task.TenantID); err != nil {
			return fmt.Errorf("attachment %q: %w", a.Filename, err)
		}
	}
	return nil
}

// requireUploadAttachmentEnabled 校验节点 uploadAttachment 开关。与 transfer/return
// 等"默认开"动作的 designerDisabled 判定相反：uploadAttachment 默认关，必须显式
// true 才放行，未配置/显式 false/解析失败一律拒绝（fail-closed，解析不出即无法
// 证明设计器开启过）。读取走 scope 连接，判定口径与详情 DTO 的按钮位同源。
func (s *TaskServiceImpl) requireUploadAttachmentEnabled(ctx context.Context, scope *InstanceScope, task *model.WfTask) error {
	if task == nil || task.ProcessInstanceID == nil || *task.ProcessInstanceID == "" {
		return fmt.Errorf("cannot resolve action permissions for uploadAttachment: %w", ErrPermissionDenied)
	}
	if scope == nil {
		return fmt.Errorf("cannot resolve action permissions for uploadAttachment: %w", ErrPermissionDenied)
	}
	instance, err := scope.Instances().Get(ctx, *task.ProcessInstanceID)
	if err != nil || instance == nil || instance.ProcessID == "" {
		logrus.WithError(err).WithField("instanceId", *task.ProcessInstanceID).
			Warn("resolve instance failed for uploadAttachment check")
		return fmt.Errorf("cannot resolve action permissions for uploadAttachment: %w", ErrPermissionDenied)
	}
	procDef, err := scope.Processes().Get(ctx, instance.ProcessID)
	if err != nil || procDef == nil {
		logrus.WithError(err).WithField("processId", instance.ProcessID).
			Warn("resolve process failed for uploadAttachment check")
		return fmt.Errorf("cannot resolve action permissions for uploadAttachment: %w", ErrPermissionDenied)
	}
	rc, err := procDef.ToRuleChain()
	if err != nil || rc == nil {
		logrus.WithError(err).WithField("processId", instance.ProcessID).
			Warn("parse rule chain failed for uploadAttachment check")
		return fmt.Errorf("cannot resolve action permissions for uploadAttachment: %w", ErrPermissionDenied)
	}
	node, ok := rc.GetNode(task.TaskDefKey)
	if !ok {
		return fmt.Errorf("cannot resolve action permissions for uploadAttachment: node %s not found: %w",
			task.TaskDefKey, ErrPermissionDenied)
	}
	ap, ok := node.GetAdditionalInfo("actionPermissions")
	if !ok {
		return fmt.Errorf("action %q not enabled by designer on node %s: %w",
			"uploadAttachment", task.TaskDefKey, ErrPermissionDenied)
	}
	v, ok := ap.(map[string]interface{})
	if !ok {
		return fmt.Errorf("cannot resolve action permissions for uploadAttachment: actionPermissions has wrong type: %w", ErrPermissionDenied)
	}
	if !designerEnabled(v, "uploadAttachment") {
		return fmt.Errorf("action %q not enabled by designer on node %s: %w",
			"uploadAttachment", task.TaskDefKey, ErrPermissionDenied)
	}
	return nil
}

// validateAttachmentPath 附件引用路径校验：必须是 {tenantID}/... 形态的相对路径，
// 拒绝绝对路径、反斜杠、空段与 "."/".." 穿越——path 是宿主 /files 下载与租户
// 隔离校验的锚点，被污染的引用会把下载口指向非预期位置。
func validateAttachmentPath(path, tenantID string) error {
	if path == "" {
		return fmt.Errorf("path cannot be empty: %w", ErrValidation)
	}
	if strings.HasPrefix(path, "/") || strings.Contains(path, "\\") {
		return fmt.Errorf("path must be relative with / separators: %w", ErrValidation)
	}
	segs := strings.Split(path, "/")
	if segs[0] != tenantID {
		return fmt.Errorf("path tenant %q does not match task tenant: %w", segs[0], ErrValidation)
	}
	for _, seg := range segs {
		if seg == "" || seg == "." || seg == ".." {
			return fmt.Errorf("path contains empty or traversal segment: %w", ErrValidation)
		}
	}
	return nil
}

// marshalAttachments 序列化附件引用为意见行 attachments 列的 JSON 文本；
// 空清单返回 nil（列保持 NULL）。
func marshalAttachments(attachments []dto.ApprovalAttachment) (*string, error) {
	if len(attachments) == 0 {
		return nil, nil
	}
	b, err := json.Marshal(attachments)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize attachments: %w", err)
	}
	s := string(b)
	return &s, nil
}

package service

import (
	"context"
	"errors"
	"testing"

	"github.com/rulego/gflow-engine/types/constants"
)

// ensureTenantAccess 的租户闸语义：跨租户拒绝，豁免只给系统身份。
//
// 背景：统一租户校验时曾把「actor 租户为空」当成系统视角一律放行，
// 于是任何构造 Actor 时漏传租户的路径（旧 token 无租户 claim、内部回调
// 自造 actor 等）都能越过租户闸操作他人实例。豁免信号必须是显式的系统
// 身份（IsSystemActor），不能是「租户字段恰好为空」这种隐式条件。
func TestEnsureTenantAccess(t *testing.T) {
	tests := []struct {
		name           string
		actor          *Actor
		resourceTenant string
		wantErr        bool
	}{
		{
			name:           "同租户放行",
			actor:          &Actor{UserID: "u1", TenantID: "t1"},
			resourceTenant: "t1",
		},
		{
			name:           "跨租户拒绝",
			actor:          &Actor{UserID: "u1", TenantID: "t1"},
			resourceTenant: "t2",
			wantErr:        true,
		},
		{
			name:           "系统身份豁免：跨租户放行（平台自身操作，如定时任务巡检）",
			actor:          &Actor{UserID: constants.UserSystem, UserName: constants.UserSystem},
			resourceTenant: "t1",
		},
		{
			name:           "空租户的普通用户：拒绝（半构造 actor，不再视为系统视角）",
			actor:          &Actor{UserID: "u1", TenantID: ""},
			resourceTenant: "t1",
			wantErr:        true,
		},
		{
			// 引擎内部级联（aspect/节点回调）的 ctx 不带 actor——ActorFromCtx 对
			// 无用户 ctx 正是回退 SystemActor，这是既定约定而非异常路径。
			// 曾按"拒绝"实现，结果 create_task_aspect 的 triggerOnCompleted 查
			// 流程定义被租户闸拦下，onCompleted 触发链静默失效。
			name:           "ctx 无 actor：放行（引擎内部级联的既定语义）",
			actor:          nil,
			resourceTenant: "t1",
		},
		{
			name:           "资源无租户：放行（单租户部署 / 历史数据）",
			actor:          &Actor{UserID: "u1", TenantID: "t1"},
			resourceTenant: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			if tt.actor != nil {
				ctx = SetUserToCtx(ctx, tt.actor)
			}
			err := ensureTenantAccess(ctx, "process instance", tt.resourceTenant)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected tenant access denied, got nil")
				}
				if !errors.Is(err, ErrPermissionDenied) {
					t.Fatalf("expected ErrPermissionDenied, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("expected access granted, got %v", err)
			}
		})
	}
}

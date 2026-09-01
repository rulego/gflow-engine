package service

import (
	"context"
)

// BaseService BPM 服务基类：提供通用的上下文身份读取能力
type BaseService struct{}

// GetUsernameFromCtx 从上下文读取用户名，无身份时返回空串
func (s *BaseService) GetUsernameFromCtx(ctx context.Context) string {
	ident := GetUserFromCtx(ctx)
	if ident != nil {
		return ident.UserName
	}
	return ""
}

// GetUserIDFromCtx 从上下文读取用户ID，无身份时返回空串
func (s *BaseService) GetUserIDFromCtx(ctx context.Context) string {
	ident := GetUserFromCtx(ctx)
	if ident != nil {
		return ident.UserID
	}
	return ""
}

// GetTenantIDFromCtx 从上下文读取租户ID，无身份时返回空串
func (s *BaseService) GetTenantIDFromCtx(ctx context.Context) string {
	ident := GetUserFromCtx(ctx)
	if ident != nil {
		return ident.TenantID
	}
	return ""
}
func (s *BaseService) GetUserFromCtx(ctx context.Context) *Actor {
	return GetUserFromCtx(ctx)
}

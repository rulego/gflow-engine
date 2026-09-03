package service

import (
	"context"
	"github.com/rulego/gflow-engine/types/constants"
	"time"
)

// User 用户实体
type User struct {
	ID         string    `json:"id"`
	FirstName  string    `json:"firstName"`
	LastName   string    `json:"lastName"`
	Email      string    `json:"email"`
	Password   string    `json:"password,omitempty"`
	PictureID  string    `json:"pictureId"`
	TenantID   string    `json:"tenantId"`
	CreateTime time.Time `json:"createTime"`
	UpdateTime time.Time `json:"updateTime"`
}

// Group 组实体
type Group struct {
	ID         string    `json:"id"`
	Name       string    `json:"name"`
	Type       string    `json:"type"`
	TenantID   string    `json:"tenantId"`
	CreateTime time.Time `json:"createTime"`
	UpdateTime time.Time `json:"updateTime"`
}

// Tenant 租户实体
type Tenant struct {
	ID         string    `json:"id"`
	Name       string    `json:"name"`
	CreateTime time.Time `json:"createTime"`
	UpdateTime time.Time `json:"updateTime"`
}

// UserQuery 用户查询条件
type UserQuery struct {
	UserID        string   `json:"userId"`
	UserIDs       []string `json:"userIds"`
	FirstName     string   `json:"firstName"`
	FirstNameLike string   `json:"firstNameLike"`
	LastName      string   `json:"lastName"`
	LastNameLike  string   `json:"lastNameLike"`
	Email         string   `json:"email"`
	EmailLike     string   `json:"emailLike"`
	MemberOfGroup string   `json:"memberOfGroup"`
	TenantID      string   `json:"tenantId"`

	// 分页参数
	Page     int `json:"page"`
	PageSize int `json:"pageSize"`

	// 排序参数
	OrderBy   string `json:"orderBy"`
	OrderDesc bool   `json:"orderDesc"`
}

// GroupQuery 组查询条件
type GroupQuery struct {
	GroupID  string   `json:"groupId"`
	GroupIDs []string `json:"groupIds"`
	Name     string   `json:"name"`
	NameLike string   `json:"nameLike"`
	Type     string   `json:"type"`
	Member   string   `json:"member"`
	TenantID string   `json:"tenantId"`

	// 分页参数
	Page     int `json:"page"`
	PageSize int `json:"pageSize"`

	// 排序参数
	OrderBy   string `json:"orderBy"`
	OrderDesc bool   `json:"orderDesc"`
}

// TenantQuery 租户查询条件
type TenantQuery struct {
	TenantID  string   `json:"tenantId"`
	TenantIDs []string `json:"tenantIds"`
	Name      string   `json:"name"`
	NameLike  string   `json:"nameLike"`

	// 分页参数
	Page     int `json:"page"`
	PageSize int `json:"pageSize"`

	// 排序参数
	OrderBy   string `json:"orderBy"`
	OrderDesc bool   `json:"orderDesc"`
}

// IdentityService 身份服务接口（SPI）：引擎只定义接口并提供测试用 Mock 实现
// （IdentityServiceImpl），不提供生产级实现；上层应用应通过
// WorkflowEngineBuilder.SetIdentityService() 注入对接自身用户、角色、部门
// 数据源的生产实现。
type IdentityService interface {
	// GetUserIDsByRoleID 根据角色ID获取所有用户ID列表
	GetUserIDsByRoleID(ctx context.Context, tenantID string, roleID string) ([]string, error)

	// GetDepartmentManagerUserID 获取部门主管用户ID
	GetDepartmentManagerUserID(ctx context.Context, tenantID string, departmentID string) (string, error)

	// GetUserIDsByGroupID 根据组ID获取所有用户ID列表
	GetUserIDsByGroupID(ctx context.Context, tenantID string, groupID string) ([]string, error)

	// GetUserIDsByDepartmentID 根据部门ID获取所有用户ID列表
	GetUserIDsByDepartmentID(ctx context.Context, tenantID string, departmentID string) ([]string, error)

	// GetUserManagerID 获取用户的直接主管ID（无主管返回空字符串）
	GetUserManagerID(ctx context.Context, tenantID string, userID string) (string, error)

	// GetUserManagerHierarchy 获取用户的多级主管层级（按层级从低到高排序，直接主管在前；
	// levels 为获取的层级数，0 表示获取所有层级）
	GetUserManagerHierarchy(ctx context.Context, tenantID string, userID string, levels int) ([]string, error)

	// GetUserDepartmentID 获取用户所属部门ID（用户不属于任何部门时返回空字符串）
	GetUserDepartmentID(ctx context.Context, tenantID string, userID string) (string, error)

	// GetRoleIDsByUserID 反向 SPI：按用户 ID 查其角色 ID 列表。
	// 用于 role 候选任务的待办查询：候选任务落库的是 role 实体，须按用户的角色匹配。
	// 生产实现由上层应用（gflow）注入。
	GetRoleIDsByUserID(ctx context.Context, tenantID string, userID string) ([]string, error)

	// GetDepartmentIDsByUserID 反向 SPI：按用户 ID 查其部门 ID 列表（含非主部门）。
	// 用于 dept 候选任务的待办查询：候选任务落库的是 department 实体，须按用户的部门匹配。
	// 生产实现由上层应用（gflow）注入。
	GetDepartmentIDsByUserID(ctx context.Context, tenantID string, userID string) ([]string, error)
}

// TenantMembershipChecker IdentityService 的可选扩展接口：宿主的 IdentityService
// 实现若同时实现本方法（引擎以类型断言探测，IdentityService 本身不变），转办/委派/
// 改派的目标用户将校验属于任务租户，阻断跨租户转派。未实现时引擎无法自行判定
// （引擎不含用户目录），跳过校验并告警留痕。
type TenantMembershipChecker interface {
	// IsUserInTenant 判断用户是否属于指定租户
	IsUserInTenant(ctx context.Context, tenantID string, userID string) (bool, error)
}

// GetUserFromCtx 从 ctx 取出 bindActor 绑定的操作人（*Actor）；未绑定返回 nil。
func GetUserFromCtx(ctx context.Context) *Actor {
	userV := ctx.Value(constants.KeyCurrentUser)
	if userV == nil {
		return nil
	}
	if user, ok := userV.(*Actor); ok {
		return user
	}
	return nil
}

// SetUserToCtx 将操作人写入 ctx（key 为 constants.KeyCurrentUser）。
// 各服务的 actor 参数在入口经 bindActor 调用本方法，一般无需手动调用。
func SetUserToCtx(ctx context.Context, user *Actor) context.Context {
	return context.WithValue(ctx, constants.KeyCurrentUser, user)
}

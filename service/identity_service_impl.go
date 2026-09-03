package service

import (
	"context"
	"fmt"

	"github.com/rulego/gflow-engine/query"
)

// IdentityServiceImpl IdentityService 的 Mock 实现
//
// 此实现仅用于单元测试和本地开发调试，不可用于生产环境。
// 生产环境应由上层应用（如 gflow）通过 WorkflowEngineBuilder.SetIdentityService() 注入真实实现。
//
// 该 Mock 实现内部维护了一个基于内存的测试数据集（见 initTestData），
// 包含预设的用户、部门、角色、管理关系等数据，方便在没有数据库的环境下跑通流程。
//
// Mock 实现不加锁，仅适用于无并发访问的测试场景。
type IdentityServiceImpl struct {
	query *query.Query
	// 模拟数据存储
	users       map[string]*User
	groups      map[string]*Group
	userGroups  map[string][]string // userID -> groupIDs
	userManager map[string]string   // userID -> managerID
	userDept    map[string]string   // userID -> departmentID
	deptManager map[string]string   // departmentID -> managerID
	roleUsers   map[string][]string // roleID -> userIDs
}

// NewIdentityService 创建 IdentityService 的 Mock 实现（仅测试用）
func NewIdentityService() IdentityService {
	service := &IdentityServiceImpl{
		users:       make(map[string]*User),
		groups:      make(map[string]*Group),
		userGroups:  make(map[string][]string),
		userManager: make(map[string]string),
		userDept:    make(map[string]string),
		deptManager: make(map[string]string),
		roleUsers:   make(map[string][]string),
	}

	// 初始化测试数据
	service.initTestData()

	return service
}

// NewIdentityServiceWithQuery 使用指定的 Query 创建 IdentityService 的 Mock 实例
// （不查库，仍使用内存 Mock 数据；供引擎内部 initServices() 兜底调用）。
// 生产环境请通过 WorkflowEngineBuilder.SetIdentityService() 注入真实实现。
func NewIdentityServiceWithQuery(q *query.Query) IdentityService {
	service := &IdentityServiceImpl{
		query:       q,
		users:       make(map[string]*User),
		groups:      make(map[string]*Group),
		userGroups:  make(map[string][]string),
		userManager: make(map[string]string),
		userDept:    make(map[string]string),
		deptManager: make(map[string]string),
		roleUsers:   make(map[string][]string),
	}

	// 初始化测试数据
	service.initTestData()

	return service
}

// initTestData 初始化测试数据
// 创建一些模拟的用户、部门、角色数据用于测试
func (s *IdentityServiceImpl) initTestData() {
	// 创建用户
	users := []*User{
		{ID: "user001", FirstName: "张", LastName: "三", Email: "zhangsan@example.com", TenantID: "tenant1"},
		{ID: "user002", FirstName: "李", LastName: "四", Email: "lisi@example.com", TenantID: "tenant1"},
		{ID: "user003", FirstName: "王", LastName: "五", Email: "wangwu@example.com", TenantID: "tenant1"},
		{ID: "user004", FirstName: "赵", LastName: "六", Email: "zhaoliu@example.com", TenantID: "tenant1"},
		{ID: "user005", FirstName: "钱", LastName: "七", Email: "qianqi@example.com", TenantID: "tenant1"},
		{ID: "manager001", FirstName: "部门", LastName: "经理", Email: "manager@example.com", TenantID: "tenant1"},
		{ID: "director001", FirstName: "总", LastName: "监", Email: "director@example.com", TenantID: "tenant1"},
	}

	for _, user := range users {
		s.users[user.ID] = user
	}

	// 创建组/部门
	groups := []*Group{
		{ID: "dept001", Name: "研发部", Type: "department", TenantID: "tenant1"},
		{ID: "dept002", Name: "人事部", Type: "department", TenantID: "tenant1"},
		{ID: "role001", Name: "开发工程师", Type: "role", TenantID: "tenant1"},
		{ID: "role002", Name: "测试工程师", Type: "role", TenantID: "tenant1"},
		{ID: "role003", Name: "产品经理", Type: "role", TenantID: "tenant1"},
	}

	for _, group := range groups {
		s.groups[group.ID] = group
	}

	s.userDept["user001"] = "dept001"    // 张三 -> 研发部
	s.userDept["user002"] = "dept001"    // 李四 -> 研发部
	s.userDept["user003"] = "dept001"    // 王五 -> 研发部
	s.userDept["user004"] = "dept002"    // 赵六 -> 人事部
	s.userDept["user005"] = "dept002"    // 钱七 -> 人事部
	s.userDept["manager001"] = "dept001" // 部门经理 -> 研发部

	s.userGroups["user001"] = []string{"dept001", "role001"} // 张三 -> 研发部, 开发工程师
	s.userGroups["user002"] = []string{"dept001", "role002"} // 李四 -> 研发部, 测试工程师
	s.userGroups["user003"] = []string{"dept001", "role003"} // 王五 -> 研发部, 产品经理
	s.userGroups["user004"] = []string{"dept002"}            // 赵六 -> 人事部
	s.userGroups["user005"] = []string{"dept002"}            // 钱七 -> 人事部
	s.userGroups["manager001"] = []string{"dept001"}         // 部门经理 -> 研发部

	s.userManager["user001"] = "manager001"     // 张三的主管是部门经理
	s.userManager["user002"] = "manager001"     // 李四的主管是部门经理
	s.userManager["user003"] = "manager001"     // 王五的主管是部门经理
	s.userManager["manager001"] = "director001" // 部门经理的主管是总监

	s.deptManager["dept001"] = "manager001" // 研发部经理
	s.deptManager["dept002"] = "user004"    // 人事部经理（赵六）

	s.roleUsers["role001"] = []string{"user001"} // 开发工程师角色
	s.roleUsers["role002"] = []string{"user002"} // 测试工程师角色
	s.roleUsers["role003"] = []string{"user003"} // 产品经理角色
}

// GetUserIDsByRoleID 根据角色ID获取所有用户ID列表
func (s *IdentityServiceImpl) GetUserIDsByRoleID(ctx context.Context, tenantID string, roleID string) ([]string, error) {
	if roleID == "" {
		return nil, fmt.Errorf("role ID cannot be empty")
	}

	userIDs, exists := s.roleUsers[roleID]
	if !exists {
		return []string{}, nil
	}

	var result []string
	for _, userID := range userIDs {
		if user, exists := s.users[userID]; exists && (tenantID == "" || user.TenantID == tenantID) {
			result = append(result, userID)
		}
	}

	return result, nil
}

// GetDepartmentManagerUserID 获取部门主管用户ID
func (s *IdentityServiceImpl) GetDepartmentManagerUserID(ctx context.Context, tenantID string, departmentID string) (string, error) {
	if departmentID == "" {
		return "", fmt.Errorf("department ID cannot be empty")
	}

	managerID, exists := s.deptManager[departmentID]
	if !exists {
		return "", nil
	}

	if user, exists := s.users[managerID]; exists && (tenantID == "" || user.TenantID == tenantID) {
		return managerID, nil
	}

	return "", nil
}

// GetUserIDsByGroupID 根据组ID获取所有用户ID列表
func (s *IdentityServiceImpl) GetUserIDsByGroupID(ctx context.Context, tenantID string, groupID string) ([]string, error) {
	if groupID == "" {
		return nil, fmt.Errorf("group ID cannot be empty")
	}

	var result []string
	for userID, groupIDs := range s.userGroups {
		for _, gID := range groupIDs {
			if gID == groupID {
				if user, exists := s.users[userID]; exists && (tenantID == "" || user.TenantID == tenantID) {
					result = append(result, userID)
				}
				break
			}
		}
	}

	return result, nil
}

// GetUserIDsByDepartmentID 根据部门ID获取所有用户ID列表
func (s *IdentityServiceImpl) GetUserIDsByDepartmentID(ctx context.Context, tenantID string, departmentID string) ([]string, error) {
	if departmentID == "" {
		return nil, fmt.Errorf("department ID cannot be empty")
	}

	var result []string
	for userID, deptID := range s.userDept {
		if deptID == departmentID {
			if user, exists := s.users[userID]; exists && (tenantID == "" || user.TenantID == tenantID) {
				result = append(result, userID)
			}
		}
	}

	return result, nil
}

// GetUserManagerID 获取用户的直接主管ID
func (s *IdentityServiceImpl) GetUserManagerID(ctx context.Context, tenantID string, userID string) (string, error) {
	if userID == "" {
		return "", fmt.Errorf("user ID cannot be empty")
	}

	managerID, exists := s.userManager[userID]
	if !exists {
		return "", nil
	}

	if user, exists := s.users[managerID]; exists && (tenantID == "" || user.TenantID == tenantID) {
		return managerID, nil
	}

	return "", nil
}

// GetUserManagerHierarchy 获取用户的多级主管层级
func (s *IdentityServiceImpl) GetUserManagerHierarchy(ctx context.Context, tenantID string, userID string, levels int) ([]string, error) {
	if userID == "" {
		return nil, fmt.Errorf("user ID cannot be empty")
	}

	var result []string
	currentUserID := userID
	level := 0
	visited := make(map[string]bool)

	for {
		if levels > 0 && level >= levels {
			break
		}
		if visited[currentUserID] {
			break
		}
		visited[currentUserID] = true

		managerID, err := s.GetUserManagerID(ctx, tenantID, currentUserID)
		if err != nil {
			return nil, err
		}
		if managerID == "" {
			break
		}

		result = append(result, managerID)
		currentUserID = managerID
		level++
	}

	return result, nil
}

// GetUserDepartmentID 获取用户所属部门ID
func (s *IdentityServiceImpl) GetUserDepartmentID(ctx context.Context, tenantID string, userID string) (string, error) {
	if userID == "" {
		return "", fmt.Errorf("user ID cannot be empty")
	}

	deptID, exists := s.userDept[userID]
	if !exists {
		return "", nil
	}

	if user, exists := s.users[userID]; exists && (tenantID == "" || user.TenantID == tenantID) {
		return deptID, nil
	}

	return "", nil
}

// GetRoleIDsByUserID 反向 SPI：按用户 ID 查其角色 ID 列表（Mock 实现）。
// 由 roleUsers 映射反推，按 tenantID 过滤。
func (s *IdentityServiceImpl) GetRoleIDsByUserID(ctx context.Context, tenantID string, userID string) ([]string, error) {
	if userID == "" {
		return nil, fmt.Errorf("user ID cannot be empty")
	}
	if _, exists := s.users[userID]; !exists {
		return []string{}, nil
	}
	if tenantID != "" {
		if user, exists := s.users[userID]; !exists || user.TenantID != tenantID {
			return []string{}, nil
		}
	}
	var result []string
	for roleID, userIDs := range s.roleUsers {
		for _, uid := range userIDs {
			if uid == userID {
				result = append(result, roleID)
				break
			}
		}
	}
	return result, nil
}

// GetDepartmentIDsByUserID 反向 SPI：按用户 ID 查其部门 ID 列表（Mock 实现）。
// 由 userDept 映射反推，按 tenantID 过滤。
func (s *IdentityServiceImpl) GetDepartmentIDsByUserID(ctx context.Context, tenantID string, userID string) ([]string, error) {
	if userID == "" {
		return nil, fmt.Errorf("user ID cannot be empty")
	}
	if _, exists := s.users[userID]; !exists {
		return []string{}, nil
	}
	if tenantID != "" {
		if user, exists := s.users[userID]; !exists || user.TenantID != tenantID {
			return []string{}, nil
		}
	}
	deptID := s.userDept[userID]
	if deptID == "" {
		return []string{}, nil
	}
	return []string{deptID}, nil
}

// AddMockUser 向 Mock 数据中添加用户（仅测试用）
func (s *IdentityServiceImpl) AddMockUser(user *User) {
	if s.users == nil {
		s.users = make(map[string]*User)
	}
	s.users[user.ID] = user
}

// AddMockRoleUsers 设置角色与用户的映射关系（仅测试用）
func (s *IdentityServiceImpl) AddMockRoleUsers(roleID string, userIDs []string) {
	if s.roleUsers == nil {
		s.roleUsers = make(map[string][]string)
	}
	s.roleUsers[roleID] = userIDs
}

// AddMockUserDepartment 设置用户与部门的映射关系（仅测试用）
func (s *IdentityServiceImpl) AddMockUserDepartment(userID, departmentID string) {
	if s.userDept == nil {
		s.userDept = make(map[string]string)
	}
	s.userDept[userID] = departmentID
}

// AddMockDepartmentManager 设置部门与部门经理的映射关系（仅测试用）
func (s *IdentityServiceImpl) AddMockDepartmentManager(departmentID, managerUserID string) {
	if s.deptManager == nil {
		s.deptManager = make(map[string]string)
	}
	s.deptManager[departmentID] = managerUserID
}

// AddMockUserManager 设置用户与直接主管的映射关系（仅测试用）
func (s *IdentityServiceImpl) AddMockUserManager(userID, managerUserID string) {
	if s.userManager == nil {
		s.userManager = make(map[string]string)
	}
	s.userManager[userID] = managerUserID
}

// IsMockIdentity is a sentinel method that marks this implementation as the
// engine's built-in mock. Real IdentityService implementations should not
// define it. The engine uses a type assertion to detect when the mock is
// wired so it can warn loudly (or, in RequireIdentityService mode, fail
// Build outright).
//
// Intentionally not on the IdentityService interface — adding it there would
// force every real implementation to define a meaningless stub.
func (*IdentityServiceImpl) IsMockIdentity() bool { return true }

// isMockIdentity reports whether s is the engine's built-in mock by checking
// for the sentinel IsMockIdentity method. Real implementations do not expose
// it, so the type assertion fails for them.
func isMockIdentity(s IdentityService) bool {
	m, ok := s.(interface{ IsMockIdentity() bool })
	return ok && m.IsMockIdentity()
}

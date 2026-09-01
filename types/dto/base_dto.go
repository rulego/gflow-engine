package dto

// DefaultPageSize 全库统一的默认分页大小（page/pageSize 口径见 PageRequest）。
const DefaultPageSize = 10

// PageRequest 分页请求参数
// 全库统一 page/pageSize 口径（引擎、宿主 HTTP、前端一致）。
type PageRequest struct {
	Page     int `json:"page" form:"page"`         // 页码（1 起；<=0 时按 1 处理）
	PageSize int `json:"pageSize" form:"pageSize"` // 每页大小（<=0 时按 DefaultPageSize 处理）
	// Keyword 关键字搜索：具体匹配哪些字段由各查询实现决定
	// （如运行时实例列表对 name/business_key 模糊匹配；历史实例列表不支持）
	Keyword  string `json:"keyword" form:"keyword"`
	TenantID string `json:"tenantId"` // 租户ID（服务层通常以 actor 租户强制覆盖，无需调用方传入）
	// Status 状态过滤：取值取决于查询的实体（任务状态 / 实例状态，见 enums 包对应枚举）
	Status []string `json:"status" form:"status"`
	// 排序参数。OrderBy 传目标表字段名（如 created_at/name/priority），
	// 传空或字段名无法识别时回退 created_at，保证分页顺序确定。
	OrderBy   string `json:"orderBy" form:"orderBy"`     // 排序字段
	OrderDesc bool   `json:"orderDesc" form:"orderDesc"` // 是否降序
}

func (r *PageRequest) GetPage() int {
	if r.Page <= 0 {
		return 1
	}
	return r.Page
}

func (r *PageRequest) GetPageSize() int {
	if r.PageSize <= 0 {
		return DefaultPageSize
	}
	return r.PageSize
}

// CalculatePages 计算总页数
func CalculatePages(total int64, pageSize int) int {
	if pageSize <= 0 {
		return 0
	}
	pages := int(total) / pageSize
	if int(total)%pageSize > 0 {
		pages++
	}
	return pages
}

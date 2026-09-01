package dao

import "github.com/rulego/gflow-engine/types/dto"

// paginate 解析分页请求为 (offset, limit)。页码 1 起、<=0 按 1 处理，
// pageSize <=0 按 dto.DefaultPageSize 处理（口径见 dto.PageRequest）。
func paginate(r *dto.PageRequest) (offset, limit int) {
	size := r.GetPageSize()
	return (r.GetPage() - 1) * size, size
}

// 手工维护文件：gorm gen 只重新生成 *.gen.go 与 gen.go，不会覆盖本文件。
// 提供底层 *gorm.DB 访问入口（如 instance_lock 的显式事务控制）。
package query

import "gorm.io/gorm"

// RawDB 返回底层 *gorm.DB。
// Query 由事务包装时（Transaction/Begin）返回的是事务句柄，
// 因此通过它执行的读写天然处于调用方事务内。
func (q *Query) RawDB() *gorm.DB {
	return q.db
}

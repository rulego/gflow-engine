package dao

import "github.com/rulego/gflow-engine/query"

// Underlying 系列返回事务入口所需的 *query.Query：service 层 WithInstanceTx
// 经此取查询束开事务，以方法而非字段暴露是为了让存储窄接口的实现不必
// 背负 gorm gen 的查询束结构。
func (d *TaskDAO) Underlying() *query.Query         { return d.Query }
func (d *HiTaskDAO) Underlying() *query.Query       { return d.Query }
func (d *InstanceDAO) Underlying() *query.Query     { return d.Query }
func (d *HiInstanceDAO) Underlying() *query.Query   { return d.Query }
func (d *ProcessDAO) Underlying() *query.Query      { return d.Query }
func (d *TaskAssigneeDAO) Underlying() *query.Query { return d.Query }
func (d *TaskCommentDAO) Underlying() *query.Query  { return d.Query }

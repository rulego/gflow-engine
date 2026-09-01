package main

import (
	"fmt"
	"strings"

	"gorm.io/driver/postgres"
	"gorm.io/gen"
	"gorm.io/gen/field"
	"gorm.io/gorm"
)

// camelCaseJSONTagName 自定义JSON标签命名策略 - 驼峰命名
func camelCaseJSONTagName(columnName string) string {
	parts := strings.Split(columnName, "_")
	if len(parts) == 1 {
		return columnName
	}

	result := parts[0]
	for i := 1; i < len(parts); i++ {
		if len(parts[i]) > 0 {
			result += strings.ToUpper(parts[i][:1]) + parts[i][1:]
		}
	}
	return result
}
func main() {
	// 连接数据库
	dsn := "host=localhost user=postgres password=postgres dbname=gflow port=5432 sslmode=disable TimeZone=Asia/Shanghai"
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		panic(fmt.Errorf("failed to connect database: %w", err))
	}

	// 创建生成器
	g := gen.NewGenerator(gen.Config{
		OutPath:       "./query",
		Mode:          gen.WithoutContext | gen.WithDefaultQuery | gen.WithQueryInterface,
		FieldNullable: true, // 将数据库中可空字段生成为指针类型
	})
	// 使用WithJSONTagNameStrategy方法自定义JSON标签命名策略
	g.WithJSONTagNameStrategy(camelCaseJSONTagName)

	// 设置数据库
	g.UseDB(db)

	// 生成模型，并配置实例关联流程定义（BelongsTo）
	//
	// 手工维护字段：model/wf_instance.gen.go 在 Process 关联之后有一个手工新增的
	// 非持久化字段 CurrentActivityName（gorm:"-"，列表返回前由 RuntimeService 装配
	// 节点名）。gorm.io/gen 只按表列生成、无法声明虚拟字段，重新生成后必须手动补回。
	process := g.GenerateModel("wf_process")
	instance := g.GenerateModel(
		"wf_instance",
		gen.FieldRelate(
			field.BelongsTo,
			"Process",
			process,
			&field.RelateConfig{
				RelatePointer: true,
				GORMTag: field.GormTag{
					"foreignKey": []string{"ProcessID"},
					"references": []string{"ID"},
				},
			},
		),
	)

	// 生成其他表的查询接口
	wfTask := g.GenerateModel("wf_task")
	wfHiInstance := g.GenerateModel("wf_hi_instance")
	wfHiTask := g.GenerateModel("wf_hi_task")
	wfTaskAssignee := g.GenerateModel("wf_task_assignee")
	wfTaskComment := g.GenerateModel("wf_task_comment")

	// 应用生成
	g.ApplyBasic(
		process,
		instance,
		wfTask,
		wfHiInstance,
		wfHiTask,
		wfTaskAssignee,
		wfTaskComment,
	)

	// 执行生成
	g.Execute()
}

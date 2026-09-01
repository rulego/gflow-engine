package dao

import "strings"

// likeEscaper 转义 LIKE 模式串中的特殊字符。
var likeEscaper = strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)

// escapeLike 转义用户输入中的 LIKE 通配符（\ % _），
// 防止关键字里的 % / _ 被当作通配符造成越界匹配。
// PostgreSQL/MySQL 的 LIKE 默认以 \ 为转义字符，转义后按字面匹配；
// SQLite 不识别 \ 转义，含 \ 的关键字可能匹配失败（欠匹配，不会越界匹配）。
func escapeLike(s string) string {
	return likeEscaper.Replace(s)
}

// likeContains 构造"包含关键字"的 LIKE 模式串（已转义通配符），
// 供 name/business_key 等字段的模糊匹配统一使用。
func likeContains(kw string) string {
	return "%" + escapeLike(kw) + "%"
}

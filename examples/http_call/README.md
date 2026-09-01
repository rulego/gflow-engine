# httpCall + switch 组合示例

演示 `httpCall` 节点与 `switch` 条件分支的组合：查询外部 HTTP 接口 → 响应字段
按 `outputMappings` 合并进流程变量 → `switch` 按查询结果路由到不同分支。

```
startTask → httpCall(查询风险接口) → switch(msg.riskLevel)
                                      ├─ case "high" → 转人工（serviceTask）
                                      └─ Default     → 自动通过（serviceTask）→ end
```

示例自包含：`main()` 在本机 `:18080` 起一个模拟的风险评估服务（按 `days`
返回 `riskLevel`/`score`），再把地址注入 `dsl.json` 的 httpCall 节点后部署运行。

## 演示要点

- **变量替换**：`url` 支持 `${msg.employeeId}` / `${msg.days}` 模板；
- **输出映射**：`outputMappings` 把响应的 `riskLevel`/`score` 精确写入流程变量
  （不配置则默认全平铺，同名覆盖表单字段；非 object 响应整体写入 `reservedKey`）；
- **条件路由**：`switch` 的 `cases` 按 `msg.riskLevel == 'high'` 命中转人工分支，
  其余走 `Default` 出边——与 GFlow 设计器的条件分支同款 DSL；
- **错误处理**：状态码 ≥400 或超时（`timeoutMs`）走 `Failure` 出边。

## 运行

```bash
cd examples/http_call
go run .   # 默认内存 SQLite，零依赖，直接运行
```

连接外部数据库时才需要前置准备：建 `gflow` 库并执行 `scripts/00.init_bpm_pg.sql`
（或 `00.init_bpm_mysql.sql`，与 leave_approval 示例同一个库即可），再通过
`GFLOW_DSN` / `GFLOW_DRIVER` 切换——用法见
[leave_approval README](../leave_approval/README.md) 的「数据存储」一节。

输出：`days=2` 走自动通过分支，`days=5` 走人工分支，实例均 `completed`。

## SSRF 说明

httpCall 内置 SSRF 防护：仅允许 http/https；主机部分含 `${...}` 动态变量时拦截
回环/链路本地/云元数据地址（可配 `allowedHosts` 白名单、`blockPrivateNetworks`）。
本例 url 为静态本机地址，不做拦截。详见 [docs/components.md](../../docs/components.md)
的 httpCall 章节。

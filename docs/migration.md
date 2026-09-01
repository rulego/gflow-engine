# GFlow Engine 数据库初始化与升级指南

本文面向全新部署 GFlow Engine 的集成者，覆盖数据库初始化与后续版本的 schema 兼容策略。
Go API 层面的破坏性变更不在本文展开，见 [CHANGELOG.md](CHANGELOG.md) 的对应版本小节。

## 兼容性承诺

**自 v1.0.0 起，运行/历史表结构进入稳定口径：**

- 非破坏性变更（加表、加列、加索引、加宽列）随 minor 版本发布，全新库直接用最新建表脚本，已有库不受影响；
- **破坏性变更**（改列类型、删列、收窄列宽）只发生在 major 版本，并在 CHANGELOG 对应小节给出迁移语句。

## 全新安装

执行建表脚本，二选一按数据库选择：

```bash
# PostgreSQL
psql -d gflow -f scripts/00.init_bpm_pg.sql

# MySQL
mysql -u root -p gflow < scripts/00.init_bpm_mysql.sql
```

> ✅ 两个脚本均为幂等（`CREATE TABLE/INDEX IF NOT EXISTS`），在已有库上重跑不会
> 删除或改写任何数据，也不会升级已有表结构。表清单与字段说明见
> [persistence.md](persistence.md)。

引擎只维护自己的工作流表，用户/角色/部门等系统表由宿主应用负责。

## 未来的 schema 演进策略

- 建表脚本（两个方言）始终是全新库的最终形态：脚本幂等、从不删表、不改动已有结构；
- 引擎自身不携带迁移工具——把引擎嵌进宿主应用的团队，请将后续版本的 schema
  变更纳入自己的迁移流程，每个版本的结构变更与迁移语句见 CHANGELOG 对应小节；
- **破坏性变更**只发生在 major 版本（见上文兼容性承诺）；
- 运行表/历史表分离的口径不变：完结数据进 `wf_hi_*` 历史表，历史表结构始终
  与运行表对齐；
- **GFlow Platform**（企业版）随版本交付附带完整的迁移步骤，企业版用户无需自行处理。

升级前请核对所用版本的 CHANGELOG 与本文，不要假设跨版本的 schema 自动兼容。

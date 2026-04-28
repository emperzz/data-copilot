"""Default markdown templates for structured memory entities."""

FACTS_INDEX_TEMPLATE = """# Facts Index

> 企业事实记忆索引。每行格式: `- [实体名](相对路径) — 简介`

## Schema (数仓架构)

- [库表索引](schema/index.md) — 所有已记录的数据库表

## Business (业务逻辑)

- [业务概念索引](business/index.md) — 所有已记录的业务定义

---

*最后更新: {last_updated}*
"""

SCHEMA_INDEX_TEMPLATE = """# Schema Index

> 数仓库表索引。每行格式: `- [db.table](tables/xxx.md) — 一句话描述`

{entries}

---

*最后更新: {last_updated}*
"""

TABLE_DETAIL_TEMPLATE = """# {table_name}

## 基本信息

- **库**: {database}
- **表**: {table}
- **分层**: {layer}
- **更新频率**: {frequency}
- **描述**: {description}

## 字段

| 字段 | 类型 | 说明 |
|------|------|------|
{fields}

## 关联

- 业务定义:
- 下游表:
- 相关任务:

---

*创建: {created_at}*
"""

BUSINESS_INDEX_TEMPLATE = """# Business Index

> 业务概念索引。每行格式: `- [概念名](xxx.md) — 一句话说明`

{entries}

---

*最后更新: {last_updated}*
"""

TASKS_INDEX_TEMPLATE = """# Tasks Index

> 历史任务记忆索引（时间倒序）。每行格式: `- [YYYY-MM-DD 标题](路径) — 一句话摘要`

{entries}

---

*最后更新: {last_updated}*
"""

TASK_SUMMARY_TEMPLATE = """# {title}

- **日期**: {date}
- **类型**: {task_type}

## 任务摘要

{summary}

## 关键产出

{key_outputs}

## 相关实体

- 涉及的表:
- 相关业务:

---

*创建: {created_at}*
"""

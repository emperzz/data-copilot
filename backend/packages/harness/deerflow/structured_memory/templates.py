"""Default markdown templates for structured memory entities."""

FACTS_INDEX_TEMPLATE = """# Facts Index

> 企业事实记忆索引。每行格式: `- [实体名](相对路径) — 简介`

## Business (业务逻辑)

- [业务概念索引](business/index.md) — 所有已记录的业务定义（指标、漏斗、业务规则、栏目体系、业务模式、业务流程）

## Technical (技术规范)

- [技术规范索引](technical/index.md) — 事件命名、参数定义、上报规则

## Warehouse (数仓结构)

- [数仓结构索引](warehouse/index.md) — 表定义、字段类型（预留给未来）

---

*最后更新: {last_updated}*
"""

WAREHOUSE_INDEX_TEMPLATE = """# Warehouse Index

> 数仓结构索引。每行格式: `- [db.table](xxx.md) — 一句话描述`

{entries}

---

*最后更新: {last_updated}*
"""

TABLE_DETAIL_TEMPLATE = """# {tablename}

## Basic Info

- **database**: {database}
- **table**: {tablename}
- **update_frequency**: {update_frequency}

## Compiled Truth

- **objective**: {objective}
- **definition**: {definition}
- **core_logic**: {core_logic}

### upstream dependencies

{source_tables}

### columns

| column | description |
|--------|-------------|
{columns}

### SQL
```sql
{sql}
```

## Timeline

{timeline_entries}

---
*created: {created_at}*
*updated: {updated_at}*
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

BUSINESS_ENTITY_TEMPLATE = """# {title}

## 基本信息

{description}

## 业务背景

{background}

## 业务属性

{attributes_block}

## 核心要点

{list_attributes_block}

## 相关实体

{related_entities_block}

## Timeline

{timeline_block}

---
*created: {created}*
*updated: {updated}*
"""

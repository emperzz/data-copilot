---
name: structured-memory
description: Enterprise structured memory for data warehouse schema, business definitions, and task history — includes judgment logic for when and how to update memories
license: MIT
---

# Structured Memory

You have access to an enterprise structured memory system that stores
knowledge about the user's data warehouse, business definitions, and task
history. This memory persists across sessions and grows more valuable over time.

## When to Update Memory

### Facts Memory (`facts/`)

Create or update fact memories when the user provides:

1. **Data warehouse schema information** — table names, field definitions,
   data layers (ODS/DWD/DWS/ADS), update frequencies, partition keys
2. **Business definitions** — metrics, KPIs, calculation formulas, business
   domain concepts, revenue recognition rules
3. **Data quality rules** — validation constraints, known data issues,
   conventions
4. **Corrections** — the user says existing memory is wrong or outdated

### Task Memory (`tasks/`)

Create a task memory after completing a significant task for the user:

1. The task involved multiple steps or tools
2. The user may want to reference or repeat this work later
3. The task produced reusable outputs (queries, scripts, reports)

Do NOT create task memory for trivial one-shot questions or clarifications.

## How to Update Memory

### Adding a new fact entity

1. Check the appropriate index file (`facts/schema/index.md` or
   `facts/business/index.md`) via `get_memory_entity`
2. Write the entity detail file via `create_memory_entity` using the template below.
   The index entry is updated automatically — no separate `update_memory_index` call needed.

### Adding a new task memory

1. Write the task summary via `create_memory_entity` under `tasks/YYYY/task-title.md`.
   The index entry is updated automatically.

### Updating existing memory

1. Read the current file via `get_memory_entity`
2. Edit the content and write back via `update_memory_entity`.
   The old index entry is removed and the new one is added automatically.

**For table entities** (`facts/schema/tables/`), use partial updates to avoid
replacing the entire file:

1. Read the current content via `get_memory_entity`
2. Identify **only the specific fields** that need changing in 基本信息 and
   Compiled Truth. Do NOT modify fields that haven't changed.
3. Provide the changed fields via the `changes` parameter as a JSON dict
   (e.g. `{"definition": "new definition", "columns": [...]}`)
4. Describe what changed via the `timeline_desc` parameter
5. `update_memory_entity` will preserve all unchanged fields, append a new timeline entry,
   and update the `*更新:*` timestamp automatically

**Timeline is APPEND-ONLY** — never remove or edit existing timeline entries.
They serve as an audit log of all changes.

### Required and Uncertain Fields

- **tablename** is REQUIRED and must never be empty
- If the user does not provide a value for a required field (e.g., objective,
  definition, update_frequency), ask the user for clarification — do NOT
  default to empty strings or made-up values
- For optional fields (e.g., sql, memory links), leave empty or omit when unknown

### Deleting memory

1. Call `delete_memory_entity` with the entity path.
   The corresponding index entry is removed automatically.

## Entity File Templates

### Table detail (`facts/schema/tables/{table_name}.md`)

Table entities use a 3-layer structure: 基本信息 (identifying attributes), Compiled Truth (curated definitive information), and Timeline (append-only audit log).

```markdown
# 基本信息

- **库**: {database}
- **表**: {tablename}
- **更新频率**: {update_frequency}   (daily/hourly/weekly/monthly/yearly/realtime/onetime)

## Compiled Truth

- **目的**: {objective}  — 表的目的：原始业务表/维度表/聚合宽表等
- **定义**: {definition}  — 从哪里获取数据，如何计算，包含哪些字段，关键节点
- **核心逻辑**: {core_logic}  — SQL处理后的核心逻辑摘要

### 上游依赖

- db.source_table1  — 简要说明
- db.source_table2  — 简要说明
  - [链接](tables/source_table2.md)  (如果该上游表有对应记忆)

### 字段

| 字段 | 说明 |
|------|------|
| field1 | 详细描述，包括定义模糊之处和特定计算条件 |
| field2 | 详细描述 |

### SQL
```sql
{original_sql_or_empty}
```

## Timeline

- **2026-04-30 14:30:00** — 首次写入
- **2026-05-01 09:00:00** — 更新了字段定义和上游依赖

---
*创建: 2026-04-30 14:30:00*
*更新: 2026-05-01 09:00:00*
```

### Business definition (`facts/business/{name}.md`)

```markdown
# {concept_name}

## 定义
{definition}

## 计算逻辑
{formula}

## 相关表
- [link](../schema/tables/xxx.md)

## 相关任务
- [link](../../../tasks/YYYY/task.md)
```

### Task summary (`tasks/{YYYY}/{title-slug}.md`)

```markdown
# {title}

- 日期: {YYYY-MM-DD}
- 类型: {query|analysis|development|troubleshooting}

## 背景
{why this task was done}

## 关键产出
{queries, scripts, reports, findings}

## 涉及实体
- 表: [link](../../facts/schema/tables/xxx.md)
- 业务: [link](../../facts/business/xxx.md)
```

## Index Format Convention

All index files use this format:

```markdown
# {Category} Index

> Description

## {Subcategory}

- [Entity Name](relative/path.md) — One-line description
```

## Tools Reference

| Tool | Purpose |
|------|---------|
| `search_structured_memory` | Search memory files by keyword |
| `get_memory_entity` | Read full content of an entity file |
| `list_memory_entities` | Browse the memory directory tree |
| `create_memory_entity` | Create a new entity file with `content`; index auto-updated |
| `update_memory_entity` | Partially update (with `changes` + `timeline_desc`) an existing entity file; index auto-updated |
| `delete_memory_entity` | Delete an entity file (index auto-updated) |
| `update_memory_index` | Manually add/remove/update index entries (rarely needed; write/delete handle this automatically) |

## Progressive Loading

1. Check the injected index in `<structured_memory>` for relevant entities
2. Use `search_structured_memory` to find information by keyword
3. Use `get_memory_entity` to load full details when needed
4. Use `list_memory_entities` to browse when exploring

## Classification Guide

| Information Type | Goes To |
|-----------------|---------|
| Table structure, fields, partitions | `facts/schema/tables/` |
| Cross-table field definitions | `facts/schema/fields/` |
| Metric formulas, business rules | `facts/business/` |
| Completed analysis/query tasks | `tasks/{YYYY}/` |
| User preferences | Personal memory system (not structured memory) |
| Temporary conversation context | Do NOT store |

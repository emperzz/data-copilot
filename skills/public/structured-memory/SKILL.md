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
2. Write the entity detail file via `write_file` using the template below
3. Add the entry to the index via `update_memory_index` with action="add"

### Adding a new task memory

1. Write the task summary via `write_file` under `tasks/YYYY/task-title.md`
2. Add the entry to `tasks/index.md` via `update_memory_index` with action="add"

### Updating existing memory

1. Read the current file via `get_memory_entity`
2. Edit via `str_replace` or `write_file`
3. If the entity name or description changes, update the index via
   `update_memory_index` with action="update"

## Entity File Templates

### Table detail (`facts/schema/tables/{table_name}.md`)

```markdown
# {db}.{table} ({description})

## 基本信息
- 库: {database}
- 表: {table}
- 分层: {layer}
- 更新频率: {frequency}
- 主键: {pk}

## 字段
| 字段 | 类型 | 说明 |
|------|------|------|
| ... | ... | ... |

## 关联
- 业务定义: [link](../business/xxx.md)
- 下游表: [link](other_table.md)
- 相关任务: [link](../../../tasks/YYYY/task.md)
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

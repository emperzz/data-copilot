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
2. Identify **only the specific fields** that need changing in Basic Info and
   Compiled Truth. Do NOT modify fields that haven't changed.
3. Provide the changed fields via the `changes` parameter as a JSON dict
   (e.g. `{"definition": "new definition", "columns": [...]}`)
4. Describe what changed via the `timeline_desc` parameter
5. `update_memory_entity` will preserve all unchanged fields, append a new timeline entry,
   and update the `*updated: ...*` timestamp automatically

**Timeline is APPEND-ONLY** — never remove or edit existing timeline entries.
They serve as an audit log of all changes.

### Information Source Rule

**All memory content must come from the user — never guess or assume.**

1. **Must be user-provided**: Every fact, definition, metric, or rule stored in memory must be explicitly stated by the user. Do not infer, deduce, or fill in gaps with plausible but unconfirmed content.
2. **Uncertain → confirm first**: If you are unsure about any field value, ask the user for clarification before writing to memory. Do not write "待确认", "unknown", or any placeholder — wait until you have a confirmed answer.
   - **For schema entities** (`facts/schema/tables/`, `facts/schema/fields/`): this is especially critical. Column meanings, table purposes, update frequencies, upstream dependencies — if the user has not stated them explicitly, ask. Only leave a field blank if the user explicitly says they also do not know.
3. **Corrections are evidence**: When the user corrects existing memory, that correction is treated as a confirmed fact. Update the entity and append a timeline entry describing the correction.

This rule takes priority over all other memory guidelines. Memory that is invented rather than confirmed is worse than no memory at all.

**System-level constraints**:
- `tablename` (for table entities) is **required** — if the user does not provide it, ask before creating the entity
- For optional fields (e.g., sql, memory links), leave empty or omit when unknown

### Deleting memory

1. Call `delete_memory_entity` with the entity path.
   The corresponding index entry is removed automatically.

### Error Handling

**Never use `write_file` to write directly into the structured memory directory.**

If `update_memory_entity` fails:
- `"not found"` → Use `create_memory_entity` to create the entity first, then retry
- `"changes parameter is required"` → Provide a valid `changes` JSON dict
- Other errors → Investigate the cause; do NOT bypass the tool by writing the file manually

If `create_memory_entity` fails:
- `"already exists"` → Entity already exists; use `update_memory_entity` instead
- Other errors → Investigate the cause; do NOT write the file manually

Writing directly with `write_file` bypasses the structured memory tool chain, skips index auto-update, and breaks the audit timeline. Always use the dedicated tools.

## Entity File Templates

### Table detail (`facts/schema/tables/{table_name}.md`)

Table entities use a 3-layer structure: Basic Info (identifying attributes), Compiled Truth (curated definitive information), and Timeline (append-only audit log).

```markdown
# {tablename}

## Basic Info

- **database**: {database}
- **table**: {tablename}
- **update_frequency**: {update_frequency}   (daily/hourly/weekly/monthly/yearly/realtime/onetime)

## Compiled Truth

- **objective**: {objective}  — purpose of the table: raw business table/dimension table/aggregate wide table/etc.
- **definition**: {definition}  — where data comes from, how it is computed, what fields are included
- **core_logic**: {core_logic}  — core logic summary after SQL processing

### upstream dependencies

- db.source_table1  — brief description
- db.source_table2  — brief description
  - [link](tables/source_table2.md)  (if the upstream table has a memory entry)

### columns

| column | description |
|--------|-------------|
| field1 | detailed description, including ambiguous definitions and specific calculation conditions |
| field2 | detailed description |

### SQL
```sql
{original_sql_or_empty}
```

## Timeline

- **2026-04-30 14:30:00** — initial write
- **2026-05-01 09:00:00** — updated field definitions and upstream dependencies

---
*created: 2026-04-30 14:30:00*
*updated: 2026-05-01 09:00:00*
```

### Business definition (`facts/business/{name}.md`)

Business entities store metric formulas, business rules, and cross-cutting concepts. Use the template below:

```markdown
# {title}

## 基本信息

{description}

## 业务背景

{background}

## 业务属性

| attribute | value | description |
|-----------|-------|-------------|
| attr1 | value1 | description1 |
| attr2 | value2 | description2 |

## 核心要点

### {section_name}

- item1
- item2

## 相关实体

- [link](./other_entity.md)  (if related entity exists in memory)

## Timeline

- **2026-04-30 14:30:00** — 首次写入

---
*created: 2026-04-30 14:30:00*
*updated: 2026-04-30 14:30:00*
```

### Task summary (`tasks/{YYYY}/{title-slug}.md`)

```markdown
# {title}

- **date**: {YYYY-MM-DD}
- **type**: {task_type}   (query/analysis/development/troubleshooting)

## 任务摘要

{summary}

## 关键产出

{key_outputs}

## 相关实体

- 涉及的表:
- 相关业务:

---
*创建: {created_at}*
```

## Index Format Convention

Index files are auto-generated by `create_memory_entity` / `update_memory_entity` / `delete_memory_entity`. The entry format depends on entity type:

| Entity Path | Index Entry Format |
|-------------|-------------------|
| `facts/schema/tables/X.md` | `- [db.table](tables/X.md) — description` |
| `facts/schema/fields/X.md` | `- [title](fields/X.md) — description` |
| `facts/business/X.md` | `- [title](X.md) — description` |
| `tasks/YYYY/X.md` | `- [title](YYYY/X.md) — description` |

Each index also has a `*最后更新: {last_updated}*` footer. Do not edit index files manually — they are updated automatically by the tool chain.

## Tools Reference

| Tool | Purpose |
|------|---------|
| `search_structured_memory` | Search memory files by keyword |
| `get_memory_entity` | Read full content of an entity file |
| `list_memory_entities` | Browse the memory directory tree |
| `create_memory_entity` | Create a new entity file with `content`; index auto-updated |
| `update_memory_entity` | Partially update (with `changes` + `timeline_desc`) an existing entity file; index auto-updated |
| `delete_memory_entity` | Delete an entity file (index auto-updated) |
| `update_memory_index` | Manually add/remove/update index entries (handled automatically by create/update/delete; rarely needed manually) |

## Progressive Loading

1. Check the summary in `<structured_memory>` (only `core.md` content is injected into prompts)
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

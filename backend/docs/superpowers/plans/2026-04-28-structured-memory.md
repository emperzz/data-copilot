# Enterprise Structured Memory — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an enterprise structured memory system for data warehouse metadata, business definitions, and task history — independent from the existing personal memory system.

**Architecture:** New `StructuredMemoryConfig` → `deerflow.structured_memory` storage module → 4 built-in tools registered in `get_available_tools()` → `{structured_memory_context}` injected into the system prompt → one skill defining judgment logic.

**Tech Stack:** Python 3.12+, Pydantic, LangChain BaseTool, ruff formatting (240 char line length), double quotes, space indentation.

**Design doc:** `docs/structured-memory-design.md`

---

## File Map

| Action | File | Responsibility |
|--------|------|---------------|
| Create | `packages/harness/deerflow/config/structured_memory_config.py` | Config model + singleton |
| Modify | `packages/harness/deerflow/config/app_config.py` | Register new config field + loader |
| Create | `packages/harness/deerflow/structured_memory/__init__.py` | Public API exports |
| Create | `packages/harness/deerflow/structured_memory/storage.py` | File I/O, path resolution, directory init |
| Create | `packages/harness/deerflow/structured_memory/search.py` | Text search across memory files |
| Create | `packages/harness/deerflow/structured_memory/templates.py` | Default markdown templates for new entities |
| Create | `packages/harness/deerflow/tools/builtins/structured_memory_tools.py` | 4 LangChain tools |
| Modify | `packages/harness/deerflow/tools/tools.py` | Register tools in `get_available_tools()` |
| Modify | `packages/harness/deerflow/agents/lead_agent/prompt.py` | Add `{structured_memory_context}` injection |
| Create | `skills/public/structured-memory/SKILL.md` | Skill for judgment/classification logic |
| Modify | `config.example.yaml` | Add `structured_memory` config section |
| Create | `tests/test_structured_memory.py` | Full test suite |

---

### Task 1: StructuredMemoryConfig

**Files:**
- Create: `packages/harness/deerflow/config/structured_memory_config.py`
- Modify: `packages/harness/deerflow/config/app_config.py`
- Modify: `config.example.yaml`

- [ ] **Step 1: Write StructuredMemoryConfig model**

Create `packages/harness/deerflow/config/structured_memory_config.py`:

```python
"""Configuration for enterprise structured memory."""

from pydantic import BaseModel, Field


class StructuredMemoryConfig(BaseModel):
    """Configuration for enterprise structured memory mechanism."""

    enabled: bool = Field(
        default=False,
        description="Whether to enable structured memory",
    )
    storage_path: str = Field(
        default="",
        description=(
            "Path to store structured memory data. "
            "If empty, defaults to `{base_dir}/structured_memory/`."
        ),
    )
    injection_enabled: bool = Field(
        default=True,
        description="Whether to inject structured memory index into system prompt",
    )
    max_index_tokens: int = Field(
        default=1500,
        ge=100,
        le=8000,
        description="Maximum tokens for structured memory index injection",
    )


# Global singleton
_structured_memory_config: StructuredMemoryConfig = StructuredMemoryConfig()


def get_structured_memory_config() -> StructuredMemoryConfig:
    """Get the current structured memory configuration."""
    return _structured_memory_config


def set_structured_memory_config(config: StructuredMemoryConfig) -> None:
    """Set the structured memory configuration (for testing)."""
    global _structured_memory_config
    _structured_memory_config = config


def load_structured_memory_config_from_dict(config_dict: dict) -> None:
    """Load structured memory configuration from a dictionary."""
    global _structured_memory_config
    _structured_memory_config = StructuredMemoryConfig(**config_dict)
```

- [ ] **Step 2: Register in AppConfig**

Modify `packages/harness/deerflow/config/app_config.py`:

Add import (after the memory_config import on line 16):
```python
from deerflow.config.structured_memory_config import StructuredMemoryConfig, load_structured_memory_config_from_dict
```

Add field to `AppConfig` class (after the `memory` field on line 63):
```python
structured_memory: StructuredMemoryConfig = Field(default_factory=StructuredMemoryConfig, description="Enterprise structured memory configuration")
```

Add loader call in `from_file()` (after the memory config block on line 128):
```python
# Load structured memory config if present
if "structured_memory" in config_data:
    load_structured_memory_config_from_dict(config_data["structured_memory"])
```

- [ ] **Step 3: Add config section to config.example.yaml**

Add to `config.example.yaml` (after the `memory:` section):

```yaml
# ── Structured Memory ─────────────────────────────────────────────────────
# Enterprise structured memory for data warehouse metadata, business
# definitions, and task history. Markdown files stored on disk, index
# injected into the system prompt, details loaded on demand via tools.
structured_memory:
  enabled: false
  storage_path: ""
  injection_enabled: true
  max_index_tokens: 1500
```

- [ ] **Step 4: Commit**

```bash
git add packages/harness/deerflow/config/structured_memory_config.py packages/harness/deerflow/config/app_config.py config.example.yaml
git commit -m "feat(structured-memory): add StructuredMemoryConfig model and registration"
```

---

### Task 2: Storage layer

**Files:**
- Create: `packages/harness/deerflow/structured_memory/__init__.py`
- Create: `packages/harness/deerflow/structured_memory/storage.py`
- Create: `packages/harness/deerflow/structured_memory/search.py`
- Create: `packages/harness/deerflow/structured_memory/templates.py`

- [ ] **Step 1: Create `__init__.py`**

Create `packages/harness/deerflow/structured_memory/__init__.py`:

```python
"""Enterprise structured memory for data warehouse knowledge."""

from deerflow.structured_memory.storage import (
    StructuredMemoryStore,
    get_structured_memory_store,
)
from deerflow.structured_memory.search import search_memory_files
from deerflow.structured_memory.templates import (
    FACTS_INDEX_TEMPLATE,
    TASKS_INDEX_TEMPLATE,
    TABLE_DETAIL_TEMPLATE,
    TASK_SUMMARY_TEMPLATE,
)

__all__ = [
    "FACTS_INDEX_TEMPLATE",
    "TASKS_INDEX_TEMPLATE",
    "TABLE_DETAIL_TEMPLATE",
    "TASK_SUMMARY_TEMPLATE",
    "StructuredMemoryStore",
    "get_structured_memory_store",
    "search_memory_files",
]
```

- [ ] **Step 2: Write storage module**

Create `packages/harness/deerflow/structured_memory/storage.py`:

```python
"""File-based storage for enterprise structured memory."""

import logging
import os
from pathlib import Path

from deerflow.config.paths import get_paths
from deerflow.config.structured_memory_config import get_structured_memory_config

logger = logging.getLogger(__name__)

MEMORY_ROOT_DIRNAME = "structured_memory"


def _resolve_storage_root() -> Path:
    """Resolve the root directory for structured memory files."""
    config = get_structured_memory_config()
    if config.storage_path:
        p = Path(config.storage_path)
        if not p.is_absolute():
            p = get_paths().base_dir / p
        return p
    return get_paths().base_dir / MEMORY_ROOT_DIRNAME


class StructuredMemoryStore:
    """Manages file I/O for the structured memory directory tree."""

    def __init__(self) -> None:
        self._root: Path | None = None

    @property
    def root(self) -> Path:
        if self._root is None:
            self._root = _resolve_storage_root()
        return self._root

    def ensure_directories(self) -> None:
        """Create the base directory structure if it doesn't exist."""
        dirs = [
            self.root,
            self.root / "facts" / "schema" / "tables",
            self.root / "facts" / "schema" / "fields",
            self.root / "facts" / "business",
            self.root / "tasks",
        ]
        for d in dirs:
            d.mkdir(parents=True, exist_ok=True)

    def resolve_path(self, relative_path: str) -> Path:
        """Resolve a relative path within the memory root.

        Rejects paths containing '..' to prevent traversal.
        """
        normalized = os.path.normpath(relative_path)
        if ".." in normalized.split(os.sep):
            raise ValueError(f"Path traversal rejected: {relative_path!r}")
        return (self.root / normalized).resolve()

    def read_file(self, relative_path: str) -> str:
        """Read a memory file by relative path."""
        target = self.resolve_path(relative_path)
        if not target.exists():
            raise FileNotFoundError(f"Memory file not found: {relative_path}")
        return target.read_text(encoding="utf-8")

    def write_file(self, relative_path: str, content: str) -> None:
        """Write (create or overwrite) a memory file."""
        target = self.resolve_path(relative_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")

    def list_dir(self, relative_path: str = "", depth: int = 2) -> str:
        """List directory contents in tree format.

        Args:
            relative_path: Relative path within memory root.
            depth: Maximum depth to traverse.

        Returns:
            Tree-formatted string of directory contents.
        """
        import os as _os

        target = self.resolve_path(relative_path) if relative_path else self.root
        if not target.exists():
            raise FileNotFoundError(f"Directory not found: {relative_path}")

        lines: list[str] = []
        prefix = str(target)

        for dirpath, dirnames, filenames in _os.walk(target):
            current_depth = len(Path(dirpath).relative_to(target).parts)
            if current_depth >= depth:
                dirnames.clear()

            indent = "  " * current_depth
            folder_name = Path(dirpath).name if current_depth > 0 else MEMORY_ROOT_DIRNAME
            if current_depth == 0:
                lines.append(folder_name + "/")
            else:
                lines.append(f"{indent}{folder_name}/")

            for fname in sorted(filenames):
                lines.append(f"{indent}  {fname}")

        return "\n".join(lines)

    def file_exists(self, relative_path: str) -> bool:
        """Check whether a memory file exists."""
        return self.resolve_path(relative_path).exists()


# Global singleton
_store: StructuredMemoryStore | None = None


def get_structured_memory_store() -> StructuredMemoryStore:
    """Return the global StructuredMemoryStore singleton."""
    global _store
    if _store is None:
        _store = StructuredMemoryStore()
    return _store
```

- [ ] **Step 3: Write search module**

Create `packages/harness/deerflow/structured_memory/search.py`:

```python
"""Text search across structured memory markdown files."""

import logging
from pathlib import Path

from deerflow.structured_memory.storage import get_structured_memory_store

logger = logging.getLogger(__name__)

SEARCHABLE_EXTENSIONS = {".md"}


def search_memory_files(query: str, category: str = "all") -> str:
    """Search memory files for a query string.

    Args:
        query: The search term(s). Simple case-insensitive substring match.
        category: 'facts', 'tasks', or 'all'.

    Returns:
        Formatted search results with file paths and matching line snippets.
    """
    store = get_structured_memory_store()

    search_roots: list[Path] = []
    if category in ("facts", "all"):
        facts_root = store.resolve_path("facts")
        if facts_root.exists():
            search_roots.append(facts_root)
    if category in ("tasks", "all"):
        tasks_root = store.resolve_path("tasks")
        if tasks_root.exists():
            search_roots.append(tasks_root)

    if not search_roots:
        return "No memory files found. The structured memory store is empty."

    query_lower = query.lower()
    results: list[str] = []
    max_results = 20

    for root in search_roots:
        for file_path in root.rglob("*"):
            if file_path.suffix not in SEARCHABLE_EXTENSIONS:
                continue
            if file_path.name.startswith("."):
                continue

            try:
                content = file_path.read_text(encoding="utf-8")
            except Exception:
                continue

            if query_lower in content.lower():
                rel_path = file_path.relative_to(store.root)
                lines = content.split("\n")
                matching_lines: list[str] = []
                for i, line in enumerate(lines):
                    if query_lower in line.lower():
                        snippet = line.strip()[:120]
                        matching_lines.append(f"  L{i+1}: {snippet}")
                        if len(matching_lines) >= 3:
                            break

                results.append(f"**{rel_path}** ({len(matching_lines)} matches)")
                results.extend(matching_lines)

                if len(results) >= max_results * 4:
                    break

        if len(results) >= max_results * 4:
            break

    if not results:
        return f"No matches found for '{query}'."

    header = f"Search results for '{query}' (category={category}):\n"
    return header + "\n".join(results[:max_results * 4])
```

- [ ] **Step 4: Write templates module**

Create `packages/harness/deerflow/structured_memory/templates.py`:

```python
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
```

- [ ] **Step 5: Commit**

```bash
git add packages/harness/deerflow/structured_memory/
git commit -m "feat(structured-memory): add storage, search, and templates module"
```

---

### Task 3: Built-in tools

**Files:**
- Create: `packages/harness/deerflow/tools/builtins/structured_memory_tools.py`
- Modify: `packages/harness/deerflow/tools/tools.py`

- [ ] **Step 1: Write the 4 tools**

Create `packages/harness/deerflow/tools/builtins/structured_memory_tools.py`:

```python
"""Built-in tools for enterprise structured memory management."""

import logging
from typing import Literal

from langchain.tools import tool

from deerflow.structured_memory import (
    StructuredMemoryStore,
    get_structured_memory_store,
    search_memory_files,
)

logger = logging.getLogger(__name__)


def _get_store() -> StructuredMemoryStore:
    store = get_structured_memory_store()
    store.ensure_directories()
    return store


@tool("search_structured_memory", parse_docstring=True)
def search_structured_memory(
    query: str,
    category: Literal["facts", "tasks", "all"] = "all",
) -> str:
    """Search structured memory files for keywords or text.

    Use this tool to find relevant enterprise knowledge before answering
    questions about data warehouse tables, business definitions, or past tasks.

    Args:
        query: The search term(s) to look for. Simple case-insensitive substring match.
        category: Which memory category to search. 'facts' for schema/business,
            'tasks' for task history, 'all' for everything.

    Returns:
        Formatted search results with file paths and matching line snippets.
    """
    try:
        return search_memory_files(query, category)
    except Exception as e:
        logger.exception("search_structured_memory failed")
        return f"Search failed: {e}"


@tool("get_memory_entity", parse_docstring=True)
def get_memory_entity(
    path: str,
) -> str:
    """Read the full content of a structured memory entity file.

    Use this to load detailed information about a specific table, business
    concept, or task after locating it via search or index browsing.

    Args:
        path: Relative path to the memory file, e.g.
            'facts/schema/tables/ods_order.md' or 'tasks/2026/task-2026-04-01.md'.

    Returns:
        The full markdown content of the memory file.
    """
    try:
        store = _get_store()
        return store.read_file(path)
    except FileNotFoundError:
        return f"Memory entity not found: {path}"
    except Exception as e:
        logger.exception("get_memory_entity failed")
        return f"Failed to read memory entity: {e}"


@tool("list_memory_entities", parse_docstring=True)
def list_memory_entities(
    path: str = "",
    depth: int = 2,
) -> str:
    """Browse the structured memory directory to see what entities exist.

    Use this tool to explore the memory structure, find available entities,
    or check the index files.

    Args:
        path: Relative path within the memory store. Empty string = root level.
        depth: How many directory levels to display (default 2).

    Returns:
        Tree-formatted directory listing.
    """
    try:
        store = _get_store()
        store.ensure_directories()
        return store.list_dir(path, depth)
    except Exception as e:
        logger.exception("list_memory_entities failed")
        return f"Failed to list memory entities: {e}"


@tool("update_memory_index", parse_docstring=True)
def update_memory_index(
    index_path: str,
    action: Literal["add", "remove", "update"],
    entry: str,
    target: str = "",
) -> str:
    """Safely update an index.md file in the structured memory store.

    Use this tool to add, remove, or update entries in index files.
    This ensures index format consistency. For writing full entity detail
    files, use the write_file tool directly.

    Args:
        index_path: Relative path to the index file, e.g. 'facts/schema/index.md'.
        action: 'add' to append a new entry, 'remove' to delete an entry by
            matching the target string, 'update' to replace an entry matching
            the target string with the new entry.
        entry: The markdown list item entry, e.g.
            '- [ods.order](tables/ods_order.md) — 订单明细表'
        target: For 'remove' and 'update', the exact line to find and replace/remove.

    Returns:
        Confirmation message describing what was changed.
    """
    try:
        store = _get_store()

        if not store.file_exists(index_path):
            if action == "add":
                store.write_file(index_path, f"# Index\n\n{entry}\n")
                return f"Created {index_path} with entry: {entry}"
            return f"Index file not found: {index_path}"

        content = store.read_file(index_path)
        lines = content.rstrip("\n").split("\n")

        if action == "add":
            if entry.strip() in content:
                return f"Entry already exists in {index_path}: {entry}"
            new_content = content.rstrip("\n") + "\n" + entry + "\n"
            store.write_file(index_path, new_content)
            return f"Added to {index_path}: {entry}"

        elif action == "remove":
            if not target:
                return "The 'target' parameter is required for 'remove' action."
            new_lines = [l for l in lines if target.strip() not in l]
            if len(new_lines) == len(lines):
                return f"Target not found in {index_path}: {target}"
            store.write_file(index_path, "\n".join(new_lines) + "\n")
            return f"Removed from {index_path}: {target}"

        elif action == "update":
            if not target:
                return "The 'target' parameter is required for 'update' action."
            found = False
            for i, line in enumerate(lines):
                if target.strip() in line:
                    lines[i] = entry
                    found = True
                    break
            if not found:
                return f"Target not found in {index_path}: {target}"
            store.write_file(index_path, "\n".join(lines) + "\n")
            return f"Updated {index_path}: replaced '{target.strip()}' with '{entry}'"

        return f"Unknown action: {action}"

    except Exception as e:
        logger.exception("update_memory_index failed")
        return f"Failed to update index: {e}"
```

- [ ] **Step 2: Register tools in get_available_tools()**

Modify `packages/harness/deerflow/tools/tools.py`:

In `get_available_tools()`, add a new conditional block after the `skill_evolution` block (after line 85 in the current code). Add after `builtin_tools.append(skill_manage_tool)`:

```python
# Add structured memory tools if enabled
structured_memory_config = getattr(config, "structured_memory", None)
if getattr(structured_memory_config, "enabled", False):
    from deerflow.tools.builtins.structured_memory_tools import (
        get_memory_entity,
        list_memory_entities,
        search_structured_memory,
        update_memory_index,
    )

    builtin_tools.extend([
        search_structured_memory,
        get_memory_entity,
        list_memory_entities,
        update_memory_index,
    ])
    logger.info("Including structured memory tools")
```

- [ ] **Step 3: Commit**

```bash
git add packages/harness/deerflow/tools/builtins/structured_memory_tools.py packages/harness/deerflow/tools/tools.py
git commit -m "feat(structured-memory): add 4 built-in tools for structured memory management"
```

---

### Task 4: Prompt injection

**Files:**
- Modify: `packages/harness/deerflow/agents/lead_agent/prompt.py`

- [ ] **Step 1: Add `{structured_memory_context}` to SYSTEM_PROMPT_TEMPLATE**

In the `SYSTEM_PROMPT_TEMPLATE` string, change the line after `{memory_context}` from:

```python
	{soul}
	{memory_context}
	
	<thinking_style>
```

To:

```python
	{soul}
	{memory_context}
	{structured_memory_context}
	
	<thinking_style>
```

- [ ] **Step 2: Add `_get_structured_memory_context()` function**

Add after the `_get_memory_context()` function (after line 568):

```python
def _get_structured_memory_context() -> str:
    """Get structured memory index for injection into system prompt.

    Injects only the top-level indexes (facts/index.md and tasks/index.md),
    not the full entity details. Agent loads details on demand via tools.

    Returns:
        Formatted structured memory context wrapped in XML tags, or empty string.
    """
    try:
        from deerflow.config.structured_memory_config import get_structured_memory_config
        from deerflow.structured_memory.storage import get_structured_memory_store

        config = get_structured_memory_config()
        if not config.enabled or not config.injection_enabled:
            return ""

        store = get_structured_memory_store()
        store.ensure_directories()

        sections: list[str] = []

        # Read facts index
        try:
            facts_index = store.read_file("facts/index.md")
            # Truncate to a reasonable size
            max_chars = config.max_index_tokens * 3  # rough char estimate
            if len(facts_index) > max_chars:
                facts_index = facts_index[:max_chars] + "\n\n... (truncated, use tools for full index)"
            sections.append(facts_index)
        except FileNotFoundError:
            sections.append("(No facts memory yet. Use `update_memory_index` and `write_file` to add.)")

        # Read tasks index
        try:
            tasks_index = store.read_file("tasks/index.md")
            max_chars = config.max_index_tokens * 2
            if len(tasks_index) > max_chars:
                tasks_index = tasks_index[:max_chars] + "\n\n... (truncated, use tools for full index)"
            sections.append(tasks_index)
        except FileNotFoundError:
            sections.append("(No task memory yet.)")

        content = "\n\n".join(sections)
        if not content.strip():
            return ""

        return f"""<structured_memory>
	Below is the enterprise structured memory index. This contains data warehouse
	metadata, business definitions, and task history the user has provided.
	Only the index is shown here — use `get_memory_entity` to load detailed
	entity files, `search_structured_memory` to find information by keyword,
	`list_memory_entities` to browse the directory, and `update_memory_index`
	to maintain index files.

	{content}
	</structured_memory>
	"""
    except Exception as e:
        logger.error("Failed to load structured memory context: %s", e)
        return ""
```

- [ ] **Step 3: Wire into `apply_prompt_template()`**

In `apply_prompt_template()`, add after the `memory_context = _get_memory_context(agent_name)` line:

```python
# Get structured memory context
structured_memory_context = _get_structured_memory_context()
```

Add `structured_memory_context=structured_memory_context` to the `SYSTEM_PROMPT_TEMPLATE.format()` call.

- [ ] **Step 4: Commit**

```bash
git add packages/harness/deerflow/agents/lead_agent/prompt.py
git commit -m "feat(structured-memory): inject structured memory index into system prompt"
```

---

### Task 5: Skill

**Files:**
- Create: `skills/public/structured-memory/SKILL.md`

- [ ] **Step 1: Write the skill**

Create `skills/public/structured-memory/SKILL.md`:

```markdown
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
```

- [ ] **Step 2: Commit**

```bash
git add skills/public/structured-memory/SKILL.md
git commit -m "feat(structured-memory): add structured-memory skill with judgment logic"
```

---

### Task 6: Tests

**Files:**
- Create: `tests/test_structured_memory.py`

- [ ] **Step 1: Write the test suite**

Create `tests/test_structured_memory.py`:

```python
"""Tests for enterprise structured memory system."""

import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from deerflow.config.structured_memory_config import (
    StructuredMemoryConfig,
    get_structured_memory_config,
    load_structured_memory_config_from_dict,
    set_structured_memory_config,
)
from deerflow.structured_memory.search import search_memory_files
from deerflow.structured_memory.storage import StructuredMemoryStore
from deerflow.structured_memory.templates import (
    FACTS_INDEX_TEMPLATE,
    TABLE_DETAIL_TEMPLATE,
    TASKS_INDEX_TEMPLATE,
    TASK_SUMMARY_TEMPLATE,
)


class TestStructuredMemoryConfig:
    """Tests for StructuredMemoryConfig model and singleton."""

    def test_default_config_is_disabled(self):
        config = StructuredMemoryConfig()
        assert config.enabled is False
        assert config.injection_enabled is True
        assert config.max_index_tokens == 1500
        assert config.storage_path == ""

    def test_load_config_from_dict(self):
        load_structured_memory_config_from_dict({
            "enabled": True,
            "storage_path": "/custom/path",
            "injection_enabled": False,
            "max_index_tokens": 500,
        })
        config = get_structured_memory_config()
        assert config.enabled is True
        assert config.storage_path == "/custom/path"
        assert config.injection_enabled is False
        assert config.max_index_tokens == 500

    def test_set_config_singleton(self):
        custom = StructuredMemoryConfig(enabled=True, max_index_tokens=3000)
        set_structured_memory_config(custom)
        assert get_structured_memory_config().max_index_tokens == 3000
        # Reset for other tests
        set_structured_memory_config(StructuredMemoryConfig())


class TestStructuredMemoryStore:
    """Tests for StructuredMemoryStore file I/O."""

    def test_resolve_path_rejects_traversal(self):
        store = StructuredMemoryStore()
        with pytest.raises(ValueError, match="traversal"):
            store.resolve_path("../etc/passwd")

    def test_write_and_read_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(StructuredMemoryStore, "root", PropertyMock(return_value=Path(tmpdir))):
                store = StructuredMemoryStore()
                store.ensure_directories()
                store.write_file("facts/index.md", "# Test Index\n\n- [test](test.md) — desc")
                content = store.read_file("facts/index.md")
                assert "# Test Index" in content

    def test_write_file_creates_parent_dirs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(StructuredMemoryStore, "root", PropertyMock(return_value=Path(tmpdir))):
                store = StructuredMemoryStore()
                store.write_file("a/b/c/test.md", "hello")
                assert store.read_file("a/b/c/test.md") == "hello"

    def test_file_exists(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(StructuredMemoryStore, "root", PropertyMock(return_value=Path(tmpdir))):
                store = StructuredMemoryStore()
                assert not store.file_exists("nope.md")
                store.write_file("nope.md", "exists")
                assert store.file_exists("nope.md")

    def test_list_dir_tree_format(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(StructuredMemoryStore, "root", PropertyMock(return_value=Path(tmpdir))):
                store = StructuredMemoryStore()
                store.ensure_directories()
                store.write_file("facts/index.md", "# idx")
                store.write_file("tasks/index.md", "# idx")
                output = store.list_dir(depth=2)
                assert "structured_memory/" in output
                assert "facts/" in output
                assert "tasks/" in output
                assert "index.md" in output

    def test_list_dir_respects_depth(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(StructuredMemoryStore, "root", PropertyMock(return_value=Path(tmpdir))):
                store = StructuredMemoryStore()
                store.write_file("facts/schema/tables/deep.md", "deep")
                output = store.list_dir(depth=1)
                assert "schema/" not in output  # depth=1 only shows top level

    def test_read_nonexistent_file_raises(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(StructuredMemoryStore, "root", PropertyMock(return_value=Path(tmpdir))):
                store = StructuredMemoryStore()
                with pytest.raises(FileNotFoundError):
                    store.read_file("does_not_exist.md")

    def test_ensure_directories_creates_structure(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(StructuredMemoryStore, "root", PropertyMock(return_value=Path(tmpdir))):
                store = StructuredMemoryStore()
                store.ensure_directories()
                assert (Path(tmpdir) / "facts" / "schema" / "tables").exists()
                assert (Path(tmpdir) / "facts" / "business").exists()
                assert (Path(tmpdir) / "tasks").exists()


class TestSearchMemoryFiles:
    """Tests for text search across memory files."""

    def test_search_finds_matches_in_facts(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            # Set up the store root
            store = StructuredMemoryStore()
            with patch.object(StructuredMemoryStore, "root", PropertyMock(return_value=root)):
                store.write_file("facts/schema/tables/ods_order.md", "订单明细表\n包含所有订单数据")
                store.write_file("facts/business/revenue.md", "收入确认规则")

                results = search_memory_files("订单", category="facts")
                assert "ods_order.md" in results

    def test_search_finds_matches_in_tasks(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            store = StructuredMemoryStore()
            with patch.object(StructuredMemoryStore, "root", PropertyMock(return_value=root)):
                store.write_file("tasks/2026/sales-analysis.md", "Q1 销售分析\n使用 SQL 查询")

                results = search_memory_files("SQL", category="tasks")
                assert "sales-analysis.md" in results

    def test_search_all_categories(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            store = StructuredMemoryStore()
            with patch.object(StructuredMemoryStore, "root", PropertyMock(return_value=root)):
                store.write_file("facts/schema/index.md", "common pattern xyz")
                store.write_file("tasks/2026/task.md", "another xyz occurrence")

                results = search_memory_files("xyz", category="all")
                assert "schema/index.md" in results
                assert "task.md" in results

    def test_search_no_matches(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            store = StructuredMemoryStore()
            with patch.object(StructuredMemoryStore, "root", PropertyMock(return_value=root)):
                store.write_file("facts/index.md", "some content")

                results = search_memory_files("nonexistent")
                assert "No matches found" in results

    def test_search_empty_store(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            with patch.object(StructuredMemoryStore, "root", PropertyMock(return_value=root)):
                results = search_memory_files("anything")
                assert "No memory files found" in results


class TestMemoryTemplates:
    """Tests for template rendering."""

    def test_facts_index_template_has_placeholders(self):
        assert "{last_updated}" in FACTS_INDEX_TEMPLATE

    def test_tasks_index_template_has_placeholders(self):
        assert "{entries}" in TASKS_INDEX_TEMPLATE
        assert "{last_updated}" in TASKS_INDEX_TEMPLATE

    def test_table_detail_template_has_all_fields(self):
        assert "{table_name}" in TABLE_DETAIL_TEMPLATE
        assert "{database}" in TABLE_DETAIL_TEMPLATE
        assert "{fields}" in TABLE_DETAIL_TEMPLATE

    def test_task_summary_template_has_all_fields(self):
        assert "{title}" in TASK_SUMMARY_TEMPLATE
        assert "{date}" in TASK_SUMMARY_TEMPLATE
        assert "{summary}" in TASK_SUMMARY_TEMPLATE


class TestStructuredMemoryTools:
    """Tests for the 4 built-in tools."""

    def test_search_structured_memory_tool_exists(self):
        from deerflow.tools.builtins.structured_memory_tools import search_structured_memory
        assert search_structured_memory.name == "search_structured_memory"

    def test_get_memory_entity_tool_exists(self):
        from deerflow.tools.builtins.structured_memory_tools import get_memory_entity
        assert get_memory_entity.name == "get_memory_entity"

    def test_list_memory_entities_tool_exists(self):
        from deerflow.tools.builtins.structured_memory_tools import list_memory_entities
        assert list_memory_entities.name == "list_memory_entities"

    def test_update_memory_index_tool_exists(self):
        from deerflow.tools.builtins.structured_memory_tools import update_memory_index
        assert update_memory_index.name == "update_memory_index"

    def test_get_memory_entity_returns_not_found_for_missing_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            from deerflow.structured_memory.storage import StructuredMemoryStore
            from deerflow.tools.builtins.structured_memory_tools import get_memory_entity

            with patch.object(StructuredMemoryStore, "root", PropertyMock(return_value=Path(tmpdir))):
                result = get_memory_entity.invoke({"path": "nonexistent.md"})
                assert "not found" in result.lower()

    def test_list_memory_entities_shows_directory(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            from deerflow.structured_memory.storage import StructuredMemoryStore
            from deerflow.tools.builtins.structured_memory_tools import list_memory_entities

            with patch.object(StructuredMemoryStore, "root", PropertyMock(return_value=Path(tmpdir))):
                store = StructuredMemoryStore()
                store.write_file("facts/index.md", "# idx")
                result = list_memory_entities.invoke({"path": "", "depth": 2})
                assert "facts/" in result
                assert "index.md" in result

    def test_update_memory_index_add_entry(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            from deerflow.structured_memory.storage import StructuredMemoryStore
            from deerflow.tools.builtins.structured_memory_tools import update_memory_index

            with patch.object(StructuredMemoryStore, "root", PropertyMock(return_value=Path(tmpdir))):
                store = StructuredMemoryStore()
                store.write_file("facts/schema/index.md", "# Schema Index\n\n")

                result = update_memory_index.invoke({
                    "index_path": "facts/schema/index.md",
                    "action": "add",
                    "entry": "- [ods.order](tables/ods_order.md) — 订单表",
                })
                assert "Added" in result

                content = store.read_file("facts/schema/index.md")
                assert "ods.order" in content

    def test_update_memory_index_remove_entry(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            from deerflow.structured_memory.storage import StructuredMemoryStore
            from deerflow.tools.builtins.structured_memory_tools import update_memory_index

            with patch.object(StructuredMemoryStore, "root", PropertyMock(return_value=Path(tmpdir))):
                store = StructuredMemoryStore()
                store.write_file("facts/index.md", "# Facts\n\n- [X](x.md) — to remove\n- [Y](y.md) — keep\n")

                result = update_memory_index.invoke({
                    "index_path": "facts/index.md",
                    "action": "remove",
                    "entry": "",
                    "target": "to remove",
                })
                assert "Removed" in result
                content = store.read_file("facts/index.md")
                assert "to remove" not in content
                assert "keep" in content

    def test_update_memory_index_update_entry(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            from deerflow.structured_memory.storage import StructuredMemoryStore
            from deerflow.tools.builtins.structured_memory_tools import update_memory_index

            with patch.object(StructuredMemoryStore, "root", PropertyMock(return_value=Path(tmpdir))):
                store = StructuredMemoryStore()
                store.write_file("facts/index.md", "# Facts\n- [old](old.md) — old desc\n")

                update_memory_index.invoke({
                    "index_path": "facts/index.md",
                    "action": "update",
                    "entry": "- [new](new.md) — new desc",
                    "target": "old desc",
                })
                content = store.read_file("facts/index.md")
                assert "new desc" in content
                assert "old desc" not in content

    def test_update_memory_index_duplicate_prevention(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            from deerflow.structured_memory.storage import StructuredMemoryStore
            from deerflow.tools.builtins.structured_memory_tools import update_memory_index

            with patch.object(StructuredMemoryStore, "root", PropertyMock(return_value=Path(tmpdir))):
                store = StructuredMemoryStore()
                store.write_file("facts/index.md", "# Facts\n- [X](x.md) — desc\n")

                result = update_memory_index.invoke({
                    "index_path": "facts/index.md",
                    "action": "add",
                    "entry": "- [X](x.md) — desc",
                })
                assert "already exists" in result.lower()


# Helper for patching properties
class PropertyMock:
    """A descriptor that returns a fixed value, usable as a patch target."""

    def __init__(self, return_value):
        self._return_value = return_value

    def __get__(self, obj, objtype=None):
        return self._return_value
```

- [ ] **Step 2: Run tests to verify they fail (TDD)**

```bash
cd backend && PYTHONPATH=. uv run pytest tests/test_structured_memory.py -v
```

Expected: All tests should pass since we wrote tests after implementation. If any fail, fix before proceeding.

- [ ] **Step 3: Commit**

```bash
git add tests/test_structured_memory.py
git commit -m "test(structured-memory): add comprehensive test suite"
```

---

### Task 7: Integration verification

- [ ] **Step 1: Run full test suite**

```bash
cd backend && make test
```

Expected: All existing tests pass, no regressions.

- [ ] **Step 2: Run harness boundary check**

```bash
cd backend && PYTHONPATH=. uv run pytest tests/test_harness_boundary.py -v
```

Expected: PASS — structured memory modules live in `deerflow.*` (harness), no `app.*` imports.

- [ ] **Step 3: Verify config loading**

```python
# Sanity check in Python REPL
cd backend && PYTHONPATH=. uv run python -c "
from deerflow.config.app_config import AppConfig
from deerflow.config.structured_memory_config import get_structured_memory_config
# With default config.yaml (structured_memory not set), should get defaults
print('structured_memory.enabled:', get_structured_memory_config().enabled)
print('OK')
"
```

Expected: `structured_memory.enabled: False` (default), `OK`.

- [ ] **Step 4: Commit any final changes**

```bash
git status
# If clean, done. If not, commit fixes.
```

# Structured Memory Core Summary Optimization

**For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Optimize structured memory flow by introducing `core.md` as the top-level summary file, injected into agent prompts instead of index files. Agent auto-determines whether to update `core.md` after each memory write.

**Architecture:** `core.md` stores domain-level summaries + key facts. Prompt injection changed from index files to `core.md` only. Agent judgment flow remains unchanged (agent decides whether to update core.md after memory writes based on its context).

**Tech Stack:** Python 3.12+, existing structured_memory modules

---

## File Structure

| File | Operation | Responsibility |
|------|----------|----------------|
| `packages/harness/deerflow/agents/lead_agent/prompt.py` | Modify | `_get_structured_memory_context()` injects only `core.md` instead of index files |
| `packages/harness/deerflow/structured_memory/storage.py` | Modify | `ensure_directories()` creates `core.md` parent dir, add `core.md` to initialized directories |
| `packages/harness/deerflow/structured_memory/__init__.py` | Modify | Export `core.md` path constant |
| `tests/test_structured_memory.py` | Modify | Add tests for core.md prompt injection |

---

## Task 1: Update Storage to Initialize core.md Directory

**Files:**
- Modify: `packages/harness/deerflow/structured_memory/storage.py:42-52`

- [ ] **Step 1: View current ensure_directories implementation**

Read the `ensure_directories()` method to confirm current directory structure.

- [ ] **Step 2: Verify core.md parent directory creation**

The `ensure_directories()` creates `facts/`, `tasks/` subdirectories. `core.md` is at root level (`structured_memory/core.md`), so no new directory needed — just ensure `self.root` exists. However, `core.md` may not exist initially, so the system should handle `FileNotFoundError` gracefully.

No code change required for `storage.py` — `core.md` lives at `structured_memory/core.md` and `self.root` is already created.

- [ ] **Step 3: Commit (no-op for storage)**

Skip if no change needed.

---

## Task 2: Export core.md Path Constant

**Files:**
- Modify: `packages/harness/deerflow/structured_memory/__init__.py`

- [ ] **Step 1: View current exports**

Read `packages/harness/deerflow/structured_memory/__init__.py`.

- [ ] **Step 2: Add CORE_MEMORY_FILENAME constant**

```python
"""DeerFlow structured memory system."""

from deerflow.structured_memory.storage import StructuredMemoryStore, get_structured_memory_store

__all__ = [
    "StructuredMemoryStore",
    "get_structured_memory_store",
    "CORE_MEMORY_FILENAME",
]

CORE_MEMORY_FILENAME = "core.md"
```

- [ ] **Step 3: Run tests to verify**

```bash
cd backend && PYTHONPATH=. uv run pytest tests/test_structured_memory.py::TestStructuredMemoryStore -v
```

Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add packages/harness/deerflow/structured_memory/__init__.py
git commit -m "refactor(structured-memory): export CORE_MEMORY_FILENAME constant"
```

---

## Task 3: Modify Prompt Injection to Use core.md Only

**Files:**
- Modify: `packages/harness/deerflow/agents/lead_agent/prompt.py:598-659`

- [ ] **Step 1: View current _get_structured_memory_context implementation**

Read the `_get_structured_memory_context()` function (lines 598-659 in `prompt.py`).

Current behavior:
- Reads `facts/index.md` and `tasks/index.md`
- Combines them into a `<structured_memory>` XML block
- Mentions tools: `get_memory_entity`, `search_structured_memory`, `list_memory_entities`, `create_memory_entity`, `update_memory_entity`, `delete_memory_entity`, `update_memory_index`

- [ ] **Step 2: Rewrite _get_structured_memory_context to inject only core.md**

Replace the entire function with:

```python
def _get_structured_memory_context() -> str:
    """Get structured memory core summary for injection into system prompt.

    Injects only the core.md top-level summary file, not the index files.
    Agent uses search/list tools to browse detailed entities on demand.

    Returns:
        Formatted structured memory context wrapped in XML tags, or empty string.
    """
    try:
        from deerflow.config.structured_memory_config import get_structured_memory_config
        from deerflow.structured_memory import CORE_MEMORY_FILENAME
        from deerflow.structured_memory.storage import get_structured_memory_store

        config = get_structured_memory_config()
        if not config.enabled or not config.injection_enabled:
            return ""

        store = get_structured_memory_store()
        store.ensure_directories()

        # Read core.md only
        try:
            core_content = store.read_file(CORE_MEMORY_FILENAME)
        except FileNotFoundError:
            core_content = (
                "(No core memory yet. Use create_memory_entity to build domain summaries. "
                "Use search_structured_memory and list_memory_entities to explore existing memory.)"
            )

        content = core_content
        if not content.strip():
            return ""

        return f"""<structured_memory>
Below is the enterprise structured memory core summary. This contains domain-level
summaries of data warehouse metadata, business definitions, and task history.
Only the core summary is shown here — use `search_structured_memory` to find specific
information by keyword, `list_memory_entities` to browse the directory structure,
`get_memory_entity` to load detailed entity files, `create_memory_entity` to create
new entity files, `update_memory_entity` to update existing files, and
`delete_memory_entity` to remove them.

{content}
</structured_memory>
"""
    except Exception as e:
        logger.error("Failed to load structured memory context: %s", e)
        return ""
```

- [ ] **Step 3: Run tests to verify**

```bash
cd backend && PYTHONPATH=. uv run pytest tests/test_lead_agent.py -v -k "structured" --tb=short
```

Expected: PASS (or skip if no existing structured memory prompt tests)

- [ ] **Step 4: Commit**

```bash
git add packages/harness/deerflow/agents/lead_agent/prompt.py
git commit -m "feat(structured-memory): inject core.md only instead of index files"
```

---

## Task 4: Add Test for core.md Prompt Injection

**Files:**
- Modify: `tests/test_structured_memory.py`

- [ ] **Step 1: View existing test structure**

Read `tests/test_structured_memory.py` to understand the test patterns used.

- [ ] **Step 2: Add test for core.md read in storage**

In `TestStructuredMemoryStore` class, add:

```python
def test_ensure_directories_creates_core_dir(self, tmp_path):
    """ensure_directories should create root directory (core.md lives at root level)."""
    from deerflow.structured_memory.storage import StructuredMemoryStore

    store = StructuredMemoryStore()
    with patch_store_root(store, tmp_path):
        store.ensure_directories()
        assert store.root.exists()
        assert (store.root / "facts" / "schema" / "tables").exists()
        assert (store.root / "facts" / "schema" / "fields").exists()
        assert (store.root / "facts" / "business").exists()
        assert (store.root / "tasks").exists()
        # core.md itself is optional, but its parent dir (root) must exist
        assert store.root.exists()
```

- [ ] **Step 3: Add test for _get_structured_memory_context injects core.md only**

In a new test class or existing test file, add:

```python
def test_get_structured_memory_context_injects_core_only(self, tmp_path):
    """_get_structured_memory_context should inject only core.md content."""
    from deerflow.config.structured_memory_config import StructuredMemoryConfig
    from deerflow.structured_memory import CORE_MEMORY_FILENAME
    from deerflow.agents.lead_agent.prompt import _get_structured_memory_context
    from unittest.mock import patch

    core_md_content = """# Core Memory

## 用户域
- 已完成 dim_user 等表的 schema 整理
- 关键事实：用户主数据包括 user_id, name, phone

## 订单域
- 已完成 dwd_order 系列表的 schema 整理
"""

    with patch("deerflow.structured_memory.storage._store", None):
        with patch("deerflow.agents.lead_agent.prompt.get_structured_memory_config") as mock_config:
            mock_config.return_value = StructuredMemoryConfig(enabled=True, injection_enabled=True)
            with patch("deerflow.agents.lead_agent.prompt.get_structured_memory_store") as mock_store:
                store = StructuredMemoryStore()
                with patch_store_root(store, tmp_path):
                    store.ensure_directories()
                    store.write_file(CORE_MEMORY_FILENAME, core_md_content)
                    mock_store.return_value = store

                    result = _get_structured_memory_context()

                    assert "用户域" in result
                    assert "dim_user" in result
                    assert "index.md" not in result
                    assert "facts/index.md" not in result
                    assert "tasks/index.md" not in result
```

- [ ] **Step 4: Add test for FileNotFoundError fallback**

```python
def test_get_structured_memory_context_core_not_found(self, tmp_path):
    """Should return fallback message when core.md does not exist."""
    from deerflow.config.structured_memory_config import StructuredMemoryConfig
    from deerflow.agents.lead_agent.prompt import _get_structured_memory_context
    from unittest.mock import patch

    with patch("deerflow.structured_memory.storage._store", None):
        with patch("deerflow.agents.lead_agent.prompt.get_structured_memory_config") as mock_config:
            mock_config.return_value = StructuredMemoryConfig(enabled=True, injection_enabled=True)
            with patch("deerflow.agents.lead_agent.prompt.get_structured_memory_store") as mock_store:
                store = StructuredMemoryStore()
                with patch_store_root(store, tmp_path):
                    store.ensure_directories()
                    mock_store.return_value = store

                    result = _get_structured_memory_context()

                    assert "No core memory yet" in result
```

- [ ] **Step 5: Run tests**

```bash
cd backend && PYTHONPATH=. uv run pytest tests/test_structured_memory.py -v -k "core" --tb=short
```

Expected: ALL PASS

- [ ] **Step 6: Commit**

```bash
git add tests/test_structured_memory.py
git commit -m "test(structured-memory): add tests for core.md prompt injection"
```

---

## Task 5: Update Structured Memory Skill Documentation

**Files:**
- Modify: `skills/public/structured-memory/SKILL.md` (if applicable)

- [ ] **Step 1: Check if skill doc references index.md injection**

Read `skills/public/structured-memory/SKILL.md` to see if it documents the prompt injection behavior.

- [ ] **Step 2: Update if needed**

If the skill doc mentions index.md injection, update it to reflect the core.md-only behavior.

- [ ] **Step 3: Commit**

```bash
git add skills/public/structured-memory/SKILL.md
git commit -m "docs(structured-memory): update skill doc for core.md-only injection"
```

---

## Task 6: Full Regression

- [ ] **Step 1: Run structured memory tests**

```bash
cd backend && PYTHONPATH=. uv run pytest tests/test_structured_memory.py -v --tb=short
```

Expected: ALL PASS

- [ ] **Step 2: Run harness boundary check**

```bash
cd backend && PYTHONPATH=. uv run pytest tests/test_harness_boundary.py -v
```

Expected: PASS

- [ ] **Step 3: Commit all remaining changes**

```bash
git status
git add -A
git commit -m "feat(structured-memory): core.md-only prompt injection for domain summaries"
```

---

## Verification Commands

```bash
# Run all structured memory tests
cd backend && PYTHONPATH=. uv run pytest tests/test_structured_memory.py -v --tb=short

# Run harness boundary check
cd backend && PYTHONPATH=. uv run pytest tests/test_harness_boundary.py -v

# Manual verification: check prompt output
cd backend && PYTHONPATH=. python -c "
from deerflow.agents.lead_agent.prompt import _get_structured_memory_context
print(_get_structured_memory_context())
"
```

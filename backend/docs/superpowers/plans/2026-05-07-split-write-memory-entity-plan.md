# Split write_memory_entity into Create/Update Tools

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split the dual-purpose `write_memory_entity` tool into two single-purpose tools — `create_memory_entity` (create-only, errors if exists) and `update_memory_entity` (update-only via `changes`, errors if not found).

**Architecture:** `structured_memory_tools.py` gains two new `@tool` functions. The original `write_memory_entity` is deleted. The shared lock helpers, store access, `register_entity`/`unregister_entity`, and `apply_partial_update` remain unchanged.

**Tech Stack:** Python, langchain `@tool` decorator, file locking (fcntl), structured memory store.

---

## Files Modified

| File | Change |
|------|--------|
| `packages/harness/deerflow/tools/builtins/structured_memory_tools.py` | Delete `write_memory_entity`, add `create_memory_entity` + `update_memory_entity` |
| `packages/harness/deerflow/tools/tools.py` | Remove `write_memory_entity` from `get_available_tools()` call |
| `tests/test_structured_memory.py` | Delete old `write_memory_entity` tests, add tests for both new tools |

---

## Task 1: Add create_memory_entity Tool

**Files:**
- Modify: `packages/harness/deerflow/tools/builtins/structured_memory_tools.py`

- [ ] **Step 1: Write the failing test**

In `TestStructuredMemoryTools`, add:

```python
def test_create_memory_entity_creates_new_file(self, tmp_path):
    from deerflow.tools.builtins.structured_memory_tools import create_memory_entity

    store = StructuredMemoryStore()
    with _patch_store_root(store, tmp_path):
        content = "# test_table\n\n## Basic Info\n\n- **database**: ods"
        result = create_memory_entity.invoke({
            "path": "facts/schema/tables/test.md",
            "content": content,
        })
        assert "written" in result.lower()
        assert store.read_file("facts/schema/tables/test.md") == content

def test_create_memory_entity_errors_if_exists(self, tmp_path):
    from deerflow.tools.builtins.structured_memory_tools import create_memory_entity

    store = StructuredMemoryStore()
    with _patch_store_root(store, tmp_path):
        store.write_file("facts/schema/tables/test.md", "existing")
        result = create_memory_entity.invoke({
            "path": "facts/schema/tables/test.md",
            "content": "# new content",
        })
        assert "already exists" in result.lower()
        assert "update_memory_entity" in result
        assert store.read_file("facts/schema/tables/test.md") == "existing"

def test_create_memory_entity_rejects_traversal(self, tmp_path):
    from deerflow.tools.builtins.structured_memory_tools import create_memory_entity

    store = StructuredMemoryStore()
    with _patch_store_root(store, tmp_path):
        result = create_memory_entity.invoke({
            "path": "../etc/passwd",
            "content": "malicious",
        })
        assert "invalid path" in result.lower()

def test_create_memory_entity_auto_indexes(self, tmp_path):
    from deerflow.tools.builtins.structured_memory_tools import create_memory_entity

    store = StructuredMemoryStore()
    with _patch_store_root(store, tmp_path):
        store.ensure_directories()
        content = "# Basic Info\n\n## Compiled Truth\n\n- **objective**: 订单明细表"
        result = create_memory_entity.invoke({
            "path": "facts/schema/tables/ods_order.md",
            "content": content,
        })
        assert "written" in result.lower()
        assert "index" in result.lower()
        index_content = store.read_file("facts/schema/index.md")
        assert "订单明细表" in index_content
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_structured_memory.py::TestStructuredMemoryTools::test_create_memory_entity_creates_new_file -v`
Expected: FAIL — `create_memory_entity` not defined

- [ ] **Step 3: Add create_memory_entity tool**

Add this new `@tool` after the existing `update_memory_index` tool:

```python
@tool("create_memory_entity", parse_docstring=True)
def create_memory_entity(
    path: str,
    content: str,
) -> str:
    """Create a new structured memory entity file.

    Use this tool to create brand-new entity files in the structured memory
    store. This writes to the actual structured memory store directory,
    NOT the sandbox workspace. The corresponding index entry is updated
    automatically.

    Args:
        path: Relative path within the memory store, e.g.
            'facts/schema/tables/ods_order.md' or 'tasks/2026/task-001.md'.
        content: The full markdown content for the new entity.

    Returns:
        Confirmation message with the path written and index update result.
    """
    try:
        store = _get_store()
        target = store.resolve_path(path)
        lock_path = str(target) + ".lock"

        lock_obj, _ = _acquire_entity_lock(lock_path)
        try:
            if store.file_exists(path):
                return (
                    f"Memory entity already exists: {path}. "
                    "Use update_memory_entity to modify existing memories."
                )

            if not content:
                return "Error: content cannot be empty when creating a new entity."

            store.write_file(path, content)
            index_msg = register_entity(store, path, None, content)
            return f"Memory entity written: {path} | {index_msg}"
        finally:
            _release_entity_lock(lock_obj, lock_path)
    except ValueError as e:
        return f"Invalid path: {e}"
    except Exception as e:
        logger.exception("create_memory_entity failed")
        return f"Failed to create memory entity: {e}"
```

- [ ] **Step 4: Run tests to verify they pass**

Run each:
```
pytest tests/test_structured_memory.py::TestStructuredMemoryTools::test_create_memory_entity_creates_new_file -v
pytest tests/test_structured_memory.py::TestStructuredMemoryTools::test_create_memory_entity_errors_if_exists -v
pytest tests/test_structured_memory.py::TestStructuredMemoryTools::test_create_memory_entity_rejects_traversal -v
pytest tests/test_structured_memory.py::TestStructuredMemoryTools::test_create_memory_entity_auto_indexes -v
```
Expected: all PASS

- [ ] **Step 5: Commit**

```bash
git add packages/harness/deerflow/tools/builtins/structured_memory_tools.py tests/test_structured_memory.py
git commit -m "feat(structured-memory): add create_memory_entity tool"
```

---

## Task 2: Add update_memory_entity Tool

**Files:**
- Modify: `packages/harness/deerflow/tools/builtins/structured_memory_tools.py`

- [ ] **Step 1: Write the failing tests**

In `TestStructuredMemoryTools`, add:

```python
def test_update_memory_entity_partial_update_preserves_unchanged(self, tmp_path):
    from deerflow.tools.builtins.structured_memory_tools import create_memory_entity, update_memory_entity

    store = StructuredMemoryStore()
    with _patch_store_root(store, tmp_path):
        initial = "# test_table\n\n## Basic Info\n\n- **database**: ods\n- **table**: test_table\n- **update_frequency**: daily\n\n## Compiled Truth\n\n- **objective**: 原始业务表\n- **definition**: 初始定义\n- **core_logic**: 初始逻辑\n\n### upstream dependencies\n\n- upstream.source\n\n### columns\n\n| column | description |\n|--------|-------------|\n| col1 | 字段1 |\n\n### SQL\n```sql\n\n```\n\n## Timeline\n\n- **2026-04-30 14:30:00** — 首次写入\n\n---\n*created: 2026-04-30 14:30:00*\n*updated: 2026-04-30 14:30:00*\n"
        create_memory_entity.invoke({
            "path": "facts/schema/tables/test.md",
            "content": initial,
        })

        result = update_memory_entity.invoke({
            "path": "facts/schema/tables/test.md",
            "changes": '{"definition": "更新后的定义"}',
            "timeline_desc": "更新了定义",
        })
        assert "written" in result.lower() or "updated" in result.lower()

        content = store.read_file("facts/schema/tables/test.md")
        assert "更新后的定义" in content
        assert "**objective**: 原始业务表" in content
        assert "**database**: ods" in content

def test_update_memory_entity_errors_if_not_found(self, tmp_path):
    from deerflow.tools.builtins.structured_memory_tools import update_memory_entity

    store = StructuredMemoryStore()
    with _patch_store_root(store, tmp_path):
        result = update_memory_entity.invoke({
            "path": "facts/schema/tables/nonexistent.md",
            "changes": '{"definition": "new"}',
            "timeline_desc": "desc",
        })
        assert "not found" in result.lower()
        assert "create_memory_entity" in result

def test_update_memory_entity_requires_changes(self, tmp_path):
    from deerflow.tools.builtins.structured_memory_tools import create_memory_entity, update_memory_entity

    store = StructuredMemoryStore()
    with _patch_store_root(store, tmp_path):
        create_memory_entity.invoke({
            "path": "facts/schema/tables/test.md",
            "content": "# test\n\n## Basic Info\n\n- **database**: ods",
        })
        result = update_memory_entity.invoke({
            "path": "facts/schema/tables/test.md",
            "changes": "",
            "timeline_desc": "",
        })
        assert "changes" in result.lower()

def test_update_memory_entity_auto_indexes(self, tmp_path):
    from deerflow.tools.builtins.structured_memory_tools import create_memory_entity, update_memory_entity

    store = StructuredMemoryStore()
    with _patch_store_root(store, tmp_path):
        store.ensure_directories()
        create_memory_entity.invoke({
            "path": "facts/schema/tables/ods_order.md",
            "content": "# Basic Info\n\n## Compiled Truth\n\n- **objective**: 订单明细表",
        })

        result = update_memory_entity.invoke({
            "path": "facts/schema/tables/ods_order.md",
            "changes": '{"definition": "从业务系统同步的订单明细数据"}',
            "timeline_desc": "更新了定义",
        })
        assert "written" in result.lower() or "updated" in result.lower()
        index_content = store.read_file("facts/schema/index.md")
        assert "订单明细表" in index_content
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_structured_memory.py::TestStructuredMemoryTools::test_update_memory_entity_partial_update_preserves_unchanged -v`
Expected: FAIL — `update_memory_entity` not defined

- [ ] **Step 3: Add update_memory_entity tool**

Add this new `@tool` after the new `create_memory_entity` tool:

```python
@tool("update_memory_entity", parse_docstring=True)
def update_memory_entity(
    path: str,
    changes: str,
    timeline_desc: str = "",
) -> str:
    """Partially update an existing structured memory entity file.

    Use this tool to update specific fields in an existing entity file.
    Only the fields specified in the `changes` JSON dict will be modified;
    all other fields are preserved. A new timeline entry is appended.

    Args:
        path: Relative path to the existing memory file, e.g.
            'facts/schema/tables/ods_order.md'.
        changes: JSON dict of changed fields (e.g.
            '{"definition": "new definition", "columns": [...]}'). Use
            ``column_updates`` (list of {name, description}) to update
            individual column descriptions without replacing the full list.
        timeline_desc: Description for the new timeline entry.

    Returns:
        Confirmation message with the path written and index update result.
    """
    try:
        store = _get_store()
        target = store.resolve_path(path)
        lock_path = str(target) + ".lock"

        lock_obj, _ = _acquire_entity_lock(lock_path)
        try:
            if not store.file_exists(path):
                return (
                    f"Memory entity not found: {path}. "
                    "Use create_memory_entity to create new memories."
                )

            changes_dict = parse_changes_json(changes)
            if not changes_dict:
                return "Error: changes parameter is required for partial updates."

            old_content = store.read_file(path)
            final_content = apply_partial_update(old_content, changes_dict, timeline_desc)

            store.write_file(path, final_content)
            index_msg = register_entity(store, path, old_content, final_content)
            return f"Memory entity written: {path} | {index_msg}"
        finally:
            _release_entity_lock(lock_obj, lock_path)
    except ValueError as e:
        return f"Invalid path: {e}"
    except Exception as e:
        logger.exception("update_memory_entity failed")
        return f"Failed to update memory entity: {e}"
```

- [ ] **Step 4: Run tests to verify they pass**

Run each:
```
pytest tests/test_structured_memory.py::TestStructuredMemoryTools::test_update_memory_entity_partial_update_preserves_unchanged -v
pytest tests/test_structured_memory.py::TestStructuredMemoryTools::test_update_memory_entity_errors_if_not_found -v
pytest tests/test_structured_memory.py::TestStructuredMemoryTools::test_update_memory_entity_requires_changes -v
pytest tests/test_structured_memory.py::TestStructuredMemoryTools::test_update_memory_entity_auto_indexes -v
```
Expected: all PASS

- [ ] **Step 5: Commit**

```bash
git add packages/harness/deerflow/tools/builtins/structured_memory_tools.py tests/test_structured_memory.py
git commit -m "feat(structured-memory): add update_memory_entity tool"
```

---

## Task 3: Remove write_memory_entity and Update tools.py

**Files:**
- Modify: `packages/harness/deerflow/tools/builtins/structured_memory_tools.py`
- Modify: `packages/harness/deerflow/tools/tools.py`

- [ ] **Step 1: Delete write_memory_entity from structured_memory_tools.py**

Remove the entire `write_memory_entity` function (lines 224–294 in the original file).

- [ ] **Step 2: Update tools.py to remove write_memory_entity**

In `packages/harness/deerflow/tools/tools.py`, find the line that imports or references `write_memory_entity` and remove it from the tool list passed to `get_available_tools()`.

Run: `grep -n "write_memory_entity" packages/harness/deerflow/tools/tools.py`
Then edit to remove it.

- [ ] **Step 3: Run the full structured memory test suite**

```bash
cd backend && PYTHONPATH=. uv run pytest tests/test_structured_memory.py -v
```
Expected: all tests PASS (old write_memory_entity tests removed, new ones pass)

- [ ] **Step 4: Commit**

```bash
git add packages/harness/deerflow/tools/builtins/structured_memory_tools.py packages/harness/deerflow/tools/tools.py
git commit -m "refactor(structured-memory): remove write_memory_entity, replaced by create/update tools"
```

---

## Task 4: Final Verification

- [ ] **Step 1: Run full test suite**

```bash
cd backend && PYTHONPATH=. uv run pytest tests/test_structured_memory.py tests/test_memory_updater.py -v
```

- [ ] **Step 2: Verify tools.py has no reference to write_memory_entity**

```bash
grep -r "write_memory_entity" packages/harness/deerflow/
```
Expected: only in git history, not in any current file.

- [ ] **Step 3: Commit**

```bash
git add -A && git commit -m "feat(structured-memory): split write_memory_entity into create/update tools"
```

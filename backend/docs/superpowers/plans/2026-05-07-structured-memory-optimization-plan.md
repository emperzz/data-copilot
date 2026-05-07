# Structured Memory 优化实现计划

**For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 修复P0竞态条件、实现Config热重载、增强Markdown反序列化能力

**Architecture:** 使用文件锁(fcntl.flock)解决竞态，从AppConfig动态读取实现热重载，添加YAML frontmatter增强反序列化

**Tech Stack:** Python 3.12+, fcntl文件锁, PyYAML, Pydantic

---

## 文件修改清单

| 文件 | 操作 | 职责 |
|------|------|------|
| `packages/harness/deerflow/structured_memory/index_service.py` | 修改 | 添加文件锁保护索引更新 |
| `packages/harness/deerflow/tools/builtins/structured_memory_tools.py` | 修改 | 添加文件锁保护写入流程 |
| `packages/harness/deerflow/structured_memory/models.py` | 修改 | 添加frontmatter序列化/反序列化 |
| `packages/harness/deerflow/config/structured_memory_config.py` | 修改 | 删除单例，改为动态读取 |
| `tests/test_structured_memory.py` | 修改 | 添加竞态和frontmatter测试用例 |

---

## Task 1: Config 热重载

**Files:**
- Modify: `packages/harness/deerflow/config/structured_memory_config.py:1-51`

- [ ] **Step 1: 查看当前实现**

读取当前 `structured_memory_config.py` 完整内容，确认现有 `get_structured_memory_config()` 实现。

- [ ] **Step 2: 修改 get_structured_memory_config 函数**

```python
def get_structured_memory_config() -> StructuredMemoryConfig:
    """Get the current structured memory configuration.
    
    Reads dynamically from AppConfig which handles mtime-based reload.
    """
    from deerflow.config import get_app_config
    app_config = get_app_config()
    return getattr(app_config, 'structured_memory', StructuredMemoryConfig())
```

- [ ] **Step 3: 删除 set_structured_memory_config 和 load_structured_memory_config_from_dict**

这两个函数仅用于测试设置单例，删除它们。

- [ ] **Step 4: 更新测试以适应新实现**

修改 `tests/test_structured_memory.py` 中 `TestStructuredMemoryConfig` 类的测试，移除对 `set_structured_memory_config` 的调用。测试现在需要 patch `get_app_config()` 来模拟配置。

- [ ] **Step 5: 运行测试验证**

```bash
cd backend && PYTHONPATH=. uv run pytest tests/test_structured_memory.py::TestStructuredMemoryConfig -v
```

Expected: PASS

- [ ] **Step 6: 提交**

```bash
git add packages/harness/deerflow/config/structured_memory_config.py tests/test_structured_memory.py
git commit -m "refactor(structured-memory): make config hot-reloadable via AppConfig"
```

---

## Task 2: 索引更新竞态条件修复

**Files:**
- Modify: `packages/harness/deerflow/structured_memory/index_service.py:1-252`

- [ ] **Step 1: 查看当前 _update_index 实现**

读取 `index_service.py`，重点关注 `_update_index` 函数（第144-198行）的读-改-写逻辑。

- [ ] **Step 2: 添加文件锁辅助函数**

在文件顶部添加 `import fcntl`（如果尚未导入），在模块末尾添加：

```python
def _acquire_index_lock(lock_path: str) -> IO:
    """Acquire exclusive lock on index file."""
    lock_file = open(lock_path, 'w')
    fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
    return lock_file

def _release_index_lock(lock_file: IO) -> None:
    """Release exclusive lock and close file."""
    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
    lock_file.close()
```

- [ ] **Step 3: 重写 _update_index 函数**

将 `_update_index` 改为 `_update_index_locked`，接收已打开的锁文件：

```python
def _update_index_locked(
    store: StructuredMemoryStore,
    index_path: str,
    action: Literal["add", "remove", "update"],
    entry: str,
    target: str = "",
    lock_file: IO | None = None,
) -> str:
    """Internal locked implementation of index update."""
    if action == "add":
        if not store.file_exists(index_path):
            _write_index(store, index_path, f"# Index\n\n{entry}\n")
            return f"Created {index_path} with entry: {entry}"
        content = _read_index(store, index_path)
        if entry.strip() in content:
            return f"Entry already exists in {index_path}"
        new_content = content.rstrip("\n") + "\n" + entry + "\n"
        _write_index(store, index_path, new_content)
        return f"Added to {index_path}: {entry}"
    # ... rest of add/remove/update logic
```

- [ ] **Step 4: 修改 register_entity 和 unregister_entity 使用锁**

在 `register_entity` 和 `unregister_entity` 的开始处获取锁，结束时释放锁：

```python
def register_entity(...) -> str:
    index_path = get_index_path(path)
    if not index_path:
        return f"No auto-indexing for: {path}"
    
    # Acquire lock
    resolved = store.resolve_path(index_path)
    lock_path = str(resolved) + ".lock"
    lock_file = _acquire_index_lock(lock_path)
    try:
        # ... existing logic calling _update_index_locked ...
    finally:
        _release_index_lock(lock_file)
```

同样修改 `unregister_entity`。

- [ ] **Step 5: 添加竞态条件测试**

在 `tests/test_structured_memory.py` 的 `TestIndexService` 类中添加：

```python
def test_register_entity_concurrent_updates(self, tmp_path):
    """Verify register_entity handles concurrent index updates safely."""
    from concurrent.futures import ThreadPoolExecutor
    from deerflow.structured_memory.index_service import register_entity
    from deerflow.structured_memory.storage import StructuredMemoryStore

    store = StructuredMemoryStore()
    with _patch_store_root(store, tmp_path):
        store.ensure_directories()
        content = """# Basic Info

## Compiled Truth

- **objective**: 测试并发更新
"""
        # 并发注册多个不同的实体
        def register_task(i):
            return register_entity(
                store, 
                f"facts/schema/tables/table_{i}.md", 
                None, 
                content + f"\n\n- **table**: table_{i}"
            )
        
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(register_task, i) for i in range(8)]
            results = [f.result() for f in futures]
        
        # All should succeed
        for r in results:
            assert "Added" in r or "created" in r.lower()
        
        # Index should contain all entries
        index = store.read_file("facts/schema/index.md")
        for i in range(8):
            assert f"table_{i}" in index
```

- [ ] **Step 6: 运行测试验证**

```bash
cd backend && PYTHONPATH=. uv run pytest tests/test_structured_memory.py::TestIndexService::test_register_entity_concurrent_updates -v
```

Expected: PASS

- [ ] **Step 7: 提交**

```bash
git add packages/harness/deerflow/structured_memory/index_service.py tests/test_structured_memory.py
git commit -m "fix(structured-memory): add file locking to prevent index update race conditions"
```

---

## Task 3: 写入流程竞态条件修复

**Files:**
- Modify: `packages/harness/deerflow/tools/builtins/structured_memory_tools.py:1-272`

- [ ] **Step 1: 查看当前 write_memory_entity 实现**

读取 `structured_memory_tools.py`，重点关注 `write_memory_entity` 函数（第179-239行）。

- [ ] **Step 2: 添加文件锁辅助函数**

在文件顶部确保 `fcntl` 已导入。在文件顶部添加：

```python
def _acquire_entity_lock(lock_path: str) -> IO:
    """Acquire exclusive lock on entity file."""
    lock_file = open(lock_path, 'w')
    fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
    return lock_file

def _release_entity_lock(lock_file: IO) -> None:
    """Release exclusive lock and close file."""
    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
    lock_file.close()
```

- [ ] **Step 3: 重写 write_memory_entity 函数**

修改 `write_memory_entity`，在函数开始处获取锁，结束时释放：

```python
def write_memory_entity(path: str, content: str, changes: str = "", timeline_desc: str = "") -> str:
    store = _get_store()
    target = store.resolve_path(path)
    lock_path = str(target) + ".lock"
    
    lock_file = _acquire_entity_lock(lock_path)
    try:
        old_content: str | None = None
        try:
            old_content = store.read_file(path)
        except FileNotFoundError:
            pass

        final_content: str
        changes_dict = parse_changes_json(changes)

        if old_content and changes_dict:
            final_content = apply_partial_update(old_content, changes_dict, timeline_desc)
        else:
            final_content = content

        if not final_content:
            return "Error: content cannot be empty when creating a new entity. Use changes parameter for partial updates."

        store.write_file(path, final_content)
        index_msg = register_entity(store, path, old_content, final_content)
        return f"Memory entity written: {path} | {index_msg}"
    except ValueError as e:
        return f"Invalid path: {e}"
    except Exception as e:
        logger.exception("write_memory_entity failed")
        return f"Failed to write memory entity: {e}"
    finally:
        _release_entity_lock(lock_file)
```

- [ ] **Step 4: 修改 delete_memory_entity 使用锁**

同样修改 `delete_memory_entity`，在删除前获取锁：

```python
def delete_memory_entity(path: str) -> str:
    store = _get_store()
    target = store.resolve_path(path)
    lock_path = str(target) + ".lock"
    
    lock_file = _acquire_entity_lock(lock_path)
    try:
        if not target.exists():
            return f"Memory entity not found: {path}"
        content = target.read_text(encoding="utf-8")
        target.unlink()
        index_msg = unregister_entity(store, path, content)
        return f"Memory entity deleted: {path} | {index_msg}"
    except ValueError as e:
        return f"Invalid path: {e}"
    except Exception as e:
        logger.exception("delete_memory_entity failed")
        return f"Failed to delete memory entity: {e}"
    finally:
        _release_entity_lock(lock_file)
```

- [ ] **Step 5: 添加写入竞态测试**

在 `tests/test_structured_memory.py` 的 `TestStructuredMemoryTools` 类中添加：

```python
def test_write_memory_entity_concurrent_updates(self, tmp_path):
    """Verify write_memory_entity handles concurrent updates to same entity."""
    from concurrent.futures import ThreadPoolExecutor
    from deerflow.tools.builtins.structured_memory_tools import write_memory_entity

    store = StructuredMemoryStore()
    with _patch_store_root(store, tmp_path):
        # Initialize entity
        initial = """# test_table

## Basic Info

- **database**: ods
- **table**: test_table
- **update_frequency**: daily

## Compiled Truth

- **objective**: 原始表
- **definition**: 初始定义
- **core_logic**: 无

## Timeline

- **2026-04-30 14:30:00** — 首次写入
"""
        write_memory_entity.invoke({
            "path": "facts/schema/tables/test_table.md",
            "content": initial,
        })

        # Concurrent partial updates
        def update_task(i):
            return write_memory_entity.invoke({
                "path": "facts/schema/tables/test_table.md",
                "content": "",
                "changes": json.dumps({"definition": f"定义更新{i}"}),
                "timeline_desc": f"并发更新{i}",
            })

        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(update_task, i) for i in range(4)]
            results = [f.result() for f in futures]

        # All should succeed
        for r in results:
            assert "written" in r.lower() or "updated" in r.lower()

        # Final content should have exactly 4 timeline entries (initial + 4 concurrent)
        final_content = store.read_file("facts/schema/tables/test_table.md")
        timeline_matches = re.findall(r"并发更新\d", final_content)
        assert len(timeline_matches) == 4
```

- [ ] **Step 6: 运行测试验证**

```bash
cd backend && PYTHONPATH=. uv run pytest tests/test_structured_memory.py::TestStructuredMemoryTools::test_write_memory_entity_concurrent_updates -v
```

Expected: PASS

- [ ] **Step 7: 提交**

```bash
git add packages/harness/deerflow/tools/builtins/structured_memory_tools.py tests/test_structured_memory.py
git commit -m "fix(structured-memory): add file locking to prevent write race conditions"
```

---

## Task 4: Markdown Frontmatter 增强

**Files:**
- Modify: `packages/harness/deerflow/structured_memory/models.py:1-447`
- Modify: `tests/test_structured_memory.py`

- [ ] **Step 1: 添加 PyYAML 依赖检查**

确认项目 `pyproject.toml` 中已有 `pyyaml` 依赖。如果没有，需要添加。

- [ ] **Step 2: 修改 table_entity_to_markdown 函数**

在文件顶部确保 `import yaml`（如未存在），修改 `table_entity_to_markdown`：

```python
def table_entity_to_markdown(entity: TableEntity) -> str:
    """Serialize a TableEntity to the structured 3-layer markdown format with YAML frontmatter."""
    # 1. 生成 frontmatter
    fm = {
        "tablename": entity.basic_info.tablename,
        "database": entity.basic_info.database,
        "update_frequency": entity.basic_info.update_frequency,
        "objective": entity.compiled_truth.objective,
        "definition": entity.compiled_truth.definition,
        "core_logic": entity.compiled_truth.core_logic,
        "created_at": entity.created_at,
        "updated_at": entity.updated_at,
    }
    # 过滤 None 值
    fm = {k: v for k, v in fm.items() if v is not None}
    fm_yaml = yaml.dump(fm, allow_unicode=True, sort_keys=False, default_flow_style=False).rstrip()

    # 2. 原有的 Markdown body 渲染
    bi = entity.basic_info
    ct = entity.compiled_truth

    source_tables_md = _format_source_tables(ct.source_tables)
    columns_md = _format_columns(ct.columns)
    timeline_md = _format_timeline(entity.timeline)
    sql_block = ct.sql if ct.sql else ""

    sql_section = f"```sql\n{sql_block}\n```" if sql_block else "```sql\n\n```"

    created = entity.created_at or ""
    updated = entity.updated_at or ""

    md_body = f"""# {bi.tablename}

## Basic Info

- **database**: {bi.database}
- **table**: {bi.tablename}
- **update_frequency**: {bi.update_frequency}

## Compiled Truth

- **objective**: {ct.objective}
- **definition**: {ct.definition}
- **core_logic**: {ct.core_logic}

### upstream dependencies

{source_tables_md}

### columns

{columns_md}

### SQL
{sql_section}

## Timeline

{timeline_md}

---
*created: {created}*
*updated: {updated}*
"""

    return f"---\n{fm_yaml}\n---\n\n{md_body}"
```

- [ ] **Step 3: 添加 frontmatter 解析辅助函数**

在 `markdown_to_table_entity` 函数上方添加：

```python
def _parse_frontmatter(content: str) -> tuple[dict | None, str]:
    """Parse YAML frontmatter from markdown content.
    
    Returns (frontmatter_dict, body_without_frontmatter) or (None, original_content).
    """
    if not content.strip().startswith("---"):
        return None, content
    
    parts = content.split("---", 2)
    if len(parts) < 3:
        return None, content
    
    fm_raw, body = parts[1], parts[2].strip()
    try:
        fm = yaml.safe_load(fm_raw)
        if not isinstance(fm, dict):
            return None, content
        return fm, body
    except yaml.YAMLError:
        return None, content
```

- [ ] **Step 4: 修改 markdown_to_table_entity 函数**

重写 `markdown_to_table_entity`，优先从 frontmatter 解析：

```python
def markdown_to_table_entity(content: str) -> TableEntity:
    """Best-effort parse of markdown content back into a TableEntity.
    
    First tries YAML frontmatter, then falls back to regex parsing.
    """
    if not content or not content.strip():
        return TableEntity(
            basic_info=TableBasicInfo(tablename="(unknown)"),
            compiled_truth=TableCompiledTruth(),
        )

    # 1. 优先从 frontmatter 解析
    fm, body = _parse_frontmatter(content)
    if fm:
        return _parse_from_frontmatter(fm, body)

    # 2. 回退到正则解析（向后兼容）
    return _parse_legacy_table_entity(content)
```

- [ ] **Step 5: 添加 _parse_from_frontmatter 辅助函数**

在 `_parse_frontmatter` 下方添加：

```python
def _parse_from_frontmatter(fm: dict, body: str) -> TableEntity:
    """Parse TableEntity from frontmatter dict and markdown body."""
    # Basic Info
    bi = TableBasicInfo(
        tablename=str(fm.get("tablename", "")) or "(unknown)",
        database=str(fm.get("database", "")),
        update_frequency=fm.get("update_frequency", "daily"),
    )

    # Compiled Truth - 需要从 body 中解析 columns, source_tables, sql
    ct_dict = _parse_compiled_truth_from_body(body)
    
    ct = TableCompiledTruth(
        objective=str(fm.get("objective", "")),
        definition=str(fm.get("definition", "")),
        core_logic=str(fm.get("core_logic", "")),
        source_tables=ct_dict.get("source_tables", []),
        columns=ct_dict.get("columns", []),
        sql=ct_dict.get("sql"),
    )

    # Timeline - 从 body 解析
    timeline = _parse_timeline(body)

    return TableEntity(
        basic_info=bi,
        compiled_truth=ct,
        timeline=timeline,
        created_at=str(fm.get("created_at", "")) or None,
        updated_at=str(fm.get("updated_at", "")) or None,
    )


def _parse_compiled_truth_from_body(body: str) -> dict:
    """Parse source_tables, columns, sql from markdown body (for frontmatter mode)."""
    # Reuse existing regex logic from legacy parser for these fields
    # ...


def _parse_legacy_table_entity(content: str) -> TableEntity:
    """Legacy parser using regex only (for backward compatibility)."""
    # 将原有 markdown_to_table_entity 的核心逻辑移到这里
    # ...
```

- [ ] **Step 6: 更新 apply_partial_update 函数**

`apply_partial_update` 目前直接操作 markdown 字符串，需要调整以支持 frontmatter：

```python
def apply_partial_update(
    existing_markdown: str,
    changes: dict,
    timeline_desc: str,
) -> str:
    """Apply a partial update to an existing table entity markdown document."""
    if not changes and not timeline_desc:
        return existing_markdown

    # 解析现有 markdown
    entity = markdown_to_table_entity(existing_markdown)
    now = _now_iso()

    # 应用更改到 Pydantic 模型
    bi = entity.basic_info
    ct = entity.compiled_truth

    if "database" in changes:
        bi.database = str(changes["database"])
    if "table" in changes:
        bi.tablename = str(changes["table"])
    elif "tablename" in changes:
        bi.tablename = str(changes["tablename"])
    if "update_frequency" in changes:
        bi.update_frequency = changes["update_frequency"]
    if "objective" in changes:
        ct.objective = str(changes["objective"])
    if "definition" in changes:
        ct.definition = str(changes["definition"])
    if "core_logic" in changes:
        ct.core_logic = str(changes["core_logic"])
    if "source_tables" in changes:
        st_list = changes["source_tables"]
        if isinstance(st_list, list):
            ct.source_tables = []
            for item in st_list:
                if isinstance(item, dict):
                    ct.source_tables.append(SourceTable(**item))
                elif isinstance(item, str):
                    ct.source_tables.append(SourceTable(full_name=item))
    if "columns" in changes:
        col_list = changes["columns"]
        if isinstance(col_list, list):
            ct.columns = []
            for item in col_list:
                if isinstance(item, dict):
                    ct.columns.append(TableColumn(**item))
                elif isinstance(item, str):
                    ct.columns.append(TableColumn(name=item, description=""))
    if "column_updates" in changes:
        col_updates = changes["column_updates"]
        if isinstance(col_updates, list):
            updates_map = {item["name"]: item for item in col_updates if isinstance(item, dict) and "name" in item}
            for col in ct.columns:
                if col.name in updates_map:
                    new_desc = updates_map[col.name].get("description")
                    if new_desc is not None:
                        col.description = new_desc
    if "sql" in changes:
        sql_val = changes["sql"]
        ct.sql = str(sql_val) if sql_val else None

    # 更新 timeline
    if timeline_desc:
        entity.timeline.append(TimelineEntry(time=now, content=timeline_desc))

    # 更新时间戳
    entity.updated_at = now
    if not entity.created_at:
        entity.created_at = now

    # 重新序列化为 markdown（包含 frontmatter）
    return table_entity_to_markdown(entity)
```

- [ ] **Step 7: 添加 Frontmatter 测试**

在 `TestTableModels` 类中添加：

```python
def test_table_entity_to_markdown_has_frontmatter(self):
    """Serialized markdown should include YAML frontmatter."""
    entity = TableEntity(
        basic_info=TableBasicInfo(database="ods", tablename="order_detail", update_frequency="daily"),
        compiled_truth=TableCompiledTruth(objective="原始业务表", definition="订单明细"),
        timeline=[TimelineEntry(time="2026-04-30 14:30:00", content="首次写入")],
        created_at="2026-04-30 14:30:00",
        updated_at="2026-04-30 14:30:00",
    )
    md = table_entity_to_markdown(entity)
    assert md.startswith("---")
    assert "\ntablename: order_detail\n" in md
    assert "\ndatabase: ods\n" in md
    assert "\nupdate_frequency: daily\n" in md
    assert "\nobjective: 原始业务表\n" in md

def test_markdown_to_table_entity_from_frontmatter(self):
    """Parser should read from frontmatter when present."""
    md = """---
tablename: order_detail
database: ods
update_frequency: daily
objective: 原始业务表
definition: 订单明细原始数据
core_logic: SELECT * FROM orders
created_at: "2026-04-30 14:30:00"
updated_at: "2026-05-01 10:00:00"
---

# order_detail

## Basic Info

- **database**: ods
- **table**: order_detail
- **update_frequency**: daily

## Compiled Truth

- **objective**: 原始业务表
- **definition**: 订单明细原始数据
- **core_logic**: SELECT * FROM orders

## Timeline

- **2026-04-30 14:30:00** — 首次写入
- **2026-05-01 10:00:00** — 更新了定义

---
*created: 2026-04-30 14:30:00*
*updated: 2026-05-01 10:00:00*
"""
    entity = markdown_to_table_entity(md)
    assert entity.basic_info.tablename == "order_detail"
    assert entity.basic_info.database == "ods"
    assert entity.compiled_truth.objective == "原始业务表"
    assert entity.created_at == "2026-04-30 14:30:00"
    assert len(entity.timeline) == 2

def test_roundtrip_with_frontmatter(self):
    """Entity → markdown → entity should preserve all data."""
    original = TableEntity(
        basic_info=TableBasicInfo(database="dwd", tablename="dim_user", update_frequency="daily"),
        compiled_truth=TableCompiledTruth(
            objective="用户维度表",
            definition="用户主数据",
            core_logic="SELECT * FROM users WHERE active = 1",
            source_tables=[SourceTable(full_name="ods.users")],
            columns=[TableColumn(name="user_id", description="用户ID"), TableColumn(name="name", description="用户名")],
        ),
        timeline=[TimelineEntry(time="2026-04-30 14:30:00", content="首次写入")],
        created_at="2026-04-30 14:30:00",
        updated_at="2026-04-30 14:30:00",
    )
    
    md = table_entity_to_markdown(original)
    parsed = markdown_to_table_entity(md)
    
    assert parsed.basic_info.database == "dwd"
    assert parsed.basic_info.tablename == "dim_user"
    assert parsed.compiled_truth.objective == "用户维度表"
    assert len(parsed.compiled_truth.source_tables) == 1
    assert parsed.compiled_truth.source_tables[0].full_name == "ods.users"
    assert len(parsed.compiled_truth.columns) == 2

def test_apply_partial_update_preserves_frontmatter(self):
    """Partial update should regenerate markdown with frontmatter."""
    existing = """---
tablename: test_table
database: ods
update_frequency: daily
objective: 原始表
definition: 初始定义
core_logic: 无
created_at: "2026-04-30 14:30:00"
updated_at: "2026-04-30 14:30:00"
---

# test_table

## Basic Info

- **database**: ods
- **table**: test_table
- **update_frequency**: daily

## Compiled Truth

- **objective**: 原始表
- **definition**: 初始定义
- **core_logic**: 无

## Timeline

- **2026-04-30 14:30:00** — 首次写入

---
*created: 2026-04-30 14:30:00*
*updated: 2026-04-30 14:30:00*
"""
    result = apply_partial_update(existing, {"definition": "更新后的定义"}, "更新了定义")
    assert result.startswith("---")
    assert "definition: 更新后的定义" in result
    assert "首次写入" in result
    assert "更新了定义" in result

def test_markdown_to_table_entity_legacy_without_frontmatter(self):
    """Legacy markdown without frontmatter should still parse correctly."""
    legacy_md = """# order_detail

## Basic Info

- **database**: ods
- **table**: order_detail
- **update_frequency**: daily

## Compiled Truth

- **objective**: 原始业务表
- **definition**: 订单明细

## Timeline

- **2026-04-30 14:30:00** — 首次写入
"""
    entity = markdown_to_table_entity(legacy_md)
    assert entity.basic_info.tablename == "order_detail"
    assert entity.basic_info.database == "ods"
    assert entity.compiled_truth.objective == "原始业务表"
```

- [ ] **Step 8: 运行所有测试验证**

```bash
cd backend && PYTHONPATH=. uv run pytest tests/test_structured_memory.py -v
```

Expected: ALL PASS

- [ ] **Step 9: 提交**

```bash
git add packages/harness/deerflow/structured_memory/models.py tests/test_structured_memory.py
git commit -m "feat(structured-memory): add YAML frontmatter for robust serialization"
```

---

## Task 5: 完整回归测试

- [ ] **Step 1: 运行完整测试套件**

```bash
cd backend && PYTHONPATH=. uv run pytest tests/test_structured_memory.py -v --tb=short
```

- [ ] **Step 2: 运行边界检查**

```bash
cd backend && PYTHONPATH=. uv run pytest tests/test_harness_boundary.py -v
```

Expected: PASS（确保没有新增 app.* 导入）

- [ ] **Step 3: 提交最终更改**

```bash
git status
git add -A
git commit -m "feat(structured-memory): race condition fixes, config hot reload, frontmatter serialization"
```

# Structured Memory 优化方案

**Date**: 2026-05-07
**Status**: Approved
**Changes**: P0竞态修复、Config热重载、Markdown frontmatter

---

## 1. P0 竞态条件修复

### 1.1 索引更新竞态 (`index_service._update_index`)

**问题**: 读-改-写操作非原子，并发写入同一索引文件会丢失数据。

**方案**: 使用 `fcntl.flock()` 文件锁保护整个读-改-写流程。

```python
import fcntl

def _update_index_with_lock(store, index_path, action, entry, target):
    resolved = store.resolve_path(index_path)
    lock_path = str(resolved) + ".lock"
    with open(lock_path, 'w') as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        try:
            # 原有的读-改-写逻辑
            ...
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
```

锁文件 `.lock` 与目标索引同目录，原子创建/删除。

### 1.2 写入流程竞态 (`write_memory_entity`)

**问题**: 读取旧内容 → merge → 写入 → 更新索引，非原子。

**方案**: 同理使用文件锁保护整个流程，锁粒度细化到单个实体文件。

```python
def write_memory_entity(path, content, changes="", timeline_desc=""):
    target = store.resolve_path(path)
    lock_path = str(target) + ".lock"
    with open(lock_path, 'w') as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        try:
            old_content = None
            if store.file_exists(path):
                old_content = store.read_file(path)
            # ... apply changes ...
            store.write_file(path, final_content)
            register_entity(...)
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
```

---

## 2. Config 热重载

**问题**: `StructuredMemoryConfig` 使用独立单例，配置变更后不生效。

**方案**: 删除独立单例，改为每次从 `AppConfig` 动态读取。

```python
# structured_memory_config.py
def get_structured_memory_config() -> StructuredMemoryConfig:
    from deerflow.config import get_app_config
    app_config = get_app_config()
    return getattr(app_config, 'structured_memory', StructuredMemoryConfig())

# 删除全局单例和相关函数
```

`AppConfig` 本身已有 mtime 检测自动重载逻辑，所以 `get_structured_memory_config()` 无需单独实现热重载。

---

## 3. Markdown 反序列化增强 (YAML Frontmatter)

**问题**: `markdown_to_table_entity()` 依赖脆弱的正则解析，格式变化易出错。

**方案**: 在实体文件头部添加 YAML frontmatter 存储所有结构化字段。

### 3.1 Frontmatter 格式

```markdown
---
tablename: order_detail
database: ods
update_frequency: daily
objective: 原始业务表
definition: 订单明细原始数据
core_logic: SELECT * FROM orders
created_at: "2026-04-30 14:30:00"
updated_at: "2026-05-07 10:00:00"
---
# order_detail

## Basic Info

- **database**: ods
- **table**: order_detail
...

## Compiled Truth
...
```

### 3.2 序列化 (`table_entity_to_markdown`)

```python
import yaml

def table_entity_to_markdown(entity: TableEntity) -> str:
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
    fm_yaml = yaml.dump(fm, allow_unicode=True, sort_keys=False).rstrip()

    # 2. 原有的 Markdown 渲染
    md = _render_markdown_body(entity)

    return f"---\n{fm_yaml}\n---\n\n{md}"
```

### 3.3 反序列化 (`markdown_to_table_entity`)

```python
def markdown_to_table_entity(content: str) -> TableEntity:
    # 1. 优先从 frontmatter 解析
    if content.startswith("---"):
        parts = content.split("---", 2)
        if len(parts) >= 3:
            fm = yaml.safe_load(parts[1])
            body = parts[2].strip()
            # 用 fm 数据填充模型
            return _parse_from_frontmatter(fm, body)

    # 2. 回退到正则解析（向后兼容）
    return _parse_legacy(content)
```

### 3.4 依赖

添加 `pyyaml` 依赖（项目可能已有）。

---

## 文件修改清单

| 文件 | 操作 |
|------|------|
| `packages/harness/deerflow/structured_memory/index_service.py` | 添加文件锁 |
| `packages/harness/deerflow/tools/builtins/structured_memory_tools.py` | 添加文件锁 |
| `packages/harness/deerflow/structured_memory/models.py` | 添加 frontmatter 序列化/反序列化 |
| `packages/harness/deerflow/config/structured_memory_config.py` | 删除单例，改为动态读取 |
| `packages/harness/deerflow/config/app_config.py` | 确认 structured_memory 字段已注册 |
| `tests/test_structured_memory.py` | 添加竞态、frontmatter 测试用例 |

---

## 验证

```bash
cd backend && PYTHONPATH=. uv run pytest tests/test_structured_memory.py -v
```

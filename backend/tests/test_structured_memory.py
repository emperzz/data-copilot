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
    BUSINESS_INDEX_TEMPLATE,
    FACTS_INDEX_TEMPLATE,
    SCHEMA_INDEX_TEMPLATE,
    TABLE_DETAIL_TEMPLATE,
    TASKS_INDEX_TEMPLATE,
    TASK_SUMMARY_TEMPLATE,
)


def _patch_store_root(store: StructuredMemoryStore, root: Path):
    """Helper to patch a store's root property with a temp directory."""
    return patch.object(StructuredMemoryStore, "root", property(lambda self: root))


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
        # Reset for other tests
        set_structured_memory_config(StructuredMemoryConfig())

    def test_set_config_singleton(self):
        custom = StructuredMemoryConfig(enabled=True, max_index_tokens=3000)
        set_structured_memory_config(custom)
        assert get_structured_memory_config().max_index_tokens == 3000
        set_structured_memory_config(StructuredMemoryConfig())


class TestStructuredMemoryStore:
    """Tests for StructuredMemoryStore file I/O."""

    def test_resolve_path_rejects_traversal(self, tmp_path):
        store = StructuredMemoryStore()
        with _patch_store_root(store, tmp_path):
            with pytest.raises(ValueError, match="traversal"):
                store.resolve_path("../etc/passwd")

    def test_write_and_read_file(self, tmp_path):
        store = StructuredMemoryStore()
        with _patch_store_root(store, tmp_path):
            store.ensure_directories()
            store.write_file("facts/index.md", "# Test Index\n\n- [test](test.md) — desc")
            content = store.read_file("facts/index.md")
            assert "# Test Index" in content

    def test_write_file_creates_parent_dirs(self, tmp_path):
        store = StructuredMemoryStore()
        with _patch_store_root(store, tmp_path):
            store.write_file("a/b/c/test.md", "hello")
            assert store.read_file("a/b/c/test.md") == "hello"

    def test_file_exists(self, tmp_path):
        store = StructuredMemoryStore()
        with _patch_store_root(store, tmp_path):
            assert not store.file_exists("nope.md")
            store.write_file("nope.md", "exists")
            assert store.file_exists("nope.md")

    def test_list_dir_tree_format(self, tmp_path):
        store = StructuredMemoryStore()
        with _patch_store_root(store, tmp_path):
            store.ensure_directories()
            store.write_file("facts/index.md", "# idx")
            store.write_file("tasks/index.md", "# idx")
            output = store.list_dir(depth=2)
            assert "structured_memory/" in output
            assert "facts/" in output
            assert "tasks/" in output
            assert "index.md" in output

    def test_list_dir_respects_depth(self, tmp_path):
        store = StructuredMemoryStore()
        with _patch_store_root(store, tmp_path):
            store.write_file("facts/schema/tables/deep.md", "deep")
            output = store.list_dir(depth=1)
            assert "schema/" not in output

    def test_read_nonexistent_file_raises(self, tmp_path):
        store = StructuredMemoryStore()
        with _patch_store_root(store, tmp_path):
            with pytest.raises(FileNotFoundError):
                store.read_file("does_not_exist.md")

    def test_ensure_directories_creates_structure(self, tmp_path):
        store = StructuredMemoryStore()
        with _patch_store_root(store, tmp_path):
            store.ensure_directories()
            assert (tmp_path / "facts" / "schema" / "tables").exists()
            assert (tmp_path / "facts" / "business").exists()
            assert (tmp_path / "tasks").exists()


class TestSearchMemoryFiles:
    """Tests for text search across memory files."""

    def test_search_finds_matches_in_facts(self, tmp_path):
        store = StructuredMemoryStore()
        with _patch_store_root(store, tmp_path):
            store.write_file("facts/schema/tables/ods_order.md", "订单明细表\n包含所有订单数据")
            store.write_file("facts/business/revenue.md", "收入确认规则")
            results = search_memory_files("订单", category="facts")
            assert "ods_order.md" in results

    def test_search_finds_matches_in_tasks(self, tmp_path):
        store = StructuredMemoryStore()
        with _patch_store_root(store, tmp_path):
            store.write_file("tasks/2026/sales-analysis.md", "Q1 销售分析\n使用 SQL 查询")
            results = search_memory_files("SQL", category="tasks")
            assert "sales-analysis.md" in results

    def test_search_all_categories(self, tmp_path):
        store = StructuredMemoryStore()
        with _patch_store_root(store, tmp_path):
            store.write_file("facts/schema/index.md", "common pattern xyz")
            store.write_file("tasks/2026/task.md", "another xyz occurrence")
            results = search_memory_files("xyz", category="all")
            assert "schema/index.md" in results
            assert "task.md" in results

    def test_search_no_matches(self, tmp_path):
        store = StructuredMemoryStore()
        with _patch_store_root(store, tmp_path):
            store.write_file("facts/index.md", "some content")
            results = search_memory_files("nonexistent")
            assert "No matches found" in results

    def test_search_empty_store(self, tmp_path):
        store = StructuredMemoryStore()
        with _patch_store_root(store, tmp_path):
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

    def test_schema_index_template_exists(self):
        assert "{entries}" in SCHEMA_INDEX_TEMPLATE
        assert "{last_updated}" in SCHEMA_INDEX_TEMPLATE

    def test_business_index_template_exists(self):
        assert "{entries}" in BUSINESS_INDEX_TEMPLATE
        assert "{last_updated}" in BUSINESS_INDEX_TEMPLATE


class TestStructuredMemoryTools:
    """Tests for the 6 built-in tools."""

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

    def test_write_memory_entity_tool_exists(self):
        from deerflow.tools.builtins.structured_memory_tools import write_memory_entity
        assert write_memory_entity.name == "write_memory_entity"

    def test_delete_memory_entity_tool_exists(self):
        from deerflow.tools.builtins.structured_memory_tools import delete_memory_entity
        assert delete_memory_entity.name == "delete_memory_entity"

    def test_get_memory_entity_returns_not_found_for_missing_file(self, tmp_path):
        from deerflow.tools.builtins.structured_memory_tools import get_memory_entity

        store = StructuredMemoryStore()
        with _patch_store_root(store, tmp_path):
            result = get_memory_entity.invoke({"path": "nonexistent.md"})
            assert "not found" in result.lower()

    def test_list_memory_entities_shows_directory(self, tmp_path):
        from deerflow.tools.builtins.structured_memory_tools import list_memory_entities

        store = StructuredMemoryStore()
        with _patch_store_root(store, tmp_path):
            store.write_file("facts/index.md", "# idx")
            result = list_memory_entities.invoke({"path": "", "depth": 2})
            assert "facts/" in result
            assert "index.md" in result

    def test_update_memory_index_add_entry(self, tmp_path):
        from deerflow.tools.builtins.structured_memory_tools import update_memory_index

        store = StructuredMemoryStore()
        with _patch_store_root(store, tmp_path):
            store.write_file("facts/schema/index.md", "# Schema Index\n\n")

            result = update_memory_index.invoke({
                "index_path": "facts/schema/index.md",
                "action": "add",
                "entry": "- [ods.order](tables/ods_order.md) — 订单表",
            })
            assert "Added" in result

            content = store.read_file("facts/schema/index.md")
            assert "ods.order" in content

    def test_update_memory_index_remove_entry(self, tmp_path):
        from deerflow.tools.builtins.structured_memory_tools import update_memory_index

        store = StructuredMemoryStore()
        with _patch_store_root(store, tmp_path):
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

    def test_update_memory_index_update_entry(self, tmp_path):
        from deerflow.tools.builtins.structured_memory_tools import update_memory_index

        store = StructuredMemoryStore()
        with _patch_store_root(store, tmp_path):
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

    def test_update_memory_index_duplicate_prevention(self, tmp_path):
        from deerflow.tools.builtins.structured_memory_tools import update_memory_index

        store = StructuredMemoryStore()
        with _patch_store_root(store, tmp_path):
            store.write_file("facts/index.md", "# Facts\n- [X](x.md) — desc\n")

            result = update_memory_index.invoke({
                "index_path": "facts/index.md",
                "action": "add",
                "entry": "- [X](x.md) — desc",
            })
            assert "already exists" in result.lower()

    def test_write_memory_entity_creates_file(self, tmp_path):
        from deerflow.tools.builtins.structured_memory_tools import write_memory_entity

        store = StructuredMemoryStore()
        with _patch_store_root(store, tmp_path):
            content = "# ods_order\n\n## Info\n- db: ods"
            result = write_memory_entity.invoke({
                "path": "facts/schema/tables/ods_order.md",
                "content": content,
            })
            assert "written" in result.lower()
            assert store.read_file("facts/schema/tables/ods_order.md") == content

    def test_write_memory_entity_overwrites_existing(self, tmp_path):
        from deerflow.tools.builtins.structured_memory_tools import write_memory_entity

        store = StructuredMemoryStore()
        with _patch_store_root(store, tmp_path):
            store.write_file("facts/schema/tables/ods_order.md", "old content")
            result = write_memory_entity.invoke({
                "path": "facts/schema/tables/ods_order.md",
                "content": "new content",
            })
            assert "written" in result.lower()
            assert store.read_file("facts/schema/tables/ods_order.md") == "new content"

    def test_write_memory_entity_rejects_traversal(self, tmp_path):
        from deerflow.tools.builtins.structured_memory_tools import write_memory_entity

        store = StructuredMemoryStore()
        with _patch_store_root(store, tmp_path):
            result = write_memory_entity.invoke({
                "path": "../etc/passwd",
                "content": "malicious",
            })
            assert "invalid path" in result.lower()

    def test_delete_memory_entity_removes_file(self, tmp_path):
        from deerflow.tools.builtins.structured_memory_tools import delete_memory_entity

        store = StructuredMemoryStore()
        with _patch_store_root(store, tmp_path):
            store.write_file("facts/schema/tables/ods_order.md", "content")
            assert store.file_exists("facts/schema/tables/ods_order.md")

            result = delete_memory_entity.invoke({
                "path": "facts/schema/tables/ods_order.md",
            })
            assert "deleted" in result.lower()
            assert not store.file_exists("facts/schema/tables/ods_order.md")

    def test_delete_memory_entity_not_found(self, tmp_path):
        from deerflow.tools.builtins.structured_memory_tools import delete_memory_entity

        store = StructuredMemoryStore()
        with _patch_store_root(store, tmp_path):
            result = delete_memory_entity.invoke({
                "path": "nonexistent.md",
            })
            assert "not found" in result.lower()


class TestIndexService:
    """Tests for the auto-indexing index_service module."""

    def test_get_index_path_tables(self):
        from deerflow.structured_memory.index_service import get_index_path

        assert get_index_path("facts/schema/tables/ods_order.md") == "facts/schema/index.md"
        assert get_index_path("facts/schema/tables/dwd_trade.md") == "facts/schema/index.md"

    def test_get_index_path_fields(self):
        from deerflow.structured_memory.index_service import get_index_path

        assert get_index_path("facts/schema/fields/common_metrics.md") == "facts/schema/index.md"

    def test_get_index_path_business(self):
        from deerflow.structured_memory.index_service import get_index_path

        assert get_index_path("facts/business/revenue_def.md") == "facts/business/index.md"

    def test_get_index_path_tasks(self):
        from deerflow.structured_memory.index_service import get_index_path

        assert get_index_path("tasks/2026/sales-q1.md") == "tasks/index.md"

    def test_get_index_path_no_auto_index(self):
        from deerflow.structured_memory.index_service import get_index_path

        assert get_index_path("facts/index.md") is None
        assert get_index_path("facts/schema/index.md") is None
        assert get_index_path("README.md") is None

    def test_parse_entity_entry_table(self):
        from deerflow.structured_memory.index_service import parse_entity_entry

        content = """# ods_order

## 基本信息
- **库**: ods
- **表**: order
- **描述**: 订单明细表
"""
        title, desc = parse_entity_entry(content)
        assert title == "ods_order"
        assert desc == "订单明细表"

    def test_parse_entity_entry_task(self):
        from deerflow.structured_memory.index_service import parse_entity_entry

        content = """# Q1 Sales Analysis

- **日期**: 2026-03-15
- **类型**: analysis

## 摘要
分析了第一季度销售数据
"""
        title, desc = parse_entity_entry(content)
        assert title == "Q1 Sales Analysis"
        # Falls back to first non-heading line

    def test_parse_entity_entry_no_title(self):
        from deerflow.structured_memory.index_service import parse_entity_entry

        title, desc = parse_entity_entry("No heading here")
        assert title == ""
        assert desc == ""

    def test_build_index_entry_table(self):
        from deerflow.structured_memory.index_service import build_index_entry

        entry = build_index_entry(
            "facts/schema/tables/ods_order.md",
            "ods_order",
            "订单明细表",
        )
        assert entry == "- [ods_order](tables/ods_order.md) — 订单明细表"

    def test_build_index_entry_field(self):
        from deerflow.structured_memory.index_service import build_index_entry

        entry = build_index_entry(
            "facts/schema/fields/common_metrics.md",
            "common_metrics",
            "通用指标定义",
        )
        assert entry == "- [common_metrics](fields/common_metrics.md) — 通用指标定义"

    def test_build_index_entry_business(self):
        from deerflow.structured_memory.index_service import build_index_entry

        entry = build_index_entry(
            "facts/business/revenue_def.md",
            "收入确认",
            "收入确认口径定义",
        )
        assert entry == "- [收入确认](revenue_def.md) — 收入确认口径定义"

    def test_build_index_entry_task(self):
        from deerflow.structured_memory.index_service import build_index_entry

        entry = build_index_entry(
            "tasks/2026/sales-q1.md",
            "Q1 Sales Analysis",
            "第一季度销售分析报告",
        )
        assert entry == "- [Q1 Sales Analysis](2026/sales-q1.md) — 第一季度销售分析报告"

    def test_register_entity_creates_index_entry(self, tmp_path):
        from deerflow.structured_memory.index_service import register_entity
        from deerflow.structured_memory.storage import StructuredMemoryStore

        store = StructuredMemoryStore()
        with _patch_store_root(store, tmp_path):
            store.ensure_directories()
            content = """# ods_order

## 基本信息
- **描述**: 订单明细表
"""
            msg = register_entity(store, "facts/schema/tables/ods_order.md", None, content)
            assert "ods_order" in msg
            index_content = store.read_file("facts/schema/index.md")
            assert "ods_order" in index_content

    def test_register_entity_update_removes_old_entry(self, tmp_path):
        from deerflow.structured_memory.index_service import register_entity
        from deerflow.structured_memory.storage import StructuredMemoryStore

        store = StructuredMemoryStore()
        with _patch_store_root(store, tmp_path):
            store.ensure_directories()
            old_content = """# ods_order

## 基本信息
- **描述**: 旧描述
"""
            new_content = """# ods_order

## 基本信息
- **描述**: 新描述
"""
            # First: create
            register_entity(store, "facts/schema/tables/ods_order.md", None, old_content)
            # Second: update
            msg = register_entity(store, "facts/schema/tables/ods_order.md", old_content, new_content)
            index_content = store.read_file("facts/schema/index.md")
            assert "新描述" in index_content
            assert "旧描述" not in index_content

    def test_register_entity_no_index_for_non_entity(self, tmp_path):
        from deerflow.structured_memory.index_service import register_entity
        from deerflow.structured_memory.storage import StructuredMemoryStore

        store = StructuredMemoryStore()
        with _patch_store_root(store, tmp_path):
            msg = register_entity(store, "facts/index.md", None, "# Facts Index\n")
            assert "No auto-indexing" in msg

    def test_unregister_entity_removes_entry(self, tmp_path):
        from deerflow.structured_memory.index_service import register_entity, unregister_entity
        from deerflow.structured_memory.storage import StructuredMemoryStore

        store = StructuredMemoryStore()
        with _patch_store_root(store, tmp_path):
            store.ensure_directories()
            content = """# ods_order

## 基本信息
- **描述**: 订单明细表
"""
            register_entity(store, "facts/schema/tables/ods_order.md", None, content)
            msg = unregister_entity(store, "facts/schema/tables/ods_order.md", content)
            assert "Removed" in msg
            index_content = store.read_file("facts/schema/index.md")
            assert "ods_order" not in index_content

    def test_write_memory_entity_auto_indexes(self, tmp_path):
        from deerflow.tools.builtins.structured_memory_tools import write_memory_entity

        store = StructuredMemoryStore()
        with _patch_store_root(store, tmp_path):
            store.ensure_directories()
            content = """# ods_order

## 基本信息
- **描述**: 订单明细表
"""
            result = write_memory_entity.invoke({
                "path": "facts/schema/tables/ods_order.md",
                "content": content,
            })
            assert "written" in result.lower()
            assert "index" in result.lower()
            assert store.file_exists("facts/schema/tables/ods_order.md")
            index_content = store.read_file("facts/schema/index.md")
            assert "ods_order" in index_content

    def test_delete_memory_entity_auto_unindexes(self, tmp_path):
        from deerflow.tools.builtins.structured_memory_tools import delete_memory_entity, write_memory_entity

        store = StructuredMemoryStore()
        with _patch_store_root(store, tmp_path):
            store.ensure_directories()
            content = """# ods_order

## 基本信息
- **描述**: 订单明细表
"""
            write_memory_entity.invoke({
                "path": "facts/schema/tables/ods_order.md",
                "content": content,
            })
            result = delete_memory_entity.invoke({
                "path": "facts/schema/tables/ods_order.md",
            })
            assert "deleted" in result.lower()
            assert "index" in result.lower()
            assert not store.file_exists("facts/schema/tables/ods_order.md")
            index_content = store.read_file("facts/schema/index.md")
            assert "ods_order" not in index_content

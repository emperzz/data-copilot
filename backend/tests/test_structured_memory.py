"""Tests for enterprise structured memory system."""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

from deerflow.config.structured_memory_config import (
    StructuredMemoryConfig,
    get_structured_memory_config,
)
from deerflow.structured_memory.models import (
    SourceTable,
    TableBasicInfo,
    TableColumn,
    TableCompiledTruth,
    TableEntity,
    TimelineEntry,
    apply_partial_update,
    markdown_to_table_entity,
    parse_changes_json,
    table_entity_to_markdown,
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


# ── New: Table Model Tests ───────────────────────────────────────────────────


class TestTableModels:
    """Tests for Pydantic models and serialization helpers."""

    def test_table_column_requires_nonempty_name(self):
        with pytest.raises(ValidationError):
            TableColumn(name="", description="some desc")

    def test_table_column_description_allows_empty(self):
        """Description is optional (agent should confirm, but model allows empty)."""
        col = TableColumn(name="col1", description="")
        assert col.description == ""

    def test_table_basic_info_requires_tablename(self):
        with pytest.raises(ValidationError):
            TableBasicInfo(database="db", tablename="", update_frequency="daily")

    def test_table_basic_info_valid_frequency_only(self):
        with pytest.raises(ValidationError):
            TableBasicInfo(database="db", tablename="t", update_frequency="bogus")

    def test_table_basic_info_accepts_all_valid_frequencies(self):
        valid = {"daily", "hourly", "weekly", "monthly", "yearly", "realtime", "onetime"}
        for freq in valid:
            info = TableBasicInfo(database="db", tablename="t", update_frequency=freq)
            assert info.update_frequency == freq

    def test_table_compiled_truth_defaults(self):
        ct = TableCompiledTruth(objective="o", definition="d", core_logic="c")
        assert ct.source_tables == []
        assert ct.columns == []
        assert ct.sql is None

    def test_table_entity_extra_field_rejected(self):
        with pytest.raises(ValidationError):
            TableEntity(
                basic_info=TableBasicInfo(database="db", tablename="t", update_frequency="daily"),
                compiled_truth=TableCompiledTruth(objective="o", definition="d", core_logic="c"),
                extra_field="nope",  # type: ignore[call-arg]
            )

    def test_timeline_entry_requires_time_and_content(self):
        with pytest.raises(ValidationError):
            TimelineEntry(time="", content="desc")
        with pytest.raises(ValidationError):
            TimelineEntry(time="2026-01-01", content="")

    def test_source_table_full_name_required(self):
        with pytest.raises(ValidationError):
            SourceTable(full_name="")

    def test_source_table_memory_link_optional(self):
        st = SourceTable(full_name="db.table1")
        assert st.memory_link is None

    # ── Roundtrip ────────────────────────────────────────────────────────

    def test_table_entity_full_roundtrip(self):
        entity = TableEntity(
            basic_info=TableBasicInfo(database="ods", tablename="order_detail", update_frequency="daily"),
            compiled_truth=TableCompiledTruth(
                objective="原始业务表",
                definition="从业务系统同步的订单明细数据，包含下单、支付、发货全流程",
                core_logic="SELECT * FROM source_order WHERE status != 'deleted'",
                source_tables=[
                    SourceTable(full_name="mysql.order", memory_link="tables/mysql_order.md"),
                    SourceTable(full_name="mysql.payment"),
                ],
                columns=[
                    TableColumn(name="order_id", description="订单唯一标识"),
                    TableColumn(name="amount", description="支付金额，单位为分"),
                ],
                sql="CREATE TABLE ods.order_detail AS SELECT ...",
            ),
            timeline=[TimelineEntry(time="2026-04-30 14:30:00", content="首次写入")],
            created_at="2026-04-30 14:30:00",
            updated_at="2026-04-30 14:30:00",
        )

        md = table_entity_to_markdown(entity)
        parsed = markdown_to_table_entity(md)

        assert parsed.basic_info.database == "ods"
        assert parsed.basic_info.tablename == "order_detail"
        assert parsed.basic_info.update_frequency == "daily"
        assert parsed.compiled_truth.objective == "原始业务表"
        assert parsed.compiled_truth.definition == "从业务系统同步的订单明细数据，包含下单、支付、发货全流程"
        assert parsed.compiled_truth.core_logic == "SELECT * FROM source_order WHERE status != 'deleted'"
        assert len(parsed.compiled_truth.source_tables) == 2
        assert parsed.compiled_truth.source_tables[0].full_name == "mysql.order"
        assert parsed.compiled_truth.source_tables[0].memory_link == "tables/mysql_order.md"
        assert parsed.compiled_truth.source_tables[1].full_name == "mysql.payment"
        assert parsed.compiled_truth.source_tables[1].memory_link is None
        assert len(parsed.compiled_truth.columns) == 2
        assert parsed.compiled_truth.columns[0].name == "order_id"
        assert parsed.compiled_truth.sql == "CREATE TABLE ods.order_detail AS SELECT ..."
        assert len(parsed.timeline) == 1
        assert parsed.timeline[0].time == "2026-04-30 14:30:00"
        assert parsed.timeline[0].content == "首次写入"
        assert parsed.created_at == "2026-04-30 14:30:00"

    def test_table_entity_roundtrip_minimal(self):
        """Roundtrip with minimal required fields."""
        entity = TableEntity(
            basic_info=TableBasicInfo(database="dw", tablename="dim_store", update_frequency="daily"),
            compiled_truth=TableCompiledTruth(objective="维度表", definition="门店维度"),
        )
        md = table_entity_to_markdown(entity)
        parsed = markdown_to_table_entity(md)
        assert parsed.basic_info.tablename == "dim_store"
        assert parsed.compiled_truth.objective == "维度表"

    def test_table_entity_roundtrip_empty_sql(self):
        """Roundtrip when sql is None (business/source table)."""
        entity = TableEntity(
            basic_info=TableBasicInfo(database="db", tablename="source_table", update_frequency="realtime"),
            compiled_truth=TableCompiledTruth(
                objective="原始业务表",
                definition="直接来自业务系统",
                core_logic="无加工逻辑",
                sql=None,
            ),
        )
        md = table_entity_to_markdown(entity)
        parsed = markdown_to_table_entity(md)
        assert parsed.compiled_truth.sql is None

    # ── Partial Update ───────────────────────────────────────────────────

    _SAMPLE_ENTITY = """# order_detail

## Basic Info

- **database**: ods
- **table**: order_detail
- **update_frequency**: daily

## Compiled Truth

- **objective**: 原始业务表
- **definition**: 订单明细原始数据
- **core_logic**: SELECT * FROM orders

### upstream dependencies

- mysql.order
- mysql.payment
  - [link](tables/mysql_payment.md)

### columns

| column | description |
|--------|-------------|
| order_id | 订单ID |
| amount | 金额 |

### SQL
```sql
SELECT * FROM orders
```

## Timeline

- **2026-04-30 14:30:00** — 首次写入

---
*created: 2026-04-30 14:30:00*
*updated: 2026-04-30 14:30:00*
"""

    def test_apply_partial_update_only_changes_target_field(self):
        result = apply_partial_update(
            self._SAMPLE_ENTITY,
            {"definition": "更新后的定义"},
            "更新了定义",
        )
        assert "**definition**: 更新后的定义" in result
        assert "**objective**: 原始业务表" in result  # unchanged
        assert "**database**: ods" in result  # unchanged
        assert "**update_frequency**: daily" in result  # unchanged

    def test_apply_partial_update_preserves_unchanged_fields(self):
        result = apply_partial_update(
            self._SAMPLE_ENTITY,
            {"objective": "聚合宽表"},
            "更新了目的",
        )
        assert "**objective**: 聚合宽表" in result
        assert "**definition**: 订单明细原始数据" in result  # preserved
        assert "**core_logic**: SELECT * FROM orders" in result  # preserved

    def test_apply_partial_update_appends_timeline_entry(self):
        result = apply_partial_update(
            self._SAMPLE_ENTITY,
            {"definition": "新定义"},
            "更新了定义",
        )
        # Old timeline entry preserved
        assert "首次写入" in result
        # New timeline entry appended
        assert "更新了定义" in result
        # Old timeline entry still present
        assert "2026-04-30 14:30:00" in result

    def test_apply_partial_update_does_not_duplicate_timeline(self):
        result = apply_partial_update(
            self._SAMPLE_ENTITY,
            {"definition": "新定义"},
            "更新了定义",
        )
        # '首次写入' should appear exactly once
        assert result.count("首次写入") == 1

    def test_apply_partial_update_updates_timestamp(self):
        result = apply_partial_update(
            self._SAMPLE_ENTITY,
            {"definition": "改"},
            "更新",
        )
        assert "*updated:" in result
        # The new timestamp should not be the old one
        assert "2026-04-30 14:30:00" in result  # still in timeline
        # But the footer update should be newer (we can't assert exact value, just presence)
        assert result.count("*updated:") >= 1

    def test_apply_partial_update_multiple_simple_fields(self):
        result = apply_partial_update(
            self._SAMPLE_ENTITY,
            {"database": "dwd", "table": "order_agg", "update_frequency": "hourly"},
            "更新了基本信息",
        )
        assert "**database**: dwd" in result
        assert "**table**: order_agg" in result  # label key
        assert "**update_frequency**: hourly" in result

    def test_apply_partial_update_source_tables(self):
        result = apply_partial_update(
            self._SAMPLE_ENTITY,
            {"source_tables": [{"full_name": "new.source"}, {"full_name": "another.src", "memory_link": "tables/another.md"}]},
            "更新了上游依赖",
        )
        assert "new.source" in result
        assert "another.src" in result
        assert "[link](tables/another.md)" in result
        assert "mysql.order" not in result

    def test_apply_partial_update_columns(self):
        result = apply_partial_update(
            self._SAMPLE_ENTITY,
            {"columns": [{"name": "new_col", "description": "新字段描述"}]},
            "更新了字段",
        )
        assert "new_col" in result
        assert "新字段描述" in result
        assert "order_id" not in result

    def test_apply_partial_update_column_updates_merge(self):
        """column_updates merges by name, preserving other columns."""
        result = apply_partial_update(
            self._SAMPLE_ENTITY,
            {"column_updates": [{"name": "order_id", "description": "新的订单ID描述"}]},
            "更新了 order_id 字段说明",
        )
        # order_id updated
        assert "order_id" in result
        assert "新的订单ID描述" in result
        # amount preserved
        assert "amount" in result
        assert "金额" in result

    def test_apply_partial_update_sql(self):
        result = apply_partial_update(
            self._SAMPLE_ENTITY,
            {"sql": "SELECT * FROM new_source"},
            "更新了SQL",
        )
        assert "SELECT * FROM new_source" in result
        # The old SQL in the sql block should be gone
        sql_section = result.split("```sql")[1].split("```")[0] if "```sql" in result else ""
        assert "SELECT * FROM orders" not in sql_section

    def test_apply_partial_update_no_changes_dict(self):
        """Empty changes dict returns unchanged content."""
        result = apply_partial_update(self._SAMPLE_ENTITY, {}, "")
        assert result == self._SAMPLE_ENTITY

    def test_parse_changes_json_valid(self):
        result = parse_changes_json('{"definition": "new", "objective": "new obj"}')
        assert result == {"definition": "new", "objective": "new obj"}

    def test_parse_changes_json_empty(self):
        assert parse_changes_json("") == {}

    def test_parse_changes_json_invalid(self):
        assert parse_changes_json("not json") == {}
        assert parse_changes_json("") == {}

    def test_parse_changes_json_not_dict(self):
        assert parse_changes_json("[1, 2, 3]") == {}

    def test_markdown_to_table_entity_handles_empty(self):
        parsed = markdown_to_table_entity("")
        assert parsed.basic_info.tablename == "(unknown)"


# ── Original Test Classes (updated where needed) ────────────────────────────


class TestStructuredMemoryConfig:
    """Tests for StructuredMemoryConfig model and singleton."""

    def test_default_config_is_disabled(self):
        config = StructuredMemoryConfig()
        assert config.enabled is False
        assert config.injection_enabled is True
        assert config.max_index_tokens == 1500
        assert config.storage_path == ""

    def test_get_config_from_app_config_with_custom_values(self):
        """get_structured_memory_config reads from AppConfig.structured_memory."""
        mock_app_config = MagicMock()
        mock_app_config.structured_memory = StructuredMemoryConfig(
            enabled=True,
            storage_path="/custom/path",
            injection_enabled=False,
            max_index_tokens=500,
        )
        with patch("deerflow.config.get_app_config", return_value=mock_app_config):
            config = get_structured_memory_config()
            assert config.enabled is True
            assert config.storage_path == "/custom/path"
            assert config.injection_enabled is False
            assert config.max_index_tokens == 500

    def test_get_config_from_app_config_returns_default_when_missing(self):
        """Returns default StructuredMemoryConfig when AppConfig has no structured_memory attr."""
        mock_app_config = MagicMock(spec=[])  # spec=[] means no attributes allowed
        with patch("deerflow.config.get_app_config", return_value=mock_app_config):
            config = get_structured_memory_config()
            assert config.enabled is False


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

    def test_table_detail_template_has_all_new_fields(self):
        """New 3-layer template has 基本信息 + Compiled Truth + Timeline fields."""
        assert "{database}" in TABLE_DETAIL_TEMPLATE
        assert "{tablename}" in TABLE_DETAIL_TEMPLATE
        assert "{update_frequency}" in TABLE_DETAIL_TEMPLATE
        assert "{objective}" in TABLE_DETAIL_TEMPLATE
        assert "{definition}" in TABLE_DETAIL_TEMPLATE
        assert "{core_logic}" in TABLE_DETAIL_TEMPLATE
        assert "{source_tables}" in TABLE_DETAIL_TEMPLATE
        assert "{columns}" in TABLE_DETAIL_TEMPLATE
        assert "{sql}" in TABLE_DETAIL_TEMPLATE
        assert "{timeline_entries}" in TABLE_DETAIL_TEMPLATE
        assert "{created_at}" in TABLE_DETAIL_TEMPLATE
        assert "{updated_at}" in TABLE_DETAIL_TEMPLATE

    def test_table_detail_template_does_not_have_legacy_fields(self):
        """Legacy fields from old template should not be present."""
        assert "{table_name}" not in TABLE_DETAIL_TEMPLATE
        assert "{layer}" not in TABLE_DETAIL_TEMPLATE
        assert "{description}" not in TABLE_DETAIL_TEMPLATE
        assert "{fields}" not in TABLE_DETAIL_TEMPLATE

    def test_table_detail_template_has_three_sections(self):
        """Rendered output contains three layer headers."""
        rendered = TABLE_DETAIL_TEMPLATE.format(
            database="ods",
            tablename="test_table",
            update_frequency="daily",
            objective="测试目的",
            definition="测试定义",
            core_logic="测试逻辑",
            source_tables="- source.tbl",
            columns="| col1 | desc |",
            sql="SELECT 1",
            timeline_entries="- **2026-04-30** — 首次写入",
            created_at="2026-04-30",
            updated_at="2026-04-30",
        )
        assert "# test_table" in rendered
        assert "## Basic Info" in rendered
        assert "## Compiled Truth" in rendered
        assert "## Timeline" in rendered

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
            content = "# 基本信息\n\n- db: ods"
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

    # ── New: Partial Update Tests ────────────────────────────────────────

    _SAMPLE_NEW_ENTITY = """# test_table

## Basic Info

- **database**: ods
- **table**: test_table
- **update_frequency**: daily

## Compiled Truth

- **objective**: 原始业务表
- **definition**: 初始定义
- **core_logic**: 初始逻辑

### upstream dependencies

- upstream.source

### columns

| column | description |
|--------|-------------|
| col1 | 字段1 |

### SQL
```sql

```

## Timeline

- **2026-04-30 14:30:00** — 首次写入

---
*created: 2026-04-30 14:30:00*
*updated: 2026-04-30 14:30:00*
"""

    def test_write_memory_entity_partial_update_preserves_unchanged(self, tmp_path):
        from deerflow.tools.builtins.structured_memory_tools import write_memory_entity

        store = StructuredMemoryStore()
        with _patch_store_root(store, tmp_path):
            # Write initial entity
            write_memory_entity.invoke({
                "path": "facts/schema/tables/test.md",
                "content": self._SAMPLE_NEW_ENTITY,
            })

            # Partial update: only change definition
            result = write_memory_entity.invoke({
                "path": "facts/schema/tables/test.md",
                "content": "",
                "changes": '{"definition": "更新后的定义"}',
                "timeline_desc": "更新了定义",
            })
            assert "written" in result.lower()

            content = store.read_file("facts/schema/tables/test.md")
            # Changed field
            assert "更新后的定义" in content
            # Unchanged fields preserved
            assert "**objective**: 原始业务表" in content
            assert "**database**: ods" in content
            assert "**table**: test_table" in content
            assert "**update_frequency**: daily" in content
            assert "**core_logic**: 初始逻辑" in content

    def test_write_memory_entity_partial_update_appends_timeline(self, tmp_path):
        from deerflow.tools.builtins.structured_memory_tools import write_memory_entity

        store = StructuredMemoryStore()
        with _patch_store_root(store, tmp_path):
            write_memory_entity.invoke({
                "path": "facts/schema/tables/test.md",
                "content": self._SAMPLE_NEW_ENTITY,
            })

            write_memory_entity.invoke({
                "path": "facts/schema/tables/test.md",
                "content": "",
                "changes": '{"core_logic": "新逻辑"}',
                "timeline_desc": "更新了核心逻辑",
            })

            content = store.read_file("facts/schema/tables/test.md")
            # Old timeline entry preserved
            assert "首次写入" in content
            # New timeline entry added
            assert "更新了核心逻辑" in content
            # Both timeline entries exist (count check)
            assert content.count("首次写入") == 1

    def test_write_memory_entity_full_overwrite_when_no_changes(self, tmp_path):
        from deerflow.tools.builtins.structured_memory_tools import write_memory_entity

        store = StructuredMemoryStore()
        with _patch_store_root(store, tmp_path):
            write_memory_entity.invoke({
                "path": "facts/schema/tables/test.md",
                "content": self._SAMPLE_NEW_ENTITY,
            })

            # Full overwrite without changes param
            new_content = "completely new content"
            result = write_memory_entity.invoke({
                "path": "facts/schema/tables/test.md",
                "content": new_content,
            })
            assert "written" in result.lower()
            assert store.read_file("facts/schema/tables/test.md") == new_content

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
                "content": "# ods_order\n\n## Basic Info\n\n- **database**: ods\n- **table**: ods_order\n- **update_frequency**: daily\n\n## Compiled Truth\n\n- **objective**: 订单明细表\n- **definition**: 初始定义",
            })

            result = update_memory_entity.invoke({
                "path": "facts/schema/tables/ods_order.md",
                "changes": '{"definition": "从业务系统同步的订单明细数据"}',
                "timeline_desc": "更新了定义",
            })
            assert "written" in result.lower() or "updated" in result.lower()
            index_content = store.read_file("facts/schema/index.md")
            assert "订单明细表" in index_content


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

    def test_parse_entity_entry_new_format(self):
        """New 3-layer format: title from **table**: field, description from **objective**:."""
        from deerflow.structured_memory.index_service import parse_entity_entry

        content = """# Basic Info

- **database**: ods
- **table**: order_detail
- **update_frequency**: daily

## Compiled Truth

- **objective**: 原始业务表，记录订单明细数据
- **definition**: 从mysql同步的订单数据
- **core_logic**: SELECT * FROM orders
"""
        title, desc = parse_entity_entry(content)
        assert title == "order_detail"
        assert desc == "原始业务表，记录订单明细数据"

    def test_parse_entity_entry_legacy_format(self):
        """Old flat format still parses correctly for backward compatibility."""
        from deerflow.structured_memory.index_service import parse_entity_entry

        content = """# ods_order

## Basic Info
- **database**: ods
- **table**: order
- **objective**: 订单明细表
"""
        title, desc = parse_entity_entry(content)
        assert title == "order"
        assert desc == "订单明细表"

    def test_parse_entity_entry_no_title(self):
        from deerflow.structured_memory.index_service import parse_entity_entry

        title, desc = parse_entity_entry("No heading here")
        assert title == ""
        assert desc == ""

    def test_parse_entity_entry_chinese_labels(self):
        """Chinese labels (**表**, **目的**, **定义**) are also recognized."""
        from deerflow.structured_memory.index_service import parse_entity_entry

        content = """# 基本信息

- **库**: newretail
- **表**: dim_upload_autolifemall_shop_pvid_supplement_is_online_union
- **更新频率**: daily

## Compiled Truth

- **目的**: PVID 每日维度表，合并多个上传表供下游使用
- **定义**: 人工维护的 PVID 维度表统一入口
"""
        title, desc = parse_entity_entry(content)
        assert title == "dim_upload_autolifemall_shop_pvid_supplement_is_online_union"
        assert desc == "PVID 每日维度表，合并多个上传表供下游使用"

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
            content = """# Basic Info

## Compiled Truth

- **objective**: 订单明细表
"""
            msg = register_entity(store, "facts/schema/tables/ods_order.md", None, content)
            assert "Basic Info" in msg
            index_content = store.read_file("facts/schema/index.md")
            assert "Basic Info" in index_content

    def test_register_entity_update_removes_old_entry(self, tmp_path):
        from deerflow.structured_memory.index_service import register_entity
        from deerflow.structured_memory.storage import StructuredMemoryStore

        store = StructuredMemoryStore()
        with _patch_store_root(store, tmp_path):
            store.ensure_directories()
            old_content = """# Basic Info

## Compiled Truth

- **objective**: 旧描述
"""
            new_content = """# Basic Info

## Compiled Truth

- **objective**: 新描述
"""
            register_entity(store, "facts/schema/tables/ods_order.md", None, old_content)
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
            content = """# Basic Info

## Compiled Truth

- **objective**: 订单明细表
"""
            register_entity(store, "facts/schema/tables/ods_order.md", None, content)
            msg = unregister_entity(store, "facts/schema/tables/ods_order.md", content)
            assert "Removed" in msg
            index_content = store.read_file("facts/schema/index.md")
            assert "Basic Info" not in index_content

    def test_write_memory_entity_auto_indexes(self, tmp_path):
        from deerflow.tools.builtins.structured_memory_tools import write_memory_entity

        store = StructuredMemoryStore()
        with _patch_store_root(store, tmp_path):
            store.ensure_directories()
            content = """# Basic Info

## Compiled Truth

- **objective**: 订单明细表
"""
            result = write_memory_entity.invoke({
                "path": "facts/schema/tables/ods_order.md",
                "content": content,
            })
            assert "written" in result.lower()
            assert "index" in result.lower()
            assert store.file_exists("facts/schema/tables/ods_order.md")
            index_content = store.read_file("facts/schema/index.md")
            assert "Basic Info" in index_content

    def test_delete_memory_entity_auto_unindexes(self, tmp_path):
        from deerflow.tools.builtins.structured_memory_tools import delete_memory_entity, write_memory_entity

        store = StructuredMemoryStore()
        with _patch_store_root(store, tmp_path):
            store.ensure_directories()
            content = """# Basic Info

## Compiled Truth

- **objective**: 订单明细表
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
            assert "Basic Info" not in index_content

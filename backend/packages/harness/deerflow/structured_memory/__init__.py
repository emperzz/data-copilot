"""Enterprise structured memory for data warehouse knowledge."""

from deerflow.structured_memory.index_service import (
    build_index_entry,
    get_index_path,
    parse_entity_entry,
    register_entity,
    unregister_entity,
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
    BusinessFieldEntry,
    BusinessEntity,
    apply_partial_update_for_business,
    markdown_to_business_entity,
    business_entity_to_markdown,
    parse_business_changes_json,
)
from deerflow.structured_memory.storage import (
    StructuredMemoryStore,
    get_structured_memory_store,
)
from deerflow.structured_memory.search import search_memory_files
from deerflow.structured_memory.templates import (
    BUSINESS_ENTITY_TEMPLATE,
    BUSINESS_INDEX_TEMPLATE,
    FACTS_INDEX_TEMPLATE,
    WAREHOUSE_INDEX_TEMPLATE,
    TABLE_DETAIL_TEMPLATE,
    TASKS_INDEX_TEMPLATE,
    TASK_SUMMARY_TEMPLATE,
)

__all__ = [
    "BUSINESS_ENTITY_TEMPLATE",
    "BUSINESS_INDEX_TEMPLATE",
    "CORE_MEMORY_FILENAME",
    "FACTS_INDEX_TEMPLATE",
    "WAREHOUSE_INDEX_TEMPLATE",
    "TABLE_DETAIL_TEMPLATE",
    "TASKS_INDEX_TEMPLATE",
    "TASK_SUMMARY_TEMPLATE",
    "BusinessEntity",
    "BusinessFieldEntry",
    "SourceTable",
    "StructuredMemoryStore",
    "TableBasicInfo",
    "TableColumn",
    "TableCompiledTruth",
    "TableEntity",
    "TimelineEntry",
    "apply_partial_update",
    "apply_partial_update_for_business",
    "build_index_entry",
    "business_entity_to_markdown",
    "get_index_path",
    "get_structured_memory_store",
    "markdown_to_business_entity",
    "markdown_to_table_entity",
    "parse_business_changes_json",
    "parse_changes_json",
    "parse_entity_entry",
    "register_entity",
    "search_memory_files",
    "table_entity_to_markdown",
    "unregister_entity",
]

CORE_MEMORY_FILENAME = "core.md"

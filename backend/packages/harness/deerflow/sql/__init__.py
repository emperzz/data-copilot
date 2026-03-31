"""SQL parsing utilities (sqlglot-based), independent of LangChain tools."""

from deerflow.sql.metadata import check_syntax_payload, error_payload, parse_sql_metadata, serialize_parse_error, transpile_payload
from deerflow.sql.lineage import extract_source_table_keys, extract_target_table_keys, parse_sql_statements, resolve_statement_kind, supports_lineage
from deerflow.sql.normalize import normalize_sql_identifier

__all__ = [
    "check_syntax_payload",
    "error_payload",
    "extract_source_table_keys",
    "extract_target_table_keys",
    "parse_sql_metadata",
    "parse_sql_statements",
    "resolve_statement_kind",
    "normalize_sql_identifier",
    "serialize_parse_error",
    "supports_lineage",
    "transpile_payload",
]

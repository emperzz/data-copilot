"""SQL lineage extraction helpers (pure parsing logic, no persistence side effects)."""

from __future__ import annotations

from typing import Any

import sqlglot
from sqlglot import expressions as exp

from deerflow.sql.normalize import normalize_sql_identifier

TableKey = tuple[str | None, str, str]
LINEAGE_PURPOSES = {"insert", "create", "merge"}


def _table_entry_to_key(entry: dict[str, str | None], cte_norm: set[str]) -> TableKey | None:
    name_raw = entry.get("name")
    if name_raw is None or not str(name_raw).strip():
        return None
    name_norm = normalize_sql_identifier(str(name_raw))
    if not name_norm or name_norm in cte_norm:
        return None
    catalog_raw = entry.get("catalog")
    catalog_norm = normalize_sql_identifier(catalog_raw) or None
    db_norm = normalize_sql_identifier(entry.get("db"))
    return (catalog_norm, db_norm, name_norm)


def _unwrap_table(node: exp.Expression | None) -> exp.Table | None:
    if node is None:
        return None
    if isinstance(node, exp.Table):
        return node
    inner = getattr(node, "this", None)
    if isinstance(inner, exp.Table):
        return inner
    return None


def supports_lineage(sql_purpose: str) -> bool:
    return sql_purpose in LINEAGE_PURPOSES


def parse_sql_statements(sql: str, source_dialect: str | None = None) -> list[exp.Expression]:
    return sqlglot.parse(sql, read=source_dialect)


def extract_target_table_keys(statement: exp.Expression, cte_norm: set[str]) -> set[TableKey]:
    targets: list[exp.Table] = []
    if isinstance(statement, (exp.Insert, exp.Merge)):
        table_node = _unwrap_table(statement.this if hasattr(statement, "this") else None)
        if table_node is not None:
            targets.append(table_node)
    elif isinstance(statement, exp.Create):
        is_ctas = statement.expression is not None
        if is_ctas:
            table_node = _unwrap_table(statement.this if hasattr(statement, "this") else None)
            if table_node is not None:
                targets.append(table_node)

    keys: set[TableKey] = set()
    for table in targets:
        entry = {
            "catalog": table.catalog.name if getattr(table.catalog, "name", None) is not None else (str(table.catalog) if table.catalog is not None else None),
            "db": table.db.name if getattr(table.db, "name", None) is not None else (str(table.db) if table.db is not None else None),
            "name": table.name,
        }
        key = _table_entry_to_key(entry, cte_norm)
        if key is not None:
            keys.add(key)
    return keys


def extract_source_table_keys(statement_metadata: dict[str, Any], cte_norm: set[str], target_keys: set[TableKey]) -> set[TableKey]:
    source_keys: set[TableKey] = set()
    for source_entry in statement_metadata.get("tables") or []:
        source_key = _table_entry_to_key(source_entry, cte_norm)
        if source_key is not None:
            source_keys.add(source_key)
    return source_keys - target_keys

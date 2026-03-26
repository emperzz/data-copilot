"""Built-in SQL tools powered by sqlglot."""

import json
from typing import Any

import sqlglot
from langchain.tools import tool
from sqlglot import expressions as exp
from sqlglot.errors import ParseError, TokenError, UnsupportedError


def _serialize_error(error: Exception) -> str:
    if isinstance(error, ParseError) and getattr(error, "errors", None):
        details = error.errors[0]
        return json.dumps(
            {
                "ok": False,
                "error": {
                    "type": "ParseError",
                    "message": details.get("description", str(error)),
                    "line": details.get("line"),
                    "column": details.get("col"),
                },
            },
            ensure_ascii=False,
        )

    return json.dumps(
        {
            "ok": False,
            "error": {
                "type": error.__class__.__name__,
                "message": str(error),
            },
        },
        ensure_ascii=False,
    )


def _maybe_sql_name(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if hasattr(value, "name"):
        return value.name
    return str(value)


def _table_to_dict(node: exp.Table) -> dict[str, Any]:
    catalog = _maybe_sql_name(getattr(node, "catalog", None))
    db = _maybe_sql_name(node.db)
    return {
        "catalog": catalog,
        "db": db,
        "name": node.name,
    }


def _column_to_dict(node: exp.Column) -> dict[str, Any]:
    return {
        "table": _maybe_sql_name(node.table),
        "name": node.name,
    }


def _cte_name(node: exp.CTE) -> str | None:
    alias = node.alias
    if alias is None:
        return None
    if hasattr(alias, "name"):
        return alias.name
    return str(alias)


def _extract_statement_metadata(statement: exp.Expression) -> dict[str, Any]:
    tables_raw = [_table_to_dict(t) for t in statement.find_all(exp.Table)]
    seen_tables: set[tuple[Any, Any, Any]] = set()
    tables: list[dict[str, Any]] = []
    for entry in tables_raw:
        key = (entry["catalog"], entry["db"], entry["name"])
        if key not in seen_tables:
            seen_tables.add(key)
            tables.append(entry)

    columns_raw = [_column_to_dict(c) for c in statement.find_all(exp.Column)]
    seen_cols: set[tuple[Any, Any]] = set()
    columns: list[dict[str, Any]] = []
    for entry in columns_raw:
        key = (entry["table"], entry["name"])
        if key not in seen_cols:
            seen_cols.add(key)
            columns.append(entry)

    cte_names: list[str] = []
    seen_cte: set[str] = set()
    for cte in statement.find_all(exp.CTE):
        name = _cte_name(cte)
        if name and name not in seen_cte:
            seen_cte.add(name)
            cte_names.append(name)

    projections: list[str] = []
    for select in statement.find_all(exp.Select):
        for proj in select.expressions:
            projections.append(proj.alias_or_name)

    return {
        "kind": type(statement).__name__,
        "tables": tables,
        "cte_names": cte_names,
        "columns": columns,
        "select_projections": projections,
    }


@tool("sql_extract_metadata", parse_docstring=True)
def sql_extract_metadata(sql: str, source_dialect: str | None = None) -> str:
    """Extract structural metadata from SQL (tables, columns, CTEs, statement kinds).

    Parses SQL with sqlglot and returns tables referenced, column references,
    CTE names, outer statement expression class, and SELECT projection aliases/names.

    Args:
        sql: SQL text to analyze.
        source_dialect: Optional source SQL dialect (for example, "mysql", "postgres", "spark").

    Returns:
        JSON string with per-statement metadata or parse error details.
    """
    try:
        parsed = sqlglot.parse(sql, read=source_dialect)
    except (ParseError, TokenError, ValueError) as error:
        return _serialize_error(error)

    statements_out: list[dict[str, Any]] = []
    for index, statement in enumerate(parsed):
        if statement is None:
            statements_out.append({"index": index, "kind": None, "error": "empty_statement"})
            continue
        meta = _extract_statement_metadata(statement)
        meta["index"] = index
        statements_out.append(meta)

    return json.dumps(
        {
            "ok": True,
            "source_dialect": source_dialect,
            "statements": statements_out,
        },
        ensure_ascii=False,
    )


@tool("sql_check_syntax", parse_docstring=True)
def sql_check_syntax(sql: str, source_dialect: str | None = None) -> str:
    """Check SQL syntax validity.

    Uses sqlglot parser to validate SQL syntax and returns a structured result.

    Args:
        sql: SQL text to validate.
        source_dialect: Optional source SQL dialect (for example, "mysql", "postgres", "spark").

    Returns:
        JSON string describing whether SQL is valid and parse details.
    """
    try:
        parsed = sqlglot.parse(sql, read=source_dialect)
    except (ParseError, TokenError, ValueError) as error:
        return _serialize_error(error)

    return json.dumps(
        {
            "ok": True,
            "statements_count": len(parsed),
            "source_dialect": source_dialect,
        },
        ensure_ascii=False,
    )


@tool("sql_transpile", parse_docstring=True)
def sql_transpile(
    sql: str,
    target_dialect: str,
    source_dialect: str | None = None,
    pretty: bool = True,
) -> str:
    """Transpile SQL between dialects.

    Uses sqlglot transpiler to convert SQL to the target database dialect.

    Args:
        sql: SQL text to transpile.
        target_dialect: Target SQL dialect (for example, "mysql", "postgres", "spark", "duckdb").
        source_dialect: Optional source SQL dialect.
        pretty: Whether to format output SQL.

    Returns:
        JSON string containing transpiled SQL statements or error details.
    """
    try:
        statements = sqlglot.transpile(
            sql,
            read=source_dialect,
            write=target_dialect,
            pretty=pretty,
        )
    except (ParseError, TokenError, UnsupportedError, ValueError) as error:
        return _serialize_error(error)

    return json.dumps(
        {
            "ok": True,
            "source_dialect": source_dialect,
            "target_dialect": target_dialect,
            "statements": statements,
        },
        ensure_ascii=False,
    )

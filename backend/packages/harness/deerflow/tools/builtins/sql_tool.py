"""Built-in SQL tools powered by sqlglot."""

import json

import sqlglot
from langchain.tools import tool
from sqlglot.errors import ParseError, TokenError, UnsupportedError

from deerflow.sql.metadata import parse_sql_metadata, serialize_parse_error


def _serialize_error(error: Exception) -> str:
    return serialize_parse_error(error)


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
    return json.dumps(parse_sql_metadata(sql, source_dialect=source_dialect), ensure_ascii=False)


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

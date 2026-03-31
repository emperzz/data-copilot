"""Built-in SQL tools powered by sqlglot."""

import json

from langchain.tools import ToolRuntime, tool
from langgraph.typing import ContextT

from deerflow.agents.thread_state import ThreadState
from deerflow.dw_catalog.repository import DwCatalogRepository
from deerflow.sql.metadata import check_syntax_payload, parse_sql_metadata, serialize_parse_error, transpile_payload


def _serialize_error(error: Exception) -> str:
    return serialize_parse_error(error)


@tool("dw_catalog_ingest_sql", parse_docstring=True)
def dw_catalog_ingest_sql(
    runtime: ToolRuntime[ContextT, ThreadState],
    sql: str,
    statement_notes_md: list[str],
    source_dialect: str | None = None,
) -> str:
    """Parse SQL, persist ingest rows and statement slices, and upsert referenced tables into the local DW catalog.

    Use this when the user wants SQL analyzed and stored for lineage or catalog tracking (not only ephemeral metadata).
    For every top-level statement in ``sql`` (same order as returned by the parser), provide one Markdown note in
    ``statement_notes_md``. Each note must include: a short keyword line (what the statement does / which data),
    a business purpose and value section, and a fenced code block with a simplified core-logic SQL only.

    Args:
        sql: SQL text to parse and store.
        statement_notes_md: One Markdown string per statement, in parser order (same length as statement count).
        source_dialect: Optional sqlglot dialect (e.g. mysql, postgres, spark).
    """
    thread_id = None
    if runtime is not None:
        thread_id = runtime.context.get("thread_id")

    payload = parse_sql_metadata(sql, source_dialect=source_dialect)
    if not payload.get("ok"):
        err = payload.get("error", {})
        msg = err.get("message", "parse failed") if isinstance(err, dict) else str(err)
        return json.dumps({"ok": False, "error": msg}, ensure_ascii=False)

    stmt_entries = payload["statements"]
    n = len(stmt_entries)
    if len(statement_notes_md) != n:
        return json.dumps(
            {
                "ok": False,
                "error": f"statement_notes_md must have {n} entries (one per statement); got {len(statement_notes_md)}",
            },
            ensure_ascii=False,
        )
    for i, note in enumerate(statement_notes_md):
        if not str(note).strip():
            return json.dumps(
                {"ok": False, "error": f"statement_notes_md[{i}] must be non-empty Markdown"},
                ensure_ascii=False,
            )

    repo = DwCatalogRepository()
    repo.ensure_schema()

    try:
        ingest, stmts, tables = repo.ingest_sql_text(
            sql,
            source_dialect=source_dialect,
            thread_id=thread_id,
            statement_notes_md=statement_notes_md,
        )
    except ValueError as exc:
        return json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False)

    return json.dumps(
        {
            "ok": True,
            "ingest_id": ingest.id,
            "sql_hash": ingest.sql_hash,
            "statement_count": len(stmts),
            "statements": [
                {
                    "id": s.id,
                    "statement_index": s.statement_index,
                    "kind": s.kind,
                    "sql_purpose": s.sql_purpose.value,
                    "sql_operation_category": s.sql_operation_category.value,
                    "statement_notes_md": s.statement_notes_md,
                }
                for s in stmts
            ],
            "tables": [
                {
                    "id": t.id,
                    "catalog": t.catalog,
                    "db": t.db,
                    "table_name": t.table_name,
                    "latest_statement_id": t.latest_statement_id,
                }
                for t in tables
            ],
        },
        ensure_ascii=False,
    )


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
    return json.dumps(
        check_syntax_payload(sql, source_dialect=source_dialect),
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
    return json.dumps(
        transpile_payload(
            sql,
            target_dialect=target_dialect,
            source_dialect=source_dialect,
            pretty=pretty,
        ),
        ensure_ascii=False,
    )

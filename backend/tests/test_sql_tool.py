import json
import importlib
from types import SimpleNamespace

sql_tool_module = importlib.import_module("deerflow.tools.builtins.sql_tool")


def test_sql_check_syntax_success() -> None:
    result = sql_tool_module.sql_check_syntax.invoke({"sql": "SELECT 1"})
    payload = json.loads(result)
    assert payload["ok"] is True
    assert payload["statements_count"] == 1


def test_sql_check_syntax_parse_error() -> None:
    result = sql_tool_module.sql_check_syntax.invoke({"sql": "SELECT FROM"})
    payload = json.loads(result)
    assert payload["ok"] is False
    assert payload["error"]["type"] in {"ParseError", "TokenError"}


def test_sql_transpile_success() -> None:
    result = sql_tool_module.sql_transpile.invoke(
        {
            "sql": "SELECT STRFTIME(x, '%y-%-m-%S')",
            "source_dialect": "duckdb",
            "target_dialect": "hive",
        }
    )
    payload = json.loads(result)
    assert payload["ok"] is True
    assert payload["target_dialect"] == "hive"
    assert len(payload["statements"]) == 1
    assert "DATE_FORMAT" in payload["statements"][0]


def test_sql_transpile_invalid_dialect() -> None:
    result = sql_tool_module.sql_transpile.invoke(
        {
            "sql": "SELECT 1",
            "target_dialect": "not_a_real_dialect",
        }
    )
    payload = json.loads(result)
    assert payload["ok"] is False
    assert payload["error"]["type"] == "ValueError"


def test_sql_extract_metadata_select_join() -> None:
    result = sql_tool_module.sql_extract_metadata.invoke(
        {
            "sql": "SELECT a, t.b FROM d AS t JOIN e ON t.id = e.id",
        }
    )
    payload = json.loads(result)
    assert payload["ok"] is True
    assert len(payload["statements"]) == 1
    stmt = payload["statements"][0]
    assert stmt["kind"] == "Select"
    table_names = {t["name"] for t in stmt["tables"]}
    assert table_names == {"d", "e"}
    column_names = {c["name"] for c in stmt["columns"]}
    assert {"a", "b", "id"}.issubset(column_names)


def test_dw_catalog_ingest_sql_success(tmp_path, monkeypatch) -> None:
    from deerflow.dw_catalog.repository import DwCatalogRepository

    db = tmp_path / "cat.db"
    repo = DwCatalogRepository(db_path=db)
    monkeypatch.setattr(sql_tool_module, "DwCatalogRepository", lambda: repo)

    note0 = (
        "## Keywords\n`JOIN`, `d`, `e`\n\n## 业务目的\n联表取字段 a。\n\n## 简化 SQL\n```sql\n"
        "SELECT a FROM d JOIN e ON d.id = e.id\n```\n"
    )
    runtime = SimpleNamespace(context={"thread_id": "t-1"}, state=None, config={})
    raw = sql_tool_module.dw_catalog_ingest_sql.func(
        runtime,
        "SELECT a FROM d JOIN e ON d.id = e.id",
        [note0],
        None,
    )
    payload = json.loads(raw)
    assert payload["ok"] is True
    assert payload["ingest_id"]
    assert payload["statement_count"] == 1
    table_names = {t["table_name"] for t in payload["tables"]}
    assert table_names == {"d", "e"}
    assert payload["statements"][0]["sql_purpose"] == "query"
    assert payload["statements"][0]["sql_operation_category"] == "dql"
    assert payload["statements"][0]["statement_notes_md"] == note0

    loaded = repo.get_ingest(payload["ingest_id"])
    assert loaded is not None
    assert loaded.thread_id == "t-1"
    persisted = repo.list_statements_for_ingest(payload["ingest_id"])
    assert len(persisted) == 1
    assert persisted[0].statement_notes_md == note0


def test_dw_catalog_ingest_sql_statement_notes_length_mismatch(tmp_path, monkeypatch) -> None:
    from deerflow.dw_catalog.repository import DwCatalogRepository

    db = tmp_path / "cat.db"
    repo = DwCatalogRepository(db_path=db)
    monkeypatch.setattr(sql_tool_module, "DwCatalogRepository", lambda: repo)

    raw = sql_tool_module.dw_catalog_ingest_sql.func(
        None,
        "SELECT 1",
        [],
        None,
    )
    payload = json.loads(raw)
    assert payload["ok"] is False
    assert "statement_notes_md" in payload["error"] or "1 entries" in payload["error"]


def test_dw_catalog_ingest_sql_parse_error(tmp_path, monkeypatch) -> None:
    from deerflow.dw_catalog.repository import DwCatalogRepository

    db = tmp_path / "cat.db"
    repo = DwCatalogRepository(db_path=db)
    monkeypatch.setattr(sql_tool_module, "DwCatalogRepository", lambda: repo)

    raw = sql_tool_module.dw_catalog_ingest_sql.func(
        None,
        "SELECT FROM",
        [],
        None,
    )
    payload = json.loads(raw)
    assert payload["ok"] is False
    assert "error" in payload


def test_sql_extract_metadata_cte() -> None:
    result = sql_tool_module.sql_extract_metadata.invoke(
        {
            "sql": "WITH x AS (SELECT 1 AS n) SELECT n FROM x",
        }
    )
    payload = json.loads(result)
    assert payload["ok"] is True
    stmt = payload["statements"][0]
    assert stmt["kind"] in {"Select", "With"}
    assert "x" in stmt["cte_names"]
    assert any(t["name"] == "x" for t in stmt["tables"])



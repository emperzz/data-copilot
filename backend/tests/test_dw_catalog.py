"""Tests for deerflow.dw_catalog SQLite repository."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from deerflow.dw_catalog import (
    DwCatalogRepository,
    DwSqlIngestCreate,
    DwSqlStatementCreate,
    DwTableCreate,
    SqlOperationCategory,
    SqlPurpose,
    infer_sql_purpose_from_kind,
    sql_content_hash,
    sql_purpose_to_category,
)


@pytest.fixture
def repo(tmp_path: Path) -> DwCatalogRepository:
    db = tmp_path / "dw_catalog.db"
    r = DwCatalogRepository(db_path=db)
    r.ensure_schema()
    return r


def test_create_ingest_and_statements(repo: DwCatalogRepository) -> None:
    ingest = repo.create_ingest(
        DwSqlIngestCreate(
            sql_hash="abc",
            sql_text="SELECT 1",
        )
    )
    assert ingest.id

    stmts = repo.create_statements(
        ingest.id,
        [
            DwSqlStatementCreate(
                statement_index=0,
                kind="Select",
                raw_json='{"index":0,"kind":"Select"}',
                sql_purpose=SqlPurpose.QUERY,
            ),
        ],
    )
    assert len(stmts) == 1
    loaded = repo.list_statements_for_ingest(ingest.id)
    assert loaded[0].raw_json == '{"index":0,"kind":"Select"}'
    assert loaded[0].sql_purpose == SqlPurpose.QUERY
    assert loaded[0].sql_operation_category == SqlOperationCategory.DQL


def test_upsert_table_idempotent(repo: DwCatalogRepository) -> None:
    a = repo.upsert_table(DwTableCreate(catalog=None, db="d", table_name="T"))
    b = repo.upsert_table(DwTableCreate(catalog=None, db="d", table_name="t"))
    assert a.id == b.id
    assert a.table_name == "t"


def test_ingest_sql_text_persists_tables_and_statements(repo: DwCatalogRepository) -> None:
    sql = "SELECT a, t.b FROM d AS t JOIN e ON t.id = e.id"
    ingest, stmts, tables = repo.ingest_sql_text(
        sql,
    )
    assert ingest.sql_hash == sql_content_hash(sql)
    assert len(stmts) == 1
    meta = json.loads(stmts[0].raw_json)
    assert meta.get("kind") == "Select"
    assert stmts[0].sql_purpose == SqlPurpose.QUERY
    names = {t.table_name for t in tables}
    assert names == {"d", "e"}


def test_ingest_sql_text_parse_error(repo: DwCatalogRepository) -> None:
    with pytest.raises(ValueError):
        repo.ingest_sql_text("SELECT FROM")


def test_ingest_sql_text_persists_statement_notes_md(repo: DwCatalogRepository) -> None:
    sql = "SELECT 1 AS x"
    notes = ["## Keywords\n`literal`\n\n## 业务目的\n占位。\n\n## 简化 SQL\n```sql\nSELECT 1 AS x\n```\n"]
    _, stmts, _ = repo.ingest_sql_text(sql, statement_notes_md=notes)
    assert len(stmts) == 1
    assert stmts[0].statement_notes_md == notes[0]
    again = repo.list_statements_for_ingest(stmts[0].ingest_id)
    assert again[0].statement_notes_md == notes[0]


def test_ingest_sql_text_statement_notes_length_mismatch(repo: DwCatalogRepository) -> None:
    with pytest.raises(ValueError, match="statement_notes_md length"):
        repo.ingest_sql_text("SELECT 1", statement_notes_md=["a", "b"])


def test_ingest_sql_text_builds_lineage_for_insert(repo: DwCatalogRepository) -> None:
    sql = "INSERT INTO d.t_target SELECT id FROM d.t_source"
    _, stmts, tables = repo.ingest_sql_text(sql)
    assert len(stmts) == 1
    assert {t.table_name for t in tables} == {"t_target", "t_source"}

    lineage = repo.list_table_lineage_for_statement(stmts[0].id)
    assert len(lineage) == 1

    source_table = repo.get_table(lineage[0].source_table_id)
    target_table = repo.get_table(lineage[0].target_table_id)
    assert source_table is not None
    assert target_table is not None
    assert source_table.table_name == "t_source"
    assert target_table.table_name == "t_target"


def test_ingest_sql_text_builds_lineage_for_ctas(repo: DwCatalogRepository) -> None:
    sql = "CREATE TABLE d.t_new AS SELECT id FROM d.t_old"
    _, stmts, tables = repo.ingest_sql_text(sql)
    assert len(stmts) == 1
    assert {t.table_name for t in tables} == {"t_new", "t_old"}

    lineage = repo.list_table_lineage_for_statement(stmts[0].id)
    assert len(lineage) == 1

    source_table = repo.get_table(lineage[0].source_table_id)
    target_table = repo.get_table(lineage[0].target_table_id)
    assert source_table is not None
    assert target_table is not None
    assert source_table.table_name == "t_old"
    assert target_table.table_name == "t_new"


def test_ingest_sql_text_does_not_build_lineage_for_query(repo: DwCatalogRepository) -> None:
    sql = "SELECT id FROM d.t_source"
    _, stmts, _ = repo.ingest_sql_text(sql)
    assert len(stmts) == 1
    assert repo.list_table_lineage_for_statement(stmts[0].id) == []


def test_ingest_sql_text_lineage_skips_cte_virtual_table(repo: DwCatalogRepository) -> None:
    sql = """
    INSERT INTO d.t_target
    WITH temp_cte AS (SELECT id FROM d.t_source)
    SELECT id FROM temp_cte
    """
    _, stmts, _ = repo.ingest_sql_text(sql)
    assert len(stmts) == 1

    lineage = repo.list_table_lineage_for_statement(stmts[0].id)
    assert len(lineage) == 1

    source_table = repo.get_table(lineage[0].source_table_id)
    target_table = repo.get_table(lineage[0].target_table_id)
    assert source_table is not None
    assert target_table is not None
    assert source_table.table_name == "t_source"
    assert target_table.table_name == "t_target"


def test_ingest_sql_text_does_not_build_lineage_for_update(repo: DwCatalogRepository) -> None:
    sql = "UPDATE d.t_target SET id = s.id FROM d.t_source AS s WHERE d.t_target.id = s.id"
    _, stmts, _ = repo.ingest_sql_text(sql)
    assert len(stmts) == 1
    assert repo.list_table_lineage_for_statement(stmts[0].id) == []


def test_ingest_sql_text_does_not_build_lineage_for_delete(repo: DwCatalogRepository) -> None:
    sql = "DELETE FROM d.t_target WHERE id IN (SELECT id FROM d.t_source)"
    _, stmts, _ = repo.ingest_sql_text(sql)
    assert len(stmts) == 1
    assert repo.list_table_lineage_for_statement(stmts[0].id) == []


def test_ingest_sql_text_builds_lineage_for_with_insert(repo: DwCatalogRepository) -> None:
    sql = """
    WITH base AS (
      SELECT id FROM d.t_source
    )
    INSERT OVERWRITE TABLE d.t_target
    SELECT id FROM base
    """
    _, stmts, _ = repo.ingest_sql_text(sql)
    assert len(stmts) == 1
    assert stmts[0].sql_purpose == SqlPurpose.INSERT
    lineage = repo.list_table_lineage_for_statement(stmts[0].id)
    assert len(lineage) == 1

    source_table = repo.get_table(lineage[0].source_table_id)
    target_table = repo.get_table(lineage[0].target_table_id)
    assert source_table is not None
    assert target_table is not None
    assert source_table.table_name == "t_source"
    assert target_table.table_name == "t_target"


def test_sql_purpose_coerce() -> None:
    assert SqlPurpose.coerce("MERGE") == SqlPurpose.MERGE
    assert SqlPurpose.coerce("  drop  ") == SqlPurpose.DROP
    assert SqlPurpose.coerce("not_a_label") == SqlPurpose.UNKNOWN


def test_sql_purpose_to_category() -> None:
    assert sql_purpose_to_category(SqlPurpose.CREATE) == SqlOperationCategory.DDL
    assert sql_purpose_to_category(SqlPurpose.DELETE) == SqlOperationCategory.DML
    assert sql_purpose_to_category(SqlPurpose.QUERY) == SqlOperationCategory.DQL
    assert sql_purpose_to_category(SqlPurpose.GRANT) == SqlOperationCategory.DCL
    assert sql_purpose_to_category(SqlPurpose.TRANSACTION) == SqlOperationCategory.TCL
    assert sql_purpose_to_category(SqlPurpose.ANALYZE) == SqlOperationCategory.UTILITY
    assert sql_purpose_to_category(SqlPurpose.UNKNOWN) == SqlOperationCategory.OTHER


def test_infer_sql_purpose_from_kind() -> None:
    assert infer_sql_purpose_from_kind("Select") == SqlPurpose.QUERY
    assert infer_sql_purpose_from_kind("Create") == SqlPurpose.CREATE
    assert infer_sql_purpose_from_kind("NotAKind") == SqlPurpose.UNKNOWN


def test_list_statements_coerces_unknown_stored_purpose(repo: DwCatalogRepository) -> None:
    ingest = repo.create_ingest(DwSqlIngestCreate(sql_hash="h1", sql_text="SELECT 1"))
    created = repo.create_statements(
        ingest.id,
        [
            DwSqlStatementCreate(statement_index=0, kind="Select", raw_json='{"index":0,"kind":"Select"}', sql_purpose=SqlPurpose.QUERY),
        ],
    )
    assert created[0].sql_purpose == SqlPurpose.QUERY

    with repo._connect() as conn:
        conn.execute("UPDATE dw_sql_statement SET sql_purpose = ? WHERE id = ?", ("legacy_or_typo", created[0].id))
        conn.commit()

    loaded = repo.list_statements_for_ingest(ingest.id)
    assert loaded[0].sql_purpose == SqlPurpose.UNKNOWN

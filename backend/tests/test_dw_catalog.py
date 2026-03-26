"""Tests for deerflow.dw_catalog SQLite repository."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from deerflow.dw_catalog import (
    DwCatalogRepository,
    DwSqlIngestCreate,
    DwSqlStatementCreate,
    DwTableCreate,
    SqlPurpose,
    sql_content_hash,
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
            source_type="direct_text",
            sql_hash="abc",
            sql_text="SELECT 1",
            sql_purpose=SqlPurpose.QUERY,
        )
    )
    assert ingest.id
    assert ingest.sql_purpose == SqlPurpose.QUERY

    stmts = repo.create_statements(
        ingest.id,
        [
            DwSqlStatementCreate(statement_index=0, kind="Select", raw_json='{"index":0,"kind":"Select"}'),
        ],
    )
    assert len(stmts) == 1
    loaded = repo.list_statements_for_ingest(ingest.id)
    assert loaded[0].raw_json == '{"index":0,"kind":"Select"}'


def test_upsert_table_idempotent(repo: DwCatalogRepository) -> None:
    a = repo.upsert_table(DwTableCreate(catalog=None, db="d", table_name="T"))
    b = repo.upsert_table(DwTableCreate(catalog=None, db="d", table_name="t"))
    assert a.id == b.id
    assert a.table_name_norm == "t"


def test_ingest_sql_text_persists_tables_and_statements(repo: DwCatalogRepository) -> None:
    sql = "SELECT a, t.b FROM d AS t JOIN e ON t.id = e.id"
    ingest, stmts, tables = repo.ingest_sql_text(
        sql,
        source_type="direct_text",
        sql_purpose=SqlPurpose.QUERY,
    )
    assert ingest.sql_hash == sql_content_hash(sql)
    assert len(stmts) == 1
    meta = json.loads(stmts[0].raw_json)
    assert meta.get("kind") == "Select"
    names = {t.table_name for t in tables}
    assert names == {"d", "e"}


def test_ingest_sql_text_parse_error(repo: DwCatalogRepository) -> None:
    with pytest.raises(ValueError):
        repo.ingest_sql_text("SELECT FROM", source_type="direct_text")

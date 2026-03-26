"""SQLite persistence for ``dw_sql_ingest``, ``dw_sql_statement``, and ``dw_table``."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path

from deerflow.config.paths import get_paths
from deerflow.dw_catalog.models import (
    DwSqlIngest,
    DwSqlIngestCreate,
    DwSqlStatement,
    DwSqlStatementCreate,
    DwTable,
    DwTableCreate,
    SqlPurpose,
)
from deerflow.dw_catalog.normalize import normalize_sql_identifier
from deerflow.sql.metadata import parse_sql_metadata


def sql_content_hash(sql: str) -> str:
    """SHA-256 hex digest of SQL text (UTF-8)."""
    return hashlib.sha256(sql.encode("utf-8")).hexdigest()


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _dt_from_sql(value: str) -> datetime:
    """Parse SQLite-stored ISO timestamps (with optional ``Z`` suffix)."""
    if value.endswith("Z"):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    return datetime.fromisoformat(value)


_SCHEMA_DDL = """
CREATE TABLE IF NOT EXISTS dw_sql_ingest (
  id TEXT PRIMARY KEY,
  created_at TEXT NOT NULL,
  source_type TEXT NOT NULL,
  thread_id TEXT,
  source_path TEXT,
  source_dialect TEXT,
  sql_hash TEXT NOT NULL,
  sql_text TEXT,
  extra_json TEXT,
  sql_purpose TEXT NOT NULL DEFAULT 'unknown'
);

CREATE INDEX IF NOT EXISTS ix_dw_sql_ingest_sql_hash ON dw_sql_ingest(sql_hash);

CREATE TABLE IF NOT EXISTS dw_sql_statement (
  id TEXT PRIMARY KEY,
  ingest_id TEXT NOT NULL REFERENCES dw_sql_ingest(id) ON DELETE CASCADE,
  statement_index INTEGER NOT NULL,
  kind TEXT,
  raw_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  UNIQUE (ingest_id, statement_index)
);

CREATE INDEX IF NOT EXISTS ix_dw_sql_statement_ingest ON dw_sql_statement(ingest_id);

CREATE TABLE IF NOT EXISTS dw_table (
  id TEXT PRIMARY KEY,
  catalog TEXT,
  db TEXT,
  table_name TEXT NOT NULL,
  catalog_norm TEXT NOT NULL DEFAULT '',
  db_norm TEXT NOT NULL DEFAULT '',
  table_name_norm TEXT NOT NULL,
  description TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE (catalog_norm, db_norm, table_name_norm)
);

CREATE INDEX IF NOT EXISTS ix_dw_table_db_norm ON dw_table(db_norm);
"""


class DwCatalogRepository:
    """CRUD for minimal catalog tables."""

    def __init__(self, db_path: Path | None = None) -> None:
        self._db_path = db_path if db_path is not None else get_paths().dw_catalog_db

    def _connect(self) -> sqlite3.Connection:
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(self._db_path))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def ensure_schema(self) -> None:
        with self._connect() as conn:
            conn.executescript(_SCHEMA_DDL)
            conn.commit()

    @staticmethod
    def _row_to_ingest(row: sqlite3.Row) -> DwSqlIngest:
        return DwSqlIngest(
            id=row["id"],
            created_at=_dt_from_sql(row["created_at"]),
            source_type=row["source_type"],
            thread_id=row["thread_id"],
            source_path=row["source_path"],
            source_dialect=row["source_dialect"],
            sql_hash=row["sql_hash"],
            sql_text=row["sql_text"],
            extra_json=row["extra_json"],
            sql_purpose=SqlPurpose(row["sql_purpose"]),
        )

    @staticmethod
    def _row_to_statement(row: sqlite3.Row) -> DwSqlStatement:
        return DwSqlStatement(
            id=row["id"],
            ingest_id=row["ingest_id"],
            statement_index=row["statement_index"],
            kind=row["kind"],
            raw_json=row["raw_json"],
            created_at=_dt_from_sql(row["created_at"]),
        )

    @staticmethod
    def _row_to_table(row: sqlite3.Row) -> DwTable:
        return DwTable(
            id=row["id"],
            catalog=row["catalog"],
            db=row["db"],
            table_name=row["table_name"],
            catalog_norm=row["catalog_norm"],
            db_norm=row["db_norm"],
            table_name_norm=row["table_name_norm"],
            description=row["description"],
            created_at=_dt_from_sql(row["created_at"]),
            updated_at=_dt_from_sql(row["updated_at"]),
        )

    def create_ingest(self, data: DwSqlIngestCreate) -> DwSqlIngest:
        ingest_id = str(uuid.uuid4())
        created = _utc_now()
        created_iso = created.isoformat().replace("+00:00", "Z")
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO dw_sql_ingest (
                  id, created_at, source_type, thread_id, source_path, source_dialect,
                  sql_hash, sql_text, extra_json, sql_purpose
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ingest_id,
                    created_iso,
                    data.source_type,
                    data.thread_id,
                    data.source_path,
                    data.source_dialect,
                    data.sql_hash,
                    data.sql_text,
                    data.extra_json,
                    data.sql_purpose.value,
                ),
            )
            conn.commit()
        row = self.get_ingest(ingest_id)
        assert row is not None
        return row

    def create_statements(self, ingest_id: str, statements: list[DwSqlStatementCreate]) -> list[DwSqlStatement]:
        created = _utc_now()
        created_iso = created.isoformat().replace("+00:00", "Z")
        out: list[DwSqlStatement] = []
        with self._connect() as conn:
            for stmt in statements:
                sid = str(uuid.uuid4())
                conn.execute(
                    """
                    INSERT INTO dw_sql_statement (id, ingest_id, statement_index, kind, raw_json, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (sid, ingest_id, stmt.statement_index, stmt.kind, stmt.raw_json, created_iso),
                )
                out.append(
                    DwSqlStatement(
                        id=sid,
                        ingest_id=ingest_id,
                        statement_index=stmt.statement_index,
                        kind=stmt.kind,
                        raw_json=stmt.raw_json,
                        created_at=created,
                    )
                )
            conn.commit()
        return out

    def upsert_table(self, data: DwTableCreate) -> DwTable:
        c_norm = normalize_sql_identifier(data.catalog)
        d_norm = normalize_sql_identifier(data.db)
        t_norm = normalize_sql_identifier(data.table_name)
        if not t_norm:
            raise ValueError("table_name normalizes to empty")

        now_iso = _utc_now().isoformat().replace("+00:00", "Z")

        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT id, created_at FROM dw_table
                WHERE catalog_norm = ? AND db_norm = ? AND table_name_norm = ?
                """,
                (c_norm, d_norm, t_norm),
            ).fetchone()

            if row:
                tid = row["id"]
                conn.execute(
                    """
                    UPDATE dw_table SET
                      catalog = ?, db = ?, table_name = ?,
                      description = COALESCE(?, description),
                      updated_at = ?
                    WHERE id = ?
                    """,
                    (data.catalog, data.db, data.table_name, data.description, now_iso, tid),
                )
            else:
                tid = str(uuid.uuid4())
                conn.execute(
                    """
                    INSERT INTO dw_table (
                      id, catalog, db, table_name, catalog_norm, db_norm, table_name_norm,
                      description, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        tid,
                        data.catalog,
                        data.db,
                        data.table_name,
                        c_norm,
                        d_norm,
                        t_norm,
                        data.description,
                        now_iso,
                        now_iso,
                    ),
                )
            conn.commit()

        table = self.get_table_by_norm(c_norm, d_norm, t_norm)
        assert table is not None
        return table

    def get_ingest(self, ingest_id: str) -> DwSqlIngest | None:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM dw_sql_ingest WHERE id = ?", (ingest_id,)).fetchone()
        if row is None:
            return None
        return self._row_to_ingest(row)

    def list_statements_for_ingest(self, ingest_id: str) -> list[DwSqlStatement]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM dw_sql_statement WHERE ingest_id = ? ORDER BY statement_index ASC",
                (ingest_id,),
            ).fetchall()
        return [self._row_to_statement(r) for r in rows]

    def get_table(self, table_id: str) -> DwTable | None:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM dw_table WHERE id = ?", (table_id,)).fetchone()
        if row is None:
            return None
        return self._row_to_table(row)

    def get_table_by_norm(self, catalog_norm: str, db_norm: str, table_name_norm: str) -> DwTable | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM dw_table WHERE catalog_norm = ? AND db_norm = ? AND table_name_norm = ?",
                (catalog_norm, db_norm, table_name_norm),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_table(row)

    def ingest_sql_text(
        self,
        sql: str,
        *,
        source_type: str,
        sql_purpose: SqlPurpose = SqlPurpose.UNKNOWN,
        source_dialect: str | None = None,
        thread_id: str | None = None,
        source_path: str | None = None,
        extra_json: str | None = None,
    ) -> tuple[DwSqlIngest, list[DwSqlStatement], list[DwTable]]:
        """Parse SQL, persist ingest + statements, upsert referenced physical tables."""
        payload = parse_sql_metadata(sql, source_dialect=source_dialect)
        if not payload.get("ok"):
            err = payload.get("error", {})
            msg = err.get("message", "parse failed")
            raise ValueError(msg)

        ingest_create = DwSqlIngestCreate(
            source_type=source_type,
            sql_hash=sql_content_hash(sql),
            thread_id=thread_id,
            source_path=source_path,
            source_dialect=source_dialect,
            sql_text=sql,
            extra_json=extra_json,
            sql_purpose=sql_purpose,
        )

        statements_payload: list[DwSqlStatementCreate] = []
        for stmt in payload["statements"]:
            idx = stmt["index"]
            kind = stmt.get("kind")
            raw_json = json.dumps(stmt, ensure_ascii=False)
            statements_payload.append(DwSqlStatementCreate(statement_index=idx, kind=kind, raw_json=raw_json))

        ingest = self.create_ingest(ingest_create)
        stmts = self.create_statements(ingest.id, statements_payload)

        tables_by_id: dict[str, DwTable] = {}
        for stmt in payload["statements"]:
            for t in stmt.get("tables") or []:
                name = t.get("name")
                if name is None or not str(name).strip():
                    continue
                tbl = self.upsert_table(
                    DwTableCreate(catalog=t.get("catalog"), db=t.get("db"), table_name=str(name))
                )
                tables_by_id[tbl.id] = tbl

        return ingest, stmts, list(tables_by_id.values())

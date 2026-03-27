"""SQLite persistence for ``dw_sql_ingest``, ``dw_sql_statement``, ``dw_table``, and history logs."""

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
    DwTableLineage,
    DwTableLineageCreate,
    DwTableCreate,
    DwTableStatementLog,
    DwTableStatementLogCreate,
    SqlPurpose,
    infer_sql_purpose_from_kind,
)
from deerflow.sql.normalize import normalize_sql_identifier
from deerflow.sql.lineage import extract_source_table_keys, extract_target_table_keys, parse_sql_statements, resolve_statement_kind, supports_lineage
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
  thread_id TEXT,
  source_dialect TEXT,
  sql_hash TEXT NOT NULL,
  sql_text TEXT,
  extra_json TEXT
);

CREATE INDEX IF NOT EXISTS ix_dw_sql_ingest_sql_hash ON dw_sql_ingest(sql_hash);

CREATE TABLE IF NOT EXISTS dw_sql_statement (
  id TEXT PRIMARY KEY,
  ingest_id TEXT NOT NULL REFERENCES dw_sql_ingest(id) ON DELETE CASCADE,
  statement_index INTEGER NOT NULL,
  kind TEXT,
  raw_json TEXT NOT NULL,
  sql_purpose TEXT NOT NULL DEFAULT 'unknown',
  created_at TEXT NOT NULL,
  UNIQUE (ingest_id, statement_index)
);

CREATE INDEX IF NOT EXISTS ix_dw_sql_statement_ingest ON dw_sql_statement(ingest_id);

CREATE TABLE IF NOT EXISTS dw_table (
  id TEXT PRIMARY KEY,
  catalog TEXT,
  db TEXT NOT NULL DEFAULT '',
  table_name TEXT NOT NULL,
  description TEXT,
  latest_statement_id TEXT REFERENCES dw_sql_statement(id) ON DELETE SET NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE (catalog, db, table_name)
);

CREATE INDEX IF NOT EXISTS ix_dw_table_db ON dw_table(db);

CREATE TABLE IF NOT EXISTS dw_table_statement_log (
  id TEXT PRIMARY KEY,
  table_id TEXT NOT NULL REFERENCES dw_table(id) ON DELETE CASCADE,
  statement_id TEXT NOT NULL REFERENCES dw_sql_statement(id) ON DELETE CASCADE,
  created_at TEXT NOT NULL,
  UNIQUE (table_id, statement_id)
);

CREATE INDEX IF NOT EXISTS ix_dw_table_statement_log_table ON dw_table_statement_log(table_id);
CREATE INDEX IF NOT EXISTS ix_dw_table_statement_log_statement ON dw_table_statement_log(statement_id);

CREATE TABLE IF NOT EXISTS dw_table_lineage (
  id TEXT PRIMARY KEY,
  source_table_id TEXT NOT NULL REFERENCES dw_table(id) ON DELETE CASCADE,
  target_table_id TEXT NOT NULL REFERENCES dw_table(id) ON DELETE CASCADE,
  statement_id TEXT NOT NULL REFERENCES dw_sql_statement(id) ON DELETE CASCADE,
  created_at TEXT NOT NULL,
  UNIQUE (source_table_id, target_table_id, statement_id)
);

CREATE INDEX IF NOT EXISTS ix_dw_table_lineage_source ON dw_table_lineage(source_table_id);
CREATE INDEX IF NOT EXISTS ix_dw_table_lineage_target ON dw_table_lineage(target_table_id);
CREATE INDEX IF NOT EXISTS ix_dw_table_lineage_statement ON dw_table_lineage(statement_id);
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
            thread_id=row["thread_id"],
            source_dialect=row["source_dialect"],
            sql_hash=row["sql_hash"],
            sql_text=row["sql_text"],
            extra_json=row["extra_json"],
        )

    @staticmethod
    def _row_to_statement(row: sqlite3.Row) -> DwSqlStatement:
        return DwSqlStatement(
            id=row["id"],
            ingest_id=row["ingest_id"],
            statement_index=row["statement_index"],
            kind=row["kind"],
            raw_json=row["raw_json"],
            sql_purpose=SqlPurpose.coerce(row["sql_purpose"]),
            created_at=_dt_from_sql(row["created_at"]),
        )

    @staticmethod
    def _row_to_table(row: sqlite3.Row) -> DwTable:
        return DwTable(
            id=row["id"],
            catalog=row["catalog"],
            db=row["db"],
            table_name=row["table_name"],
            description=row["description"],
            latest_statement_id=row["latest_statement_id"],
            created_at=_dt_from_sql(row["created_at"]),
            updated_at=_dt_from_sql(row["updated_at"]),
        )

    @staticmethod
    def _row_to_table_statement_log(row: sqlite3.Row) -> DwTableStatementLog:
        return DwTableStatementLog(
            id=row["id"],
            table_id=row["table_id"],
            statement_id=row["statement_id"],
            created_at=_dt_from_sql(row["created_at"]),
        )

    @staticmethod
    def _row_to_table_lineage(row: sqlite3.Row) -> DwTableLineage:
        return DwTableLineage(
            id=row["id"],
            source_table_id=row["source_table_id"],
            target_table_id=row["target_table_id"],
            statement_id=row["statement_id"],
            created_at=_dt_from_sql(row["created_at"]),
        )

    def create_ingest(self, data: DwSqlIngestCreate) -> DwSqlIngest:
        ingest_id = str(uuid.uuid4())
        created = _utc_now()
        created_iso = created.isoformat().replace("+00:00", "Z")
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO dw_sql_ingest (
                  id, created_at, thread_id, source_dialect, sql_hash, sql_text, extra_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ingest_id,
                    created_iso,
                    data.thread_id,
                    data.source_dialect,
                    data.sql_hash,
                    data.sql_text,
                    data.extra_json,
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
                    INSERT INTO dw_sql_statement (id, ingest_id, statement_index, kind, raw_json, sql_purpose, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (sid, ingest_id, stmt.statement_index, stmt.kind, stmt.raw_json, stmt.sql_purpose.value, created_iso),
                )
                out.append(
                    DwSqlStatement(
                        id=sid,
                        ingest_id=ingest_id,
                        statement_index=stmt.statement_index,
                        kind=stmt.kind,
                        raw_json=stmt.raw_json,
                        sql_purpose=stmt.sql_purpose,
                        created_at=created,
                    )
                )
            conn.commit()
        return out

    def upsert_table(self, data: DwTableCreate) -> DwTable:
        c_norm = normalize_sql_identifier(data.catalog)
        d_norm = normalize_sql_identifier(data.db) or ""
        t_norm = normalize_sql_identifier(data.table_name)
        if not t_norm or not str(t_norm).strip():
            raise ValueError("table_name normalizes to empty")

        now_iso = _utc_now().isoformat().replace("+00:00", "Z")

        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT id, created_at FROM dw_table
                WHERE catalog = ? AND db = ? AND table_name = ?
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
                      latest_statement_id = COALESCE(?, latest_statement_id),
                      updated_at = ?
                    WHERE id = ?
                    """,
                    (c_norm, d_norm, t_norm, data.description, data.latest_statement_id, now_iso, tid),
                )
            else:
                tid = str(uuid.uuid4())
                conn.execute(
                    """
                    INSERT INTO dw_table (
                      id, catalog, db, table_name, description, latest_statement_id, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        tid,
                        c_norm,
                        d_norm,
                        t_norm,
                        data.description,
                        data.latest_statement_id,
                        now_iso,
                        now_iso,
                    ),
                )
            conn.commit()

        table = self.get_table_by_key(c_norm, d_norm, t_norm)
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

    def get_table_by_key(self, catalog: str | None, db: str, table_name: str) -> DwTable | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM dw_table WHERE catalog = ? AND db = ? AND table_name = ?",
                (normalize_sql_identifier(catalog), normalize_sql_identifier(db) or "", normalize_sql_identifier(table_name)),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_table(row)

    def ingest_sql_text(
        self,
        sql: str,
        *,
        source_dialect: str | None = None,
        thread_id: str | None = None,
        extra_json: str | None = None,
    ) -> tuple[DwSqlIngest, list[DwSqlStatement], list[DwTable]]:
        """Parse SQL, persist ingest + statements, upsert referenced physical tables, and write history logs."""
        payload = parse_sql_metadata(sql, source_dialect=source_dialect)
        if not payload.get("ok"):
            err = payload.get("error", {})
            msg = err.get("message", "parse failed")
            raise ValueError(msg)
        parsed_statements = parse_sql_statements(sql, source_dialect=source_dialect)
        ingest_create = DwSqlIngestCreate(
            sql_hash=sql_content_hash(sql),
            thread_id=thread_id,
            source_dialect=source_dialect,
            sql_text=sql,
            extra_json=extra_json,
        )

        statements_payload: list[DwSqlStatementCreate] = []
        for stmt in payload["statements"]:
            idx = stmt["index"]
            kind = stmt.get("kind")
            if idx < len(parsed_statements):
                resolved_kind = resolve_statement_kind(parsed_statements[idx])
                if resolved_kind is not None and resolved_kind != kind:
                    kind = resolved_kind
            raw_json = json.dumps(stmt, ensure_ascii=False)
            purpose = infer_sql_purpose_from_kind(kind)
            statements_payload.append(DwSqlStatementCreate(statement_index=idx, kind=kind, raw_json=raw_json, sql_purpose=purpose))

        ingest = self.create_ingest(ingest_create)
        stmts = self.create_statements(ingest.id, statements_payload)

        tables_by_id: dict[str, DwTable] = {}
        stmt_by_index: dict[int, DwSqlStatement] = {s.statement_index: s for s in stmts}

        for stmt in payload["statements"]:
            stmt_idx = int(stmt["index"])
            stmt_row = stmt_by_index.get(stmt_idx)
            if stmt_row is None:
                continue

            cte_norm: set[str] = {normalize_sql_identifier(n) for n in (stmt.get("cte_names") or []) if n}

            for t in stmt.get("tables") or []:
                name = t.get("name")
                if name is None or not str(name).strip():
                    continue

                name_norm = normalize_sql_identifier(str(name))
                if name_norm and name_norm in cte_norm:
                    continue  # do not record virtual tables (CTEs)

                purpose = stmt_row.sql_purpose
                latest_statement_id = stmt_row.id if purpose in {SqlPurpose.CREATE, SqlPurpose.ALTER} else None

                tbl = self.upsert_table(DwTableCreate(catalog=t.get("catalog"), db=t.get("db"), table_name=str(name), latest_statement_id=latest_statement_id))
                tables_by_id[tbl.id] = tbl

                self.create_table_statement_log(DwTableStatementLogCreate(table_id=tbl.id, statement_id=stmt_row.id))

            if not supports_lineage(stmt_row.sql_purpose.value):
                continue

            if stmt_idx >= len(parsed_statements):
                continue
            statement_expr = parsed_statements[stmt_idx]

            target_keys = extract_target_table_keys(statement_expr, cte_norm)
            if not target_keys:
                continue

            source_keys = extract_source_table_keys(stmt, cte_norm, target_keys)
            if not source_keys:
                continue

            for source_key in source_keys:
                source_tbl = self.get_table_by_key(source_key[0], source_key[1], source_key[2])
                if source_tbl is None:
                    continue
                for target_key in target_keys:
                    target_tbl = self.get_table_by_key(target_key[0], target_key[1], target_key[2])
                    if target_tbl is None or source_tbl.id == target_tbl.id:
                        continue
                    self.create_table_lineage(
                        DwTableLineageCreate(
                            source_table_id=source_tbl.id,
                            target_table_id=target_tbl.id,
                            statement_id=stmt_row.id,
                        )
                    )

        return ingest, stmts, list(tables_by_id.values())

    def create_table_statement_log(self, data: DwTableStatementLogCreate) -> DwTableStatementLog:
        log_id = str(uuid.uuid4())
        created = _utc_now()
        created_iso = created.isoformat().replace("+00:00", "Z")
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO dw_table_statement_log (id, table_id, statement_id, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (log_id, data.table_id, data.statement_id, created_iso),
            )
            conn.commit()
            row = conn.execute("SELECT * FROM dw_table_statement_log WHERE table_id = ? AND statement_id = ?", (data.table_id, data.statement_id)).fetchone()
        assert row is not None
        return self._row_to_table_statement_log(row)

    def create_table_lineage(self, data: DwTableLineageCreate) -> DwTableLineage:
        lineage_id = str(uuid.uuid4())
        created = _utc_now()
        created_iso = created.isoformat().replace("+00:00", "Z")
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO dw_table_lineage (id, source_table_id, target_table_id, statement_id, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (lineage_id, data.source_table_id, data.target_table_id, data.statement_id, created_iso),
            )
            conn.commit()
            row = conn.execute(
                """
                SELECT * FROM dw_table_lineage
                WHERE source_table_id = ? AND target_table_id = ? AND statement_id = ?
                """,
                (data.source_table_id, data.target_table_id, data.statement_id),
            ).fetchone()
        assert row is not None
        return self._row_to_table_lineage(row)

    def list_table_lineage_for_statement(self, statement_id: str) -> list[DwTableLineage]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM dw_table_lineage WHERE statement_id = ? ORDER BY created_at ASC",
                (statement_id,),
            ).fetchall()
        return [self._row_to_table_lineage(r) for r in rows]

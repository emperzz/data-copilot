"""Pydantic models for the minimal DW catalog schema."""

from __future__ import annotations

from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field, computed_field


class SqlOperationCategory(str, Enum):
    """First-level grouping aligned with ANSI SQL / warehouse practice (DDL/DML/DQL/DCL/TCL).

    Derived from ``SqlPurpose`` (stored per-statement).
    """

    DDL = "ddl"
    DML = "dml"
    DQL = "dql"
    DCL = "dcl"
    TCL = "tcl"
    UTILITY = "utility"
    OTHER = "other"


class SqlPurpose(str, Enum):
    """Fine-grained SQL / warehouse operation label stored on each statement."""

    # DDL — object definition (ANSI SQL + common engines)
    CREATE = "create"
    ALTER = "alter"
    DROP = "drop"
    TRUNCATE = "truncate"
    RENAME = "rename"
    COMMENT = "comment"
    # DML — row / dataset changes
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"
    MERGE = "merge"
    COPY = "copy"
    # DQL — reads & plan inspection
    QUERY = "query"
    EXPLAIN = "explain"
    # DCL
    GRANT = "grant"
    REVOKE = "revoke"
    # TCL
    TRANSACTION = "transaction"
    # Warehouse maintenance / table services (ANALYZE, VACUUM, OPTIMIZE, MSCK, etc.)
    ANALYZE = "analyze"
    OPTIMIZE = "optimize"
    # Fallback
    UNKNOWN = "unknown"

    @classmethod
    def coerce(cls, value: str | None) -> SqlPurpose:
        """Parse a stored or caller-supplied label; unknown strings become ``UNKNOWN``."""
        if value is None or not str(value).strip():
            return cls.UNKNOWN
        key = str(value).strip().lower()
        try:
            return cls(key)
        except ValueError:
            return cls.UNKNOWN


def infer_sql_purpose_from_kind(kind: str | None) -> SqlPurpose:
    """Infer SqlPurpose from sqlglot statement expression class name."""
    if kind is None or not str(kind).strip():
        return SqlPurpose.UNKNOWN
    key = str(kind).strip().lower()

    mapping = {
        "create": SqlPurpose.CREATE,
        "alter": SqlPurpose.ALTER,
        "drop": SqlPurpose.DROP,
        "truncate": SqlPurpose.TRUNCATE,
        "rename": SqlPurpose.RENAME,
        "comment": SqlPurpose.COMMENT,
        "insert": SqlPurpose.INSERT,
        "update": SqlPurpose.UPDATE,
        "delete": SqlPurpose.DELETE,
        "merge": SqlPurpose.MERGE,
        "copy": SqlPurpose.COPY,
        "select": SqlPurpose.QUERY,
        "union": SqlPurpose.QUERY,
        "except": SqlPurpose.QUERY,
        "intersect": SqlPurpose.QUERY,
        "explain": SqlPurpose.EXPLAIN,
        "grant": SqlPurpose.GRANT,
        "revoke": SqlPurpose.REVOKE,
        "transaction": SqlPurpose.TRANSACTION,
        "analyze": SqlPurpose.ANALYZE,
        "optimize": SqlPurpose.OPTIMIZE,
    }
    return mapping.get(key, SqlPurpose.UNKNOWN)


def sql_purpose_to_category(purpose: SqlPurpose) -> SqlOperationCategory:
    """Map a fine-grained purpose to an ANSI-style operation category."""
    ddl = {
        SqlPurpose.CREATE,
        SqlPurpose.ALTER,
        SqlPurpose.DROP,
        SqlPurpose.TRUNCATE,
        SqlPurpose.RENAME,
        SqlPurpose.COMMENT,
    }
    dml = {SqlPurpose.INSERT, SqlPurpose.UPDATE, SqlPurpose.DELETE, SqlPurpose.MERGE, SqlPurpose.COPY}
    dql = {SqlPurpose.QUERY, SqlPurpose.EXPLAIN}
    dcl = {SqlPurpose.GRANT, SqlPurpose.REVOKE}
    tcl = {SqlPurpose.TRANSACTION}
    utility = {SqlPurpose.ANALYZE, SqlPurpose.OPTIMIZE}

    if purpose in ddl:
        return SqlOperationCategory.DDL
    if purpose in dml:
        return SqlOperationCategory.DML
    if purpose in dql:
        return SqlOperationCategory.DQL
    if purpose in dcl:
        return SqlOperationCategory.DCL
    if purpose in tcl:
        return SqlOperationCategory.TCL
    if purpose in utility:
        return SqlOperationCategory.UTILITY
    return SqlOperationCategory.OTHER


class DwSqlIngestCreate(BaseModel):
    sql_hash: str = Field(..., min_length=1)
    thread_id: str | None = None
    source_dialect: str | None = None
    sql_text: str | None = None
    extra_json: str | None = None


class DwSqlIngest(DwSqlIngestCreate):
    id: str
    created_at: datetime


class DwSqlStatementCreate(BaseModel):
    statement_index: int = Field(..., ge=0)
    kind: str | None = None
    raw_json: str = Field(..., min_length=1, description="JSON payload for one statement slice")
    sql_purpose: SqlPurpose = SqlPurpose.UNKNOWN


class DwSqlStatement(DwSqlStatementCreate):
    id: str
    ingest_id: str
    created_at: datetime

    @computed_field
    @property
    def sql_operation_category(self) -> SqlOperationCategory:
        return sql_purpose_to_category(self.sql_purpose)


class DwTableCreate(BaseModel):
    catalog: str | None = None
    db: str | None = Field(default=None, description="Normalized database/schema name (optional)")
    table_name: str = Field(..., min_length=1)
    description: str | None = None
    latest_statement_id: str | None = None


class DwTable(DwTableCreate):
    id: str
    created_at: datetime
    updated_at: datetime


class DwTableStatementLogCreate(BaseModel):
    table_id: str = Field(..., min_length=1)
    statement_id: str = Field(..., min_length=1)


class DwTableStatementLog(DwTableStatementLogCreate):
    id: str
    created_at: datetime


class DwTableLineageCreate(BaseModel):
    source_table_id: str = Field(..., min_length=1)
    target_table_id: str = Field(..., min_length=1)
    statement_id: str = Field(..., min_length=1)


class DwTableLineage(DwTableLineageCreate):
    id: str
    created_at: datetime

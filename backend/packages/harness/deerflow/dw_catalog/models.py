"""Pydantic models for the minimal DW catalog schema."""

from __future__ import annotations

from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field


class SqlPurpose(str, Enum):
    """High-level intent for an ingest (user- or caller-supplied)."""

    BUILD = "build"
    QUERY = "query"
    UNKNOWN = "unknown"


class DwSqlIngestCreate(BaseModel):
    source_type: str = Field(..., description="e.g. thread_upload, direct_text, scan_path")
    sql_hash: str = Field(..., min_length=1)
    thread_id: str | None = None
    source_path: str | None = None
    source_dialect: str | None = None
    sql_text: str | None = None
    extra_json: str | None = None
    sql_purpose: SqlPurpose = SqlPurpose.UNKNOWN


class DwSqlIngest(DwSqlIngestCreate):
    id: str
    created_at: datetime


class DwSqlStatementCreate(BaseModel):
    statement_index: int = Field(..., ge=0)
    kind: str | None = None
    raw_json: str = Field(..., min_length=1, description="JSON payload for one statement slice")


class DwSqlStatement(DwSqlStatementCreate):
    id: str
    ingest_id: str
    created_at: datetime


class DwTableCreate(BaseModel):
    catalog: str | None = None
    db: str | None = None
    table_name: str = Field(..., min_length=1)
    description: str | None = None


class DwTable(DwTableCreate):
    id: str
    catalog_norm: str
    db_norm: str
    table_name_norm: str
    created_at: datetime
    updated_at: datetime

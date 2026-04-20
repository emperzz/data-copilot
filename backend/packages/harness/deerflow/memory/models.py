"""Shared memory models for layered memory management."""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, Field

DEFAULT_MEMORY_USER = "default-user"
DEFAULT_MEMORY_AGENT = "unknown-agent"

TITLE_MAX_LENGTH = 500


class MemoryTier(StrEnum):
    RAW = "raw"
    DISTILLED = "distilled"
    CORE = "core"


class StructuredMemoryWriteError(ValueError):
    """User- or agent-facing validation failure for a structured memory write."""


class MemoryRecordCommon(BaseModel):
    """Common fields shared by all memory tiers."""

    id: str = Field(..., min_length=1)
    title: str = Field(..., min_length=1, max_length=TITLE_MAX_LENGTH)
    content: str = Field(..., min_length=1)
    created_at: str = Field(..., min_length=1)
    updated_at: str = Field(..., min_length=1)
    tags: list[str] = Field(default_factory=list)
    source_agent: str = Field(default=DEFAULT_MEMORY_AGENT, min_length=1)
    user: str = Field(default=DEFAULT_MEMORY_USER, min_length=1)


class RawMemoryRecord(MemoryRecordCommon):
    """Raw memory derived from a single conversation thread.

    The agent may fold user uploads (files, images) and inline URLs from that
    dialogue into one or more raw rows; provenance stays anchored on
    ``source_thread_id`` with optional path/URL lists for attachments cited
    in the organized content.
    """

    source_thread_id: str = Field(..., min_length=1)
    attachment_file_paths: list[str] = Field(default_factory=list)
    attachment_image_paths: list[str] = Field(default_factory=list)
    inline_web_urls: list[str] = Field(default_factory=list)


class DistilledMemoryRecord(MemoryRecordCommon):
    """Distilled memory produced from one or more raw rows."""

    raw_memory_ids: list[str] = Field(..., min_length=1)


class CoreMemoryRecord(MemoryRecordCommon):
    """Core memory synthesized from one or more distilled rows."""

    distilled_memory_ids: list[str] = Field(..., min_length=1)


class TagManifestRecord(BaseModel):
    """Persisted manifest entry for one tag (counts + extension fields).

    The counts represent how many records per tier currently carry this tag.
    ``definition`` and ``scope_keywords`` are reserved for the upcoming
    ``TagDefinitionService``; they default to empty in this release so the
    schema can accept future metadata without another migration.
    """

    tag: str = Field(..., min_length=1)
    counts_by_tier: dict[str, int] = Field(default_factory=dict)
    total: int = Field(default=0, ge=0)
    created_at: str = Field(..., min_length=1)
    updated_at: str = Field(..., min_length=1)
    definition: str = Field(default="")
    scope_keywords: list[str] = Field(default_factory=list)

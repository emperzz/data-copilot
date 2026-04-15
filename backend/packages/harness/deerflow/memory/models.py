"""Shared memory models for layered memory management."""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, Field

DEFAULT_MEMORY_USER = "default-user"
DEFAULT_MEMORY_AGENT = "unknown-agent"

TITLE_MAX_LENGTH = 500


class MemoryTier(StrEnum):
    """Supported memory tiers."""

    RAW = "raw"
    DISTILLED = "distilled"
    CORE = "core"


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

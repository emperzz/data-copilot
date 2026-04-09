"""Shared memory models for layered memory management."""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field, model_validator

DEFAULT_MEMORY_USER = "default-user"
DEFAULT_MEMORY_AGENT = "unknown-agent"


class MemoryTier(str, Enum):
    """Supported memory tiers."""

    RAW = "raw"
    DISTILLED = "distilled"
    CORE = "core"


class RawMemoryKind(str, Enum):
    """Supported raw memory source kinds."""

    SESSION = "session"
    FILE = "file"
    IMAGE = "image"
    WEBPAGE = "webpage"


class MemoryRecordCommon(BaseModel):
    """Common fields shared by all memory tiers."""

    id: str = Field(..., min_length=1)
    content: str = Field(..., min_length=1)
    created_at: str = Field(..., min_length=1)
    updated_at: str = Field(..., min_length=1)
    tags: list[str] = Field(default_factory=list)
    source_agent: str = Field(default=DEFAULT_MEMORY_AGENT, min_length=1)
    user: str = Field(default=DEFAULT_MEMORY_USER, min_length=1)


class RawMemoryRecord(MemoryRecordCommon):
    """Raw memory entry sourced from sessions/files/images/webpages."""

    raw_kind: RawMemoryKind
    source_thread_id: str | None = None
    source_file_path: str | None = None
    source_image_path: str | None = None
    source_web_url: str | None = None

    @model_validator(mode="after")
    def validate_source_link(self) -> RawMemoryRecord:
        """Require source links according to raw kind."""
        if self.raw_kind == RawMemoryKind.SESSION and not self.source_thread_id:
            raise ValueError("Session raw memory requires source_thread_id.")
        if self.raw_kind == RawMemoryKind.FILE and not self.source_file_path:
            raise ValueError("File raw memory requires source_file_path.")
        if self.raw_kind == RawMemoryKind.IMAGE and not self.source_image_path:
            raise ValueError("Image raw memory requires source_image_path.")
        if self.raw_kind == RawMemoryKind.WEBPAGE and not self.source_web_url:
            raise ValueError("Webpage raw memory requires source_web_url.")
        return self


class DistilledMemoryRecord(MemoryRecordCommon):
    """Distilled memory entry generated from one or more raw memories."""

    raw_memory_ids: list[str] = Field(..., min_length=1)


class CoreMemoryRecord(MemoryRecordCommon):
    """Core memory entry generated from one or more distilled memories."""

    distilled_memory_ids: list[str] = Field(..., min_length=1)

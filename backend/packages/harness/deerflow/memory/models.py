from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class MemoryMetadata(BaseModel):
    namespace: str
    memory_key: str | None = None
    source_skill: str | None = None
    extra: dict[str, Any] = Field(default_factory=dict)


class MemoryItem(BaseModel):
    id: str
    content: str
    metadata: MemoryMetadata
    created_at: datetime
    updated_at: datetime


class MemorySearchHit(BaseModel):
    item: MemoryItem
    score: float

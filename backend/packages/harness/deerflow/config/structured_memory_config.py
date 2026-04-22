"""Configuration for Chroma-backed structured memory (parallel to session memory)."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

StructuredMemoryStoreName = Literal["chroma"]

DEFAULT_MAX_CONTENT_LENGTH = 100_000


class StructuredMemoryDisabledError(RuntimeError):
    """Raised when code requests the repository while structured memory is disabled."""


class StructuredMemoryWriteConfig(BaseModel):
    """Write-path limits for structured memory (tool + service)."""

    max_content_length: int = Field(
        default=DEFAULT_MAX_CONTENT_LENGTH,
        ge=256,
        le=10_000_000,
        description="Maximum UTF-8 character length for a single memory record body.",
    )


class StructuredMemoryQueryConfig(BaseModel):
    """Query-path defaults for structured memory search tools."""

    default_top_k: int = Field(
        default=5,
        ge=1,
        le=100,
        description="Default number of results returned when top_k is not specified.",
    )
    max_top_k: int = Field(
        default=20,
        ge=1,
        le=200,
        description="Hard ceiling for a single query to prevent runaway token cost.",
    )
    default_tiers: list[str] = Field(
        default_factory=lambda: ["core", "distilled"],
        description="Tiers searched when the caller omits tier_filter (avoids raw noise by default).",
    )


class StructuredMemoryTagManifestConfig(BaseModel):
    """Tag manifest snapshot + prompt injection controls.

    The manifest summarises which tags exist and how many records per tier use
    each tag. It is produced by scanning the repository once and incrementally
    updated on successful writes/updates/deletes; a TTL triggers a periodic
    rescan to repair drift.
    """

    inject_in_prompt: bool = Field(
        default=True,
        description="When true, append a rendered tag manifest to <structured_memory_system>.",
    )
    cache_ttl_seconds: int = Field(
        default=60,
        ge=0,
        le=86_400,
        description="Process-level cache TTL; 0 disables caching and forces a rescan per snapshot.",
    )
    max_tags_in_prompt: int = Field(
        default=80,
        ge=1,
        le=1000,
        description="Upper bound on tags rendered into the system prompt (top by total count).",
    )


class StructuredMemoryConfig(BaseModel):
    """Structured memory subsystem: independent of ``memory.enabled`` (session memory)."""

    enabled: bool = Field(
        default=True,
        description="Master switch for structured memory (Chroma tiers). Off disables repository access.",
    )
    store: StructuredMemoryStoreName = Field(
        default="chroma",
        description="Backend for structured memory. Only ``chroma`` is supported in this release.",
    )
    embedding_model_name: str = Field(
        default="all-MiniLM-L6-v2",
        min_length=1,
        description="Sentence-Transformers model name used by Chroma embedding function.",
    )
    write: StructuredMemoryWriteConfig = Field(
        default_factory=StructuredMemoryWriteConfig,
        description="Write tool and service limits.",
    )
    query: StructuredMemoryQueryConfig = Field(
        default_factory=StructuredMemoryQueryConfig,
        description="Query tool defaults and limits.",
    )
    tag_manifest: StructuredMemoryTagManifestConfig = Field(
        default_factory=StructuredMemoryTagManifestConfig,
        description="Tag manifest cache and prompt injection controls.",
    )


_structured_memory_config: StructuredMemoryConfig = StructuredMemoryConfig()


def get_structured_memory_config() -> StructuredMemoryConfig:
    return _structured_memory_config


def set_structured_memory_config(config: StructuredMemoryConfig) -> None:
    global _structured_memory_config
    _structured_memory_config = config


def load_structured_memory_config_from_dict(config_dict: dict) -> None:
    global _structured_memory_config
    _structured_memory_config = StructuredMemoryConfig(**config_dict)

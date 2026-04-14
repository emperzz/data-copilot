"""Configuration for Chroma-backed structured memory (parallel to session memory)."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

StructuredMemoryStoreName = Literal["chroma"]


class StructuredMemoryDisabledError(RuntimeError):
    """Raised when code requests the repository while structured memory is disabled."""


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


_structured_memory_config: StructuredMemoryConfig = StructuredMemoryConfig()


def get_structured_memory_config() -> StructuredMemoryConfig:
    return _structured_memory_config


def set_structured_memory_config(config: StructuredMemoryConfig) -> None:
    global _structured_memory_config
    _structured_memory_config = config


def load_structured_memory_config_from_dict(config_dict: dict) -> None:
    global _structured_memory_config
    _structured_memory_config = StructuredMemoryConfig(**config_dict)

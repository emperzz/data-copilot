"""Configuration for enterprise structured memory."""

from pydantic import BaseModel, Field


class StructuredMemoryConfig(BaseModel):
    """Configuration for enterprise structured memory mechanism."""

    enabled: bool = Field(
        default=False,
        description="Whether to enable structured memory",
    )
    storage_path: str = Field(
        default="",
        description=(
            "Path to store structured memory data. "
            "If empty, defaults to `{base_dir}/structured_memory/`."
        ),
    )
    injection_enabled: bool = Field(
        default=True,
        description="Whether to inject structured memory index into system prompt",
    )
    max_index_tokens: int = Field(
        default=1500,
        ge=100,
        le=8000,
        description="Maximum tokens for structured memory index injection",
    )


def get_structured_memory_config() -> StructuredMemoryConfig:
    """Get the current structured memory configuration from AppConfig."""
    from deerflow.config import get_app_config

    app_config = get_app_config()
    return getattr(app_config, "structured_memory", StructuredMemoryConfig())
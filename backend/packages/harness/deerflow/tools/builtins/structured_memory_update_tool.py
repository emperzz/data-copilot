"""Structured memory update tool."""

from __future__ import annotations

from langchain.tools import tool

from deerflow.memory.structured_memory_mutation_service import (
    StructuredMemoryMutationError,
    StructuredMemoryMutationService,
    format_mutation_success,
)


@tool("structured_memory_update", parse_docstring=True)
def structured_memory_update_tool(
    memory_id: str,
    title: str,
    content: str,
    tags: list[str] | None = None,
    source_agent: str | None = None,
    user: str | None = None,
) -> str:
    """Update one existing structured memory record by id.

    Use this when the knowledge is still valid but needs correction or refresh.
    It updates only common fields (title/content/tags/source_agent/user) and
    preserves tier-specific lineage fields.

    Args:
        memory_id: Exact record id (raw_..., distilled_..., core_...).
        title: New short label for the record.
        content: New main body text.
        tags: Optional replacement tag list.
        source_agent: Optional replacement writer name.
        user: Optional replacement user id.
    """
    service = StructuredMemoryMutationService()
    try:
        result = service.update_memory(
            memory_id=memory_id,
            title=title,
            content=content,
            tags=tags,
            source_agent=source_agent,
            user=user,
        )
    except StructuredMemoryMutationError as exc:
        return f"structured_memory_update failed: {exc}"
    except Exception as exc:
        return f"structured_memory_update error: {exc}"
    return format_mutation_success(result)

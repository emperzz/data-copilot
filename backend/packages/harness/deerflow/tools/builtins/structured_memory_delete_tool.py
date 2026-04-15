"""Structured memory delete tool."""

from __future__ import annotations

from langchain.tools import tool

from deerflow.memory.structured_memory_mutation_service import (
    StructuredMemoryMutationError,
    StructuredMemoryMutationService,
    format_mutation_success,
)


@tool("structured_memory_delete", parse_docstring=True)
def structured_memory_delete_tool(memory_id: str) -> str:
    """Delete one structured memory record by id.

    Delete is blocked when the record is referenced by downstream lineage:
    - raw referenced by distilled
    - distilled referenced by core

    Delete downstream records first, or update their lineage before retrying.

    Args:
        memory_id: Exact record id (raw_..., distilled_..., core_...).
    """
    service = StructuredMemoryMutationService()
    try:
        result = service.delete_memory(memory_id=memory_id)
    except StructuredMemoryMutationError as exc:
        return f"structured_memory_delete failed: {exc}"
    except Exception as exc:
        return f"structured_memory_delete error: {exc}"
    return format_mutation_success(result)

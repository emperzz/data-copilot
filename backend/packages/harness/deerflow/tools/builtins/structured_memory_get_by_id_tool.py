"""Structured memory exact-id lookup with optional upstream lineage."""

from __future__ import annotations

from langchain.tools import tool

from deerflow.memory.structured_memory_search_service import (
    StructuredMemorySearchService,
)


@tool("structured_memory_get_by_id", parse_docstring=True)
def structured_memory_get_by_id_tool(
    memory_id: str,
    include_upstream: bool = False,
) -> str:
    """Retrieve a single structured memory record by its exact id.

    When ``include_upstream`` is true, the response also contains the
    upstream lineage records (core → distilled → raw) so you can drill
    into the detailed source material behind a high-level abstraction.

    Typical workflow:
    1. ``structured_memory_query`` returns a core/distilled hit that lacks detail.
    2. Call this tool with the hit's id and ``include_upstream=True``.
    3. Read the attached raw records for full context.

    Args:
        memory_id: Exact id of the record to fetch (e.g. ``core_abc123...``).
        include_upstream: If true, attach upstream lineage (up to 2 levels deep).
    """
    service = StructuredMemorySearchService()
    try:
        result = service.get_by_id(memory_id, include_upstream=include_upstream)
    except Exception as exc:
        return f"structured_memory_get_by_id error: {exc}"
    if result is None:
        return f"No structured memory record found with id={memory_id!r}."
    return StructuredMemorySearchService.format_lineage(result)

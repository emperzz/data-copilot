"""Structured memory query tool – vector search with metadata filters."""

from __future__ import annotations

from langchain.tools import tool

from deerflow.memory.structured_memory_search_service import (
    StructuredMemorySearchError,
    StructuredMemorySearchService,
)


@tool("structured_memory_query", parse_docstring=True)
def structured_memory_query_tool(
    query_text: str,
    tier_filter: list[str] | None = None,
    tags: list[str] | None = None,
    top_k: int | None = None,
) -> str:
    """Search structured memory for business knowledge relevant to a query.

    Returns the top matching records ranked by semantic similarity.
    Use ``tier_filter`` and ``tags`` to narrow scope. Default tiers are
    core + distilled (raw is excluded to reduce noise unless you need it).

    Before calling this tool, consider calling ``structured_memory_list_tags``
    to discover available tag categories so you can filter effectively.

    Args:
        query_text: Natural-language query describing what you are looking for.
        tier_filter: Optional list of tiers to search (raw, distilled, core).
        tags: Optional tag filters; only records with ALL listed tags are returned.
        top_k: Maximum results to return (default from config, hard ceiling enforced).
    """
    service = StructuredMemorySearchService()
    try:
        items = service.search(
            query_text=query_text,
            tier_filter=tier_filter,
            tags=tags,
            top_k=top_k,
        )
    except StructuredMemorySearchError as exc:
        return f"structured_memory_query failed: {exc}"
    except Exception as exc:
        return f"structured_memory_query error: {exc}"
    return StructuredMemorySearchService.format_search_results(items)

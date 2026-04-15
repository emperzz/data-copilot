"""Structured memory tag discovery tool."""

from __future__ import annotations

from langchain.tools import tool

from deerflow.memory.structured_memory_search_service import (
    StructuredMemorySearchService,
)


@tool("structured_memory_list_tags", parse_docstring=True)
def structured_memory_list_tags_tool(
    tier_filter: list[str] | None = None,
) -> str:
    """List all distinct tags stored in structured memory with per-tag record counts.

    Call this before ``structured_memory_query`` when you are unsure which tag
    categories exist, so you can build effective filters.

    Args:
        tier_filter: Optional list of tiers to scan (raw, distilled, core). Scans all tiers when omitted.
    """
    service = StructuredMemorySearchService()
    try:
        summaries = service.list_tags(tier_filter=tier_filter)
    except Exception as exc:
        return f"structured_memory_list_tags error: {exc}"
    return StructuredMemorySearchService.format_tag_list(summaries)

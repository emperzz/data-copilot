"""Text search across structured memory markdown files."""

import logging
from pathlib import Path

from deerflow.structured_memory.storage import get_structured_memory_store

logger = logging.getLogger(__name__)

SEARCHABLE_EXTENSIONS = {".md"}


def search_memory_files(query: str, category: str = "all") -> str:
    """Search memory files for a query string.

    Args:
        query: The search term(s). Simple case-insensitive substring match.
        category: 'facts', 'tasks', or 'all'.

    Returns:
        Formatted search results with file paths and matching line snippets.
    """
    store = get_structured_memory_store()

    search_roots: list[Path] = []
    if category in ("facts", "all"):
        facts_root = store.resolve_path("facts")
        if facts_root.exists():
            search_roots.append(facts_root)
    if category in ("tasks", "all"):
        tasks_root = store.resolve_path("tasks")
        if tasks_root.exists():
            search_roots.append(tasks_root)

    if not search_roots:
        return "No memory files found. The structured memory store is empty."

    query_lower = query.lower()
    results: list[str] = []
    max_results = 20

    for root in search_roots:
        for file_path in root.rglob("*"):
            if file_path.suffix not in SEARCHABLE_EXTENSIONS:
                continue
            if file_path.name.startswith("."):
                continue

            try:
                content = file_path.read_text(encoding="utf-8")
            except Exception:
                continue

            if query_lower in content.lower():
                rel_path = file_path.relative_to(store.root)
                lines = content.split("\n")
                matching_lines: list[str] = []
                for i, line in enumerate(lines):
                    if query_lower in line.lower():
                        snippet = line.strip()[:120]
                        matching_lines.append(f"  L{i+1}: {snippet}")
                        if len(matching_lines) >= 3:
                            break

                results.append(f"**{rel_path}** ({len(matching_lines)} matches)")
                results.extend(matching_lines)

                if len(results) >= max_results * 4:
                    break

        if len(results) >= max_results * 4:
            break

    if not results:
        return f"No matches found for '{query}'."

    header = f"Search results for '{query}' (category={category}):\n"
    return header + "\n".join(results[: max_results * 4])

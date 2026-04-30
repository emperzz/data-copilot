"""Auto-indexing service for structured memory entity files."""

import re
from typing import Literal

from deerflow.structured_memory.storage import StructuredMemoryStore


def get_index_path(entity_path: str) -> str | None:
    """Map an entity file path to its parent index file.

    Returns None if the entity path does not correspond to an auto-indexed
    category (e.g., root facts/index.md, README.md, etc.).
    """
    path = entity_path.strip("/")
    if path.startswith("facts/schema/tables/"):
        return "facts/schema/index.md"
    if path.startswith("facts/schema/fields/"):
        return "facts/schema/index.md"
    if path.startswith("facts/business/"):
        return "facts/business/index.md"
    if path.startswith("tasks/"):
        return "tasks/index.md"
    return None


def parse_entity_entry(content: str) -> tuple[str, str]:
    """Extract (title, description) from an entity file's markdown content.

    Title: first line matching '^#\\s+(.+)$' (the first markdown heading).
    Description heuristic:
      - Table files: first '**描述**:' field value
      - Task files: first non-empty line after the heading (up to 80 chars)
      - Business files: first '**定义**:' or first line after heading
      - Fallback: first non-empty, non-heading line (up to 80 chars)
    """
    lines = content.split("\n")
    title = ""
    for line in lines:
        m = re.match(r"^#\s+(.+)$", line)
        if m:
            title = m.group(1).strip()
            break

    if not title:
        return "", ""

    desc = _extract_description(content, title)
    return title, desc


def _extract_description(content: str, title: str) -> str:
    """Extract a short description from entity content.

    Strategy:
      1. Look for '**描述**:' in table/business files (literal '**描述**:' not '**描述:**')
      2. Look for '**类型**:' in task files
      3. Fall back to the first non-empty, non-heading line (max 80 chars)
    """
    # Try **目的**: (new table format — best short description for index)
    m = re.search(r"\*\*目的\*\*\s*:\s*(.+?)(?:\n|$)", content)
    if m:
        return m.group(1).strip()[:100]

    # Try **描述**: (legacy table/business definition)
    m = re.search(r"\*\*描述\*\*\s*:\s*(.+?)(?:\n|$)", content)
    if m:
        return m.group(1).strip()[:100]

    # Try **定义**: (business definition)
    m = re.search(r"\*\*定义\*\*\s*:\s*(.+?)(?:\n|$)", content)
    if m:
        return m.group(1).strip()[:100]

    # Fallback: first meaningful line after title
    lines = content.split("\n")
    for line in lines:
        stripped = line.strip()
        if stripped and not stripped.startswith("#") and not stripped.startswith("---"):
            return stripped[:80]

    return ""


def build_index_entry(entity_path: str, title: str, description: str) -> str:
    """Build a markdown list-item entry for an index file.

    The entry format is category-specific based on the entity path:
      - facts/schema/tables/X.md  →  '- [title](tables/X.md) — desc'
      - facts/schema/fields/X.md  →  '- [title](fields/X.md) — desc'
      - facts/business/X.md        →  '- [title](X.md) — desc'
      - tasks/YYYY/X.md            →  '- [title](YYYY/X.md) — desc'
    """
    entity = entity_path.strip("/")
    parts = entity.split("/")

    if entity.startswith("facts/schema/tables/"):
        filename = parts[-1]
        link = f"tables/{filename}"
    elif entity.startswith("facts/schema/fields/"):
        filename = parts[-1]
        link = f"fields/{filename}"
    elif entity.startswith("facts/business/"):
        filename = parts[-1]
        link = filename
    elif entity.startswith("tasks/"):
        # Keep the full relative path from tasks/ root
        link = "/".join(parts[1:])  # e.g. "2026/sales-q1.md"
    else:
        link = entity_path

    desc = description.strip() if description else ""
    return f"- [{title}]({link}) — {desc}"


def _read_index(store: StructuredMemoryStore, index_path: str) -> str:
    """Read an index file, returning empty string if it doesn't exist."""
    try:
        return store.read_file(index_path)
    except FileNotFoundError:
        return ""


def _write_index(store: StructuredMemoryStore, index_path: str, content: str) -> None:
    """Write an index file atomically, creating parent directories as needed."""
    store.write_file(index_path, content)


def _update_index(
    store: StructuredMemoryStore,
    index_path: str,
    action: Literal["add", "remove", "update"],
    entry: str,
    target: str = "",
) -> str:
    """Update an index file: add, remove, or update an entry.

    This is the storage-layer equivalent of the update_memory_index tool
    (no path traversal — index_path is always a known index file).
    """
    if action == "add":
        if not store.file_exists(index_path):
            _write_index(store, index_path, f"# Index\n\n{entry}\n")
            return f"Created {index_path} with entry: {entry}"
        content = _read_index(store, index_path)
        if entry.strip() in content:
            return f"Entry already exists in {index_path}"
        new_content = content.rstrip("\n") + "\n" + entry + "\n"
        _write_index(store, index_path, new_content)
        return f"Added to {index_path}: {entry}"

    elif action == "remove":
        if not target:
            return f"The 'target' parameter is required for 'remove' action."
        if not store.file_exists(index_path):
            return f"Index file not found: {index_path}"
        content = _read_index(store, index_path)
        lines = content.rstrip("\n").split("\n")
        new_lines = [l for l in lines if target.strip() not in l]
        if len(new_lines) == len(lines):
            return f"Target not found in {index_path}: {target}"
        _write_index(store, index_path, "\n".join(new_lines) + "\n")
        return f"Removed from {index_path}: {target}"

    elif action == "update":
        if not target:
            return f"The 'target' parameter is required for 'update' action."
        if not store.file_exists(index_path):
            return f"Index file not found: {index_path}"
        content = _read_index(store, index_path)
        lines = content.rstrip("\n").split("\n")
        found = False
        for i, line in enumerate(lines):
            if target.strip() in line:
                lines[i] = entry
                found = True
                break
        if not found:
            return f"Target not found in {index_path}: {target}"
        _write_index(store, index_path, "\n".join(lines) + "\n")
        return f"Updated {index_path}: replaced '{target.strip()}' with '{entry}'"

    return f"Unknown action: {action}"


def register_entity(
    store: StructuredMemoryStore,
    path: str,
    old_content: str | None,
    new_content: str,
) -> str:
    """Register an entity in its parent index after a write operation.

    If old_content is provided, this is an update: the old entry is removed
    before the new one is added. If old_content is None, this is a create.

    Returns a confirmation message describing what was changed.
    """
    index_path = get_index_path(path)
    if not index_path:
        return f"No auto-indexing for: {path}"

    new_title, new_desc = parse_entity_entry(new_content)
    new_entry = build_index_entry(path, new_title, new_desc)
    messages: list[str] = []

    if old_content:
        old_title, old_desc = parse_entity_entry(old_content)
        old_entry = build_index_entry(path, old_title, old_desc)
        msg = _update_index(store, index_path, "remove", entry=old_entry, target=old_entry)
        messages.append(msg)

    msg = _update_index(store, index_path, "add", entry=new_entry)
    messages.append(msg)

    return " | ".join(messages)


def unregister_entity(
    store: StructuredMemoryStore,
    path: str,
    content: str,
) -> str:
    """Remove an entity from its parent index before deletion.

    Returns a confirmation message describing what was changed.
    """
    index_path = get_index_path(path)
    if not index_path:
        return f"No auto-indexing for: {path}"

    title, desc = parse_entity_entry(content)
    entry = build_index_entry(path, title, desc)
    # For remove, target=entry (same string used to find and remove the line)
    msg = _update_index(store, index_path, "remove", entry=entry, target=entry)
    return msg

"""Built-in tools for enterprise structured memory management."""

import logging
import os
import sys
from typing import Literal

from langchain.tools import tool

# Platform detection for file locking
if sys.platform != "win32":
    import fcntl
    _LOCK_AVAILABLE = True
else:
    _LOCK_AVAILABLE = False

    class _NoOpLock:
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass
        def fileno(self):
            return -1

from deerflow.structured_memory import (
    StructuredMemoryStore,
    apply_partial_update,
    get_structured_memory_store,
    parse_changes_json,
    register_entity,
    search_memory_files,
    unregister_entity,
)

logger = logging.getLogger(__name__)


def _get_store() -> StructuredMemoryStore:
    store = get_structured_memory_store()
    store.ensure_directories()
    return store


def _acquire_entity_lock(lock_path: str) -> tuple:
    """Acquire exclusive lock on entity file. Returns (lock_obj, lock_path)."""
    if _LOCK_AVAILABLE:
        lock_file = open(lock_path, 'w')
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        return lock_file, lock_path
    else:
        return _NoOpLock(), ""


def _release_entity_lock(lock_obj, lock_path: str) -> None:
    """Release exclusive lock and close file."""
    if _LOCK_AVAILABLE:
        fcntl.flock(lock_obj.fileno(), fcntl.LOCK_UN)
        lock_obj.close()
        try:
            os.unlink(lock_path)
        except OSError:
            pass


@tool("search_structured_memory", parse_docstring=True)
def search_structured_memory(
    query: str,
    category: Literal["facts", "tasks", "all"] = "all",
) -> str:
    """Search structured memory files for keywords or text.

    Use this tool to find relevant enterprise knowledge before answering
    questions about data warehouse tables, business definitions, or past tasks.

    Args:
        query: The search term(s) to look for. Simple case-insensitive substring match.
        category: Which memory category to search. 'facts' for schema/business,
            'tasks' for task history, 'all' for everything.

    Returns:
        Formatted search results with file paths and matching line snippets.
    """
    try:
        return search_memory_files(query, category)
    except Exception as e:
        logger.exception("search_structured_memory failed")
        return f"Search failed: {e}"


@tool("get_memory_entity", parse_docstring=True)
def get_memory_entity(
    path: str,
) -> str:
    """Read the full content of a structured memory entity file.

    Use this to load detailed information about a specific table, business
    concept, or task after locating it via search or index browsing.

    Args:
        path: Relative path to the memory file, e.g.
            'facts/schema/tables/ods_order.md' or 'tasks/2026/task-2026-04-01.md'.

    Returns:
        The full markdown content of the memory file.
    """
    try:
        store = _get_store()
        return store.read_file(path)
    except FileNotFoundError:
        return f"Memory entity not found: {path}"
    except Exception as e:
        logger.exception("get_memory_entity failed")
        return f"Failed to read memory entity: {e}"


@tool("list_memory_entities", parse_docstring=True)
def list_memory_entities(
    path: str = "",
    depth: int = 2,
) -> str:
    """Browse the structured memory directory to see what entities exist.

    Use this tool to explore the memory structure, find available entities,
    or check the index files.

    Args:
        path: Relative path within the memory store. Empty string = root level.
        depth: How many directory levels to display (default 2).

    Returns:
        Tree-formatted directory listing.
    """
    try:
        store = _get_store()
        store.ensure_directories()
        return store.list_dir(path, depth)
    except Exception as e:
        logger.exception("list_memory_entities failed")
        return f"Failed to list memory entities: {e}"


@tool("update_memory_index", parse_docstring=True)
def update_memory_index(
    index_path: str,
    action: Literal["add", "remove", "update"],
    entry: str,
    target: str = "",
) -> str:
    """Safely update an index.md file in the structured memory store.

    Use this tool to add, remove, or update entries in index files.
    This ensures index format consistency. For writing full entity detail
    files, use the write_file tool directly.

    Args:
        index_path: Relative path to the index file, e.g. 'facts/schema/index.md'.
        action: 'add' to append a new entry, 'remove' to delete an entry by
            matching the target string, 'update' to replace an entry matching
            the target string with the new entry.
        entry: The markdown list item entry, e.g.
            '- [ods.order](tables/ods_order.md) — 订单明细表'
        target: For 'remove' and 'update' actions, the exact text to find
            and remove/replace.

    Returns:
        Confirmation message describing what was changed.
    """
    try:
        store = _get_store()
        index_target = store.resolve_path(index_path)
        lock_path = str(index_target) + ".lock"

        lock_obj, _ = _acquire_entity_lock(lock_path)
        try:
            if not store.file_exists(index_path):
                if action == "add":
                    store.write_file(index_path, f"# Index\n\n{entry}\n")
                    return f"Created {index_path} with entry: {entry}"
                return f"Index file not found: {index_path}"

            content = store.read_file(index_path)
            lines = content.rstrip("\n").split("\n")

            if action == "add":
                if entry.strip() in content:
                    return f"Entry already exists in {index_path}: {entry}"
                new_content = content.rstrip("\n") + "\n" + entry + "\n"
                store.write_file(index_path, new_content)
                return f"Added to {index_path}: {entry}"

            elif action == "remove":
                if not target:
                    return "The 'target' parameter is required for 'remove' action."
                new_lines = [l for l in lines if target.strip() not in l]
                if len(new_lines) == len(lines):
                    return f"Target not found in {index_path}: {target}"
                store.write_file(index_path, "\n".join(new_lines) + "\n")
                return f"Removed from {index_path}: {target}"

            elif action == "update":
                if not target:
                    return "The 'target' parameter is required for 'update' action."
                found = False
                for i, line in enumerate(lines):
                    if target.strip() in line:
                        lines[i] = entry
                        found = True
                        break
                if not found:
                    return f"Target not found in {index_path}: {target}"
                store.write_file(index_path, "\n".join(lines) + "\n")
                return f"Updated {index_path}: replaced '{target.strip()}' with '{entry}'"

            return f"Unknown action: {action}"
        finally:
            _release_entity_lock(lock_obj, lock_path)
    except ValueError as e:
        return f"Invalid path: {e}"
    except Exception as e:
        logger.exception("update_memory_index failed")
        return f"Failed to update index: {e}"


@tool("write_memory_entity", parse_docstring=True)
def write_memory_entity(
    path: str,
    content: str,
    changes: str = "",
    timeline_desc: str = "",
) -> str:
    """Write (create or partially update) a structured memory entity file.

    Use this tool to create or update entity detail files in the structured
    memory store. This writes to the actual structured memory store directory,
    NOT the sandbox workspace. The corresponding index entry is updated
    automatically.

    For UPDATING an existing table entity, prefer using the `changes` and
    `timeline_desc` parameters for partial updates -- only the specified
    fields will be modified, unchanged fields are preserved, and a new
    timeline entry is appended.

    Args:
        path: Relative path within the memory store
        content: The full markdown content for initial creation, or complete
            overwrite when changes is empty
        changes: JSON dict of changed fields for partial update (e.g.
            definition and columns). Only fields in this dict are modified.
            Use ``column_updates`` (list of {name, description}) to update
            individual column descriptions without replacing the full list.
        timeline_desc: Description for the new timeline entry when using
            partial update

    Returns:
        Confirmation message with the path written and index update result.
    """
    try:
        store = _get_store()
        target = store.resolve_path(path)
        lock_path = str(target) + ".lock"

        lock_obj, _ = _acquire_entity_lock(lock_path)
        try:
            # Capture old content for update detection
            old_content: str | None = None
            try:
                old_content = store.read_file(path)
            except FileNotFoundError:
                pass

            # Determine final content based on mode
            final_content: str
            changes_dict = parse_changes_json(changes)

            if old_content and changes_dict:
                # Partial update mode: merge only changed fields
                final_content = apply_partial_update(old_content, changes_dict, timeline_desc)
            else:
                # Full write mode (initial creation or explicit overwrite)
                final_content = content

            if not final_content:
                return "Error: content cannot be empty when creating a new entity. Use changes parameter for partial updates."

            store.write_file(path, final_content)
            index_msg = register_entity(store, path, old_content, final_content)
            return f"Memory entity written: {path} | {index_msg}"
        finally:
            _release_entity_lock(lock_obj, lock_path)
    except ValueError as e:
        return f"Invalid path: {e}"
    except Exception as e:
        logger.exception("write_memory_entity failed")
        return f"Failed to write memory entity: {e}"


@tool("delete_memory_entity", parse_docstring=True)
def delete_memory_entity(
    path: str,
) -> str:
    """Delete a structured memory entity file.

    Use this tool to remove an entity detail file from the structured memory
    store. The corresponding index entry is removed automatically.

    Args:
        path: Relative path to the memory file to delete, e.g.
            'facts/schema/tables/ods_order.md'.

    Returns:
        Confirmation message describing deletion and index cleanup.
    """
    try:
        store = _get_store()
        target = store.resolve_path(path)

        lock_path = str(target) + ".lock"
        lock_obj, _ = _acquire_entity_lock(lock_path)
        try:
            if not target.exists():
                return f"Memory entity not found: {path}"

            content = target.read_text(encoding="utf-8")
            target.unlink()
            index_msg = unregister_entity(store, path, content)
            return f"Memory entity deleted: {path} | {index_msg}"
        finally:
            _release_entity_lock(lock_obj, lock_path)
    except ValueError as e:
        return f"Invalid path: {e}"
    except Exception as e:
        logger.exception("delete_memory_entity failed")
        return f"Failed to delete memory entity: {e}"

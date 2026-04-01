"""Built-in memory tools backed by chroma memory repository."""

from __future__ import annotations

import json
from typing import Any

from langchain.tools import tool

from deerflow.memory import get_memory_repository


def _parse_metadata_json(metadata_json: str | None) -> dict[str, Any]:
    if metadata_json is None or not metadata_json.strip():
        return {}
    raw = json.loads(metadata_json)
    if not isinstance(raw, dict):
        raise ValueError("metadata_json must be a JSON object")
    return raw


@tool("memory_upsert", parse_docstring=True)
def memory_upsert(
    namespace: str,
    content: str,
    memory_key: str | None = None,
    source_skill: str | None = None,
    metadata_json: str | None = None,
) -> str:
    """Create or update one memory item in a namespace.

    Args:
        namespace: Logical namespace (for example: "skill:datawarehouse-processor").
        content: Memory content text to store.
        memory_key: Optional business key for idempotent upsert.
        source_skill: Optional skill name that produced this memory.
        metadata_json: Optional metadata JSON object string.
    """
    metadata = _parse_metadata_json(metadata_json)
    item = get_memory_repository().upsert_memory(
        namespace=namespace,
        content=content,
        memory_key=memory_key,
        source_skill=source_skill,
        metadata=metadata,
    )
    return json.dumps(
        {
            "ok": True,
            "item": item.model_dump(mode="json"),
        },
        ensure_ascii=False,
    )


@tool("memory_search", parse_docstring=True)
def memory_search(namespace: str, query: str, limit: int = 5) -> str:
    """Search memory items from one namespace by semantic similarity.

    Args:
        namespace: Logical namespace.
        query: Search query text.
        limit: Maximum number of hits to return.
    """
    hits = get_memory_repository().search(namespace=namespace, query=query, limit=limit)
    return json.dumps(
        {
            "ok": True,
            "hits": [
                {
                    "score": hit.score,
                    "item": hit.item.model_dump(mode="json"),
                }
                for hit in hits
            ],
        },
        ensure_ascii=False,
    )


@tool("memory_list_recent", parse_docstring=True)
def memory_list_recent(namespace: str, limit: int = 20) -> str:
    """List recent memory items in one namespace.

    Args:
        namespace: Logical namespace.
        limit: Maximum number of items.
    """
    items = get_memory_repository().list_memories(namespace=namespace, limit=limit)
    return json.dumps(
        {
            "ok": True,
            "items": [item.model_dump(mode="json") for item in items],
        },
        ensure_ascii=False,
    )


@tool("memory_delete", parse_docstring=True)
def memory_delete(memory_id: str) -> str:
    """Delete one memory item by id.

    Args:
        memory_id: Memory item id.
    """
    deleted = get_memory_repository().delete_memory(memory_id)
    return json.dumps({"ok": True, "deleted": deleted}, ensure_ascii=False)

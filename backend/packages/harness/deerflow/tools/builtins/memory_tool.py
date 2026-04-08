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
    """Upsert a single Chroma-backed memory item (document) scoped by `namespace`.

    Args:
        namespace: Required scope key (example: "skill:datawarehouse-processor").
        content: Document text stored in the collection.
        memory_key: Optional idempotency key within the same namespace; if it exists, the item is updated in-place.
        source_skill: Optional producer identifier stored in metadata.
        metadata_json: Optional user metadata as a JSON object string (must be a JSON dict); returned as `item.metadata.extra`.

    Returns:
        JSON string:
        - ok: bool
        - item: MemoryItem (id, content, metadata{namespace,memory_key,source_skill,extra}, created_at, updated_at)
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
    """Semantic search over documents in one `namespace` using Chroma's embedding function.

    Args:
        namespace: Required scope key (same as `memory_upsert`).
        query: Natural language query text. Chroma embeds the query and retrieves nearest documents.
        limit: Maximum hits to return.

    Returns:
        JSON string:
        - ok: bool
        - hits: list of { score: float, item: MemoryItem }

        Notes:
        - score is computed as 1/(1+distance); higher is more similar.
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
    """List memory items in one `namespace`, ordered by `updated_at` descending.

    Args:
        namespace: Required scope key (same as `memory_upsert`).
        limit: Maximum number of items to return.

    Returns:
        JSON string:
        - ok: bool
        - items: list[MemoryItem]
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
    """Delete one memory item by id (collection-wide).

    Args:
        memory_id: Memory item id.

    Returns:
        JSON string:
        - ok: bool
        - deleted: bool (True if the id existed and was deleted)
    """
    deleted = get_memory_repository().delete_memory(memory_id)
    return json.dumps({"ok": True, "deleted": deleted}, ensure_ascii=False)

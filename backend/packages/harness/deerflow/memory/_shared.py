"""Shared utilities for structured memory modules.

This module consolidates utility functions that were previously duplicated
across repository.py, search_service.py, and tag_manifest_service.py.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from deerflow.memory.models import (
    TITLE_MAX_LENGTH,
    CoreMemoryRecord,
    DistilledMemoryRecord,
    MemoryTier,
    RawMemoryRecord,
)

if TYPE_CHECKING:  # pragma: no cover - typing only
    from collections.abc import Iterable

    from deerflow.memory.models import MemoryRecordCommon


# ---------------------------------------------------------------------------
# Timestamp helpers
# ---------------------------------------------------------------------------

def utc_now_iso_z() -> str:
    """Return current UTC timestamp in ISO8601 with Z suffix."""
    return datetime.now(UTC).isoformat().removesuffix("+00:00") + "Z"


# ---------------------------------------------------------------------------
# JSON serialization helpers
# ---------------------------------------------------------------------------

def _string_list_to_json(values: list[str]) -> str:
    return json.dumps(values, ensure_ascii=False)


def _json_to_string_list(raw: object) -> list[str]:
    if not raw or not isinstance(raw, str):
        return []
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item) for item in parsed if isinstance(item, str)]


# ---------------------------------------------------------------------------
# Record conversion helpers
# ---------------------------------------------------------------------------

def _record_to_dict(record: MemoryRecordCommon) -> dict[str, Any]:
    """Convert a memory record model to a plain dict."""
    return record.model_dump()


def _memory_record_from_metadata(
    tier: MemoryTier,
    ids: list[str],
    metadata: dict[str, Any],
    document: str,
) -> RawMemoryRecord | DistilledMemoryRecord | CoreMemoryRecord | None:
    """Deserialize a memory record from Chroma metadata and document.

    Returns None when the id list is empty or the tier is unsupported.
    Handles legacy metadata fields for backward compatibility.
    """
    if not ids:
        return None
    memory_id = ids[0]
    meta = dict(metadata)

    def _str(value: object, fallback: str) -> str:
        return str(value).strip() if value is not None else fallback

    def _fallback_title(doc: str) -> str:
        text = doc.strip().replace("\n", " ")
        if not text:
            return "Untitled"
        if len(text) <= TITLE_MAX_LENGTH:
            return text
        return text[: TITLE_MAX_LENGTH - 1].rstrip() + "…"

    def _ensure_non_empty(value: str, fallback: str) -> str:
        return value if value else fallback

    created_at = _ensure_non_empty(_str(meta.get("created_at"), ""), utc_now_iso_z())
    updated_at = _ensure_non_empty(_str(meta.get("updated_at"), ""), utc_now_iso_z())

    common = {
        "id": memory_id,
        "title": _str(meta.get("title"), "") or _fallback_title(document),
        "content": document,
        "created_at": created_at,
        "updated_at": updated_at,
        "tags": _json_to_string_list(meta.get("tags_json")),
        "source_agent": _str(meta.get("source_agent"), "unknown-agent"),
        "user": _str(meta.get("user"), "default-user"),
    }

    if tier == MemoryTier.RAW:
        files = _json_to_string_list(meta.get("attachment_file_paths_json"))
        images = _json_to_string_list(meta.get("attachment_image_paths_json"))
        urls = _json_to_string_list(meta.get("inline_web_urls_json"))

        legacy_file = meta.get("source_file_path")
        if isinstance(legacy_file, str) and legacy_file.strip() and legacy_file not in files:
            files.append(legacy_file)
        legacy_image = meta.get("source_image_path")
        if isinstance(legacy_image, str) and legacy_image.strip() and legacy_image not in images:
            images.append(legacy_image)
        legacy_url = meta.get("source_web_url")
        if isinstance(legacy_url, str) and legacy_url.strip() and legacy_url not in urls:
            urls.append(legacy_url)

        thread_id = meta.get("source_thread_id")
        if not isinstance(thread_id, str) or not thread_id.strip():
            thread_id = "legacy-unknown-thread"

        return RawMemoryRecord(
            **common,
            source_thread_id=str(thread_id).strip(),
            attachment_file_paths=files,
            attachment_image_paths=images,
            inline_web_urls=urls,
        )

    if tier == MemoryTier.DISTILLED:
        return DistilledMemoryRecord(
            **common,
            raw_memory_ids=_json_to_string_list(meta.get("raw_memory_ids_json")),
        )

    if tier == MemoryTier.CORE:
        distilled_ids = _json_to_string_list(meta.get("distilled_memory_ids_json"))
        if not distilled_ids:
            return None
        return CoreMemoryRecord(
            **common,
            distilled_memory_ids=distilled_ids,
        )

    return None


# ---------------------------------------------------------------------------
# Tier resolution helpers
# ---------------------------------------------------------------------------

TIER_TO_COLLECTION_ATTR: dict[MemoryTier, str] = {
    MemoryTier.RAW: "_raw_collection",
    MemoryTier.DISTILLED: "_distilled_collection",
    MemoryTier.CORE: "_core_collection",
}


def resolve_memory_id_prefix(tier: MemoryTier) -> str:
    """Return the id prefix for a given tier (e.g. 'raw_' for RAW)."""
    return f"{tier.value}_"


def parse_memory_id_tier(memory_id: str) -> MemoryTier | None:
    """Parse a memory id and return its tier, or None if the prefix is unknown."""
    normalized = memory_id.strip()
    if normalized.startswith("raw_"):
        return MemoryTier.RAW
    if normalized.startswith("distilled_"):
        return MemoryTier.DISTILLED
    if normalized.startswith("core_"):
        return MemoryTier.CORE
    return None


def normalize_tiers(
    value: str | MemoryTier | None,
    *,
    default: Iterable[str | MemoryTier],
) -> list[MemoryTier]:
    """Normalize a single tier value to a MemoryTier, applying the default if None/empty."""
    if value is None:
        return list(default)
    if isinstance(value, MemoryTier):
        return [value]
    normalized = str(value).strip().lower()
    if not normalized:
        return list(default)
    try:
        return [MemoryTier(normalized)]
    except ValueError:
        return list(default)


def resolve_tiers(
    tier_filter: Iterable[str | MemoryTier] | None,
    defaults: Iterable[str | MemoryTier],
) -> list[MemoryTier]:
    """Normalize a tier filter to a list of MemoryTier, applying defaults if empty."""
    raw_values = list(tier_filter) if tier_filter is not None else list(defaults)
    result: list[MemoryTier] = []
    for v in raw_values:
        val = v.value if isinstance(v, MemoryTier) else str(v).strip().lower()
        try:
            result.append(MemoryTier(val))
        except ValueError:
            pass
    if not result:
        result = [MemoryTier.CORE, MemoryTier.DISTILLED]
    return result


def resolve_tier_filter(tier_filter: Iterable[str | MemoryTier] | None) -> set[str] | None:
    """Normalize a tier filter to a set of tier value strings, or None if no filter."""
    if tier_filter is None:
        return None
    allowed: set[str] = set()
    for value in tier_filter:
        if isinstance(value, MemoryTier):
            allowed.add(value.value)
        else:
            try:
                allowed.add(MemoryTier(str(value).strip().lower()).value)
            except ValueError:
                continue
    return allowed or None

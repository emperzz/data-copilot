"""Shared utilities for structured memory modules.

This module consolidates utility functions that were previously duplicated
across repository.py, search_service.py, and tag_manifest_service.py.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from deerflow.memory.models import MemoryTier

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

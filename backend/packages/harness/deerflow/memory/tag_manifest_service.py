"""Tag manifest snapshot for structured memory (read + counter cache).

The manifest gives the agent an at-a-glance view of which tags exist and how
many records per tier use each tag, so it can discover categories *before*
calling ``structured_memory_query``. Counts are maintained by a process-level
cache that is seeded from a full scan of ``StructuredMemoryRepository`` and
incrementally updated on successful writes/updates/deletes. A TTL forces a
periodic rescan to repair drift caused by missed bump calls or external edits.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from deerflow.config.structured_memory_config import get_structured_memory_config
from deerflow.memory.models import MemoryTier
from deerflow.memory.repository import StructuredMemoryRepository, get_structured_memory_repository
from deerflow.memory.structured_memory_search_service import StructuredMemorySearchService

if TYPE_CHECKING:  # pragma: no cover - typing only
    from collections.abc import Iterable


@dataclass(frozen=True)
class TagManifestEntry:
    """Snapshot of one tag: total record count and breakdown by tier."""

    tag: str
    total: int
    counts_by_tier: dict[str, int] = field(default_factory=dict)


@dataclass
class _CacheState:
    """Internal cache: tag -> {tier -> count} with a load timestamp."""

    counts: dict[str, dict[str, int]] = field(default_factory=dict)
    loaded_at: float = 0.0

    @property
    def is_loaded(self) -> bool:
        return self.loaded_at > 0.0


def _normalize_tier(value: str | MemoryTier) -> MemoryTier | None:
    if isinstance(value, MemoryTier):
        return value
    try:
        return MemoryTier(str(value).strip().lower())
    except ValueError:
        return None


class TagManifestService:
    """Read-only tag manifest with write-driven counter bumps.

    Reuses ``StructuredMemorySearchService.list_tags`` for the full-scan path so
    tag aggregation semantics stay consistent with the existing list-tags tool.
    """

    def __init__(
        self,
        *,
        repository: StructuredMemoryRepository | None = None,
        search_service: StructuredMemorySearchService | None = None,
    ) -> None:
        self._repository = repository
        self._search_service = search_service
        self._state = _CacheState()
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # repository / service accessors
    # ------------------------------------------------------------------

    def _repo(self) -> StructuredMemoryRepository:
        if self._repository is not None:
            return self._repository
        if self._search_service is not None:
            return self._search_service._repo()  # noqa: SLF001 - shared access pattern within memory package
        return get_structured_memory_repository()

    # ------------------------------------------------------------------
    # cache lifecycle
    # ------------------------------------------------------------------

    def invalidate(self) -> None:
        """Drop the cache so the next ``snapshot`` call triggers a full rescan."""
        with self._lock:
            self._state = _CacheState()

    def _cache_is_fresh(self, ttl_seconds: int) -> bool:
        if not self._state.is_loaded:
            return False
        if ttl_seconds <= 0:
            return False
        return (time.monotonic() - self._state.loaded_at) < ttl_seconds

    def _rebuild_from_repository(self) -> None:
        """Populate cache by scanning the repository.

        The caller must hold ``self._lock``. A single pass per tier is cheaper
        than calling ``list_tags`` + per-tag rescans, and gives us exact
        per-tier counts directly.
        """
        counts = _scan_tag_counts_by_tier(self._repo())
        self._state = _CacheState(counts=counts, loaded_at=time.monotonic())

    # ------------------------------------------------------------------
    # public read API
    # ------------------------------------------------------------------

    def snapshot(
        self,
        *,
        tier_filter: Iterable[str | MemoryTier] | None = None,
    ) -> list[TagManifestEntry]:
        """Return current tag manifest, rebuilding on TTL expiry."""
        cfg = get_structured_memory_config()
        ttl = cfg.tag_manifest.cache_ttl_seconds
        allowed = _resolve_tier_filter(tier_filter)

        with self._lock:
            if not self._cache_is_fresh(ttl):
                self._rebuild_from_repository()
            entries = _project_entries(self._state.counts, allowed)
        entries.sort(key=lambda e: (-e.total, e.tag))
        return entries

    # ------------------------------------------------------------------
    # public mutation API (called from write / update / delete services)
    # ------------------------------------------------------------------

    def bump_counters(
        self,
        *,
        tags: Iterable[str],
        tier: MemoryTier | str,
        delta: int,
    ) -> None:
        """Apply a delta to cached counts for ``tags`` under ``tier``.

        No-op when the cache has not been loaded yet (the next ``snapshot``
        call will rebuild from the repository). Tags whose count reaches 0 are
        removed so they do not show up as zero-count entries in the prompt.
        """
        if delta == 0:
            return
        resolved_tier = _normalize_tier(tier)
        if resolved_tier is None:
            return
        normalized: list[str] = []
        seen: set[str] = set()
        for raw_tag in tags:
            t = str(raw_tag).strip()
            if not t or t in seen:
                continue
            seen.add(t)
            normalized.append(t)
        if not normalized:
            return

        with self._lock:
            if not self._state.is_loaded:
                return
            tier_key = resolved_tier.value
            for tag in normalized:
                per_tier = self._state.counts.setdefault(tag, {})
                new_count = per_tier.get(tier_key, 0) + delta
                if new_count <= 0:
                    per_tier.pop(tier_key, None)
                else:
                    per_tier[tier_key] = new_count
                if not per_tier:
                    self._state.counts.pop(tag, None)

    # ------------------------------------------------------------------
    # formatting helpers (prompt injection + tooling)
    # ------------------------------------------------------------------

    def snapshot_text(self, *, max_tags: int | None = None) -> str:
        """Render the current manifest as a compact prompt-friendly block.

        Returns an empty string when the manifest is empty so callers can
        conditionally concat without adding blank lines.
        """
        entries = self.snapshot()
        if not entries:
            return ""
        limit = max_tags or get_structured_memory_config().tag_manifest.max_tags_in_prompt
        limited = entries[: max(1, limit)]
        lines: list[str] = [
            "<structured_memory_tag_manifest>",
            f"Known tags ({len(limited)} of {len(entries)} shown; format: tag total [tier:count ...]):",
        ]
        for entry in limited:
            parts = " ".join(f"{tier}:{count}" for tier, count in sorted(entry.counts_by_tier.items()))
            lines.append(f"- {entry.tag} {entry.total} [{parts}]")
        lines.append("</structured_memory_tag_manifest>")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# module-level helpers
# ---------------------------------------------------------------------------


def _scan_tag_counts_by_tier(repo: StructuredMemoryRepository) -> dict[str, dict[str, int]]:
    """Single-pass scan producing ``{tag: {tier: count}}`` across all tiers."""
    from deerflow.memory.structured_memory_search_service import TIER_TO_COLLECTION_ATTR, _json_to_string_list

    counts: dict[str, dict[str, int]] = {}
    for tier in MemoryTier:
        collection = getattr(repo, TIER_TO_COLLECTION_ATTR[tier])
        if collection.count() == 0:
            continue
        result = collection.get(include=["metadatas"])
        tier_key = tier.value
        for meta in result.get("metadatas") or []:
            if not meta:
                continue
            for tag in _json_to_string_list(meta.get("tags_json")):
                per_tier = counts.setdefault(tag, {})
                per_tier[tier_key] = per_tier.get(tier_key, 0) + 1
    return counts


def _resolve_tier_filter(
    tier_filter: Iterable[str | MemoryTier] | None,
) -> set[str] | None:
    if tier_filter is None:
        return None
    allowed: set[str] = set()
    for value in tier_filter:
        tier = _normalize_tier(value)
        if tier is not None:
            allowed.add(tier.value)
    return allowed or None


def _project_entries(
    counts: dict[str, dict[str, int]],
    allowed: set[str] | None,
) -> list[TagManifestEntry]:
    entries: list[TagManifestEntry] = []
    for tag, per_tier in counts.items():
        if allowed is None:
            projected = dict(per_tier)
        else:
            projected = {t: c for t, c in per_tier.items() if t in allowed}
            if not projected:
                continue
        total = sum(projected.values())
        entries.append(TagManifestEntry(tag=tag, total=total, counts_by_tier=projected))
    return entries


# ---------------------------------------------------------------------------
# process-level singleton
# ---------------------------------------------------------------------------


_service_instance: TagManifestService | None = None
_service_lock = threading.Lock()


def get_tag_manifest_service() -> TagManifestService:
    """Return the process-wide tag manifest service singleton."""
    global _service_instance
    if _service_instance is not None:
        return _service_instance
    with _service_lock:
        if _service_instance is None:
            _service_instance = TagManifestService()
    return _service_instance


def reset_tag_manifest_service_singleton() -> None:
    """Drop the singleton (for tests or config hot-swap)."""
    global _service_instance
    with _service_lock:
        _service_instance = None

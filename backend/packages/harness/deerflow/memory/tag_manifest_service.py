"""Tag manifest snapshot backed by a persisted Chroma collection.

The manifest records how many structured-memory rows per tier carry each tag,
so the agent can pick an existing tag category *before* calling
``structured_memory_query``.

Persistence layout (owned by ``StructuredMemoryRepository``):

* Collection ``memory_tag_manifest`` — one row per tag, keyed by the tag
  string itself. The counts map is stored as ``metadata.counts_json`` so
  future extensions (definition, scope keywords) can ride on the same
  record without another migration.

Lifecycle:

* Writes/updates/deletes route through ``bump_counters`` which **always**
  persists the delta to Chroma so values survive process restarts.
* ``snapshot`` reads from a per-process TTL cache; on miss it reloads from
  the persisted collection. If the persisted collection is empty (first
  run after upgrade), it seeds itself by full-scanning the raw/distilled
  /core tiers once.
* ``rebuild_from_tiers`` is the explicit drift-repair entry point; it
  rescans the tiers, overwrites the persisted manifest, and clears the
  in-memory cache.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from deerflow.config.structured_memory_config import get_structured_memory_config
from deerflow.memory.models import MemoryTier
from deerflow.memory.repository import StructuredMemoryRepository, get_structured_memory_repository

if TYPE_CHECKING:  # pragma: no cover - typing only
    from collections.abc import Iterable


@dataclass(frozen=True)
class TagManifestEntry:
    """In-memory snapshot of one tag: total record count and per-tier breakdown."""

    tag: str
    total: int
    counts_by_tier: dict[str, int] = field(default_factory=dict)


@dataclass
class _CacheState:
    """Internal TTL cache: tag -> {tier -> count} with a load timestamp."""

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
    """Tag manifest with write-through persistence via ``StructuredMemoryRepository``."""

    def __init__(
        self,
        *,
        repository: StructuredMemoryRepository | None = None,
    ) -> None:
        self._repository = repository
        self._state = _CacheState()
        self._lock = threading.Lock()

    def _repo(self) -> StructuredMemoryRepository:
        if self._repository is not None:
            return self._repository
        return get_structured_memory_repository()

    # ------------------------------------------------------------------
    # cache lifecycle
    # ------------------------------------------------------------------

    def invalidate(self) -> None:
        """Drop the in-memory cache; next ``snapshot`` reloads from persistence."""
        with self._lock:
            self._state = _CacheState()

    def _cache_is_fresh(self, ttl_seconds: int) -> bool:
        if not self._state.is_loaded:
            return False
        if ttl_seconds <= 0:
            return False
        return (time.monotonic() - self._state.loaded_at) < ttl_seconds

    def _reload_cache(self) -> None:
        """Populate cache from the persisted manifest.

        Seeds the persisted collection from a tier full-scan when it is empty,
        which covers the first run after an upgrade (existing tier rows but no
        persisted manifest yet). Caller must hold ``self._lock``.
        """
        repo = self._repo()
        records = repo.list_tag_manifest()
        if not records:
            scanned = _scan_tag_counts_by_tier(repo)
            if scanned:
                for tag, per_tier in scanned.items():
                    try:
                        repo.upsert_tag_manifest(tag=tag, counts_by_tier=per_tier)
                    except Exception:  # pragma: no cover - best-effort seed
                        pass
            self._state = _CacheState(
                counts={tag: dict(v) for tag, v in scanned.items()},
                loaded_at=time.monotonic(),
            )
            return
        self._state = _CacheState(
            counts={rec.tag: dict(rec.counts_by_tier) for rec in records},
            loaded_at=time.monotonic(),
        )

    # ------------------------------------------------------------------
    # public read API
    # ------------------------------------------------------------------

    def snapshot(
        self,
        *,
        tier_filter: Iterable[str | MemoryTier] | None = None,
    ) -> list[TagManifestEntry]:
        """Return current tag manifest (persisted), reloading on TTL expiry."""
        cfg = get_structured_memory_config()
        ttl = cfg.tag_manifest.cache_ttl_seconds
        allowed = _resolve_tier_filter(tier_filter)

        with self._lock:
            if not self._cache_is_fresh(ttl):
                self._reload_cache()
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
        """Apply ``delta`` to counts for ``tags`` under ``tier`` and persist.

        Writes are always persisted via the repository so values survive
        restarts. Cache is updated in-place when loaded so active readers see
        the new values immediately. A count that drops to zero in every tier
        causes the tag manifest record to be removed.
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

        repo = self._repo()
        tier_key = resolved_tier.value

        with self._lock:
            cache_loaded = self._state.is_loaded
            for tag in normalized:
                current: dict[str, int]
                if cache_loaded:
                    current = dict(self._state.counts.get(tag, {}))
                else:
                    persisted = repo.get_tag_manifest(tag)
                    current = dict(persisted.counts_by_tier) if persisted is not None else {}

                new_count = current.get(tier_key, 0) + delta
                if new_count <= 0:
                    current.pop(tier_key, None)
                else:
                    current[tier_key] = new_count

                if current:
                    try:
                        repo.upsert_tag_manifest(tag=tag, counts_by_tier=current)
                    except Exception:  # pragma: no cover - cache maintenance must not block writes
                        pass
                else:
                    try:
                        repo.delete_tag_manifest(tag)
                    except Exception:  # pragma: no cover
                        pass

                if cache_loaded:
                    if current:
                        self._state.counts[tag] = current
                    else:
                        self._state.counts.pop(tag, None)

    def rebuild_from_tiers(self) -> None:
        """Re-seed the persisted manifest from a fresh full scan of all tiers.

        Use this to repair drift after manual edits or tests that bypass the
        service layer. Clears the in-memory cache so the next ``snapshot``
        reflects the rebuilt state.
        """
        repo = self._repo()
        scanned = _scan_tag_counts_by_tier(repo)
        existing = {rec.tag for rec in repo.list_tag_manifest()}

        for tag, per_tier in scanned.items():
            try:
                repo.upsert_tag_manifest(tag=tag, counts_by_tier=per_tier)
            except Exception:  # pragma: no cover
                pass

        obsolete = existing - set(scanned.keys())
        for tag in obsolete:
            try:
                repo.delete_tag_manifest(tag)
            except Exception:  # pragma: no cover
                pass

        self.invalidate()

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

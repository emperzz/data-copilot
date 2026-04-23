"""Search, tag discovery, and lineage traversal for structured memory."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from deerflow.config.structured_memory_config import get_structured_memory_config
from deerflow.memory._shared import (
    _json_to_string_list,
    _record_to_dict,
)
from deerflow.memory._shared import (
    resolve_tiers as _resolve_tiers,
)
from deerflow.memory.models import (
    CoreMemoryRecord,
    DistilledMemoryRecord,
    MemoryRecordCommon,
)
from deerflow.memory.repository import StructuredMemoryRepository, get_structured_memory_repository
from deerflow.memory.tag_manifest_service import TagManifestEntry, TagManifestService, get_tag_manifest_service


class StructuredMemorySearchError(ValueError):
    """Validation or runtime failure for a structured memory query."""


@dataclass(frozen=True)
class SearchResultItem:
    """One hit from a vector / metadata search."""

    memory_id: str
    tier: str
    title: str
    content: str
    tags: list[str]
    distance: float | None = None
    updated_at: str = ""
    source_agent: str = ""


@dataclass(frozen=True)
class MemoryWithLineage:
    """A single record with optional upstream records attached."""

    record: dict[str, Any]
    tier: str
    upstream: list[dict[str, Any]] = field(default_factory=list)


def _build_where_filter(
    *,
    user: str | None = None,
    source_agent: str | None = None,
) -> dict[str, Any] | None:
    """Construct a Chroma ``where`` clause from scalar metadata filters.

    Tag filtering is intentionally handled post-retrieval in Python because
    Chroma stores tags as a JSON-encoded string (``tags_json``) and its
    ``$contains`` operator does not perform substring matching on strings.
    """
    conditions: list[dict[str, Any]] = []
    if user:
        conditions.append({"user": user})
    if source_agent:
        conditions.append({"source_agent": source_agent})
    if not conditions:
        return None
    if len(conditions) == 1:
        return conditions[0]
    return {"$and": conditions}


def _record_matches_tags(meta: dict[str, Any], required_tags: list[str]) -> bool:
    """Return True if the record's tags contain ALL of ``required_tags``."""
    record_tags = set(_json_to_string_list(meta.get("tags_json")))
    return all(t in record_tags for t in required_tags)


class StructuredMemorySearchService:
    """Stateless service layer over ``StructuredMemoryRepository`` for read operations."""

    def __init__(self, repository: StructuredMemoryRepository | None = None) -> None:
        self._repository = repository
        self._tag_manifest: TagManifestService | None = (
            TagManifestService(repository=repository) if repository is not None else None
        )

    def _repo(self) -> StructuredMemoryRepository:
        if self._repository is not None:
            return self._repository
        return get_structured_memory_repository()

    # ------------------------------------------------------------------
    # search – vector + metadata hybrid
    # ------------------------------------------------------------------

    def search(
        self,
        *,
        query_text: str,
        tier_filter: list[str] | None = None,
        tags: list[str] | None = None,
        user: str | None = None,
        source_agent: str | None = None,
        top_k: int | None = None,
    ) -> list[SearchResultItem]:
        cfg = get_structured_memory_config()
        effective_top_k = min(top_k or cfg.query.default_top_k, cfg.query.max_top_k)
        tiers = _resolve_tiers(tier_filter, cfg.query.default_tiers)

        if not query_text.strip():
            raise StructuredMemorySearchError("query_text must be non-empty.")

        repo = self._repo()
        where = _build_where_filter(user=user, source_agent=source_agent)

        # Over-fetch when tag filtering is needed (post-retrieval) so we can
        # still return ``effective_top_k`` results after dropping non-matches.
        fetch_k = effective_top_k * 3 if tags else effective_top_k
        results: list[SearchResultItem] = []

        for tier in tiers:
            collection = repo._collection_for_tier(tier)
            query_kwargs: dict[str, Any] = {
                "query_texts": [query_text],
                "n_results": fetch_k,
                "include": ["documents", "metadatas", "distances"],
            }
            if where is not None:
                query_kwargs["where"] = where

            try:
                raw_result = collection.query(**query_kwargs)
            except Exception:
                continue

            ids_list = raw_result.get("ids") or [[]]
            docs_list = raw_result.get("documents") or [[]]
            metas_list = raw_result.get("metadatas") or [[]]
            distances_list = raw_result.get("distances") or [[]]

            for idx, mid in enumerate(ids_list[0]):
                meta = (metas_list[0][idx] if idx < len(metas_list[0]) else {}) or {}
                if tags and not _record_matches_tags(meta, tags):
                    continue
                doc = (docs_list[0][idx] if idx < len(docs_list[0]) else "") or ""
                dist = distances_list[0][idx] if idx < len(distances_list[0]) else None
                results.append(
                    SearchResultItem(
                        memory_id=mid,
                        tier=tier.value,
                        title=str(meta.get("title", "")),
                        content=doc,
                        tags=_json_to_string_list(meta.get("tags_json")),
                        distance=dist,
                        updated_at=str(meta.get("updated_at", "")),
                        source_agent=str(meta.get("source_agent", "")),
                    )
                )

        results.sort(key=lambda r: r.distance if r.distance is not None else float("inf"))
        return results[:effective_top_k]

    # ------------------------------------------------------------------
    # list_tags – distinct tag discovery across tiers
    # ------------------------------------------------------------------

    def list_tags(self, *, tier_filter: list[str] | None = None) -> list[TagManifestEntry]:
        manifest = self._tag_manifest or get_tag_manifest_service()
        return manifest.snapshot(tier_filter=tier_filter)

    # ------------------------------------------------------------------
    # get_by_id – exact lookup with optional upstream lineage
    # ------------------------------------------------------------------

    def get_by_id(
        self,
        memory_id: str,
        *,
        include_upstream: bool = False,
        max_depth: int = 2,
    ) -> MemoryWithLineage | None:
        repo = self._repo()
        record, tier = self._resolve_record(repo, memory_id)
        if record is None or tier is None:
            return None

        upstream: list[dict[str, Any]] = []
        if include_upstream and max_depth > 0:
            upstream = self._collect_upstream(repo, record, tier, remaining_depth=max_depth)

        return MemoryWithLineage(
            record=_record_to_dict(record),
            tier=tier,
            upstream=upstream,
        )

    # ------------------------------------------------------------------
    # format helpers
    # ------------------------------------------------------------------

    @staticmethod
    def format_search_results(items: list[SearchResultItem]) -> str:
        if not items:
            return "No structured memory results found."
        lines: list[str] = [f"Found {len(items)} structured memory result(s):\n"]
        for i, item in enumerate(items, 1):
            tags_str = ", ".join(item.tags) if item.tags else "none"
            lines.append(
                f"{i}. [{item.tier}] {item.title}\n"
                f"   id: {item.memory_id}\n"
                f"   tags: {tags_str}\n"
                f"   updated: {item.updated_at}\n"
                f"   content: {_truncate(item.content, 300)}\n"
            )
        return "\n".join(lines)

    @staticmethod
    def format_tag_list(entries: list[TagManifestEntry]) -> str:
        if not entries:
            return "No tags found in structured memory."
        lines: list[str] = [f"Available tags ({len(entries)} total):\n"]
        for e in entries:
            parts = " ".join(f"{tier}:{count}" for tier, count in sorted(e.counts_by_tier.items()))
            lines.append(f"- {e.tag} ({e.total} records, tiers: [{parts}])")
        return "\n".join(lines)

    @staticmethod
    def format_lineage(result: MemoryWithLineage) -> str:
        r = result.record
        lines: list[str] = [
            f"[{result.tier}] {r.get('title', '')}\n"
            f"id: {r.get('id', '')}\n"
            f"tags: {', '.join(r.get('tags', []))}\n"
            f"updated: {r.get('updated_at', '')}\n"
            f"content: {r.get('content', '')}\n"
        ]
        if result.upstream:
            lines.append(f"\nUpstream records ({len(result.upstream)}):\n")
            for u in result.upstream:
                tier_label = u.get("_tier", "unknown")
                lines.append(
                    f"  [{tier_label}] {u.get('title', '')}\n"
                    f"  id: {u.get('id', '')}\n"
                    f"  content: {_truncate(u.get('content', ''), 200)}\n"
                )
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _resolve_record(
        repo: StructuredMemoryRepository, memory_id: str
    ) -> tuple[MemoryRecordCommon | None, str | None]:
        if memory_id.startswith("raw_"):
            rec = repo.get_raw_memory(memory_id)
            return (rec, "raw") if rec else (None, None)
        if memory_id.startswith("distilled_"):
            rec = repo.get_distilled_memory(memory_id)
            return (rec, "distilled") if rec else (None, None)
        if memory_id.startswith("core_"):
            rec = repo.get_core_memory(memory_id)
            return (rec, "core") if rec else (None, None)
        for getter, tier_label in [
            (repo.get_core_memory, "core"),
            (repo.get_distilled_memory, "distilled"),
            (repo.get_raw_memory, "raw"),
        ]:
            rec = getter(memory_id)
            if rec is not None:
                return rec, tier_label
        return None, None

    @staticmethod
    def _collect_upstream(
        repo: StructuredMemoryRepository,
        record: MemoryRecordCommon,
        tier: str,
        *,
        remaining_depth: int,
    ) -> list[dict[str, Any]]:
        upstream: list[dict[str, Any]] = []
        if remaining_depth <= 0:
            return upstream

        if tier == "core" and isinstance(record, CoreMemoryRecord):
            for did in record.distilled_memory_ids:
                d = repo.get_distilled_memory(did)
                if d is None:
                    continue
                entry = _record_to_dict(d)
                entry["_tier"] = "distilled"
                upstream.append(entry)
                if remaining_depth > 1:
                    for rid in d.raw_memory_ids:
                        r = repo.get_raw_memory(rid)
                        if r is not None:
                            raw_entry = _record_to_dict(r)
                            raw_entry["_tier"] = "raw"
                            upstream.append(raw_entry)

        elif tier == "distilled" and isinstance(record, DistilledMemoryRecord):
            for rid in record.raw_memory_ids:
                r = repo.get_raw_memory(rid)
                if r is None:
                    continue
                entry = _record_to_dict(r)
                entry["_tier"] = "raw"
                upstream.append(entry)

        return upstream


def _truncate(text: str, max_len: int) -> str:
    if len(text) <= max_len:
        return text
    return text[: max_len - 1].rstrip() + "…"

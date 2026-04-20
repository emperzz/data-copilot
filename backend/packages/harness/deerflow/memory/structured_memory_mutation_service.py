"""Validate and execute structured memory update/delete operations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from deerflow.config.structured_memory_config import get_structured_memory_config
from deerflow.memory.models import MemoryTier
from deerflow.memory.repository import StructuredMemoryRepository, get_structured_memory_repository
from deerflow.memory.structured_memory_write_service import _normalize_content, _normalize_tags, _normalize_title
from deerflow.memory.tag_manifest_service import get_tag_manifest_service

MutationActionLiteral = Literal["update", "delete"]


class StructuredMemoryMutationError(ValueError):
    """User- or agent-facing validation failure for update/delete."""


@dataclass(frozen=True)
class StructuredMemoryMutationResult:
    action: MutationActionLiteral
    memory_id: str
    tier: str
    title: str | None = None


class StructuredMemoryMutationService:
    """Service layer for update/delete over structured memory tiers."""

    def __init__(self, repository: StructuredMemoryRepository | None = None) -> None:
        self._repository = repository

    def _repo(self) -> StructuredMemoryRepository:
        if self._repository is not None:
            return self._repository
        return get_structured_memory_repository()

    @staticmethod
    def _resolve_tier(memory_id: str) -> MemoryTier:
        normalized_id = memory_id.strip()
        if normalized_id.startswith("raw_"):
            return MemoryTier.RAW
        if normalized_id.startswith("distilled_"):
            return MemoryTier.DISTILLED
        if normalized_id.startswith("core_"):
            return MemoryTier.CORE
        raise StructuredMemoryMutationError(
            "memory_id must start with raw_, distilled_, or core_.",
        )

    def update_memory(
        self,
        *,
        memory_id: str,
        title: str,
        content: str,
        tags: list[str] | None = None,
        source_agent: str | None = None,
        user: str | None = None,
    ) -> StructuredMemoryMutationResult:
        sm = get_structured_memory_config()
        if self._repository is None and not sm.enabled:
            raise StructuredMemoryMutationError(
                "Structured memory is disabled; set structured_memory.enabled to true in config.yaml.",
            )
        normalized_id = memory_id.strip()
        if not normalized_id:
            raise StructuredMemoryMutationError("memory_id must be non-empty.")
        tier = self._resolve_tier(normalized_id)

        normalized_title = _normalize_title(title)
        normalized_content = _normalize_content(content, max_len=sm.write.max_content_length)
        normalized_tags = _normalize_tags(tags)

        repo = self._repo()
        previous_tags = _fetch_tags(repo, memory_id=normalized_id, tier=tier)

        updated = repo.update_memory(
            memory_id=normalized_id,
            title=normalized_title,
            content=normalized_content,
            tags=normalized_tags,
            source_agent=source_agent,
            user=user,
        )
        if not updated:
            raise StructuredMemoryMutationError(f"No structured memory record found with id={normalized_id!r}.")
        _apply_tag_delta(previous_tags=previous_tags, next_tags=normalized_tags, tier=tier)
        return StructuredMemoryMutationResult(
            action="update",
            memory_id=normalized_id,
            tier=tier.value,
            title=normalized_title,
        )

    def delete_memory(self, *, memory_id: str) -> StructuredMemoryMutationResult:
        sm = get_structured_memory_config()
        if self._repository is None and not sm.enabled:
            raise StructuredMemoryMutationError(
                "Structured memory is disabled; set structured_memory.enabled to true in config.yaml.",
            )
        normalized_id = memory_id.strip()
        if not normalized_id:
            raise StructuredMemoryMutationError("memory_id must be non-empty.")
        tier = self._resolve_tier(normalized_id)

        repo = self._repo()
        self._ensure_no_downstream_references(repo, memory_id=normalized_id, tier=tier)
        previous_tags = _fetch_tags(repo, memory_id=normalized_id, tier=tier)
        deleted = repo.delete_memory(normalized_id)
        if not deleted:
            raise StructuredMemoryMutationError(f"No structured memory record found with id={normalized_id!r}.")
        _apply_tag_delta(previous_tags=previous_tags, next_tags=[], tier=tier)
        return StructuredMemoryMutationResult(
            action="delete",
            memory_id=normalized_id,
            tier=tier.value,
        )

    @staticmethod
    def _ensure_no_downstream_references(repo: StructuredMemoryRepository, *, memory_id: str, tier: MemoryTier) -> None:
        if tier == MemoryTier.RAW:
            distilled_result = repo._distilled_collection.get(include=["metadatas"])
            blockers: list[str] = []
            for idx, metadata in enumerate(distilled_result.get("metadatas") or []):
                if not metadata:
                    continue
                raw_ids = set(_parse_ids(metadata.get("raw_memory_ids_json")))
                if memory_id in raw_ids:
                    dist_ids = distilled_result.get("ids") or []
                    blockers.append(dist_ids[idx] if idx < len(dist_ids) else "distilled_unknown")
            if blockers:
                raise StructuredMemoryMutationError(
                    "Cannot delete raw memory that is referenced by distilled memory: "
                    + ", ".join(blockers)
                    + ". Delete or update downstream records first.",
                )
            return

        if tier == MemoryTier.DISTILLED:
            core_result = repo._core_collection.get(include=["metadatas"])
            blockers = []
            for idx, metadata in enumerate(core_result.get("metadatas") or []):
                if not metadata:
                    continue
                distilled_ids = set(_parse_ids(metadata.get("distilled_memory_ids_json")))
                if memory_id in distilled_ids:
                    core_ids = core_result.get("ids") or []
                    blockers.append(core_ids[idx] if idx < len(core_ids) else "core_unknown")
            if blockers:
                raise StructuredMemoryMutationError(
                    "Cannot delete distilled memory that is referenced by core memory: "
                    + ", ".join(blockers)
                    + ". Delete or update downstream records first.",
                )


def _parse_ids(raw: object) -> list[str]:
    from deerflow.memory.repository import _json_to_string_list

    return _json_to_string_list(raw)


def _fetch_tags(repo: StructuredMemoryRepository, *, memory_id: str, tier: MemoryTier) -> list[str]:
    """Look up current tags so manifest deltas can account for tag changes."""
    if tier == MemoryTier.RAW:
        record = repo.get_raw_memory(memory_id)
    elif tier == MemoryTier.DISTILLED:
        record = repo.get_distilled_memory(memory_id)
    else:
        record = repo.get_core_memory(memory_id)
    return list(record.tags) if record is not None else []


def _apply_tag_delta(*, previous_tags: list[str], next_tags: list[str], tier: MemoryTier) -> None:
    """Push tag count deltas into the manifest cache (best-effort).

    Tags present in both lists net to zero; removed tags get -1, added tags +1.
    """
    previous = set(previous_tags)
    nxt = set(next_tags)
    removed = [t for t in previous_tags if t in (previous - nxt)]
    added = [t for t in next_tags if t in (nxt - previous)]
    if not removed and not added:
        return
    try:
        manifest = get_tag_manifest_service()
        if removed:
            manifest.bump_counters(tags=removed, tier=tier, delta=-1)
        if added:
            manifest.bump_counters(tags=added, tier=tier, delta=+1)
    except Exception:  # pragma: no cover - cache maintenance must not block mutations
        pass


def format_mutation_success(result: StructuredMemoryMutationResult) -> str:
    if result.action == "update":
        return (
            f"Structured memory updated: id={result.memory_id} tier={result.tier} "
            f"title={result.title!r}"
        )
    return f"Structured memory deleted: id={result.memory_id} tier={result.tier}"

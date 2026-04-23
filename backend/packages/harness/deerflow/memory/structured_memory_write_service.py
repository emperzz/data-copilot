"""Validate and persist structured memory writes (raw / distilled / core)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from deerflow.config.structured_memory_config import get_structured_memory_config
from deerflow.memory._shared import (
    _normalize_content,
    _normalize_tags,
    _normalize_title,
)
from deerflow.memory.models import MemoryTier, StructuredMemoryWriteError
from deerflow.memory.repository import StructuredMemoryRepository, get_structured_memory_repository
from deerflow.memory.tag_manifest_service import get_tag_manifest_service

MemoryTierLiteral = Literal["raw", "distilled", "core"]


@dataclass(frozen=True)
class StructuredMemoryWriteResult:
    """Outcome of a successful structured memory write."""

    memory_id: str
    tier: MemoryTierLiteral
    title: str


class StructuredMemoryWriteService:
    """Normalize inputs and call ``StructuredMemoryRepository`` create APIs."""

    def __init__(self, repository: StructuredMemoryRepository | None = None) -> None:
        self._repository = repository

    def _repo(self) -> StructuredMemoryRepository:
        if self._repository is not None:
            return self._repository
        return get_structured_memory_repository()

    def normalize_and_write(
        self,
        *,
        tier: MemoryTierLiteral,
        title: str,
        content: str,
        tags: list[str] | None = None,
        source_thread_id: str | None = None,
        attachment_file_paths: list[str] | None = None,
        attachment_image_paths: list[str] | None = None,
        inline_web_urls: list[str] | None = None,
        raw_memory_ids: list[str] | None = None,
        distilled_memory_ids: list[str] | None = None,
        source_agent: str | None = None,
        user: str | None = None,
    ) -> StructuredMemoryWriteResult:
        sm = get_structured_memory_config()
        if self._repository is None and not sm.enabled:
            raise StructuredMemoryWriteError(
                "Structured memory is disabled; set structured_memory.enabled to true in config.yaml."
            )
        max_len = sm.write.max_content_length
        norm_title = _normalize_title(title)
        norm_content = _normalize_content(content, max_len=max_len)
        norm_tags = _normalize_tags(tags)

        if tier == MemoryTier.RAW.value:
            sid = (source_thread_id or "").strip()
            if not sid:
                raise StructuredMemoryWriteError(
                    "raw tier requires a non-empty source_thread_id: the LangGraph conversation thread "
                    "this raw row documents (never inferred in the service layer)."
                )

        repo = self._repo()

        if tier == MemoryTier.RAW.value:
            sid = (source_thread_id or "").strip()
            record = repo.create_raw_memory(
                title=norm_title,
                content=norm_content,
                source_thread_id=sid,
                tags=norm_tags,
                source_agent=source_agent,
                user=user,
                attachment_file_paths=_normalize_path_list(attachment_file_paths),
                attachment_image_paths=_normalize_path_list(attachment_image_paths),
                inline_web_urls=_normalize_path_list(inline_web_urls),
            )
            _bump_manifest(record.tags, MemoryTier.RAW, +1)
            return StructuredMemoryWriteResult(memory_id=record.id, tier="raw", title=record.title)

        if tier == MemoryTier.DISTILLED.value:
            ids = _non_empty_id_list(raw_memory_ids, field_name="raw_memory_ids")
            _ensure_raw_ids_exist(repo, ids)
            record = repo.create_distilled_memory(
                title=norm_title,
                content=norm_content,
                raw_memory_ids=ids,
                tags=norm_tags,
                source_agent=source_agent,
                user=user,
            )
            _bump_manifest(record.tags, MemoryTier.DISTILLED, +1)
            return StructuredMemoryWriteResult(memory_id=record.id, tier="distilled", title=record.title)

        if tier == MemoryTier.CORE.value:
            ids = _non_empty_id_list(distilled_memory_ids, field_name="distilled_memory_ids")
            _ensure_distilled_ids_exist(repo, ids)
            record = repo.create_core_memory(
                title=norm_title,
                content=norm_content,
                distilled_memory_ids=ids,
                tags=norm_tags,
                source_agent=source_agent,
                user=user,
            )
            _bump_manifest(record.tags, MemoryTier.CORE, +1)
            return StructuredMemoryWriteResult(memory_id=record.id, tier="core", title=record.title)

        raise StructuredMemoryWriteError(f"Unknown tier {tier!r}; use raw, distilled, or core.")


def _normalize_path_list(values: list[str] | None) -> list[str]:
    if not values:
        return []
    out: list[str] = []
    seen: set[str] = set()
    for v in values:
        s = v.strip()
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def _non_empty_id_list(values: list[str] | None, *, field_name: str) -> list[str]:
    if not values:
        raise StructuredMemoryWriteError(f"{field_name} must contain at least one non-empty id.")
    ids = [v.strip() for v in values if v and str(v).strip()]
    if not ids:
        raise StructuredMemoryWriteError(f"{field_name} must contain at least one non-empty id.")
    return ids


def _ensure_raw_ids_exist(repo: StructuredMemoryRepository, ids: list[str]) -> None:
    missing = [i for i in ids if repo.get_raw_memory(i) is None]
    if missing:
        raise StructuredMemoryWriteError(
            f"raw_memory_ids reference unknown raw record(s): {', '.join(missing)}"
        )


def _ensure_distilled_ids_exist(repo: StructuredMemoryRepository, ids: list[str]) -> None:
    missing = [i for i in ids if repo.get_distilled_memory(i) is None]
    if missing:
        raise StructuredMemoryWriteError(
            f"distilled_memory_ids reference unknown distilled record(s): {', '.join(missing)}"
        )


def _bump_manifest(tags: list[str], tier: MemoryTier, delta: int) -> None:
    """Best-effort tag manifest cache update; never raises to callers."""
    if not tags or delta == 0:
        return
    try:
        get_tag_manifest_service().bump_counters(tags=tags, tier=tier, delta=delta)
    except Exception:  # pragma: no cover - cache maintenance must not block writes
        pass


def format_write_success(result: StructuredMemoryWriteResult) -> str:
    """Human-readable success line for tool output."""
    return (
        f"Structured memory written: id={result.memory_id} tier={result.tier} title={result.title!r}"
    )

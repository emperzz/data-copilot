"""ChromaDB-backed structured memory repository."""

from __future__ import annotations

import json
import threading
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import chromadb
from chromadb.utils import embedding_functions

from deerflow.config.memory_config import get_memory_config
from deerflow.config.paths import get_paths
from deerflow.config.structured_memory_config import (
    StructuredMemoryDisabledError,
    get_structured_memory_config,
)
from deerflow.memory._shared import (
    TIER_TO_COLLECTION_ATTR,
    _json_to_string_list,
    _memory_record_from_metadata,
    _string_list_to_json,
    utc_now_iso_z,
)
from deerflow.memory.models import (
    DEFAULT_MEMORY_AGENT,
    DEFAULT_MEMORY_USER,
    TITLE_MAX_LENGTH,
    CoreMemoryRecord,
    DistilledMemoryRecord,
    MemoryTier,
    RawMemoryRecord,
    TagManifestRecord,
)

RAW_COLLECTION_NAME = "memory_raw"
DISTILLED_COLLECTION_NAME = "memory_distilled"
CORE_COLLECTION_NAME = "memory_core"
TAG_MANIFEST_COLLECTION_NAME = "memory_tag_manifest"


def _fallback_title(document: str, *, max_len: int = TITLE_MAX_LENGTH) -> str:
    """Derive a non-empty title from body when metadata lacks one (e.g. legacy rows)."""
    text = document.strip().replace("\n", " ")
    if not text:
        return "Untitled"
    if len(text) <= max_len:
        return text
    return text[: max_len - 1].rstrip() + "…"


def _tag_manifest_from_metadata(tag_id: str, metadata: dict[str, Any]) -> TagManifestRecord | None:
    """Rebuild a ``TagManifestRecord`` from Chroma metadata; ``None`` on bad rows."""
    tag = (str(metadata.get("tag") or tag_id)).strip()
    if not tag:
        return None
    counts_raw = metadata.get("counts_json")
    counts: dict[str, int] = {}
    if isinstance(counts_raw, str) and counts_raw:
        try:
            parsed = json.loads(counts_raw)
        except json.JSONDecodeError:
            parsed = {}
        if isinstance(parsed, dict):
            for tier_key, value in parsed.items():
                try:
                    count_int = int(value)
                except (TypeError, ValueError):
                    continue
                if count_int > 0:
                    counts[str(tier_key)] = count_int
    total_raw = metadata.get("total")
    try:
        total = int(total_raw) if total_raw is not None else sum(counts.values())
    except (TypeError, ValueError):
        total = sum(counts.values())
    fallback_ts = utc_now_iso_z()
    return TagManifestRecord(
        tag=tag,
        counts_by_tier=counts,
        total=total,
        created_at=str(metadata.get("created_at") or fallback_ts),
        updated_at=str(metadata.get("updated_at") or fallback_ts),
        definition=str(metadata.get("definition") or ""),
        scope_keywords=_json_to_string_list(metadata.get("scope_keywords_json")),
    )


def _resolve_structured_persist_directory() -> Path:
    """Resolve structured memory storage directory using memory.json path rules."""
    config = get_memory_config()
    if config.storage_path:
        memory_file = Path(config.storage_path)
        if not memory_file.is_absolute():
            memory_file = get_paths().base_dir / memory_file
    else:
        memory_file = get_paths().memory_file
    return memory_file.parent / "chromadb"


def _resolve_structured_embedding_cache_directory() -> Path:
    """Resolve persistent cache dir for sentence-transformers model files."""
    # Keep embedding cache colocated with DeerFlow runtime state so it is
    # stable across host/container path differences.
    memory_file = get_paths().memory_file
    return memory_file.parent / "huggingface"


class StructuredMemoryRepository:
    """Repository for raw/distilled/core structured memory tiers."""

    def __init__(
        self,
        persist_directory: str | Path | None = None,
        default_user: str = DEFAULT_MEMORY_USER,
        default_source_agent: str = DEFAULT_MEMORY_AGENT,
        embedding_model_name: str | None = None,
    ) -> None:
        default_path = get_paths().base_dir / "memory" / "chromadb"
        self._persist_directory = Path(persist_directory or default_path)
        self._persist_directory.mkdir(parents=True, exist_ok=True)
        self._embedding_cache_directory = _resolve_structured_embedding_cache_directory()
        self._embedding_cache_directory.mkdir(parents=True, exist_ok=True)
        configured_model_name = embedding_model_name or get_structured_memory_config().embedding_model_name
        self._embedding_function = embedding_functions.SentenceTransformerEmbeddingFunction(
            model_name=configured_model_name,
            cache_folder=str(self._embedding_cache_directory),
        )
        self._client = chromadb.PersistentClient(path=str(self._persist_directory))
        self._raw_collection = self._client.get_or_create_collection(name=RAW_COLLECTION_NAME, embedding_function=self._embedding_function)
        self._distilled_collection = self._client.get_or_create_collection(name=DISTILLED_COLLECTION_NAME, embedding_function=self._embedding_function)
        self._core_collection = self._client.get_or_create_collection(name=CORE_COLLECTION_NAME, embedding_function=self._embedding_function)
        self._tag_manifest_collection = self._client.get_or_create_collection(name=TAG_MANIFEST_COLLECTION_NAME)
        self._default_user = default_user
        self._default_source_agent = default_source_agent

    @staticmethod
    def _resolve_collection_and_tier(memory_id: str) -> tuple[str, str] | None:
        memory_id = memory_id.strip()
        if memory_id.startswith("raw_"):
            return ("_raw_collection", "raw")
        if memory_id.startswith("distilled_"):
            return ("_distilled_collection", "distilled")
        if memory_id.startswith("core_"):
            return ("_core_collection", "core")
        return None

    def _resolve_common_fields(
        self,
        *,
        title: str,
        content: str,
        tags: list[str] | None,
        source_agent: str | None,
        user: str | None,
    ) -> dict[str, Any]:
        now = utc_now_iso_z()
        return {
            "title": title.strip()[:TITLE_MAX_LENGTH],
            "content": content,
            "created_at": now,
            "updated_at": now,
            "tags": tags or [],
            "source_agent": source_agent or self._default_source_agent,
            "user": user or self._default_user,
        }

    def create_raw_memory(
        self,
        *,
        title: str,
        content: str,
        source_thread_id: str,
        tags: list[str] | None = None,
        source_agent: str | None = None,
        user: str | None = None,
        attachment_file_paths: list[str] | None = None,
        attachment_image_paths: list[str] | None = None,
        inline_web_urls: list[str] | None = None,
    ) -> RawMemoryRecord:
        """Create and persist one raw memory record anchored on a conversation thread."""
        record_id = f"raw_{uuid.uuid4().hex}"
        payload = self._resolve_common_fields(
            title=title,
            content=content,
            tags=tags,
            source_agent=source_agent,
            user=user,
        )
        files = list(attachment_file_paths or [])
        images = list(attachment_image_paths or [])
        urls = list(inline_web_urls or [])
        record = RawMemoryRecord(
            id=record_id,
            title=payload["title"],
            content=payload["content"],
            created_at=payload["created_at"],
            updated_at=payload["updated_at"],
            tags=payload["tags"],
            source_agent=payload["source_agent"],
            user=payload["user"],
            source_thread_id=source_thread_id.strip(),
            attachment_file_paths=files,
            attachment_image_paths=images,
            inline_web_urls=urls,
        )
        metadata: dict[str, Any] = {
            "created_at": record.created_at,
            "updated_at": record.updated_at,
            "title": record.title,
            "tags_json": _string_list_to_json(record.tags),
            "source_agent": record.source_agent,
            "user": record.user,
            "source_thread_id": record.source_thread_id,
            "attachment_file_paths_json": _string_list_to_json(record.attachment_file_paths),
            "attachment_image_paths_json": _string_list_to_json(record.attachment_image_paths),
            "inline_web_urls_json": _string_list_to_json(record.inline_web_urls),
        }
        self._raw_collection.add(
            ids=[record.id],
            documents=[record.content],
            metadatas=[metadata],
        )
        return record

    def create_distilled_memory(
        self,
        *,
        title: str,
        content: str,
        raw_memory_ids: list[str],
        tags: list[str] | None = None,
        source_agent: str | None = None,
        user: str | None = None,
    ) -> DistilledMemoryRecord:
        """Create and persist one distilled memory record."""
        record_id = f"distilled_{uuid.uuid4().hex}"
        payload = self._resolve_common_fields(
            title=title,
            content=content,
            tags=tags,
            source_agent=source_agent,
            user=user,
        )
        record = DistilledMemoryRecord(
            id=record_id,
            title=payload["title"],
            content=payload["content"],
            created_at=payload["created_at"],
            updated_at=payload["updated_at"],
            tags=payload["tags"],
            source_agent=payload["source_agent"],
            user=payload["user"],
            raw_memory_ids=raw_memory_ids,
        )
        metadata = {
            "created_at": record.created_at,
            "updated_at": record.updated_at,
            "title": record.title,
            "tags_json": _string_list_to_json(record.tags),
            "source_agent": record.source_agent,
            "user": record.user,
            "raw_memory_ids_json": _string_list_to_json(record.raw_memory_ids),
        }
        self._distilled_collection.add(
            ids=[record.id],
            documents=[record.content],
            metadatas=[metadata],
        )
        return record

    def create_core_memory(
        self,
        *,
        title: str,
        content: str,
        distilled_memory_ids: list[str],
        tags: list[str] | None = None,
        source_agent: str | None = None,
        user: str | None = None,
    ) -> CoreMemoryRecord:
        """Create and persist one core memory record linked to one or more distilled rows."""
        record_id = f"core_{uuid.uuid4().hex}"
        payload = self._resolve_common_fields(
            title=title,
            content=content,
            tags=tags,
            source_agent=source_agent,
            user=user,
        )
        record = CoreMemoryRecord(
            id=record_id,
            title=payload["title"],
            content=payload["content"],
            created_at=payload["created_at"],
            updated_at=payload["updated_at"],
            tags=payload["tags"],
            source_agent=payload["source_agent"],
            user=payload["user"],
            distilled_memory_ids=distilled_memory_ids,
        )
        metadata = {
            "created_at": record.created_at,
            "updated_at": record.updated_at,
            "title": record.title,
            "tags_json": _string_list_to_json(record.tags),
            "source_agent": record.source_agent,
            "user": record.user,
            "distilled_memory_ids_json": _string_list_to_json(record.distilled_memory_ids),
        }
        self._core_collection.add(
            ids=[record.id],
            documents=[record.content],
            metadatas=[metadata],
        )
        return record

    def _migrate_legacy_raw_metadata(self, metadata: dict[str, Any], document: str) -> RawMemoryRecord:
        """Build RawMemoryRecord from Chroma metadata, including pre-refactor rows."""
        files = _json_to_string_list(metadata.get("attachment_file_paths_json"))
        images = _json_to_string_list(metadata.get("attachment_image_paths_json"))
        urls = _json_to_string_list(metadata.get("inline_web_urls_json"))

        legacy_file = metadata.get("source_file_path")
        if isinstance(legacy_file, str) and legacy_file.strip() and legacy_file not in files:
            files.append(legacy_file)
        legacy_image = metadata.get("source_image_path")
        if isinstance(legacy_image, str) and legacy_image.strip() and legacy_image not in images:
            images.append(legacy_image)
        legacy_url = metadata.get("source_web_url")
        if isinstance(legacy_url, str) and legacy_url.strip() and legacy_url not in urls:
            urls.append(legacy_url)

        thread_id = metadata.get("source_thread_id")
        if not isinstance(thread_id, str) or not thread_id.strip():
            thread_id = "legacy-unknown-thread"

        raw_title = metadata.get("title")
        title = (str(raw_title).strip() if raw_title is not None else "") or _fallback_title(document)

        memory_id = str(metadata.get("id", "")).strip()
        return RawMemoryRecord(
            id=memory_id,
            title=title[:TITLE_MAX_LENGTH],
            content=document,
            created_at=str(metadata.get("created_at", "")),
            updated_at=str(metadata.get("updated_at", "")),
            tags=_json_to_string_list(metadata.get("tags_json")),
            source_agent=str(metadata.get("source_agent", self._default_source_agent)),
            user=str(metadata.get("user", self._default_user)),
            source_thread_id=thread_id.strip(),
            attachment_file_paths=files,
            attachment_image_paths=images,
            inline_web_urls=urls,
        )

    def get_raw_memory(self, memory_id: str) -> RawMemoryRecord | None:
        """Fetch one raw memory by id."""
        result = self._raw_collection.get(ids=[memory_id], include=["documents", "metadatas"])
        ids = result.get("ids") or []
        if not ids:
            return None
        metadata = (result.get("metadatas") or [{}])[0] or {}
        document = ((result.get("documents") or [""])[0] or "").strip()
        metadata = {**metadata, "id": ids[0]}
        return _memory_record_from_metadata(MemoryTier.RAW, ids, metadata, document)

    def get_distilled_memory(self, memory_id: str) -> DistilledMemoryRecord | None:
        """Fetch one distilled memory by id."""
        result = self._distilled_collection.get(ids=[memory_id], include=["documents", "metadatas"])
        ids = result.get("ids") or []
        if not ids:
            return None
        metadata = (result.get("metadatas") or [{}])[0] or {}
        document = ((result.get("documents") or [""])[0] or "").strip()
        return _memory_record_from_metadata(MemoryTier.DISTILLED, ids, metadata, document)

    def get_core_memory(self, memory_id: str) -> CoreMemoryRecord | None:
        """Fetch one core memory by id."""
        result = self._core_collection.get(ids=[memory_id], include=["documents", "metadatas"])
        ids = result.get("ids") or []
        if not ids:
            return None
        metadata = (result.get("metadatas") or [{}])[0] or {}
        document = ((result.get("documents") or [""])[0] or "").strip()
        return _memory_record_from_metadata(MemoryTier.CORE, ids, metadata, document)

    def update_memory(
        self,
        *,
        memory_id: str,
        title: str,
        content: str,
        tags: list[str] | None = None,
        source_agent: str | None = None,
        user: str | None = None,
    ) -> bool:
        """Update common fields for one memory record by id.

        Returns ``True`` when the record exists and was updated, ``False`` when
        id prefix is unsupported or no record with that id exists.
        """
        resolved = self._resolve_collection_and_tier(memory_id)
        if resolved is None:
            return False
        collection_attr, _tier = resolved
        collection = getattr(self, collection_attr)

        result = collection.get(ids=[memory_id], include=["documents", "metadatas"])
        ids = result.get("ids") or []
        if not ids:
            return False
        metadata = (result.get("metadatas") or [{}])[0] or {}

        now = utc_now_iso_z()
        next_metadata: dict[str, Any] = {
            **metadata,
            "title": title.strip()[:TITLE_MAX_LENGTH],
            "updated_at": now,
            "tags_json": _string_list_to_json(tags or []),
            "source_agent": source_agent or str(metadata.get("source_agent", self._default_source_agent)),
            "user": user or str(metadata.get("user", self._default_user)),
        }
        collection.update(
            ids=[memory_id],
            documents=[content],
            metadatas=[next_metadata],
        )
        return True

    def delete_memory(self, memory_id: str) -> bool:
        """Delete one memory record by id, returning whether it existed."""
        resolved = self._resolve_collection_and_tier(memory_id)
        if resolved is None:
            return False
        collection_attr, _tier = resolved
        collection = getattr(self, collection_attr)
        ids = collection.get(ids=[memory_id], include=[]).get("ids") or []
        if not ids:
            return False
        collection.delete(ids=[memory_id])
        return True

    # ------------------------------------------------------------------
    # tag manifest (sibling collection used by TagManifestService)
    # ------------------------------------------------------------------

    def get_tag_manifest(self, tag: str) -> TagManifestRecord | None:
        """Return persisted manifest for ``tag`` or ``None`` if missing."""
        normalized = tag.strip()
        if not normalized:
            return None
        result = self._tag_manifest_collection.get(ids=[normalized], include=["metadatas"])
        ids = result.get("ids") or []
        if not ids:
            return None
        metadata = (result.get("metadatas") or [{}])[0] or {}
        return _tag_manifest_from_metadata(normalized, metadata)

    def list_tag_manifest(self) -> list[TagManifestRecord]:
        """Return every persisted tag manifest record (unordered)."""
        if self._tag_manifest_collection.count() == 0:
            return []
        result = self._tag_manifest_collection.get(include=["metadatas"])
        records: list[TagManifestRecord] = []
        for idx, tag_id in enumerate(result.get("ids") or []):
            metadata = (result.get("metadatas") or [{}])[idx] or {}
            record = _tag_manifest_from_metadata(tag_id, metadata)
            if record is not None:
                records.append(record)
        return records

    def upsert_tag_manifest(
        self,
        *,
        tag: str,
        counts_by_tier: dict[str, int],
        definition: str | None = None,
        scope_keywords: list[str] | None = None,
    ) -> TagManifestRecord:
        """Insert or update one tag manifest record; preserves ``created_at``."""
        normalized_tag = tag.strip()
        if not normalized_tag:
            raise ValueError("tag must be non-empty")
        filtered = {str(t): int(c) for t, c in (counts_by_tier or {}).items() if int(c) > 0}
        total = sum(filtered.values())
        now = utc_now_iso_z()

        existing_meta: dict[str, Any] = {}
        existing = self._tag_manifest_collection.get(ids=[normalized_tag], include=["metadatas"])
        if existing.get("ids"):
            existing_meta = (existing.get("metadatas") or [{}])[0] or {}
        created_at = str(existing_meta.get("created_at") or now)

        metadata = {
            "tag": normalized_tag,
            "counts_json": json.dumps(filtered, ensure_ascii=False, sort_keys=True),
            "total": total,
            "created_at": created_at,
            "updated_at": now,
            "definition": definition if definition is not None else str(existing_meta.get("definition") or ""),
            "scope_keywords_json": _string_list_to_json(
                scope_keywords if scope_keywords is not None else _json_to_string_list(existing_meta.get("scope_keywords_json")),
            ),
        }
        self._tag_manifest_collection.upsert(
            ids=[normalized_tag],
            documents=[normalized_tag],
            metadatas=[metadata],
        )
        return TagManifestRecord(
            tag=normalized_tag,
            counts_by_tier=filtered,
            total=total,
            created_at=created_at,
            updated_at=now,
            definition=str(metadata["definition"]),
            scope_keywords=_json_to_string_list(metadata["scope_keywords_json"]),
        )

    def delete_tag_manifest(self, tag: str) -> bool:
        """Remove one tag manifest record; returns whether it existed."""
        normalized = tag.strip()
        if not normalized:
            return False
        existing = self._tag_manifest_collection.get(ids=[normalized], include=[])
        if not (existing.get("ids") or []):
            return False
        self._tag_manifest_collection.delete(ids=[normalized])
        return True

    # ------------------------------------------------------------------
    # tier-scoped public API (replaces getattr access)
    # ------------------------------------------------------------------

    def iter_tag_metadata(self, tier: MemoryTier) -> Iterator[dict[str, Any]]:
        """Yield metadata dicts for all records in ``tier``."""
        collection = self._collection_for_tier(tier)
        if collection.count() == 0:
            return
        result = collection.get(include=["metadatas"])
        for meta in result.get("metadatas") or []:
            if meta:
                yield meta

    def iter_lineage_refs(
        self, tier: MemoryTier
    ) -> Iterator[tuple[str, list[str]]]:
        """Yield (id, upstream_ids) for records in ``tier`` that have lineage.

        - DISTILLED → yields (distilled_id, raw_memory_ids)
        - CORE → yields (core_id, distilled_memory_ids)
        - RAW → yields nothing (raw has no upstream)
        """
        if tier == MemoryTier.RAW:
            return
        collection = self._collection_for_tier(tier)
        if collection.count() == 0:
            return
        result = collection.get(ids=None, include=["metadatas"])
        ids_list = result.get("ids") or []
        metas_list = result.get("metadatas") or []
        for idx, mid in enumerate(ids_list):
            meta = metas_list[idx] if idx < len(metas_list) else {}
            if not meta:
                continue
            if tier == MemoryTier.DISTILLED:
                upstream = _json_to_string_list(meta.get("raw_memory_ids_json"))
            elif tier == MemoryTier.CORE:
                upstream = _json_to_string_list(meta.get("distilled_memory_ids_json"))
            else:
                continue
            if upstream:
                yield (mid, upstream)

    def find_distilled_referencing_raw(self, raw_id: str) -> list[str]:
        """Return ids of distilled records that reference ``raw_id``."""
        blockers: list[str] = []
        for dist_id, raw_ids in self.iter_lineage_refs(MemoryTier.DISTILLED):
            if raw_id in raw_ids:
                blockers.append(dist_id)
        return blockers

    def find_core_referencing_distilled(self, distilled_id: str) -> list[str]:
        """Return ids of core records that reference ``distilled_id``."""
        blockers: list[str] = []
        for core_id, distilled_ids in self.iter_lineage_refs(MemoryTier.CORE):
            if distilled_id in distilled_ids:
                blockers.append(core_id)
        return blockers

    def count(self, tier: MemoryTier) -> int:
        """Return the number of records in ``tier``."""
        return self._collection_for_tier(tier).count()

    def _collection_for_tier(self, tier: MemoryTier):
        """Return the Chroma collection for ``tier``."""
        return getattr(self, TIER_TO_COLLECTION_ATTR[tier])


_repository_instance: StructuredMemoryRepository | None = None
_repository_lock = threading.Lock()


def initialize_structured_memory_chromadb() -> StructuredMemoryRepository:
    """Reset structured-memory Chroma collections and recreate them.

    This function is intentionally backend-only and meant for manual invocation
    during development/testing cleanup. It:
    1) drops all structured-memory collections to remove historical/dirty data;
    2) recreates them with the current configured embedding function to avoid
       persisted embedding-function conflicts;
    3) resets process singletons and returns a fresh repository instance.
    """
    sm_cfg = get_structured_memory_config()
    if not sm_cfg.enabled:
        raise StructuredMemoryDisabledError(
            "Structured memory is disabled; set structured_memory.enabled to true in config.yaml.",
        )

    persist_directory = _resolve_structured_persist_directory()
    persist_directory.mkdir(parents=True, exist_ok=True)
    embedding_cache_directory = _resolve_structured_embedding_cache_directory()
    embedding_cache_directory.mkdir(parents=True, exist_ok=True)
    client = chromadb.PersistentClient(path=str(persist_directory))

    existing_names = {collection.name for collection in client.list_collections()}
    for name in (
        RAW_COLLECTION_NAME,
        DISTILLED_COLLECTION_NAME,
        CORE_COLLECTION_NAME,
        TAG_MANIFEST_COLLECTION_NAME,
    ):
        if name in existing_names:
            client.delete_collection(name=name)

    embedding_function = embedding_functions.SentenceTransformerEmbeddingFunction(
        model_name=sm_cfg.embedding_model_name,
        cache_folder=str(embedding_cache_directory),
    )
    client.get_or_create_collection(name=RAW_COLLECTION_NAME, embedding_function=embedding_function)
    client.get_or_create_collection(name=DISTILLED_COLLECTION_NAME, embedding_function=embedding_function)
    client.get_or_create_collection(name=CORE_COLLECTION_NAME, embedding_function=embedding_function)
    client.get_or_create_collection(name=TAG_MANIFEST_COLLECTION_NAME)

    reset_structured_memory_repository_singleton()
    from deerflow.memory.tag_manifest_service import reset_tag_manifest_service_singleton

    reset_tag_manifest_service_singleton()
    return get_structured_memory_repository()


def reset_structured_memory_repository_singleton() -> None:
    """Clear the process-wide repository singleton (for tests or config hot-swap)."""
    global _repository_instance
    _repository_instance = None


def get_structured_memory_repository() -> StructuredMemoryRepository:
    """Return global structured memory repository singleton."""
    sm_cfg = get_structured_memory_config()
    if not sm_cfg.enabled:
        raise StructuredMemoryDisabledError(
            "Structured memory is disabled; set structured_memory.enabled to true in config.yaml.",
        )
    global _repository_instance
    if _repository_instance is not None:
        return _repository_instance
    with _repository_lock:
        if _repository_instance is None:
            _repository_instance = StructuredMemoryRepository(
                persist_directory=_resolve_structured_persist_directory(),
                embedding_model_name=sm_cfg.embedding_model_name,
            )
    return _repository_instance

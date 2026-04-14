"""ChromaDB-backed structured memory repository."""

from __future__ import annotations

import json
import threading
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import chromadb

from deerflow.config.memory_config import get_memory_config
from deerflow.config.paths import get_paths
from deerflow.config.structured_memory_config import (
    StructuredMemoryDisabledError,
    get_structured_memory_config,
)
from deerflow.memory.models import (
    DEFAULT_MEMORY_AGENT,
    DEFAULT_MEMORY_USER,
    CoreMemoryRecord,
    DistilledMemoryRecord,
    RawMemoryRecord,
    TITLE_MAX_LENGTH,
)

RAW_COLLECTION_NAME = "memory_raw"
DISTILLED_COLLECTION_NAME = "memory_distilled"
CORE_COLLECTION_NAME = "memory_core"


def utc_now_iso_z() -> str:
    """Return current UTC timestamp in ISO8601 with Z suffix."""
    return datetime.now(UTC).isoformat().removesuffix("+00:00") + "Z"


def _string_list_to_json(values: list[str]) -> str:
    return json.dumps(values, ensure_ascii=False)


def _json_to_string_list(raw: Any) -> list[str]:
    if not raw:
        return []
    if not isinstance(raw, str):
        return []
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item) for item in parsed if isinstance(item, str)]


def _fallback_title(document: str, *, max_len: int = TITLE_MAX_LENGTH) -> str:
    """Derive a non-empty title from body when metadata lacks one (e.g. legacy rows)."""
    text = document.strip().replace("\n", " ")
    if not text:
        return "Untitled"
    if len(text) <= max_len:
        return text
    return text[: max_len - 1].rstrip() + "…"


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


class StructuredMemoryRepository:
    """Repository for raw/distilled/core structured memory tiers."""

    def __init__(
        self,
        persist_directory: str | Path | None = None,
        default_user: str = DEFAULT_MEMORY_USER,
        default_source_agent: str = DEFAULT_MEMORY_AGENT,
    ) -> None:
        default_path = get_paths().base_dir / "memory" / "chromadb"
        self._persist_directory = Path(persist_directory or default_path)
        self._persist_directory.mkdir(parents=True, exist_ok=True)
        self._client = chromadb.PersistentClient(path=str(self._persist_directory))
        self._raw_collection = self._client.get_or_create_collection(name=RAW_COLLECTION_NAME)
        self._distilled_collection = self._client.get_or_create_collection(name=DISTILLED_COLLECTION_NAME)
        self._core_collection = self._client.get_or_create_collection(name=CORE_COLLECTION_NAME)
        self._default_user = default_user
        self._default_source_agent = default_source_agent

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
        return self._migrate_legacy_raw_metadata(metadata, document)

    def get_distilled_memory(self, memory_id: str) -> DistilledMemoryRecord | None:
        """Fetch one distilled memory by id."""
        result = self._distilled_collection.get(ids=[memory_id], include=["documents", "metadatas"])
        ids = result.get("ids") or []
        if not ids:
            return None
        metadata = (result.get("metadatas") or [{}])[0] or {}
        document = ((result.get("documents") or [""])[0] or "").strip()
        title_raw = metadata.get("title")
        title = (str(title_raw).strip() if title_raw is not None else "") or _fallback_title(document)
        return DistilledMemoryRecord(
            id=ids[0],
            title=title[:TITLE_MAX_LENGTH],
            content=document,
            created_at=str(metadata.get("created_at", "")),
            updated_at=str(metadata.get("updated_at", "")),
            tags=_json_to_string_list(metadata.get("tags_json")),
            source_agent=str(metadata.get("source_agent", self._default_source_agent)),
            user=str(metadata.get("user", self._default_user)),
            raw_memory_ids=_json_to_string_list(metadata.get("raw_memory_ids_json")),
        )

    def get_core_memory(self, memory_id: str) -> CoreMemoryRecord | None:
        """Fetch one core memory by id."""
        result = self._core_collection.get(ids=[memory_id], include=["documents", "metadatas"])
        ids = result.get("ids") or []
        if not ids:
            return None
        metadata = (result.get("metadatas") or [{}])[0] or {}
        document = ((result.get("documents") or [""])[0] or "").strip()
        title_raw = metadata.get("title")
        title = (str(title_raw).strip() if title_raw is not None else "") or _fallback_title(document)
        distilled_ids = _json_to_string_list(metadata.get("distilled_memory_ids_json"))
        if not distilled_ids:
            return None
        return CoreMemoryRecord(
            id=ids[0],
            title=title[:TITLE_MAX_LENGTH],
            content=document,
            created_at=str(metadata.get("created_at", "")),
            updated_at=str(metadata.get("updated_at", "")),
            tags=_json_to_string_list(metadata.get("tags_json")),
            source_agent=str(metadata.get("source_agent", self._default_source_agent)),
            user=str(metadata.get("user", self._default_user)),
            distilled_memory_ids=distilled_ids,
        )


_repository_instance: StructuredMemoryRepository | None = None
_repository_lock = threading.Lock()


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
            )
    return _repository_instance

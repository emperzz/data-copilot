"""ChromaDB-backed layered memory repository."""

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
from deerflow.memory.models import (
    DEFAULT_MEMORY_AGENT,
    DEFAULT_MEMORY_USER,
    CoreMemoryRecord,
    DistilledMemoryRecord,
    RawMemoryKind,
    RawMemoryRecord,
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


def _resolve_layered_persist_directory() -> Path:
    """Resolve layered memory storage directory using memory.json path rules."""
    config = get_memory_config()
    if config.storage_path:
        memory_file = Path(config.storage_path)
        if not memory_file.is_absolute():
            memory_file = get_paths().base_dir / memory_file
    else:
        memory_file = get_paths().memory_file
    return memory_file.parent / "chromadb"


class LayeredMemoryRepository:
    """Repository for raw/distilled/core memory tiers."""

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
        content: str,
        tags: list[str] | None,
        source_agent: str | None,
        user: str | None,
    ) -> dict[str, Any]:
        now = utc_now_iso_z()
        return {
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
        content: str,
        raw_kind: RawMemoryKind | str,
        tags: list[str] | None = None,
        source_agent: str | None = None,
        user: str | None = None,
        source_thread_id: str | None = None,
        source_file_path: str | None = None,
        source_image_path: str | None = None,
        source_web_url: str | None = None,
    ) -> RawMemoryRecord:
        """Create and persist one raw memory record."""
        record_id = f"raw_{uuid.uuid4().hex}"
        payload = self._resolve_common_fields(
            content=content,
            tags=tags,
            source_agent=source_agent,
            user=user,
        )
        record = RawMemoryRecord(
            id=record_id,
            content=payload["content"],
            created_at=payload["created_at"],
            updated_at=payload["updated_at"],
            tags=payload["tags"],
            source_agent=payload["source_agent"],
            user=payload["user"],
            raw_kind=RawMemoryKind(raw_kind),
            source_thread_id=source_thread_id,
            source_file_path=source_file_path,
            source_image_path=source_image_path,
            source_web_url=source_web_url,
        )
        metadata = {
            "created_at": record.created_at,
            "updated_at": record.updated_at,
            "tags_json": _string_list_to_json(record.tags),
            "source_agent": record.source_agent,
            "user": record.user,
            "raw_kind": record.raw_kind.value,
        }
        if record.source_thread_id:
            metadata["source_thread_id"] = record.source_thread_id
        if record.source_file_path:
            metadata["source_file_path"] = record.source_file_path
        if record.source_image_path:
            metadata["source_image_path"] = record.source_image_path
        if record.source_web_url:
            metadata["source_web_url"] = record.source_web_url
        self._raw_collection.add(
            ids=[record.id],
            documents=[record.content],
            metadatas=[metadata],
        )
        return record

    def create_distilled_memory(
        self,
        *,
        content: str,
        raw_memory_ids: list[str],
        tags: list[str] | None = None,
        source_agent: str | None = None,
        user: str | None = None,
    ) -> DistilledMemoryRecord:
        """Create and persist one distilled memory record."""
        record_id = f"distilled_{uuid.uuid4().hex}"
        payload = self._resolve_common_fields(
            content=content,
            tags=tags,
            source_agent=source_agent,
            user=user,
        )
        record = DistilledMemoryRecord(
            id=record_id,
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
        content: str,
        distilled_memory_ids: list[str],
        tags: list[str] | None = None,
        source_agent: str | None = None,
        user: str | None = None,
    ) -> CoreMemoryRecord:
        """Create and persist one core memory record."""
        record_id = f"core_{uuid.uuid4().hex}"
        payload = self._resolve_common_fields(
            content=content,
            tags=tags,
            source_agent=source_agent,
            user=user,
        )
        record = CoreMemoryRecord(
            id=record_id,
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

    def get_raw_memory(self, memory_id: str) -> RawMemoryRecord | None:
        """Fetch one raw memory by id."""
        result = self._raw_collection.get(ids=[memory_id], include=["documents", "metadatas"])
        ids = result.get("ids") or []
        if not ids:
            return None
        metadata = (result.get("metadatas") or [{}])[0] or {}
        document = ((result.get("documents") or [""])[0] or "").strip()
        return RawMemoryRecord(
            id=ids[0],
            content=document,
            created_at=str(metadata.get("created_at", "")),
            updated_at=str(metadata.get("updated_at", "")),
            tags=_json_to_string_list(metadata.get("tags_json")),
            source_agent=str(metadata.get("source_agent", self._default_source_agent)),
            user=str(metadata.get("user", self._default_user)),
            raw_kind=RawMemoryKind(str(metadata.get("raw_kind", RawMemoryKind.SESSION.value))),
            source_thread_id=metadata.get("source_thread_id"),
            source_file_path=metadata.get("source_file_path"),
            source_image_path=metadata.get("source_image_path"),
            source_web_url=metadata.get("source_web_url"),
        )

    def get_distilled_memory(self, memory_id: str) -> DistilledMemoryRecord | None:
        """Fetch one distilled memory by id."""
        result = self._distilled_collection.get(ids=[memory_id], include=["documents", "metadatas"])
        ids = result.get("ids") or []
        if not ids:
            return None
        metadata = (result.get("metadatas") or [{}])[0] or {}
        document = ((result.get("documents") or [""])[0] or "").strip()
        return DistilledMemoryRecord(
            id=ids[0],
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
        return CoreMemoryRecord(
            id=ids[0],
            content=document,
            created_at=str(metadata.get("created_at", "")),
            updated_at=str(metadata.get("updated_at", "")),
            tags=_json_to_string_list(metadata.get("tags_json")),
            source_agent=str(metadata.get("source_agent", self._default_source_agent)),
            user=str(metadata.get("user", self._default_user)),
            distilled_memory_ids=_json_to_string_list(metadata.get("distilled_memory_ids_json")),
        )


_repository_instance: LayeredMemoryRepository | None = None
_repository_lock = threading.Lock()


def get_layered_memory_repository() -> LayeredMemoryRepository:
    """Return global layered memory repository singleton."""
    global _repository_instance
    if _repository_instance is not None:
        return _repository_instance
    with _repository_lock:
        if _repository_instance is None:
            _repository_instance = LayeredMemoryRepository(
                persist_directory=_resolve_layered_persist_directory(),
            )
    return _repository_instance

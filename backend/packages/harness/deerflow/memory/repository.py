"""Chroma persistence for skill-oriented memory items."""

from __future__ import annotations

import hashlib
import json
import math
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from deerflow.config.memory_config import get_memory_config
from deerflow.config.paths import get_paths
from deerflow.memory.models import MemoryItem, MemoryMetadata, MemorySearchHit

_COLLECTION_NAME = "skill_memory"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _dt_to_iso(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _dt_from_iso(value: str) -> datetime:
    if value.endswith("Z"):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    return datetime.fromisoformat(value)


def _hash_embed(text: str, dimensions: int) -> list[float]:
    source = text.strip() or "empty"
    vec = [0.0] * dimensions
    for token in source.lower().split():
        digest = hashlib.sha256(token.encode("utf-8")).digest()
        slot = int.from_bytes(digest[:4], "big") % dimensions
        sign = 1.0 if digest[4] % 2 == 0 else -1.0
        magnitude = (digest[5] / 255.0) + 0.05
        vec[slot] += sign * magnitude

    norm = math.sqrt(sum(v * v for v in vec))
    if norm == 0:
        return vec
    return [v / norm for v in vec]


class MemoryRepository:
    def __init__(self, db_path: Path | None = None, vector_dimensions: int | None = None) -> None:
        config = get_memory_config()
        if db_path is not None:
            self._db_path = db_path
        elif config.db_path:
            p = Path(config.db_path)
            self._db_path = p if p.is_absolute() else get_paths().base_dir / p
        else:
            self._db_path = get_paths().memory_chroma_dir

        self._vector_dimensions = vector_dimensions if vector_dimensions is not None else config.vector_dimensions

    def _build_embedding(self, text: str) -> list[float]:
        return _hash_embed(text, self._vector_dimensions)

    def _get_collection(self):
        try:
            import chromadb  # type: ignore
        except ImportError as exc:
            raise RuntimeError("chromadb is required for memory provider 'chroma'") from exc

        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        client = chromadb.PersistentClient(path=str(self._db_path))
        return client.get_or_create_collection(name=_COLLECTION_NAME)

    def ensure_schema(self) -> None:
        self._get_collection()

    @staticmethod
    def _to_item(memory_id: str, document: str, metadata: dict[str, Any] | None) -> MemoryItem:
        metadata = metadata or {}
        raw_user_metadata = metadata.get("user_metadata_json", "{}")
        try:
            user_metadata = json.loads(raw_user_metadata)
        except json.JSONDecodeError:
            user_metadata = {}

        return MemoryItem(
            id=memory_id,
            content=document,
            metadata=MemoryMetadata(
                namespace=str(metadata.get("namespace", "")),
                memory_key=str(metadata["memory_key"]) if "memory_key" in metadata else None,
                source_skill=str(metadata["source_skill"]) if "source_skill" in metadata else None,
                extra=user_metadata if isinstance(user_metadata, dict) else {},
            ),
            created_at=_dt_from_iso(str(metadata.get("created_at", _dt_to_iso(_utc_now())))),
            updated_at=_dt_from_iso(str(metadata.get("updated_at", _dt_to_iso(_utc_now())))),
        )

    def upsert_memory(
        self,
        *,
        namespace: str,
        content: str,
        memory_key: str | None = None,
        source_skill: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> MemoryItem:
        if not namespace.strip():
            raise ValueError("namespace cannot be empty")
        if not content.strip():
            raise ValueError("content cannot be empty")

        collection = self._get_collection()
        now_iso = _dt_to_iso(_utc_now())
        user_metadata_json = json.dumps(metadata or {}, ensure_ascii=False)

        memory_id = str(uuid.uuid4())
        created_at = now_iso

        if memory_key is not None:
            existing = collection.get(
                where={"$and": [{"namespace": namespace}, {"memory_key": memory_key}]},
                include=["metadatas"],
            )
            existing_ids = existing.get("ids") or []
            if existing_ids:
                memory_id = str(existing_ids[0])
                existing_metadatas = existing.get("metadatas") or []
                if existing_metadatas and existing_metadatas[0] and "created_at" in existing_metadatas[0]:
                    created_at = str(existing_metadatas[0]["created_at"])

        chroma_metadata: dict[str, Any] = {
            "namespace": namespace,
            "created_at": created_at,
            "updated_at": now_iso,
            "user_metadata_json": user_metadata_json,
        }
        if memory_key is not None:
            chroma_metadata["memory_key"] = memory_key
        if source_skill is not None:
            chroma_metadata["source_skill"] = source_skill

        collection.upsert(
            ids=[memory_id],
            documents=[content],
            metadatas=[chroma_metadata],
            embeddings=[self._build_embedding(content)],
        )

        return self._to_item(memory_id, content, chroma_metadata)

    def list_memories(self, *, namespace: str, limit: int = 50) -> list[MemoryItem]:
        if limit <= 0:
            return []
        collection = self._get_collection()
        result = collection.get(where={"namespace": namespace}, include=["documents", "metadatas"])
        ids = result.get("ids") or []
        documents = result.get("documents") or []
        metadatas = result.get("metadatas") or []

        items: list[MemoryItem] = []
        for memory_id, document, metadata in zip(ids, documents, metadatas, strict=False):
            items.append(self._to_item(str(memory_id), str(document or ""), metadata))

        items.sort(key=lambda item: item.updated_at, reverse=True)
        return items[:limit]

    def search(self, *, namespace: str, query: str, limit: int = 5) -> list[MemorySearchHit]:
        if not query.strip() or limit <= 0:
            return []
        collection = self._get_collection()
        result = collection.query(
            query_embeddings=[self._build_embedding(query)],
            n_results=limit,
            where={"namespace": namespace},
            include=["documents", "metadatas", "distances"],
        )

        ids_list = result.get("ids") or [[]]
        docs_list = result.get("documents") or [[]]
        metas_list = result.get("metadatas") or [[]]
        dists_list = result.get("distances") or [[]]

        hits: list[MemorySearchHit] = []
        for memory_id, document, metadata, distance in zip(
            ids_list[0],
            docs_list[0],
            metas_list[0],
            dists_list[0],
            strict=False,
        ):
            score = 1.0 / (1.0 + float(distance))
            hits.append(
                MemorySearchHit(
                    item=self._to_item(str(memory_id), str(document or ""), metadata),
                    score=score,
                )
            )
        return hits

    def delete_memory(self, memory_id: str) -> bool:
        collection = self._get_collection()
        existing = collection.get(ids=[memory_id], include=[])
        existed = bool(existing.get("ids"))
        if existed:
            collection.delete(ids=[memory_id])
        return existed


_memory_repository: MemoryRepository | None = None


def get_memory_repository() -> MemoryRepository:
    global _memory_repository
    if _memory_repository is None:
        _memory_repository = MemoryRepository()
        _memory_repository.ensure_schema()
    return _memory_repository


def reset_memory_repository() -> None:
    global _memory_repository
    _memory_repository = None

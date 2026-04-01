from __future__ import annotations

import json
import math
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from deerflow.memory import MemoryRepository, reset_memory_repository
from deerflow.tools.builtins import memory_tool as memory_tool_module


class _FakeCollection:
    def __init__(self) -> None:
        self._rows: dict[str, dict] = {}

    def upsert(
        self,
        *,
        ids: list[str],
        documents: list[str],
        metadatas: list[dict],
        embeddings: list[list[float]],
    ) -> None:
        for memory_id, document, metadata, embedding in zip(ids, documents, metadatas, embeddings, strict=True):
            self._rows[memory_id] = {
                "id": memory_id,
                "document": document,
                "metadata": metadata,
                "embedding": embedding,
            }

    def get(self, where=None, ids=None, include=None):  # noqa: ANN001
        rows = list(self._rows.values())
        if ids is not None:
            id_set = set(ids)
            rows = [r for r in rows if r["id"] in id_set]
        if where is not None:
            rows = [r for r in rows if _match_where(r["metadata"], where)]
        return {
            "ids": [r["id"] for r in rows],
            "documents": [r["document"] for r in rows],
            "metadatas": [r["metadata"] for r in rows],
        }

    def query(
        self,
        *,
        query_embeddings: list[list[float]],
        n_results: int,
        where=None,  # noqa: ANN001
        include=None,  # noqa: ANN001
    ):
        rows = list(self._rows.values())
        if where is not None:
            rows = [r for r in rows if _match_where(r["metadata"], where)]
        query_vec = query_embeddings[0]
        scored = []
        for row in rows:
            distance = _cosine_distance(query_vec, row["embedding"])
            scored.append((distance, row))
        scored.sort(key=lambda x: x[0])
        top = scored[:n_results]
        return {
            "ids": [[row["id"] for _, row in top]],
            "documents": [[row["document"] for _, row in top]],
            "metadatas": [[row["metadata"] for _, row in top]],
            "distances": [[distance for distance, _ in top]],
        }

    def delete(self, *, ids: list[str]) -> None:
        for memory_id in ids:
            self._rows.pop(memory_id, None)


class _FakePersistentClient:
    _collections: dict[str, _FakeCollection] = {}

    def __init__(self, path: str) -> None:  # noqa: ARG002
        pass

    def get_or_create_collection(self, *, name: str) -> _FakeCollection:
        if name not in self._collections:
            self._collections[name] = _FakeCollection()
        return self._collections[name]


def _match_where(metadata: dict, where: dict) -> bool:
    if "$and" in where:
        return all(_match_where(metadata, sub) for sub in where["$and"])
    for key, value in where.items():
        if metadata.get(key) != value:
            return False
    return True


def _cosine_distance(left: list[float], right: list[float]) -> float:
    dot = sum(a * b for a, b in zip(left, right, strict=True))
    left_norm = math.sqrt(sum(a * a for a in left))
    right_norm = math.sqrt(sum(b * b for b in right))
    if left_norm == 0 or right_norm == 0:
        return 1.0
    similarity = dot / (left_norm * right_norm)
    return 1.0 - similarity


@pytest.fixture
def fake_chroma(monkeypatch: pytest.MonkeyPatch) -> None:
    _FakePersistentClient._collections = {}
    monkeypatch.setitem(sys.modules, "chromadb", SimpleNamespace(PersistentClient=_FakePersistentClient))
    reset_memory_repository()


def test_memory_repository_upsert_list_and_metadata(fake_chroma: None, tmp_path: Path) -> None:
    repo = MemoryRepository(db_path=tmp_path / "memory_chroma", vector_dimensions=32)
    repo.ensure_schema()
    item = repo.upsert_memory(
        namespace="skill:datawarehouse-processor",
        content="confirmed engine is spark sql",
        memory_key="engine",
        source_skill="datawarehouse-processor",
        metadata={"confidence": "high"},
    )

    assert item.metadata.namespace == "skill:datawarehouse-processor"
    assert item.metadata.memory_key == "engine"
    assert item.metadata.source_skill == "datawarehouse-processor"
    assert item.metadata.extra["confidence"] == "high"

    recent = repo.list_memories(namespace="skill:datawarehouse-processor", limit=10)
    assert len(recent) == 1
    assert recent[0].id == item.id


def test_memory_repository_upsert_with_key_is_idempotent(fake_chroma: None, tmp_path: Path) -> None:
    repo = MemoryRepository(db_path=tmp_path / "memory_chroma", vector_dimensions=32)
    repo.ensure_schema()

    first = repo.upsert_memory(
        namespace="skill:datawarehouse-processor",
        content="initial memory",
        memory_key="session_rule",
        source_skill="datawarehouse-processor",
    )
    second = repo.upsert_memory(
        namespace="skill:datawarehouse-processor",
        content="updated memory",
        memory_key="session_rule",
        source_skill="datawarehouse-processor",
    )

    assert first.id == second.id
    listed = repo.list_memories(namespace="skill:datawarehouse-processor", limit=10)
    assert len(listed) == 1
    assert listed[0].content == "updated memory"


def test_memory_repository_search_and_delete(fake_chroma: None, tmp_path: Path) -> None:
    repo = MemoryRepository(db_path=tmp_path / "memory_chroma", vector_dimensions=64)
    repo.ensure_schema()
    target = repo.upsert_memory(
        namespace="skill:datawarehouse-processor",
        content="metric revenue uses paid_amount",
        memory_key="metric_revenue",
        source_skill="datawarehouse-processor",
    )
    repo.upsert_memory(
        namespace="skill:datawarehouse-processor",
        content="table dim_user keeps user profile attributes",
        memory_key="table_dim_user",
        source_skill="datawarehouse-processor",
    )

    hits = repo.search(
        namespace="skill:datawarehouse-processor",
        query="revenue metric definition",
        limit=2,
    )
    assert len(hits) == 2
    assert hits[0].item.id == target.id

    deleted = repo.delete_memory(target.id)
    assert deleted is True
    after_delete = repo.list_memories(namespace="skill:datawarehouse-processor", limit=10)
    assert all(item.id != target.id for item in after_delete)


def test_memory_tool_upsert_and_search(fake_chroma: None, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    repo = MemoryRepository(db_path=tmp_path / "memory_chroma", vector_dimensions=32)
    repo.ensure_schema()
    monkeypatch.setattr(memory_tool_module, "get_memory_repository", lambda: repo)

    upsert_raw = memory_tool_module.memory_upsert.invoke(
        {
            "namespace": "skill:datawarehouse-processor",
            "content": "confirmed partition field is dt",
            "memory_key": "partition_field",
            "source_skill": "datawarehouse-processor",
            "metadata_json": json.dumps({"owner": "bi-team"}, ensure_ascii=False),
        }
    )
    upsert_payload = json.loads(upsert_raw)
    assert upsert_payload["ok"] is True
    assert upsert_payload["item"]["metadata"]["source_skill"] == "datawarehouse-processor"
    assert upsert_payload["item"]["metadata"]["extra"]["owner"] == "bi-team"

    search_raw = memory_tool_module.memory_search.invoke(
        {
            "namespace": "skill:datawarehouse-processor",
            "query": "what is partition field",
            "limit": 3,
        }
    )
    search_payload = json.loads(search_raw)
    assert search_payload["ok"] is True
    assert len(search_payload["hits"]) >= 1

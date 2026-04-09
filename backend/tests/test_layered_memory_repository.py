"""Tests for ChromaDB layered memory repository."""

import pytest

from deerflow.memory.models import RawMemoryKind
from deerflow.memory.repository import LayeredMemoryRepository


class TestLayeredMemoryRepository:
    """Validate raw/distilled/core memory persistence behavior."""

    def test_create_raw_session_memory(self, tmp_path):
        repository = LayeredMemoryRepository(
            persist_directory=tmp_path / "chroma",
            default_user="test-user",
            default_source_agent="test-agent",
        )

        record = repository.create_raw_memory(
            content="user asked to run daily report",
            raw_kind=RawMemoryKind.SESSION,
            source_thread_id="thread_001",
            tags=["session", "report"],
        )

        assert record.raw_kind == RawMemoryKind.SESSION
        assert record.source_thread_id == "thread_001"
        assert record.source_agent == "test-agent"
        assert record.user == "test-user"
        assert record.created_at
        assert record.updated_at

        loaded = repository.get_raw_memory(record.id)
        assert loaded is not None
        assert loaded.source_thread_id == "thread_001"
        assert loaded.tags == ["session", "report"]

    @pytest.mark.parametrize(
        ("raw_kind", "source_field", "source_value"),
        [
            (RawMemoryKind.FILE, "source_file_path", "/tmp/uploads/contract.pdf"),
            (RawMemoryKind.IMAGE, "source_image_path", "/tmp/uploads/screenshot.png"),
            (RawMemoryKind.WEBPAGE, "source_web_url", "https://example.com/doc"),
        ],
    )
    def test_create_raw_with_source_links(self, tmp_path, raw_kind, source_field, source_value):
        repository = LayeredMemoryRepository(persist_directory=tmp_path / "chroma")

        create_kwargs = {
            "content": f"raw content for {raw_kind.value}",
            "raw_kind": raw_kind,
            source_field: source_value,
        }
        record = repository.create_raw_memory(**create_kwargs)

        loaded = repository.get_raw_memory(record.id)
        assert loaded is not None
        assert getattr(loaded, source_field) == source_value

    def test_create_distilled_with_raw_links(self, tmp_path):
        repository = LayeredMemoryRepository(persist_directory=tmp_path / "chroma")
        raw_a = repository.create_raw_memory(
            content="first raw note",
            raw_kind=RawMemoryKind.SESSION,
            source_thread_id="thread_a",
        )
        raw_b = repository.create_raw_memory(
            content="second raw note",
            raw_kind=RawMemoryKind.WEBPAGE,
            source_web_url="https://example.com",
        )

        distilled = repository.create_distilled_memory(
            content="distilled key points",
            raw_memory_ids=[raw_a.id, raw_b.id],
            tags=["distilled"],
            source_agent="planner-agent",
        )
        loaded = repository.get_distilled_memory(distilled.id)

        assert loaded is not None
        assert loaded.raw_memory_ids == [raw_a.id, raw_b.id]
        assert loaded.source_agent == "planner-agent"
        assert loaded.tags == ["distilled"]

    def test_create_core_with_distilled_links(self, tmp_path):
        repository = LayeredMemoryRepository(persist_directory=tmp_path / "chroma")
        raw = repository.create_raw_memory(
            content="raw memory for core test",
            raw_kind=RawMemoryKind.FILE,
            source_file_path="/tmp/uploads/source.sql",
        )
        distilled = repository.create_distilled_memory(
            content="distilled for core test",
            raw_memory_ids=[raw.id],
        )

        core = repository.create_core_memory(
            content="core memory summary",
            distilled_memory_ids=[distilled.id],
            tags=["core"],
            user="alice",
            source_agent="memory-agent",
        )
        loaded = repository.get_core_memory(core.id)

        assert loaded is not None
        assert loaded.distilled_memory_ids == [distilled.id]
        assert loaded.user == "alice"
        assert loaded.source_agent == "memory-agent"
        assert loaded.tags == ["core"]

    def test_create_raw_requires_matching_source_link(self, tmp_path):
        repository = LayeredMemoryRepository(persist_directory=tmp_path / "chroma")

        with pytest.raises(ValueError):
            repository.create_raw_memory(
                content="session raw without thread id",
                raw_kind=RawMemoryKind.SESSION,
            )

    def test_create_distilled_requires_raw_links(self, tmp_path):
        repository = LayeredMemoryRepository(persist_directory=tmp_path / "chroma")

        with pytest.raises(ValueError):
            repository.create_distilled_memory(
                content="distilled without links",
                raw_memory_ids=[],
            )

    def test_create_core_requires_distilled_links(self, tmp_path):
        repository = LayeredMemoryRepository(persist_directory=tmp_path / "chroma")

        with pytest.raises(ValueError):
            repository.create_core_memory(
                content="core without links",
                distilled_memory_ids=[],
            )

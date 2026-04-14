"""Tests for ChromaDB structured memory repository."""

import pytest
from pydantic import ValidationError

from deerflow.memory.repository import StructuredMemoryRepository


class TestStructuredMemoryRepository:
    """Validate raw/distilled/core memory persistence behavior."""

    def test_create_raw_session_memory(self, tmp_path):
        repository = StructuredMemoryRepository(
            persist_directory=tmp_path / "chroma",
            default_user="test-user",
            default_source_agent="test-agent",
        )

        record = repository.create_raw_memory(
            title="Daily report request",
            content="user asked to run daily report",
            source_thread_id="thread_001",
            tags=["session", "report"],
        )

        assert record.source_thread_id == "thread_001"
        assert record.title == "Daily report request"
        assert record.source_agent == "test-agent"
        assert record.user == "test-user"
        assert record.created_at
        assert record.updated_at
        assert record.attachment_file_paths == []
        assert record.attachment_image_paths == []
        assert record.inline_web_urls == []

        loaded = repository.get_raw_memory(record.id)
        assert loaded is not None
        assert loaded.source_thread_id == "thread_001"
        assert loaded.title == "Daily report request"
        assert loaded.tags == ["session", "report"]

    def test_create_raw_with_dialogue_attachments_and_links(self, tmp_path):
        repository = StructuredMemoryRepository(persist_directory=tmp_path / "chroma")

        record = repository.create_raw_memory(
            title="Contract and screenshot context",
            content="raw content summarizing uploads and link from the same thread",
            source_thread_id="thread_002",
            attachment_file_paths=["/tmp/uploads/contract.pdf"],
            attachment_image_paths=["/tmp/uploads/screenshot.png"],
            inline_web_urls=["https://example.com/doc"],
        )

        loaded = repository.get_raw_memory(record.id)
        assert loaded is not None
        assert loaded.attachment_file_paths == ["/tmp/uploads/contract.pdf"]
        assert loaded.attachment_image_paths == ["/tmp/uploads/screenshot.png"]
        assert loaded.inline_web_urls == ["https://example.com/doc"]

    def test_create_distilled_with_raw_links(self, tmp_path):
        repository = StructuredMemoryRepository(persist_directory=tmp_path / "chroma")
        raw_a = repository.create_raw_memory(
            title="First note",
            content="first raw note",
            source_thread_id="thread_a",
        )
        raw_b = repository.create_raw_memory(
            title="Second note",
            content="second raw note",
            source_thread_id="thread_a",
            inline_web_urls=["https://example.com"],
        )

        distilled = repository.create_distilled_memory(
            title="Merged key points",
            content="distilled key points",
            raw_memory_ids=[raw_a.id, raw_b.id],
            tags=["distilled"],
            source_agent="planner-agent",
        )
        loaded = repository.get_distilled_memory(distilled.id)

        assert loaded is not None
        assert loaded.title == "Merged key points"
        assert loaded.raw_memory_ids == [raw_a.id, raw_b.id]
        assert loaded.source_agent == "planner-agent"
        assert loaded.tags == ["distilled"]

    def test_create_core_with_distilled_links(self, tmp_path):
        repository = StructuredMemoryRepository(persist_directory=tmp_path / "chroma")
        raw = repository.create_raw_memory(
            title="SQL context",
            content="raw memory for core test",
            source_thread_id="thread_core",
            attachment_file_paths=["/tmp/uploads/source.sql"],
        )
        distilled_a = repository.create_distilled_memory(
            title="Distilled A",
            content="distilled slice A",
            raw_memory_ids=[raw.id],
        )
        distilled_b = repository.create_distilled_memory(
            title="Distilled B",
            content="distilled slice B",
            raw_memory_ids=[raw.id],
        )

        core = repository.create_core_memory(
            title="Core synthesis",
            content="core memory summary",
            distilled_memory_ids=[distilled_a.id, distilled_b.id],
            tags=["core"],
            user="alice",
            source_agent="memory-agent",
        )
        loaded = repository.get_core_memory(core.id)

        assert loaded is not None
        assert loaded.title == "Core synthesis"
        assert loaded.distilled_memory_ids == [distilled_a.id, distilled_b.id]
        assert loaded.user == "alice"
        assert loaded.source_agent == "memory-agent"
        assert loaded.tags == ["core"]

    def test_create_raw_requires_thread_id(self, tmp_path):
        repository = StructuredMemoryRepository(persist_directory=tmp_path / "chroma")

        with pytest.raises(ValidationError):
            repository.create_raw_memory(
                title="No thread",
                content="session raw without thread id",
                source_thread_id="",
            )

    def test_create_distilled_requires_raw_links(self, tmp_path):
        repository = StructuredMemoryRepository(persist_directory=tmp_path / "chroma")

        with pytest.raises(ValidationError):
            repository.create_distilled_memory(
                title="Bad distilled",
                content="distilled without links",
                raw_memory_ids=[],
            )

    def test_create_core_with_single_distilled_link(self, tmp_path):
        repository = StructuredMemoryRepository(persist_directory=tmp_path / "chroma")
        raw = repository.create_raw_memory(
            title="R",
            content="r",
            source_thread_id="t",
        )
        distilled = repository.create_distilled_memory(
            title="D",
            content="d",
            raw_memory_ids=[raw.id],
        )
        core = repository.create_core_memory(
            title="Single-source core",
            content="core from one distilled",
            distilled_memory_ids=[distilled.id],
        )
        loaded = repository.get_core_memory(core.id)
        assert loaded is not None
        assert loaded.distilled_memory_ids == [distilled.id]

    def test_create_core_requires_distilled_links_non_empty(self, tmp_path):
        repository = StructuredMemoryRepository(persist_directory=tmp_path / "chroma")

        with pytest.raises(ValidationError):
            repository.create_core_memory(
                title="C",
                content="core without links",
                distilled_memory_ids=[],
            )

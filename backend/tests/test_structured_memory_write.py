"""Tests for structured memory write service, config, and tool wiring."""

from __future__ import annotations

import importlib
from types import SimpleNamespace

import pytest

from deerflow.config.structured_memory_config import (
    StructuredMemoryConfig,
    StructuredMemoryWriteConfig,
    get_structured_memory_config,
    load_structured_memory_config_from_dict,
    set_structured_memory_config,
)
from deerflow.config.extensions_config import ExtensionsConfig
from deerflow.memory.repository import (
    StructuredMemoryRepository,
    reset_structured_memory_repository_singleton,
)
from deerflow.memory.structured_memory_write_service import (
    StructuredMemoryWriteError,
    StructuredMemoryWriteService,
)
from deerflow.tools.tools import get_available_tools

sm_write_tool_module = importlib.import_module("deerflow.tools.builtins.structured_memory_write_tool")


@pytest.fixture(autouse=True)
def _restore_sm_state():
    previous = get_structured_memory_config().model_copy()
    yield
    set_structured_memory_config(previous)
    reset_structured_memory_repository_singleton()


def test_structured_memory_write_config_nested_defaults() -> None:
    cfg = StructuredMemoryConfig()
    assert cfg.write.max_content_length == 100_000


def test_load_structured_memory_write_max_from_dict() -> None:
    load_structured_memory_config_from_dict(
        {"enabled": True, "store": "chroma", "write": {"max_content_length": 2048}},
    )
    assert get_structured_memory_config().write.max_content_length == 2048


def test_write_service_raw_requires_source_thread_id(tmp_path) -> None:
    repo = StructuredMemoryRepository(persist_directory=tmp_path / "c")
    svc = StructuredMemoryWriteService(repository=repo)
    result = svc.normalize_and_write(
        tier="raw",
        title="  Hello  ",
        content=" body ",
        source_thread_id="tid-99",
        tags=["a", "a", " b "],
    )
    assert result.tier == "raw"
    assert result.memory_id.startswith("raw_")
    loaded = repo.get_raw_memory(result.memory_id)
    assert loaded is not None
    assert loaded.source_thread_id == "tid-99"
    assert loaded.title == "Hello"
    assert loaded.content == "body"
    assert loaded.tags == ["a", "b"]


def test_write_service_raw_explicit_source_thread(tmp_path) -> None:
    repo = StructuredMemoryRepository(persist_directory=tmp_path / "c")
    svc = StructuredMemoryWriteService(repository=repo)
    result = svc.normalize_and_write(
        tier="raw",
        title="t",
        content="c",
        source_thread_id=" explicit ",
    )
    loaded = repo.get_raw_memory(result.memory_id)
    assert loaded is not None
    assert loaded.source_thread_id == "explicit"


def test_write_service_raw_requires_thread_when_missing(tmp_path) -> None:
    repo = StructuredMemoryRepository(persist_directory=tmp_path / "c")
    svc = StructuredMemoryWriteService(repository=repo)
    with pytest.raises(StructuredMemoryWriteError, match="source_thread_id"):
        svc.normalize_and_write(tier="raw", title="t", content="c", source_thread_id=None)


def test_write_service_content_length_limit(tmp_path, monkeypatch) -> None:
    repo = StructuredMemoryRepository(persist_directory=tmp_path / "c")
    svc = StructuredMemoryWriteService(repository=repo)
    set_structured_memory_config(
        StructuredMemoryConfig(enabled=True, store="chroma", write=StructuredMemoryWriteConfig(max_content_length=256)),
    )
    long_body = "x" * 260
    with pytest.raises(StructuredMemoryWriteError, match="max_content_length"):
        svc.normalize_and_write(tier="raw", title="t", content=long_body, source_thread_id="x")


def test_write_service_distilled_and_core(tmp_path) -> None:
    repo = StructuredMemoryRepository(persist_directory=tmp_path / "c")
    svc = StructuredMemoryWriteService(repository=repo)
    raw = repo.create_raw_memory(title="r", content="rc", source_thread_id="th")
    d = svc.normalize_and_write(
        tier="distilled",
        title="d",
        content="dc",
        raw_memory_ids=[raw.id],
    )
    assert d.tier == "distilled"
    core = svc.normalize_and_write(
        tier="core",
        title="k",
        content="kc",
        distilled_memory_ids=[d.memory_id],
    )
    assert core.tier == "core"


def test_write_service_rejects_missing_raw_refs(tmp_path) -> None:
    repo = StructuredMemoryRepository(persist_directory=tmp_path / "c")
    svc = StructuredMemoryWriteService(repository=repo)
    with pytest.raises(StructuredMemoryWriteError, match="unknown raw"):
        svc.normalize_and_write(tier="distilled", title="d", content="x", raw_memory_ids=["raw_nope"])


def test_write_service_rejects_missing_distilled_refs(tmp_path) -> None:
    repo = StructuredMemoryRepository(persist_directory=tmp_path / "c")
    svc = StructuredMemoryWriteService(repository=repo)
    with pytest.raises(StructuredMemoryWriteError, match="unknown distilled"):
        svc.normalize_and_write(tier="core", title="k", content="x", distilled_memory_ids=["distilled_nope"])


def test_get_available_tools_structured_memory_write_toggle(monkeypatch) -> None:
    fake_config = SimpleNamespace(
        tools=[],
        models=[],
        tool_search=SimpleNamespace(enabled=False),
        skill_evolution=SimpleNamespace(enabled=False),
        get_model_config=lambda name: None,
    )
    monkeypatch.setattr("deerflow.tools.tools.get_app_config", lambda: fake_config)
    monkeypatch.setattr(
        "deerflow.config.extensions_config.ExtensionsConfig.from_file",
        classmethod(lambda cls: ExtensionsConfig(mcp_servers={}, skills={})),
    )

    load_structured_memory_config_from_dict({"enabled": True, "store": "chroma"})
    names_on = [t.name for t in get_available_tools(include_mcp=False, subagent_enabled=False)]
    assert "structured_memory_write" in names_on

    load_structured_memory_config_from_dict({"enabled": False, "store": "chroma"})
    names_off = [t.name for t in get_available_tools(include_mcp=False, subagent_enabled=False)]
    assert "structured_memory_write" not in names_off


def test_structured_memory_write_tool_raw_success(tmp_path, monkeypatch) -> None:
    repo = StructuredMemoryRepository(persist_directory=tmp_path / "c")
    monkeypatch.setattr(
        "deerflow.memory.repository.get_structured_memory_repository",
        lambda: repo,
    )
    load_structured_memory_config_from_dict({"enabled": True, "store": "chroma"})

    runtime = SimpleNamespace(context={"thread_id": "tool-thread"}, config={})
    out = sm_write_tool_module.structured_memory_write_tool.func(
        runtime=runtime,
        tier="raw",
        title="My fact",
        content="Fact body",
        tags=None,
        source_thread_id=None,
        attachment_file_paths=None,
        attachment_image_paths=None,
        inline_web_urls=None,
        raw_memory_ids=None,
        distilled_memory_ids=None,
        source_agent=None,
        user=None,
    )
    assert "Structured memory written" in out
    assert "tier=raw" in out


def test_structured_memory_write_tool_raw_rejects_empty_source_thread() -> None:
    runtime = SimpleNamespace(context={}, config={})
    out = sm_write_tool_module.structured_memory_write_tool.func(
        runtime=runtime,
        tier="raw",
        title="t",
        content="c",
        tags=None,
        source_thread_id=None,
        attachment_file_paths=None,
        attachment_image_paths=None,
        inline_web_urls=None,
        raw_memory_ids=None,
        distilled_memory_ids=None,
        source_agent=None,
        user=None,
    )
    assert "source_thread_id" in out
    assert "failed" in out

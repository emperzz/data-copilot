"""Tests for structured memory update/delete service and tools."""

from __future__ import annotations

import importlib
from types import SimpleNamespace

import pytest

from deerflow.config.structured_memory_config import (
    get_structured_memory_config,
    load_structured_memory_config_from_dict,
    set_structured_memory_config,
)
from deerflow.memory.repository import StructuredMemoryRepository, reset_structured_memory_repository_singleton
from deerflow.memory.structured_memory_mutation_service import (
    StructuredMemoryMutationError,
    StructuredMemoryMutationService,
)
from deerflow.tools.tools import get_available_tools

sm_update_tool_module = importlib.import_module("deerflow.tools.builtins.structured_memory_update_tool")
sm_delete_tool_module = importlib.import_module("deerflow.tools.builtins.structured_memory_delete_tool")


@pytest.fixture(autouse=True)
def _restore_sm_state():
    previous = get_structured_memory_config().model_copy()
    yield
    set_structured_memory_config(previous)
    reset_structured_memory_repository_singleton()


@pytest.fixture()
def repo(tmp_path) -> StructuredMemoryRepository:
    return StructuredMemoryRepository(persist_directory=tmp_path / "chroma")


@pytest.fixture()
def seeded_repo(repo: StructuredMemoryRepository) -> tuple[str, str, str]:
    raw = repo.create_raw_memory(
        title="raw title",
        content="raw content",
        source_thread_id="th-1",
        tags=["raw-tag"],
    )
    distilled = repo.create_distilled_memory(
        title="distilled title",
        content="distilled content",
        raw_memory_ids=[raw.id],
        tags=["distilled-tag"],
    )
    core = repo.create_core_memory(
        title="core title",
        content="core content",
        distilled_memory_ids=[distilled.id],
        tags=["core-tag"],
    )
    return raw.id, distilled.id, core.id


def test_mutation_service_update_raw(repo: StructuredMemoryRepository) -> None:
    raw = repo.create_raw_memory(title="old", content="old body", source_thread_id="th-9", tags=["a"])
    svc = StructuredMemoryMutationService(repository=repo)
    result = svc.update_memory(
        memory_id=raw.id,
        title="new title",
        content="new body",
        tags=["a", " b ", "a"],
    )
    assert result.action == "update"
    assert result.tier == "raw"
    loaded = repo.get_raw_memory(raw.id)
    assert loaded is not None
    assert loaded.title == "new title"
    assert loaded.content == "new body"
    assert loaded.tags == ["a", "b"]


def test_mutation_service_update_not_found(repo: StructuredMemoryRepository) -> None:
    svc = StructuredMemoryMutationService(repository=repo)
    with pytest.raises(StructuredMemoryMutationError, match="No structured memory"):
        svc.update_memory(memory_id="raw_not_exist", title="x", content="y")


def test_mutation_service_delete_core_then_distilled_then_raw(
    repo: StructuredMemoryRepository,
    seeded_repo: tuple[str, str, str],
) -> None:
    raw_id, distilled_id, core_id = seeded_repo
    svc = StructuredMemoryMutationService(repository=repo)

    svc.delete_memory(memory_id=core_id)
    assert repo.get_core_memory(core_id) is None

    svc.delete_memory(memory_id=distilled_id)
    assert repo.get_distilled_memory(distilled_id) is None

    svc.delete_memory(memory_id=raw_id)
    assert repo.get_raw_memory(raw_id) is None


def test_mutation_service_delete_blocks_raw_with_downstream_reference(
    repo: StructuredMemoryRepository,
    seeded_repo: tuple[str, str, str],
) -> None:
    raw_id, _distilled_id, _core_id = seeded_repo
    svc = StructuredMemoryMutationService(repository=repo)
    with pytest.raises(StructuredMemoryMutationError, match="referenced by distilled"):
        svc.delete_memory(memory_id=raw_id)


def test_mutation_service_delete_blocks_distilled_with_downstream_reference(
    repo: StructuredMemoryRepository,
    seeded_repo: tuple[str, str, str],
) -> None:
    _raw_id, distilled_id, _core_id = seeded_repo
    svc = StructuredMemoryMutationService(repository=repo)
    with pytest.raises(StructuredMemoryMutationError, match="referenced by core"):
        svc.delete_memory(memory_id=distilled_id)


def test_mutation_update_tool_success(repo: StructuredMemoryRepository, monkeypatch) -> None:
    raw = repo.create_raw_memory(title="old", content="old", source_thread_id="th-2")
    monkeypatch.setattr(
        "deerflow.memory.structured_memory_mutation_service.get_structured_memory_repository",
        lambda: repo,
    )
    load_structured_memory_config_from_dict({"enabled": True, "store": "chroma"})
    out = sm_update_tool_module.structured_memory_update_tool.func(
        memory_id=raw.id,
        title="new",
        content="new body",
        tags=["k1"],
        source_agent=None,
        user=None,
    )
    assert "Structured memory updated" in out
    assert raw.id in out


def test_mutation_delete_tool_success(repo: StructuredMemoryRepository, monkeypatch) -> None:
    raw = repo.create_raw_memory(title="only", content="one", source_thread_id="th-2")
    monkeypatch.setattr(
        "deerflow.memory.structured_memory_mutation_service.get_structured_memory_repository",
        lambda: repo,
    )
    load_structured_memory_config_from_dict({"enabled": True, "store": "chroma"})
    out = sm_delete_tool_module.structured_memory_delete_tool.func(memory_id=raw.id)
    assert "Structured memory deleted" in out
    assert repo.get_raw_memory(raw.id) is None


def test_mutation_delete_tool_returns_validation_message(
    repo: StructuredMemoryRepository,
    seeded_repo: tuple[str, str, str],
    monkeypatch,
) -> None:
    raw_id, _distilled_id, _core_id = seeded_repo
    monkeypatch.setattr(
        "deerflow.memory.structured_memory_mutation_service.get_structured_memory_repository",
        lambda: repo,
    )
    load_structured_memory_config_from_dict({"enabled": True, "store": "chroma"})
    out = sm_delete_tool_module.structured_memory_delete_tool.func(memory_id=raw_id)
    assert "failed" in out
    assert "referenced by distilled" in out


def test_get_available_tools_structured_memory_mutation_toggle(monkeypatch) -> None:
    from deerflow.config.extensions_config import ExtensionsConfig

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
    assert "structured_memory_update" in names_on
    assert "structured_memory_delete" in names_on

    load_structured_memory_config_from_dict({"enabled": False, "store": "chroma"})
    names_off = [t.name for t in get_available_tools(include_mcp=False, subagent_enabled=False)]
    assert "structured_memory_update" not in names_off
    assert "structured_memory_delete" not in names_off

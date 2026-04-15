"""Tests for structured memory search service, query config, and query tools."""

from __future__ import annotations

import importlib
from types import SimpleNamespace

import pytest

from deerflow.config.structured_memory_config import (
    StructuredMemoryConfig,
    StructuredMemoryQueryConfig,
    get_structured_memory_config,
    load_structured_memory_config_from_dict,
    set_structured_memory_config,
)
from deerflow.memory.repository import (
    StructuredMemoryRepository,
    reset_structured_memory_repository_singleton,
)
from deerflow.memory.structured_memory_search_service import (
    StructuredMemorySearchError,
    StructuredMemorySearchService,
)

sm_query_tool_module = importlib.import_module("deerflow.tools.builtins.structured_memory_query_tool")
sm_list_tags_tool_module = importlib.import_module("deerflow.tools.builtins.structured_memory_list_tags_tool")
sm_get_by_id_tool_module = importlib.import_module("deerflow.tools.builtins.structured_memory_get_by_id_tool")


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
def seeded_repo(repo: StructuredMemoryRepository) -> StructuredMemoryRepository:
    """Repository pre-loaded with a small graph: 2 raw → 1 distilled → 1 core."""
    repo.create_raw_memory(
        title="Table A schema",
        content="Table A has columns id, name, created_at. Primary key is id.",
        source_thread_id="th-1",
        tags=["dw-table-schema", "database"],
    )
    repo.create_raw_memory(
        title="ETL rule for pipeline X",
        content="Pipeline X runs daily at 03:00 UTC and inserts into table A.",
        source_thread_id="th-1",
        tags=["etl-rule"],
    )
    raw_ids = [
        r for r in (repo._raw_collection.get(include=[])["ids"]) if r
    ]
    d = repo.create_distilled_memory(
        title="Table A overview",
        content="Table A is the central fact table with daily ETL ingestion.",
        raw_memory_ids=raw_ids,
        tags=["dw-table-schema"],
    )
    repo.create_core_memory(
        title="Data warehouse core facts",
        content="The warehouse centres on Table A, refreshed daily.",
        distilled_memory_ids=[d.id],
        tags=["dw-table-schema", "core-knowledge"],
    )
    return repo


# ---------------------------------------------------------------------------
# Query config tests
# ---------------------------------------------------------------------------


def test_query_config_defaults() -> None:
    cfg = StructuredMemoryConfig()
    assert cfg.query.default_top_k == 5
    assert cfg.query.max_top_k == 20
    assert cfg.query.default_tiers == ["core", "distilled"]


def test_query_config_from_dict() -> None:
    load_structured_memory_config_from_dict({
        "enabled": True,
        "store": "chroma",
        "query": {"default_top_k": 3, "max_top_k": 10, "default_tiers": ["raw"]},
    })
    cfg = get_structured_memory_config()
    assert cfg.query.default_top_k == 3
    assert cfg.query.max_top_k == 10
    assert cfg.query.default_tiers == ["raw"]


# ---------------------------------------------------------------------------
# SearchService.search tests
# ---------------------------------------------------------------------------


def test_search_returns_results(seeded_repo: StructuredMemoryRepository) -> None:
    svc = StructuredMemorySearchService(repository=seeded_repo)
    results = svc.search(query_text="Table A schema", tier_filter=["raw", "distilled", "core"])
    assert len(results) > 0
    assert all(r.memory_id for r in results)


def test_search_respects_tier_filter(seeded_repo: StructuredMemoryRepository) -> None:
    svc = StructuredMemorySearchService(repository=seeded_repo)
    results = svc.search(query_text="Table A", tier_filter=["core"])
    assert all(r.tier == "core" for r in results)


def test_search_respects_tag_filter(seeded_repo: StructuredMemoryRepository) -> None:
    svc = StructuredMemorySearchService(repository=seeded_repo)
    results = svc.search(query_text="pipeline", tier_filter=["raw"], tags=["etl-rule"])
    assert len(results) >= 1
    assert all("etl-rule" in r.tags for r in results)


def test_search_rejects_empty_query(seeded_repo: StructuredMemoryRepository) -> None:
    svc = StructuredMemorySearchService(repository=seeded_repo)
    with pytest.raises(StructuredMemorySearchError, match="non-empty"):
        svc.search(query_text="   ")


def test_search_top_k_capped(seeded_repo: StructuredMemoryRepository) -> None:
    set_structured_memory_config(
        StructuredMemoryConfig(
            enabled=True,
            store="chroma",
            query=StructuredMemoryQueryConfig(max_top_k=1),
        ),
    )
    svc = StructuredMemorySearchService(repository=seeded_repo)
    results = svc.search(query_text="Table A", tier_filter=["raw", "distilled", "core"], top_k=100)
    assert len(results) <= 1


# ---------------------------------------------------------------------------
# SearchService.list_tags tests
# ---------------------------------------------------------------------------


def test_list_tags_discovers_all(seeded_repo: StructuredMemoryRepository) -> None:
    svc = StructuredMemorySearchService(repository=seeded_repo)
    summaries = svc.list_tags()
    tag_names = {s.tag for s in summaries}
    assert "dw-table-schema" in tag_names
    assert "etl-rule" in tag_names


def test_list_tags_respects_tier_filter(seeded_repo: StructuredMemoryRepository) -> None:
    svc = StructuredMemorySearchService(repository=seeded_repo)
    summaries = svc.list_tags(tier_filter=["core"])
    for s in summaries:
        assert "core" in s.tiers
        assert "raw" not in s.tiers


def test_list_tags_empty_repo(repo: StructuredMemoryRepository) -> None:
    svc = StructuredMemorySearchService(repository=repo)
    summaries = svc.list_tags()
    assert summaries == []


# ---------------------------------------------------------------------------
# SearchService.get_by_id tests
# ---------------------------------------------------------------------------


def test_get_by_id_returns_record(seeded_repo: StructuredMemoryRepository) -> None:
    svc = StructuredMemorySearchService(repository=seeded_repo)
    raw_ids = seeded_repo._raw_collection.get(include=[])["ids"]
    result = svc.get_by_id(raw_ids[0])
    assert result is not None
    assert result.tier == "raw"
    assert result.record["id"] == raw_ids[0]


def test_get_by_id_not_found(repo: StructuredMemoryRepository) -> None:
    svc = StructuredMemorySearchService(repository=repo)
    assert svc.get_by_id("nonexistent_id") is None


def test_get_by_id_with_upstream_core(seeded_repo: StructuredMemoryRepository) -> None:
    svc = StructuredMemorySearchService(repository=seeded_repo)
    core_ids = seeded_repo._core_collection.get(include=[])["ids"]
    result = svc.get_by_id(core_ids[0], include_upstream=True)
    assert result is not None
    assert result.tier == "core"
    assert len(result.upstream) >= 1
    upstream_tiers = {u["_tier"] for u in result.upstream}
    assert "distilled" in upstream_tiers


def test_get_by_id_with_upstream_distilled(seeded_repo: StructuredMemoryRepository) -> None:
    svc = StructuredMemorySearchService(repository=seeded_repo)
    distilled_ids = seeded_repo._distilled_collection.get(include=[])["ids"]
    result = svc.get_by_id(distilled_ids[0], include_upstream=True)
    assert result is not None
    assert result.tier == "distilled"
    assert len(result.upstream) >= 1
    assert all(u["_tier"] == "raw" for u in result.upstream)


def test_get_by_id_no_upstream_when_disabled(seeded_repo: StructuredMemoryRepository) -> None:
    svc = StructuredMemorySearchService(repository=seeded_repo)
    core_ids = seeded_repo._core_collection.get(include=[])["ids"]
    result = svc.get_by_id(core_ids[0], include_upstream=False)
    assert result is not None
    assert result.upstream == []


# ---------------------------------------------------------------------------
# Format helpers
# ---------------------------------------------------------------------------


def test_format_search_results_empty() -> None:
    assert "No structured memory" in StructuredMemorySearchService.format_search_results([])


def test_format_tag_list_empty() -> None:
    assert "No tags" in StructuredMemorySearchService.format_tag_list([])


# ---------------------------------------------------------------------------
# Tool-level smoke tests
# ---------------------------------------------------------------------------


def test_query_tool_returns_results(seeded_repo: StructuredMemoryRepository, monkeypatch) -> None:
    monkeypatch.setattr(
        "deerflow.memory.structured_memory_search_service.get_structured_memory_repository",
        lambda: seeded_repo,
    )
    load_structured_memory_config_from_dict({"enabled": True, "store": "chroma"})
    out = sm_query_tool_module.structured_memory_query_tool.func(
        query_text="Table A",
        tier_filter=["raw", "distilled", "core"],
        tags=None,
        top_k=None,
    )
    assert "result" in out.lower() or "Table A" in out


def test_list_tags_tool_returns_tags(seeded_repo: StructuredMemoryRepository, monkeypatch) -> None:
    monkeypatch.setattr(
        "deerflow.memory.structured_memory_search_service.get_structured_memory_repository",
        lambda: seeded_repo,
    )
    load_structured_memory_config_from_dict({"enabled": True, "store": "chroma"})
    out = sm_list_tags_tool_module.structured_memory_list_tags_tool.func(tier_filter=None)
    assert "dw-table-schema" in out


def test_get_by_id_tool_returns_record(seeded_repo: StructuredMemoryRepository, monkeypatch) -> None:
    monkeypatch.setattr(
        "deerflow.memory.structured_memory_search_service.get_structured_memory_repository",
        lambda: seeded_repo,
    )
    load_structured_memory_config_from_dict({"enabled": True, "store": "chroma"})
    raw_ids = seeded_repo._raw_collection.get(include=[])["ids"]
    out = sm_get_by_id_tool_module.structured_memory_get_by_id_tool.func(
        memory_id=raw_ids[0],
        include_upstream=False,
    )
    assert raw_ids[0] in out


def test_get_by_id_tool_not_found(repo: StructuredMemoryRepository, monkeypatch) -> None:
    monkeypatch.setattr(
        "deerflow.memory.structured_memory_search_service.get_structured_memory_repository",
        lambda: repo,
    )
    load_structured_memory_config_from_dict({"enabled": True, "store": "chroma"})
    out = sm_get_by_id_tool_module.structured_memory_get_by_id_tool.func(
        memory_id="nonexistent_123",
        include_upstream=False,
    )
    assert "No structured memory" in out


# ---------------------------------------------------------------------------
# tools.py registration
# ---------------------------------------------------------------------------


def test_get_available_tools_includes_query_tools(monkeypatch) -> None:
    from deerflow.config.extensions_config import ExtensionsConfig
    from deerflow.tools.tools import get_available_tools

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
    names = [t.name for t in get_available_tools(include_mcp=False, subagent_enabled=False)]
    assert "structured_memory_query" in names
    assert "structured_memory_list_tags" in names
    assert "structured_memory_get_by_id" in names


def test_get_available_tools_excludes_query_when_disabled(monkeypatch) -> None:
    from deerflow.config.extensions_config import ExtensionsConfig
    from deerflow.tools.tools import get_available_tools

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
    load_structured_memory_config_from_dict({"enabled": False, "store": "chroma"})
    names = [t.name for t in get_available_tools(include_mcp=False, subagent_enabled=False)]
    assert "structured_memory_query" not in names
    assert "structured_memory_list_tags" not in names
    assert "structured_memory_get_by_id" not in names

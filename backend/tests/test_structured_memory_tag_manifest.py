"""Tests for the structured memory tag manifest service and prompt injection."""

from __future__ import annotations

import pytest

from deerflow.config.structured_memory_config import (
    StructuredMemoryConfig,
    StructuredMemoryTagManifestConfig,
    get_structured_memory_config,
    load_structured_memory_config_from_dict,
    set_structured_memory_config,
)
from deerflow.memory.models import MemoryTier
from deerflow.memory.repository import (
    StructuredMemoryRepository,
    reset_structured_memory_repository_singleton,
)
from deerflow.memory.structured_memory_mutation_service import StructuredMemoryMutationService
from deerflow.memory.structured_memory_write_service import StructuredMemoryWriteService
from deerflow.memory.tag_manifest_service import (
    TagManifestService,
    reset_tag_manifest_service_singleton,
)


@pytest.fixture(autouse=True)
def _restore_sm_state():
    previous = get_structured_memory_config().model_copy()
    yield
    set_structured_memory_config(previous)
    reset_structured_memory_repository_singleton()
    reset_tag_manifest_service_singleton()


@pytest.fixture()
def repo(tmp_path) -> StructuredMemoryRepository:
    return StructuredMemoryRepository(persist_directory=tmp_path / "chroma")


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


def test_tag_manifest_config_defaults() -> None:
    cfg = StructuredMemoryConfig()
    assert cfg.tag_manifest.inject_in_prompt is True
    assert cfg.tag_manifest.cache_ttl_seconds == 60
    assert cfg.tag_manifest.max_tags_in_prompt == 80


def test_load_tag_manifest_config_from_dict() -> None:
    load_structured_memory_config_from_dict(
        {
            "enabled": True,
            "store": "chroma",
            "tag_manifest": {
                "inject_in_prompt": False,
                "cache_ttl_seconds": 0,
                "max_tags_in_prompt": 5,
            },
        },
    )
    cfg = get_structured_memory_config()
    assert cfg.tag_manifest.inject_in_prompt is False
    assert cfg.tag_manifest.cache_ttl_seconds == 0
    assert cfg.tag_manifest.max_tags_in_prompt == 5


# ---------------------------------------------------------------------------
# snapshot (rebuild from repository)
# ---------------------------------------------------------------------------


def test_snapshot_on_empty_repo_returns_empty(repo: StructuredMemoryRepository) -> None:
    service = TagManifestService(repository=repo)
    assert service.snapshot() == []


def test_snapshot_aggregates_tags_across_tiers(repo: StructuredMemoryRepository) -> None:
    r1 = repo.create_raw_memory(title="r1", content="c1", source_thread_id="t", tags=["alpha", "beta"])
    repo.create_raw_memory(title="r2", content="c2", source_thread_id="t", tags=["alpha"])
    repo.create_distilled_memory(title="d", content="dc", raw_memory_ids=[r1.id], tags=["alpha", "gamma"])
    service = TagManifestService(repository=repo)

    entries = service.snapshot()
    by_tag = {e.tag: e for e in entries}

    assert by_tag["alpha"].total == 3
    assert by_tag["alpha"].counts_by_tier == {"raw": 2, "distilled": 1}
    assert by_tag["beta"].counts_by_tier == {"raw": 1}
    assert by_tag["gamma"].counts_by_tier == {"distilled": 1}


def test_snapshot_respects_tier_filter(repo: StructuredMemoryRepository) -> None:
    r1 = repo.create_raw_memory(title="r", content="c", source_thread_id="t", tags=["alpha"])
    repo.create_distilled_memory(title="d", content="dc", raw_memory_ids=[r1.id], tags=["alpha"])
    service = TagManifestService(repository=repo)

    entries = service.snapshot(tier_filter=["raw"])
    assert len(entries) == 1
    assert entries[0].counts_by_tier == {"raw": 1}


def test_snapshot_sorted_by_total_desc_then_name(repo: StructuredMemoryRepository) -> None:
    repo.create_raw_memory(title="r1", content="c", source_thread_id="t", tags=["zulu"])
    repo.create_raw_memory(title="r2", content="c", source_thread_id="t", tags=["alpha"])
    repo.create_raw_memory(title="r3", content="c", source_thread_id="t", tags=["alpha"])
    service = TagManifestService(repository=repo)

    entries = service.snapshot()
    assert [e.tag for e in entries] == ["alpha", "zulu"]


# ---------------------------------------------------------------------------
# bump_counters / cache lifecycle
# ---------------------------------------------------------------------------


def test_bump_counters_persists_before_cache_load(repo: StructuredMemoryRepository) -> None:
    """With persistence, bumps must write through to Chroma even if cache is cold."""
    service = TagManifestService(repository=repo)
    service.bump_counters(tags=["new-tag"], tier=MemoryTier.RAW, delta=1)

    persisted = repo.get_tag_manifest("new-tag")
    assert persisted is not None
    assert persisted.counts_by_tier == {"raw": 1}

    entries = service.snapshot()
    assert [e.tag for e in entries] == ["new-tag"]
    assert entries[0].counts_by_tier == {"raw": 1}


def test_bump_counters_updates_loaded_cache(repo: StructuredMemoryRepository) -> None:
    repo.create_raw_memory(title="r", content="c", source_thread_id="t", tags=["alpha"])
    service = TagManifestService(repository=repo)
    service.snapshot()  # prime cache (bootstraps manifest from tier scan)

    service.bump_counters(tags=["alpha", "beta"], tier=MemoryTier.RAW, delta=2)

    entries = {e.tag: e for e in service.snapshot()}
    assert entries["alpha"].counts_by_tier == {"raw": 3}
    assert entries["beta"].counts_by_tier == {"raw": 2}


def test_bump_counters_removes_tag_when_count_hits_zero(repo: StructuredMemoryRepository) -> None:
    repo.create_raw_memory(title="r", content="c", source_thread_id="t", tags=["alpha"])
    service = TagManifestService(repository=repo)
    service.snapshot()

    service.bump_counters(tags=["alpha"], tier=MemoryTier.RAW, delta=-1)

    assert service.snapshot() == []
    assert repo.get_tag_manifest("alpha") is None


def test_bump_counters_ignores_invalid_tier(repo: StructuredMemoryRepository) -> None:
    repo.create_raw_memory(title="r", content="c", source_thread_id="t", tags=["alpha"])
    service = TagManifestService(repository=repo)
    service.snapshot()

    service.bump_counters(tags=["alpha"], tier="nonsense", delta=1)

    entries = {e.tag: e for e in service.snapshot()}
    assert entries["alpha"].counts_by_tier == {"raw": 1}


def test_invalidate_reloads_from_persistence(repo: StructuredMemoryRepository) -> None:
    """After invalidate, snapshot must reload from the persisted manifest."""
    service = TagManifestService(repository=repo)
    service.snapshot()  # primes empty manifest in cache

    service.bump_counters(tags=["alpha"], tier=MemoryTier.RAW, delta=1)

    # Build a second service on the same repo; it must see the persisted bump.
    other = TagManifestService(repository=repo)
    entries = other.snapshot()
    assert [e.tag for e in entries] == ["alpha"]
    assert entries[0].counts_by_tier == {"raw": 1}

    # Invalidate the original and confirm it also reloads persisted state.
    service.invalidate()
    entries_again = service.snapshot()
    assert entries_again == entries


def test_cache_ttl_zero_always_reloads_from_persistence(
    repo: StructuredMemoryRepository,
) -> None:
    set_structured_memory_config(
        StructuredMemoryConfig(
            enabled=True,
            store="chroma",
            tag_manifest=StructuredMemoryTagManifestConfig(cache_ttl_seconds=0),
        ),
    )
    service = TagManifestService(repository=repo)
    service.snapshot()

    service.bump_counters(tags=["alpha"], tier=MemoryTier.RAW, delta=1)
    entries = service.snapshot()  # must reload from persistence since ttl=0
    assert [e.tag for e in entries] == ["alpha"]


# ---------------------------------------------------------------------------
# Persistence + bootstrap + rebuild_from_tiers
# ---------------------------------------------------------------------------


def test_first_snapshot_bootstraps_persistence_from_tiers(
    repo: StructuredMemoryRepository,
) -> None:
    """On first run with existing tier data, manifest seeds itself from scan."""
    repo.create_raw_memory(title="r", content="c", source_thread_id="t", tags=["alpha", "beta"])
    repo.create_raw_memory(title="r2", content="c", source_thread_id="t", tags=["alpha"])

    assert repo.list_tag_manifest() == []  # no persisted manifest yet

    service = TagManifestService(repository=repo)
    service.snapshot()

    persisted = {rec.tag: rec for rec in repo.list_tag_manifest()}
    assert persisted["alpha"].counts_by_tier == {"raw": 2}
    assert persisted["beta"].counts_by_tier == {"raw": 1}


def test_bump_persists_across_service_instances(
    repo: StructuredMemoryRepository,
) -> None:
    first = TagManifestService(repository=repo)
    first.bump_counters(tags=["alpha"], tier=MemoryTier.CORE, delta=3)

    second = TagManifestService(repository=repo)
    entries = {e.tag: e for e in second.snapshot()}
    assert entries["alpha"].counts_by_tier == {"core": 3}


def test_rebuild_from_tiers_repairs_drift(repo: StructuredMemoryRepository) -> None:
    service = TagManifestService(repository=repo)

    # Poison the persisted manifest with wrong counts and a stale tag.
    repo.upsert_tag_manifest(tag="alpha", counts_by_tier={"raw": 99})
    repo.upsert_tag_manifest(tag="ghost", counts_by_tier={"core": 1})

    repo.create_raw_memory(title="r", content="c", source_thread_id="t", tags=["alpha"])

    service.rebuild_from_tiers()

    persisted = {rec.tag: rec for rec in repo.list_tag_manifest()}
    assert "ghost" not in persisted  # tag no longer present anywhere
    assert persisted["alpha"].counts_by_tier == {"raw": 1}
    assert [e.tag for e in service.snapshot()] == ["alpha"]


# ---------------------------------------------------------------------------
# Repository-level CRUD
# ---------------------------------------------------------------------------


def test_repo_upsert_tag_manifest_preserves_created_at(
    repo: StructuredMemoryRepository,
) -> None:
    first = repo.upsert_tag_manifest(tag="alpha", counts_by_tier={"raw": 1})
    second = repo.upsert_tag_manifest(tag="alpha", counts_by_tier={"raw": 2})

    assert first.created_at == second.created_at
    assert second.total == 2


def test_repo_upsert_strips_zero_and_negative_counts(
    repo: StructuredMemoryRepository,
) -> None:
    rec = repo.upsert_tag_manifest(
        tag="alpha",
        counts_by_tier={"raw": 3, "distilled": 0, "core": -5},
    )
    assert rec.counts_by_tier == {"raw": 3}


def test_repo_get_missing_tag_returns_none(repo: StructuredMemoryRepository) -> None:
    assert repo.get_tag_manifest("missing") is None


def test_repo_delete_tag_manifest(repo: StructuredMemoryRepository) -> None:
    repo.upsert_tag_manifest(tag="alpha", counts_by_tier={"raw": 1})
    assert repo.delete_tag_manifest("alpha") is True
    assert repo.get_tag_manifest("alpha") is None
    assert repo.delete_tag_manifest("alpha") is False  # second delete is a no-op


# ---------------------------------------------------------------------------
# snapshot_text (prompt rendering)
# ---------------------------------------------------------------------------


def test_snapshot_text_empty_returns_empty_string(repo: StructuredMemoryRepository) -> None:
    service = TagManifestService(repository=repo)
    assert service.snapshot_text() == ""


def test_snapshot_text_lists_tags_and_counts(repo: StructuredMemoryRepository) -> None:
    repo.create_raw_memory(title="r", content="c", source_thread_id="t", tags=["alpha", "beta"])
    service = TagManifestService(repository=repo)

    rendered = service.snapshot_text()
    assert "<structured_memory_tag_manifest>" in rendered
    assert "</structured_memory_tag_manifest>" in rendered
    assert "- alpha 1 [raw:1]" in rendered
    assert "- beta 1 [raw:1]" in rendered


def test_snapshot_text_respects_max_tags(repo: StructuredMemoryRepository) -> None:
    for i in range(5):
        repo.create_raw_memory(title=f"r{i}", content="c", source_thread_id="t", tags=[f"tag{i}"])
    service = TagManifestService(repository=repo)

    rendered = service.snapshot_text(max_tags=2)
    tag_lines = [line for line in rendered.splitlines() if line.startswith("- ")]
    assert len(tag_lines) == 2
    assert "2 of 5 shown" in rendered


# ---------------------------------------------------------------------------
# Integration with WriteService / MutationService (manifest is auto-bumped)
# ---------------------------------------------------------------------------


def _patch_manifest_singleton(monkeypatch, service: TagManifestService) -> None:
    """Override the manifest lookup at every call site so bumps hit our fixture.

    ``from deerflow.memory.tag_manifest_service import get_tag_manifest_service``
    binds the function into each consumer module at import time, so we need to
    replace the name in every module that uses it.
    """
    for module_path in (
        "deerflow.memory.tag_manifest_service",
        "deerflow.memory.structured_memory_write_service",
        "deerflow.memory.structured_memory_mutation_service",
    ):
        monkeypatch.setattr(f"{module_path}.get_tag_manifest_service", lambda svc=service: svc)


def test_write_service_bumps_manifest(repo: StructuredMemoryRepository, monkeypatch) -> None:
    service = TagManifestService(repository=repo)
    _patch_manifest_singleton(monkeypatch, service)
    service.snapshot()  # prime cache so bump takes effect

    write_service = StructuredMemoryWriteService(repository=repo)
    write_service.normalize_and_write(
        tier="raw",
        title="r",
        content="c",
        source_thread_id="t",
        tags=["alpha"],
    )

    entries = {e.tag: e for e in service.snapshot()}
    assert entries["alpha"].counts_by_tier == {"raw": 1}


def test_mutation_service_update_shifts_tag_delta(
    repo: StructuredMemoryRepository, monkeypatch
) -> None:
    raw = repo.create_raw_memory(title="r", content="c", source_thread_id="t", tags=["alpha"])

    service = TagManifestService(repository=repo)
    _patch_manifest_singleton(monkeypatch, service)
    service.snapshot()  # {alpha: {raw: 1}}

    mutation = StructuredMemoryMutationService(repository=repo)
    mutation.update_memory(memory_id=raw.id, title="r", content="c2", tags=["beta"])

    entries = {e.tag: e for e in service.snapshot()}
    assert "alpha" not in entries
    assert entries["beta"].counts_by_tier == {"raw": 1}


def test_mutation_service_delete_decrements(repo: StructuredMemoryRepository, monkeypatch) -> None:
    raw = repo.create_raw_memory(title="r", content="c", source_thread_id="t", tags=["alpha"])

    service = TagManifestService(repository=repo)
    _patch_manifest_singleton(monkeypatch, service)
    service.snapshot()

    mutation = StructuredMemoryMutationService(repository=repo)
    mutation.delete_memory(memory_id=raw.id)

    assert service.snapshot() == []


# ---------------------------------------------------------------------------
# Prompt integration
# ---------------------------------------------------------------------------


def test_prompt_section_includes_manifest_when_enabled(
    repo: StructuredMemoryRepository, monkeypatch
) -> None:
    from deerflow.agents.lead_agent import prompt as prompt_module

    load_structured_memory_config_from_dict({"enabled": True, "store": "chroma"})
    service = TagManifestService(repository=repo)
    _patch_manifest_singleton(monkeypatch, service)

    repo.create_raw_memory(title="r", content="c", source_thread_id="t", tags=["alpha"])

    rendered = prompt_module._build_structured_memory_section()
    assert "<structured_memory_system>" in rendered
    assert "<structured_memory_tag_manifest>" in rendered
    assert "- alpha 1 [raw:1]" in rendered


def test_prompt_section_omits_manifest_when_disabled_via_config(
    repo: StructuredMemoryRepository, monkeypatch
) -> None:
    from deerflow.agents.lead_agent import prompt as prompt_module

    load_structured_memory_config_from_dict(
        {
            "enabled": True,
            "store": "chroma",
            "tag_manifest": {"inject_in_prompt": False},
        },
    )
    service = TagManifestService(repository=repo)
    _patch_manifest_singleton(monkeypatch, service)
    repo.create_raw_memory(title="r", content="c", source_thread_id="t", tags=["alpha"])

    rendered = prompt_module._build_structured_memory_section()
    assert "<structured_memory_system>" in rendered
    assert "<structured_memory_tag_manifest>" not in rendered


def test_prompt_manifest_uses_cached_snapshot_in_event_loop_thread(monkeypatch) -> None:
    from deerflow.agents.lead_agent import prompt as prompt_module

    load_structured_memory_config_from_dict({"enabled": True, "store": "chroma"})

    class _FakeService:
        def __init__(self) -> None:
            self.snapshot_called = False
            self.snapshot_cached_called = False

        def snapshot_text(self) -> str:
            self.snapshot_called = True
            return "<structured_memory_tag_manifest>sync</structured_memory_tag_manifest>"

        def snapshot_text_cached(self) -> str:
            self.snapshot_cached_called = True
            return "<structured_memory_tag_manifest>cached</structured_memory_tag_manifest>"

    fake_service = _FakeService()
    monkeypatch.setattr(
        "deerflow.memory.tag_manifest_service.get_tag_manifest_service",
        lambda: fake_service,
    )
    monkeypatch.setattr(prompt_module, "_is_running_event_loop_thread", lambda: True)

    rendered = prompt_module._build_structured_memory_section()
    assert "cached" in rendered
    assert fake_service.snapshot_cached_called is True
    assert fake_service.snapshot_called is False


def test_prompt_section_empty_when_structured_memory_disabled() -> None:
    from deerflow.agents.lead_agent import prompt as prompt_module

    load_structured_memory_config_from_dict({"enabled": False, "store": "chroma"})
    assert prompt_module._build_structured_memory_section() == ""

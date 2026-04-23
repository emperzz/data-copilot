from __future__ import annotations

import json

import pytest
import yaml
from pydantic import ValidationError

from deerflow.config.app_config import get_app_config, reset_app_config
from deerflow.config.memory_config import get_memory_config, load_memory_config_from_dict
from deerflow.config.structured_memory_config import (
    StructuredMemoryConfig,
    StructuredMemoryDisabledError,
    get_structured_memory_config,
    load_structured_memory_config_from_dict,
    set_structured_memory_config,
)
from deerflow.memory.repository import (
    get_structured_memory_repository,
    reset_structured_memory_repository_singleton,
)
from deerflow.memory.tag_manifest_service import reset_tag_manifest_service_singleton


@pytest.fixture(autouse=True)
def _restore_structured_memory_config():
    previous = get_structured_memory_config().model_copy()
    yield
    set_structured_memory_config(previous)
    reset_structured_memory_repository_singleton()
    reset_tag_manifest_service_singleton()


def test_structured_memory_config_defaults() -> None:
    cfg = StructuredMemoryConfig()
    assert cfg.enabled is True
    assert cfg.store == "chroma"
    assert cfg.write.max_content_length == 100_000


def test_load_structured_memory_config_from_dict() -> None:
    load_structured_memory_config_from_dict({"enabled": False, "store": "chroma"})
    cfg = get_structured_memory_config()
    assert cfg.enabled is False
    assert cfg.store == "chroma"


def test_invalid_store_rejected() -> None:
    with pytest.raises(ValidationError):
        StructuredMemoryConfig(enabled=True, store="pgvector")  # type: ignore[arg-type]


def test_get_structured_memory_repository_raises_when_disabled() -> None:
    load_structured_memory_config_from_dict({"enabled": False, "store": "chroma"})
    reset_structured_memory_repository_singleton()
    with pytest.raises(StructuredMemoryDisabledError):
        get_structured_memory_repository()


def test_get_structured_memory_repository_returns_when_enabled(tmp_path) -> None:
    prev_memory = get_memory_config().model_copy()
    try:
        load_memory_config_from_dict({"storage_path": str(tmp_path / "memory.json")})
        load_structured_memory_config_from_dict({"enabled": True, "store": "chroma"})
        reset_structured_memory_repository_singleton()
        repo = get_structured_memory_repository()
        assert repo is not None
    finally:
        load_memory_config_from_dict(prev_memory.model_dump())
        reset_structured_memory_repository_singleton()


def test_app_config_yaml_updates_structured_memory_singleton(tmp_path, monkeypatch) -> None:
    extensions_path = tmp_path / "extensions_config.json"
    extensions_path.write_text(json.dumps({"mcpServers": {}, "skills": {}}), encoding="utf-8")
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"},
                "models": [
                    {
                        "name": "m",
                        "use": "langchain_openai:ChatOpenAI",
                        "model": "gpt-test",
                    }
                ],
                "structured_memory": {"enabled": False, "store": "chroma"},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("DEER_FLOW_CONFIG_PATH", str(config_path))
    monkeypatch.setenv("DEER_FLOW_EXTENSIONS_CONFIG_PATH", str(extensions_path))
    reset_app_config()
    try:
        app = get_app_config()
        assert app.structured_memory.enabled is False
        assert get_structured_memory_config().enabled is False
    finally:
        reset_app_config()
        reset_structured_memory_repository_singleton()

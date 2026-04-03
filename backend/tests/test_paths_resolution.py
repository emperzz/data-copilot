"""Tests for Paths.base_dir resolution (monorepo vs package cwd)."""

from pathlib import Path

import pytest

from deerflow.config.paths import Paths


def test_base_dir_monorepo_when_cwd_is_packages_harness(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    backend = tmp_path / "backend"
    harness = backend / "packages" / "harness"
    harness.mkdir(parents=True)
    (backend / "pyproject.toml").write_text("[project]\nname = 'backend'\n", encoding="utf-8")
    (harness / "pyproject.toml").write_text("[project]\nname = 'harness'\n", encoding="utf-8")

    monkeypatch.chdir(harness)

    assert Paths().base_dir == backend / ".deer-flow"


def test_base_dir_backend_named_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    backend = tmp_path / "backend"
    backend.mkdir()
    (backend / "pyproject.toml").write_text("[project]\nname = 'backend'\n", encoding="utf-8")

    monkeypatch.chdir(backend)

    assert Paths().base_dir == backend / ".deer-flow"

"""Contain ordinary temporary files and release aggressive-mode scratch promptly."""

from __future__ import annotations

import shutil
import tempfile
import warnings
from pathlib import Path

import pytest

_SCRATCH_PATH = pytest.StashKey[Path]()


@pytest.fixture(autouse=True)
def _contain_system_tempfiles(
    request: pytest.FixtureRequest,
    tmp_path: Path,
    tmp_path_factory: pytest.TempPathFactory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Give Python and child processes the same per-test temporary-file lifetime."""
    # Only register descendants owned by pytest, even if a test overrides tmp_path.
    base = tmp_path_factory.getbasetemp().resolve()
    resolved = tmp_path.resolve()
    if resolved != base and resolved.is_relative_to(base):
        request.node.stash[_SCRATCH_PATH] = tmp_path
    system_temp = tmp_path / "system-temp"
    system_temp.mkdir(exist_ok=True)
    # tempfile caches its default directory, so environment changes alone do not
    # contain allocations in the current process.
    monkeypatch.setattr(tempfile, "tempdir", str(system_temp))
    for name in ("TMPDIR", "TEMP", "TMP"):
        monkeypatch.setenv(name, str(system_temp))


@pytest.hookimpl(hookwrapper=True, tryfirst=True)
def pytest_runtest_teardown(item: pytest.Item):
    """Remove only this test's scratch after all fixture finalizers in none mode."""
    try:
        yield
    finally:
        path = item.stash.get(_SCRATCH_PATH, None)
        if (
            path is not None
            and item.config.getini("tmp_path_retention_policy") == "none"
        ):
            # Native pytest only removes passing cases mid-run under "failed";
            # "none" otherwise defers deletion to session-history cleanup.
            try:
                shutil.rmtree(path)
            except FileNotFoundError:
                pass
            except OSError as error:
                warnings.warn(
                    pytest.PytestWarning(
                        f"Could not remove test scratch {path}: {error}"
                    ),
                    stacklevel=1,
                )

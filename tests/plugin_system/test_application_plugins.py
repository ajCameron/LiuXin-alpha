"""Exercise application startup, shared priority policy and registry publication."""

from __future__ import annotations

import io
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event

import pytest

from LiuXin_alpha.file_formats import plugin_defaults, plugin_runtime
from LiuXin_alpha.file_formats.conversion.plugin import InputFormatPlugin
from LiuXin_alpha.plugin_system import application, configuration
from LiuXin_alpha.plugin_system.zipplugin import loader


@pytest.fixture
def registry(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    """Keep settings, providers and both process registries local to each test."""
    settings = {
        "plugins": {},
        "plugin_customization": {},
        "disabled_plugins": set(),
        "enabled_plugins": set(),
    }
    for module in (application, plugin_runtime, plugin_defaults, configuration):
        monkeypatch.setattr(module, "config", settings)
    monkeypatch.setattr(application, "plugin_dir", str(tmp_path))
    monkeypatch.setattr(plugin_defaults, "plugin_dir", str(tmp_path))
    monkeypatch.setattr(application, "_initialized_plugins", None)
    monkeypatch.setattr(application, "_initializing", False)
    previous = plugin_runtime._initialized_plugins
    plugin_runtime.reset_plugins()
    yield settings
    if previous is None:
        plugin_runtime.reset_plugins()
    else:
        plugin_runtime.bind_plugins(previous)


@pytest.mark.parametrize("module", ("application", "contracts", "builtins"))
def test_import_does_not_discover_or_initialize_plugins(tmp_path: Path, module: str):
    script = f"""
import importlib
import sys
from LiuXin_alpha.plugin_system.base import Plugin
def unexpected(self):
    raise AssertionError('import started a plugin')
Plugin.initialize = unexpected
importlib.import_module('LiuXin_alpha.plugin_system.{module}')
from LiuXin_alpha.plugin_system import application
from LiuXin_alpha.file_formats import plugin_runtime
assert application._initialized_plugins is None
assert plugin_runtime._initialized_plugins is None
assert 'LiuXin_alpha.plugin_system.builtins.registry' not in sys.modules
assert 'LiuXin_alpha.file_formats.plugin_defaults' not in sys.modules
assert not [m for m in sys.modules if m.startswith(('LiuXin_alpha.metadata',
    'LiuXin_alpha.catalog', 'LiuXin_alpha.library', 'LiuXin_alpha.surfaces'))]
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=30,
        env={
            **os.environ,
            "LIUXIN_BASE_DIR": str(tmp_path),
            "PYTHONPATH": str(Path(__file__).resolve().parents[2] / "src"),
        },
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize("host", (False, True))
def test_priority_and_ties_match_for_standalone_and_host(registry, monkeypatch, host):
    calls = []

    class Bundled(InputFormatPlugin):
        name = "Bundled"
        file_types = {"example"}
        priority = 1

        def initialize(self):
            calls.append(self.name)

    class External(Bundled):
        name = "External"

    class Preferred(Bundled):
        name = "Preferred"
        priority = 10

    registry["plugins"][External.name] = "external.zip"
    registry["plugin_customization"][Preferred.name] = "configured"
    monkeypatch.setattr(loader, "load", lambda path: External)
    for owner in (application, plugin_defaults):
        monkeypatch.setattr(owner, "builtin_plugins", lambda: [Bundled, Preferred])
    if host:
        application.initialize_plugins()
    assert plugin_runtime.plugin_for_input_format("example").name == "Preferred"
    assert (
        plugin_runtime.plugin_for_input_format("example").site_customization
        == "configured"
    )
    plugins = tuple(plugin_runtime.initialized_plugins())
    assert [plugin.name for plugin in plugins] == ["Preferred", "External", "Bundled"]
    assert calls == ["External", "Bundled", "Preferred"]
    if host:
        assert tuple(application.initialized_plugins()) == plugins
        assert calls == ["External", "Bundled", "Preferred"]


def test_conflicting_installed_name_does_not_delete_files_or_reinitialize(
    registry, monkeypatch, tmp_path
):
    archive = tmp_path / "Conflict.zip"
    archive.write_bytes(b"user archive")
    registry["plugins"]["Conflict"] = str(archive)
    calls = []

    class Conflict(InputFormatPlugin):
        name = "Conflict"

        def initialize(self):
            calls.append(self)

    monkeypatch.setattr(application, "builtin_plugins", lambda: [Conflict])
    monkeypatch.setattr(
        loader, "load", lambda path: pytest.fail("conflicting ZIP loaded")
    )
    assert application.find_plugin("Conflict") is calls[0]
    assert application.find_plugin("Conflict") is calls[0]
    assert len(calls) == 1
    assert archive.read_bytes() == b"user archive"
    assert registry["plugins"] == {"Conflict": str(archive)}


def test_failed_discovery_can_retry_first_startup(registry, monkeypatch):
    def unavailable():
        raise RuntimeError("discovery failed")

    monkeypatch.setattr(application, "builtin_plugins", unavailable)
    with pytest.raises(RuntimeError, match="discovery failed"):
        tuple(application.initialized_plugins())
    assert plugin_runtime._initialized_plugins is None
    assert application._initialized_plugins is None
    assert application._initializing is False
    monkeypatch.setattr(application, "builtin_plugins", lambda: [])
    assert tuple(application.initialized_plugins()) == ()
    assert plugin_runtime._initialized_plugins == []


@pytest.mark.parametrize("host", (False, True))
def test_failed_provider_restores_streams_before_next_provider(
    registry, monkeypatch, host
):
    stdout, stderr = sys.stdout, sys.stderr

    class Broken(InputFormatPlugin):
        name = "Broken"

        def initialize(self):
            sys.stdout, sys.stderr = io.StringIO(), io.StringIO()
            raise ValueError("failed provider")

    class Healthy(InputFormatPlugin):
        name = "Healthy"
        file_types = {"example"}

        def initialize(self):
            assert sys.stdout is stdout and sys.stderr is stderr

    for owner in (application, plugin_defaults):
        monkeypatch.setattr(owner, "builtin_plugins", lambda: [Broken, Healthy])
    if host:
        application.initialize_plugins()
    assert plugin_runtime.plugin_for_input_format("example").name == "Healthy"
    assert sys.stdout is stdout and sys.stderr is stderr


def test_interrupted_refresh_restores_previous_binding_and_streams(
    registry, monkeypatch
):
    class Working(InputFormatPlugin):
        name = "Working"
        file_types = {"example"}

    monkeypatch.setattr(application, "builtin_plugins", lambda: [Working])
    application.initialize_plugins()
    previous = application.find_plugin("Working")
    stdout, stderr = sys.stdout, sys.stderr

    class Interrupted(Working):
        def initialize(self):
            sys.stdout, sys.stderr = io.StringIO(), io.StringIO()
            raise KeyboardInterrupt

    monkeypatch.setattr(application, "builtin_plugins", lambda: [Interrupted])
    with pytest.raises(KeyboardInterrupt):
        application.initialize_plugins()
    assert sys.stdout is stdout and sys.stderr is stderr
    assert application.find_plugin("Working") is previous
    assert plugin_runtime.plugin_for_input_format("example") is previous


def test_concurrent_and_reentrant_lookups_do_not_start_duplicate_registries(
    registry, monkeypatch
):
    entered, release = Event(), Event()
    calls = []

    class Reentrant(InputFormatPlugin):
        name = "Reentrant"
        file_types = {"example"}

        def initialize(self):
            calls.append(self)
            assert tuple(application.initialized_plugins()) == ()
            assert tuple(plugin_runtime.initialized_plugins()) == ()
            entered.set()
            assert release.wait(10)

    monkeypatch.setattr(application, "builtin_plugins", lambda: [Reentrant])
    monkeypatch.setattr(
        plugin_defaults,
        "build_plugins",
        lambda: pytest.fail("duplicate standalone startup"),
    )
    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(lambda: tuple(application.initialized_plugins()))
        try:
            assert entered.wait(10)
            second = executor.submit(lambda: tuple(application.initialized_plugins()))
        finally:
            release.set()
        assert first.result(timeout=10) == second.result(timeout=10) == tuple(calls)
    assert len(calls) == 1

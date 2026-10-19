"""Exercise setting ownership, profile isolation and import-time resource boundaries."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from LiuXin_alpha.configuration import ConfigurationPaths, use_preferences
from LiuXin_alpha.preferences import Preferences, preferences


def _store(root: Path) -> Preferences:
    paths = ConfigurationPaths.resolve(system_root=root, environment={})
    return Preferences(paths=paths, upgrade_on_load=False)


def test_path_precedence_and_no_filesystem_writes(tmp_path):
    before = set(tmp_path.iterdir())
    env = {
        "LIUXIN_PREFS_DIR": str(tmp_path / "env-prefs"),
        "LIUXIN_CONFIG_DIR": str(tmp_path / "env-config"),
        "CALIBRE_CONFIG_DIRECTORY": str(tmp_path / "calibre"),
    }
    selected = ConfigurationPaths.resolve(
        system_root=tmp_path / "deployment", environment=env
    )
    assert selected.preferences == tmp_path / "env-prefs"
    assert selected.components == tmp_path / "env-config"
    explicit = ConfigurationPaths.resolve(
        preferences_dir=tmp_path / "explicit-prefs",
        config_dir=tmp_path / "explicit-config",
        environment=env,
    )
    assert explicit.preferences == tmp_path / "explicit-prefs"
    assert explicit.components == tmp_path / "explicit-config"
    fallback = ConfigurationPaths.resolve(
        environment={"CALIBRE_CONFIG_DIRECTORY": str(tmp_path / "calibre")}
    )
    assert fallback.components == tmp_path / "calibre"
    isolated = ConfigurationPaths.resolve(
        system_root=tmp_path / "deployment", environment={}
    )
    assert isolated.preferences == tmp_path / "deployment/LiuXin_prefs"
    assert isolated.components == isolated.preferences / "calibre_config"
    assert set(tmp_path.iterdir()) == before


@pytest.mark.parametrize(
    "first",
    [
        "LiuXin_alpha.constants",
        "LiuXin_alpha.utils.config.config_base",
        "LiuXin_alpha.core.settings",
        "LiuXin_alpha.utils.text.icu",
    ],
)
def test_imports_share_paths_without_loading_or_executing_settings(tmp_path, first):
    component = tmp_path / "config"
    component.mkdir()
    (component / "tweaks.py").write_text(
        "raise RuntimeError('settings executed during import')\n"
    )
    source = Path(__file__).resolve().parents[2] / "src"
    env = {
        **os.environ,
        "PYTHONPATH": str(source),
        "LIUXIN_CONFIG_DIR": str(component),
        "LIUXIN_PREFS_DIR": str(tmp_path / "prefs"),
    }
    script = f"""
import importlib
importlib.import_module({first!r})
from LiuXin_alpha import configuration, constants
from LiuXin_alpha.constants import paths
from LiuXin_alpha.preferences import preferences
from LiuXin_alpha.utils.config.config_base import prefs, tweaks
assert constants.config_dir == paths.config_dir
assert prefs is preferences and tweaks is preferences
assert configuration._default is None
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        env=env,
        text=True,
        capture_output=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    assert not (tmp_path / "prefs").exists()
    assert sorted(p.name for p in component.iterdir()) == ["tweaks.py"]


def test_one_store_resolves_conflicts_and_persists_through_every_view(tmp_path):
    from LiuXin_alpha.utils.config.config_base import prefs, tweaks

    paths = ConfigurationPaths.resolve(system_root=tmp_path, environment={})
    paths.components.mkdir(parents=True)
    tweak_file = paths.components / "tweaks.py"
    global_file = paths.components / "global.py.json"
    tweak_file.write_text(
        "network_timeout = 12\ntitle_series_sorting = 'strictly_alphabetic'\n"
    )
    global_file.write_text(json.dumps({"network_timeout": 25, "language": "fr"}))
    ini = paths.preferences / Preferences.config_file_name
    ini.write_text('[Misc]\nlanguage = str:"de"\n')
    store = Preferences(paths=paths, upgrade_on_load=False)
    original_components = (tweak_file.read_bytes(), global_file.read_bytes())
    with use_preferences(store):
        assert prefs["network_timeout"] == 25
        assert tweaks["language"] == "de"
        assert preferences["title_series_sorting"] == "strictly_alphabetic"
        for view in (preferences, prefs, tweaks):
            view["network_timeout"] += 1
            assert (
                preferences["network_timeout"]
                == prefs["network_timeout"]
                == tweaks["network_timeout"]
            )
        prefs["new_setting"] = "first"
        tweaks["new_setting"] = (
            "second"  # New keys retain their section on subsequent writes.
        )
    reloaded = Preferences(paths=paths, upgrade_on_load=False)
    assert reloaded["network_timeout"] == 28
    assert reloaded["new_setting"] == "second"
    assert original_components == (tweak_file.read_bytes(), global_file.read_bytes())


def test_component_objects_keep_their_profile_paths(tmp_path):
    from LiuXin_alpha.utils.config.config_base import Config
    from LiuXin_alpha.utils.config.config_tools import DynamicConfig, JSONConfig

    first, second = _store(tmp_path / "one"), _store(tmp_path / "two")
    with use_preferences(first):
        config = Config("plugin")
        config.add_opt("value", default=0)
        dynamic = DynamicConfig("dynamic")
        json_config = JSONConfig("reader")
    with use_preferences(second):
        config.set("value", 1)
        dynamic["value"] = 2
        json_config["value"] = 3
        other = JSONConfig("reader")
        assert other.get("value") is None
    assert Path(config.config_file_path).parent == first.paths.components
    assert Path(dynamic.file_path).parent == first.paths.components
    assert Path(json_config.file_path).parent == first.paths.components
    assert not second.paths.preferences.exists()


def test_concurrent_profiles_and_live_article_rules_do_not_leak(tmp_path):
    from LiuXin_alpha.metadata.title_names import title_to_title_sort
    from LiuXin_alpha.utils.config.config_base import tweaks

    first, second = _store(tmp_path / "one"), _store(tmp_path / "two")
    first["per_language_title_sort_articles"] = {"eng": [r"The\s+"]}
    second["per_language_title_sort_articles"] = {"eng": [r"A\s+"]}

    def read(store):
        with use_preferences(store):
            return [title_to_title_sort("The Book", lang="eng") for _ in range(20)]

    with ThreadPoolExecutor(max_workers=2) as executor:
        one, two = list(executor.map(read, (first, second)))
    assert one == ["Book, The"] * 20
    assert two == ["The Book"] * 20
    with use_preferences(first):
        tweaks["per_language_title_sort_articles"] = {"eng": [r"A\s+"]}
        assert title_to_title_sort("The Book", lang="eng") == "The Book"
        with pytest.raises(RuntimeError), use_preferences(second):
            raise RuntimeError("restore outer context")
        assert preferences["application_id"] == first["application_id"]


def test_profile_reaches_real_core_database_and_remote_preferences(
    tmp_path, monkeypatch
):
    from LiuXin_alpha.core import CoreHttpDaemon, RemoteCoreClient
    from LiuXin_alpha.core.commands import CoreCommand
    from LiuXin_alpha.core.queries import CoreQuery
    from LiuXin_alpha.metadata.title_names import title_to_title_sort
    from LiuXin_alpha.surfaces.core import open_surface_core_from_args

    sessions = []
    try:
        for name in ("first", "second"):
            root = tmp_path / name
            root.mkdir()
            manifest = root / "liuxin-system.json"
            manifest.write_text(
                json.dumps(
                    {
                        "format": "liuxin.system",
                        "version": 1,
                        "database": "catalogue.sqlite",
                        "preferences_directory": "settings",
                        "config_directory": "components",
                    }
                )
            )
            session = open_surface_core_from_args(
                argparse.Namespace(profile=str(manifest)),
                create=True,
                enable_storage_manager=False,
                enable_maintenance=False,
            )
            sessions.append(session)
            runtime = session.runtime
            assert runtime.services.preferences is runtime.database.preferences
            assert runtime.database.preferences.paths.preferences == root / "settings"
            runtime.database._initialise_runtime_collaborators(enable_maintenance=False)
            assert runtime.services.preferences is runtime.database.preferences
            runtime.register_query_handler(
                "test.sort", lambda _runtime, _query: title_to_title_sort("The Book")
            )
        first, second = [s.runtime for s in sessions]
        daemon = CoreHttpDaemon(first)
        daemon.start()
        try:
            remote = RemoteCoreClient(endpoint=daemon.base_url)
            result = remote.execute_command(
                CoreCommand(
                    "preferences.set",
                    {
                        "scope": "application",
                        "key": "title_series_sorting",
                        "value": "strictly_alphabetic",
                    },
                )
            )
            assert result.result["updated"] is True
            assert remote.execute_query(CoreQuery("test.sort")).result == "The Book"
            assert second.execute_query(CoreQuery("test.sort")).result == "Book, The"
            listed = remote.execute_query(
                CoreQuery("preferences.list", {"scope": "application"})
            ).result
            assert listed["values"]["title_series_sorting"] == "strictly_alphabetic"
        finally:
            daemon.stop()
    finally:
        for session in reversed(sessions):
            session.close()


@pytest.mark.parametrize("backend", ["serial", "process", "spawn"])
def test_managed_workers_keep_submitting_profile(tmp_path, monkeypatch, backend):
    import multiprocessing

    from LiuXin_alpha.utils.jobs import api
    from LiuXin_alpha.utils.jobs.manager import InMemoryJobManager

    if backend == "spawn":
        monkeypatch.setattr(
            api, "Process", multiprocessing.get_context("spawn").Process
        )
    manager = InMemoryJobManager(
        max_workers=1, default_backend="process" if backend == "spawn" else backend
    )
    request = api.JobRequest(
        module_name="def read():\n from LiuXin_alpha.preferences import preferences\n return preferences['network_timeout']\n",
        function_name="read",
        module_is_source_code=True,
    )
    first, second = _store(tmp_path / "one"), _store(tmp_path / "two")
    first["network_timeout"], second["network_timeout"] = 11, 22
    try:
        submitted = []
        for store in (first, second):
            with use_preferences(store):
                submitted.append(manager.submit(request, no_output=True, timeout=20))
        for job_id, expected in zip(submitted, (11, 22), strict=True):
            result = manager.wait(job_id, timeout=30).execution
            assert result.ok, result.traceback
            assert result.result == expected
    finally:
        manager.shutdown(wait=True)


def test_invalid_settings_fail_before_opening_catalogue(tmp_path):
    from LiuXin_alpha.core import create_core

    store = _store(tmp_path)
    store.paths.components.mkdir(parents=True)
    (store.paths.components / "global.py.json").write_text("[]")
    database = tmp_path / "must-not-open.sqlite"
    with pytest.raises(ValueError, match="JSON object"):
        create_core(
            database_path=database, create=True, configuration_paths=store.paths
        )
    assert not database.exists()


def test_temporary_tweak_does_not_persist_or_reset_unrelated_settings(tmp_path):
    from LiuXin_alpha.utils.config.config_base import (
        Tweak,
        reset_tweaks_to_default,
        tweaks,
    )

    store = _store(tmp_path)
    store["network_timeout"] = 99
    ini = Path(store.config_file_path)
    saved = ini.read_bytes()
    with use_preferences(store):
        with Tweak("author_sort_copy_method", "copy"):
            assert tweaks["author_sort_copy_method"] == "copy"
            assert ini.read_bytes() == saved
        assert tweaks["author_sort_copy_method"] == "comma"
        reset_tweaks_to_default()
        assert tweaks["network_timeout"] == 99


@pytest.mark.parametrize(
    "source",
    [
        "import os",
        "value = __import__('os')",
        "value = 1 + 2",
        "settings.value = 1",
        "__secret = 1",
    ],
)
def test_executable_tweak_inputs_are_rejected_before_side_effects(tmp_path, source):
    from LiuXin_alpha.configuration_defaults import parse_tweak_values

    with pytest.raises(ValueError, match="allowed"):
        parse_tweak_values(source, filename="example-tweaks.py")
    store = _store(tmp_path)
    store.paths.components.mkdir(parents=True)
    marker = tmp_path / "executed"
    (store.paths.components / "tweaks.py").write_text(
        f"marker = open({str(marker)!r}, 'w')"
    )
    with pytest.raises(ValueError, match="literal setting values"):
        Preferences(paths=store.paths, upgrade_on_load=False)
    assert not marker.exists()


def test_collation_uses_effective_profile_settings_without_import_side_effects(
    tmp_path, monkeypatch
):
    from copy import copy

    from LiuXin_alpha.utils.text import icu

    class RecordingCollator:
        def __init__(self, locale):
            self.locale = locale
            self.numeric = False

        def clone(self):
            return copy(self)

        def sort_key(self, value):
            return self.locale, self.numeric, value

    monkeypatch.setattr(icu._icu, "Collator", RecordingCollator)
    first, second = _store(tmp_path / "one"), _store(tmp_path / "two")
    first["locale_for_sorting"], second["locale_for_sorting"] = (
        "profile-one",
        "profile-two",
    )
    first["numeric_collation"], second["numeric_collation"] = False, True
    with use_preferences(first):
        assert icu.sort_key("2") == ("profile-one", False, "2")
        icu.change_locale("override-one")
        assert icu.sort_key("2") == ("override-one", False, "2")
    with use_preferences(second):
        assert icu.sort_key("2") == ("profile-two", True, "2")
        second["numeric_collation"] = False
        assert icu.sort_key("2") == ("profile-two", False, "2")
    with use_preferences(first):
        assert icu.sort_key("2") == ("override-one", False, "2")
        icu.change_locale()


def test_loaded_process_defaults_keep_component_paths_when_environment_changes(
    tmp_path, monkeypatch
):
    from LiuXin_alpha import configuration
    from LiuXin_alpha.utils.config.config_tools import JSONConfig

    selected = _store(tmp_path / "selected")
    monkeypatch.setattr(configuration, "_default", selected)
    monkeypatch.setenv("LIUXIN_CONFIG_DIR", str(tmp_path / "later-env"))
    assert configuration.configuration_paths() == selected.paths
    assert Path(JSONConfig("component").file_path).parent == selected.paths.components

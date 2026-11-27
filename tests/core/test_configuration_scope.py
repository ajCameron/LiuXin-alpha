"""Exercise configuration capture across Core lifetimes, dispatch and workers."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from LiuXin_alpha.configuration import (
    ConfigurationPaths,
    current_preferences,
    use_preferences,
)
from LiuXin_alpha.core.commands import CoreCommand
from LiuXin_alpha.core.queries import CoreQuery
from LiuXin_alpha.core.runtime import CoreRuntime
from LiuXin_alpha.core.services import CoreServices
from LiuXin_alpha.preferences import Preferences, preferences


def store_at(root: Path) -> Preferences:
    return Preferences(
        paths=ConfigurationPaths.resolve(system_root=root, environment={}),
        upgrade_on_load=False,
    )


@pytest.mark.parametrize(
    "module",
    [
        "LiuXin_alpha.utils.ptempfiles",
        "LiuXin_alpha.utils.paths",
        "LiuXin_alpha.configuration",
        "LiuXin_alpha.core.factory",
    ],
)
def test_fresh_imports_do_not_install_startup_hooks_or_create_paths(tmp_path, module):
    source = Path(__file__).resolve().parents[2] / "src"
    script = f"""
import builtins, importlib, pathlib, sys
before_paths = tuple(pathlib.Path({str(tmp_path)!r}).rglob("*"))
before_builtins = dict(vars(builtins))
importlib.import_module({module!r})
assert not any(type(h).__name__ == "PyQt4Ban" for h in sys.meta_path)
assert vars(builtins) == before_builtins
assert "LiuXin_alpha.startup_scripts" not in sys.modules
from LiuXin_alpha import configuration
assert configuration._default is None
assert tuple(pathlib.Path({str(tmp_path)!r}).rglob("*")) == before_paths
"""
    env = {**os.environ, "PYTHONPATH": str(source), "PYTHONDONTWRITEBYTECODE": "1"}
    for variable, directory in [
        ("LIUXIN_BASE_DIR", "base"),
        ("LIUXIN_PREFS_DIR", "prefs"),
        ("LIUXIN_CONFIG_DIR", "components"),
        ("LIUXIN_SCRATCH_DIR", "scratch"),
        ("LIUXIN_PLUGIN_CACHE_PATH", "plugins.json"),
    ]:
        env[variable] = str(tmp_path / directory)
    result = subprocess.run(
        [sys.executable, "-c", script],
        env=env,
        text=True,
        capture_output=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(
    "selection", ["omitted", "proxy", "database_proxy", "database", "explicit"]
)
def test_services_capture_store_before_first_use_and_restore_context(
    tmp_path, selection
):
    first, second = store_at(tmp_path / "first"), store_at(tmp_path / "second")
    database = SimpleNamespace()
    kwargs = {}
    if selection == "proxy":
        kwargs["preferences"] = preferences
    elif selection == "database_proxy":
        database.preferences = preferences
    elif selection == "database":
        database.preferences = first
    elif selection == "explicit":
        kwargs["preferences"] = first
    with use_preferences(first):
        services = CoreServices(library=SimpleNamespace(db=database), **kwargs)
    try:
        with use_preferences(second):
            assert services.preferences is first
            assert current_preferences() is second
    finally:
        services.close()


@pytest.mark.parametrize("fail", [False, True])
def test_nested_runtime_dispatch_restores_each_profile(tmp_path, fail):
    first, second = store_at(tmp_path / "first"), store_at(tmp_path / "second")
    one = CoreRuntime(library=SimpleNamespace(db=object()), preferences=first)
    two = CoreRuntime(library=SimpleNamespace(db=object()), preferences=second)
    seen = []

    def inner(runtime, query):
        seen.append(current_preferences())
        if fail:
            raise RuntimeError("inner failure")
        return {"value": "ok"}

    def outer(runtime, command):
        seen.append(current_preferences())
        try:
            two.execute_query(CoreQuery(name="test.profile"))
        except Exception:
            if not fail:
                raise
        seen.append(current_preferences())
        return {}

    one.register_command_handler("test.profile", outer)
    two.register_query_handler("test.profile", inner)
    try:
        with use_preferences({"outer": "sentinel"}):
            surrounding = current_preferences()
            one.execute_command(CoreCommand(name="test.profile"))
            assert current_preferences() is surrounding
        assert seen == [first, second, first]
    finally:
        one.shutdown()
        two.shutdown()


@pytest.mark.parametrize("backend", ["serial", "process"])
def test_retry_retains_original_profile_instead_of_retry_callers(tmp_path, backend):
    from LiuXin_alpha.utils.jobs.api import JobRequest
    from LiuXin_alpha.utils.jobs.manager import InMemoryJobManager

    first, second = store_at(tmp_path / "first"), store_at(tmp_path / "second")
    first["network_timeout"], second["network_timeout"] = 17, 93
    request = JobRequest(
        module_name="def read():\n from LiuXin_alpha.preferences import preferences\n return preferences['network_timeout']\n",
        function_name="read",
        module_is_source_code=True,
    )
    manager = InMemoryJobManager(max_workers=1, default_backend=backend)
    try:
        with use_preferences(first):
            original = manager.submit(request, no_output=True)
        assert manager.result(original, timeout=20).result == 17
        with use_preferences(second):
            replay = manager.retry(original, allow_succeeded=True)
            assert current_preferences() is second
        assert manager.result(replay, timeout=20).result == 17
    finally:
        manager.shutdown(wait=True, cancel_pending=True)


@pytest.mark.parametrize("fail", [False, True])
def test_lazy_service_construction_and_close_use_captured_profile(
    tmp_path, monkeypatch, fail
):
    first, second = store_at(tmp_path / "first"), store_at(tmp_path / "second")
    seen = []

    def construct(database):
        seen.append(current_preferences())
        if fail:
            raise RuntimeError("lazy failure")
        return object()

    def close():
        seen.append(current_preferences())

    monkeypatch.setattr("LiuXin_alpha.catalog.catalog.Catalog", construct)
    services = CoreServices(
        library=SimpleNamespace(db=object(), close=close),
        preferences=first,
        close_library_on_shutdown=True,
    )
    with use_preferences(second):
        if fail:
            with pytest.raises(RuntimeError, match="lazy failure"):
                _ = services.catalog
        else:
            _ = services.catalog
        assert current_preferences() is second
        services.close()
        assert current_preferences() is second
    assert seen == [first, first]


def test_empty_application_and_library_settings_keep_distinct_owners():
    application, library = {}, {}
    services = CoreServices(
        library=SimpleNamespace(db=object()),
        preferences=application,
        library_preferences=library,
    )
    try:
        assert services.preferences is application
        assert services.library_preferences is library
        services.preferences["only_application"] = True
        assert library == {}
    finally:
        services.close()


def test_temporary_files_create_only_the_selected_scratch_root_on_use(
    tmp_path, monkeypatch
):
    from LiuXin_alpha.utils import ptempfiles

    scratch = tmp_path / "nested" / "scratch"
    monkeypatch.setattr(ptempfiles, "_base_dir", str(scratch))
    assert not scratch.exists()
    with ptempfiles.TemporaryFile() as directory:
        assert Path(directory).parent == scratch
        assert Path(directory).is_file()
    assert not Path(directory).exists()
    assert scratch.is_dir()


def test_scratch_collision_does_not_replace_an_existing_file(tmp_path, monkeypatch):
    from LiuXin_alpha.utils import ptempfiles

    scratch = tmp_path / "scratch-file"
    scratch.write_text("retain")
    monkeypatch.setattr(ptempfiles, "_base_dir", str(scratch))
    with pytest.raises(OSError):
        ptempfiles.base_dir()
    assert scratch.read_text() == "retain"


def test_parallel_core_dispatches_restore_context_after_intermittent_errors(tmp_path):
    """Force both runtimes into their handlers together across repeated successes/failures."""
    import threading
    from concurrent.futures import ThreadPoolExecutor

    from LiuXin_alpha.core.errors import CoreHandlerError

    stores = [store_at(tmp_path / name) for name in ("first", "second")]
    cores = [
        CoreRuntime(library=SimpleNamespace(db=object()), preferences=store)
        for store in stores
    ]
    barrier = threading.Barrier(2, timeout=10)

    def handler(runtime, query):
        expected = runtime.services.preferences
        assert current_preferences() is expected
        barrier.wait()
        assert current_preferences() is expected
        if query.payload["iteration"] % 7 == 0:
            raise RuntimeError("intentional dispatch failure")
        return {"path": str(expected.paths.preferences)}

    for core in cores:
        core.register_query_handler("test.profile", handler)

    def exercise(index):
        sentinel = {"outside": index}
        with use_preferences(sentinel):
            for iteration in range(40):
                query = CoreQuery(name="test.profile", payload={"iteration": iteration})
                if iteration % 7 == 0:
                    with pytest.raises(
                        CoreHandlerError, match="intentional dispatch failure"
                    ):
                        cores[index].execute_query(query)
                else:
                    result = cores[index].execute_query(query).result
                    assert result["path"] == str(stores[index].paths.preferences)
                assert current_preferences() is sentinel

    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(exercise, i) for i in range(2)]
            for future in futures:
                future.result(timeout=60)
    finally:
        for core in cores:
            core.shutdown()


def test_repeated_graph_contexts_do_not_retain_closed_graph_context_variables():
    """A persistent worker context must not accumulate a key for every reopened graph."""
    from contextlib import ExitStack
    from contextvars import Context, copy_context

    from LiuXin_alpha.core.library_services import LibraryServices

    def exercise():
        initial = set(copy_context())
        for index in range(30):
            store = {"library": index}
            owner = LibraryServices(
                library=object(),
                core_services=SimpleNamespace(preferences=store),
                database=object(),
                storage=None,
                maintenance=None,
                exit_stack=ExitStack(),
                close_failures=[],
            )
            with owner:
                assert current_preferences() is store
            assert set(copy_context()) == initial

    Context().run(exercise)

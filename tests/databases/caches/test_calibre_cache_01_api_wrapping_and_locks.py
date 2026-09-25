"""
Check opt-in legacy cache wrapper aliases, read/write lock counters, and safe read acquisition.

These tests require LIUXIN_ENABLE_LEGACY_CALIBRE_CACHE_TESTS to be truthy and retain
assumptions about the deprecated Calibre-shaped schema. They skip at module import
by default under the FRBR-first schema. Enabling the gate does not make the fixture
schema compatible.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/caches/test_calibre_cache_01_api_wrapping_and_locks.py
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Tuple

import pytest

import os


_ENABLE = os.environ.get("LIUXIN_ENABLE_LEGACY_CALIBRE_CACHE_TESTS", "").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}

if not _ENABLE:
    pytest.skip(
        "Legacy CalibreCache tests are disabled under FRBR-first schema. "
        "Set LIUXIN_ENABLE_LEGACY_CALIBRE_CACHE_TESTS=1 to run them.",
        allow_module_level=True,
    )

# --- Make bundled libs importable (liuxin_dateutil is in utils/libraries) ---
_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_LIBS = _PROJECT_ROOT / "src" / "LiuXin_alpha" / "utils" / "libraries"
if _LIBS.exists() and str(_LIBS) not in sys.path:
    sys.path.insert(0, str(_LIBS))

from LiuXin_alpha.library.caches.calibre.cache import CalibreCache
from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.catalog.field_metadata import FieldMetadata
from LiuXin_alpha.databases.locking import DowngradeLockError


class TestPrefs(dict):
    """
    Provide explicit dictionary preferences with a separate fallback defaults mapping.

    Example:
        >>> prefs = TestPrefs({'flag': False})
        >>> (prefs['flag'], 'flag' in prefs)
        (False, False)
    """

    def __init__(self, defaults: dict | None = None):
        """
        Start with empty explicit storage and retain a truthy defaults mapping by reference.

        Example:
            >>> defaults = {'flag': False}
            >>> TestPrefs(defaults).defaults is defaults
            True


        :param defaults: Fallback mapping; not copied when nonempty.
        :return: None; false or absent defaults are replaced by a new empty dictionary.
        """
        super().__init__()
        self.defaults = defaults or {}

    def __getitem__(self, key):
        """
        Read explicit storage first, then defaults, raising KeyError if both lack the key.

        Example:
            >>> TestPrefs({'x': 2})['x']
            2


        :param key: Preference key to look up or store.
        :return: Stored or default preference value.
        """
        if key in self:
            return super().__getitem__(key)
        if key in self.defaults:
            return self.defaults[key]
        raise KeyError(key)

    def get(self, key, default=None):
        """
        Read explicit storage or defaults, otherwise return the supplied fallback.

        Example:
            >>> TestPrefs().get('missing', 3)
            3


        :param key: Preference key to look up or store.
        :param default: Fallback returned when neither explicit storage nor defaults
            contains the key.
        :return: Preference value or fallback; missing keys do not raise KeyError.
        """
        if key in self:
            return super().get(key)
        return self.defaults.get(key, default)

    def set(self, key, value) -> None:
        """
        Assign a preference into explicit dictionary storage.

        Example:
            >>> prefs = TestPrefs({'x': 2})
            >>> prefs.set('x', 4)
            >>> prefs['x']
            4


        :param key: Preference key to look up or store.
        :param value: Preference value to store without conversion.
        :return: None; does not persist outside this in-memory shim.
        """
        self[key] = value


class DummyFSM:
    """
    Produce deterministic dummy path strings without reading or creating assets.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_calibre_cache_01_api_wrapping_and_locks.py
    """

    def __init__(self, root: Path):
        """
        Convert and retain the root as a Path without creating it.

        Example:
            >>> DummyFSM('root').root == Path('root')
            True


        :param root: Base path for synthesized dummy locations.
        :return: None.
        """
        self.root = Path(root)

    def get_loc(self, *args, **kwargs):
        """
        Select a row from arguments and synthesize a location from its first recognized ID.

        book_folder_row overrides asset_row, which overrides the first positional argument,
        including explicit None values. For dictionaries try file_id, cover_id, folder_id,
        then id; unrecognized rows use unknown.

        Example:
            >>> DummyFSM('root').get_loc({'file_id': 7}) == str(Path('root') / 'file_id_7')
            True
            >>> DummyFSM('root').get_loc() is None
            True


        :param args: Optional positional values; only the first supplies a candidate row.
        :param kwargs: Optional asset_row or book_folder_row values; other keywords are
            ignored.
        :return: Path string, or None when the selected row is None.
        """
        row = None
        if args:
            row = args[0]
        row = kwargs.get("asset_row", row)
        row = kwargs.get("book_folder_row", row)

        if row is None:
            return None
        for k in ("file_id", "cover_id", "folder_id", "id"):
            if isinstance(row, dict) and k in row:
                return str(self.root / f"{k}_{row[k]}")
        return str(self.root / "unknown")


def _get_provision_fixture(request) -> Any:
    """
    Try the named database provisioner before the older provisioning fixture name.

    Only FixtureLookupError from a lookup triggers the fallback. If both lookups fail,
    attempt to construct a final FixtureLookupError with the diagnostic message; other
    errors propagate.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_calibre_cache_01_api_wrapping_and_locks.py


    :param request: Pytest request used to resolve a provisioning fixture by name.
    :return: First resolved fixture value; does not return when both lookups fail.
    """
    for name in ("provision_named_test_database", "provision_test_database"):
        try:
            return request.getfixturevalue(name)
        except pytest.FixtureLookupError:
            continue
    raise pytest.FixtureLookupError(
        "Expected a database provisioning fixture named 'provision_named_test_database' "
        "or 'provision_test_database' to exist."
    )


@pytest.fixture()
def calibre_backend_db(tmp_path: Path, request):
    """
    Provision test_db_0 and attach the legacy cache tables, lock, filesystem, preferences, and metadata shims.

    Return an open Database without a local cleanup finalizer.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_calibre_cache_01_api_wrapping_and_locks.py


    :param tmp_path: Pytest-provided temporary directory for isolated database or TOML
        files.
    :param request: Pytest request used to resolve a provisioning fixture by name.
    :return: Database configured as a legacy CalibreCache backend.
    """
    provision = _get_provision_fixture(request)
    prov = provision(name="test_db_0", dst_dir=tmp_path)

    db = Database(metadata={"database_path": str(prov.db_path)})

    # CalibreCache expects these attributes to exist on the backend
    db.tables = {}
    db.conn = db.driver_wrapper.lock  # used as `with backend.conn:`
    db.fsm = DummyFSM(tmp_path / "fsm_root")

    # Preferences CalibreCache reads early
    db.prefs = TestPrefs(
        defaults={
            "bools_are_tristate": False,
            "update_all_last_mod_dates_on_start": False,
            "metadata_backup_interval": 0,
        }
    )
    db.default_prefs = dict(db.prefs.defaults)
    db.pref_progress_callback = None

    # CalibreCache calls: initialize_prefs(default_prefs, restore_all_prefs, progress_callback)
    db.restore_all_prefs = False

    def _init_prefs(default_prefs=None, restore_all_prefs=False, progress_callback=None):
        """
        Merge truthy defaults, materialize missing explicit keys, and optionally report the defaults count.

        Existing explicit preferences remain unchanged. For any non-None defaults, call a
        callable progress callback with (None, len(default_prefs)); ignore the restore flag.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/caches/test_calibre_cache_01_api_wrapping_and_locks.py


        :param default_prefs: Optional mapping of fallback preferences to merge.
        :param restore_all_prefs: Accepted for legacy call compatibility; unused.
        :param progress_callback: Optional callable receiving the defaults count.
        :return: None; mutates the enclosing database preferences.
        """
        if default_prefs:
            db.prefs.defaults.update(default_prefs)
            # Materialize defaults into storage if missing (matches legacy behaviour)
            for k, v in default_prefs.items():
                if k not in db.prefs:
                    db.prefs[k] = v
        if callable(progress_callback) and default_prefs is not None:
            progress_callback(None, len(default_prefs))

    db.initialize_prefs = _init_prefs

    db.field_metadata = FieldMetadata()

    # Some codepaths call backend.custom_table_names(...)
    db.custom_table_names = db.driver_wrapper.custom_table_names

    return db


@pytest.fixture()
def live_calibre_cache(calibre_backend_db):
    """
    Construct the legacy cache, call init, and return it without a local close finalizer.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_calibre_cache_01_api_wrapping_and_locks.py


    :param calibre_backend_db: Provisioned Database carrying the legacy cache backend
        shims; the local fixture has no close finalizer.
    :return: Initialized CalibreCache; setup errors propagate.
    """
    cache = CalibreCache(backend=calibre_backend_db)
    cache.init()
    return cache


@dataclass
class SpyLock:
    """
    Record context acquisitions, releases, and depth without synchronization or ownership checks.

    Example:
        >>> SpyLock('read').depth
        0
    """

    name: str
    acquisitions: int = 0
    releases: int = 0
    depth: int = 0

    def acquire(self):
        """
        Increment acquisition count and nesting depth without blocking.

        Example:
            >>> lock = SpyLock('read')
            >>> lock.acquire()
            True
            >>> (lock.acquisitions, lock.depth)
            (1, 1)


        :return: True.
        """
        self.acquisitions += 1
        self.depth += 1
        return True

    def release(self, *args):
        """
        Increment releases and decrement depth without validating prior acquisition.

        Example:
            >>> lock = SpyLock('read')
            >>> lock.release()
            >>> (lock.releases, lock.depth)
            (1, -1)


        :param args: Unused positional arguments accepted for lock compatibility.
        :return: None; depth can become negative.
        """
        self.releases += 1
        self.depth -= 1

    def __enter__(self):
        """
        Record an acquisition and return this spy.

        Example:
            >>> lock = SpyLock('read')
            >>> lock.__enter__() is lock
            True


        :return: The same SpyLock instance.
        """
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, tb):
        """
        Record a release and leave any context-body exception unsuppressed.

        Example:
            >>> lock = SpyLock('read')
            >>> with lock:
            ...     pass
            >>> (lock.acquisitions, lock.releases, lock.depth)
            (1, 1, 0)


        :param exc_type: Exception type from the context body, ignored.
        :param exc: Exception instance, ignored.
        :param tb: Traceback, ignored.
        :return: False.
        """
        self.release()
        return False


@pytest.fixture()
def cache_with_spy_locks(monkeypatch, calibre_backend_db) -> Tuple[CalibreCache, SpyLock, SpyLock]:
    """
    Patch BaseCache lock creation, construct a cache, and reset acquisition/release counters after checking zero depth.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_calibre_cache_01_api_wrapping_and_locks.py


    :param monkeypatch: Pytest patch fixture; restores replaced generator or lookup
        attributes after the test.
    :param calibre_backend_db: Provisioned Database carrying the legacy cache backend
        shims; the local fixture has no close finalizer.
    :return: Tuple of (uninitialized cache, read spy, write spy); monkeypatch restores
        the lock factory.
    """
    import LiuXin_alpha.customize.cache.base_cache as base_cache

    spy_read = SpyLock("read")
    spy_write = SpyLock("write")

    # BaseCache imports create_locks into its module namespace; patch that symbol.
    monkeypatch.setattr(base_cache, "create_locks", lambda: (spy_read, spy_write))

    cache = CalibreCache(backend=calibre_backend_db)

    # __init__ may have acquired locks for internal setup; reset counters to focus on our calls.
    assert spy_read.depth == 0 and spy_write.depth == 0
    spy_read.acquisitions = spy_read.releases = 0
    spy_write.acquisitions = spy_write.releases = 0

    return cache, spy_read, spy_write


def _same_bound_method(a, b) -> bool:
    """
    Compare receiver and function attributes by identity, defaulting missing attributes to None.

    Objects lacking both attributes compare equal under this helper, so it is intended
    for bound-method assertions.

    Example:
        >>> prefs = TestPrefs()
        >>> _same_bound_method(prefs.set, prefs.set)
        True
        >>> _same_bound_method(prefs.set, TestPrefs().set)
        False


    :param a: First candidate bound method.
    :param b: Second candidate bound method.
    :return: True when both attribute identities match.
    """
    return getattr(a, "__self__", None) is getattr(b, "__self__", None) and getattr(a, "__func__", None) is getattr(
        b, "__func__", None
    )


def test_read_write_api_methods_are_wrapped_and_aliased(cache_with_spy_locks):
    """
    Check pref/set_pref wrapper identities and lock counts, unlocked aliases, and the absence of an auto-wrapped init alias.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_calibre_cache_01_api_wrapping_and_locks.py::test_read_write_api_methods_are_wrapped_and_aliased


    :param cache_with_spy_locks: Uninitialized legacy cache plus read/write SpyLock
        instances from the patched fixture.
    :return: None; failed expectations raise AssertionError.
    """
    cache, spy_read, spy_write = cache_with_spy_locks

    # read_api: pref
    assert hasattr(cache, "_pref"), "BaseCache should save original as _pref"
    assert hasattr(cache.unlock, "pref"), "BaseCache should expose unlocked alias on cache.unlock"
    assert getattr(cache.pref, "__wrapped__", None) is not None, "Wrapped method should have __wrapped__"
    assert _same_bound_method(cache.pref.__wrapped__, cache._pref)
    assert _same_bound_method(cache.unlock.pref, cache._pref)

    # write_api: set_pref
    assert hasattr(cache, "_set_pref"), "BaseCache should save original as _set_pref"
    assert hasattr(cache.unlock, "set_pref"), "BaseCache should expose unlocked alias on cache.unlock"
    assert getattr(cache.set_pref, "__wrapped__", None) is not None
    assert _same_bound_method(cache.set_pref.__wrapped__, cache._set_pref)
    assert _same_bound_method(cache.unlock.set_pref, cache._set_pref)

    # api-only: init should NOT be auto-wrapped by BaseCache (it does its own locking).
    assert not hasattr(cache, "_init"), "@api methods should not be auto-wrapped into _init"

    # Wrapped read should acquire read lock
    _ = cache.pref("bools_are_tristate")
    assert spy_read.acquisitions == 1
    assert spy_read.releases == 1
    assert spy_read.depth == 0
    assert spy_write.acquisitions == 0

    # Unlocked versions should not acquire locks.
    _ = cache._pref("bools_are_tristate")
    _ = cache.unlock.pref("bools_are_tristate")
    assert spy_read.acquisitions == 1
    assert spy_write.acquisitions == 0

    # Wrapped write should acquire write lock
    cache.set_pref("unit_test_pref", 123)
    assert spy_write.acquisitions == 1
    assert spy_write.releases == 1
    assert spy_write.depth == 0

    # Unlocked write variants should not acquire locks
    cache._set_pref("unit_test_pref2", 456)
    cache.unlock.set_pref("unit_test_pref3", 789)
    assert spy_write.acquisitions == 1


def test_safe_read_lock_suppresses_downgrade_error(live_calibre_cache):
    """
    Check safe-read objects are fresh and usable inside a write lock where ordinary read acquisition raises DowngradeLockError.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_calibre_cache_01_api_wrapping_and_locks.py::test_safe_read_lock_suppresses_downgrade_error


    :param live_calibre_cache: CalibreCache initialized by the module fixture, available
        only when legacy tests are enabled.
    :return: None; failed expectations raise AssertionError.
    """
    cache = live_calibre_cache

    # Access should return a new SafeReadLock each time.
    assert cache.safe_read_lock is not cache.safe_read_lock

    with cache.write_lock:
        # A plain read lock acquisition inside a write lock should raise.
        with pytest.raises(DowngradeLockError):
            cache.read_lock.acquire()

        # SafeReadLock should suppress the downgrade error.
        with cache.safe_read_lock:
            assert cache.pref("bools_are_tristate") in (True, False)

"""
Step 01: CalibreCache API wrapping + locking (single drop-in file)

Drop this file into: tests/databases/caches/test_calibre_cache_01_api_wrapping_and_locks.py

What it checks:
- BaseCache's @read_api / @write_api wrapping happens:
    * original methods preserved as cache._method
    * unlocked aliases exposed as cache.unlock.method
    * public methods are wrapped and acquire the correct lock
- safe_read_lock suppresses DowngradeLockError when inside write lock

Assumptions:
- Your repo provides a fixture that provisions a named test database copy.
  Historically this has been either:
    - provision_test_database(name=..., dst_dir=...)
    - provision_named_test_database(name=..., dst_dir=...)
  This file supports either (via request.getfixturevalue()).
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Tuple

import pytest

# --- Make bundled libs importable (liuxin_dateutil is in utils/libraries) ---
_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_LIBS = _PROJECT_ROOT / "src" / "LiuXin_alpha" / "utils" / "libraries"
if _LIBS.exists() and str(_LIBS) not in sys.path:
    sys.path.insert(0, str(_LIBS))

from LiuXin_alpha.databases.caches.calibre.cache import CalibreCache
from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.databases.field_metadata import FieldMetadata
from LiuXin_alpha.databases.locking import DowngradeLockError


class TestPrefs(dict):
    """Tiny prefs shim used by CalibreCache during init()."""

    def __init__(self, defaults: dict | None = None):
        super().__init__()
        self.defaults = defaults or {}

    def __getitem__(self, key):
        if key in self:
            return super().__getitem__(key)
        if key in self.defaults:
            return self.defaults[key]
        raise KeyError(key)

    def get(self, key, default=None):
        if key in self:
            return super().get(key)
        return self.defaults.get(key, default)

    def set(self, key, value) -> None:
        self[key] = value


class DummyFSM:
    """Minimal fsm used by formats/covers/path tables during init()."""

    def __init__(self, root: Path):
        self.root = Path(root)

    def get_loc(self, *args, **kwargs):
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
    Support both fixture spellings used across the repo, without asking you to rename anything.
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
    A Database instance with the minimum shims CalibreCache expects.
    Uses test_db_0 by default (non-empty).
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
    """A fully initialized CalibreCache instance."""
    cache = CalibreCache(backend=calibre_backend_db)
    cache.init()
    return cache


@dataclass
class SpyLock:
    """A minimal context-manager lock that records acquisitions/releases."""

    name: str
    acquisitions: int = 0
    releases: int = 0
    depth: int = 0

    def acquire(self):
        self.acquisitions += 1
        self.depth += 1
        return True

    def release(self, *args):
        self.releases += 1
        self.depth -= 1

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.release()
        return False


@pytest.fixture()
def cache_with_spy_locks(monkeypatch, calibre_backend_db) -> Tuple[CalibreCache, SpyLock, SpyLock]:
    """
    Construct a CalibreCache with spy locks (without calling init()).
    We patch BaseCache's create_locks() symbol so @read_api/@write_api wrappers use our spies.
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
    """Compare two bound methods for identity (instance + underlying function)."""
    return getattr(a, "__self__", None) is getattr(b, "__self__", None) and getattr(a, "__func__", None) is getattr(
        b, "__func__", None
    )


def test_read_write_api_methods_are_wrapped_and_aliased(cache_with_spy_locks):
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

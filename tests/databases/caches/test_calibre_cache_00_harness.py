"""
Provide an opt-in legacy cache fixture and check its core initialized surface.

These tests require LIUXIN_ENABLE_LEGACY_CALIBRE_CACHE_TESTS to be truthy and retain
assumptions about the deprecated Calibre-shaped schema. They skip at module import
by default under the FRBR-first schema. Enabling the gate does not make the fixture
schema compatible.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/caches/test_calibre_cache_00_harness.py
"""
import sys
from pathlib import Path
import pytest

import os


# ---------------------------------------------------------------------------
# NOTE: legacy CalibreCache
# ---------------------------------------------------------------------------
#
# These tests exercise the historical CalibreCache layer, which targets the
# deprecated calibre-shaped schema (meta/books/authors/tags...). Under the
# FRBR-first generator, those writable tables no longer exist.
#
# Keep these tests opt-in while the cache layer is either removed or
# reimplemented on top of FRBR views.

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

from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.catalog.field_metadata import FieldMetadata
from LiuXin_alpha.library.caches.calibre.cache import CalibreCache


class TestPrefs(dict):
    """
    Provide explicit dictionary preferences with a separate fallback defaults mapping.

    Example:
        >>> prefs = TestPrefs({'flag': False})
        >>> (prefs['flag'], 'flag' in prefs)
        (False, False)
    """

    def __init__(self, defaults=None):
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

    def set(self, key, value):
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

            python -m pytest -q tests/databases/caches/test_calibre_cache_00_harness.py
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
        # produce stable dummy locations
        if row is None:
            return None
        # try common id keys
        for k in ("file_id", "cover_id", "folder_id", "id"):
            if isinstance(row, dict) and k in row:
                return str(self.root / f"{k}_{row[k]}")
        return str(self.root / "unknown")


@pytest.fixture()
def calibre_backend_db(tmp_path, provision_named_test_database):
    # provision a real sqlite db copy
    """
    Provision test_db_0 and attach the legacy cache tables, lock, filesystem, preferences, and metadata shims.

    Return an open Database without a local cleanup finalizer.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_calibre_cache_00_harness.py


    :param tmp_path: Pytest-provided temporary directory for isolated database or TOML
        files.
    :param provision_named_test_database: Fixture factory that copies a named test
        database into an isolated location.
    :return: Database configured as a legacy CalibreCache backend.
    """
    prov = provision_named_test_database(name="test_db_0", dst_dir=tmp_path)

    db = Database(metadata={"database_path": str(prov.db_path)})

    # Patch in what CalibreCache expects on the backend
    db.tables = {}  # BaseCache expects it to exist; CalibreCache will replace it
    db.conn = db.driver_wrapper.lock  # used as `with backend.conn:`
    db.fsm = DummyFSM(tmp_path / "fsm_root")

    # preferences CalibreCache reads immediately
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
        # keep defaults in sync with what the cache passes in
        """
        Merge truthy defaults, materialize missing explicit keys, and optionally report the defaults count.

        Existing explicit preferences remain unchanged. For any non-None defaults, call a
        callable progress callback with (None, len(default_prefs)); ignore the restore flag.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/caches/test_calibre_cache_00_harness.py


        :param default_prefs: Optional mapping of fallback preferences to merge.
        :param restore_all_prefs: Accepted for legacy call compatibility; unused.
        :param progress_callback: Optional callable receiving the defaults count.
        :return: None; mutates the enclosing database preferences.
        """
        if default_prefs:
            db.prefs.defaults.update(default_prefs)
            # optionally materialize defaults into prefs storage if missing
            for k, v in default_prefs.items():
                if k not in db.prefs:
                    db.prefs[k] = v

        # progress_callback is optional; mimic the shape Calibre expects
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

            python -m pytest -q tests/databases/caches/test_calibre_cache_00_harness.py


    :param calibre_backend_db: Provisioned Database carrying the legacy cache backend
        shims; the local fixture has no close finalizer.
    :return: Initialized CalibreCache; setup errors propagate.
    """
    cache = CalibreCache(backend=calibre_backend_db)
    cache.init()
    return cache


def test_live_cache_fixture_initializes(live_calibre_cache):
    """
    Check the initialized cache exposes a title field and a dictionary of tables.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_calibre_cache_00_harness.py::test_live_cache_fixture_initializes


    :param live_calibre_cache: CalibreCache initialized by the module fixture, available
        only when legacy tests are enabled.
    :return: None; failed expectations raise AssertionError.
    """
    cache = live_calibre_cache
    assert cache is not None
    assert getattr(cache, "fields", None) is not None
    assert "title" in cache.fields  # core field should exist
    assert isinstance(cache.tables, dict)

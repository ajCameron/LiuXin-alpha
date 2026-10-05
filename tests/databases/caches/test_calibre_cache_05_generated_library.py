"""
Check legacy cache initialization against a generated nonempty Calibre library.

If enabled, the module also skips when the optional CalibreCache import raises
ModuleNotFoundError.

These tests require LIUXIN_ENABLE_LEGACY_CALIBRE_CACHE_TESTS to be truthy and retain
assumptions about the deprecated Calibre-shaped schema. They skip at module import
by default under the FRBR-first schema. Enabling the gate does not make the fixture
schema compatible.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/caches/test_calibre_cache_05_generated_library.py
"""
from __future__ import annotations

from pathlib import Path

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

from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.catalog.field_metadata import FieldMetadata

try:
    from LiuXin_alpha.library.caches.calibre.cache import CalibreCache
except ModuleNotFoundError as e:
    # Some minimal test environments omit optional calibre-compat deps.
    pytest.skip(f"Skipping CalibreCache smoke test; missing dependency: {e}", allow_module_level=True)


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

            python -m pytest -q tests/databases/caches/test_calibre_cache_05_generated_library.py
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


def _backend_from_library(tmp_path: Path, metadata_db_path: Path) -> Database:
    """
    Open the generated metadata database and attach legacy tables, lock, filesystem, preference, and metadata shims.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_calibre_cache_05_generated_library.py


    :param tmp_path: Pytest-provided temporary directory for isolated database or TOML
        files.
    :param metadata_db_path: Path to the generated library metadata.db file.
    :return: Open Database; this helper does not install a close finalizer.
    """
    db = Database(metadata={"database_path": str(metadata_db_path)})
    db.tables = {}
    db.conn = db.driver_wrapper.lock
    db.fsm = DummyFSM(tmp_path / "fsm_root")

    db.prefs = TestPrefs(
        defaults={
            "bools_are_tristate": False,
            "update_all_last_mod_dates_on_start": False,
            "metadata_backup_interval": 0,
        }
    )
    db.default_prefs = dict(db.prefs.defaults)
    db.pref_progress_callback = None
    db.restore_all_prefs = False

    def _init_prefs(default_prefs=None, restore_all_prefs=False, progress_callback=None):
        """
        Merge fallback defaults and materialize missing explicit preferences, then optionally report their count.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/caches/test_calibre_cache_05_generated_library.py


        :param default_prefs: Optional defaults mapping; existing explicit values are
            preserved.
        :param restore_all_prefs: Compatibility argument, accepted but unused.
        :param progress_callback: Callable receiving (None, len(default_prefs)) when
            defaults are not None.
        :return: None; mutates the enclosing database preferences.
        """
        if default_prefs:
            db.prefs.defaults.update(default_prefs)
            for k, v in default_prefs.items():
                if k not in db.prefs:
                    db.prefs[k] = v
        if callable(progress_callback) and default_prefs is not None:
            progress_callback(None, len(default_prefs))

    db.initialize_prefs = _init_prefs
    db.field_metadata = FieldMetadata()
    db.custom_table_names = db.driver_wrapper.custom_table_names
    return db


def test_cache_init_on_generated_calibre_library(tmp_path, provision_populated_calibre_library):
    """
    Add a book with an EPUB, author, and tag to a generated library and check cache initialization exposes title.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_calibre_cache_05_generated_library.py::test_cache_init_on_generated_calibre_library


    :param tmp_path: Pytest-provided temporary directory for isolated database or TOML
        files.
    :param provision_populated_calibre_library: Fixture factory returning a library and
        mutable builder; owns generated library files.
    :return: None; failed expectations raise AssertionError.
    """
    lib, builder = provision_populated_calibre_library(name="calibre_lib_for_cache")

    # Make the library non-empty (exercises more init paths).
    builder.add_book(
        title="Cache Smoke",
        authors=["Constance Thrane"],
        formats={"EPUB": b"epub"},
        tags=["smoke"],
    )

    backend = _backend_from_library(tmp_path, Path(lib.root) / "metadata.db")
    cache = CalibreCache(backend=backend)
    cache.init()

    assert "title" in cache.fields

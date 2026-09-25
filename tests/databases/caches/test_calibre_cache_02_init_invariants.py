"""
Check initialized legacy cache tables, fields, metadata, backend aliases, and ID maps.

These tests require LIUXIN_ENABLE_LEGACY_CALIBRE_CACHE_TESTS to be truthy and retain
assumptions about the deprecated Calibre-shaped schema. They skip at module import
by default under the FRBR-first schema. Enabling the gate does not make the fixture
schema compatible.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/caches/test_calibre_cache_02_init_invariants.py
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

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

# Table classes used for a few “specialization” assertions (kept minimal to reduce brittleness)
from LiuXin_alpha.library.caches.calibre.tables.one_one_tables import (
    CalibreOneToOneTable,
    CalibreUUIDTable,
    CalibrePathTable,
)
from LiuXin_alpha.library.caches.calibre.tables.one_one_tables import CalibreCoversTable
from LiuXin_alpha.library.caches.calibre.tables.many_many_tables import CalibreAuthorsTable, CalibreFormatsTable
from LiuXin_alpha.library.caches.calibre.tables.one_many_tables import CalibreIdentifiersTable
from LiuXin_alpha.library.caches.calibre.tables.many_one_tables import CalibreRatingTable
from LiuXin_alpha.library.caches.calibre.tables.base import CalibreVirtualTable


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

            python -m pytest -q tests/databases/caches/test_calibre_cache_02_init_invariants.py
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

            python -m pytest -q tests/databases/caches/test_calibre_cache_02_init_invariants.py


    :param request: Pytest request used to resolve a provisioning fixture by name.
    :return: First resolved fixture value; does not return when both lookups fail.
    """
    for name in ("provision_named_test_database", "provision_test_database"):
        try:
            return request.getfixturevalue(name)
        except pytest.FixtureLookupError:
            continue
    raise pytest.FixtureLookupError(
        "Expected fixture 'provision_named_test_database' or 'provision_test_database' to exist."
    )


@pytest.fixture()
def calibre_backend_db(tmp_path: Path, request):
    """
    Provision test_db_0 and attach the legacy cache tables, lock, filesystem, preferences, and metadata shims.

    Return an open Database without a local cleanup finalizer. On TypeError, retry
    provisioning without dst_dir.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_calibre_cache_02_init_invariants.py


    :param tmp_path: Pytest-provided temporary directory for isolated database or TOML
        files.
    :param request: Pytest request used to resolve a provisioning fixture by name.
    :return: Database configured as a legacy CalibreCache backend.
    """
    provision = _get_provision_fixture(request)

    # Handle both provisioner shapes:
    # - provision_named_test_database(name=..., dst_dir=...)
    # - provision_test_database(name=..., dst_dir=...) OR provision_test_database(name=...) (legacy)
    try:
        prov = provision(name="test_db_0", dst_dir=tmp_path)
    except TypeError:
        prov = provision(name="test_db_0")

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

                python -m pytest -q tests/databases/caches/test_calibre_cache_02_init_invariants.py


        :param default_prefs: Optional mapping of fallback preferences to merge.
        :param restore_all_prefs: Accepted for legacy call compatibility; unused.
        :param progress_callback: Optional callable receiving the defaults count.
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

    # Some codepaths call backend.custom_table_names(...)
    db.custom_table_names = db.driver_wrapper.custom_table_names

    return db


@pytest.fixture()
def live_calibre_cache(calibre_backend_db):
    """
    Construct the legacy cache, call init, and return it without a local close finalizer.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_calibre_cache_02_init_invariants.py


    :param calibre_backend_db: Provisioned Database carrying the legacy cache backend
        shims; the local fixture has no close finalizer.
    :return: Initialized CalibreCache; setup errors propagate.
    """
    cache = CalibreCache(backend=calibre_backend_db)
    cache.init()
    return cache


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


# -------------------------------------------------------------------------------------------------
# Step 02 tests


def test_init_sets_expected_flags_and_backend_methods(live_calibre_cache):
    """
    Check init_called and that the three compatibility methods are bound to this cache on the backend.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_calibre_cache_02_init_invariants.py::test_init_sets_expected_flags_and_backend_methods


    :param live_calibre_cache: CalibreCache initialized by the module fixture, available
        only when legacy tests are enabled.
    :return: None; failed expectations raise AssertionError.
    """
    cache = live_calibre_cache
    assert getattr(cache, "init_called", False) is True

    # The cache is expected to patch these methods onto the backend for compatibility
    assert hasattr(cache.backend, "read_tables")
    assert hasattr(cache.backend, "initialize_tables")
    assert hasattr(cache.backend, "initialize_custom_columns")

    # They should be bound to *this* cache instance
    assert getattr(cache.backend.read_tables, "__self__", None) is cache
    assert getattr(cache.backend.initialize_tables, "__self__", None) is cache
    assert getattr(cache.backend.initialize_custom_columns, "__self__", None) is cache


def test_tables_include_expected_builtin_subset(live_calibre_cache):
    """
    Check a nonempty table dictionary includes the required built-in subset, allowing additional tables.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_calibre_cache_02_init_invariants.py::test_tables_include_expected_builtin_subset


    :param live_calibre_cache: CalibreCache initialized by the module fixture, available
        only when legacy tests are enabled.
    :return: None; failed expectations raise AssertionError.
    """
    cache = live_calibre_cache
    assert isinstance(cache.tables, dict)
    assert cache.tables, "tables should not be empty after init()"

    expected = {
        # one-to-one
        "title",
        "sort",
        "author_sort",
        "series_index",
        "timestamp",
        "pubdate",
        "uuid",
        "path",
        "last_modified",
        "notes",
        "cover",
        # many-to-one / many-many etc.
        "series",
        "publisher",
        "subjects",
        "synopses",
        "genre",
        "comments",
        "authors",
        "tags",
        "formats",
        "identifiers",
        "languages",
        "rating",
        # virtual
        "size",
    }

    missing = sorted(expected.difference(cache.tables.keys()))
    assert not missing, f"missing expected builtin tables: {missing!r}"


def test_some_tables_are_specialized_classes(live_calibre_cache):
    """
    Check eight selected built-in tables use the expected specialized classes.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_calibre_cache_02_init_invariants.py::test_some_tables_are_specialized_classes


    :param live_calibre_cache: CalibreCache initialized by the module fixture, available
        only when legacy tests are enabled.
    :return: None; failed expectations raise AssertionError.
    """
    cache = live_calibre_cache
    assert isinstance(cache.tables["title"], CalibreOneToOneTable)
    assert isinstance(cache.tables["uuid"], CalibreUUIDTable)
    assert isinstance(cache.tables["path"], CalibrePathTable)
    assert isinstance(cache.tables["cover"], CalibreCoversTable)

    assert isinstance(cache.tables["authors"], CalibreAuthorsTable)
    assert isinstance(cache.tables["formats"], CalibreFormatsTable)
    assert isinstance(cache.tables["identifiers"], CalibreIdentifiersTable)
    assert isinstance(cache.tables["rating"], CalibreRatingTable)


def test_fields_created_for_tables_and_point_to_table_objects(live_calibre_cache):
    """
    Check table fields retain exact table identities and ondevice is a separate virtual field.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_calibre_cache_02_init_invariants.py::test_fields_created_for_tables_and_point_to_table_objects


    :param live_calibre_cache: CalibreCache initialized by the module fixture, available
        only when legacy tests are enabled.
    :return: None; failed expectations raise AssertionError.
    """
    cache = live_calibre_cache
    assert isinstance(cache.fields, dict)
    assert cache.fields, "fields should not be empty after init()"

    # For DB-backed tables: field.table should be the exact same object as cache.tables[name]
    for name, table in cache.tables.items():
        assert name in cache.fields, f"missing field for table {name!r}"
        field = cache.fields[name]
        assert getattr(field, "name", None) == name
        assert getattr(field, "table", None) is table

    # Virtual field: ondevice exists as a field but not in cache.tables
    assert "ondevice" in cache.fields
    assert "ondevice" not in cache.tables
    ondevice = cache.fields["ondevice"]
    assert isinstance(ondevice.table, CalibreVirtualTable)
    assert getattr(ondevice.table, "name", None) == "ondevice"


def test_cross_linking_invariants(live_calibre_cache):
    """
    Check author/title sort links, reciprocal series-index links, and the series internal-update flag.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_calibre_cache_02_init_invariants.py::test_cross_linking_invariants


    :param live_calibre_cache: CalibreCache initialized by the module fixture, available
        only when legacy tests are enabled.
    :return: None; failed expectations raise AssertionError.
    """
    cache = live_calibre_cache

    # authors should have author_sort_field
    authors = cache.fields["authors"]
    assert getattr(authors, "author_sort_field", None) is cache.fields["author_sort"]

    # title should have title_sort_field
    title = cache.fields["title"]
    assert getattr(title, "title_sort_field", None) is cache.fields["sort"]

    # series should have index_field and series_index should point back
    series = cache.fields["series"]
    series_index = cache.fields["series_index"]
    assert getattr(series, "index_field", None) is series_index
    assert getattr(series_index, "series_field", None) is series

    # CalibreCache sets this as a legacy behaviour flag
    assert getattr(series, "internal_update_used", None) is True


def test_field_metadata_contains_minimum_contract(live_calibre_cache):
    """
    Check the selected built-in metadata entries are dictionaries containing datatype.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_calibre_cache_02_init_invariants.py::test_field_metadata_contains_minimum_contract


    :param live_calibre_cache: CalibreCache initialized by the module fixture, available
        only when legacy tests are enabled.
    :return: None; failed expectations raise AssertionError.
    """
    cache = live_calibre_cache
    fm = cache.field_metadata

    # We only assert presence and basic shape for a small builtin subset.
    for name in ("title", "authors", "tags", "series", "uuid", "path", "formats", "identifiers", "last_modified"):
        assert name in fm, f"field_metadata missing key {name!r}"
        md = fm[name]
        assert isinstance(md, dict)
        assert "datatype" in md


def test_field_map_has_unique_integer_positions(live_calibre_cache):
    """
    Check id maps to zero, positions are unique integer instances, and required field keys exist.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_calibre_cache_02_init_invariants.py::test_field_map_has_unique_integer_positions


    :param live_calibre_cache: CalibreCache initialized by the module fixture, available
        only when legacy tests are enabled.
    :return: None; failed expectations raise AssertionError.
    """
    cache = live_calibre_cache
    fm = cache.FIELD_MAP
    assert isinstance(fm, dict)
    assert "id" in fm and fm["id"] == 0

    values = list(fm.values())
    assert all(isinstance(v, int) for v in values)
    assert len(set(values)) == len(values), "FIELD_MAP positions should be unique"

    # Minimal key presence (don’t overfit; comment in code notes this map may evolve)
    for k in ("title", "authors", "tags", "formats", "uuid", "last_modified", "identifiers"):
        assert k in fm


def test_all_book_ids_consistent_with_uuid_table(live_calibre_cache):
    """
    Check all_book_ids is a frozenset of integer instances equal to the UUID table key set.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_calibre_cache_02_init_invariants.py::test_all_book_ids_consistent_with_uuid_table


    :param live_calibre_cache: CalibreCache initialized by the module fixture, available
        only when legacy tests are enabled.
    :return: None; failed expectations raise AssertionError.
    """
    cache = live_calibre_cache
    book_ids = cache.all_book_ids()
    assert isinstance(book_ids, frozenset)

    # all_book_ids() is defined as the keys of uuid.table.book_col_map
    uuid_keys = frozenset(cache.fields["uuid"].table.book_col_map)
    assert book_ids == uuid_keys

    # Type sanity: IDs should be ints (or at least int-like)
    assert all(isinstance(x, int) for x in book_ids)

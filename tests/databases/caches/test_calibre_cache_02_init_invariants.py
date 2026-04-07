"""
Step 02: CalibreCache init invariants (single drop-in file)

Drop this file into:
    tests/databases/caches/test_calibre_cache_02_init_invariants.py

What it checks (post-init structural invariants):
- cache.init_called True; tables/fields dicts exist and contain expected builtins
- builtin tables created with expected specialized classes for a few key columns
- fields created for every table and point at the same table objects
- virtual 'ondevice' field exists and is not a DB-backed table
- cross-linking invariants (authors->author_sort, title->sort, series<->series_index)
- FIELD_MAP is sane (contains id=0, unique integer positions)
- all_book_ids() is consistent with uuid table cache (no assumptions about non-empty DB)
- backend has legacy-compat methods patched to cache methods (read_tables etc.)

Assumptions:
- Repo provides one of:
    provision_named_test_database(name=..., dst_dir=...)
    provision_test_database(name=..., dst_dir=...)  (or provision_test_database(name=...) depending on harness)
  This file supports either via request.getfixturevalue().
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
from LiuXin_alpha.interfaces.field_metadata import FieldMetadata

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
    """Support both fixture spellings used across the repo."""
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
    A Database instance with the minimum shims CalibreCache expects.
    Uses test_db_0 by default.
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
    cache = CalibreCache(backend=calibre_backend_db)
    cache.init()
    return cache


def _same_bound_method(a, b) -> bool:
    """Compare two bound methods for identity (instance + underlying function)."""
    return getattr(a, "__self__", None) is getattr(b, "__self__", None) and getattr(a, "__func__", None) is getattr(
        b, "__func__", None
    )


# -------------------------------------------------------------------------------------------------
# Step 02 tests


def test_init_sets_expected_flags_and_backend_methods(live_calibre_cache):
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
    Keep this minimal: we only sanity-check a few “special” ones that should remain stable.
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
    cache = live_calibre_cache
    fm = cache.field_metadata

    # We only assert presence and basic shape for a small builtin subset.
    for name in ("title", "authors", "tags", "series", "uuid", "path", "formats", "identifiers", "last_modified"):
        assert name in fm, f"field_metadata missing key {name!r}"
        md = fm[name]
        assert isinstance(md, dict)
        assert "datatype" in md


def test_field_map_has_unique_integer_positions(live_calibre_cache):
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
    cache = live_calibre_cache
    book_ids = cache.all_book_ids()
    assert isinstance(book_ids, frozenset)

    # all_book_ids() is defined as the keys of uuid.table.book_col_map
    uuid_keys = frozenset(cache.fields["uuid"].table.book_col_map)
    assert book_ids == uuid_keys

    # Type sanity: IDs should be ints (or at least int-like)
    assert all(isinstance(x, int) for x in book_ids)

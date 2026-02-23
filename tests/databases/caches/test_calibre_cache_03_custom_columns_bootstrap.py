"""
Step 03: CalibreCache custom columns bootstrap (single drop-in file)

Drop into:
    tests/databases/caches/test_calibre_cache_03_custom_columns_bootstrap.py

What it checks:
- initialize_custom_columns() loads custom column metadata into backend maps
  (custom_column_label_map / custom_column_num_map), builds multiple separators,
  registers custom_data_adapters, and creates the TEMP delete trigger when needed.
- initialize_tables() registers custom columns as the appropriate table classes:
    * normalized + is_multiple -> CalibreManyToManyTable
    * normalized + single      -> CalibreManyToOneTable
    * non-normalized           -> CalibreOneToOneTable
    * series adds an *_index one-to-one table (expected shape)
- custom columns marked for delete are removed (tables dropped + row deleted)
  and the pref 'update_all_last_mod_dates_on_start' is set True.
- orphaned custom column records (missing required tables) are removed.

Assumptions:
- Repo provides one of:
    provision_named_test_database(name=..., dst_dir=...)
    provision_test_database(name=..., dst_dir=...)  (or legacy provision_test_database(name=...))
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Optional

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

from LiuXin_alpha.databases.caches.calibre.cache import CalibreCache
from LiuXin_alpha.databases.custom_columns import CustomColumns
from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.databases.field_metadata import FieldMetadata

from LiuXin_alpha.databases.caches.calibre.tables.one_one_tables import CalibreOneToOneTable
from LiuXin_alpha.databases.caches.calibre.tables.many_one_tables.many_to_one_table import CalibreManyToOneTable
from LiuXin_alpha.databases.caches.calibre.tables.many_many_tables.many_to_many_table import CalibreManyToManyTable


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

    try:
        prov = provision(name="test_db_0", dst_dir=tmp_path)
    except TypeError:
        prov = provision(name="test_db_0")

    db = Database(metadata={"database_path": str(prov.db_path)})

    # CalibreCache expects these attributes to exist on the backend
    db.tables = {}
    db.conn = db.driver_wrapper.lock  # used as `with backend.conn:`
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
    """A fully initialized CalibreCache instance."""
    cache = CalibreCache(backend=calibre_backend_db)
    cache.init()
    return cache


# -------------------------------------------------------------------------------------------------
# Helpers


def _refresh_backend_custom_columns(db: Database) -> None:
    """
    Ensure db.custom_columns exists and has populated FieldMetadata entries for custom fields.
    """
    # Do not pass a connection object; CustomColumns resolves a live one from db.driver.conn.
    db.custom_columns = CustomColumns(db=db, field_metadata=db.field_metadata)


def _create_custom_column(
    db: Database,
    *,
    label: str,
    datatype: str,
    is_multiple: bool = False,
    display: Optional[dict] = None,
    name: Optional[str] = None,
) -> int:
    """
    Create a calibre-style custom column (row in custom_columns + underlying tables),
    then refresh db metadata so CalibreCache.initialize_custom_columns can see those tables.
    """
    # Use a short-lived CustomColumns instance for creation; it doesn't auto-refresh FieldMetadata afterwards.
    # Do not pass a connection object; CustomColumns resolves a live one from db.driver.conn.
    cc = CustomColumns(db=db, field_metadata=db.field_metadata)

    num = cc.create_custom_column(
        label=label,
        name=name or f"UT {label}",
        datatype=datatype,
        is_multiple=is_multiple,
        display=display or {},
        editable=True,
        table="books",
        make_category=True,
    )

    # IMPORTANT: Database caches all_tables/custom_tables; refresh so cache.initialize_custom_columns doesn't
    # think the tables are missing and delete the record.
    db.refresh_db_metadata()

    # Rebuild the CustomColumns helper so FieldMetadata gets the new custom field entries.
    _refresh_backend_custom_columns(db)

    return int(num)


def _temp_trigger_names(db: Database) -> set[str]:
    rows = db.driver_wrapper.execute(
        "SELECT name FROM sqlite_temp_master WHERE type='trigger'"
    )
    # driver_wrapper.execute may return iterator/rows depending on driver; normalize
    out = set()
    for r in rows:
        if isinstance(r, dict):
            out.add(r.get("name"))
        else:
            try:
                out.add(r[0])
            except Exception:
                pass
    out.discard(None)
    return out


# -------------------------------------------------------------------------------------------------
# Step 03 tests


def test_initialize_custom_columns_builds_maps_seps_trigger_and_adapters(calibre_backend_db):
    db = calibre_backend_db

    # Create one multi-value text custom column, marked as "names"
    num = _create_custom_column(
        db,
        label="ut_names",
        datatype="text",
        is_multiple=True,
        display={"is_names": True},
        name="UT Names",
    )

    cache = CalibreCache(backend=db)

    # Minimal startup path (avoid full init if you only want bootstrap behaviour)
    cache._do_backend_prefs_startup()
    cache.initialize_custom_columns()

    assert "ut_names" in db.custom_column_label_map
    data = db.custom_column_label_map["ut_names"]
    assert data["num"] == num
    assert data["datatype"] == "text"
    assert data["is_multiple"] is True
    assert data["display"].get("is_names") is True

    # The "names" mode should use '&' as ui_to_list
    assert data["multiple_seps"]["ui_to_list"] == "&"
    assert data["multiple_seps"]["list_to_ui"] == " & "

    # Adapters should exist for core custom datatypes
    assert isinstance(db.custom_data_adapters, dict)
    for k in ("text", "comments", "datetime", "int", "float", "bool", "rating", "enumeration", "series"):
        assert k in db.custom_data_adapters

    # A normalized custom column should cause the TEMP delete trigger to exist
    triggers = _temp_trigger_names(db)
    assert "custom_books_delete_trg" in triggers


def test_initialize_tables_registers_custom_columns_with_expected_table_classes(calibre_backend_db):
    db = calibre_backend_db

    # Many-to-many (normalized + multiple)
    num_tags = _create_custom_column(
        db,
        label="ut_tags",
        datatype="text",
        is_multiple=True,
        display={},
        name="UT Tags",
    )

    # Many-to-one (normalized + single)
    num_pub = _create_custom_column(
        db,
        label="ut_pub",
        datatype="text",
        is_multiple=False,
        display={},
        name="UT Publisher-ish",
    )

    # Series (normalized + single, plus *_index)
    num_series = _create_custom_column(
        db,
        label="ut_series",
        datatype="series",
        is_multiple=False,
        display={},
        name="UT Series",
    )

    # Non-normalized one-to-one
    num_notes = _create_custom_column(
        db,
        label="ut_notes",
        datatype="comments",
        is_multiple=False,
        display={},
        name="UT Notes",
    )

    cache = CalibreCache(backend=db)
    cache._do_backend_prefs_startup()
    cache.initialize_custom_columns()
    cache.initialize_tables()

    # Field keys for custom columns are prefixed with '#'
    assert "#ut_tags" in cache.tables
    assert "#ut_pub" in cache.tables
    assert "#ut_series" in cache.tables
    assert "#ut_notes" in cache.tables

    assert isinstance(cache.tables["#ut_tags"], CalibreManyToManyTable)
    assert isinstance(cache.tables["#ut_pub"], CalibreManyToOneTable)
    assert isinstance(cache.tables["#ut_notes"], CalibreOneToOneTable)

    # Link table naming should be calibre-style for normalized columns
    assert cache.tables["#ut_tags"].link_table == f"books_custom_column_{num_tags}_link"
    assert cache.tables["#ut_pub"].link_table == f"books_custom_column_{num_pub}_link"

    # Series should have an index table
    assert "#ut_series_index" in cache.tables
    assert isinstance(cache.tables["#ut_series_index"], CalibreOneToOneTable)
    assert cache.tables["#ut_series_index"].metadata["table"] == f"books_custom_column_{num_series}_link"
    assert cache.tables["#ut_series_index"].metadata["column"] == "extra"


def test_mark_for_delete_drops_tables_deletes_row_and_sets_pref(calibre_backend_db):
    db = calibre_backend_db

    num = _create_custom_column(
        db,
        label="ut_todelete",
        datatype="text",
        is_multiple=True,  # normalized + link table
        display={},
        name="UT To Delete",
    )

    table, link_table = db.custom_table_names(num)
    assert table in db.all_tables
    assert link_table in db.all_tables
    assert table in db.custom_tables
    assert link_table in db.custom_tables

    # Mark for delete
    db.driver_wrapper.execute(
        "UPDATE custom_columns SET custom_column_mark_for_delete=1 WHERE custom_column_id=?",
        (num,),
    )

    cache = CalibreCache(backend=db)
    cache._do_backend_prefs_startup()
    cache.initialize_custom_columns()

    # Row removed
    rows = list(
        db.driver_wrapper.search(
            table="custom_columns",
            column="custom_column_id",
            search_term=num,
        )
    )
    assert not rows, "custom_columns row should be deleted when marked for delete"

    # Tables dropped (and removed from metadata sets that cache mutates)
    db.refresh_db_metadata()
    assert table not in db.all_tables
    assert link_table not in db.all_tables

    # Pref flag should be set (cache/init uses this later)
    assert db.prefs["update_all_last_mod_dates_on_start"] is True


def test_orphaned_custom_column_record_is_removed(calibre_backend_db):
    db = calibre_backend_db

    orphan_id = 9001
    orphan_label = "ut_orphan"

    # Insert a custom_columns record WITHOUT creating the backing tables.
    db.driver_wrapper.execute(
        """
        INSERT INTO custom_columns(
            custom_column_id,
            custom_column_label,
            custom_column_name,
            custom_column_datatype,
            custom_column_is_multiple,
            custom_column_editable,
            custom_column_display,
            custom_column_normalized,
            custom_column_display_sort,
            custom_column_in_table,
            custom_column_mark_for_delete
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            orphan_id,
            orphan_label,
            "UT Orphan",
            "text",
            1,
            1,
            json.dumps({}),
            1,
            0,
            "books",
            0,
        ),
    )

    cache = CalibreCache(backend=db)
    cache._do_backend_prefs_startup()
    cache.initialize_custom_columns()

    # initialize_custom_columns should detect missing tables and delete the record
    rows = list(
        db.driver_wrapper.search(
            table="custom_columns",
            column="custom_column_id",
            search_term=orphan_id,
        )
    )
    assert not rows, "orphaned custom column record should be removed"


if __name__ == "__main__":
    raise SystemExit("Run with pytest, not as a script.")

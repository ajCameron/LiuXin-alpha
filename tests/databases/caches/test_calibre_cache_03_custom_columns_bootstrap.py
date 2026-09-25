"""
Check legacy custom-column bootstrap, table registration, and cleanup.

These tests require LIUXIN_ENABLE_LEGACY_CALIBRE_CACHE_TESTS to be truthy and retain
assumptions about the deprecated Calibre-shaped schema. They skip at module import
by default under the FRBR-first schema. Enabling the gate does not make the fixture
schema compatible.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/caches/test_calibre_cache_03_custom_columns_bootstrap.py
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

from LiuXin_alpha.library.caches.calibre.cache import CalibreCache
from LiuXin_alpha.databases.custom_columns import CustomColumns
from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.catalog.field_metadata import FieldMetadata

from LiuXin_alpha.library.caches.calibre.tables.one_one_tables import CalibreOneToOneTable
from LiuXin_alpha.library.caches.calibre.tables.many_one_tables.many_to_one_table import CalibreManyToOneTable
from LiuXin_alpha.library.caches.calibre.tables.many_many_tables.many_to_many_table import CalibreManyToManyTable


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

            python -m pytest -q tests/databases/caches/test_calibre_cache_03_custom_columns_bootstrap.py
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

            python -m pytest -q tests/databases/caches/test_calibre_cache_03_custom_columns_bootstrap.py


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

            python -m pytest -q tests/databases/caches/test_calibre_cache_03_custom_columns_bootstrap.py


    :param tmp_path: Pytest-provided temporary directory for isolated database or TOML
        files.
    :param request: Pytest request used to resolve a provisioning fixture by name.
    :return: Database configured as a legacy CalibreCache backend.
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
        """
        Merge truthy defaults, materialize missing explicit keys, and optionally report the defaults count.

        Existing explicit preferences remain unchanged. For any non-None defaults, call a
        callable progress callback with (None, len(default_prefs)); ignore the restore flag.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/caches/test_calibre_cache_03_custom_columns_bootstrap.py


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

            python -m pytest -q tests/databases/caches/test_calibre_cache_03_custom_columns_bootstrap.py


    :param calibre_backend_db: Provisioned Database carrying the legacy cache backend
        shims; the local fixture has no close finalizer.
    :return: Initialized CalibreCache; setup errors propagate.
    """
    cache = CalibreCache(backend=calibre_backend_db)
    cache.init()
    return cache


# -------------------------------------------------------------------------------------------------
# Helpers


def _refresh_backend_custom_columns(db: Database) -> None:
    """
    Replace db.custom_columns with a fresh helper that populates the supplied FieldMetadata.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_calibre_cache_03_custom_columns_bootstrap.py


    :param db: Caller-owned Database to inspect or mutate; this helper does not close
        it.
    :return: None; updates the caller-owned database helper and metadata.
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
    Create an editable custom column, refresh database metadata, and rebuild the backend custom-column helper.

    Creation mutates the database and propagates errors; the caller owns the database
    connection. The column belongs to books and requests category exposure.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_calibre_cache_03_custom_columns_bootstrap.py


    :param db: Caller-owned Database to inspect or mutate; this helper does not close
        it.
    :param label: Internal custom-column label.
    :param datatype: Custom-column datatype passed to the creation API.
    :param is_multiple: Whether the column stores multiple values.
    :param display: Display mapping; false values are replaced by an empty dictionary.
    :param name: Display name; false values fall back to UT plus the label.
    :return: Integer ID returned by custom-column creation.
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
    """
    Read temporary SQLite trigger names from mapping or positional result rows.

    Ignore positional extraction errors and remove None names; query errors propagate.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_calibre_cache_03_custom_columns_bootstrap.py


    :param db: Caller-owned Database to inspect or mutate; this helper does not close
        it.
    :return: Set of extracted trigger names.
    """
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
    """
    Check custom label metadata, name separators, required datatype adapters, and the temporary book-delete trigger.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_calibre_cache_03_custom_columns_bootstrap.py::test_initialize_custom_columns_builds_maps_seps_trigger_and_adapters


    :param calibre_backend_db: Provisioned Database carrying the legacy cache backend
        shims; the local fixture has no close finalizer.
    :return: None; failed expectations raise AssertionError.
    """
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
    """
    Check custom field keys, selected relation-table classes, link-table names, and series-index metadata.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_calibre_cache_03_custom_columns_bootstrap.py::test_initialize_tables_registers_custom_columns_with_expected_table_classes


    :param calibre_backend_db: Provisioned Database carrying the legacy cache backend
        shims; the local fixture has no close finalizer.
    :return: None; failed expectations raise AssertionError.
    """
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
    """
    Mark a normalized custom column for deletion and check its metadata row and backing tables disappear and the last-modified preference is set.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_calibre_cache_03_custom_columns_bootstrap.py::test_mark_for_delete_drops_tables_deletes_row_and_sets_pref


    :param calibre_backend_db: Provisioned Database carrying the legacy cache backend
        shims; the local fixture has no close finalizer.
    :return: None; failed expectations raise AssertionError.
    """
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
    """
    Insert custom-column metadata without its backing tables and check bootstrap removes the orphaned row.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_calibre_cache_03_custom_columns_bootstrap.py::test_orphaned_custom_column_record_is_removed


    :param calibre_backend_db: Provisioned Database carrying the legacy cache backend
        shims; the local fixture has no close finalizer.
    :return: None; failed expectations raise AssertionError.
    """
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

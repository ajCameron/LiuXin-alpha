"""
Check legacy tag-browser visibility for custom and composite fields.

These tests require LIUXIN_ENABLE_LEGACY_CALIBRE_CACHE_TESTS to be truthy and retain
assumptions about the deprecated Calibre-shaped schema. They skip at module import
by default under the FRBR-first schema. Enabling the gate does not make the fixture
schema compatible.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/caches/test_calibre_cache_04_categories.py
"""

from __future__ import annotations

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

            python -m pytest -q tests/databases/caches/test_calibre_cache_04_categories.py
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

            python -m pytest -q tests/databases/caches/test_calibre_cache_04_categories.py


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

            python -m pytest -q tests/databases/caches/test_calibre_cache_04_categories.py


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

    # prefs the cache reads immediately + ones used by categories.py
    db.prefs = TestPrefs(
        defaults={
            "bools_are_tristate": False,
            "update_all_last_mod_dates_on_start": False,
            "metadata_backup_interval": 0,
            # Category-related prefs (safe defaults; categories.py uses .pref(..., default))
            "user_categories": {},
            "grouped_search_make_user_categories": [],
            "grouped_search_terms": {},
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

                python -m pytest -q tests/databases/caches/test_calibre_cache_04_categories.py


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


# -------------------------------------------------------------------------------------------------
# Helpers


def _refresh_backend_custom_columns(db: Database) -> None:
    """
    Replace db.custom_columns with a fresh helper that populates the supplied FieldMetadata.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_calibre_cache_04_categories.py


    :param db: Caller-owned Database to inspect or mutate; this helper does not close
        it.
    :return: None; updates the caller-owned database helper and metadata.
    """
    db.custom_columns = CustomColumns(db=db, field_metadata=db.field_metadata)


def _create_custom_column(
    db: Database,
    *,
    label: str,
    datatype: str,
    is_multiple: bool = False,
    display: Optional[dict] = None,
    name: Optional[str] = None,
    table: str = "books",
    make_category: bool = True,
) -> int:
    """
    Create an editable custom column, refresh database metadata, and rebuild the backend custom-column helper.

    Creation mutates the database and propagates errors; the caller owns the database
    connection. The caller chooses its owning relation and category flag.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_calibre_cache_04_categories.py


    :param db: Caller-owned Database to inspect or mutate; this helper does not close
        it.
    :param label: Internal custom-column label.
    :param datatype: Custom-column datatype passed to the creation API.
    :param is_multiple: Whether the column stores multiple values.
    :param display: Display mapping; false values are replaced by an empty dictionary.
    :param name: Display name; false values fall back to UT plus the label.
    :param table: Owning relation, defaulting to books.
    :param make_category: Whether to request category exposure, defaulting to True.
    :return: Integer ID returned by custom-column creation.
    """
    cc = CustomColumns(db=db, field_metadata=db.field_metadata)

    num = cc.create_custom_column(
        label=label,
        name=name or f"UT {label}",
        datatype=datatype,
        is_multiple=is_multiple,
        display=display or {},
        editable=True,
        table=table,
        make_category=make_category,
    )

    # Database caches all_tables/custom_tables; refresh so cache/init doesn't think tables are missing.
    db.refresh_db_metadata()
    _refresh_backend_custom_columns(db)

    return int(num)


def _insert_minimal_book(db, title: str = "Ganymede") -> int:
    """
    Insert a linked work, expression, manifestation, and digital item for category computation.

    Write FRBR rows directly through the wrapper using SQLite last-insert IDs; do not
    explicitly commit or close the caller-owned database.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_calibre_cache_04_categories.py


    :param db: Caller-owned Database to inspect or mutate; this helper does not close
        it.
    :param title: Work title used verbatim for title/canonical title and lowercased for
        sort title; defaults to Ganymede.
    :return: Integer ID of the new work.
    """

    def _insert_and_lastrowid(sql: str, params: tuple) -> int:
        """
        Execute an insert and retrieve SQLite last_insert_rowid through a cursor or iterator fallback.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/caches/test_calibre_cache_04_categories.py


        :param sql: Insert SQL passed to the enclosing database wrapper.
        :param params: Bound parameter tuple for the insert.
        :return: Integer ID from the first result cell; empty or incompatible results raise.
        """
        db.driver_wrapper.execute(sql, params)
        cur = db.driver_wrapper.execute("SELECT last_insert_rowid();")
        try:
            row = cur.fetchone()
        except Exception:
            row = next(iter(cur), None)
        return int(row[0])

    # Work
    work_id = _insert_and_lastrowid(
        "INSERT INTO works (work_title, work_canonical_title, work_sort_title) VALUES (?, ?, ?);",
        (title, title, title.lower()),
    )

    # Expression
    expression_id = _insert_and_lastrowid(
        "INSERT INTO expressions (expression_label, expression_mode, expression_is_preferred) VALUES (?, ?, ?);",
        ("Default", "text", 1),
    )

    # Manifestation
    manifestation_id = _insert_and_lastrowid(
        "INSERT INTO manifestations (manifestation_carrier_type, manifestation_format_detail, manifestation_pub_year) "
        "VALUES (?, ?, ?);",
        ("ebook", "EPUB", 2000),
    )

    # Links
    db.driver_wrapper.execute(
        "INSERT INTO expression_work_links "
        "(expression_work_link_expression_id, expression_work_link_work_id, expression_work_link_priority, "
        "expression_work_link_primary, expression_work_link_origin) "
        "VALUES (?, ?, ?, ?, ?);",
        (expression_id, work_id, 1, 1, "tests"),
    )
    db.driver_wrapper.execute(
        "INSERT INTO expression_manifestation_links "
        "(expression_manifestation_link_expression_id, expression_manifestation_link_manifestation_id, "
        "expression_manifestation_link_priority, expression_manifestation_link_primary, expression_manifestation_link_origin) "
        "VALUES (?, ?, ?, ?, ?);",
        (expression_id, manifestation_id, 1, 1, "tests"),
    )

    # Item
    _insert_and_lastrowid(
        "INSERT INTO items (item_manifestation_id, item_type, item_source, item_source_detail) VALUES (?, ?, ?, ?);",
        (manifestation_id, "digital", "tests", "seed"),
    )

    return work_id



def _cat_name(x: Any) -> str:
    """
    Normalize a category item using its string value, non-None name attribute, or str fallback.

    Example:
        >>> _cat_name('Ganymede')
        'Ganymede'
        >>> _cat_name(7)
        '7'


    :param x: String or category-like object to normalize.
    :return: Comparable string representation.
    """
    if isinstance(x, str):
        return x
    n = getattr(x, "name", None)
    if n is not None:
        return str(n)
    return str(x)


# -------------------------------------------------------------------------------------------------
# Step 04 tests


def test_get_categories_includes_custom_column_when_make_category_true(calibre_backend_db):
    """
    Check a books custom category appears under its hash-prefixed label with a list value.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_calibre_cache_04_categories.py::test_get_categories_includes_custom_column_when_make_category_true


    :param calibre_backend_db: Provisioned Database carrying the legacy cache backend
        shims; the local fixture has no close finalizer.
    :return: None; failed expectations raise AssertionError.
    """
    db = calibre_backend_db

    _create_custom_column(
        db,
        label="ut_tags",
        datatype="text",
        is_multiple=True,
        display={},
        table="books",
        make_category=True,
        name="UT Tags",
    )

    cache = CalibreCache(backend=db)
    cache.init()

    cats = cache.get_categories(sort="name")

    # FieldMetadata custom fields are keyed with prefix "#"
    assert "#ut_tags" in cats
    assert isinstance(cats["#ut_tags"], list)


def test_get_categories_excludes_custom_columns_attached_to_non_books_tables(calibre_backend_db):
    """
    Check a category-enabled custom column on titles is absent from the legacy books tag browser.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_calibre_cache_04_categories.py::test_get_categories_excludes_custom_columns_attached_to_non_books_tables


    :param calibre_backend_db: Provisioned Database carrying the legacy cache backend
        shims; the local fixture has no close finalizer.
    :return: None; failed expectations raise AssertionError.
    """
    db = calibre_backend_db

    # Attach a custom column to 'titles' (non-books). It should not appear in Tag Browser categories.
    _create_custom_column(
        db,
        label="ut_titles_only",
        datatype="text",
        is_multiple=False,
        display={},
        table="titles",
        make_category=True,
        name="UT Titles Only",
    )

    cache = CalibreCache(backend=db)
    cache.init()

    cats = cache.get_categories(sort="name")
    assert "#ut_titles_only" not in cats


def test_composite_custom_column_respects_make_category_and_can_generate_category_values(calibre_backend_db):
    """
    Check the category flag controls composite visibility and the enabled title template yields Ganymede.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/caches/test_calibre_cache_04_categories.py::test_composite_custom_column_respects_make_category_and_can_generate_category_values


    :param calibre_backend_db: Provisioned Database carrying the legacy cache backend
        shims; the local fixture has no close finalizer.
    :return: None; failed expectations raise AssertionError.
    """
    db = calibre_backend_db

    # Ensure at least one book exists so composite categories have something to compute.
    _insert_minimal_book(db, title="Ganymede")

    # Composite shown as category
    _create_custom_column(
        db,
        label="ut_comp_cat",
        datatype="composite",
        is_multiple=False,
        display={"composite_template": "{title}"},
        table="books",
        make_category=True,
        name="UT Composite Category",
    )

    # Composite NOT shown as category
    _create_custom_column(
        db,
        label="ut_comp_nocat",
        datatype="composite",
        is_multiple=False,
        display={"composite_template": "{title}"},
        table="books",
        make_category=False,
        name="UT Composite NoCat",
    )

    cache = CalibreCache(backend=db)
    cache.init()

    cats = cache.get_categories(sort="name")

    assert "#ut_comp_cat" in cats
    assert "#ut_comp_nocat" not in cats

    # Composite category should yield at least one value (the title from the template).
    values = cats["#ut_comp_cat"]
    assert isinstance(values, list)
    assert any(_cat_name(t) == "Ganymede" for t in values)

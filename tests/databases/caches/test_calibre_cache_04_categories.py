"""
Step 04: Tag Browser "categories" (CalibreCache.get_categories)

Drop into:
    tests/databases/caches/test_calibre_cache_04_categories.py

What this covers:
- Built-in + custom columns that are "categories" appear as keys in CalibreCache.get_categories().
- Custom columns attached to non-books tables are NOT exposed as tag-browser categories.
- Composite custom columns only appear as categories when make_category=True (and can yield values).

This step intentionally focuses on *category exposure rules* rather than deep correctness of
per-field category generation (that can come later).
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Optional

import pytest

# --- Make bundled libs importable (liuxin_dateutil is in utils/libraries) ---
_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_LIBS = _PROJECT_ROOT / "src" / "LiuXin_alpha" / "utils" / "libraries"
if _LIBS.exists() and str(_LIBS) not in sys.path:
    sys.path.insert(0, str(_LIBS))

from LiuXin_alpha.databases.caches.calibre.cache import CalibreCache
from LiuXin_alpha.databases.custom_columns import CustomColumns
from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.databases.field_metadata import FieldMetadata


class TestPrefs(dict):
    """
    Tiny prefs shim used by CalibreCache during init() and get_categories().
    """

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
    """
    Minimal fsm used by formats/covers/path tables during init().
    """

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
    Support both fixture spellings used across the repo.
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
    Ensure db.custom_columns exists and has populated FieldMetadata entries for custom fields.
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
    Create a calibre-style custom column (row in custom_columns + underlying tables),
    then refresh db metadata so CalibreCache can see those tables/fields.
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
    Insert a minimal book row suitable for CalibreCache initialization + composite templates.

    LiuXin minimal schema:
        books.book_id == titles.title_id (FK), and meta.title comes from titles.title

    calibre-style schema:
        books has a title column (books.title)
    """
    # Create a book row (in LiuXin schema, this also creates the matching titles row).
    book_row = db.driver_wrapper.get_blank_row("books")
    book_id_col = db.driver_wrapper.get_id_column("books")
    book_id = int(book_row[book_id_col])

    # Introspect columns so we don't emit invalid SQL (avoids noisy log output).
    driver = getattr(db, "driver", None) or getattr(getattr(db, "driver_wrapper", None), "driver", None)
    tables_cols = {}
    if driver is not None and hasattr(driver, "direct_get_tables_and_columns"):
        tables_cols = driver.direct_get_tables_and_columns()

    books_cols = set(tables_cols.get("books", []))
    titles_cols = set(tables_cols.get("titles", []))

    # Prefer titles.title when present (LiuXin schema; meta.title sources it).
    if "title" in titles_cols:
        title_id_col = db.driver_wrapper.get_id_column("titles")
        db.driver_wrapper.execute(
            f"UPDATE titles SET title=? WHERE {title_id_col}=?",
            (title, book_id),
        )
        # Keep sort aligned if present.
        if "title_sort" in titles_cols:
            db.driver_wrapper.execute(
                f"UPDATE titles SET title_sort=? WHERE {title_id_col}=?",
                (title, book_id),
            )

    # calibre-style schema
    elif "title" in books_cols:
        db.driver_wrapper.execute(
            f"UPDATE books SET title=? WHERE {book_id_col}=?",
            (title, book_id),
        )

    return book_id



def _cat_name(x: Any) -> str:
    """
    Category items can be Tag-like objects (with .name) or plain strings.
    Normalize to a comparable string.
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

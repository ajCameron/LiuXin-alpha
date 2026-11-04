"""Keep compatibility projections and unsafe cache operations out of table SQL."""

import ast
import tokenize
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import pytest

from LiuXin_alpha.caches.write.covers_writer import CoversWrite
from LiuXin_alpha.caches.write.generic_writers.many_to_many_writer import (
    ManyToManyWriter,
)
from LiuXin_alpha.caches.write.generic_writers.one_to_one_writer import OneToOneWriter
from LiuXin_alpha.catalog.catalog import Catalog
from LiuXin_alpha.databases import metadata_sql
from LiuXin_alpha.databases.api import MetadataSQLAPI
from LiuXin_alpha.library.caches.calibre.tables.many_many_tables.specific_many_to_many_tables import (
    CalibreAuthorsTable,
    CalibreFormatsTable,
)
from LiuXin_alpha.library.caches.calibre.tables.one_one_tables import (
    CalibreCoversTable,
    CalibreSizeTable,
)

ROOT = Path(__file__).resolve().parents[2]
RETIRED_METHODS = (
    "add_creator_tag_link",
    "add_series_tag_link",
    "add_tag_title_link",
    "add_title_identifier",
    "break_creator_tag_link",
    "break_creator_title_links",
    "break_generic_link",
    "break_generic_single_link",
    "break_lang_title_links",
    "break_lang_title_primary_link",
    "break_series_title_link",
    "break_tag_title_link",
    "check_for_creator_tag_link",
    "check_for_series_tag_link",
    "check_for_series_title_link",
    "check_for_tag_title_link",
    "check_for_title_author_link",
    "check_for_title_id_publisher_id_link",
    "clear_creator_tag_links_for_creator",
    "clear_null_publisher_links_from_title",
    "clear_publisher_title_links_by_title_id",
    "clear_series_tag_links_for_series",
    "clear_tag_title_links_for_title",
    "clear_title_comments_from_title_id",
    "clear_title_creator_links_for_given_type_and_title",
    "creator_clear_unused",
    "delete_book",
    "delete_conversion_options",
    "delete_item_by_id",
    "delete_title",
    "delete_title_identifiers",
    "get_creator_link",
    "get_creator_sort",
    "get_primary_series_index",
    "get_title_series_ids_set",
    "library_unset_series",
    "link_null_series_to_title",
    "link_publisher_to_null_publisher_row",
    "make_creator_title_links",
    "publisher_clear_unused",
    "read_all_identifiers",
    "read_book_id_with_cover_id_and_cover_nmame",
    "read_book_id_with_file_id_file_ext_file_name_and_file_size",
    "read_book_sizes_max_mode",
    "read_book_sizes_min_mode",
    "read_book_sizes_sum_mode",
    "read_creator_with_sort_and_link",
    "read_file_backups_for_book",
    "read_file_properties_for_book",
    "read_primary_title_series_id_from_meta",
    "remove_unused_series",
    "replace_in_cover_path",
    "replace_in_file_path",
    "replace_in_folder_path",
    "replace_in_folder_store_marker_path",
    "replace_in_folder_store_path",
    "set_author_sort",
    "set_conversion_options",
    "set_has_cover",
    "set_override_book_path",
    "set_title_identifier",
    "set_title_isbn",
    "set_title_primary_language",
    "set_title_rating",
    "unapply_series_tags",
    "update_book_last_modified",
    "update_creator_links",
    "update_creator_sorts",
    "update_index_for_series_title_link",
    "update_title",
    "update_title_author_link_priority",
    "update_title_creator_sort",
)
RETIRED_MODULES = {
    "books_mixin",
    "titles_mixin",
    "title_values_mixin",
    "series_title_links_mixin",
}


def test_metadata_sql_retains_table_helpers_without_compatibility_facades(db):
    source = ROOT / "src/LiuXin_alpha"
    for name in RETIRED_MODULES:
        assert not (source / "databases/metadata_sql" / (name + ".py")).exists()
    assert not (source / "catalog/catalog_macros.py").exists()
    assert "CMTitlesMacrosMixin" not in metadata_sql.__all__
    # The physical Folders reader replaces the old path-rewriting mixin.
    assert "FoldersSQLMixin" in metadata_sql.__all__
    assert "FoldersMacrosMixin" not in metadata_sql.__all__
    for name in RETIRED_METHODS:
        assert not hasattr(db.metadata_sql, name), name
        assert not hasattr(MetadataSQLAPI, name), name


def test_no_production_dependency_on_retired_metadata_sql_operations():
    for path in (ROOT / "src/LiuXin_alpha").rglob("*.py"):
        with tokenize.open(path) as handle:
            source = handle.read()
        if "metadata_sql" not in source:
            continue
        for node in ast.walk(ast.parse(source)):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                modules = [alias.name for alias in node.names]
                if isinstance(node, ast.ImportFrom):
                    modules.append(node.module or "")
                assert not any(
                    "metadata_sql" in module.split(".")
                    and set(module.split(".")) & RETIRED_MODULES
                    for module in modules
                ), (path, node.lineno)
            elif (
                isinstance(node, ast.Attribute)
                and node.attr in RETIRED_METHODS
                and isinstance(node.value, ast.Attribute)
                and node.value.attr == "metadata_sql"
            ):
                pytest.fail(f"{path}:{node.lineno}: retired projection helper")


class NoDatabaseAccess:
    def __getattr__(self, name):
        raise AssertionError(f"unsupported operation accessed database: {name}")


@pytest.mark.parametrize(
    "method, arguments",
    [
        (CalibreSizeTable.read, ()),
        (CalibreCoversTable.read_maps, ()),
        (CalibreAuthorsTable.read_id_maps, ()),
        (CalibreFormatsTable.read_maps, ()),
        (CalibreFormatsTable.reload_book_from_db, (7,)),
    ],
)
def test_retired_cache_reads_preserve_existing_maps(method, arguments):
    cache = SimpleNamespace(
        book_col_map={7: [3]},
        asort_map={3: "Author"},
        fname_map={7: {"EPUB": "keep.epub"}},
    )
    before = deepcopy(vars(cache))
    with pytest.raises(NotImplementedError, match="retired"):
        method(cache, NoDatabaseAccess(), *arguments)
    assert vars(cache) == before


@pytest.mark.parametrize(
    "method", [CalibreAuthorsTable.set_links, CalibreAuthorsTable.set_sort_names]
)
def test_retired_creator_writes_preserve_existing_maps(method):
    cache = SimpleNamespace(asort_map={3: "Original"}, alink_map={3: "Original"})
    before = deepcopy(vars(cache))
    with pytest.raises(NotImplementedError, match="retired"):
        method(cache, {3: "Changed"}, NoDatabaseAccess())
    assert vars(cache) == before


@pytest.mark.parametrize(
    "method",
    [
        ManyToManyWriter.do_creators_many_many_clear_unused,
        ManyToManyWriter.do_publisher_many_one_clear_unused,
    ],
)
def test_role_specific_cleanup_requires_an_agent_retention_policy(method):
    with pytest.raises(NotImplementedError, match="every role"):
        method(NoDatabaseAccess(), None, None)


def test_series_position_is_explicitly_unsupported():
    with pytest.raises(NotImplementedError, match="sequence-position"):
        OneToOneWriter.series_index_one_one_db_updater(
            NoDatabaseAccess(), {7: 2.5}, None, None
        )


def test_series_memberships_use_current_links_and_compose_with_rollback(db):
    catalog = Catalog(db)
    first = catalog.works.create({"title": "First work"})
    other = catalog.works.create({"title": "Other work"})
    series = [catalog.series.create({"name": name}) for name in ("One", "Two", "Keep")]
    write = ManyToManyWriter.do_series_many_many_db_update

    def memberships():
        return {
            (row["series_work_link_work_id"], row["series_work_link_series_id"])
            for row in db.macros.get_rows("series_work_links")
        }

    assert write(db, updated={first: series[:2], other: series[2]}) == (None, None)
    expected = {(first, series[0]), (first, series[1]), (other, series[2])}
    assert memberships() == expected
    with pytest.raises(RuntimeError, match="rollback"):
        with db.macros.transaction():
            write(db, updated={first: series[2]}, deleted=[other])
            assert memberships() == {(first, series[2])}
            raise RuntimeError("rollback")
    assert memberships() == expected
    write(db, updated={first: series[2]}, deleted=[first])
    assert memberships() == {(other, series[2])}
    write(db, deleted=[other])
    assert memberships() == set()
    assert all(catalog.series.require(series_id) for series_id in series)


def test_format_removal_refuses_before_deleting_rows_or_mutating_cache():
    cache = SimpleNamespace(book_file_map={7: {"EPUB_1": 3}})
    before = deepcopy(vars(cache))
    with pytest.raises(NotImplementedError, match="retired"):
        CalibreFormatsTable.remove_formats(cache, {7: ["EPUB_1"]}, NoDatabaseAccess())
    assert vars(cache) == before


@pytest.mark.parametrize("value", [True, False])
def test_cover_flags_refuse_before_accessing_database_or_field(value):
    with pytest.raises(NotImplementedError, match="retired"):
        CoversWrite.set_cover_exists({7: value}, NoDatabaseAccess(), NoDatabaseAccess())
    assert (
        CoversWrite.set_cover_exists({}, NoDatabaseAccess(), NoDatabaseAccess())
        == set()
    )

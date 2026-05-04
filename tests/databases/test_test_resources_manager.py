from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from LiuXin_alpha.databases.database import Database
from tests.support import test_resources_manager as trm


ALL_TEST_DB_NAMES = tuple(f"test_db_{i}" for i in range(26))
ASSET_HEAVY_DB_NAMES = ("test_db_3", "test_db_5", "test_db_11")
BENCHMARK_DB_NAMES = ("benchmark_db_smoke", "benchmark_db_medium", "benchmark_db_large")
SEMANTIC_DB_NAMES = (
    "metadata_rich_db_0",
    "metadata_rich_db_1",
    "stores_assets_db_0",
    "stores_assets_db_1",
    "images_covers_db_0",
    "images_covers_db_1",
    "custom_columns_populated_db_0",
    "custom_columns_populated_db_1",
    "identifiers_db_0",
    "identifiers_db_1",
    "pathological_relations_db_0",
    "weird_data_db_0",
)


def _relation_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type IN ('table', 'view') AND name = ? LIMIT 1;",
        (name,),
    ).fetchone()
    return row is not None


def _count_relation(conn: sqlite3.Connection, name: str) -> int:
    if not _relation_exists(conn, name):
        return 0
    return int(conn.execute(f"SELECT COUNT(*) FROM {name};").fetchone()[0])


def _expected_book_count(name: str) -> int:
    if name == "test_db_0":
        return 0
    if name == "test_db_2":
        return 1
    if name == "test_db_3":
        return 1
    if name == "test_db_13":
        return 0

    profile = trm._legacy_test_db_profiles().get(name, {"books": 12})
    return int(profile.get("books", 12))


def _expected_title_count(name: str) -> int:
    if name == "test_db_0":
        return 1
    return _expected_book_count(name)


def _expected_asset_counts(name: str) -> tuple[int, int]:
    if name == "test_db_3":
        return 497, 2440
    if name in {"test_db_5", "test_db_11"}:
        profile = trm._legacy_test_db_profiles()[name]
        return int(profile["folders"]), int(profile["files"])
    return 0, 0


def _count_titles(db_path: Path) -> int:
    conn = sqlite3.connect(str(db_path))
    try:
        row = conn.execute("SELECT COUNT(*) FROM titles;").fetchone()
        assert row is not None
        return int(row[0])
    finally:
        conn.close()


def test_resources_manager_lists_default_dbs(test_resources_manager) -> None:
    names = test_resources_manager.available_test_databases()
    assert "test_db_0" in names
    assert "test_db_2" in names
    assert "test_db_3" in names
    assert "test_db_13" in names


def test_resources_manager_lists_full_legacy_range(test_resources_manager) -> None:
    names = set(test_resources_manager.available_test_databases())
    expected = set(ALL_TEST_DB_NAMES)
    assert expected.issubset(names)


def test_resources_manager_lists_benchmark_dbs(test_resources_manager) -> None:
    names = set(test_resources_manager.available_test_databases())
    assert set(BENCHMARK_DB_NAMES).issubset(names)


def test_resources_manager_lists_semantic_dbs_via_supported_imported_entrypoints(test_resources_manager) -> None:
    names = set(test_resources_manager.available_test_databases())
    assert set(SEMANTIC_DB_NAMES).issubset(names)
    assert "_semantic_fixture_builders" not in names
    assert "test_tree_generators" not in names
    assert "test_legacy_objects_smoke" not in names


def test_imported_provider_prefers_supported_semantic_modules_only(test_resources_manager) -> None:
    for name in SEMANTIC_DB_NAMES:
        assert type(test_resources_manager._db_registry.resolve(name)).__name__ == "ImportedModuleDatabaseProvider"
    assert type(test_resources_manager._db_registry.resolve("test_db_1")).__name__ == "BuiltinSpecDatabaseProvider"


def test_provisioned_database_opens(provision_test_database) -> None:
    provisioned = provision_test_database("test_db_0")

    # DB should be a valid sqlite file with expected schema.
    conn = sqlite3.connect(str(provisioned.db_path))
    try:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type IN ('table', 'view') AND name='titles' LIMIT 1;"
        ).fetchone()
        assert row is not None
    finally:
        conn.close()


def test_test_db_2_generates_and_is_pruned(provision_test_database) -> None:
    provisioned = provision_test_database("test_db_2")

    conn = sqlite3.connect(str(provisioned.db_path))
    try:
        # Should have the standard schema.
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type IN ('table', 'view') AND name='titles' LIMIT 1;"
        ).fetchone()
        assert row is not None

        # test_db_2 is expected to expose a single title/book projection.
        max_id = conn.execute("SELECT MAX(title_id) FROM titles;").fetchone()[0]
        assert int(max_id) == 1

        title_count = conn.execute("SELECT COUNT(*) FROM titles;").fetchone()[0]
        assert int(title_count) == 1

        book_count = conn.execute("SELECT COUNT(*) FROM books;").fetchone()[0]
        assert int(book_count) == 1

        tags = {
            str(row[0])
            for row in conn.execute("SELECT tag FROM tags ORDER BY tag;").fetchall()
        }
        assert tags == {
            trm.PROFILED_COMMON_TAG,
            trm.PROFILED_FIRST_TAG,
            trm.PROFILED_ODD_TAG,
            "test_db_2",
        }

        tag_link_count = conn.execute("SELECT COUNT(*) FROM tag_work_links;").fetchone()[0]
        assert int(tag_link_count) == 4

        tag_facets = {
            str(row[0])
            for row in conn.execute(
                "SELECT facet_text FROM subjects_tags_v WHERE facet_kind = 'tag';"
            ).fetchall()
        }
        assert tag_facets == tags
    finally:
        conn.close()


def test_test_db_3_generates_formats_fixture(provision_test_database) -> None:
    provisioned = provision_test_database("test_db_3")

    conn = sqlite3.connect(str(provisioned.db_path))
    try:
        book_count = int(conn.execute("SELECT COUNT(*) FROM books;").fetchone()[0])
        folder_count = _count_relation(conn, "folders")
        file_count = _count_relation(conn, "files")
        fwl_count = int(conn.execute("SELECT COUNT(*) FROM folder_work_links;").fetchone()[0])
        ffl_count = int(conn.execute("SELECT COUNT(*) FROM file_folder_links;").fetchone()[0])

        # These counts are deterministic.
        assert book_count == 1
        assert folder_count == 497
        assert file_count == 2440
        assert fwl_count == 497
        assert ffl_count == 2440

        ext_counts = dict(
            (row[0], int(row[1]))
            for row in conn.execute(
                "SELECT file_extension, COUNT(*) FROM files GROUP BY file_extension;"
            ).fetchall()
        )
        assert ext_counts == {"epub": 814, "mobi": 813, "pdf": 813}

        viol = conn.execute("PRAGMA foreign_key_check;").fetchall()
        assert viol == []
    finally:
        conn.close()


@pytest.mark.db
@pytest.mark.slow
def test_benchmark_db_smoke_generates_expected_shape(provision_test_database) -> None:
    provisioned = provision_test_database("benchmark_db_smoke")

    conn = sqlite3.connect(str(provisioned.db_path))
    try:
        book_count = int(conn.execute("SELECT COUNT(*) FROM books;").fetchone()[0])
        folder_count = _count_relation(conn, "folders")
        file_count = _count_relation(conn, "files")
        fwl_count = int(conn.execute("SELECT COUNT(*) FROM folder_work_links;").fetchone()[0])
        ffl_count = int(conn.execute("SELECT COUNT(*) FROM file_folder_links;").fetchone()[0])

        assert book_count == 250
        assert folder_count == 1000
        assert file_count == 4000
        assert fwl_count == 1000
        assert ffl_count == 4000

        viol = conn.execute("PRAGMA foreign_key_check;").fetchall()
        assert viol == []
    finally:
        conn.close()


@pytest.mark.db
@pytest.mark.slow
@pytest.mark.parametrize("db_name", ALL_TEST_DB_NAMES)
def test_all_test_db_profiles_smoke_and_shape(provision_test_database, db_name: str) -> None:
    provisioned = provision_test_database(db_name)
    conn = sqlite3.connect(str(provisioned.db_path))
    try:
        assert conn.execute("PRAGMA integrity_check;").fetchone()[0] == "ok"
        assert conn.execute("PRAGMA foreign_key_check;").fetchall() == []

        assert _relation_exists(conn, "titles")
        assert _relation_exists(conn, "books")
        assert _relation_exists(conn, "works")
        title_count = _count_relation(conn, "titles")
        book_count = _count_relation(conn, "books")
        folder_count = _count_relation(conn, "folders")
        file_count = _count_relation(conn, "files")

        exp_titles = _expected_title_count(db_name)
        exp_books = _expected_book_count(db_name)
        exp_folders, exp_files = _expected_asset_counts(db_name)

        if exp_folders > 0:
            assert _relation_exists(conn, "folders")
        if exp_files > 0:
            assert _relation_exists(conn, "files")

        assert title_count == exp_titles
        assert book_count == exp_books
        assert folder_count == exp_folders
        assert file_count == exp_files

        if exp_folders > 0:
            assert _relation_exists(conn, "folder_work_links")
            assert _relation_exists(conn, "file_folder_links")
            fwl_count = int(conn.execute("SELECT COUNT(*) FROM folder_work_links;").fetchone()[0])
            ffl_count = int(conn.execute("SELECT COUNT(*) FROM file_folder_links;").fetchone()[0])
            assert fwl_count == exp_folders
            assert ffl_count == exp_files
    finally:
        conn.close()


@pytest.mark.db
def test_semantic_asset_profile_partition(provision_test_database) -> None:
    asset_set = set(ASSET_HEAVY_DB_NAMES)
    all_file_counts: dict[str, int] = {}
    all_folder_counts: dict[str, int] = {}

    for db_name in ALL_TEST_DB_NAMES:
        provisioned = provision_test_database(db_name)
        conn = sqlite3.connect(str(provisioned.db_path))
        try:
            all_folder_counts[db_name] = _count_relation(conn, "folders")
            all_file_counts[db_name] = _count_relation(conn, "files")
        finally:
            conn.close()

    for db_name in asset_set:
        assert all_folder_counts[db_name] > 0
        assert all_file_counts[db_name] > 0

    for db_name in (set(ALL_TEST_DB_NAMES) - asset_set):
        assert all_folder_counts[db_name] == 0
        assert all_file_counts[db_name] == 0


@pytest.mark.db
@pytest.mark.parametrize(
    ("db_name", "expected_folders", "expected_files"),
    (
        ("test_db_4", 0, 0),
        ("test_db_11", 40, 120),
    ),
)
def test_provisioned_profiles_do_not_materialize_legacy_folder_stores(
    provision_test_database,
    db_name: str,
    expected_folders: int,
    expected_files: int,
) -> None:
    provisioned = provision_test_database(db_name)
    conn = sqlite3.connect(str(provisioned.db_path))
    try:
        folder_store_table = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'folder_stores' LIMIT 1;"
        ).fetchone()
        assert folder_store_table is None

        folder_count = _count_relation(conn, "folders")
        file_count = _count_relation(conn, "files")

        assert folder_count == expected_folders
        assert file_count == expected_files
    finally:
        conn.close()


@pytest.mark.db
def test_semantic_book_count_bands(provision_test_database) -> None:
    counts: dict[str, int] = {}
    for db_name in ("test_db_2", "test_db_16", "test_db_7", "test_db_8", "test_db_14", "test_db_17", "test_db_4", "test_db_20"):
        provisioned = provision_test_database(db_name)
        conn = sqlite3.connect(str(provisioned.db_path))
        try:
            counts[db_name] = int(conn.execute("SELECT COUNT(*) FROM books;").fetchone()[0])
        finally:
            conn.close()

    assert counts["test_db_2"] == 1
    assert counts["test_db_16"] == 1
    assert counts["test_db_7"] == 6
    assert counts["test_db_8"] == 6
    assert counts["test_db_14"] == 10
    assert counts["test_db_17"] == 10
    assert counts["test_db_20"] > counts["test_db_4"] > counts["test_db_17"]

def test_provisioned_copies_are_independent(tmp_path, test_resources_manager) -> None:
    db1 = test_resources_manager.provision_named_test_database(name="test_db_0", dst_dir=tmp_path / "a")
    db2 = test_resources_manager.provision_named_test_database(name="test_db_0", dst_dir=tmp_path / "b")

    assert _count_titles(db1.db_path) == _count_titles(db2.db_path)

    from tests.support.test_resources_manager import _insert_minimal_row, _register_sqlite_test_functions

    conn = sqlite3.connect(str(db1.db_path))
    try:
        _register_sqlite_test_functions(conn)
        _insert_minimal_row(conn, table="titles", preferred_text_value="Another Title")
        conn.commit()
    finally:
        conn.close()

    assert _count_titles(db1.db_path) == _count_titles(db2.db_path) + 1


@pytest.mark.db
def test_metadata_rich_db_0_provisions_distinct_optional_metadata(provision_test_database) -> None:
    provisioned = provision_test_database("metadata_rich_db_0")
    conn = sqlite3.connect(str(provisioned.db_path))
    try:
        assert conn.execute("PRAGMA integrity_check;").fetchone()[0] == "ok"
        assert conn.execute("PRAGMA foreign_key_check;").fetchall() == []

        assert int(conn.execute("SELECT COUNT(*) FROM books;").fetchone()[0]) == 3
        assert int(conn.execute("SELECT COUNT(*) FROM human_agents;").fetchone()[0]) == 4
        assert int(conn.execute("SELECT COUNT(*) FROM org_agents;").fetchone()[0]) == 1
        assert int(conn.execute("SELECT COUNT(*) FROM agent_work_links;").fetchone()[0]) == 7
        assert int(conn.execute("SELECT COUNT(*) FROM labels;").fetchone()[0]) == 4
        assert int(conn.execute("SELECT COUNT(*) FROM label_work_links;").fetchone()[0]) == 5
        assert int(conn.execute("SELECT COUNT(*) FROM series;").fetchone()[0]) == 3  # includes the required null row
        assert int(conn.execute("SELECT COUNT(*) FROM series_work_links;").fetchone()[0]) == 3
        assert int(conn.execute("SELECT COUNT(*) FROM subjects;").fetchone()[0]) == 3
        assert int(conn.execute("SELECT COUNT(*) FROM subject_work_links;").fetchone()[0]) == 5
        assert int(conn.execute("SELECT COUNT(*) FROM language_work_links;").fetchone()[0]) == 4
        assert int(conn.execute("SELECT COUNT(*) FROM notes;").fetchone()[0]) == 2
        assert int(conn.execute("SELECT COUNT(*) FROM comments;").fetchone()[0]) == 2
        assert int(conn.execute("SELECT COUNT(*) FROM synopses;").fetchone()[0]) == 2
        assert int(conn.execute("SELECT COUNT(*) FROM annotations;").fetchone()[0]) == 2
        assert int(conn.execute("SELECT COUNT(*) FROM entity_identifiers;").fetchone()[0]) == 3
        assert int(conn.execute("SELECT COUNT(*) FROM item_identifiers;").fetchone()[0]) == 3

        titles = {
            str(row[0])
            for row in conn.execute("SELECT work_title FROM works ORDER BY work_id;").fetchall()
        }
        assert titles == {
            "Metadata Rich Book One",
            "Metadata Rich Book Two",
            "Metadata Rich Book Three",
        }

        credit_types = {
            str(row[0])
            for row in conn.execute("SELECT DISTINCT agent_work_link_type FROM agent_work_links ORDER BY agent_work_link_type;").fetchall()
        }
        assert credit_types == {"aut", "edt", "pbl", "trl"}
    finally:
        conn.close()


@pytest.mark.db
def test_metadata_rich_db_1_provisions_multilingual_dense_metadata(provision_test_database) -> None:
    provisioned = provision_test_database("metadata_rich_db_1")
    conn = sqlite3.connect(str(provisioned.db_path))
    try:
        assert conn.execute("PRAGMA integrity_check;").fetchone()[0] == "ok"
        assert conn.execute("PRAGMA foreign_key_check;").fetchall() == []

        assert int(conn.execute("SELECT COUNT(*) FROM books;").fetchone()[0]) == 4
        assert int(conn.execute("SELECT COUNT(*) FROM human_agents;").fetchone()[0]) == 5
        assert int(conn.execute("SELECT COUNT(*) FROM org_agents;").fetchone()[0]) == 1
        assert int(conn.execute("SELECT COUNT(*) FROM agent_work_links;").fetchone()[0]) == 10
        assert int(conn.execute("SELECT COUNT(*) FROM labels;").fetchone()[0]) == 5
        assert int(conn.execute("SELECT COUNT(*) FROM label_work_links;").fetchone()[0]) == 7
        assert int(conn.execute("SELECT COUNT(*) FROM series;").fetchone()[0]) == 4
        assert int(conn.execute("SELECT COUNT(*) FROM series_work_links;").fetchone()[0]) == 4
        assert int(conn.execute("SELECT COUNT(*) FROM subjects;").fetchone()[0]) == 4
        assert int(conn.execute("SELECT COUNT(*) FROM subject_work_links;").fetchone()[0]) == 7
        assert int(conn.execute("SELECT COUNT(*) FROM language_work_links;").fetchone()[0]) == 6
        assert int(conn.execute("SELECT COUNT(*) FROM notes;").fetchone()[0]) == 3
        assert int(conn.execute("SELECT COUNT(*) FROM comments;").fetchone()[0]) == 3
        assert int(conn.execute("SELECT COUNT(*) FROM synopses;").fetchone()[0]) == 3
        assert int(conn.execute("SELECT COUNT(*) FROM annotations;").fetchone()[0]) == 3
        assert int(conn.execute("SELECT COUNT(*) FROM entity_identifiers;").fetchone()[0]) == 4
        assert int(conn.execute("SELECT COUNT(*) FROM item_identifiers;").fetchone()[0]) == 4

        titles = {str(row[0]) for row in conn.execute("SELECT work_title FROM works ORDER BY work_id;")}
        assert titles == {
            "Metadata Spectrum One",
            "Metadata Spectrum Two",
            "Metadata Spectrum Three",
            "Metadata Spectrum Four",
        }
    finally:
        conn.close()


@pytest.mark.db
def test_stores_assets_db_0_provisions_real_store_backed_assets(provision_test_database, driver_spec, tmp_path: Path) -> None:
    provisioned = provision_test_database("stores_assets_db_0")
    conn = sqlite3.connect(str(provisioned.db_path))
    try:
        assert conn.execute("PRAGMA integrity_check;").fetchone()[0] == "ok"
        assert conn.execute("PRAGMA foreign_key_check;").fetchall() == []

        assert int(conn.execute("SELECT COUNT(*) FROM stores;").fetchone()[0]) == 1
        assert int(conn.execute("SELECT COUNT(*) FROM folders;").fetchone()[0]) == 2
        assert int(conn.execute("SELECT COUNT(*) FROM files;").fetchone()[0]) == 2
        assert int(conn.execute("SELECT COUNT(*) FROM images;").fetchone()[0]) == 2
        assert int(conn.execute("SELECT COUNT(*) FROM folder_work_links;").fetchone()[0]) == 4
        assert int(conn.execute("SELECT COUNT(*) FROM file_folder_links;").fetchone()[0]) == 2
        assert int(conn.execute("SELECT COUNT(*) FROM image_work_links;").fetchone()[0]) == 2

        store_root_uri = str(conn.execute("SELECT store_root_uri FROM stores LIMIT 1;").fetchone()[0])
        assert store_root_uri == str((provisioned.root / "store_root").resolve())

        original_paths = [
            Path(str(row[0]))
            for row in conn.execute(
                "SELECT file_original_path FROM files UNION ALL SELECT image_original_path FROM images;"
            ).fetchall()
        ]
        assert original_paths
        for path in original_paths:
            assert path.exists()
            assert path.is_file()
            assert provisioned.root.resolve() in path.resolve().parents

        item_paths = [
            Path(str(row[0]))
            for row in conn.execute("SELECT item_source_path FROM items ORDER BY item_id;").fetchall()
        ]
        assert item_paths
        for path in item_paths:
            assert path.exists()
            assert provisioned.root.resolve() in path.resolve().parents
    finally:
        conn.close()

    with Database(
        metadata={"database_path": str(provisioned.db_path)},
        db_type=driver_spec.db_type,
        create=False,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        assert db.storage is not None
        first_file = db.get_all_rows("files", iterator_return=False)[0]
        got = db.storage.locate_file(
            metadata={
                "file_storage_key": str(first_file["file_storage_key"]),
                "file_store_id": int(first_file["file_store_id"]),
            }
        )
        assert got.as_bytes() == b"EPUB-ASSET-ONE\n"


@pytest.mark.db
def test_stores_assets_db_1_provisions_multi_store_assets(provision_test_database, driver_spec) -> None:
    provisioned = provision_test_database("stores_assets_db_1")
    conn = sqlite3.connect(str(provisioned.db_path))
    try:
        assert conn.execute("PRAGMA integrity_check;").fetchone()[0] == "ok"
        assert conn.execute("PRAGMA foreign_key_check;").fetchall() == []

        assert int(conn.execute("SELECT COUNT(*) FROM stores;").fetchone()[0]) == 2
        assert int(conn.execute("SELECT COUNT(*) FROM folders;").fetchone()[0]) == 4
        assert int(conn.execute("SELECT COUNT(*) FROM files;").fetchone()[0]) == 4
        assert int(conn.execute("SELECT COUNT(*) FROM images;").fetchone()[0]) == 3
        assert int(conn.execute("SELECT COUNT(*) FROM folder_work_links;").fetchone()[0]) == 6
        assert int(conn.execute("SELECT COUNT(*) FROM file_folder_links;").fetchone()[0]) == 4
        assert int(conn.execute("SELECT COUNT(*) FROM image_work_links;").fetchone()[0]) == 3

        roots = {
            str(row[0])
            for row in conn.execute("SELECT store_root_uri FROM stores ORDER BY store_id;").fetchall()
        }
        assert roots == {
            str((provisioned.root / "primary_store_root").resolve()),
            str((provisioned.root / "secondary_store_root").resolve()),
        }
    finally:
        conn.close()

    with Database(
        metadata={"database_path": str(provisioned.db_path)},
        db_type=driver_spec.db_type,
        create=False,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        assert db.storage is not None
        rows = db.get_all_rows("files", iterator_return=False)
        primary = next(row for row in rows if str(row["file_name"]).endswith(".epub"))
        secondary = next(row for row in rows if str(row["file_name"]).endswith(".mobi"))
        got_primary = db.storage.locate_file(
            metadata={
                "file_storage_key": str(primary["file_storage_key"]),
                "file_store_id": int(primary["file_store_id"]),
            }
        )
        got_secondary = db.storage.locate_file(
            metadata={
                "file_storage_key": str(secondary["file_storage_key"]),
                "file_store_id": int(secondary["file_store_id"]),
            }
        )
        assert got_primary.as_bytes() == b"PRIMARY-EPUB-ONE\n"
        assert got_secondary.as_bytes() == b"MOBI-TWO\n"


@pytest.mark.db
def test_images_covers_db_0_provisions_cover_heavy_store_assets(provision_test_database, driver_spec) -> None:
    provisioned = provision_test_database("images_covers_db_0")
    conn = sqlite3.connect(str(provisioned.db_path))
    try:
        assert conn.execute("PRAGMA integrity_check;").fetchone()[0] == "ok"
        assert conn.execute("PRAGMA foreign_key_check;").fetchall() == []

        assert int(conn.execute("SELECT COUNT(*) FROM books;").fetchone()[0]) == 3
        assert int(conn.execute("SELECT COUNT(*) FROM stores;").fetchone()[0]) == 1
        assert int(conn.execute("SELECT COUNT(*) FROM folders;").fetchone()[0]) == 1
        assert int(conn.execute("SELECT COUNT(*) FROM images;").fetchone()[0]) == 5
        assert int(conn.execute("SELECT COUNT(*) FROM image_work_links;").fetchone()[0]) == 5
        assert int(conn.execute("SELECT COUNT(*) FROM folder_work_links;").fetchone()[0]) == 3

        link_types = {
            str(row[0])
            for row in conn.execute("SELECT DISTINCT image_work_link_type FROM image_work_links ORDER BY image_work_link_type;")
        }
        assert link_types == {"cover", "diagram", "illustration"}

        work_one_priorities = conn.execute(
            "SELECT image_work_link_priority FROM image_work_links WHERE image_work_link_work_id = 1 ORDER BY image_work_link_priority;"
        ).fetchall()
        assert [int(row[0]) for row in work_one_priorities] == [1, 2]

        image_paths = [
            Path(str(row[0]))
            for row in conn.execute("SELECT image_original_path FROM images ORDER BY image_id;").fetchall()
        ]
        assert image_paths
        for path in image_paths:
            assert path.exists()
            assert provisioned.root.resolve() in path.resolve().parents
    finally:
        conn.close()

    with Database(
        metadata={"database_path": str(provisioned.db_path)},
        db_type=driver_spec.db_type,
        create=False,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        assert db.storage is not None
        first_image = db.get_all_rows("images", iterator_return=False)[0]
        got = db.storage.locate_file(
            metadata={
                "file_storage_key": str(first_image["image_storage_key"]),
                "file_store_id": int(first_image["image_store_id"]),
            }
        )
        assert got.as_bytes().startswith(b"\x89PNG\r\n\x1a\n")


@pytest.mark.db
def test_images_covers_db_1_provisions_cover_variants_and_gaps(provision_test_database, driver_spec) -> None:
    provisioned = provision_test_database("images_covers_db_1")
    conn = sqlite3.connect(str(provisioned.db_path))
    try:
        assert conn.execute("PRAGMA integrity_check;").fetchone()[0] == "ok"
        assert conn.execute("PRAGMA foreign_key_check;").fetchall() == []

        assert int(conn.execute("SELECT COUNT(*) FROM books;").fetchone()[0]) == 4
        assert int(conn.execute("SELECT COUNT(*) FROM stores;").fetchone()[0]) == 1
        assert int(conn.execute("SELECT COUNT(*) FROM folders;").fetchone()[0]) == 2
        assert int(conn.execute("SELECT COUNT(*) FROM images;").fetchone()[0]) == 6
        assert int(conn.execute("SELECT COUNT(*) FROM image_work_links;").fetchone()[0]) == 6
        assert int(conn.execute("SELECT COUNT(*) FROM folder_work_links;").fetchone()[0]) == 5

        link_types = {
            str(row[0])
            for row in conn.execute("SELECT DISTINCT image_work_link_type FROM image_work_links ORDER BY image_work_link_type;")
        }
        assert link_types == {"cover", "diagram", "illustration", "map"}

        covered_work_ids = {
            int(row[0]) for row in conn.execute("SELECT DISTINCT image_work_link_work_id FROM image_work_links;").fetchall()
        }
        assert covered_work_ids == {1, 2, 3}
    finally:
        conn.close()

    with Database(
        metadata={"database_path": str(provisioned.db_path)},
        db_type=driver_spec.db_type,
        create=False,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        first_image = db.get_all_rows("images", iterator_return=False)[0]
        got = db.storage.locate_file(
            metadata={
                "file_storage_key": str(first_image["image_storage_key"]),
                "file_store_id": int(first_image["image_store_id"]),
            }
        )
        assert got.as_bytes().startswith(b"\x89PNG\r\n\x1a\n")


@pytest.mark.db
def test_custom_columns_populated_db_0_provisions_live_generated_tables(provision_test_database) -> None:
    provisioned = provision_test_database("custom_columns_populated_db_0")
    conn = sqlite3.connect(str(provisioned.db_path))
    try:
        assert conn.execute("PRAGMA integrity_check;").fetchone()[0] == "ok"
        assert conn.execute("PRAGMA foreign_key_check;").fetchall() == []

        rows = conn.execute(
            "SELECT custom_column_label, custom_column_datatype, custom_column_is_multiple, custom_column_in_table "
            "FROM custom_columns ORDER BY custom_column_id;"
        ).fetchall()
        assert rows == [
            ("facet_tags", "text", 1, "works"),
            ("editor_rating", "rating", 0, "works"),
            ("featured_pick", "bool", 0, "works"),
            ("staff_note", "comments", 0, "works"),
        ]

        tables = {
            str(row[0])
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND (name LIKE 'custom_column_%' OR name LIKE 'works_custom_column_%');"
            ).fetchall()
        }
        assert {
            "custom_column_1",
            "custom_column_2",
            "custom_column_3",
            "custom_column_4",
            "works_custom_column_1_link",
            "works_custom_column_2_link",
        }.issubset(tables)

        assert int(conn.execute("SELECT COUNT(*) FROM custom_column_1;").fetchone()[0]) == 4
        assert int(conn.execute("SELECT COUNT(*) FROM works_custom_column_1_link;").fetchone()[0]) == 4
        assert int(conn.execute("SELECT COUNT(*) FROM custom_column_2;").fetchone()[0]) == 3
        assert int(conn.execute("SELECT COUNT(*) FROM works_custom_column_2_link;").fetchone()[0]) == 3
        assert int(conn.execute("SELECT COUNT(*) FROM custom_column_3;").fetchone()[0]) == 2
        assert int(conn.execute("SELECT COUNT(*) FROM custom_column_4;").fetchone()[0]) == 2

        tag_values = conn.execute(
            "SELECT c.custom_column_1_value "
            "FROM works_custom_column_1_link l "
            "JOIN custom_column_1 c ON c.custom_column_1_id = l.works_custom_column_1_link_value "
            "WHERE l.works_custom_column_1_link_book = 1 "
            "ORDER BY c.custom_column_1_value;"
        ).fetchall()
        assert [str(row[0]) for row in tag_values] == ["annotated", "featured"]

        ratings = conn.execute(
            "SELECT c.custom_column_2_value "
            "FROM works_custom_column_2_link l "
            "JOIN custom_column_2 c ON c.custom_column_2_id = l.works_custom_column_2_link_value "
            "ORDER BY l.works_custom_column_2_link_book;"
        ).fetchall()
        assert [int(row[0]) for row in ratings] == [8, 4, 10]

        bool_values = conn.execute(
            "SELECT custom_column_3_book, custom_column_3_value FROM custom_column_3 ORDER BY custom_column_3_book;"
        ).fetchall()
        assert bool_values == [(1, 1), (2, 0)]
    finally:
        conn.close()


@pytest.mark.db
def test_custom_columns_populated_db_1_provisions_series_and_scalar_variants(provision_test_database) -> None:
    provisioned = provision_test_database("custom_columns_populated_db_1")
    conn = sqlite3.connect(str(provisioned.db_path))
    try:
        assert conn.execute("PRAGMA integrity_check;").fetchone()[0] == "ok"
        assert conn.execute("PRAGMA foreign_key_check;").fetchall() == []

        rows = conn.execute(
            "SELECT custom_column_label, custom_column_datatype, custom_column_is_multiple, custom_column_in_table "
            "FROM custom_columns ORDER BY custom_column_id;"
        ).fetchall()
        assert rows == [
            ("curator_tags", "text", 1, "works"),
            ("reading_order", "series", 0, "works"),
            ("priority_score", "float", 0, "works"),
            ("reference_flag", "bool", 0, "works"),
            ("review_blob", "comments", 0, "works"),
        ]

        assert int(conn.execute("SELECT COUNT(*) FROM custom_column_1;").fetchone()[0]) == 5
        assert int(conn.execute("SELECT COUNT(*) FROM works_custom_column_1_link;").fetchone()[0]) == 5
        assert int(conn.execute("SELECT COUNT(*) FROM custom_column_2;").fetchone()[0]) == 2
        assert int(conn.execute("SELECT COUNT(*) FROM works_custom_column_2_link;").fetchone()[0]) == 4
        assert int(conn.execute("SELECT COUNT(*) FROM custom_column_3;").fetchone()[0]) == 4
        assert int(conn.execute("SELECT COUNT(*) FROM custom_column_4;").fetchone()[0]) == 3
        assert int(conn.execute("SELECT COUNT(*) FROM custom_column_5;").fetchone()[0]) == 3

        order_values = conn.execute(
            "SELECT works_custom_column_2_link_extra FROM works_custom_column_2_link ORDER BY works_custom_column_2_link_book;"
        ).fetchall()
        assert [float(row[0]) for row in order_values] == [1.0, 2.0, 1.5, 3.0]
    finally:
        conn.close()


@pytest.mark.db
def test_identifiers_db_0_provisions_rich_identifier_views(provision_test_database) -> None:
    provisioned = provision_test_database("identifiers_db_0")
    conn = sqlite3.connect(str(provisioned.db_path))
    try:
        assert conn.execute("PRAGMA integrity_check;").fetchone()[0] == "ok"
        assert conn.execute("PRAGMA foreign_key_check;").fetchall() == []

        assert int(conn.execute("SELECT COUNT(*) FROM entity_identifiers;").fetchone()[0]) == 7
        assert int(conn.execute("SELECT COUNT(*) FROM item_identifiers;").fetchone()[0]) == 3

        entity_types = conn.execute(
            "SELECT entity_type, COUNT(*) FROM identifiers_v GROUP BY entity_type ORDER BY entity_type;"
        ).fetchall()
        assert entity_types == [
            ("expression", 1),
            ("item", 4),
            ("manifestation", 3),
            ("work", 2),
        ]

        origins = conn.execute(
            "SELECT identifier_origin, COUNT(*) FROM identifiers_v GROUP BY identifier_origin ORDER BY identifier_origin;"
        ).fetchall()
        assert origins == [("entity", 7), ("item", 3)]

        values = conn.execute(
            "SELECT identifier_scheme, identifier_value FROM identifiers_v ORDER BY identifier_scheme, identifier_value;"
        ).fetchall()
        assert values == [
            ("asin", "B000000001"),
            ("asset-id", "asset-three"),
            ("barcode", "200000000002"),
            ("doi", "10.5555/work-one"),
            ("handle", "hdl:9999/three"),
            ("isbn10", "0000000002"),
            ("isbn13", "9780000000001"),
            ("oclc", "oclc-10001"),
            ("uri", "urn:expression:two"),
            ("vendor", "vendor-three"),
        ]
    finally:
        conn.close()


@pytest.mark.db
def test_identifiers_db_1_provisions_wider_identifier_matrix(provision_test_database) -> None:
    provisioned = provision_test_database("identifiers_db_1")
    conn = sqlite3.connect(str(provisioned.db_path))
    try:
        assert conn.execute("PRAGMA integrity_check;").fetchone()[0] == "ok"
        assert conn.execute("PRAGMA foreign_key_check;").fetchall() == []

        assert int(conn.execute("SELECT COUNT(*) FROM entity_identifiers;").fetchone()[0]) == 10
        assert int(conn.execute("SELECT COUNT(*) FROM item_identifiers;").fetchone()[0]) == 5

        entity_types = conn.execute(
            "SELECT entity_type, COUNT(*) FROM identifiers_v GROUP BY entity_type ORDER BY entity_type;"
        ).fetchall()
        assert entity_types == [
            ("expression", 2),
            ("item", 7),
            ("manifestation", 2),
            ("work", 4),
        ]
    finally:
        conn.close()


@pytest.mark.db
def test_pathological_relations_db_0_provisions_dense_relation_graph(provision_test_database) -> None:
    provisioned = provision_test_database("pathological_relations_db_0")
    conn = sqlite3.connect(str(provisioned.db_path))
    try:
        assert conn.execute("PRAGMA integrity_check;").fetchone()[0] == "ok"
        assert conn.execute("PRAGMA foreign_key_check;").fetchall() == []

        assert int(conn.execute("SELECT COUNT(*) FROM books;").fetchone()[0]) == 8
        assert int(conn.execute("SELECT COUNT(*) FROM human_agents;").fetchone()[0]) == 6
        assert int(conn.execute("SELECT COUNT(*) FROM org_agents;").fetchone()[0]) == 1
        assert int(conn.execute("SELECT COUNT(*) FROM agent_work_links;").fetchone()[0]) == 24
        assert int(conn.execute("SELECT COUNT(*) FROM labels;").fetchone()[0]) == 4
        assert int(conn.execute("SELECT COUNT(*) FROM label_work_links;").fetchone()[0]) == 24
        assert int(conn.execute("SELECT COUNT(*) FROM subjects;").fetchone()[0]) == 4
        assert int(conn.execute("SELECT COUNT(*) FROM subject_work_links;").fetchone()[0]) == 16
        assert int(conn.execute("SELECT COUNT(*) FROM series;").fetchone()[0]) == 4
        assert int(conn.execute("SELECT COUNT(*) FROM series_work_links;").fetchone()[0]) == 8
        assert int(conn.execute("SELECT COUNT(*) FROM language_work_links;").fetchone()[0]) == 10

        max_labels_per_work = int(
            conn.execute(
                "SELECT MAX(c) FROM (SELECT COUNT(*) AS c FROM label_work_links GROUP BY label_work_link_work_id);"
            ).fetchone()[0]
        )
        max_agents_per_work = int(
            conn.execute(
                "SELECT MAX(c) FROM (SELECT COUNT(*) AS c FROM agent_work_links GROUP BY agent_work_link_work_id);"
            ).fetchone()[0]
        )
        common_label_fanout = int(
            conn.execute(
                "SELECT COUNT(*) FROM label_work_links l JOIN labels t ON t.label_id = l.label_work_link_label_id WHERE t.label_text = 'common';"
            ).fetchone()[0]
        )

        assert max_labels_per_work == 3
        assert max_agents_per_work == 3
        assert common_label_fanout == 8
    finally:
        conn.close()


@pytest.mark.db
def test_weird_data_db_0_provisions_unicode_and_odd_paths(provision_test_database) -> None:
    provisioned = provision_test_database("weird_data_db_0")
    conn = sqlite3.connect(str(provisioned.db_path))
    try:
        assert conn.execute("PRAGMA integrity_check;").fetchone()[0] == "ok"
        assert conn.execute("PRAGMA foreign_key_check;").fetchall() == []

        assert int(conn.execute("SELECT COUNT(*) FROM books;").fetchone()[0]) == 3
        assert int(conn.execute("SELECT COUNT(*) FROM notes;").fetchone()[0]) == 1
        assert int(conn.execute("SELECT COUNT(*) FROM comments;").fetchone()[0]) == 1
        assert int(conn.execute("SELECT COUNT(*) FROM synopses;").fetchone()[0]) == 1
        assert int(conn.execute("SELECT COUNT(*) FROM labels;").fetchone()[0]) == 3
        assert int(conn.execute("SELECT COUNT(*) FROM stores;").fetchone()[0]) == 1
        assert int(conn.execute("SELECT COUNT(*) FROM folders;").fetchone()[0]) == 2
        assert int(conn.execute("SELECT COUNT(*) FROM files;").fetchone()[0]) == 3
        assert int(conn.execute("SELECT COUNT(*) FROM images;").fetchone()[0]) == 1

        titles = {str(row[0]) for row in conn.execute("SELECT work_title FROM works ORDER BY work_id;")}
        assert titles == {
            "El Niño — édition finale",
            "漢字とかなの本",
            "Emoji Field Notes ☕",
        }

        file_names = {str(row[0]) for row in conn.execute("SELECT file_name FROM files ORDER BY file_id;")}
        assert file_names == {
            "El Niño — édition finale.EPUB",
            "漢字とかなの本.PDF",
            "emoji-field-notes-☕.txt",
        }

        original_paths = [
            Path(str(row[0]))
            for row in conn.execute(
                "SELECT file_original_path FROM files UNION ALL SELECT image_original_path FROM images;"
            ).fetchall()
        ]
        assert original_paths
        for path in original_paths:
            assert path.exists()
            assert provisioned.root.resolve() in path.resolve().parents
    finally:
        conn.close()

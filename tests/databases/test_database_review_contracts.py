"""Regressions for DBR03/05/08/09/10/11/13/14 on actual database boundaries."""

import queue
import sqlite3
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace

import pytest

from LiuXin_alpha.databases.column_metadata import infer_column_metadata
from LiuXin_alpha.databases.connection_hooks import SQLiteCompletionConnection
from LiuXin_alpha.databases.database.dirtied_mixin import DatabaseDirtiedRecordsMixin
from LiuXin_alpha.databases.dbprefs import DBPrefs
from LiuXin_alpha.databases.preferences import (
    audit_preference_keys,
    migrate_preference_keys,
)
from LiuXin_alpha.databases.query_indexes import rebuild_sqlite_search_indexes
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.databases.schema_specs import StorageLinkSpec
from LiuXin_alpha.databases.search_functions import validate_search_index_semantics
from LiuXin_alpha.databases.sqlite_values import decode_sqlite_value
from LiuXin_alpha.databases.transactions import transaction_scope
from LiuXin_alpha.databases.view_reads import read_view_row
from LiuXin_alpha.metadata.value_policies import ColumnNormalizationProfile
from LiuXin_alpha.storage.durable_manager import StorageManager
from tests.databases.test_row_write_safety import db as db
from tests.databases.test_row_write_safety import stored
from tests.databases.test_row_write_safety import template as template


@pytest.mark.parametrize(
    "command",
    ["PRAGMA foreign_keys=OFF", "VACUUM", "ATTACH ':memory:' AS other", "DETACH other"],
)
def test_maintained_script_rejects_connection_commands_before_effects(command):
    with closing(
        sqlite3.connect(":memory:", factory=SQLiteCompletionConnection)
    ) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        conn.executescript(
            "CREATE TABLE maintenance_metadata(id INTEGER);"
            "CREATE TABLE parent(id INTEGER PRIMARY KEY);"
            "CREATE TABLE child(id INTEGER REFERENCES parent ON DELETE CASCADE);"
            "INSERT INTO parent VALUES(1);INSERT INTO child VALUES(1);"
        )
        conn._liuxin_before_completion = lambda: None
        with pytest.raises(RuntimeError, match="connection commands"):
            conn.executescript(
                f"INSERT INTO parent VALUES(2);{command};DROP TABLE parent;"
            )
        assert conn.execute("SELECT * FROM parent").fetchall() == [(1,)]
        assert conn.execute("SELECT * FROM child").fetchall() == [(1,)]


def test_owned_scope_refuses_callers_pending_work():
    with closing(sqlite3.connect(":memory:")) as conn:
        conn.execute("CREATE TABLE staged(id INTEGER)")
        conn.execute("INSERT INTO staged VALUES(1)")
        with pytest.raises(RuntimeError, match="idle connection"):
            with transaction_scope(conn, owns_transaction=True):
                pytest.fail("body must not run")
        assert conn.in_transaction
        assert conn.execute("SELECT * FROM staged").fetchall() == [(1,)]
        conn.rollback()
        assert conn.execute("SELECT * FROM staged").fetchall() == []


def test_failed_preference_serialization_preserves_existing_backup(tmp_path):
    prefs = dict.__new__(DBPrefs)
    dict.__setitem__(prefs, "binary", b"valid utf8")
    prefs.write_serialized(tmp_path)
    backup = tmp_path / "metadata_db_prefs_backup.json"
    original = backup.read_bytes()
    assert DBPrefs.read_serialized(tmp_path)["binary"] == b"valid utf8"
    dict.__setitem__(prefs, "bad", Path("unserializable"))
    with pytest.raises(TypeError):
        prefs.write_serialized(tmp_path)
    assert backup.read_bytes() == original
    assert not list(tmp_path.glob(".liuxin-prefs-*"))


def test_draft_read_does_not_override_database_defaults(db):
    row = Row(db, {"work_title": "Draft"})
    original = dict(row.row_dict)
    assert row["work_created_timestamp_ep_k"] is None
    assert row.row_dict == original
    row.sync()
    assert (
        stored(
            db,
            "SELECT work_created_timestamp_ep_k FROM works WHERE work_id=?",
            (row.row_id,),
        )[0][0]
        is not None
    )


@pytest.mark.parametrize(
    "payload",
    [
        {"review_item_id": 424242},
        {"review_item_id": 424242, "review_item_text": "Absent"},
    ],
)
def test_identified_row_cannot_silently_update_missing_target(db, payload):
    with pytest.raises(Exception, match="No row"):
        Row(db, payload).sync()
    assert stored(db, "SELECT work_id FROM works WHERE work_id=424242") == []


def test_view_reads_keep_numeric_looking_text(db):
    titles = ("1984", "007", "1e3")
    ids = [db.driver_wrapper.add_row({"work_title": title}) for title in titles]
    with closing(db.driver.get_connection()) as conn:
        conn.execute(
            "CREATE VIEW review_coalesced AS SELECT work_id AS id, COALESCE(work_canonical_title,work_title) AS title FROM works"
        ).close()
        conn.commit()
    assert [
        read_view_row(db.driver, "review_coalesced", row_id)["title"] for row_id in ids
    ] == list(titles)
    assert [row["title"] for row in db.driver_wrapper.get_all_rows("titles")] == list(
        titles
    )


@pytest.mark.parametrize(
    "table,column",
    [("tags", "tag_phash"), ("genres", "genre_phash"), ("subjects", "subject_phash")],
)
def test_identity_text_is_not_classified_as_a_hash(table, column):
    metadata = infer_column_metadata(table, column, "TEXT")
    assert metadata.semantic_role.value == "normalized_key"
    assert metadata.validation_profile.value == "normalized_key"


def test_legacy_identity_requires_explicit_profile_upgrade(db):
    first = db.macros.ensure_table_value(
        "series", "series", "Star Trek - Next Generation"
    )
    with closing(db.driver.get_connection()) as conn:
        conn.execute(
            "UPDATE normalized_identities SET normalized_identity_normalization_profile='title_search_term' WHERE normalized_identity_table_name='series'"
        )
        conn.execute(
            "UPDATE column_metadata SET column_metadata_normalization_profile='title_search_term' WHERE column_metadata_table_name='series' AND column_metadata_column_name='series'"
        )
        conn.execute("UPDATE series SET series_name_norm='star_trek'")
        conn.commit()
    with pytest.raises(Exception, match="Lossy identity profile"):
        db.macros.ensure_table_value("series", "series", "Star Trek - Deep Space Nine")
    profiles = {
        (
            "series",
            "series",
        ): ColumnNormalizationProfile.UNICODE_NFKC_WHITESPACE_CASEFOLD
    }
    assert (
        db.macros.audit_normalized_identities(
            target_profiles=profiles
        ).rows_needing_update
        == 1
    )
    db.macros.migrate_normalized_identities(target_profiles=profiles)
    second = db.macros.ensure_table_value(
        "series", "series", "Star Trek - Deep Space Nine"
    )
    assert first != second


def test_preferences_publish_only_after_commit_and_preserve_binary(db):
    prefs = DBPrefs(db)
    prefs["binary"] = b"ordinary utf8"
    assert DBPrefs(db)["binary"] == b"ordinary utf8"
    with pytest.raises(RuntimeError, match="abort"):
        with db.macros.transaction():
            prefs["binary"] = b"replacement"
            raise RuntimeError("abort")
    assert prefs["binary"] == DBPrefs(db)["binary"] == b"ordinary utf8"
    prefs.disable_setting = True
    del prefs["binary"]
    assert DBPrefs(db)["binary"] == b"ordinary utf8"


def test_preference_duplicate_resolution_is_explicit(db):
    with closing(db.driver.get_connection()) as conn:
        conn.execute("DROP TABLE preferences")
        conn.execute(
            "CREATE TABLE preferences(preference_id INTEGER PRIMARY KEY, preference_key TEXT, preference_value TEXT)"
        )
        conn.executemany(
            "INSERT INTO preferences VALUES(?, 'size', ?)", [(1, "100"), (2, "50")]
        )
        conn.commit()
    assert audit_preference_keys(db) == {"size": (1, 2)}
    with pytest.raises(Exception, match="Explicit preference conflict"):
        migrate_preference_keys(db)
    assert len(stored(db, "SELECT * FROM preferences")) == 2
    migrate_preference_keys(db, keep={"size": 1})
    prefs = DBPrefs(db)
    assert prefs["size"] == 100
    prefs["size"] = 75
    assert DBPrefs(db)["size"] == 75
    assert len(stored(db, "SELECT * FROM preferences")) == 1


def test_search_index_semantics_require_explicit_rebuild(db):
    db.driver_wrapper.add_row({"work_title": "Dune"})
    with closing(sqlite3.connect(db.driver.database_path)) as conn:
        conn.execute(
            "UPDATE search_index_metadata SET search_index_metadata_version='old Unicode'"
        )
        conn.commit()
        with pytest.raises(Exception, match="Search index semantics"):
            validate_search_index_semantics(conn)
    path = db.driver.database_path
    db.close()
    rebuild_sqlite_search_indexes(path)
    with closing(sqlite3.connect(path)) as conn:
        validate_search_index_semantics(conn)


def test_container_bindings_are_local_and_lossless(db):
    adapters, converters = dict(sqlite3.adapters), dict(sqlite3.converters)
    with closing(db.driver.get_connection()) as conn:
        values = ({"", "O'Brien", "comma,slash\\"}, ["007", 1], {"title": "1984"})
        row = conn.execute("SELECT ?, ?, ?", values).fetchone()
        assert (
            tuple(
                decode_sqlite_value(value, kind)
                for value, kind in zip(row, ("PYSET", "PYLIST", "PYDICT"), strict=True)
            )
            == values
        )
    assert dict(sqlite3.adapters) == adapters
    assert dict(sqlite3.converters) == converters


def test_pruning_protects_references_omitted_from_link_specs(db):
    tag = db.macros.ensure_table_value("tags", "tag", "Item-only tag")
    with closing(db.driver.get_connection()) as conn:
        conn.execute("INSERT INTO items(item_id) VALUES (5001)")
        conn.execute(
            "INSERT INTO item_tag_links(item_tag_link_item_id,item_tag_link_tag_id) VALUES (?,?)",
            (5001, tag),
        )
        conn.commit()
    spec = StorageLinkSpec(
        "works",
        "tags",
        "tag_work_links",
        primary_id_col="work_id",
        secondary_id_col="tag_id",
        primary_link_col="tag_work_link_work_id",
        secondary_link_col="tag_work_link_tag_id",
    )
    assert db.macros.delete_unreferenced_rows("tags", (spec,)) == ()
    assert stored(db, "SELECT tag_id FROM tags WHERE tag_id=?", (tag,)) == [(tag,)]
    assert stored(db, "SELECT item_tag_link_tag_id FROM item_tag_links") == [(tag,)]


def test_profile_upgrade_collision_leaves_keys_and_declarations_unchanged(db):
    with closing(db.driver.get_connection()) as conn:
        conn.executemany(
            "INSERT INTO tags(tag,tag_phash) VALUES(?,?)",
            [("Same", "old-a"), (" SAME ", "old-b")],
        )
        conn.commit()
    profiles = {
        ("tags", "tag"): ColumnNormalizationProfile.UNICODE_NFKC_WHITESPACE_CASEFOLD
    }
    assert db.macros.audit_normalized_identities(target_profiles=profiles).collisions
    before = stored(db, "SELECT * FROM normalized_identities")
    with pytest.raises(Exception, match="collisions"):
        db.macros.migrate_normalized_identities(target_profiles=profiles)
    assert stored(db, "SELECT tag_phash FROM tags ORDER BY tag_id") == [
        ("old-a",),
        ("old-b",),
    ]
    assert stored(db, "SELECT * FROM normalized_identities") == before


@pytest.mark.parametrize("reload", [False, True])
def test_row_table_hint_controls_insert_reload_and_update(db, reload):
    with closing(db.driver.get_connection()) as conn:
        conn.execute(
            "CREATE TABLE review_shadow(review_item_id INTEGER PRIMARY KEY, review_item_text TEXT, review_item_other TEXT)"
        )
        conn.commit()
    row = Row.from_idless_row_dict(
        db,
        {"review_item_text": "Targeted"},
        table="review_shadow",
        reload_from_db=reload,
    )
    assert row.table == "review_shadow"
    row["review_item_text"] = "Updated"
    row.sync()
    assert stored(db, "SELECT review_item_text FROM review_shadow") == [("Updated",)]
    assert stored(db, "SELECT * FROM review_items") == []


@pytest.mark.parametrize("strict", [False, True])
def test_failed_bootstrap_restores_startup_setting_without_inventing_counts(
    db, monkeypatch, strict
):
    with StorageManager(db=db, startup_on_add=False) as manager:
        db.storage = manager

        def fail(*args, **kwargs):
            raise RuntimeError("enumeration failed")

        monkeypatch.setattr(manager, "load_from_database", fail)
        if strict:
            with pytest.raises(RuntimeError, match="enumeration failed"):
                db.bootstrap_storage_manager(startup_on_add=True, strict=True)
        else:
            report = db.bootstrap_storage_manager(startup_on_add=True, strict=False)
            assert not report.completed and not report.ok
            assert report.discovered_configurations == report.failed_configurations == 0
        assert manager.startup_on_add is False


def test_dirty_persistence_failure_is_distinct_from_an_empty_queue():
    pending = queue.Queue()
    pending.put(("works", 7, "changed"))

    def fail(*args):
        raise RuntimeError("persistence failed")

    host = SimpleNamespace(
        metadata_dirtied_table="metadata_dirtied",
        all_tables={"metadata_dirtied"},
        dirty_records_queue=pending,
        _database=SimpleNamespace(
            driver_wrapper=SimpleNamespace(
                get_column_headings=lambda table: [
                    "metadata_dirtied_id",
                    "metadata_dirtied_table",
                    "metadata_dirtied_table_id",
                    "metadata_dirtied_reason",
                ],
                executemany=fail,
            )
        ),
    )
    with pytest.raises(RuntimeError, match="persistence failed"):
        DatabaseDirtiedRecordsMixin.persist_dirtied_records(host)
    assert pending.get_nowait() == ("works", 7, "changed")


def test_declared_identity_role_overrides_old_hash_metadata(db):
    with closing(db.driver.get_connection()) as conn:
        conn.execute(
            "UPDATE column_metadata SET column_metadata_semantic_role='hash', column_metadata_validation_profile='hash' WHERE column_metadata_table_name='tags' AND column_metadata_column_name='tag_phash'"
        )
        conn.commit()
    metadata = db.driver.direct_get_column_metadata("tags", "tag_phash")
    assert (
        metadata.semantic_role.value
        == metadata.validation_profile.value
        == "normalized_key"
    )

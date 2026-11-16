"""RR03–RR06: supported formats, offline recovery and conservative pruning."""

import os
import shutil
import sqlite3
import subprocess
import sys
from contextlib import closing
from unittest.mock import MagicMock

import pytest

from LiuXin_alpha.databases.custom_columns.native_service import CustomFieldService
from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.databases.database_driver_plugins.registry import load_database_driver
from LiuXin_alpha.databases.db_types.macro_types import UnreferencedRowsSpec
from LiuXin_alpha.databases.query_indexes import rebuild_sqlite_search_indexes
from LiuXin_alpha.databases.schema_revision import CURRENT_SCHEMA_REVISION
from LiuXin_alpha.databases.schema_specs import StorageLinkSpec
from LiuXin_alpha.databases.search_functions import (
    _rebuild_command,
    search_index_semantics,
)
from LiuXin_alpha.errors import DatabaseIntegrityError, InputIntegrityError
from tests.databases.test_row_write_safety import db as db
from tests.databases.test_row_write_safety import template as template

REVISION_DAMAGE = {
    "absent": "DROP TABLE schema_revision",
    "empty": "DELETE FROM schema_revision",
    "future": "UPDATE schema_revision SET schema_revision_revision=999999",
    "old": "PRAGMA ignore_check_constraints=ON; UPDATE schema_revision SET schema_revision_revision=0",
    "malformed": "DROP TABLE schema_revision; CREATE TABLE schema_revision(other TEXT)",
    "multiple": "DROP TABLE schema_revision; CREATE TABLE schema_revision(schema_revision_id, schema_revision_revision); INSERT INTO schema_revision VALUES(1,1),(2,1)",
}


@pytest.mark.parametrize("damage", REVISION_DAMAGE)
def test_schema_revision_refuses_before_open_effects(template, tmp_path, damage):
    backend, source = template
    path = tmp_path / "unsupported.sqlite"
    shutil.copyfile(source, path)
    with closing(sqlite3.connect(path)) as conn:
        conn.executescript(REVISION_DAMAGE[damage])
        conn.commit()
    original = path.read_bytes()
    for open_catalogue in (
        lambda: Database(metadata={"database_path": str(path)}, db_type=backend),
        lambda: load_database_driver(backend)({"database_path": str(path)}, db=None),
        lambda: rebuild_sqlite_search_indexes(path),
    ):
        with pytest.raises(DatabaseIntegrityError, match="[Rr]egenerate"):
            open_catalogue()
        assert path.read_bytes() == original


def assert_dynamic_fields_keep_base_revision(db):
    """The base marker remains stable when native fields add physical tables."""
    with closing(db.driver.get_connection()) as conn:
        assert conn.execute("SELECT * FROM schema_revision").fetchall() == [
            (1, CURRENT_SCHEMA_REVISION)
        ]
    CustomFieldService(db).create_field("works", "revision_probe")
    with closing(db.driver.get_connection()) as conn:
        assert conn.execute("SELECT * FROM schema_revision").fetchall() == [
            (1, CURRENT_SCHEMA_REVISION)
        ]


def test_dynamic_fields_keep_base_revision(db):
    assert_dynamic_fields_keep_base_revision(db)


@pytest.mark.parametrize("damage", ["absent", "future"])
def test_backup_cannot_publish_unsupported_format(db, tmp_path, damage):
    path = db.driver.database_path
    with closing(sqlite3.connect(path)) as conn:
        conn.executescript(REVISION_DAMAGE[damage])
        conn.commit()
    destination = tmp_path / "backup.sqlite"
    with pytest.raises(DatabaseIntegrityError, match="Regenerate"):
        db.driver.direct_backup(destination)
    assert not destination.exists()


def test_unicode_mismatch_has_executable_offline_recovery(db, tmp_path):
    original_path, backend = db.driver.database_path, db.db_type
    work = db.driver_wrapper.add_row({"work_title": "Straße"})
    with closing(db.driver.get_connection()) as conn:
        conn.execute(
            "UPDATE search_index_metadata SET search_index_metadata_version=?",
            ("previous Unicode policy",),
        )
        conn.commit()
    db.close()
    path = tmp_path / "library with space 'quote'.sqlite"
    shutil.copyfile(original_path, path)
    with pytest.raises(DatabaseIntegrityError) as caught:
        Database(metadata={"database_path": path}, db_type=backend)
    message = str(caught.value)
    assert "previous Unicode policy" in message
    assert search_index_semantics() in message
    assert "LiuXin_alpha.databases.rebuild_search_indexes" in message
    command = message.splitlines()[1]
    # Execute the text the user sees, including shell quoting. Constructing a
    # separate argv would miss escaping introduced by the exception renderer.
    invocation = (
        ["powershell", "-NoProfile", "-NonInteractive", "-Command", command]
        if os.name == "nt"
        else command
    )
    result = subprocess.run(
        invocation,
        shell=os.name != "nt",
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    with Database(
        metadata={"database_path": path},
        db_type=backend,
        enable_storage_manager=False,
        enable_maintenance=False,
    ) as reopened:
        assert reopened.get(
            "SELECT work_id FROM works WHERE liuxin_text_key_v1(work_title)=liuxin_text_key_v1(?)",
            ("STRASSE",),
        ) == [(work,)]


def test_displayed_powershell_recovery_preserves_quotes_and_backslashes(monkeypatch):
    """Exercise the Windows renderer even when the host runs a POSIX shell."""
    connection = MagicMock()
    connection.execute.return_value.__iter__.return_value = iter(
        [(0, "main", r"C:\Users\O'Brien\My Library\catalogue.sqlite")]
    )
    with monkeypatch.context() as patch:
        patch.setattr(os, "name", "nt")
        patch.setattr(sys, "executable", r"C:\Program Files\Python\python.exe")
        command = _rebuild_command(connection)
    displayed = str(
        DatabaseIntegrityError(f"recorded={search_index_semantics()!r}\n{command}")
    )
    assert displayed.splitlines()[1] == (
        "& 'C:\\Program Files\\Python\\python.exe' '-m' "
        "'LiuXin_alpha.databases.rebuild_search_indexes' '--database-path' "
        "'C:\\Users\\O''Brien\\My Library\\catalogue.sqlite'"
    )


# Keep the expectations independent of the implementation's reference inventory.
UNDECLARED = (
    ("annotations", "annotation_item_id", "items", "item_id"),
    ("annotations", "annotation_device_id", "devices", "device_id"),
    (
        "backup_presence_links",
        "backup_presence_link_source_file_id",
        "files",
        "file_id",
    ),
    ("backup_workflow_sources", "backup_workflow_source_file_id", "files", "file_id"),
    (
        "backup_workflow_sources",
        "backup_workflow_source_store_id",
        "stores",
        "store_id",
    ),
    (
        "backup_workflow_sources",
        "backup_workflow_source_asset_replica_id",
        "asset_replicas",
        "asset_replica_id",
    ),
)


def test_undeclared_reference_inventory_matches_generated_schema(template):
    """RR10: inspect the generated SQLite schema before fixtures can fill gaps."""
    from LiuXin_alpha.databases.database_driver_plugins.SQL.macros.repair_storage import (
        UNDECLARED_REFERENCE_CANDIDATES,
    )

    assert set(UNDECLARED_REFERENCE_CANDIDATES) == set(UNDECLARED)
    with closing(sqlite3.connect(template[1])) as conn:
        for source, column, target, key in UNDECLARED:
            columns = {row[1] for row in conn.execute(f'PRAGMA table_info("{source}")')}
            assert column in columns, f"Missing generated reference: {source}.{column}"
            target_columns = {
                row[1] for row in conn.execute(f'PRAGMA table_info("{target}")')
            }
            assert key in target_columns, f"Missing generated target: {target}.{key}"
            foreign_keys = conn.execute(
                f'PRAGMA foreign_key_list("{source}")'
            ).fetchall()
            assert not any(row[3] == column for row in foreign_keys), (
                f"{source}.{column} now has a foreign key; review and retire its RR05 refusal entry"
            )


def assert_pruning_refuses_undeclared_reference(db, reference, bulk):
    source, column, target, key = reference
    with closing(db.driver.get_connection()) as conn:
        # Some optional storage relations are absent on PostgreSQL. Install the
        # same undeclared-reference shape to exercise the portable refusal there.
        conn.execute(
            f'CREATE TABLE IF NOT EXISTS "{target}" ("{key}" BIGINT PRIMARY KEY)'
        ).close()
        conn.execute(
            f'CREATE TABLE IF NOT EXISTS "{source}" ("{column}" BIGINT)'
        ).close()
        conn.execute(
            "CREATE TABLE rereview_links (rereview_link_id INTEGER PRIMARY KEY, target_id BIGINT, other_id BIGINT)"
        ).close()
        conn.execute(f'INSERT INTO "{target}" ("{key}") VALUES (7002)').close()
        conn.execute(f'INSERT INTO "{source}" ("{column}") VALUES (7002)').close()
        conn.commit()
    spec = StorageLinkSpec(
        target,
        "works",
        "rereview_links",
        primary_id_col=key,
        secondary_id_col="work_id",
        primary_link_col="target_id",
        secondary_link_col="other_id",
    )
    with pytest.raises(InputIntegrityError, match="known undeclared reference"):
        if bulk:
            db.macros.delete_unreferenced_rows_bulk(
                (UnreferencedRowsSpec(target, (spec,), id_column=key),)
            )
        else:
            db.macros.delete_unreferenced_rows(target, (spec,), id_column=key)
    with closing(db.driver.get_connection()) as conn:
        assert conn.execute(
            f'SELECT "{key}" FROM "{target}" WHERE "{key}"=7002'
        ).fetchall() == [(7002,)]
        assert conn.execute(
            f'SELECT "{column}" FROM "{source}" WHERE "{column}"=7002'
        ).fetchall() == [(7002,)]


@pytest.mark.parametrize(
    "reference", UNDECLARED, ids=[entry[1] for entry in UNDECLARED]
)
@pytest.mark.parametrize("bulk", [False, True])
def test_pruning_refuses_undeclared_reference(db, reference, bulk):
    assert_pruning_refuses_undeclared_reference(db, reference, bulk)


def test_bulk_pruning_refusal_rolls_back_earlier_target(db):
    with closing(db.driver.get_connection()) as conn:
        conn.execute("INSERT INTO review_items(review_item_id) VALUES (7003)")
        conn.execute("CREATE TABLE rereview_links (target_id BIGINT, other_id BIGINT)")
        conn.commit()
    first = StorageLinkSpec(
        "review_items",
        "works",
        "rereview_links",
        primary_id_col="review_item_id",
        secondary_id_col="work_id",
        primary_link_col="target_id",
        secondary_link_col="other_id",
    )
    second = StorageLinkSpec(
        "items",
        "works",
        "rereview_links",
        primary_id_col="item_id",
        secondary_id_col="work_id",
        primary_link_col="target_id",
        secondary_link_col="other_id",
    )
    with pytest.raises(InputIntegrityError, match="annotations.annotation_item_id"):
        db.macros.delete_unreferenced_rows_bulk(
            (
                UnreferencedRowsSpec("review_items", (first,)),
                UnreferencedRowsSpec("items", (second,)),
            )
        )
    assert db.get("SELECT review_item_id FROM review_items") == [(7003,)]


def test_declared_reference_protects_owner_and_permits_other_orphan_pruning(db):
    with closing(db.driver.get_connection()) as conn:
        conn.execute("DROP TABLE annotations")
        conn.execute(
            "CREATE TABLE annotations (annotation_id INTEGER PRIMARY KEY, annotation_item_id INTEGER REFERENCES items(item_id) ON DELETE RESTRICT)"
        )
        conn.execute("INSERT INTO items(item_id) VALUES (7002), (7003)")
        conn.execute("INSERT INTO annotations(annotation_item_id) VALUES (7002)")
        conn.commit()
    spec = StorageLinkSpec(
        "items",
        "tags",
        "item_tag_links",
        primary_id_col="item_id",
        secondary_id_col="tag_id",
        primary_link_col="item_tag_link_item_id",
        secondary_link_col="item_tag_link_tag_id",
    )
    deleted = db.macros.delete_unreferenced_rows("items", (spec,))
    assert 7003 in deleted and 7002 not in deleted
    assert db.get("SELECT annotation_item_id FROM annotations") == [(7002,)]


def test_current_schema_profile_migration_remains_supported(db):
    from LiuXin_alpha.metadata.value_policies import ColumnNormalizationProfile

    db.macros.ensure_table_value("series", "series", "Profile example")
    profiles = {
        ("series", "series"): ColumnNormalizationProfile.UNICODE_NFC_TRIM_CASEFOLD
    }
    db.macros.migrate_normalized_identities(target_profiles=profiles)
    assert not db.macros.audit_normalized_identities(
        target_profiles=profiles
    ).rows_needing_update

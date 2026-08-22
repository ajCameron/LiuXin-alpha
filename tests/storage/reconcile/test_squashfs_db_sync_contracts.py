"""Fast state and persistence contracts for SquashFS reconciliation."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest

from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.errors import InputIntegrityError
from LiuXin_alpha.storage.api import FileInfo, Location
from LiuXin_alpha.storage.reconcile import squashfs_db_sync as sync
from LiuXin_alpha.storage.store_backend_plugins.squashfs_readonly import (
    SquashfsBuildReport,
)
from tests.storage._mini_db import build_mini_db


@pytest.fixture
def mini_db(tmp_path: Path):
    db = build_mini_db(tmp_path / "squashfs-contracts.sqlite")
    try:
        yield db
    finally:
        db.conn.close()


def _insert_store(
    db,
    *,
    name: str,
    root_uri: str,
    kind: str = "on_disk_existing_managed_drive",
) -> Row:
    return Row.from_idless_row_dict(
        db,
        row_dict={
            "store_name": name,
            "store_kind": kind,
            "store_access_protocol": "file",
            "store_root_uri": root_uri,
            "store_is_read_only": 0,
            "store_online_status": "online",
        },
        table="stores",
    )


def _insert_file(
    db,
    *,
    store_id: int | None,
    path: Path,
    storage_key: str | None = None,
    file_hash: str | None = None,
) -> Row:
    return Row.from_idless_row_dict(
        db,
        row_dict={
            "file_store_id": store_id,
            "file_storage_key": storage_key,
            "file_name": path.name,
            "file_base_name": path.stem,
            "file_extension": path.suffix.lstrip("."),
            "file_size_bytes": path.stat().st_size if path.exists() else None,
            "file_hash_sha256": file_hash,
            "file_integrity_status": "ok",
            "file_source": "contract-test",
        },
        table="files",
    )


@pytest.mark.parametrize(
    ("raw", "expected"),
    (
        (" books\\one.epub ", "books/one.epub"),
        ("./books//one.epub", "books/one.epub"),
        ("books/./one.epub", "books/one.epub"),
    ),
)
def test_archive_paths_are_normalized(raw: str, expected: str) -> None:
    assert sync._normalize_archive_path(raw) == expected


@pytest.mark.parametrize(
    ("raw", "message"),
    (
        ("", "cannot be empty"),
        ("/absolute.epub", "must be relative"),
        ("books/../escape.epub", "cannot contain"),
        (".//", "resolves to empty"),
    ),
)
def test_archive_paths_reject_empty_absolute_or_traversing_values(
    raw: str,
    message: str,
) -> None:
    with pytest.raises(InputIntegrityError, match=message):
        sync._normalize_archive_path(raw)


def test_scalar_hash_and_json_coercion_rejects_malformed_values() -> None:
    digest = "A" * 64

    assert sync._coerce_int(None) is None
    assert sync._coerce_int("7") == 7
    assert sync._coerce_int("bad") is None
    assert sync._normalize_sha256(None) is None
    assert sync._normalize_sha256(" ") is None
    assert sync._normalize_sha256("bad") is None
    assert sync._normalize_sha256(digest) == digest.lower()

    for parser in (sync._parse_policy_json, sync._parse_json_object):
        assert parser(None) == {}
        assert parser(" ") == {}
        assert parser("not-json") == {}
        assert parser("[]") == {}
        assert parser('{"value": 1}') == {"value": 1}


@pytest.mark.parametrize(
    ("kind", "expected"),
    (
        (None, sync.STORE_STATE_OPEN),
        (" ", sync.STORE_STATE_OPEN),
        (sync.LOCKED_SQUASHFS_STORE_KIND, sync.STORE_STATE_LOCKED),
        (sync.OPEN_SQUASHFS_STORE_KIND, sync.STORE_STATE_OPEN),
        (sync.OPEN_SQUASHFS_STORE_KIND_COMPAT.upper(), sync.STORE_STATE_OPEN),
        ("other", sync.STORE_STATE_OPEN),
    ),
)
def test_store_state_is_inferred_from_compatible_store_kinds(
    kind: str | None,
    expected: str,
) -> None:
    assert sync._infer_store_state_from_kind(kind) == expected


def test_open_store_kind_handles_none_and_compatibility_typo() -> None:
    assert not sync._is_open_store_kind(None)
    assert sync._is_open_store_kind(sync.OPEN_SQUASHFS_STORE_KIND)
    assert sync._is_open_store_kind(sync.OPEN_SQUASHFS_STORE_KIND_COMPAT.upper())
    assert not sync._is_open_store_kind("other")


def test_transition_history_sanitizes_rows_and_updates_repeated_states() -> None:
    history = [
        "invalid",
        {"state": "", "timestamp_ep_k": 1},
        {"state": "open", "timestamp_ep_k": "2", "detail": 3},
        {"state": "building", "timestamp_ep_k": "bad"},
    ]

    appended = sync._history_with_transition(
        history,
        to_state="failed",
        now_epk=10,
        detail="failure",
    )
    repeated = sync._history_with_transition(
        appended,
        to_state="failed",
        now_epk=11,
    )

    assert appended == [
        {"state": "open", "timestamp_ep_k": 2, "detail": "3"},
        {"state": "building"},
        {"state": "failed", "timestamp_ep_k": 10, "detail": "failure"},
    ]
    assert repeated[-1]["timestamp_ep_k"] == 11
    assert repeated[-1]["detail"] == "failure"


def test_state_transition_validation_rejects_unknown_or_forbidden_edges() -> None:
    with pytest.raises(InputIntegrityError, match="Unknown store state"):
        sync._validate_transition(
            current_state="unknown",
            next_state="open",
            transitions=sync.STORE_STATE_TRANSITIONS,
            kind="store",
        )
    with pytest.raises(InputIntegrityError, match="Invalid store state transition"):
        sync._validate_transition(
            current_state="locked",
            next_state="open",
            transitions=sync.STORE_STATE_TRANSITIONS,
            kind="store",
        )


def test_store_scratch_and_link_policy_apply_validated_state_changes() -> None:
    scratch = sync._store_scratch_with_state(
        None,
        next_state=sync.STORE_STATE_OPEN,
        now_epk=1,
        detail="created",
    )
    building = sync._store_scratch_with_state(
        scratch,
        next_state=sync.STORE_STATE_BUILDING,
        now_epk=2,
    )
    policy = sync._policy_with_state(
        None,
        next_state=sync.LINK_STATE_DESIGNATED,
        now_epk=3,
        detail="designated",
    )

    assert json.loads(building)["squashfs_state"] == "building"
    assert policy["state"] == "designated"
    assert policy["detail"] == "designated"

    with pytest.raises(InputIntegrityError, match="Unknown store state"):
        sync._store_scratch_with_state(
            None,
            next_state="unknown",
            now_epk=4,
        )
    with pytest.raises(InputIntegrityError, match="Unknown designation link state"):
        sync._policy_with_state(
            None,
            next_state="unknown",
            now_epk=4,
        )


class _TransactionConnection:
    def __init__(
        self,
        *,
        rollback_error: bool = False,
        close_error: bool = False,
    ) -> None:
        self.executed: list[tuple[object, object]] = []
        self.commits = 0
        self.rollbacks = 0
        self.closes = 0
        self.rollback_error = rollback_error
        self.close_error = close_error

    def execute(self, statement: str, params=None):
        self.executed.append((statement, params))
        return self

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1
        if self.rollback_error:
            raise RuntimeError("rollback failed")

    def close(self) -> None:
        self.closes += 1
        if self.close_error:
            raise RuntimeError("close failed")


def _transaction_db(conn: _TransactionConnection):
    return SimpleNamespace(
        driver=SimpleNamespace(get_connection=lambda: conn),
    )


def test_database_transaction_commits_and_closes() -> None:
    conn = _TransactionConnection()

    with sync._db_transaction(_transaction_db(conn)) as yielded:
        assert yielded is conn

    assert conn.executed == [("BEGIN IMMEDIATE", None)]
    assert conn.commits == 1
    assert conn.rollbacks == 0
    assert conn.closes == 1


def test_database_transaction_rolls_back_and_preserves_original_error() -> None:
    conn = _TransactionConnection(rollback_error=True, close_error=True)

    with pytest.raises(ValueError, match="body failed"):
        with sync._db_transaction(_transaction_db(conn)):
            raise ValueError("body failed")

    assert conn.rollbacks == 1
    assert conn.closes == 1


def test_transaction_update_ignores_empty_update_mappings() -> None:
    conn = _TransactionConnection()

    sync._update_row_in_tx(
        conn,
        table="stores",
        id_column="store_id",
        row_id=1,
        updates={},
    )

    assert conn.executed == []


class _SchemaDb:
    def __init__(
        self,
        tables: set[str],
        columns: dict[str, set[str]],
    ) -> None:
        self.tables = tables
        self.columns = columns

    def get_tables(self) -> set[str]:
        return self.tables

    def get_column_headings(self, table: str) -> set[str]:
        return self.columns.get(table, set())


def test_schema_support_reports_missing_tables_and_columns() -> None:
    with pytest.raises(InputIntegrityError, match="missing required tables"):
        sync._ensure_schema_support(_SchemaDb({"stores"}, {}))

    db = _SchemaDb(
        {"stores", "files", "file_store_links"},
        {
            "stores": {"store_root_uri"},
            "files": {"file_store_id"},
            "file_store_links": {"file_store_link_file_id"},
        },
    )
    with pytest.raises(InputIntegrityError) as exc_info:
        sync._ensure_schema_support(db)

    message = str(exc_info.value)
    assert "stores missing columns: store_kind" in message
    assert "files missing columns: file_storage_key" in message
    assert "file_store_links missing columns" in message


def test_schema_support_ignores_legacy_derivation_table() -> None:
    db = _SchemaDb(
        {"stores", "files", "file_store_links", "file_derivations"},
        {
            "stores": {"store_root_uri", "store_kind"},
            "files": {"file_store_id", "file_storage_key"},
            "file_store_links": {
                "file_store_link_file_id",
                "file_store_link_store_id",
                "file_store_link_type",
            },
            "file_derivations": {"file_derivation_parent_file_id"},
        },
    )

    tables, _stores, _files, links = sync._ensure_schema_support(db)

    assert "file_derivations" in tables
    assert links == {
        "file_store_link_file_id",
        "file_store_link_store_id",
        "file_store_link_type",
    }


def test_schema_support_succeeds_without_optional_derivation_table() -> None:
    db = _SchemaDb(
        {"stores", "files", "file_store_links"},
        {
            "stores": {"store_root_uri", "store_kind"},
            "files": {"file_store_id", "file_storage_key"},
            "file_store_links": {
                "file_store_link_file_id",
                "file_store_link_store_id",
                "file_store_link_type",
            },
        },
    )

    result = sync._ensure_schema_support(db)

    assert len(result) == 4


@pytest.mark.parametrize(
    ("item", "expected"),
    (
        (7, (7, None)),
        ("8", (8, None)),
        ((9, " books/nine.epub "), (9, "books/nine.epub")),
        ({"file_id": 10, "archive_path": "ten.epub"}, (10, "ten.epub")),
        ({"id": 11, "target": "eleven.epub"}, (11, "eleven.epub")),
    ),
)
def test_designation_items_accept_current_input_shapes(
    item: object,
    expected: tuple[int, str | None],
) -> None:
    assert sync._coerce_designation_item(item) == expected


@pytest.mark.parametrize(
    "item",
    (
        {"archive_path": "missing.epub"},
        (1,),
        ("bad", "book.epub"),
        object(),
    ),
)
def test_designation_items_reject_invalid_shapes(item: object) -> None:
    with pytest.raises(InputIntegrityError):
        sync._coerce_designation_item(item)


def test_open_store_creation_reopen_lock_and_directory_guards(
    mini_db,
    tmp_path: Path,
) -> None:
    archive = tmp_path / "archive.squashfs"
    created = sync.ensure_open_squashfs_store(
        mini_db,
        archive_path=archive,
        store_name="archive",
    )
    reopened = sync.ensure_open_squashfs_store(
        mini_db,
        archive_path=archive,
        store_name="renamed",
    )

    assert reopened.row_id == created.row_id
    assert reopened["store_name"] == "renamed"
    scratch = json.loads(reopened["store_scratch"])
    assert scratch["squashfs_state"] == sync.STORE_STATE_OPEN
    assert scratch["squashfs_state_history"][-1]["detail"] == "reopened"

    reopened["store_kind"] = sync.LOCKED_SQUASHFS_STORE_KIND
    reopened.sync()
    with pytest.raises(InputIntegrityError, match="locked SquashFS archive"):
        sync.ensure_open_squashfs_store(mini_db, archive_path=archive)

    directory = tmp_path / "directory"
    directory.mkdir()
    with pytest.raises(IsADirectoryError):
        sync.ensure_open_squashfs_store(mini_db, archive_path=directory)


def test_source_paths_and_designation_lifecycle(
    mini_db,
    tmp_path: Path,
) -> None:
    source_root = tmp_path / "source"
    source_root.mkdir()
    first_path = source_root / "first.epub"
    second_path = source_root / "second.epub"
    first_path.write_bytes(b"FIRST")
    second_path.write_bytes(b"SECOND")
    source_store = _insert_store(
        mini_db,
        name="source",
        root_uri=str(source_root),
    )
    first = _insert_file(
        mini_db,
        store_id=int(source_store.row_id),
        path=first_path,
        storage_key="first.epub",
    )
    second = _insert_file(
        mini_db,
        store_id=int(source_store.row_id),
        path=second_path,
        storage_key=str(second_path),
    )
    open_store = sync.ensure_open_squashfs_store(
        mini_db,
        archive_path=tmp_path / "target.squashfs",
    )
    store_id = int(open_store.row_id)

    assert sync._resolve_source_file_path(
        mini_db,
        file_row=first,
        store_cache={},
    ) == first_path.resolve()
    assert sync._resolve_source_file_path(
        mini_db,
        file_row=second,
        store_cache={},
    ) == second_path.resolve()

    created = sync.designate_files_for_squashfs_store(
        mini_db,
        store_id=store_id,
        designations=[first],
    )
    unchanged = sync.designate_files_for_squashfs_store(
        mini_db,
        store_id=store_id,
        designations=[int(first.row_id)],
    )
    assert created.created_links == 1
    assert unchanged.unchanged_links == 1

    with pytest.raises(InputIntegrityError, match="already designated"):
        sync.designate_files_for_squashfs_store(
            mini_db,
            store_id=store_id,
            designations=[(int(first.row_id), "retargeted.epub")],
        )

    updated = sync.designate_files_for_squashfs_store(
        mini_db,
        store_id=store_id,
        designations=[(int(first.row_id), "retargeted.epub")],
        replace_existing=True,
    )
    assert updated.updated_links == 1

    with pytest.raises(InputIntegrityError, match="already designated"):
        sync.designate_files_for_squashfs_store(
            mini_db,
            store_id=store_id,
            designations=[(int(second.row_id), "retargeted.epub")],
        )
    with pytest.raises(InputIntegrityError, match="missing file row"):
        sync.designate_files_for_squashfs_store(
            mini_db,
            store_id=store_id,
            designations=[999_999],
        )

    designations = sync._collect_designations(mini_db, store_id=store_id)
    assert [item.archive_path for item in designations] == ["retargeted.epub"]
    assert sync._validate_snapshot_consistency(designations) == []

    first_path.write_bytes(b"OTHER")
    hash_errors = sync._validate_snapshot_consistency(designations)
    assert "changed hash" in hash_errors[0]

    first_path.write_bytes(b"LONGER-PAYLOAD")
    size_errors = sync._validate_snapshot_consistency(designations)
    assert "changed size" in size_errors[0]


def test_collect_designations_supports_legacy_policy_fallbacks(
    mini_db,
    tmp_path: Path,
) -> None:
    source = tmp_path / "legacy.epub"
    source.write_bytes(b"LEGACY")
    source_store = _insert_store(
        mini_db,
        name="legacy-source",
        root_uri=str(tmp_path),
    )
    file_row = _insert_file(
        mini_db,
        store_id=int(source_store.row_id),
        path=source,
        storage_key="legacy.epub",
        file_hash=None,
    )
    open_store = sync.ensure_open_squashfs_store(
        mini_db,
        archive_path=tmp_path / "legacy.squashfs",
    )
    store_id = int(open_store.row_id)
    sync.designate_files_for_squashfs_store(
        mini_db,
        store_id=store_id,
        designations=[int(file_row.row_id)],
    )
    link = sync._designation_link_rows_for_store(mini_db, store_id=store_id)[0]
    link["file_store_link_policy"] = "{}"
    link.sync()

    designation = sync._collect_designations(mini_db, store_id=store_id)[0]

    assert designation.archive_path == "legacy.epub"
    assert designation.snapshot_sha256 == hashlib.sha256(b"LEGACY").hexdigest()
    assert designation.snapshot_size_bytes == len(b"LEGACY")
    assert designation.snapshot_mtime_ns is not None


def test_designation_and_source_path_error_cases(
    mini_db,
    tmp_path: Path,
) -> None:
    source_root = tmp_path / "source-errors"
    source_root.mkdir()
    source = source_root / "source.epub"
    source.write_bytes(b"SOURCE")
    source_store = _insert_store(
        mini_db,
        name="source-errors",
        root_uri=str(source_root),
    )
    no_store = _insert_file(
        mini_db,
        store_id=None,
        path=source,
        storage_key="source.epub",
    )
    class _Rowish(dict):
        row_id = 999_999

    missing_store = _Rowish(
        file_store_id=999_999,
        file_storage_key="source.epub",
    )
    missing_key = _insert_file(
        mini_db,
        store_id=int(source_store.row_id),
        path=source,
        storage_key=None,
    )

    with pytest.raises(InputIntegrityError, match="no file_store_id"):
        sync._resolve_source_file_path(mini_db, file_row=no_store, store_cache={})
    with pytest.raises(InputIntegrityError, match="missing source store"):
        sync._resolve_source_file_path(
            mini_db,
            file_row=missing_store,
            store_cache={},
        )
    with pytest.raises(InputIntegrityError, match="Cannot resolve source path"):
        sync._resolve_source_file_path(
            mini_db,
            file_row=missing_key,
            store_cache={},
        )

    missing_path = _insert_file(
        mini_db,
        store_id=int(source_store.row_id),
        path=source,
        storage_key="missing.epub",
    )
    with pytest.raises(FileNotFoundError, match="missing on disk"):
        sync._resolve_source_file_path(
            mini_db,
            file_row=missing_path,
            store_cache={},
        )

    non_open = _insert_store(
        mini_db,
        name="not-open",
        root_uri=str(tmp_path / "not-open"),
    )
    with pytest.raises(InputIntegrityError, match="not an open"):
        sync.designate_files_for_squashfs_store(
            mini_db,
            store_id=int(non_open.row_id),
            designations=[],
        )
    with pytest.raises(InputIntegrityError, match="Store row not found"):
        sync._get_store_row(mini_db, store_id=999_999)

    empty_open = sync.ensure_open_squashfs_store(
        mini_db,
        archive_path=tmp_path / "empty.squashfs",
    )
    with pytest.raises(InputIntegrityError, match="No designated files"):
        sync._collect_designations(
            mini_db,
            store_id=int(empty_open.row_id),
        )


def test_link_state_locking_primary_links_and_current_state(
    mini_db,
    tmp_path: Path,
) -> None:
    source = tmp_path / "state.epub"
    source.write_bytes(b"STATE")
    source_store = _insert_store(
        mini_db,
        name="state-source",
        root_uri=str(tmp_path),
    )
    file_row = _insert_file(
        mini_db,
        store_id=int(source_store.row_id),
        path=source,
        storage_key="state.epub",
    )
    open_store = sync.ensure_open_squashfs_store(
        mini_db,
        archive_path=tmp_path / "state.squashfs",
    )
    store_id = int(open_store.row_id)
    sync.designate_files_for_squashfs_store(
        mini_db,
        store_id=store_id,
        designations=[int(file_row.row_id)],
    )
    link = sync._designation_link_rows_for_store(mini_db, store_id=store_id)[0]

    sync._upsert_designation_state(
        link,
        state=sync.LINK_STATE_BUILDING,
        archive_path="state.epub",
        source_hash="a" * 64,
        archive_hash="b" * 64,
        detail="building",
    )
    policy = json.loads(link["file_store_link_policy"])
    assert policy["state"] == "building"
    assert policy["archive_hash_sha256"] == "b" * 64

    link_columns = set(mini_db.get_column_headings("file_store_links"))
    sync._ensure_primary_link_for_file(
        mini_db,
        file_id=int(file_row.row_id),
        store_id=store_id,
        link_columns=link_columns,
    )
    sync._ensure_primary_link_for_file(
        mini_db,
        file_id=int(file_row.row_id),
        store_id=store_id,
        link_columns=link_columns,
    )
    primary = [
        row
        for row in mini_db.search(
            "file_store_links",
            "file_store_link_file_id",
            int(file_row.row_id),
        )
        if row["file_store_link_type"] == "primary"
    ]
    assert len(primary) == 1
    assert len(sync._designation_link_rows_for_store(mini_db, store_id=store_id)) == 1

    assert sync._current_store_state(open_store) == sync.STORE_STATE_OPEN
    open_store["store_scratch"] = sync._store_scratch_with_state(
        open_store["store_scratch"],
        next_state=sync.STORE_STATE_BUILDING,
        now_epk=1,
    )
    open_store.sync()
    sync._lock_store_row_for_squashfs(
        open_store,
        archive_path=tmp_path / "state.squashfs",
    )
    assert sync._current_store_state(open_store) == sync.STORE_STATE_LOCKED
    assert open_store["store_kind"] == sync.LOCKED_SQUASHFS_STORE_KIND

    fallback_row = {
        "store_scratch": "{}",
        "store_kind": sync.LOCKED_SQUASHFS_STORE_KIND,
    }
    assert sync._current_store_state(fallback_row) == sync.STORE_STATE_LOCKED  # type: ignore[arg-type]


def test_duplicate_rows_and_primary_links_use_one_transaction(
    mini_db,
    tmp_path: Path,
) -> None:
    source = tmp_path / "duplicate.epub"
    source.write_bytes(b"DUPLICATE")
    source_store = _insert_store(
        mini_db,
        name="duplicate-source",
        root_uri=str(tmp_path),
    )
    source_row = _insert_file(
        mini_db,
        store_id=int(source_store.row_id),
        path=source,
        storage_key="duplicate.epub",
    )
    target_store = _insert_store(
        mini_db,
        name="duplicate-target",
        root_uri=str(tmp_path / "target.squashfs"),
        kind=sync.LOCKED_SQUASHFS_STORE_KIND,
    )
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    file_columns = set(mini_db.get_column_headings("files"))
    link_columns = set(mini_db.get_column_headings("file_store_links"))

    inserted, skipped, child_id = sync._duplicate_verified_file_row(
        mini_db.conn,
        source_row=source_row,
        source_path=source,
        locked_store_id=int(target_store.row_id),
        archive_path="books/duplicate.epub",
        archive_hash=digest,
        archive_size=source.stat().st_size,
        file_columns=file_columns,
        link_columns=link_columns,
        existing_rows_by_key={},
    )
    assert (inserted, skipped) == (True, False)
    assert child_id is not None

    existing = {
        "books/duplicate.epub": {
            "row_id": child_id,
            "file_hash_sha256": digest,
        }
    }
    assert sync._duplicate_verified_file_row(
        mini_db.conn,
        source_row=source_row,
        source_path=source,
        locked_store_id=int(target_store.row_id),
        archive_path="books/duplicate.epub",
        archive_hash=digest,
        archive_size=source.stat().st_size,
        file_columns=file_columns,
        link_columns=link_columns,
        existing_rows_by_key=existing,
    ) == (False, True, child_id)

    with pytest.raises(InputIntegrityError, match="conflicts"):
        sync._duplicate_verified_file_row(
            mini_db.conn,
            source_row=source_row,
            source_path=source,
            locked_store_id=int(target_store.row_id),
            archive_path="books/duplicate.epub",
            archive_hash="b" * 64,
            archive_size=source.stat().st_size,
            file_columns=file_columns,
            link_columns=link_columns,
            existing_rows_by_key=existing,
        )

    existing_row = mini_db.get_row_from_id("files", int(child_id))
    assert sync._duplicate_verified_file_row(
        mini_db.conn,
        source_row=source_row,
        source_path=source,
        locked_store_id=int(target_store.row_id),
        archive_path="books/existing-row.epub",
        archive_hash=digest,
        archive_size=source.stat().st_size,
        file_columns=file_columns,
        link_columns=link_columns,
        existing_rows_by_key={"books/existing-row.epub": existing_row},
    ) == (False, True, child_id)

    alternate = sync._duplicate_verified_file_row(
        mini_db.conn,
        source_row=source_row,
        source_path=source,
        locked_store_id=int(target_store.row_id),
        archive_path="books/alternate.epub",
        archive_hash=digest,
        archive_size=source.stat().st_size,
        file_columns=file_columns | {"not_a_file_column"},
        link_columns=link_columns,
        existing_rows_by_key={},
    )
    assert alternate[0] is True

    sync._ensure_primary_link_for_file_tx(
        mini_db.conn,
        file_id=int(child_id),
        store_id=int(target_store.row_id),
        link_columns=link_columns,
    )
    rows = mini_db.conn.execute("SELECT * FROM file_derivations").fetchall()
    assert rows == []


def test_reproducibility_metadata_filters_unknown_build_fields() -> None:
    scratch = sync._add_reproducibility_metadata_to_scratch(
        '{"keep":true}',
        build_report={
            "manifest_sha256": "a" * 64,
            "output_sha256": "b" * 64,
            "deterministic": True,
            "unknown": "ignored",
        },
        now_epk=5,
        published_state="locked",
    )
    without_report = sync._add_reproducibility_metadata_to_scratch(
        "{}",
        build_report=None,
        now_epk=6,
        published_state="failed",
    )

    payload = json.loads(scratch)
    assert payload["keep"] is True
    assert payload["squashfs_last_build"]["manifest_sha256"] == "a" * 64
    assert "unknown" not in payload["squashfs_last_build"]
    assert json.loads(without_report)["squashfs_last_build"] == {
        "published_state": "failed",
        "published_timestamp_ep_k": 6,
    }


def test_publish_build_failures_mark_store_failed_without_squashfs_tools(
    mini_db,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "publish.epub"
    source.write_bytes(b"PUBLISH")
    source_store = _insert_store(
        mini_db,
        name="publish-source",
        root_uri=str(tmp_path),
    )
    file_row = _insert_file(
        mini_db,
        store_id=int(source_store.row_id),
        path=source,
        storage_key="publish.epub",
    )
    open_store = sync.ensure_open_squashfs_store(
        mini_db,
        archive_path=tmp_path / "publish.squashfs",
    )
    sync.designate_files_for_squashfs_store(
        mini_db,
        store_id=int(open_store.row_id),
        designations=[int(file_row.row_id)],
    )

    def fail_build(**_kwargs: object):
        raise RuntimeError("builder failed")

    monkeypatch.setattr(sync, "build_squashfs_from_manifest", fail_build)
    report = sync.publish_open_squashfs_store(
        mini_db,
        store_id=int(open_store.row_id),
        refresh_storage_manager=False,
    )

    assert "builder failed" in report.errors[0]
    refreshed = mini_db.get_row_from_id("stores", int(open_store.row_id))
    assert sync._current_store_state(refreshed) == sync.STORE_STATE_FAILED

    with pytest.raises(RuntimeError, match="builder failed"):
        sync.publish_open_squashfs_store(
            mini_db,
            store_id=int(open_store.row_id),
            output_archive=tmp_path / "explicit-output.squashfs",
            strict=True,
            refresh_storage_manager=False,
        )


class _NonClosingConnection:
    def __init__(self, connection) -> None:
        self.connection = connection

    def execute(self, *args: object, **kwargs: object):
        return self.connection.execute(*args, **kwargs)

    def commit(self) -> None:
        self.connection.commit()

    def rollback(self) -> None:
        self.connection.rollback()

    def close(self) -> None:
        return None


class _MixedArchiveBackend:
    def __init__(self, *, url: str, name: str | None) -> None:
        self.url = url
        self.name = name
        self.store_ref = UUID(int=1)

    def locate(self, key: str) -> Location:
        return Location(self.store_ref, key)

    @staticmethod
    def exists(location: Location) -> bool:
        return not location.key.startswith("missing/")

    @staticmethod
    def stat(location: Location) -> FileInfo:
        return FileInfo(location=location, size=0)

    @staticmethod
    def read_bytes(_location: Location) -> bytes:
        return b"WRONG-ARCHIVE-PAYLOAD"


def test_publish_persists_missing_and_hash_mismatch_states(
    mini_db,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_store = _insert_store(
        mini_db,
        name="mixed-source",
        root_uri=str(tmp_path),
    )
    rows: list[Row] = []
    for name, payload in (("missing.epub", b"MISSING"), ("mismatch.epub", b"MATCH")):
        path = tmp_path / name
        path.write_bytes(payload)
        rows.append(
            _insert_file(
                mini_db,
                store_id=int(source_store.row_id),
                path=path,
                storage_key=name,
            )
        )
    open_store = sync.ensure_open_squashfs_store(
        mini_db,
        archive_path=tmp_path / "mixed.squashfs",
    )
    store_id = int(open_store.row_id)
    sync.designate_files_for_squashfs_store(
        mini_db,
        store_id=store_id,
        designations=[
            (int(rows[0].row_id), "missing/book.epub"),
            (int(rows[1].row_id), "mismatch/book.epub"),
        ],
    )

    report_data = SquashfsBuildReport(
        manifest_path="manifest.json",
        output_archive=str(tmp_path / "mixed.squashfs"),
        file_count=2,
        total_input_bytes=12,
        output_bytes=8,
        compression="zstd",
        deterministic=True,
        manifest_sha256="a" * 64,
        output_sha256="b" * 64,
        mksquashfs_executable="mksquashfs",
        mksquashfs_version="test",
        build_flags=("-noappend",),
    )
    monkeypatch.setattr(
        sync,
        "build_squashfs_from_manifest",
        lambda **_kwargs: report_data,
    )
    monkeypatch.setattr(sync, "SquashfsReadOnlyStorageBackend", _MixedArchiveBackend)
    mini_db.driver = SimpleNamespace(  # type: ignore[attr-defined]
        get_connection=lambda: _NonClosingConnection(mini_db.conn)
    )

    def fail_bootstrap(*, clear_existing: bool, strict: bool) -> None:
        assert clear_existing
        assert strict
        raise RuntimeError("refresh failed")

    mini_db.bootstrap_storage_manager = fail_bootstrap  # type: ignore[attr-defined]
    report = sync.publish_open_squashfs_store(
        mini_db,
        store_id=store_id,
        deterministic=True,
    )

    assert len(report.errors) == 2
    assert "missing_in_archive" in report.errors[0]
    assert "storage_manager_bootstrap_failed" in report.errors[1]
    assert len(report.hash_mismatches) == 1
    assert report.verified_files == 0
    store = mini_db.get_row_from_id("stores", store_id)
    assert sync._current_store_state(store) == sync.STORE_STATE_FAILED
    policies = [
        json.loads(row["file_store_link_policy"])
        for row in sync._designation_link_rows_for_store(mini_db, store_id=store_id)
    ]
    assert {policy["state"] for policy in policies} == {
        sync.LINK_STATE_MISSING,
        sync.LINK_STATE_HASH_MISMATCH,
    }


def test_publish_rejects_non_open_invalid_state_and_missing_root(
    mini_db,
    tmp_path: Path,
) -> None:
    non_open = _insert_store(
        mini_db,
        name="non-open-publish",
        root_uri=str(tmp_path / "non-open"),
    )
    with pytest.raises(InputIntegrityError, match="not an open"):
        sync.publish_open_squashfs_store(
            mini_db,
            store_id=int(non_open.row_id),
        )

    invalid_state = sync.ensure_open_squashfs_store(
        mini_db,
        archive_path=tmp_path / "invalid-state.squashfs",
    )
    invalid_state["store_scratch"] = json.dumps(
        {"squashfs_state": sync.STORE_STATE_BUILDING}
    )
    invalid_state.sync()
    with pytest.raises(InputIntegrityError, match="expected one of"):
        sync.publish_open_squashfs_store(
            mini_db,
            store_id=int(invalid_state.row_id),
        )

    missing_root = _insert_store(
        mini_db,
        name="missing-root",
        root_uri="placeholder",
        kind=sync.OPEN_SQUASHFS_STORE_KIND,
    )
    missing_root["store_root_uri"] = None
    missing_root.sync()
    with pytest.raises(InputIntegrityError, match="has no store_root_uri"):
        sync.publish_open_squashfs_store(
            mini_db,
            store_id=int(missing_root.row_id),
        )


def test_publish_convenience_wrapper_delegates_all_steps(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    calls: dict[str, object] = {}
    store_row = SimpleNamespace(row_id=7)

    def ensure(db, *, archive_path, store_name):
        calls["ensure"] = (db, archive_path, store_name)
        return store_row

    def designate(db, *, store_id, designations, replace_existing):
        calls["designate"] = (db, store_id, designations, replace_existing)

    expected = object()

    def publish(db, **kwargs):
        calls["publish"] = (db, kwargs)
        return expected

    monkeypatch.setattr(sync, "ensure_open_squashfs_store", ensure)
    monkeypatch.setattr(sync, "_store_row_id", lambda row: row.row_id)
    monkeypatch.setattr(sync, "designate_files_for_squashfs_store", designate)
    monkeypatch.setattr(sync, "publish_open_squashfs_store", publish)
    db = object()
    archive = tmp_path / "wrapper.squashfs"

    result = sync.publish_squashfs_archive_from_file_ids(
        db,
        file_ids=[1, "2"],  # type: ignore[list-item]
        archive_path=archive,
        store_name="wrapper",
        compression="xz",
        deterministic=True,
        force=True,
        strict=True,
        refresh_storage_manager=False,
    )

    assert result is expected
    assert calls["ensure"] == (db, archive, "wrapper")
    assert calls["designate"] == (db, 7, [1, 2], False)
    assert calls["publish"][1] == {
        "store_id": 7,
        "output_archive": archive,
        "compression": "xz",
        "deterministic": True,
        "force": True,
        "duplicate_verified_files": True,
        "strict": True,
        "refresh_storage_manager": False,
    }

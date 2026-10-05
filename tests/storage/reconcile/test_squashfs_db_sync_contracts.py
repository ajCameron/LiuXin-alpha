"""
Check SquashFS reconciliation helpers with scalar cases, SQLite, and explicit doubles.

State/schema/transaction recorder tests need no archive tools. The mini database
supports real legacy rows; selected publication cases substitute builder and archive
responses and supply the necessary connection/bootstrap seams. These contracts
distinguish metadata persistence from live archive verification and full manager
integration, which the separate real-tool workflow suite exercises.

Example:
    >>> test_transition_history_sanitizes_rows_and_updates_repeated_states()
"""

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
from LiuXin_alpha.storage.store_backend_plugins.squashfs_readonly.squashfs_manifest_builder import (
    SquashfsBuildReport,
)
from tests.storage._mini_db import build_mini_db


@pytest.fixture
def mini_db(tmp_path: Path):
    """
    Yield a small real SQLite catalogue and close its shared connection during teardown.

    Individual tests add missing workflow seams explicitly; this fixture is not a fully bootstrapped
    StorageManager integration environment.

    Example:
        >>> database = next(mini_db(tmp_path))  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for the contract-test SQLite file.
    :return: Fixture generator yielding the mini database; its finally block closes the connection.
    """
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
    """
    Insert a writable, online-labeled legacy Store row without constructing a backend.

    Example:
        >>> row = _insert_store(db, name="Source", root_uri=str(root))  # doctest: +SKIP


    :param db: Mini database receiving a real stores row.
    :param name: Fixture Store name.
    :param root_uri: Source/archive root text retained verbatim.
    :param kind: Store-kind fixture label, defaulting to managed local storage.
    :return: Inserted Row; declaration labels are not health-probe results.
    """
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
    """
    Insert source metadata while allowing absent Store/key/hash values for failure cases.

    Stat size only when the path exists and retain the supplied digest claim without hashing. The
    integrity label is a fixture value, not a verification result.

    Example:
        >>> row = _insert_file(db, store_id=1, path=path, storage_key="book.epub")  # doctest: +SKIP


    :param db: Mini database receiving the legacy files row.
    :param store_id: Optional source Store identity, including None for invalid-metadata cases.
    :param path: Path supplying filename components and optional stat size.
    :param storage_key: Optional key retained without normalization.
    :param file_hash: Optional digest claim retained without validation.
    :return: Inserted Row with fixture metadata.
    """
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
    """
    Normalize backslashes, repeated separators, and dot components for the listed relative targets.

    Example:
        >>> test_archive_paths_are_normalized(raw, expected)  # doctest: +SKIP


    :param raw: Parameterized noncanonical member target.
    :param expected: Exact normalized POSIX target.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Reject the listed empty, absolute, parent-traversing, or empty-after-normalization targets with
    diagnostic text.

    Example:
        >>> test_archive_paths_reject_empty_absolute_or_traversing_values(raw, message)  # doctest: +SKIP


    :param raw: Parameterized rejected member target.
    :param message: Expected exception-message regex.
    :return: None after the stated regression assertions pass.
    """
    with pytest.raises(InputIntegrityError, match=message):
        sync._normalize_archive_path(raw)


def test_scalar_hash_and_json_coercion_rejects_malformed_values() -> None:
    """
    Check integer/hash coercion and permissive JSON-object fallback without filesystem access.

    Require lowercase valid digest text and empty mappings for absent, malformed, or non-object JSON
    in both parsers.

    Example:
        >>> test_scalar_hash_and_json_coercion_rejects_malformed_values()


    :return: None after the stated regression assertions pass.
    """
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
    """
    Infer locked for the locked kind and open for the listed absent, compatible, and unknown labels.

    Example:
        >>> test_store_state_is_inferred_from_compatible_store_kinds(kind, expected)  # doctest: +SKIP


    :param kind: Parameterized optional Store-kind label.
    :param expected: Expected inferred state label.
    :return: None after the stated regression assertions pass.
    """
    assert sync._infer_store_state_from_kind(kind) == expected


def test_open_store_kind_handles_none_and_compatibility_typo() -> None:
    """
    Recognize canonical and historical-typo open kinds case-insensitively while rejecting None and
    an unrelated label.

    Example:
        >>> test_open_store_kind_handles_none_and_compatibility_typo()


    :return: None after the stated regression assertions pass.
    """
    assert not sync._is_open_store_kind(None)
    assert sync._is_open_store_kind(sync.OPEN_SQUASHFS_STORE_KIND)
    assert sync._is_open_store_kind(sync.OPEN_SQUASHFS_STORE_KIND_COMPAT.upper())
    assert not sync._is_open_store_kind("other")


def test_transition_history_sanitizes_rows_and_updates_repeated_states() -> None:
    """
    Drop unusable history entries, coerce retained fields, and refresh a repeated final state
    timestamp without losing detail.

    Example:
        >>> test_transition_history_sanitizes_rows_and_updates_repeated_states()


    :return: None after the stated regression assertions pass.
    """
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
    """
    Reject an unknown source state and the forbidden locked-to-open Store transition.

    Example:
        >>> test_state_transition_validation_rejects_unknown_or_forbidden_edges()


    :return: None after the stated regression assertions pass.
    """
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
    """
    Encode allowed initial/building states and reject unknown destination labels for Store scratch
    and designation policy.

    Example:
        >>> test_store_scratch_and_link_policy_apply_validated_state_changes()


    :return: None after the stated regression assertions pass.
    """
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
    """
    Record transaction calls and optionally fail rollback or close without executing SQL.

    Example:
        >>> _TransactionConnection().commits
        0
    """
    def __init__(
        self,
        *,
        rollback_error: bool = False,
        close_error: bool = False,
    ) -> None:
        """
        Initialize empty call history, counters, and cleanup-failure switches.

        Example:
            >>> conn = _TransactionConnection(rollback_error=True)
            >>> conn.rollbacks
            0


        :param rollback_error: Whether rollback increments its counter and then raises RuntimeError.
        :param close_error: Whether close increments its counter and then raises RuntimeError.
        :return: None after initializing the recorder.
        """
        self.executed: list[tuple[object, object]] = []
        self.commits = 0
        self.rollbacks = 0
        self.closes = 0
        self.rollback_error = rollback_error
        self.close_error = close_error

    def execute(self, statement: str, params=None):
        """
        Append the statement/parameters by reference without executing them.

        Example:
            >>> conn = _TransactionConnection()
            >>> conn.execute("BEGIN IMMEDIATE") is conn
            True


        :param statement: SQL-like text retained in the call log.
        :param params: Optional parameters retained without copying.
        :return: This recorder instance, acting as a cursor-like return value.
        """
        self.executed.append((statement, params))
        return self

    def commit(self) -> None:
        """
        Increment the commit counter without changing any database.

        Example:
            >>> conn = _TransactionConnection()
            >>> conn.commit()
            >>> conn.commits
            1


        :return: None after recording the call.
        """
        self.commits += 1

    def rollback(self) -> None:
        """
        Count rollback and optionally raise the configured cleanup error.

        Example:
            >>> conn = _TransactionConnection()
            >>> conn.rollback()
            >>> conn.rollbacks
            1


        :return: None unless rollback_error requests RuntimeError after counting.
        """
        self.rollbacks += 1
        if self.rollback_error:
            raise RuntimeError("rollback failed")

    def close(self) -> None:
        """
        Count close and optionally raise the configured cleanup error.

        Example:
            >>> conn = _TransactionConnection()
            >>> conn.close()
            >>> conn.closes
            1


        :return: None unless close_error requests RuntimeError after counting.
        """
        self.closes += 1
        if self.close_error:
            raise RuntimeError("close failed")


def _transaction_db(conn: _TransactionConnection):
    """
    Expose one recorder through the database.driver.get_connection seam.

    Example:
        >>> conn = _TransactionConnection()
        >>> _transaction_db(conn).driver.get_connection() is conn
        True


    :param conn: Recorder returned on every connection request.
    :return: Nested SimpleNamespace facade with no real persistence.
    """
    return SimpleNamespace(
        driver=SimpleNamespace(get_connection=lambda: conn),
    )


def test_database_transaction_commits_and_closes() -> None:
    """
    Record BEGIN IMMEDIATE, yield the same connection, and commit/close once on normal context exit.

    The recorder proves call behavior rather than real database isolation.

    Example:
        >>> test_database_transaction_commits_and_closes()


    :return: None after the stated regression assertions pass.
    """
    conn = _TransactionConnection()

    with sync._db_transaction(_transaction_db(conn)) as yielded:
        assert yielded is conn

    assert conn.executed == [("BEGIN IMMEDIATE", None)]
    assert conn.commits == 1
    assert conn.rollbacks == 0
    assert conn.closes == 1


def test_database_transaction_rolls_back_and_preserves_original_error() -> None:
    """
    Keep the body ValueError visible despite injected rollback and close failures.

    Assert both cleanup attempts with a recording connection; no real rows participate.

    Example:
        >>> test_database_transaction_rolls_back_and_preserves_original_error()


    :return: None after the stated regression assertions pass.
    """
    conn = _TransactionConnection(rollback_error=True, close_error=True)

    with pytest.raises(ValueError, match="body failed"):
        with sync._db_transaction(_transaction_db(conn)):
            raise ValueError("body failed")

    assert conn.rollbacks == 1
    assert conn.closes == 1


def test_transaction_update_ignores_empty_update_mappings() -> None:
    """
    Perform no execute call when the transaction update mapping is empty.

    Example:
        >>> test_transaction_update_ignores_empty_update_mappings()


    :return: None after the stated regression assertions pass.
    """
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
    """
    Expose caller-owned schema sets for table/column validation tests without a database.

    Example:
        >>> _SchemaDb({"stores"}, {}).get_tables()
        {'stores'}
    """
    def __init__(
        self,
        tables: set[str],
        columns: dict[str, set[str]],
    ) -> None:
        """
        Retain table and column collections by reference for schema responses.

        Example:
            >>> db = _SchemaDb(set(), {})
            >>> db.get_tables()
            set()


        :param tables: Set returned directly by get_tables.
        :param columns: Table-name mapping whose values are returned without copying.
        :return: None after storing the supplied collections.
        """
        self.tables = tables
        self.columns = columns

    def get_tables(self) -> set[str]:
        """
        Return the original advertised table set without copying.

        Example:
            >>> _SchemaDb(set(), {}).get_tables()
            set()


        :return: Caller-owned table set; mutations remain visible to the fixture.
        """
        return self.tables

    def get_column_headings(self, table: str) -> set[str]:
        """
        Return the stored column set, or a fresh empty set for an unknown table.

        Example:
            >>> _SchemaDb(set(), {}).get_column_headings("missing")
            set()


        :param table: Exact mapping key to look up.
        :return: Stored set by reference or a fresh empty fallback.
        """
        return self.columns.get(table, set())


def test_schema_support_reports_missing_tables_and_columns() -> None:
    """
    Report absent required tables and all three affected column groups using controlled schema
    declarations.

    Example:
        >>> test_schema_support_reports_missing_tables_and_columns()


    :return: None after the stated regression assertions pass.
    """
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
    """
    Accept the required Store/file/link schema even when an advertised derivation table is
    incomplete.

    Archive replication does not require derivation columns.

    Example:
        >>> test_schema_support_ignores_legacy_derivation_table()


    :return: None after the stated regression assertions pass.
    """
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
    """
    Accept the minimal required schema without a derivation table and return four inventory sets.

    Example:
        >>> test_schema_support_succeeds_without_optional_derivation_table()


    :return: None after the stated regression assertions pass.
    """
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
    """
    Coerce the listed scalar, pair, and mapping aliases to exact file-ID/optional-target pairs.

    Example:
        >>> test_designation_items_accept_current_input_shapes(item, expected)  # doctest: +SKIP


    :param item: Parameterized supported designation shape.
    :param expected: Exact expected integer ID and optional normalized text pair.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Reject missing identities, malformed pairs, invalid ID text, and an unsupported object.

    Example:
        >>> test_designation_items_reject_invalid_shapes(item)  # doctest: +SKIP


    :param item: Parameterized invalid designation value.
    :return: None after the stated regression assertions pass.
    """
    with pytest.raises(InputIntegrityError):
        sync._coerce_designation_item(item)


def test_open_store_creation_reopen_lock_and_directory_guards(
    mini_db,
    tmp_path: Path,
) -> None:
    """
    Create and rename/reopen one real SQLite target declaration, then reject its locked kind and an
    existing directory path.

    Assert retained identity and reopen state detail without invoking archive tools.

    Example:
        >>> test_open_store_creation_reopen_lock_and_directory_guards(mini_db, tmp_path)  # doctest: +SKIP


    :param mini_db: Mini SQLite fixture with real legacy tables and a shared connection.
    :param tmp_path: Temporary directory for source files and catalogue/archive target paths.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Resolve relative/absolute source keys and exercise create, unchanged, retarget, collision, and
    missing-row designation behavior.

    Use real bytes and SQLite rows, then alter source bytes to distinguish same-size hash drift from
    size drift. No archive is built.

    Example:
        >>> test_source_paths_and_designation_lifecycle(mini_db, tmp_path)  # doctest: +SKIP


    :param mini_db: Mini SQLite fixture with real legacy tables and a shared connection.
    :param tmp_path: Temporary directory for source files and catalogue/archive target paths.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Recover target/hash/size/mtime from source metadata and bytes after clearing a designation
    policy.

    Assert the legacy fallback produces a usable current snapshot without claiming an original
    historical snapshot.

    Example:
        >>> test_collect_designations_supports_legacy_policy_fallbacks(mini_db, tmp_path)  # doctest: +SKIP


    :param mini_db: Mini SQLite fixture with real legacy tables and a shared connection.
    :param tmp_path: Temporary directory for source files and catalogue/archive target paths.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Reject absent Store IDs/rows/keys, missing local files, non-open targets, missing target rows,
    and an empty designation set.

    Use a Row-like dict only for the invalid foreign-key case; other metadata and path checks use
    SQLite and real temporary files.

    Example:
        >>> test_designation_and_source_path_error_cases(mini_db, tmp_path)  # doctest: +SKIP


    :param mini_db: Mini SQLite fixture with real legacy tables and a shared connection.
    :param tmp_path: Temporary directory for source files and catalogue/archive target paths.
    :return: None after the stated regression assertions pass.
    """
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
        """
        Supply dict-style file fields and a fixed row_id for a nonexistent-Store lookup.

        This avoids inserting an invalid foreign key into the real SQLite fixture.

        Example:
            >>> _Rowish(file_store_id=999999, file_storage_key="source.epub").row_id  # doctest: +SKIP
            999999
        """
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
    """
    Persist building link policy, avoid duplicate primary links, and lock Store metadata from
    building state.

    The archive path need not contain an archive: these assertions cover metadata helpers and state
    inference, not verified publication.

    Example:
        >>> test_link_state_locking_primary_links_and_current_state(mini_db, tmp_path)  # doctest: +SKIP


    :param mini_db: Mini SQLite fixture with real legacy tables and a shared connection.
    :param tmp_path: Temporary directory for source files and catalogue/archive target paths.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Insert archive metadata and primary links on a shared SQLite connection, then reuse matching
    digests and reject a conflict.

    Exercise mapping and Row cache forms, tolerate an unsupported source column, and assert no
    derivation rows. This test does not inject rollback or prove multi-phase publication atomicity.

    Example:
        >>> test_duplicate_rows_and_primary_links_use_one_transaction(mini_db, tmp_path)  # doctest: +SKIP


    :param mini_db: Mini SQLite fixture with real legacy tables and a shared connection.
    :param tmp_path: Temporary directory for source files and catalogue/archive target paths.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Retain unrelated scratch keys while selecting recognized build fields and dropping an unknown
    field.

    Also assert the minimal state/time payload without a build report; hashes are metadata fixtures,
    not recalculated bytes.

    Example:
        >>> test_reproducibility_metadata_filters_unknown_build_fields()


    :return: None after the stated regression assertions pass.
    """
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
    """
    Persist a failed Store state after an injected builder exception and re-raise that exception
    under strict policy.

    Designation and source checks use real SQLite/files; archive construction is replaced and
    manager refresh is disabled.

    Example:
        >>> test_publish_build_failures_mark_store_failed_without_squashfs_tools(mini_db, tmp_path, monkeypatch)  # doctest: +SKIP


    :param mini_db: Mini SQLite fixture with real legacy tables and a shared connection.
    :param tmp_path: Temporary directory for source files and catalogue/archive target paths.
    :param monkeypatch: Pytest fixture restoring injected builder/backend/helper seams.
    :return: None after the stated regression assertions pass.
    """
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
        """
        Raise a deterministic builder failure without creating an archive.

        Example:
            >>> fail_build(output_archive=path)  # doctest: +SKIP


        :param _kwargs: Accepted builder keyword arguments, deliberately unused.
        :return: Never returns; raises RuntimeError with the builder-failure marker.
        """
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
    """
    Forward real SQLite operations while leaving connection ownership with the fixture.

    Example:
        >>> proxy = _NonClosingConnection(connection)  # doctest: +SKIP
    """
    def __init__(self, connection) -> None:
        """
        Retain the shared connection without opening another connection or transaction.

        Example:
            >>> proxy = _NonClosingConnection(connection)  # doctest: +SKIP


        :param connection: Fixture-owned SQLite connection.
        :return: None after storing the reference.
        """
        self.connection = connection

    def execute(self, *args: object, **kwargs: object):
        """
        Forward positional and keyword execute arguments to the real shared connection.

        Example:
            >>> cursor = proxy.execute("SELECT 1")  # doctest: +SKIP


        :param args: Positional execute arguments forwarded unchanged.
        :param kwargs: Keyword execute arguments forwarded unchanged.
        :return: Underlying cursor/result; SQL errors propagate.
        """
        return self.connection.execute(*args, **kwargs)

    def commit(self) -> None:
        """
        Commit pending work on the shared SQLite connection.

        Example:
            >>> proxy.commit()  # doctest: +SKIP


        :return: None after underlying commit returns; errors propagate.
        """
        self.connection.commit()

    def rollback(self) -> None:
        """
        Roll back pending work on the shared SQLite connection.

        Example:
            >>> proxy.rollback()  # doctest: +SKIP


        :return: None after underlying rollback returns; errors propagate.
        """
        self.connection.rollback()

    def close(self) -> None:
        """
        Leave the shared connection open so the fixture can inspect committed rows.

        Example:
            >>> proxy.close()  # doctest: +SKIP


        :return: None without closing the underlying connection.
        """
        return None


class _MixedArchiveBackend:
    """
    Simulate missing and hash-mismatched members without creating or reading an archive.

    Use a fixed Store UUID, prefix-based existence, and constant wrong payload bytes.

    Example:
        >>> backend = _MixedArchiveBackend(url="archive.squashfs", name=None)
        >>> backend.exists(backend.locate("missing/book.epub"))
        False
    """
    def __init__(self, *, url: str, name: str | None) -> None:
        """
        Retain display arguments and assign the fixed fixture Store identity.

        Example:
            >>> _MixedArchiveBackend(url="archive.squashfs", name=None).store_ref == UUID(int=1)
            True


        :param url: Archive display path, not opened.
        :param name: Optional display name retained as supplied.
        :return: None after initializing fixture state.
        """
        self.url = url
        self.name = name
        self.store_ref = UUID(int=1)

    def locate(self, key: str) -> Location:
        """
        Combine the fixture Store UUID and supplied key into a real Location value.

        Example:
            >>> _MixedArchiveBackend(url="archive", name=None).locate("book.epub").key
            'book.epub'


        :param key: Member key forwarded to Location construction.
        :return: Location value; no existence check occurs.
        """
        return Location(self.store_ref, key)

    @staticmethod
    def exists(location: Location) -> bool:
        """
        Declare only keys starting with missing/ absent.

        Example:
            >>> _MixedArchiveBackend.exists(Location(UUID(int=1), "missing/book.epub"))
            False


        :param location: Location whose key selects the canned result.
        :return: False for the missing/ prefix, otherwise True.
        """
        return not location.key.startswith("missing/")

    @staticmethod
    def stat(location: Location) -> FileInfo:
        """
        Return zero-sized FileInfo without a digest to force the publisher read fallback.

        Example:
            >>> _MixedArchiveBackend.stat(Location(UUID(int=1), "book.epub")).digest is None
            True


        :param location: Location echoed in the returned metadata.
        :return: Synthetic FileInfo with size zero and default metadata fields.
        """
        return FileInfo(location=location, size=0)

    @staticmethod
    def read_bytes(_location: Location) -> bytes:
        """
        Return constant bytes that differ from every source fixture in the mixed-outcome test.

        Example:
            >>> _MixedArchiveBackend.read_bytes(Location(UUID(int=1), "book.epub"))
            b'WRONG-ARCHIVE-PAYLOAD'


        :param _location: Accepted Location, ignored by the fixed response.
        :return: The fixed wrong-payload byte string.
        """
        return b"WRONG-ARCHIVE-PAYLOAD"


def test_publish_persists_missing_and_hash_mismatch_states(
    mini_db,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Commit missing/hash-mismatch link states and failed Store state with a fake builder/backend,
    then record bootstrap failure.

    Use real source bytes and SQLite transaction operations through a nonclosing proxy. The archive
    results and build hashes are synthetic; no archive tool runs.

    Example:
        >>> test_publish_persists_missing_and_hash_mismatch_states(mini_db, tmp_path, monkeypatch)  # doctest: +SKIP


    :param mini_db: Mini SQLite fixture with real legacy tables and a shared connection.
    :param tmp_path: Temporary directory for source files and catalogue/archive target paths.
    :param monkeypatch: Pytest fixture restoring injected builder/backend/helper seams.
    :return: None after the stated regression assertions pass.
    """
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
        """
        Assert strict clearing bootstrap flags, then inject a manager-refresh error.

        Example:
            >>> fail_bootstrap(clear_existing=True, strict=True)  # doctest: +SKIP


        :param clear_existing: Expected truthy manager-reset flag.
        :param strict: Expected truthy strict-bootstrap flag.
        :return: Never returns; failed flags assert, otherwise RuntimeError signals refresh failure.
        """
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
    """
    Refuse publication before building for a non-open kind, a building scratch state, or an absent
    root.

    Assertions use actual legacy rows in the mini catalogue.

    Example:
        >>> test_publish_rejects_non_open_invalid_state_and_missing_root(mini_db, tmp_path)  # doctest: +SKIP


    :param mini_db: Mini SQLite fixture with real legacy tables and a shared connection.
    :param tmp_path: Temporary directory for source files and catalogue/archive target paths.
    :return: None after the stated regression assertions pass.
    """
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
    """
    Forward target, int-converted IDs, duplication, build, strict, and refresh options through three
    recording doubles.

    Assert result identity without creating an archive or database rows.

    Example:
        >>> test_publish_convenience_wrapper_delegates_all_steps(monkeypatch, tmp_path)  # doctest: +SKIP


    :param monkeypatch: Pytest fixture restoring injected builder/backend/helper seams.
    :param tmp_path: Temporary directory for source files and catalogue/archive target paths.
    :return: None after the stated regression assertions pass.
    """
    calls: dict[str, object] = {}
    store_row = SimpleNamespace(row_id=7)

    def ensure(db, *, archive_path, store_name):
        """
        Record target-setup arguments and return the preselected Store identity object.

        Example:
            >>> row = ensure(db, archive_path=path, store_name="Archive")  # doctest: +SKIP


        :param db: Opaque database sentinel recorded unchanged.
        :param archive_path: Target path recorded unchanged.
        :param store_name: Requested display name recorded unchanged.
        :return: Closure-owned Store-like object with row_id 7.
        """
        calls["ensure"] = (db, archive_path, store_name)
        return store_row

    def designate(db, *, store_id, designations, replace_existing):
        """
        Record the ID list and replacement policy without creating links.

        Example:
            >>> designate(db, store_id=7, designations=[1, 2], replace_existing=False)  # doctest: +SKIP


        :param db: Opaque database sentinel.
        :param store_id: Target Store identity recorded by the wrapper.
        :param designations: Materialized integer ID list retained by reference.
        :param replace_existing: Replacement flag expected to remain false.
        :return: None after updating the closure call mapping.
        """
        calls["designate"] = (db, store_id, designations, replace_existing)

    expected = object()

    def publish(db, **kwargs):
        """
        Record publication options and return the exact expected result sentinel.

        Example:
            >>> result = publish(db, store_id=7)  # doctest: +SKIP


        :param db: Opaque database sentinel.
        :param kwargs: Publication keyword mapping retained in the closure.
        :return: Expected result object by identity, without publication.
        """
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

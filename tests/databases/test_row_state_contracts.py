"""Distinguish local drafts, missing loads and read-only Row method binding."""

from __future__ import annotations

from collections.abc import Callable
from copy import deepcopy
from typing import Literal

import pytest

from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.databases.row import FixedTableStorageRow, Row
from LiuXin_alpha.errors import InputIntegrityError, RowReadOnlyError
from tests.databases.test_row_write_safety import db as db, template as template


def test_empty_draft_exposes_live_keys_without_inserting(db: Database) -> None:
    row = Row(db)
    keys = row.keys()
    assert row.table is None and row.row_dict == {} and not keys
    with pytest.raises(InputIntegrityError):
        _ = row.row_id

    row["review_item_text"] = "local draft"
    assert row.table == "review_items" and row.row_id is None
    assert "review_item_text" in keys
    assert db.driver_wrapper.fetch_scalar("SELECT COUNT(*) FROM review_items") == 0


@pytest.mark.parametrize("missing", [False, None])
def test_missing_load_retains_identity_and_never_becomes_a_draft(
    db: Database, monkeypatch: pytest.MonkeyPatch, missing: Literal[False] | None
) -> None:
    row = Row(db, {"review_item_id": 7, "review_item_text": "unsaved"})
    monkeypatch.setattr(db.driver_wrapper, "get_row_from_id", lambda **kwargs: missing)
    row.load_row_from_id(99999, "review_items")
    assert row.row_dict is missing and row.int_row_dict is missing
    assert row.table == "review_items" and row.row_id == 99999
    row.sync()  # The existing missing-load sync behavior does not insert a record.

    operations: tuple[Callable[[], object], ...] = (
        row.keys,
        lambda: row["review_item_text"],
        lambda: row.__setitem__("review_item_text", "replacement"),
        lambda: list(row),
        lambda: "review_item_text" in row,
        row.ensure_row_has_id,
    )
    for operation in operations:
        with pytest.raises(TypeError, match="record was not found"):
            operation()
        assert row.row_dict is missing
    assert db.driver_wrapper.fetch_scalar("SELECT COUNT(*) FROM review_items") == 0


def test_backend_missing_load_preserves_its_sentinel(db: Database) -> None:
    row = Row(db)
    row.load_row_from_id(-99999, "review_items")
    assert row.row_dict is False or row.row_dict is None
    assert row.table == "review_items" and row.row_id == -99999
    assert db.get_row_from_id("review_items", -99999) is None


class ReviewItemRow(FixedTableStorageRow):
    TABLE_NAME = "review_items"
    validation_count: int = 0

    def validate(self) -> None:
        self.validation_count += 1


def test_read_only_binding_survives_flag_changes_and_copy_is_writable(
    db: Database,
) -> None:
    row_id = db.driver_wrapper.add_row({"review_item_text": "local"})
    row = ReviewItemRow(
        db, {"review_item_id": row_id, "review_item_text": "local"}, read_only=True
    )
    initial_checks = row.validation_count
    row["review_item_other"] = "still local"
    row.read_only = False
    with pytest.raises(RowReadOnlyError):
        row.sync()
    assert row.validation_count == initial_checks
    assert (
        db.driver_wrapper.fetch_scalar("SELECT review_item_other FROM review_items")
        is None
    )

    clone = deepcopy(row)
    assert type(clone) is Row and not clone.read_only
    clone.sync()
    assert (
        db.driver_wrapper.fetch_scalar("SELECT review_item_other FROM review_items")
        == "still local"
    )


def test_unidentified_draft_cannot_enter_table_specific_operations(
    db: Database,
) -> None:
    row = Row(db)
    with pytest.raises(InputIntegrityError, match="identified table"):
        db.get_children(row)
    with pytest.raises(InputIntegrityError, match="identified table"):
        db.get_intralink_rows(row)
    with pytest.raises(InputIntegrityError, match="identified table"):
        db.interlink_rows(row, row)


def test_facade_column_lookup_and_text_representation(db: Database) -> None:
    assert db.get_table_from_column("review_item_text") == "review_items"
    with pytest.raises(InputIntegrityError):
        db.get_table_from_column("missing_column")
    assert str(db) == db.__unicode__()
    assert "SQLite" in str(db)


def test_duplicate_rejects_missing_payload_before_allocating(db: Database) -> None:
    row = Row(db)
    row.load_row_from_id(-99999, "review_items")
    with pytest.raises(TypeError, match="record was not found"):
        db.dupe_row(row)
    assert db.driver_wrapper.fetch_scalar("SELECT COUNT(*) FROM review_items") == 0


def test_wrapper_rebinding_uses_the_declared_macro_setter(db):
    """A real wrapper has a read-only macros property and an explicit setter."""
    wrapper = db.driver_wrapper
    db.set_driver_wrapper(wrapper)
    assert db.driver_wrapper is wrapper
    assert wrapper.macros is db.macros


def test_raw_bulk_return_contracts_and_required_field(db):
    """Distinguish empty batches, completed writes and missing column inputs."""
    from LiuXin_alpha.errors import InputIntegrityError

    driver = db.driver
    assert driver.direct_add_multiple_simple_row_dicts(iter(())) is True
    assert driver.direct_update_columns({}) is None
    with pytest.raises(InputIntegrityError, match="column is required"):
        driver.direct_update_columns({1: "unassigned"})
    assert (
        driver.direct_add_multiple_simple_row_dicts(
            iter(({"review_item_text": "bulk contract"},))
        )
        is None
    )
    row = db.driver_wrapper.search("review_items", "review_item_text", "bulk contract")[
        0
    ]
    assert (
        driver.direct_update_row_dict({"review_item_id": row["review_item_id"]}) is True
    )
    assert (
        db.driver_wrapper.delete("review_items", "review_item_text", ["bulk contract"])
        is None
    )
    assert (
        db.driver_wrapper.search("review_items", "review_item_text", "bulk contract")
        == []
    )


def test_explicit_state_tracks_draft_identity_and_missing_load(db):
    from LiuXin_alpha.databases.row import RowState

    row = Row(db)
    assert row.state is RowState.DRAFT
    row["review_item_text"] = "draft state"
    assert row.state is RowState.DRAFT
    row.sync()
    assert row.state is RowState.IDENTIFIED
    identity = row.row_id
    row.load_row_from_id(-99999, "review_items")
    assert row.state is RowState.MISSING
    row.sync()
    assert row.state is RowState.MISSING
    row.load_row_from_id(identity, "review_items")
    assert row.state is RowState.IDENTIFIED


def test_failed_draft_sync_does_not_allocate_a_blank_row_or_identity(db):
    from LiuXin_alpha.errors import DatabaseIntegrityError

    db.driver_wrapper.add_row({"review_item_text": "duplicate"})
    row = Row(db, {"review_item_text": "duplicate"})
    with pytest.raises(DatabaseIntegrityError):
        row.sync()
    assert row.row_id is None
    assert row.row_dict == {"review_item_text": "duplicate"}
    assert db.driver_wrapper.fetch_scalar("SELECT COUNT(*) FROM review_items") == 1
    row["review_item_text"] = "retry"
    row.sync()
    assert row.row_id is not None
    assert db.driver_wrapper.fetch_scalar("SELECT COUNT(*) FROM review_items") == 2


def test_draft_sync_accepts_an_explicit_null_identity(db):
    from LiuXin_alpha.databases.row import Row, RowState

    row = Row(db, {"review_item_id": None, "review_item_text": "null identity"})
    row.sync()
    assert row.state is RowState.IDENTIFIED
    assert row.row_id is not None
    assert row.row_dict["review_item_id"] == row.row_id

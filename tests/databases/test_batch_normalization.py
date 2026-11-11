"""Check bounded policy reads without changing stored identities or atomic writes."""

from __future__ import annotations

from copy import deepcopy

import pytest


@pytest.mark.parametrize("route", ("macros", "driver"))
def test_batches_read_policy_once_and_refresh_between_calls(db, monkeypatch, route):
    """A database policy edit, including deletion, takes effect on the next batch."""
    reads = []
    original = db.driver.direct_iter_normalized_identity_specs

    def declarations(*, table=None):
        reads.append(table)
        return original(table=table)

    monkeypatch.setattr(
        db.driver, "direct_iter_normalized_identity_specs", declarations
    )

    def insert(rows):
        if route == "macros":
            db.macros.insert_rows("tags", iter(rows))
        else:
            db.driver.direct_add_multiple_simple_row_dicts(rows)

    rows = [{"tag": f"  Ａ Tag {i}  ", "tag_phash": "incorrect"} for i in range(50)]
    before = deepcopy(rows)
    insert(rows)
    assert reads == ["tags"]
    assert rows == before
    stored = db.macros.get_rows("tags", order_by=("tag_id",))
    assert [row["tag_phash"] for row in stored] == [f"a tag {i}" for i in range(50)]

    with db.macros.transaction() as conn:
        conn.execute(
            "UPDATE normalized_identities SET normalized_identity_normalization_profile = ? "
            "WHERE normalized_identity_table_name = ?",
            ("unicode_nfc_trim_casefold", "tags"),
        )
    reads.clear()
    insert([{"tag": "  Ｂ  Value  "}])
    assert reads == ["tags"]
    assert (
        db.macros.get_rows("tags", where={"tag": "  Ｂ  Value  "})[0]["tag_phash"]
        == "ｂ  value"
    )

    with db.macros.transaction() as conn:
        conn.execute(
            "DELETE FROM normalized_identities WHERE normalized_identity_table_name = 'tags'"
        )
    reads.clear()
    insert([{"tag": "Unspecified", "tag_phash": "supplied-key"}])
    assert reads == ["tags"]
    assert (
        db.macros.get_rows("tags", where={"tag": "Unspecified"})[0]["tag_phash"]
        == "supplied-key"
    )


def test_batch_updates_repair_keys_from_uncommitted_display_values(db, monkeypatch):
    """One policy snapshot still reads key-only updates on the writer connection."""
    macros = db.macros
    macros.insert_rows("tags", [{"tag": "First"}, {"tag": "Second"}])
    ids = [row["tag_id"] for row in macros.get_rows("tags", order_by=("tag_id",))]
    original = db.driver.direct_iter_normalized_identity_specs
    reads = []

    def declarations(*, table=None):
        reads.append(table)
        return original(table=table)

    monkeypatch.setattr(
        db.driver, "direct_iter_normalized_identity_specs", declarations
    )
    with pytest.raises(RuntimeError, match="undo"):
        with macros.transaction() as conn:
            conn.execute(
                "UPDATE tags SET tag = '  Pending  Value  ' WHERE tag_id = ?", (ids[0],)
            )
            macros.update_rows(
                "tags", {row_id: {"tag_phash": "wrong"} for row_id in ids}
            )
            assert reads == ["tags"]
            assert macros.get_row("tags", ids[0])["tag_phash"] == "pending value"
            assert macros.get_row("tags", ids[1])["tag_phash"] == "second"
            raise RuntimeError("undo")
    assert macros.get_row("tags", ids[0])["tag_phash"] == "first"


def test_empty_batches_do_not_load_normalization_policy(db, monkeypatch):
    """No-op inputs cannot fail on unrelated normalization declarations."""

    def unexpected(**kwargs):
        raise AssertionError("empty batch read policy")

    monkeypatch.setattr(db.driver, "direct_iter_normalized_identity_specs", unexpected)
    db.macros.insert_rows("tags", iter(()))
    db.macros.update_rows("tags", {})
    db.macros.update_rows("tags", {1: {}})
    assert db.driver.direct_add_multiple_simple_row_dicts([]) is True

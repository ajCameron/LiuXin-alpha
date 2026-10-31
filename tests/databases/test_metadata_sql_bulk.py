"""Exercise table batches and data-only identity repair on the real schema."""

from contextlib import contextmanager

import pytest

from LiuXin_alpha.errors import DatabaseIntegrityError, InputIntegrityError


@pytest.mark.parametrize("table,value_column", [("tags", "tag"), ("series", "series")])
def test_vocabulary_batches_preserve_display_and_share_scalar_identity(
    db, table, value_column
):
    sql = db.metadata_sql
    ensure = getattr(sql, f"ensure_{table}")
    lookup = getattr(sql, f"get_{value_column}_ids_from_values")
    existing = db.macros.ensure_table_value(table, value_column, "Café History")
    values = ("  CAFE\u0301   HISTORY ", "New Value", " NEW VALUE ", "New Value")
    result = ensure(iter(values))
    assert result[values[0]] == existing
    assert result["New Value"] == result[" NEW VALUE "]
    assert len(result) == 3
    assert db.macros.get_row(table, existing)[value_column] == "Café History"
    assert db.macros.get_row(table, result["New Value"])[value_column] == "New Value"
    assert lookup(["café history", "new value", "absent"]) == {
        "café history": existing,
        "new value": result["New Value"],
        "absent": None,
    }
    assert ensure([]) == lookup([]) == {}
    with pytest.raises(RuntimeError, match="rollback"):
        with db.macros.transaction():
            ensure(["Pending"])
            getattr(sql, f"delete_{table}_by_id")([existing, existing, -1])
            assert lookup(["café history"]) == {"café history": None}
            raise RuntimeError("rollback")
    assert lookup(["Pending", "café history"]) == {
        "Pending": None,
        "café history": existing,
    }
    getattr(sql, f"delete_{table}_by_id")([existing])
    assert lookup(["café history"]) == {"café history": None}


@pytest.mark.parametrize("bad_input", ["blank", "generator"])
def test_ensure_preflights_all_input_before_insertion(db, bad_input):
    def values():
        yield "First"
        if bad_input == "blank":
            yield "   "
        else:
            raise RuntimeError("input failed")

    error = InputIntegrityError if bad_input == "blank" else RuntimeError
    with pytest.raises(error):
        db.metadata_sql.ensure_tags(values())
    assert db.metadata_sql.get_tag_id_from_value("First") is None


def test_batch_ensure_scopes_null_parents_and_retains_scalar_fallback(db):
    macros = db.macros
    parent = macros.ensure_table_value(
        "genres", "genre", "Parent", additional_values={"genre_parent_id": None}
    )
    roots = macros.ensure_table_values(
        "genres",
        "genre",
        ["History", " HISTORY "],
        additional_values={"genre_parent_id": None},
    )
    children = macros.ensure_table_values(
        "genres", "genre", ["History"], additional_values={"genre_parent_id": parent}
    )
    assert roots["History"] == roots[" HISTORY "] != children["History"]
    assert macros.find_table_values(
        "genres", "genre", ["history"], additional_values={"genre_parent_id": parent}
    ) == {"history": children["History"]}
    with pytest.raises(InputIntegrityError, match="scope"):
        macros.ensure_table_values("genres", "genre", ["Unscoped"])
    works = macros.ensure_table_values(
        "works", "work_title", ["Die Straße", "DIE STRASSE"]
    )
    assert works["Die Straße"] == works["DIE STRASSE"]
    assert macros.find_table_values(
        "works", "work_title", ["die strasse", "Missing"]
    ) == {"die strasse": works["Die Straße"], "Missing": None}


def test_bulk_ensure_uses_bounded_key_lookups(db, monkeypatch):
    """Count data SELECTs, not wall time: 1,001 identities need six lookups."""
    original = db.macros._macro_transaction
    lookups = []
    batches = []

    class CountedConnection:
        def execute(self, sql, bindings=()):
            if sql.startswith("SELECT") and 'FROM "tags"' in sql:
                lookups.append(len(bindings))
            return self.connection.execute(sql, bindings)

        def executemany(self, sql, bindings):
            bindings = tuple(bindings)
            batches.append(len(bindings))
            return self.connection.executemany(sql, bindings)

    @contextmanager
    def counted_transaction():
        with original() as connection:
            counted = CountedConnection()
            counted.connection = connection
            yield counted

    monkeypatch.setattr(db.macros, "_macro_transaction", counted_transaction)
    result = db.metadata_sql.ensure_tags(f"Tag {index}" for index in range(1001))
    assert len(set(result.values())) == 1001
    assert lookups == [500, 500, 1, 500, 500, 1]
    assert batches == [500, 500, 1]
    lookups.clear()
    assert db.metadata_sql.ensure_tags(result) == result
    assert lookups == [500, 500, 1]
    assert batches == [500, 500, 1]


def test_ensure_rolls_back_insertions_on_unresolved_unique_conflict(db):
    with db.macros.transaction() as conn:
        conn.execute("CREATE UNIQUE INDEX reject_second_tag ON tags (length(tag))")
    with pytest.raises(DatabaseIntegrityError, match="Could not ensure"):
        db.metadata_sql.ensure_tags(["First", "Other"])
    assert not db.macros.get_rows("tags")


def test_file_batches_update_metadata_without_reading_or_touching_bytes(
    db, tmp_path, monkeypatch
):
    path = tmp_path / "source.epub"
    path.write_bytes(b"preserve source")
    first = db.macros.insert_row("files", {"file_name": path.name})
    second = db.macros.insert_row("files", {"file_name": "other.epub"})

    def unexpected_read(*args, **kwargs):
        pytest.fail("File updates should not read each row")

    with monkeypatch.context() as patch:
        patch.setattr(db.macros, "get_row", unexpected_read)
        db.metadata_sql.update_files_bulk(
            {
                first: {"file_name": "renamed.epub", "file_size_bytes": 15},
                second: {"file_size_bytes": 8},
                -1: {"file_name": "absent"},
            }
        )
    assert db.macros.get_row("files", first)["file_name"] == "renamed.epub"
    assert db.macros.get_row("files", second)["file_size_bytes"] == 8
    before = db.macros.get_rows("files")
    with pytest.raises(RuntimeError, match="rollback"):
        with db.macros.transaction():
            db.metadata_sql.update_files_bulk(
                {first: {"file_name": "pending"}, second: {"file_name": "pending too"}}
            )
            raise RuntimeError("rollback")
    assert db.macros.get_rows("files") == before
    db.metadata_sql.delete_files_by_id([first, second])
    assert path.read_bytes() == b"preserve source"


def test_batch_updates_validate_all_payloads_and_rollback_late_constraint_failure(db):
    first = db.macros.insert_row("files", {"file_name": "keep"})
    second = db.macros.insert_row("files", {"file_name": "also keep"})
    with pytest.raises(ValueError, match="file_name and file_size_bytes"):
        db.metadata_sql.update_files_bulk(
            {first: {"file_name": "pending"}, second: {"file_size": 12}}
        )
    with db.macros.transaction() as conn:
        conn.execute(
            "CREATE TRIGGER reject_file_update BEFORE UPDATE OF file_name ON files WHEN NEW.file_name = 'fail' BEGIN SELECT RAISE(ABORT, 'test failure'); END"
        )
    before = db.macros.get_rows("files")
    with pytest.raises(Exception, match="test failure"):
        db.metadata_sql.update_files_bulk(
            {first: {"file_name": "pending"}, second: {"file_name": "fail"}}
        )
    assert db.macros.get_rows("files") == before
    db.metadata_sql.update_files_bulk({first: {}, -1: {"file_name": "absent"}})
    assert db.macros.get_rows("files") == before


def test_bulk_delete_chunks_and_rolls_back_earlier_chunks(db):
    ids = list(db.metadata_sql.ensure_tags(f"Tag {i}" for i in range(502)).values())
    with db.macros.transaction() as conn:
        conn.execute(
            f"CREATE TRIGGER reject_tag_delete BEFORE DELETE ON tags WHEN OLD.tag_id = {ids[-1]} BEGIN SELECT RAISE(ABORT, 'test failure'); END"
        )
    with pytest.raises(Exception, match="test failure"):
        db.metadata_sql.delete_tags_by_id([*ids, ids[0], -1])
    assert len(db.macros.get_rows("tags")) == 502
    with db.macros.transaction() as conn:
        conn.execute("DROP TRIGGER reject_tag_delete")
    db.metadata_sql.delete_tags_by_id([*ids[:-1], ids[0], -1])
    assert [r["tag_id"] for r in db.macros.get_rows("tags")] == ids[-1:]


@pytest.mark.parametrize("bad_id", [True, "1", ("work", 1), 1.0])
def test_batch_row_ids_reject_non_integer_identifiers_before_effects(db, bad_id):
    row_id = db.metadata_sql.add_tag("Keep")
    with pytest.raises(InputIntegrityError, match="integers"):
        db.macros.delete_rows_by_id("tags", [row_id, bad_id])
    with pytest.raises(InputIntegrityError, match="integers"):
        db.macros.update_rows("tags", {bad_id: {"tag": "changed"}})
    assert db.macros.get_row("tags", row_id)["tag"] == "Keep"


def test_generic_batches_reject_views_and_unknown_columns(db):
    with db.macros.transaction() as conn:
        conn.execute("CREATE VIEW batch_tag_view AS SELECT * FROM tags")
    for table in ("batch_tag_view", "missing_table"):
        with pytest.raises(InputIntegrityError, match="physical table"):
            db.macros.delete_rows_by_id(table, [])
        with pytest.raises(InputIntegrityError, match="physical table"):
            db.macros.update_rows(table, {})
        with pytest.raises(InputIntegrityError, match="physical table"):
            db.macros.find_table_values(table, "tag", [])
        with pytest.raises(InputIntegrityError, match="physical table"):
            db.macros.audit_normalized_keys(table)
    row_id = db.metadata_sql.add_tag("Keep")
    for payload in ({"tag_id": 42}, {"missing_column": "value"}):
        with pytest.raises(InputIntegrityError):
            db.macros.update_rows("tags", {row_id: payload})


def test_generic_updates_derive_keys_and_ignore_forged_key_only_payloads(db):
    ids = db.metadata_sql.ensure_tags(["First", "Second"])
    db.macros.update_rows(
        "tags",
        {
            ids["First"]: {"tag": " NEW   VALUE ", "tag_phash": "forged"},
            ids["Second"]: {"tag_phash": "forged"},
        },
    )
    assert db.macros.get_row("tags", ids["First"])["tag_phash"] == "new value"
    assert db.macros.get_row("tags", ids["Second"])["tag_phash"] == "second"


def _set_tag_keys(db, replacements):
    """Simulate stale stored keys without invoking normalized-value preparation."""
    with db.macros.transaction() as conn:
        conn.executemany(
            "UPDATE tags SET tag_phash = NULL WHERE tag_id = ?",
            [(row_id,) for row_id in replacements],
        )
        conn.executemany(
            "UPDATE tags SET tag_phash = ? WHERE tag_id = ?",
            [(key, row_id) for row_id, key in replacements.items()],
        )


def test_normalized_key_repair_is_scoped_data_only_and_can_be_rolled_back(db):
    ids = db.metadata_sql.ensure_tags(["Café", "Other"])
    _set_tag_keys(db, {ids["Café"]: "stale cafe", ids["Other"]: "stale other"})
    before = db.macros.get_rows("tags")
    schema = db.get("SELECT name, type, sql FROM sqlite_master ORDER BY name")
    declarations = db.macros.get_rows("normalized_identities")
    report = db.metadata_sql.audit_normalized_keys("tags", row_ids=[ids["Café"]])
    assert (report.rows_examined, report.rows_needing_update, report.rows_updated) == (
        2,
        1,
        0,
    )
    assert not report.collisions
    assert db.macros.get_rows("tags") == before
    with pytest.raises(RuntimeError, match="rollback"):
        with db.macros.transaction():
            repaired = db.metadata_sql.rebuild_normalized_keys(
                "tags", value_columns=["tag"], row_ids=[ids["Café"]]
            )
            assert repaired.rows_updated == 1
            raise RuntimeError("rollback")
    assert db.macros.get_rows("tags") == before
    repaired = db.metadata_sql.rebuild_normalized_keys("tags", row_ids=[ids["Café"]])
    assert repaired.rows_updated == 1
    assert repaired.columns_added == repaired.indexes_created == ()
    assert db.macros.get_row("tags", ids["Café"])["tag"] == "Café"
    assert db.macros.get_row("tags", ids["Café"])["tag_phash"] == "café"
    assert db.macros.get_row("tags", ids["Other"])["tag_phash"] == "stale other"
    assert db.get("SELECT name, type, sql FROM sqlite_master ORDER BY name") == schema
    assert db.macros.get_rows("normalized_identities") == declarations
    assert (
        db.metadata_sql.rebuild_normalized_keys("tags", row_ids=[]).rows_examined == 0
    )
    assert (
        db.metadata_sql.rebuild_normalized_keys("tags", row_ids=[-1]).rows_updated == 0
    )
    assert (
        db.metadata_sql.rebuild_normalized_keys("tags", value_columns=[]).rows_updated
        == 0
    )


def test_selected_key_collision_uses_unselected_stored_keys_and_full_repair_supports_swaps(
    db,
):
    ids = db.metadata_sql.ensure_tags(["First", "Second"])
    _set_tag_keys(db, {ids["First"]: "second", ids["Second"]: "first"})
    report = db.metadata_sql.audit_normalized_keys("tags", row_ids=[ids["First"]])
    assert len(report.collisions) == 1
    assert set(report.collisions[0].row_ids) == set(ids.values())
    before = db.macros.get_rows("tags")
    with pytest.raises(DatabaseIntegrityError, match="collisions"):
        db.metadata_sql.rebuild_normalized_keys("tags", row_ids=[ids["First"]])
    assert db.macros.get_rows("tags") == before
    assert db.metadata_sql.rebuild_normalized_keys("tags").rows_updated == 2
    assert db.metadata_sql.audit_normalized_keys("tags").rows_needing_update == 0


def test_key_repair_rolls_back_clear_phase_and_earlier_updates_on_failure(db):
    ids = db.metadata_sql.ensure_tags(["First", "Second"])
    _set_tag_keys(db, {row_id: f"stale {row_id}" for row_id in ids.values()})
    with db.macros.transaction() as conn:
        conn.execute(
            "CREATE TRIGGER reject_key_repair BEFORE UPDATE OF tag_phash ON tags WHEN NEW.tag_phash = 'second' BEGIN SELECT RAISE(ABORT, 'test failure'); END"
        )
    before = db.macros.get_rows("tags")
    with pytest.raises(Exception, match="test failure"):
        db.metadata_sql.rebuild_normalized_keys("tags")
    assert db.macros.get_rows("tags") == before


def test_key_repair_respects_authoritative_empty_declarations_and_column_selection(db):
    with pytest.raises(InputIntegrityError, match="declared"):
        db.metadata_sql.audit_normalized_keys("files")
    with pytest.raises(InputIntegrityError, match="declared"):
        db.metadata_sql.rebuild_normalized_keys("tags", value_columns=["tag_phash"])
    with db.macros.transaction() as conn:
        conn.execute("DELETE FROM normalized_identities")
    with pytest.raises(InputIntegrityError, match="declared"):
        db.metadata_sql.rebuild_normalized_keys("tags")


def test_normalization_collision_detection_respects_parent_scope_and_null_roots(db):
    macros = db.macros
    parent = macros.ensure_table_value(
        "genres", "genre", "Parent", additional_values={"genre_parent_id": None}
    )
    root = macros.ensure_table_value(
        "genres", "genre", "History", additional_values={"genre_parent_id": None}
    )
    child = macros.ensure_table_value(
        "genres", "genre", "History", additional_values={"genre_parent_id": parent}
    )
    other = macros.ensure_table_value(
        "genres", "genre", "Other", additional_values={"genre_parent_id": None}
    )
    with macros.transaction() as conn:
        conn.execute(
            "UPDATE genres SET genre = ' HISTORY ' WHERE genre_id = ?", (other,)
        )
    report = db.metadata_sql.audit_normalized_keys("genres")
    assert len(report.collisions) == 1
    assert set(report.collisions[0].row_ids) == {root, other}
    assert report.collisions[0].scope_values == {"genre_parent_id": None}
    # A conflict elsewhere in the table must not block a selected child's repair.
    with macros.transaction() as conn:
        conn.execute(
            "UPDATE genres SET genre_phash = 'stale' WHERE genre_id = ?", (child,)
        )
    report = db.metadata_sql.rebuild_normalized_keys("genres", row_ids=[child])
    assert report.rows_updated == 1
    assert not report.collisions
    assert macros.get_row("genres", child)["genre_phash"] == "history"

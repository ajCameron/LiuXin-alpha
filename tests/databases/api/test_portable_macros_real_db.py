from __future__ import annotations

import uuid

import pytest

from LiuXin_alpha.databases.macro_types import LinkValue
from LiuXin_alpha.databases.schema_specs import StorageColumnSpec, StorageLinkSpec


def _column(name: str, ordinal: int, *, primary: bool = False) -> StorageColumnSpec:
    return StorageColumnSpec(
        name=name,
        ordinal=ordinal,
        declared_type="INTEGER" if name.endswith("_id") or name == "priority" else "TEXT",
        is_primary_key=primary,
    )


def test_portable_macros_through_real_database_wiring(db):
    db.driver_wrapper.executescript(
        """
        CREATE TABLE macro_test_left (
            macro_test_left_id INTEGER PRIMARY KEY,
            macro_test_left_name TEXT
        );
        CREATE TABLE macro_test_right (
            macro_test_right_id INTEGER PRIMARY KEY,
            macro_test_right_name TEXT
        );
        CREATE TABLE macro_test_links (
            macro_test_link_id INTEGER PRIMARY KEY,
            macro_test_link_left_id INTEGER NOT NULL,
            macro_test_link_right_id INTEGER NOT NULL,
            macro_test_link_type TEXT,
            macro_test_link_priority INTEGER,
            macro_test_link_note TEXT,
            UNIQUE(macro_test_link_left_id, macro_test_link_right_id),
            UNIQUE(macro_test_link_left_id, macro_test_link_priority)
        );
        INSERT INTO macro_test_left VALUES (1, 'left one'), (2, 'left two');
        INSERT INTO macro_test_right VALUES (10, 'ten'), (11, 'eleven'), (12, 'twelve');
        """
    )
    spec = StorageLinkSpec(
        primary_table="macro_test_left",
        secondary_table="macro_test_right",
        link_table="macro_test_links",
        primary_id_col="macro_test_left_id",
        secondary_id_col="macro_test_right_id",
        primary_link_col="macro_test_link_left_id",
        secondary_link_col="macro_test_link_right_id",
        type_link_col="macro_test_link_type",
        priority_link_col="macro_test_link_priority",
        typed=True,
        ordered=True,
        extra_link_columns=(
            _column("macro_test_link_id", 0, primary=True),
            _column("macro_test_link_note", 5),
        ),
    )

    rows = db.macros.replace_links(
        spec,
        1,
        (
            LinkValue(10, link_type="author", extra={"macro_test_link_note": "kept"}),
            LinkValue(11, link_type="editor"),
        ),
    )
    assert [row.secondary_id for row in rows] == [10, 11]
    assert [row.priority for row in rows] == [2, 1]

    rows = db.macros.replace_links(
        spec,
        1,
        (
            LinkValue(11, link_type="editor"),
            LinkValue(10, link_type="author"),
            LinkValue(12, link_type="translator"),
        ),
    )
    assert [row.secondary_id for row in rows] == [11, 10, 12]
    assert next(row for row in rows if row.secondary_id == 10).extra[
        "macro_test_link_note"
    ] == "kept"

    grouped = db.macros.get_link_rows_bulk(spec, (1, 2))
    assert len(grouped[1]) == 3
    assert grouped[2] == ()

    digest = db.macros.fingerprint_table(
        "macro_test_links",
        (
            "macro_test_link_left_id",
            "macro_test_link_right_id",
            "macro_test_link_type",
            "macro_test_link_priority",
        ),
    )
    assert digest == db.macros.fingerprint_table(
        "macro_test_links",
        (
            "macro_test_link_left_id",
            "macro_test_link_right_id",
            "macro_test_link_type",
            "macro_test_link_priority",
        ),
    )

    with db.macros.temporary_id_table((10, 11, 12)) as temp_table:
        count = db.driver.conn.execute(
            f'SELECT COUNT(*) FROM temp."{temp_table}"'
        ).fetchone()[0]
        assert count == 3

    tag_text = f"Portable Macro {uuid.uuid4().hex}"
    tag_id = db.macros.ensure_table_value("tags", "tag", tag_text)
    assert db.macros.ensure_table_value(
        "tags",
        "tag",
        tag_text.swapcase(),
    ) == tag_id


def test_portable_row_crud_and_nested_transaction_rollback(db):
    db.driver_wrapper.executescript(
        """
        CREATE TABLE macro_row_crud (
            macro_row_crud_id INTEGER PRIMARY KEY,
            macro_row_crud_name TEXT NOT NULL,
            macro_row_crud_group TEXT
        );
        """
    )

    with db.macros.transaction():
        first_id = db.macros.insert_row(
            "macro_row_crud",
            {
                "macro_row_crud_name": "first",
                "macro_row_crud_group": "kept",
            },
        )
        with db.macros.transaction():
            second_id = db.macros.insert_row(
                "macro_row_crud",
                {
                    "macro_row_crud_name": "second",
                    "macro_row_crud_group": "kept",
                },
            )
            db.macros.update_row(
                "macro_row_crud",
                first_id,
                {"macro_row_crud_name": "first updated"},
            )

    assert db.macros.get_row("macro_row_crud", first_id) == {
        "macro_row_crud_id": first_id,
        "macro_row_crud_name": "first updated",
        "macro_row_crud_group": "kept",
    }
    assert [
        row["macro_row_crud_id"]
        for row in db.macros.get_rows(
            "macro_row_crud",
            where={"macro_row_crud_group": "kept"},
            order_by=("macro_row_crud_id",),
        )
    ] == [first_id, second_id]

    with pytest.raises(RuntimeError, match="force rollback"):
        with db.macros.transaction():
            db.macros.update_row(
                "macro_row_crud",
                first_id,
                {"macro_row_crud_name": "must roll back"},
            )
            db.macros.delete_row("macro_row_crud", second_id)
            raise RuntimeError("force rollback")

    assert db.macros.get_row("macro_row_crud", first_id)[
        "macro_row_crud_name"
    ] == "first updated"
    assert db.macros.get_row("macro_row_crud", second_id) is not None

    db.macros.delete_row("macro_row_crud", second_id)
    assert db.macros.get_row("macro_row_crud", second_id) is None


def test_real_link_spec_detects_type_as_part_of_nonexclusive_identity(db):
    candidates = [
        spec
        for spec in db.driver_wrapper.iter_link_specs(force_refresh=True)
        if spec.typed and spec.type_part_of_identity
    ]
    assert candidates, "FRBR schema should expose at least one non-exclusive typed link"


def test_unique_group_introspection_ignores_partial_indexes(db):
    db.driver_wrapper.executescript(
        """
        CREATE TABLE macro_partial_unique (
            left_id INTEGER NOT NULL,
            right_id INTEGER NOT NULL,
            active INTEGER NOT NULL
        );
        CREATE UNIQUE INDEX macro_partial_unique_active
          ON macro_partial_unique(left_id, right_id)
          WHERE active = 1;
        CREATE UNIQUE INDEX macro_expression_unique
          ON macro_partial_unique(LOWER(left_id), right_id);
        """
    )

    groups = db.driver._get_unique_column_groups("macro_partial_unique")
    assert ("left_id", "right_id") not in groups
    assert ("right_id",) not in groups

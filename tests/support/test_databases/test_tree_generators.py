from __future__ import annotations

from datetime import datetime, timedelta
from itertools import cycle

from LiuXin_alpha.databases.database import Database
from tests.support.test_databases._tree_generators import generate_test_tree, generate_test_tree_with_datestamps


def _new_subject_root(db, name: str):
    row = db.get_blank_row("subjects")
    row["subject"] = name
    row.sync()
    return row


def test_generate_test_tree_preserves_legacy_subject_tree_shape(
    provision_test_database, driver_spec
) -> None:
    provisioned = provision_test_database("test_db_1")

    with Database(
        metadata={"database_path": str(provisioned.db_path)},
        db_type=driver_spec.db_type,
        create=False,
        backup=False,
    ) as db:
        subject_count_before = db.driver_wrapper.get_record_count("subjects")
        subject_row = _new_subject_root(db, "ROOT FOR SUBJECT TREE 1")

        generate_test_tree(root_row=subject_row, parent_position=True, seed=95340)

        subject_count_after = db.driver_wrapper.get_record_count("subjects")
        assert subject_count_after - subject_count_before == 28


def test_generate_test_tree_accepts_python3_iterators_for_generated_names(
    provision_test_database, driver_spec
) -> None:
    provisioned = provision_test_database("test_db_1")

    with Database(
        metadata={"database_path": str(provisioned.db_path)},
        db_type=driver_spec.db_type,
        create=False,
        backup=False,
    ) as db:
        subject_row = _new_subject_root(db, "ROOT FOR SUBJECT TREE 1")

        generate_test_tree(
            root_row=subject_row,
            row_name_str="ROW ID - {} TEST TREE - {}",
            uuid_stream=cycle(["1aa", "2bb", "3cc", "4dd", "5ee"]),
            parent_position=True,
            seed=95340,
        )

        generated_names = [
            row[0]
            for row in db.execute(
                "SELECT subject FROM subjects WHERE subject_id > ? ORDER BY subject_id",
                (subject_row.row_id,),
            )
        ]
        assert generated_names
        assert any("2bb" in str(name) for name in generated_names)
        assert all(str(name).startswith("ROW ID - ") for name in generated_names)


def test_generate_test_tree_with_datestamps_assigns_monotonic_values(
    provision_test_database, driver_spec
) -> None:
    provisioned = provision_test_database("test_db_1")

    with Database(
        metadata={"database_path": str(provisioned.db_path)},
        db_type=driver_spec.db_type,
        create=False,
        backup=False,
    ) as db:
        subject_row = _new_subject_root(db, "ROOT FOR DATED TREE")
        datestamp_col = db.driver_wrapper.get_datestamp_column("subjects")

        start = datetime(2020, 1, 1, 12, 0, 0)
        delta = timedelta(days=1)

        generate_test_tree_with_datestamps(
            root_row=subject_row,
            datestamp_col=datestamp_col,
            datestamp_start=start,
            datestamp_delta=delta,
            row_name_str="DATED {} {}",
            uuid_stream=cycle(["aa", "bb", "cc", "dd"]),
            parent_position=True,
            seed=95340,
        )

        generated = list(
            db.execute(
                f"""
                SELECT `{datestamp_col}`
                FROM subjects
                WHERE subject_id > ?
                ORDER BY subject_id
                """,
                (subject_row.row_id,),
            )
        )
        assert generated

        values = [row[0] for row in generated]
        expected = [str(start + (delta * i)) for i in range(len(values))]
        assert values == expected

from __future__ import annotations

from pathlib import Path

from tests.support.test_databases._semantic_fixture_builders import (
    build_base_profiled_db,
    create_custom_column,
    finalize_fixture,
    insert_custom_normalized_value,
    insert_custom_scalar_value,
    open_fixture_database,
    open_fixture_db,
    ordered_ids,
)


DB_NAME = "custom_columns_populated_db_1"


def populate_bundle(bundle_dir: Path) -> None:
    bundle_dir = Path(bundle_dir)
    bundle_dir.mkdir(parents=True, exist_ok=True)
    db_path = build_base_profiled_db(bundle_dir=bundle_dir, db_name=DB_NAME, books=4)

    conn = open_fixture_db(db_path)
    try:
        work_ids = ordered_ids(conn, "works", "work_id")
        if len(work_ids) != 4:
            raise AssertionError(f"Unexpected seeded work count for {DB_NAME}")
        for work_id, title in zip(
            work_ids,
            ("Advanced Custom Book One", "Advanced Custom Book Two", "Advanced Custom Book Three", "Advanced Custom Book Four"),
            strict=True,
        ):
            conn.execute(
                "UPDATE works SET work_title = ?, work_canonical_title = ?, work_sort_title = ?, work_creator_sort = ? WHERE work_id = ?;",
                (title, title, title, title, work_id),
            )
        conn.commit()
    finally:
        conn.close()

    db = open_fixture_database(db_path, storage_startup_on_add=False)
    try:
        tags_num, tags_table, tags_link_table = create_custom_column(
            db,
            label="curator_tags",
            name="Curator Tags",
            datatype="text",
            is_multiple=True,
            table="works",
        )
        series_num, series_table, series_link_table = create_custom_column(
            db,
            label="reading_order",
            name="Reading Order",
            datatype="series",
            is_multiple=False,
            table="works",
        )
        float_num, float_table, _ = create_custom_column(
            db,
            label="priority_score",
            name="Priority Score",
            datatype="float",
            is_multiple=False,
            table="works",
        )
        bool_num, bool_table, _ = create_custom_column(
            db,
            label="reference_flag",
            name="Reference Flag",
            datatype="bool",
            is_multiple=False,
            table="works",
        )
        comments_num, comments_table, _ = create_custom_column(
            db,
            label="review_blob",
            name="Review Blob",
            datatype="comments",
            is_multiple=False,
            table="works",
        )
        if tags_link_table is None or series_link_table is None:
            raise AssertionError(f"Expected normalized custom column link tables for {DB_NAME}")
    finally:
        db.close()

    conn = open_fixture_db(db_path)
    try:
        for target_id, value in (
            (work_ids[0], "featured"),
            (work_ids[0], "reference"),
            (work_ids[1], "translated"),
            (work_ids[2], "serial"),
            (work_ids[3], "staff-pick"),
        ):
            insert_custom_normalized_value(conn, cc_table=tags_table, link_table=tags_link_table, target_id=target_id, value=value)

        for target_id, value, extra in (
            (work_ids[0], "Cycle A", 1.0),
            (work_ids[1], "Cycle A", 2.0),
            (work_ids[2], "Cycle B", 1.5),
            (work_ids[3], "Cycle A", 3.0),
        ):
            insert_custom_normalized_value(
                conn,
                cc_table=series_table,
                link_table=series_link_table,
                target_id=target_id,
                value=value,
                extra=extra,
            )

        for target_id, value in (
            (work_ids[0], 9.5),
            (work_ids[1], 6.25),
            (work_ids[2], 4.0),
            (work_ids[3], 8.75),
        ):
            insert_custom_scalar_value(conn, cc_table=float_table, target_id=target_id, value=value)

        for target_id, value in (
            (work_ids[0], 1),
            (work_ids[2], 1),
            (work_ids[3], 0),
        ):
            insert_custom_scalar_value(conn, cc_table=bool_table, target_id=target_id, value=value)

        for target_id, value in (
            (work_ids[0], "<p>Review blob one.</p>"),
            (work_ids[1], "<p>Review blob two.</p>"),
            (work_ids[3], "<p>Review blob four.</p>"),
        ):
            insert_custom_scalar_value(conn, cc_table=comments_table, target_id=target_id, value=value)

        finalize_fixture(conn, db_name=DB_NAME)
    finally:
        conn.close()


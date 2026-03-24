from __future__ import annotations

from pathlib import Path

from tests.support.test_databases._semantic_fixture_builders import (
    build_base_profiled_db,
    create_custom_column,
    finalize_fixture,
    insert_custom_normalized_value,
    insert_custom_scalar_value,
    open_fixture_database,
    ordered_ids,
    open_fixture_db,
)


DB_NAME = "custom_columns_populated_db_0"


def populate_bundle(bundle_dir: Path) -> None:
    bundle_dir = Path(bundle_dir)
    bundle_dir.mkdir(parents=True, exist_ok=True)
    db_path = build_base_profiled_db(bundle_dir=bundle_dir, db_name=DB_NAME, books=3)

    conn = open_fixture_db(db_path)
    try:
        work_ids = ordered_ids(conn, "works", "work_id")
        if len(work_ids) != 3:
            raise AssertionError(f"Unexpected seeded work count for {DB_NAME}")

        titles = (
            (work_ids[0], "Custom Column Book One"),
            (work_ids[1], "Custom Column Book Two"),
            (work_ids[2], "Custom Column Book Three"),
        )
        for work_id, title in titles:
            conn.execute(
                "UPDATE works SET work_title = ?, work_canonical_title = ?, work_sort_title = ?, work_creator_sort = ? WHERE work_id = ?;",
                (title, title, title, title, work_id),
            )
        conn.commit()
    finally:
        conn.close()

    db = open_fixture_database(db_path, storage_startup_on_add=False)
    try:
        tag_num, tag_table, tag_link_table = create_custom_column(
            db,
            label="facet_tags",
            name="Facet Tags",
            datatype="text",
            is_multiple=True,
            table="works",
        )
        rating_num, rating_table, rating_link_table = create_custom_column(
            db,
            label="editor_rating",
            name="Editor Rating",
            datatype="rating",
            is_multiple=False,
            table="works",
        )
        bool_num, bool_table, _bool_link_table = create_custom_column(
            db,
            label="featured_pick",
            name="Featured Pick",
            datatype="bool",
            is_multiple=False,
            table="works",
        )
        notes_num, notes_table, _notes_link_table = create_custom_column(
            db,
            label="staff_note",
            name="Staff Note",
            datatype="comments",
            is_multiple=False,
            table="works",
        )

        if tag_link_table is None or rating_link_table is None:
            raise AssertionError(f"Expected normalized custom column link tables for {DB_NAME}")
    finally:
        db.close()

    conn = open_fixture_db(db_path)
    try:
        insert_custom_normalized_value(
            conn,
            cc_table=tag_table,
            link_table=tag_link_table,
            target_id=work_ids[0],
            value="featured",
        )
        insert_custom_normalized_value(
            conn,
            cc_table=tag_table,
            link_table=tag_link_table,
            target_id=work_ids[0],
            value="annotated",
        )
        insert_custom_normalized_value(
            conn,
            cc_table=tag_table,
            link_table=tag_link_table,
            target_id=work_ids[1],
            value="translated",
        )
        insert_custom_normalized_value(
            conn,
            cc_table=tag_table,
            link_table=tag_link_table,
            target_id=work_ids[2],
            value="reference",
        )

        insert_custom_normalized_value(
            conn,
            cc_table=rating_table,
            link_table=rating_link_table,
            target_id=work_ids[0],
            value=8,
        )
        insert_custom_normalized_value(
            conn,
            cc_table=rating_table,
            link_table=rating_link_table,
            target_id=work_ids[1],
            value=4,
        )
        insert_custom_normalized_value(
            conn,
            cc_table=rating_table,
            link_table=rating_link_table,
            target_id=work_ids[2],
            value=10,
        )

        insert_custom_scalar_value(conn, cc_table=bool_table, target_id=work_ids[0], value=1)
        insert_custom_scalar_value(conn, cc_table=bool_table, target_id=work_ids[1], value=0)
        insert_custom_scalar_value(conn, cc_table=notes_table, target_id=work_ids[0], value="<p>Lead recommendation.</p>")
        insert_custom_scalar_value(conn, cc_table=notes_table, target_id=work_ids[2], value="<p>Use for browse regression coverage.</p>")

        finalize_fixture(conn, db_name=DB_NAME)
    finally:
        conn.close()

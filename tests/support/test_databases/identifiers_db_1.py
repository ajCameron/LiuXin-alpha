from __future__ import annotations

from pathlib import Path

from tests.support.test_databases._semantic_fixture_builders import (
    build_base_profiled_db,
    finalize_fixture,
    open_fixture_db,
    ordered_ids,
)


DB_NAME = "identifiers_db_1"


def populate_bundle(bundle_dir: Path) -> None:
    bundle_dir = Path(bundle_dir)
    bundle_dir.mkdir(parents=True, exist_ok=True)
    db_path = build_base_profiled_db(bundle_dir=bundle_dir, db_name=DB_NAME, books=4)

    conn = open_fixture_db(db_path)
    try:
        work_ids = ordered_ids(conn, "works", "work_id")
        expression_ids = ordered_ids(conn, "expressions", "expression_id")
        manifestation_ids = ordered_ids(conn, "manifestations", "manifestation_id")
        item_ids = ordered_ids(conn, "items", "item_id")
        if len(work_ids) != 4 or len(expression_ids) != 4 or len(manifestation_ids) != 4 or len(item_ids) != 4:
            raise AssertionError(f"Unexpected seeded WEMI counts for {DB_NAME}")

        for work_id, title in zip(
            work_ids,
            ("Identifier Matrix One", "Identifier Matrix Two", "Identifier Matrix Three", "Identifier Matrix Four"),
            strict=True,
        ):
            conn.execute(
                "UPDATE works SET work_title = ?, work_canonical_title = ?, work_sort_title = ?, work_creator_sort = ? WHERE work_id = ?;",
                (title, title, title, title, work_id),
            )

        conn.executemany(
            "INSERT INTO entity_identifiers (entity_identifier_entity_type, entity_identifier_entity_id, entity_identifier_scheme, entity_identifier_value, entity_identifier_is_primary, entity_identifier_provenance) VALUES (?, ?, ?, ?, ?, ?);",
            (
                ("work", work_ids[0], "isbn13", "9782222222221", 1, "fixture"),
                ("work", work_ids[0], "doi", "10.5555/matrix-one", 0, "fixture"),
                ("work", work_ids[1], "isbn10", "2222222222", 1, "fixture"),
                ("work", work_ids[2], "oclc", "oclc-22223", 1, "fixture"),
                ("expression", expression_ids[1], "uri", "urn:expression:matrix-two", 1, "fixture"),
                ("expression", expression_ids[3], "urn", "urn:expression:matrix-four", 1, "fixture"),
                ("manifestation", manifestation_ids[0], "handle", "hdl:22/one", 1, "fixture"),
                ("manifestation", manifestation_ids[2], "local-call", "LC-0003", 1, "fixture"),
                ("item", item_ids[1], "asset-id", "matrix-item-two", 1, "fixture"),
                ("item", item_ids[3], "archive-id", "matrix-item-four", 1, "fixture"),
            ),
        )
        conn.executemany(
            "INSERT INTO item_identifiers (item_identifier_item_id, item_identifier_scheme, item_identifier_value, item_identifier_source) VALUES (?, ?, ?, ?);",
            (
                (item_ids[0], "asin", "B000000101", "fixture"),
                (item_ids[1], "barcode", "310000000001", "fixture"),
                (item_ids[2], "vendor", "vendor-identifier-three", "fixture"),
                (item_ids[3], "uuid-ish", "uuid-ish-4444", "fixture"),
                (item_ids[3], "shortcode", "SC-44", "fixture"),
            ),
        )

        finalize_fixture(conn, db_name=DB_NAME)
    finally:
        conn.close()


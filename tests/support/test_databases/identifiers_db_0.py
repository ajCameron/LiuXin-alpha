from __future__ import annotations

from pathlib import Path

from tests.support.test_databases._semantic_fixture_builders import (
    build_base_profiled_db,
    finalize_fixture,
    open_fixture_db,
    ordered_ids,
)


DB_NAME = "identifiers_db_0"


def populate_bundle(bundle_dir: Path) -> None:
    bundle_dir = Path(bundle_dir)
    bundle_dir.mkdir(parents=True, exist_ok=True)
    db_path = build_base_profiled_db(bundle_dir=bundle_dir, db_name=DB_NAME, books=3)

    conn = open_fixture_db(db_path)
    try:
        work_ids = ordered_ids(conn, "works", "work_id")
        expression_ids = ordered_ids(conn, "expressions", "expression_id")
        manifestation_ids = ordered_ids(conn, "manifestations", "manifestation_id")
        item_ids = ordered_ids(conn, "items", "item_id")
        if len(work_ids) != 3 or len(expression_ids) != 3 or len(manifestation_ids) != 3 or len(item_ids) != 3:
            raise AssertionError(f"Unexpected seeded WEMI counts for {DB_NAME}")

        titles = (
            (work_ids[0], "Identifier Book One"),
            (work_ids[1], "Identifier Book Two"),
            (work_ids[2], "Identifier Book Three"),
        )
        for work_id, title in titles:
            conn.execute(
                "UPDATE works SET work_title = ?, work_canonical_title = ?, work_sort_title = ?, work_creator_sort = ? WHERE work_id = ?;",
                (title, title, title, title, work_id),
            )

        for manifestation_id, detail in zip(manifestation_ids, ("epub", "pdf", "mobi"), strict=True):
            conn.execute(
                "UPDATE manifestations SET manifestation_format_detail = ?, manifestation_carrier_type = ? WHERE manifestation_id = ?;",
                (detail, "digital", manifestation_id),
            )
        for item_id, item_type in zip(item_ids, ("digital", "digital", "digital"), strict=True):
            conn.execute(
                "UPDATE items SET item_type = ?, item_source = ?, item_source_detail = ? WHERE item_id = ?;",
                (item_type, "fixture-identifiers", DB_NAME, item_id),
            )

        conn.executemany(
            "INSERT INTO entity_identifiers (entity_identifier_entity_type, entity_identifier_entity_id, entity_identifier_scheme, entity_identifier_value, entity_identifier_is_primary, entity_identifier_provenance) VALUES (?, ?, ?, ?, ?, ?);",
            (
                ("work", work_ids[0], "isbn13", "9780000000001", 1, "fixture"),
                ("work", work_ids[0], "doi", "10.5555/work-one", 0, "fixture"),
                ("expression", expression_ids[1], "uri", "urn:expression:two", 1, "fixture"),
                ("manifestation", manifestation_ids[0], "oclc", "oclc-10001", 1, "fixture"),
                ("manifestation", manifestation_ids[1], "isbn10", "0000000002", 1, "fixture"),
                ("manifestation", manifestation_ids[2], "handle", "hdl:9999/three", 1, "fixture"),
                ("item", item_ids[2], "asset-id", "asset-three", 1, "fixture"),
            ),
        )
        conn.executemany(
            "INSERT INTO item_identifiers (item_identifier_item_id, item_identifier_scheme, item_identifier_value, item_identifier_source) VALUES (?, ?, ?, ?);",
            (
                (item_ids[0], "asin", "B000000001", "fixture"),
                (item_ids[1], "barcode", "200000000002", "fixture"),
                (item_ids[2], "vendor", "vendor-three", "fixture"),
            ),
        )

        finalize_fixture(conn, db_name=DB_NAME)
    finally:
        conn.close()

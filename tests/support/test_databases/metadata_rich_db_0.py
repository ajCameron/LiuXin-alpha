from __future__ import annotations

from pathlib import Path

from tests.support.test_databases._semantic_fixture_builders import (
    build_base_profiled_db,
    finalize_fixture,
    lookup_language_id,
    norm_text,
    open_fixture_db,
    ordered_ids,
)


DB_NAME = "metadata_rich_db_0"


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

        eng_id = lookup_language_id(conn, "eng")
        fra_id = lookup_language_id(conn, "fra")

        work_payloads = (
            {
                "work_id": work_ids[0],
                "title": "Metadata Rich Book One",
                "sort_title": "Metadata Rich Book One",
                "creator_sort": "Author, Ada",
                "year": 2001,
                "language_id": eng_id,
            },
            {
                "work_id": work_ids[1],
                "title": "Metadata Rich Book Two",
                "sort_title": "Metadata Rich Book Two",
                "creator_sort": "Babbage, Bruno",
                "year": 2004,
                "language_id": eng_id,
            },
            {
                "work_id": work_ids[2],
                "title": "Metadata Rich Book Three",
                "sort_title": "Metadata Rich Book Three",
                "creator_sort": "Author, Ada",
                "year": 2009,
                "language_id": eng_id,
            },
        )
        for payload in work_payloads:
            conn.execute(
                "UPDATE works "
                "SET work_type = ?, work_medium = ?, work_title = ?, work_canonical_title = ?, "
                "work_sort_title = ?, work_creator_sort = ?, work_original_language_id = ?, "
                "work_original_year = ?, work_is_fiction = ?, work_completion_status = ?, work_discovery_note = ? "
                "WHERE work_id = ?;",
                (
                    "novel",
                    "text",
                    payload["title"],
                    payload["title"],
                    payload["sort_title"],
                    payload["creator_sort"],
                    payload["language_id"],
                    payload["year"],
                    1,
                    "complete",
                    f"fixture:{DB_NAME}",
                    payload["work_id"],
                ),
            )

        expression_payloads = (
            (expression_ids[0], "Author text", eng_id, "text"),
            (expression_ids[1], "Translated text", fra_id, "translation"),
            (expression_ids[2], "Reference text", eng_id, "text"),
        )
        for expression_id, label, language_id, mode in expression_payloads:
            conn.execute(
                "UPDATE expressions "
                "SET expression_label = ?, expression_language_id = ?, expression_mode = ?, expression_status = ? "
                "WHERE expression_id = ?;",
                (label, language_id, mode, "available", expression_id),
            )

        manifestation_payloads = (
            (manifestation_ids[0], "epub", "First edition"),
            (manifestation_ids[1], "pdf", "Annotated edition"),
            (manifestation_ids[2], "mobi", "Library edition"),
        )
        for manifestation_id, format_detail, edition in manifestation_payloads:
            conn.execute(
                "UPDATE manifestations "
                "SET manifestation_format_detail = ?, manifestation_carrier_type = ?, manifestation_edition_statement = ?, manifestation_status = ? "
                "WHERE manifestation_id = ?;",
                (format_detail, "digital", edition, "available", manifestation_id),
            )

        for item_id, source_name in zip(item_ids, ("ingest-a", "ingest-b", "ingest-c"), strict=True):
            conn.execute(
                "UPDATE items "
                "SET item_type = ?, item_source = ?, item_source_detail = ?, item_lifecycle_status = ? "
                "WHERE item_id = ?;",
                ("digital", "fixture-import", source_name, "active", item_id),
            )

        def _insert_agent(*, agent_type: str, canonical_name: str, sort_name: str) -> int:
            cur = conn.execute(
                "INSERT INTO agents (agent_type, agent_canonical_name, agent_sort_name, agent_note) VALUES (?, ?, ?, ?);",
                (agent_type, canonical_name, sort_name, f"fixture:{DB_NAME}"),
            )
            return int(cur.lastrowid)

        ada_id = _insert_agent(agent_type="person", canonical_name="Ada Author", sort_name="Author, Ada")
        bruno_id = _insert_agent(agent_type="person", canonical_name="Bruno Babbage", sort_name="Babbage, Bruno")
        theo_id = _insert_agent(agent_type="person", canonical_name="Theo Translator", sort_name="Translator, Theo")
        ines_id = _insert_agent(agent_type="person", canonical_name="Ines Editor", sort_name="Editor, Ines")
        northwind_id = _insert_agent(agent_type="organisation", canonical_name="Northwind Press", sort_name="Northwind Press")

        conn.executemany(
            "INSERT INTO human_agents (human_agent_agent_id, human_agent_given_name, human_agent_family_name, human_agent_preferred_name, human_agent_nationality) VALUES (?, ?, ?, ?, ?);",
            (
                (ada_id, "Ada", "Author", "Ada Author", "GB"),
                (bruno_id, "Bruno", "Babbage", "Bruno Babbage", "GB"),
                (theo_id, "Theo", "Translator", "Theo Translator", "FR"),
                (ines_id, "Ines", "Editor", "Ines Editor", "ES"),
            ),
        )
        conn.execute(
            "INSERT INTO org_agents (org_agent_agent_id, org_agent_legal_name, org_agent_trading_name, org_agent_registration_id, org_agent_website, org_agent_contact_email) VALUES (?, ?, ?, ?, ?, ?);",
            (
                northwind_id,
                "Northwind Press LLC",
                "Northwind Press",
                "NW-0001",
                "https://example.test/northwind",
                "rights@example.test",
            ),
        )

        conn.executemany(
            "INSERT INTO agent_work_links (agent_work_link_agent_id, agent_work_link_work_id, agent_work_link_priority, agent_work_link_type) VALUES (?, ?, ?, ?);",
            (
                (ada_id, work_ids[0], 1, "aut"),
                (theo_id, work_ids[0], 1, "trl"),
                (northwind_id, work_ids[0], 1, "pbl"),
                (bruno_id, work_ids[1], 1, "aut"),
                (ines_id, work_ids[1], 1, "edt"),
                (ada_id, work_ids[2], 2, "aut"),
                (northwind_id, work_ids[2], 2, "pbl"),
            ),
        )

        def _insert_simple_row(table: str, column: str, value: str) -> int:
            cur = conn.execute(f"INSERT INTO {table} ({column}) VALUES (?);", (value,))
            return int(cur.lastrowid)

        labels = {
            "metadata-rich": _insert_simple_row("labels", "label_text", "metadata-rich"),
            "translated": _insert_simple_row("labels", "label_text", "translated"),
            "reference": _insert_simple_row("labels", "label_text", "reference"),
            "staff-pick": _insert_simple_row("labels", "label_text", "staff-pick"),
        }
        for label_id, value in ((label_id, text) for text, label_id in labels.items()):
            conn.execute(
                "UPDATE labels SET label_text_norm = ?, label_description = ? WHERE label_id = ?;",
                (norm_text(value), f"fixture:{DB_NAME}:{value}", label_id),
            )
        conn.executemany(
            "INSERT INTO label_work_links (label_work_link_label_id, label_work_link_work_id, label_work_link_priority) VALUES (?, ?, ?);",
            (
                (labels["metadata-rich"], work_ids[0], 1),
                (labels["translated"], work_ids[1], 1),
                (labels["reference"], work_ids[2], 1),
                (labels["staff-pick"], work_ids[0], 1),
                (labels["staff-pick"], work_ids[2], 2),
            ),
        )

        series_rows = {
            "Alpha Cycle": int(
                conn.execute(
                    "INSERT INTO series (series, series_name_norm, series_sort, series_full) VALUES (?, ?, ?, ?);",
                    ("Alpha Cycle", norm_text("Alpha Cycle"), "Alpha Cycle", "Alpha Cycle"),
                ).lastrowid
            ),
            "Field Notes": int(
                conn.execute(
                    "INSERT INTO series (series, series_name_norm, series_sort, series_full) VALUES (?, ?, ?, ?);",
                    ("Field Notes", norm_text("Field Notes"), "Field Notes", "Field Notes"),
                ).lastrowid
            ),
        }
        conn.executemany(
            "INSERT INTO series_work_links (series_work_link_series_id, series_work_link_work_id, series_work_link_priority, series_work_link_type) VALUES (?, ?, ?, ?);",
            (
                (series_rows["Alpha Cycle"], work_ids[0], 1, "main"),
                (series_rows["Alpha Cycle"], work_ids[1], 2, "main"),
                (series_rows["Field Notes"], work_ids[2], 1, "main"),
            ),
        )

        subjects = {
            "Libraries": int(
                conn.execute(
                    "INSERT INTO subjects (subject, subject_sort, subject_full) VALUES (?, ?, ?);",
                    ("Libraries", "Libraries", "Libraries"),
                ).lastrowid
            ),
            "Speculative Fiction": int(
                conn.execute(
                    "INSERT INTO subjects (subject, subject_sort, subject_full) VALUES (?, ?, ?);",
                    ("Speculative Fiction", "Speculative Fiction", "Speculative Fiction"),
                ).lastrowid
            ),
            "Digital Humanities": int(
                conn.execute(
                    "INSERT INTO subjects (subject, subject_sort, subject_full) VALUES (?, ?, ?);",
                    ("Digital Humanities", "Digital Humanities", "Digital Humanities"),
                ).lastrowid
            ),
        }
        conn.executemany(
            "INSERT INTO subject_work_links (subject_work_link_subject_id, subject_work_link_work_id, subject_work_link_priority) VALUES (?, ?, ?);",
            (
                (subjects["Libraries"], work_ids[0], 1),
                (subjects["Speculative Fiction"], work_ids[0], 2),
                (subjects["Speculative Fiction"], work_ids[1], 1),
                (subjects["Digital Humanities"], work_ids[2], 1),
                (subjects["Libraries"], work_ids[2], 2),
            ),
        )

        conn.executemany(
            "INSERT INTO language_work_links (language_work_link_language_id, language_work_link_work_id, language_work_link_priority, language_work_link_type) VALUES (?, ?, ?, ?);",
            (
                (eng_id, work_ids[0], 1, "original"),
                (eng_id, work_ids[1], 2, "original"),
                (fra_id, work_ids[1], 1, "translation"),
                (eng_id, work_ids[2], 3, "original"),
            ),
        )

        note_ids = [
            _insert_simple_row("notes", "note", "Editorial note: includes restored chapter headings."),
            _insert_simple_row("notes", "note", "Discovery note: seeded for rich browse coverage."),
        ]
        conn.executemany(
            "INSERT INTO note_work_links (note_work_link_note_id, note_work_link_work_id, note_work_link_priority) VALUES (?, ?, ?);",
            (
                (note_ids[0], work_ids[0], 1),
                (note_ids[1], work_ids[2], 1),
            ),
        )

        comment_ids = [
            _insert_simple_row("comments", "comment", "A compact test comment with HTML-like <em>shape</em>."),
            _insert_simple_row("comments", "comment", "Second comment for browse and detail rendering."),
        ]
        conn.executemany(
            "INSERT INTO comment_work_links (comment_work_link_comment_id, comment_work_link_work_id, comment_work_link_priority) VALUES (?, ?, ?);",
            (
                (comment_ids[0], work_ids[0], 1),
                (comment_ids[1], work_ids[1], 1),
            ),
        )

        synopsis_ids = [
            _insert_simple_row("synopses", "synopsis", "Book one synopsis for metadata-rich navigation and previews."),
            _insert_simple_row("synopses", "synopsis", "Book two synopsis covering translation and editorial roles."),
        ]
        conn.executemany(
            "INSERT INTO synopsis_work_links (synopsis_work_link_synopsis_id, synopsis_work_link_work_id, synopsis_work_link_priority, synopsis_work_link_type) VALUES (?, ?, ?, ?);",
            (
                (synopsis_ids[0], work_ids[0], 1, "short"),
                (synopsis_ids[1], work_ids[1], 1, "short"),
            ),
        )

        conn.executemany(
            "INSERT INTO annotations (annotation_item_id, annotation_kind, annotation_anchor_type, annotation_anchor_start, annotation_anchor_end, annotation_selected_text, annotation_note_text, annotation_source) VALUES (?, ?, ?, ?, ?, ?, ?, ?);",
            (
                (item_ids[0], "highlight", "cfi", "epubcfi(/6/2[one]!/4/1:0)", "epubcfi(/6/2[one]!/4/1:18)", "Metadata rich highlight", "Important quote for interface tests", "fixture"),
                (item_ids[1], "note", "page", "12", "12", "Translated passage", "Translator note attached to page 12", "fixture"),
            ),
        )

        conn.executemany(
            "INSERT INTO entity_identifiers (entity_identifier_entity_type, entity_identifier_entity_id, entity_identifier_scheme, entity_identifier_value, entity_identifier_is_primary, entity_identifier_provenance) VALUES (?, ?, ?, ?, ?, ?);",
            (
                ("work", work_ids[0], "isbn13", "9780000000001", 1, "fixture"),
                ("work", work_ids[1], "isbn13", "9780000000002", 1, "fixture"),
                ("work", work_ids[2], "oclc", "oclc-fixture-0003", 1, "fixture"),
            ),
        )
        conn.executemany(
            "INSERT INTO item_identifiers (item_identifier_item_id, item_identifier_scheme, item_identifier_value, item_identifier_source) VALUES (?, ?, ?, ?);",
            (
                (item_ids[0], "barcode", "MR-ITEM-0001", "fixture"),
                (item_ids[1], "barcode", "MR-ITEM-0002", "fixture"),
                (item_ids[2], "barcode", "MR-ITEM-0003", "fixture"),
            ),
        )

        finalize_fixture(conn, db_name=DB_NAME)
    finally:
        conn.close()

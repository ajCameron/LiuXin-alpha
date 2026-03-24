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


DB_NAME = "metadata_rich_db_1"


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

        eng_id = lookup_language_id(conn, "eng")
        fra_id = lookup_language_id(conn, "fra")
        spa_id = lookup_language_id(conn, "spa")

        work_payloads = (
            (work_ids[0], "Metadata Spectrum One", "Author, Mara", eng_id, 1999, 1, "complete"),
            (work_ids[1], "Metadata Spectrum Two", "Author, Jules", eng_id, 2003, 1, "complete"),
            (work_ids[2], "Metadata Spectrum Three", "Author, Nia", spa_id, 2012, 0, "complete"),
            (work_ids[3], "Metadata Spectrum Four", "Author, Nia", fra_id, 2019, 1, "draft"),
        )
        for work_id, title, creator_sort, language_id, year, is_fiction, status in work_payloads:
            conn.execute(
                "UPDATE works "
                "SET work_type = ?, work_medium = ?, work_title = ?, work_canonical_title = ?, work_sort_title = ?, "
                "work_creator_sort = ?, work_original_language_id = ?, work_original_year = ?, work_is_fiction = ?, "
                "work_completion_status = ?, work_discovery_note = ? "
                "WHERE work_id = ?;",
                (
                    "novel" if is_fiction else "essay_collection",
                    "text",
                    title,
                    title,
                    title,
                    creator_sort,
                    language_id,
                    year,
                    is_fiction,
                    status,
                    f"fixture:{DB_NAME}",
                    work_id,
                ),
            )

        for expression_id, label, language_id, mode in (
            (expression_ids[0], "English text", eng_id, "text"),
            (expression_ids[1], "French translation", fra_id, "translation"),
            (expression_ids[2], "Spanish source", spa_id, "text"),
            (expression_ids[3], "Collected notes", eng_id, "annotated"),
        ):
            conn.execute(
                "UPDATE expressions SET expression_label = ?, expression_language_id = ?, expression_mode = ?, expression_status = ? WHERE expression_id = ?;",
                (label, language_id, mode, "available", expression_id),
            )

        for manifestation_id, detail, edition in (
            (manifestation_ids[0], "epub", "Trade edition"),
            (manifestation_ids[1], "pdf", "Translator's proof"),
            (manifestation_ids[2], "mobi", "Pocket edition"),
            (manifestation_ids[3], "html", "Reference edition"),
        ):
            conn.execute(
                "UPDATE manifestations SET manifestation_format_detail = ?, manifestation_carrier_type = ?, manifestation_edition_statement = ?, manifestation_status = ? WHERE manifestation_id = ?;",
                (detail, "digital", edition, "available", manifestation_id),
            )

        for item_id, source_name in zip(item_ids, ("mara-src", "jules-src", "nia-src", "ref-src"), strict=True):
            conn.execute(
                "UPDATE items SET item_type = ?, item_source = ?, item_source_detail = ?, item_lifecycle_status = ? WHERE item_id = ?;",
                ("digital", "fixture-import", source_name, "active", item_id),
            )

        def _insert_agent(agent_type: str, canonical_name: str, sort_name: str) -> int:
            return int(
                conn.execute(
                    "INSERT INTO agents (agent_type, agent_canonical_name, agent_sort_name, agent_note) VALUES (?, ?, ?, ?);",
                    (agent_type, canonical_name, sort_name, f"fixture:{DB_NAME}"),
                ).lastrowid
            )

        mara_id = _insert_agent("person", "Mara Author", "Author, Mara")
        jules_id = _insert_agent("person", "Jules Author", "Author, Jules")
        nia_id = _insert_agent("person", "Nia Author", "Author, Nia")
        theo_id = _insert_agent("person", "Theo Translator", "Translator, Theo")
        erin_id = _insert_agent("person", "Erin Editor", "Editor, Erin")
        meridian_id = _insert_agent("organisation", "Meridian House", "Meridian House")

        conn.executemany(
            "INSERT INTO human_agents (human_agent_agent_id, human_agent_given_name, human_agent_family_name, human_agent_preferred_name, human_agent_nationality) VALUES (?, ?, ?, ?, ?);",
            (
                (mara_id, "Mara", "Author", "Mara Author", "GB"),
                (jules_id, "Jules", "Author", "Jules Author", "CA"),
                (nia_id, "Nia", "Author", "Nia Author", "ES"),
                (theo_id, "Theo", "Translator", "Theo Translator", "FR"),
                (erin_id, "Erin", "Editor", "Erin Editor", "IE"),
            ),
        )
        conn.execute(
            "INSERT INTO org_agents (org_agent_agent_id, org_agent_legal_name, org_agent_trading_name, org_agent_registration_id, org_agent_website, org_agent_contact_email) VALUES (?, ?, ?, ?, ?, ?);",
            (
                meridian_id,
                "Meridian House Ltd",
                "Meridian House",
                "MH-0042",
                "https://example.test/meridian",
                "contact@example.test",
            ),
        )

        conn.executemany(
            "INSERT INTO agent_work_links (agent_work_link_agent_id, agent_work_link_work_id, agent_work_link_priority, agent_work_link_type) VALUES (?, ?, ?, ?);",
            (
                (mara_id, work_ids[0], 1, "aut"),
                (erin_id, work_ids[0], 1, "edt"),
                (jules_id, work_ids[1], 1, "aut"),
                (theo_id, work_ids[1], 1, "trl"),
                (meridian_id, work_ids[1], 1, "pbl"),
                (nia_id, work_ids[2], 2, "aut"),
                (erin_id, work_ids[2], 2, "edt"),
                (meridian_id, work_ids[2], 2, "pbl"),
                (nia_id, work_ids[3], 3, "aut"),
                (theo_id, work_ids[3], 3, "trl"),
            ),
        )

        def _insert_simple_row(table: str, column: str, value: str) -> int:
            return int(conn.execute(f"INSERT INTO {table} ({column}) VALUES (?);", (value,)).lastrowid)

        label_ids = {
            "metadata-spectrum": _insert_simple_row("labels", "label_text", "metadata-spectrum"),
            "translation": _insert_simple_row("labels", "label_text", "translation"),
            "essay": _insert_simple_row("labels", "label_text", "essay"),
            "staff-pick": _insert_simple_row("labels", "label_text", "staff-pick"),
            "serial": _insert_simple_row("labels", "label_text", "serial"),
        }
        for text, label_id in label_ids.items():
            conn.execute(
                "UPDATE labels SET label_text_norm = ?, label_description = ? WHERE label_id = ?;",
                (norm_text(text), f"fixture:{DB_NAME}:{text}", label_id),
            )
        conn.executemany(
            "INSERT INTO label_work_links (label_work_link_label_id, label_work_link_work_id, label_work_link_priority) VALUES (?, ?, ?);",
            (
                (label_ids["metadata-spectrum"], work_ids[0], 1),
                (label_ids["translation"], work_ids[1], 1),
                (label_ids["essay"], work_ids[2], 1),
                (label_ids["staff-pick"], work_ids[2], 2),
                (label_ids["serial"], work_ids[3], 1),
                (label_ids["staff-pick"], work_ids[3], 3),
                (label_ids["metadata-spectrum"], work_ids[3], 4),
            ),
        )

        series_ids = {
            "Meridian Cycle": int(
                conn.execute(
                    "INSERT INTO series (series, series_name_norm, series_sort, series_full) VALUES (?, ?, ?, ?);",
                    ("Meridian Cycle", norm_text("Meridian Cycle"), "Meridian Cycle", "Meridian Cycle"),
                ).lastrowid
            ),
            "Field Reports": int(
                conn.execute(
                    "INSERT INTO series (series, series_name_norm, series_sort, series_full) VALUES (?, ?, ?, ?);",
                    ("Field Reports", norm_text("Field Reports"), "Field Reports", "Field Reports"),
                ).lastrowid
            ),
            "Reference Shelf": int(
                conn.execute(
                    "INSERT INTO series (series, series_name_norm, series_sort, series_full) VALUES (?, ?, ?, ?);",
                    ("Reference Shelf", norm_text("Reference Shelf"), "Reference Shelf", "Reference Shelf"),
                ).lastrowid
            ),
        }
        conn.executemany(
            "INSERT INTO series_work_links (series_work_link_series_id, series_work_link_work_id, series_work_link_priority, series_work_link_type) VALUES (?, ?, ?, ?);",
            (
                (series_ids["Meridian Cycle"], work_ids[0], 1, "main"),
                (series_ids["Meridian Cycle"], work_ids[1], 2, "main"),
                (series_ids["Field Reports"], work_ids[2], 1, "main"),
                (series_ids["Reference Shelf"], work_ids[3], 1, "main"),
            ),
        )

        subject_ids = {
            "Libraries": int(conn.execute("INSERT INTO subjects (subject, subject_sort, subject_full) VALUES (?, ?, ?);", ("Libraries", "Libraries", "Libraries")).lastrowid),
            "Translation Studies": int(conn.execute("INSERT INTO subjects (subject, subject_sort, subject_full) VALUES (?, ?, ?);", ("Translation Studies", "Translation Studies", "Translation Studies")).lastrowid),
            "Essay": int(conn.execute("INSERT INTO subjects (subject, subject_sort, subject_full) VALUES (?, ?, ?);", ("Essay", "Essay", "Essay")).lastrowid),
            "Digital Archives": int(conn.execute("INSERT INTO subjects (subject, subject_sort, subject_full) VALUES (?, ?, ?);", ("Digital Archives", "Digital Archives", "Digital Archives")).lastrowid),
        }
        conn.executemany(
            "INSERT INTO subject_work_links (subject_work_link_subject_id, subject_work_link_work_id, subject_work_link_priority) VALUES (?, ?, ?);",
            (
                (subject_ids["Libraries"], work_ids[0], 1),
                (subject_ids["Translation Studies"], work_ids[1], 1),
                (subject_ids["Essay"], work_ids[2], 1),
                (subject_ids["Digital Archives"], work_ids[3], 1),
                (subject_ids["Libraries"], work_ids[3], 2),
                (subject_ids["Essay"], work_ids[1], 2),
                (subject_ids["Digital Archives"], work_ids[2], 2),
            ),
        )

        conn.executemany(
            "INSERT INTO language_work_links (language_work_link_language_id, language_work_link_work_id, language_work_link_priority, language_work_link_type) VALUES (?, ?, ?, ?);",
            (
                (eng_id, work_ids[0], 1, "original"),
                (eng_id, work_ids[1], 2, "original"),
                (fra_id, work_ids[1], 1, "translation"),
                (spa_id, work_ids[2], 1, "original"),
                (fra_id, work_ids[3], 1, "original"),
                (eng_id, work_ids[3], 3, "translation"),
            ),
        )

        note_ids = [
            _insert_simple_row("notes", "note", "Metadata spectrum note one."),
            _insert_simple_row("notes", "note", "Metadata spectrum note two."),
            _insert_simple_row("notes", "note", "Metadata spectrum note three."),
        ]
        conn.executemany(
            "INSERT INTO note_work_links (note_work_link_note_id, note_work_link_work_id, note_work_link_priority) VALUES (?, ?, ?);",
            (
                (note_ids[0], work_ids[0], 1),
                (note_ids[1], work_ids[2], 1),
                (note_ids[2], work_ids[3], 2),
            ),
        )

        comment_ids = [
            _insert_simple_row("comments", "comment", "Comment one for dense metadata browse."),
            _insert_simple_row("comments", "comment", "Comment two for translated material."),
            _insert_simple_row("comments", "comment", "Comment three for draft/reference handling."),
        ]
        conn.executemany(
            "INSERT INTO comment_work_links (comment_work_link_comment_id, comment_work_link_work_id, comment_work_link_priority) VALUES (?, ?, ?);",
            (
                (comment_ids[0], work_ids[0], 1),
                (comment_ids[1], work_ids[1], 1),
                (comment_ids[2], work_ids[3], 1),
            ),
        )

        synopsis_ids = [
            _insert_simple_row("synopses", "synopsis", "Synopsis one for metadata spectrum."),
            _insert_simple_row("synopses", "synopsis", "Synopsis two for multilingual routing."),
            _insert_simple_row("synopses", "synopsis", "Synopsis three for reference materials."),
        ]
        conn.executemany(
            "INSERT INTO synopsis_work_links (synopsis_work_link_synopsis_id, synopsis_work_link_work_id, synopsis_work_link_priority, synopsis_work_link_type) VALUES (?, ?, ?, ?);",
            (
                (synopsis_ids[0], work_ids[0], 1, "short"),
                (synopsis_ids[1], work_ids[1], 1, "short"),
                (synopsis_ids[2], work_ids[3], 1, "short"),
            ),
        )

        conn.executemany(
            "INSERT INTO annotations (annotation_item_id, annotation_kind, annotation_anchor_type, annotation_anchor_start, annotation_anchor_end, annotation_selected_text, annotation_note_text, annotation_source) VALUES (?, ?, ?, ?, ?, ?, ?, ?);",
            (
                (item_ids[0], "highlight", "cfi", "epubcfi(/6/2[one]!/4/1:0)", "epubcfi(/6/2[one]!/4/1:12)", "Spectrum note", "Annotation one", "fixture"),
                (item_ids[1], "note", "page", "15", "15", "Translated passage", "Annotation two", "fixture"),
                (item_ids[3], "note", "page", "3", "3", "Reference note", "Annotation three", "fixture"),
            ),
        )

        conn.executemany(
            "INSERT INTO entity_identifiers (entity_identifier_entity_type, entity_identifier_entity_id, entity_identifier_scheme, entity_identifier_value, entity_identifier_is_primary, entity_identifier_provenance) VALUES (?, ?, ?, ?, ?, ?);",
            (
                ("work", work_ids[0], "isbn13", "9781111111111", 1, "fixture"),
                ("work", work_ids[1], "doi", "10.5555/metadata-two", 1, "fixture"),
                ("expression", expression_ids[2], "uri", "urn:expression:metadata-three", 1, "fixture"),
                ("manifestation", manifestation_ids[3], "oclc", "oclc-metadata-4", 1, "fixture"),
            ),
        )
        conn.executemany(
            "INSERT INTO item_identifiers (item_identifier_item_id, item_identifier_scheme, item_identifier_value, item_identifier_source) VALUES (?, ?, ?, ?);",
            (
                (item_ids[0], "barcode", "MR1-0001", "fixture"),
                (item_ids[1], "barcode", "MR1-0002", "fixture"),
                (item_ids[2], "vendor", "MR1-VENDOR-0003", "fixture"),
                (item_ids[3], "asset-id", "MR1-ASSET-0004", "fixture"),
            ),
        )

        finalize_fixture(conn, db_name=DB_NAME)
    finally:
        conn.close()


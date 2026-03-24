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


DB_NAME = "pathological_relations_db_0"


def populate_bundle(bundle_dir: Path) -> None:
    bundle_dir = Path(bundle_dir)
    bundle_dir.mkdir(parents=True, exist_ok=True)
    db_path = build_base_profiled_db(bundle_dir=bundle_dir, db_name=DB_NAME, books=8)

    conn = open_fixture_db(db_path)
    try:
        work_ids = ordered_ids(conn, "works", "work_id")
        item_ids = ordered_ids(conn, "items", "item_id")
        if len(work_ids) != 8 or len(item_ids) != 8:
            raise AssertionError(f"Unexpected seeded WEMI counts for {DB_NAME}")

        eng_id = lookup_language_id(conn, "eng")
        fra_id = lookup_language_id(conn, "fra")

        for idx, (work_id, item_id) in enumerate(zip(work_ids, item_ids, strict=True), start=1):
            title = f"Pathological Relation Book {idx:02d}"
            conn.execute(
                "UPDATE works SET work_title = ?, work_canonical_title = ?, work_sort_title = ?, work_creator_sort = ?, work_discovery_note = ? WHERE work_id = ?;",
                (title, title, title, title, f"fixture:{DB_NAME}", work_id),
            )
            conn.execute(
                "UPDATE items SET item_type = ?, item_source = ?, item_source_detail = ? WHERE item_id = ?;",
                ("digital", "fixture-relations", f"item-{idx:02d}", item_id),
            )

        def _insert_agent(agent_type: str, canonical_name: str, sort_name: str) -> int:
            return int(
                conn.execute(
                    "INSERT INTO agents (agent_type, agent_canonical_name, agent_sort_name, agent_note) VALUES (?, ?, ?, ?);",
                    (agent_type, canonical_name, sort_name, f"fixture:{DB_NAME}"),
                ).lastrowid
            )

        author_ids = [
            _insert_agent("person", "Alpha Author", "Author, Alpha"),
            _insert_agent("person", "Beta Author", "Author, Beta"),
            _insert_agent("person", "Gamma Author", "Author, Gamma"),
            _insert_agent("person", "Delta Author", "Author, Delta"),
        ]
        translator_id = _insert_agent("person", "Dense Translator", "Translator, Dense")
        editor_id = _insert_agent("person", "Dense Editor", "Editor, Dense")
        publisher_id = _insert_agent("organisation", "Dense Press", "Dense Press")

        conn.executemany(
            "INSERT INTO human_agents (human_agent_agent_id, human_agent_given_name, human_agent_family_name, human_agent_preferred_name, human_agent_nationality) VALUES (?, ?, ?, ?, ?);",
            (
                (author_ids[0], "Alpha", "Author", "Alpha Author", "GB"),
                (author_ids[1], "Beta", "Author", "Beta Author", "GB"),
                (author_ids[2], "Gamma", "Author", "Gamma Author", "GB"),
                (author_ids[3], "Delta", "Author", "Delta Author", "GB"),
                (translator_id, "Dense", "Translator", "Dense Translator", "FR"),
                (editor_id, "Dense", "Editor", "Dense Editor", "IE"),
            ),
        )
        conn.execute(
            "INSERT INTO org_agents (org_agent_agent_id, org_agent_legal_name, org_agent_trading_name, org_agent_registration_id, org_agent_website, org_agent_contact_email) VALUES (?, ?, ?, ?, ?, ?);",
            (publisher_id, "Dense Press Ltd", "Dense Press", "DENSE-1", "https://example.test/dense", "dense@example.test"),
        )

        agent_links = []
        for idx, work_id in enumerate(work_ids, start=1):
            agent_links.append((author_ids[(idx - 1) % len(author_ids)], work_id, idx, "aut"))
            agent_links.append((publisher_id, work_id, idx, "pbl"))
            if idx % 2 == 0:
                agent_links.append((translator_id, work_id, idx, "trl"))
            else:
                agent_links.append((editor_id, work_id, idx, "edt"))
        conn.executemany(
            "INSERT INTO agent_work_links (agent_work_link_agent_id, agent_work_link_work_id, agent_work_link_priority, agent_work_link_type) VALUES (?, ?, ?, ?);",
            tuple(agent_links),
        )

        label_ids = {
            "common": int(conn.execute("INSERT INTO labels (label_text, label_text_norm, label_description) VALUES (?, ?, ?);", ("common", "common", f"fixture:{DB_NAME}:common")).lastrowid),
            "dense": int(conn.execute("INSERT INTO labels (label_text, label_text_norm, label_description) VALUES (?, ?, ?);", ("dense-web", norm_text("dense-web"), f"fixture:{DB_NAME}:dense")).lastrowid),
            "even": int(conn.execute("INSERT INTO labels (label_text, label_text_norm, label_description) VALUES (?, ?, ?);", ("even-cluster", norm_text("even-cluster"), f"fixture:{DB_NAME}:even")).lastrowid),
            "odd": int(conn.execute("INSERT INTO labels (label_text, label_text_norm, label_description) VALUES (?, ?, ?);", ("odd-cluster", norm_text("odd-cluster"), f"fixture:{DB_NAME}:odd")).lastrowid),
        }
        label_links = []
        for idx, work_id in enumerate(work_ids, start=1):
            label_links.append((label_ids["common"], work_id, idx))
            label_links.append((label_ids["dense"], work_id, idx + 20))
            if idx % 2 == 0:
                label_links.append((label_ids["even"], work_id, idx))
            else:
                label_links.append((label_ids["odd"], work_id, idx))
        conn.executemany(
            "INSERT INTO label_work_links (label_work_link_label_id, label_work_link_work_id, label_work_link_priority) VALUES (?, ?, ?);",
            tuple(label_links),
        )

        subject_ids = {
            "Networks": int(conn.execute("INSERT INTO subjects (subject, subject_sort, subject_full) VALUES (?, ?, ?);", ("Networks", "Networks", "Networks")).lastrowid),
            "Graphs": int(conn.execute("INSERT INTO subjects (subject, subject_sort, subject_full) VALUES (?, ?, ?);", ("Graphs", "Graphs", "Graphs")).lastrowid),
            "Catalogues": int(conn.execute("INSERT INTO subjects (subject, subject_sort, subject_full) VALUES (?, ?, ?);", ("Catalogues", "Catalogues", "Catalogues")).lastrowid),
            "Stress": int(conn.execute("INSERT INTO subjects (subject, subject_sort, subject_full) VALUES (?, ?, ?);", ("Stress", "Stress", "Stress")).lastrowid),
        }
        subject_links = []
        for idx, work_id in enumerate(work_ids, start=1):
            subject_links.append((subject_ids["Networks"], work_id, idx))
            subject_links.append((subject_ids["Graphs" if idx % 2 == 0 else "Catalogues"], work_id, idx + 10))
        conn.executemany(
            "INSERT INTO subject_work_links (subject_work_link_subject_id, subject_work_link_work_id, subject_work_link_priority) VALUES (?, ?, ?);",
            tuple(subject_links),
        )

        series_ids = {
            "Dense A": int(conn.execute("INSERT INTO series (series, series_name_norm, series_sort, series_full) VALUES (?, ?, ?, ?);", ("Dense A", norm_text("Dense A"), "Dense A", "Dense A")).lastrowid),
            "Dense B": int(conn.execute("INSERT INTO series (series, series_name_norm, series_sort, series_full) VALUES (?, ?, ?, ?);", ("Dense B", norm_text("Dense B"), "Dense B", "Dense B")).lastrowid),
            "Dense C": int(conn.execute("INSERT INTO series (series, series_name_norm, series_sort, series_full) VALUES (?, ?, ?, ?);", ("Dense C", norm_text("Dense C"), "Dense C", "Dense C")).lastrowid),
        }
        conn.executemany(
            "INSERT INTO series_work_links (series_work_link_series_id, series_work_link_work_id, series_work_link_priority, series_work_link_type) VALUES (?, ?, ?, ?);",
            tuple(
                (series_ids["Dense A" if idx <= 3 else "Dense B" if idx <= 6 else "Dense C"], work_id, idx, "main")
                for idx, work_id in enumerate(work_ids, start=1)
            ),
        )

        language_links = []
        for idx, work_id in enumerate(work_ids, start=1):
            language_links.append((eng_id, work_id, idx, "original"))
            if idx % 3 == 0:
                language_links.append((fra_id, work_id, idx + 10, "translation"))
        conn.executemany(
            "INSERT INTO language_work_links (language_work_link_language_id, language_work_link_work_id, language_work_link_priority, language_work_link_type) VALUES (?, ?, ?, ?);",
            tuple(language_links),
        )

        finalize_fixture(conn, db_name=DB_NAME)
    finally:
        conn.close()

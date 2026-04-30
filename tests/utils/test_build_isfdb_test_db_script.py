from __future__ import annotations

import importlib.util
import sqlite3
import sys
from pathlib import Path


def _load_script():
    script_path = Path(__file__).resolve().parents[2] / "scripts" / "build_isfdb_test_db.py"
    spec = importlib.util.spec_from_file_location("build_isfdb_test_db", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _new_stage_conn(module) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    for spec in module.STAGE_SPECS.values():
        conn.execute(spec.create_sql)
    return conn


def test_build_frbr_target_handles_reused_agents_publishers_and_series(tmp_path: Path) -> None:
    module = _load_script()
    stage_conn = _new_stage_conn(module)
    try:
        stage_conn.executemany(
            module.STAGE_SPECS["authors"].insert_sql,
            [
                (1, "Jane Author", "Jane Author", 504, "Author", 1, "Seeded author row note"),
            ],
        )
        stage_conn.executemany(
            module.STAGE_SPECS["titles"].insert_sql,
            [
                (
                    10,
                    "Book One",
                    None,
                    502,
                    501,
                    7,
                    1,
                    "2000-01-01",
                    "NOVEL",
                    None,
                    None,
                    1,
                    None,
                    None,
                    None,
                ),
                (
                    20,
                    "Arabian Frights",
                    None,
                    None,
                    None,
                    7,
                    1,
                    "2001-01-01",
                    "NOVEL",
                    None,
                    None,
                    1,
                    None,
                    None,
                    None,
                ),
                (
                    30,
                    "The And",
                    None,
                    None,
                    None,
                    None,
                    None,
                    "2001-01-01",
                    "NOVEL",
                    None,
                    None,
                    1,
                    None,
                    None,
                    None,
                ),
            ],
        )
        stage_conn.executemany(
            module.STAGE_SPECS["pubs"].insert_sql,
            [
                (
                    100,
                    "Book One First",
                    "First",
                    "2000-01-01",
                    5,
                    "320",
                    "NOVEL",
                    "hc",
                    "978-0-306-40615-7",
                    503,
                    None,
                    None,
                    None,
                ),
                (
                    101,
                    "Book One Second",
                    "Second",
                    "2001-01-01",
                    5,
                    "322",
                    "NOVEL",
                    "pb",
                    "0-306-40615-2",
                    None,
                    None,
                    None,
                    None,
                ),
                (
                    200,
                    "Book Two First",
                    "First",
                    "2002-01-01",
                    5,
                    "410",
                    "NOVEL",
                    "hc",
                    None,
                    None,
                    None,
                    None,
                    None,
                ),
            ],
        )
        stage_conn.executemany(
            module.STAGE_SPECS["pub_content"].insert_sql,
            [
                (1000, 10, 100, None),
                (1001, 10, 101, None),
                (1002, 20, 200, None),
                (1003, 30, 200, None),
            ],
        )
        stage_conn.executemany(
            module.STAGE_SPECS["publishers"].insert_sql,
            [
                (5, "Acme Press", 505),
            ],
        )
        stage_conn.executemany(
            module.STAGE_SPECS["series"].insert_sql,
            [
                (7, "Shared Saga", None, None, None),
            ],
        )
        stage_conn.executemany(
            module.STAGE_SPECS["canonical_author"].insert_sql,
            [
                (1, 10, 1, 1),
                (3, 10, 1, 1),
                (2, 20, 1, 1),
                (4, 30, 1, 1),
            ],
        )
        stage_conn.executemany(
            module.STAGE_SPECS["languages"].insert_sql,
            [
                (1, "English", "en"),
            ],
        )
        stage_conn.executemany(
            module.STAGE_SPECS["identifier_types"].insert_sql,
            [
                (1, "ASIN", "Amazon Standard Identification Number"),
                (8, "Goodreads", "Goodreads"),
                (17, "Audible-ASIN", "Audible ASIN"),
            ],
        )
        stage_conn.executemany(
            module.STAGE_SPECS["identifiers"].insert_sql,
            [
                (1, 1, "B000123456", 100),
                (2, 17, "B000AUD123", 101),
                (3, 8, "goodreads-ignored", 100),
                (4, 1, "B000UNUSED", 999),
            ],
        )
        stage_conn.executemany(
            module.STAGE_SPECS["notes"].insert_sql,
            [
                (501, "Title note for Book One."),
                (502, "Synopsis for Book One."),
                (503, "Publication note for the first edition."),
                (504, "Author biographical note."),
                (505, "Publisher history note."),
                (506, "Unselected note."),
            ],
        )
        stage_conn.executemany(
            module.STAGE_SPECS["tags"].insert_sql,
            [
                (1, "Space Opera", 0),
                (2, "Shared Tag", 1),
                (3, "Unselected Tag", 0),
                (4, "sci fi", 0),
            ],
        )
        stage_conn.executemany(
            module.STAGE_SPECS["tag_mapping"].insert_sql,
            [
                (1, 1, 10, 2),
                (2, 2, 10, 3),
                (3, 1, 10, 4),
                (4, 1, 20, 2),
                (5, 3, 999, 2),
                (6, 4, 20, 2),
            ],
        )
        stage_conn.commit()

        module._create_stage_indexes(stage_conn)
        selection_counts = module._materialize_selected_subset(stage_conn, max_pubs=None)
        assert selection_counts["selected_tags"] == 3
        assert selection_counts["selected_tag_mappings"] == 4
        assert selection_counts["selected_pub_isbns"] == 2
        assert selection_counts["selected_pub_external_identifiers"] == 2
        assert selection_counts["selected_title_notes"] == 1
        assert selection_counts["selected_title_synopses"] == 1
        assert selection_counts["selected_author_notes"] == 1
        assert selection_counts["selected_publisher_notes"] == 1
        assert selection_counts["selected_pub_notes"] == 1

        output_db = tmp_path / "isfdb_regression.test_db"
        counts = module._build_frbr_target(stage_conn=stage_conn, output_db=output_db)

        assert counts["works"] == 3
        assert counts["expressions"] == 3
        assert counts["manifestations"] == 3
        assert counts["agent_work_links"] == 3
        assert counts["agent_manifestation_links"] == 3
        assert counts["language_work_links"] == 3
        assert counts["series"] == 2
        assert counts["series_work_links"] == 3
        assert counts["labels"] == 6
        assert counts["label_work_links"] == 7
        assert counts["genres"] == 3
        assert counts["genre_work_links"] == 4
        assert counts["subjects"] == 5
        assert counts["subject_work_links"] == 6
        assert counts["notes"] == 3
        assert counts["comments"] == 1
        assert counts["comment_work_links"] == 1
        assert counts["synopses"] == 1
        assert counts["ratings"] == 3
        assert counts["rating_work_links"] == 3
        assert counts["annotations"] == 2
        assert counts["note_work_links"] == 1
        assert counts["agent_note_links"] == 2
        assert counts["synopsis_work_links"] == 1
        assert counts["entity_identifiers"] == 4
        assert counts["item_identifiers"] == 4
        assert counts["expression_manifestation_links"] == 4

        conn = sqlite3.connect(str(output_db))
        try:
            publisher_priorities = [
                row[0]
                for row in conn.execute(
                    """
                    SELECT agent_manifestation_link_priority
                    FROM agent_manifestation_links
                    ORDER BY agent_manifestation_link_priority;
                    """
                )
            ]
            assert len(publisher_priorities) == len(set(publisher_priorities)) == 3

            author_priorities = [
                row[0]
                for row in conn.execute(
                    """
                    SELECT agent_work_link_priority
                    FROM agent_work_links
                    ORDER BY agent_work_link_priority;
                    """
                )
            ]
            assert len(author_priorities) == len(set(author_priorities)) == 3

            language_priorities = [
                row[0]
                for row in conn.execute(
                    """
                    SELECT language_work_link_priority
                    FROM language_work_links
                    ORDER BY language_work_link_priority;
                    """
                )
            ]
            assert len(language_priorities) == len(set(language_priorities)) == 3

            series_priorities = [
                row[0]
                for row in conn.execute(
                    """
                    SELECT series_work_link_priority
                    FROM series_work_links
                    ORDER BY series_work_link_priority;
                    """
                )
            ]
            assert len(series_priorities) == len(set(series_priorities)) == 3

            linked_series = conn.execute(
                """
                SELECT w.work_scratch, s.series, swl.series_work_link_type, swl.series_work_link_source
                FROM series_work_links swl
                JOIN series s ON s.series_id = swl.series_work_link_series_id
                JOIN works w ON w.work_id = swl.series_work_link_work_id
                ORDER BY w.work_scratch, s.series;
                """
            ).fetchall()
            assert linked_series == [
                ("isfdb:title:10", "Shared Saga", "main", "isfdb"),
                ("isfdb:title:20", "Shared Saga", "main", "isfdb"),
                (
                    "isfdb:title:30",
                    "Standalone / Unseriesed",
                    "generated_standalone",
                    "isfdb:generated",
                ),
            ]

            per_expression = conn.execute(
                """
                SELECT
                    expression_manifestation_link_expression_id,
                    COUNT(*) AS link_count,
                    SUM(COALESCE(expression_manifestation_link_primary, 0)) AS primary_count,
                    COUNT(DISTINCT expression_manifestation_link_priority) AS distinct_priority_count
                FROM expression_manifestation_links
                GROUP BY expression_manifestation_link_expression_id
                ORDER BY expression_manifestation_link_expression_id;
                """
            ).fetchall()
            assert any(
                link_count == 2 and primary_count == 1 and distinct_priority_count == 2
                for _, link_count, primary_count, distinct_priority_count in per_expression
            )
            assert all(primary_count == 1 for _, _, primary_count, _ in per_expression)

            label_rows = conn.execute(
                """
                SELECT label_text, label_text_norm, label_scratch
                FROM labels
                ORDER BY label_text_norm;
                """
            ).fetchall()
            assert label_rows == [
                ("Arabian", "arabian", "isfdb:generated:title_word:arabian"),
                ("Frights", "frights", "isfdb:generated:title_word:frights"),
                ("Untagged", "isfdb-generated-untagged", "isfdb:generated:fallback_label"),
                ("sci fi", "sci-fi", "isfdb:tag:4;status:0"),
                ("Shared Tag", "shared-tag", "isfdb:tag:2;status:1"),
                ("Space Opera", "space-opera", "isfdb:tag:1;status:0"),
            ]

            linked_labels = conn.execute(
                """
                SELECT
                    w.work_scratch,
                    l.label_text,
                    lwl.label_work_link_priority,
                    lwl.label_work_link_source
                FROM label_work_links lwl
                JOIN labels l ON l.label_id = lwl.label_work_link_label_id
                JOIN works w ON w.work_id = lwl.label_work_link_work_id
                ORDER BY w.work_scratch, l.label_text;
                """
            ).fetchall()
            assert [
                (work, label) for work, label, _priority, _source in linked_labels
            ] == [
                ("isfdb:title:10", "Shared Tag"),
                ("isfdb:title:10", "Space Opera"),
                ("isfdb:title:20", "Arabian"),
                ("isfdb:title:20", "Frights"),
                ("isfdb:title:20", "Space Opera"),
                ("isfdb:title:20", "sci fi"),
                ("isfdb:title:30", "Untagged"),
            ]
            priorities_by_label = {}
            sources_by_label = {}
            for _work, label, priority, source in linked_labels:
                priorities_by_label.setdefault(label, set()).add(priority)
                sources_by_label.setdefault(label, set()).add(source)
            assert len(priorities_by_label["Space Opera"]) == 2
            assert sources_by_label["Arabian"] == {"isfdb:generated"}
            assert sources_by_label["Untagged"] == {"isfdb:generated"}
            assert sources_by_label["Space Opera"] == {"isfdb:tag"}

            genre_rows = conn.execute(
                """
                SELECT genre, genre_sort, genre_phash, genre_full, genre_scratch
                FROM genres
                ORDER BY genre;
                """
            ).fetchall()
            assert genre_rows == [
                (
                    "Science Fiction",
                    "Science Fiction",
                    "science_fiction",
                    "Science Fiction",
                    "isfdb:genre_from_tag:4;tag:sci fi",
                ),
                (
                    "Space Opera",
                    "Space Opera",
                    "space_opera",
                    "Space Opera",
                    "isfdb:genre_from_tag:1;tag:Space Opera",
                ),
                (
                    "Unclassified",
                    "Unclassified",
                    "unclassified",
                    "Unclassified",
                    "isfdb:generated:fallback_genre",
                ),
            ]

            linked_genres = conn.execute(
                """
                SELECT w.work_scratch, g.genre, gwl.genre_work_link_type, gwl.genre_work_link_source
                FROM genre_work_links gwl
                JOIN genres g ON g.genre_id = gwl.genre_work_link_genre_id
                JOIN works w ON w.work_id = gwl.genre_work_link_work_id
                ORDER BY w.work_scratch, g.genre;
                """
            ).fetchall()
            assert linked_genres == [
                ("isfdb:title:10", "Space Opera", "genre", "isfdb:tag"),
                ("isfdb:title:20", "Science Fiction", "genre", "isfdb:tag"),
                ("isfdb:title:20", "Space Opera", "genre", "isfdb:tag"),
                (
                    "isfdb:title:30",
                    "Unclassified",
                    "generated_fallback",
                    "isfdb:generated",
                ),
            ]

            subject_rows = conn.execute(
                """
                SELECT s.subject, parent.subject, s.subject_full, s.subject_tree_id
                FROM subjects s
                LEFT JOIN subjects parent ON parent.subject_id = s.subject_parent_id
                ORDER BY s.subject_full;
                """
            ).fetchall()
            assert subject_rows == [
                ("ISFDB Generated", None, "ISFDB Generated", "isfdb-generated"),
                (
                    "Original Decade",
                    "ISFDB Generated",
                    "ISFDB Generated > Original Decade",
                    "isfdb-generated",
                ),
                (
                    "2000s",
                    "Original Decade",
                    "ISFDB Generated > Original Decade > 2000s",
                    "isfdb-generated",
                ),
                (
                    "Title Type",
                    "ISFDB Generated",
                    "ISFDB Generated > Title Type",
                    "isfdb-generated",
                ),
                (
                    "Novel",
                    "Title Type",
                    "ISFDB Generated > Title Type > Novel",
                    "isfdb-generated",
                ),
            ]

            linked_subjects = conn.execute(
                """
                SELECT w.work_scratch, s.subject_full, swl.subject_work_link_source
                FROM subject_work_links swl
                JOIN subjects s ON s.subject_id = swl.subject_work_link_subject_id
                JOIN works w ON w.work_id = swl.subject_work_link_work_id
                ORDER BY w.work_scratch, s.subject_full;
                """
            ).fetchall()
            assert linked_subjects == [
                ("isfdb:title:10", "ISFDB Generated > Original Decade > 2000s", "isfdb:generated"),
                ("isfdb:title:10", "ISFDB Generated > Title Type > Novel", "isfdb:generated"),
                ("isfdb:title:20", "ISFDB Generated > Original Decade > 2000s", "isfdb:generated"),
                ("isfdb:title:20", "ISFDB Generated > Title Type > Novel", "isfdb:generated"),
                ("isfdb:title:30", "ISFDB Generated > Original Decade > 2000s", "isfdb:generated"),
                ("isfdb:title:30", "ISFDB Generated > Title Type > Novel", "isfdb:generated"),
            ]

            generated_comments = conn.execute(
                """
                SELECT w.work_scratch, c.comment, c.comment_scratch, cwl.comment_work_link_source
                FROM comment_work_links cwl
                JOIN comments c ON c.comment_id = cwl.comment_work_link_comment_id
                JOIN works w ON w.work_id = cwl.comment_work_link_work_id;
                """
            ).fetchall()
            assert generated_comments == [
                (
                    "isfdb:title:10",
                    "Generated deterministic ISFDB test comment for Book One (source title 10).",
                    "isfdb:title:10;generated_comment",
                    "isfdb:generated",
                ),
            ]

            generated_ratings = conn.execute(
                """
                SELECT
                    w.work_scratch,
                    r.rating,
                    r.rating_out_of,
                    r.rating_for_calibre_tag_viewer,
                    r.rating_source,
                    rwl.rating_work_link_type,
                    rwl.rating_work_link_origin,
                    rwl.rating_work_link_source
                FROM rating_work_links rwl
                JOIN ratings r ON r.rating_id = rwl.rating_work_link_rating_id
                JOIN works w ON w.work_id = rwl.rating_work_link_work_id
                ORDER BY w.work_scratch;
                """
            ).fetchall()
            assert generated_ratings == [
                ("isfdb:title:10", 3.5, 5, 7, "isfdb:generated", "generated", "synthetic", "isfdb:generated"),
                ("isfdb:title:20", 5.0, 5, 10, "isfdb:generated", "generated", "synthetic", "isfdb:generated"),
                ("isfdb:title:30", 3.0, 5, 6, "isfdb:generated", "generated", "synthetic", "isfdb:generated"),
            ]

            entity_identifiers = conn.execute(
                """
                SELECT entity_identifier_scheme, entity_identifier_value, entity_identifier_is_primary
                FROM entity_identifiers
                ORDER BY entity_identifier_scheme, entity_identifier_value;
                """
            ).fetchall()
            assert entity_identifiers == [
                ("asin", "B000123456", 1),
                ("asin", "B000AUD123", 1),
                ("isbn_10", "0306406152", 1),
                ("isbn_13", "9780306406157", 1),
            ]

            item_identifiers = conn.execute(
                """
                SELECT item_identifier_scheme, item_identifier_value
                FROM item_identifiers
                ORDER BY item_identifier_scheme, item_identifier_value;
                """
            ).fetchall()
            assert item_identifiers == [
                ("asin", "B000123456"),
                ("asin", "B000AUD123"),
                ("isbn_10", "0306406152"),
                ("isbn_13", "9780306406157"),
            ]

            work_notes = conn.execute(
                """
                SELECT w.work_scratch, n.note, n.note_scratch, nwl.note_work_link_source
                FROM note_work_links nwl
                JOIN notes n ON n.note_id = nwl.note_work_link_note_id
                JOIN works w ON w.work_id = nwl.note_work_link_work_id;
                """
            ).fetchall()
            assert work_notes == [
                ("isfdb:title:10", "Title note for Book One.", "isfdb:note:501", "isfdb"),
            ]

            agent_notes = conn.execute(
                """
                SELECT a.agent_canonical_name, n.note
                FROM agent_note_links anl
                JOIN agents a ON a.agent_id = anl.agent_note_link_agent_id
                JOIN notes n ON n.note_id = anl.agent_note_link_note_id
                ORDER BY a.agent_canonical_name;
                """
            ).fetchall()
            assert agent_notes == [
                ("Acme Press", "Publisher history note."),
                ("Jane Author", "Author biographical note."),
            ]

            work_synopses = conn.execute(
                """
                SELECT w.work_scratch, s.synopsis, swl.synopsis_work_link_type
                FROM synopsis_work_links swl
                JOIN synopses s ON s.synopsis_id = swl.synopsis_work_link_synopsis_id
                JOIN works w ON w.work_id = swl.synopsis_work_link_work_id;
                """
            ).fetchall()
            assert work_synopses == [
                ("isfdb:title:10", "Synopsis for Book One.", "short"),
            ]

            manifestation_note = conn.execute(
                """
                SELECT manifestation_note
                FROM manifestations
                WHERE manifestation_scratch = 'isfdb:pub:100';
                """
            ).fetchone()
            assert manifestation_note is not None
            assert "Publication note for the first edition." in manifestation_note[0]

            generated_annotations = conn.execute(
                """
                SELECT
                    i.item_scratch,
                    a.annotation_kind,
                    a.annotation_anchor_start,
                    a.annotation_anchor_end,
                    a.annotation_selected_text,
                    a.annotation_note_text,
                    a.annotation_source,
                    a.annotation_extra_json,
                    a.annotation_scratch
                FROM annotations a
                JOIN items i ON i.item_id = a.annotation_item_id
                ORDER BY i.item_scratch;
                """
            ).fetchall()
            assert generated_annotations == [
                (
                    "isfdb:pub:101",
                    "highlight",
                    "0.510",
                    "0.525",
                    "Generated highlight for Book One Second.",
                    "Deterministic annotation for ISFDB publication 101.",
                    "isfdb:generated",
                    '{"source_pub_id": 101}',
                    "isfdb:pub:101;generated_annotation",
                ),
                (
                    "isfdb:pub:200",
                    "highlight",
                    "0.027",
                    "0.042",
                    "Generated highlight for Book Two First.",
                    "Deterministic annotation for ISFDB publication 200.",
                    "isfdb:generated",
                    '{"source_pub_id": 200}',
                    "isfdb:pub:200;generated_annotation",
                ),
            ]

            for link_table, work_column in [
                ("label_work_links", "label_work_link_work_id"),
                ("genre_work_links", "genre_work_link_work_id"),
                ("series_work_links", "series_work_link_work_id"),
            ]:
                missing_count = conn.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM works w
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM {link_table} links
                        WHERE links.{work_column} = w.work_id
                    );
                    """
                ).fetchone()[0]
                assert missing_count == 0

            unexpected_nulls = []
            for table in module.METADATA_FIXTURE_BACKFILL_TABLES:
                table_exists = conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?;",
                    (table,),
                ).fetchone()
                if table_exists is None:
                    continue
                if conn.execute(f'SELECT COUNT(*) FROM "{table}";').fetchone()[0] == 0:
                    continue
                columns = conn.execute(f'PRAGMA table_info("{table}");').fetchall()
                pk_columns = [str(row[1]) for row in columns if int(row[5] or 0) > 0]
                assert len(pk_columns) == 1
                for _cid, column, _column_type, _not_null, _default, _pk in columns:
                    if not module._should_backfill_metadata_fixture_column(
                        str(column), pk_columns[0]
                    ):
                        continue
                    null_count = conn.execute(
                        f'SELECT COUNT(*) FROM "{table}" WHERE "{column}" IS NULL;'
                    ).fetchone()[0]
                    if null_count:
                        unexpected_nulls.append((table, str(column), null_count))

            assert unexpected_nulls == []
            assert conn.execute(
                """
                SELECT COUNT(*)
                FROM annotations
                WHERE annotation_source_deleted_datestamp_ep_k IS NULL
                  AND annotation_device_id IS NULL;
                """
            ).fetchone()[0] == counts["annotations"]
        finally:
            conn.close()
    finally:
        stage_conn.close()

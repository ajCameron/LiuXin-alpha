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
                (1, "Jane Author", "Jane Author", None, "Author", 1, None),
            ],
        )
        stage_conn.executemany(
            module.STAGE_SPECS["titles"].insert_sql,
            [
                (
                    10,
                    "Book One",
                    None,
                    None,
                    None,
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
                    "Book Two",
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
                    None,
                    None,
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
                    None,
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
            ],
        )
        stage_conn.executemany(
            module.STAGE_SPECS["publishers"].insert_sql,
            [
                (5, "Acme Press", None),
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
            ],
        )
        stage_conn.executemany(
            module.STAGE_SPECS["languages"].insert_sql,
            [
                (1, "English", "en"),
            ],
        )
        stage_conn.executemany(
            module.STAGE_SPECS["tags"].insert_sql,
            [
                (1, "Space Opera", 0),
                (2, "Shared Tag", 1),
                (3, "Unselected Tag", 0),
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
            ],
        )
        stage_conn.commit()

        module._create_stage_indexes(stage_conn)
        selection_counts = module._materialize_selected_subset(stage_conn, max_pubs=None)
        assert selection_counts["selected_tags"] == 2
        assert selection_counts["selected_tag_mappings"] == 3

        output_db = tmp_path / "isfdb_regression.test_db"
        counts = module._build_frbr_target(stage_conn=stage_conn, output_db=output_db)

        assert counts["works"] == 2
        assert counts["expressions"] == 2
        assert counts["manifestations"] == 3
        assert counts["agent_work_links"] == 2
        assert counts["agent_manifestation_links"] == 3
        assert counts["language_work_links"] == 2
        assert counts["series_work_links"] == 2
        assert counts["labels"] == 2
        assert counts["label_work_links"] == 3
        assert counts["expression_manifestation_links"] == 3

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
            assert len(author_priorities) == len(set(author_priorities)) == 2

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
            assert len(language_priorities) == len(set(language_priorities)) == 2

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
            assert len(series_priorities) == len(set(series_priorities)) == 2

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
                ("Shared Tag", "shared-tag", "isfdb:tag:2;status:1"),
                ("Space Opera", "space-opera", "isfdb:tag:1;status:0"),
            ]

            linked_labels = conn.execute(
                """
                SELECT w.work_scratch, l.label_text, lwl.label_work_link_priority
                FROM label_work_links lwl
                JOIN labels l ON l.label_id = lwl.label_work_link_label_id
                JOIN works w ON w.work_id = lwl.label_work_link_work_id
                ORDER BY w.work_scratch, l.label_text;
                """
            ).fetchall()
            assert [(work, label) for work, label, _priority in linked_labels] == [
                ("isfdb:title:10", "Shared Tag"),
                ("isfdb:title:10", "Space Opera"),
                ("isfdb:title:20", "Space Opera"),
            ]
            priorities_by_label = {}
            for _work, label, priority in linked_labels:
                priorities_by_label.setdefault(label, set()).add(priority)
            assert len(priorities_by_label["Space Opera"]) == 2
        finally:
            conn.close()
    finally:
        stage_conn.close()

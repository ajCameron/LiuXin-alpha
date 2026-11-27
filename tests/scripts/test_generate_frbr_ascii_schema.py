"""
Keep the FRBR schema sketch's generated lists true to a generated catalogue.
"""

from __future__ import annotations

import sqlite3
from contextlib import closing

from LiuXin_alpha.databases.database import Database
from scripts import generate_frbr_ascii_schema as sketch


def test_every_listed_table_exists_in_a_generated_catalogue(tmp_path):
    """
    Each table, link table and view the report names exists in a new catalogue.

    Example:
        >>> test_every_listed_table_exists_in_a_generated_catalogue(tmp)  # doctest: +SKIP


    :param tmp_path: Per-test directory.
    :return: ``None`` after comparing names.
    """
    path = tmp_path / "sketch.sqlite"
    with Database(
        metadata={"database_path": str(path)},
        create=True,
        backup=False,
        repair_bootstrap_rows=False,
        enable_storage_manager=False,
        enable_maintenance=False,
    ):
        pass
    with closing(sqlite3.connect(path)) as connection:
        present = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type IN ('table', 'view')"
            )
        }
    root = sketch.ROOT / sketch.GENERATOR
    groups, _ = sketch.load_tables(root)
    listed = {name for names in groups.values() for name in names}
    listed |= {row["table"] for row in sketch.load_interlinks(root)}
    listed |= {row["table"] for row in sketch.load_intralinks(root)}
    listed |= set(sketch.load_views(root))
    assert listed - present == set()
    assert {"synopsis_work_links", "digital_asset_hash_links"} <= listed


def test_report_lists_every_group_and_counts():
    """
    The report has its sections and lists each ``table_sql`` table once.

    Example:
        >>> test_report_lists_every_group_and_counts()  # doctest: +SKIP


    :return: ``None`` after checking the report.
    """
    report = sketch.render_report()
    for heading in (
        "## Counts",
        "## Overview",
        "## Tables by group",
        "## Interlink tables",
    ):
        assert heading in report
    groups, _ = sketch.load_tables(sketch.ROOT / sketch.GENERATOR)
    for name in (name for names in groups.values() for name in names):
        assert f"- `{name}`" in report

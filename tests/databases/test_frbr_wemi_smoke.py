# tests/databases/database_driver_plugins/sqlite_database_driver/test_sqlite_database_driver_generator_frbr_smoke.py

"""Smoke tests for the FRBR database generator.

These tests are intentionally small and are meant to fail loudly if the
generator cannot run end-to-end.
"""

from __future__ import annotations

import pathlib
import sqlite3

from LiuXin_alpha.databases.column_metadata import (
    DISPLAY_COLUMNS,
    ColumnEmptyValuePolicy,
    ColumnMergePolicy,
    ColumnSemanticRole,
    default_column_case_sensitive,
    infer_column_metadata,
)
from LiuXin_alpha.databases.database_driver_plugins.SQL.database_generator_frbr import database_generator as frbr_gen
from LiuXin_alpha.databases.database_driver_plugins.SQL.utility_mixins import ColumnNameMixin


def _frbr_pkg_root() -> pathlib.Path:
    return pathlib.Path(frbr_gen.__file__).resolve().parent


def test_frbr_generator_resources_are_present() -> None:
    """Sanity-check that the FRBR generator resources are packaged and non-empty."""
    pkg_root = _frbr_pkg_root()

    # TOML-first generator inputs.
    for rel in ["interlink_table_requests.toml", "intralink_table_requests.toml", "aggregate_tables.toml"]:
        p = pkg_root / rel
        assert p.is_file(), f"Missing FRBR generator spec file: {p}"
        assert p.read_text(encoding="utf-8", errors="replace").strip(), f"Empty FRBR generator spec file: {p}"

    # Main-table & trigger DDL still lives in folders (until TOML fully replaces legacy SQL bundles).
    for folder in ["table_sql", "trigger_sql"]:
        root = pkg_root / folder
        assert root.is_dir(), f"Expected {folder}/ under {pkg_root}"
        sql_files = sorted(root.rglob("*.sql"))
        assert sql_files, f"No .sql files found under {root}"

        # Guard against accidentally packaging empty placeholder files.
        for path in sql_files[:20]:
            text = path.read_text(encoding="utf-8", errors="replace")
            assert text.strip(), f"SQL file is empty: {path}"


def test_frbr_generator_create_new_database_smoke(tmp_path: pathlib.Path) -> None:
    """Run the generator end-to-end and assert that core tables exist afterwards."""
    db_path = tmp_path / "frbr_smoke.db"
    conn = sqlite3.connect(str(db_path))
    try:
        # Make FK issues surface immediately during generation.
        conn.execute("PRAGMA foreign_keys = ON;")
        try:
            frbr_gen.create_new_database(conn)
        except Exception as e:  # pragma: no cover
            raise AssertionError("FRBR generator did not run to completion") from e

        # Basic existence checks for core WEMI tables.
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table';")}

        expected_main = {
            "agents",
            "works",
            "expressions",
            "manifestations",
            "items",
            "column_metadata",
        }
        missing_main = sorted(expected_main - tables)
        assert not missing_main, f"Missing expected main tables: {missing_main}. Present: {sorted(tables)[:50]}"

        policies = {
            (row[0], row[1]): bool(row[2])
            for row in conn.execute(
                """
                SELECT
                  column_metadata_table_name,
                  column_metadata_column_name,
                  column_metadata_case_sensitive
                FROM column_metadata
                WHERE (column_metadata_table_name = 'tags' AND column_metadata_column_name = 'tag')
                   OR (column_metadata_table_name = 'works' AND column_metadata_column_name = 'work_title');
                """
            )
        }
        assert policies == {
            ("tags", "tag"): False,
            ("works", "work_title"): False,
        }

        tag_index_columns = [
            row[2]
            for row in conn.execute("PRAGMA index_info(`idx_tags_unique_phash`);")
        ]
        assert tag_index_columns == ["tag_phash"]

        stored_policies = {
            (row[0], row[1]): row[2:]
            for row in conn.execute(
                """
                SELECT
                  column_metadata_table_name,
                  column_metadata_column_name,
                  column_metadata_case_sensitive,
                  column_metadata_semantic_role,
                  column_metadata_normalization_profile,
                  column_metadata_comparison_column,
                  column_metadata_empty_value_policy,
                  column_metadata_merge_policy,
                  column_metadata_validation_profile
                FROM column_metadata;
                """
            )
        }
        physical_metadata = {}
        managed_tables = sorted(
            table for table in tables if not table.startswith("sqlite_")
        )
        table_columns = {}
        for table in managed_tables:
            quoted_table = table.replace("`", "``")
            column_info = list(
                conn.execute(f"PRAGMA table_info(`{quoted_table}`);")
            )
            foreign_keys = {
                str(row[3])
                for row in conn.execute(
                    f"PRAGMA foreign_key_list(`{quoted_table}`);"
                )
            }
            table_columns[table] = {
                str(row[1]): str(row[2]).upper() for row in column_info
            }
            for row in column_info:
                column = str(row[1])
                physical_metadata[(table, column)] = infer_column_metadata(
                    table,
                    column,
                    str(row[2] or ""),
                    is_primary_key=bool(row[5]),
                    is_foreign_key=column in foreign_keys,
                )

        assert set(stored_policies) == set(physical_metadata)
        for key, metadata in physical_metadata.items():
            assert stored_policies[key] == (
                int(metadata.case_sensitive),
                metadata.semantic_role.value,
                metadata.normalization_profile.value,
                metadata.comparison_column,
                metadata.empty_value_policy.value,
                metadata.merge_policy.value,
                metadata.validation_profile.value,
            )
            if metadata.comparison_column is not None:
                assert metadata.comparison_column in table_columns[metadata.table]

        assert physical_metadata[("works", "work_id")].semantic_role is ColumnSemanticRole.IDENTIFIER
        assert (
            physical_metadata[("works", "work_original_language_id")].semantic_role
            is ColumnSemanticRole.RELATIONSHIP_KEY
        )
        assert (
            physical_metadata[("works", "work_is_fiction")].semantic_role
            is ColumnSemanticRole.BOOLEAN
        )
        assert (
            physical_metadata[("works", "work_created_timestamp_ep_k")].semantic_role
            is ColumnSemanticRole.DATE_TIME
        )
        assert (
            physical_metadata[("works", "work_created_timestamp_ep_k")].merge_policy
            is ColumnMergePolicy.PRESERVE_EXISTING
        )
        assert (
            physical_metadata[("works", "work_scratch")].empty_value_policy
            is ColumnEmptyValuePolicy.PRESERVE
        )
        assert (
            physical_metadata[
                ("backup_policies", "backup_policy_required_store_tags_json")
            ].semantic_role
            is ColumnSemanticRole.STRUCTURED_DATA
        )
        assert (
            physical_metadata[
                ("backup_workflow_sources", "backup_workflow_source_expected_size")
            ].semantic_role
            is ColumnSemanticRole.NUMBER
        )
        assert (
            physical_metadata[
                (
                    "digital_asset_last_read_position_links",
                    "digital_asset_last_read_position_link_priority",
                )
            ].semantic_role
            is ColumnSemanticRole.ORDERING
        )
        assert (
            physical_metadata[("languages", "language_bcp47_primary")].semantic_role
            is ColumnSemanticRole.CODE
        )
        assert (
            physical_metadata[("works", "work_wikipedia_link")].semantic_role
            is ColumnSemanticRole.LOCATOR
        )
        assert (
            physical_metadata[
                ("last_read_positions", "last_read_position_cfi")
            ].semantic_role
            is ColumnSemanticRole.LOCATOR
        )
        assert (
            physical_metadata[
                ("entity_identifiers", "entity_identifier_value")
            ].semantic_role
            is ColumnSemanticRole.IDENTIFIER
        )

        policy_document = (
            pathlib.Path(__file__).resolve().parents[2]
            / "docs"
            / "development"
            / "column-metadata.md"
        ).read_text(encoding="utf-8")
        for table, column in DISPLAY_COLUMNS:
            headings = table_columns[table]
            assert headings[column] == "TEXT"
            metadata = physical_metadata[(table, column)]
            assert stored_policies[(table, column)] == (
                int(default_column_case_sensitive(table, column)),
                metadata.semantic_role.value,
                metadata.normalization_profile.value,
                metadata.comparison_column,
                metadata.empty_value_policy.value,
                metadata.merge_policy.value,
                metadata.validation_profile.value,
            )
            if metadata.comparison_column is not None:
                assert headings[metadata.comparison_column] == "TEXT"
            assert f"`{table}`" in policy_document
            assert f"`{column}`" in policy_document

        # Basic existence check for at least one canonical interlink table.
        agent_work_link, _ = ColumnNameMixin.get_interlink_table_name("agents", "works")
        agent_expr_link, _ = ColumnNameMixin.get_interlink_table_name("agents", "expressions")
        assert (
            agent_work_link in tables or agent_expr_link in tables
        ), f"Expected interlink table missing ({agent_work_link!r} or {agent_expr_link!r}). Present: {sorted(tables)[:80]}"

    finally:
        conn.close()

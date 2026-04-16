"""Unit tests for link-table SQL generation requested columns.

These are lightweight and do not rely on the full FRBR generator bundle.
"""

from __future__ import annotations

from LiuXin_alpha.databases.database_driver_plugins.SQL.utility_mixins import SQLiteTableLinkingMixin


class _Dummy(SQLiteTableLinkingMixin):
    pass


def test_link_sql_all_includes_origin_policy_data() -> None:
    d = _Dummy()
    sql_list, _ = d.direct_get_direct_link_main_tables_sql(
        primary_table="agents",
        secondary_table="works",
        requested_cols="all",
        nullable_fks=True,
    )
    sql = "\n".join(sql_list)
    assert "_origin" in sql
    assert "_policy" in sql
    assert "_data" in sql


def test_link_sql_bespoke_columns_are_emitted_as_text() -> None:
    d = _Dummy()
    sql_list, table_name = d.direct_get_direct_link_main_tables_sql(
        primary_table="agents",
        secondary_table="works",
        requested_cols={"priority", "type", "origin", "policy", "data", "extra_meta"},
        nullable_fks=True,
    )
    sql = "\n".join(sql_list)
    assert "`agent_work_link_extra_meta` TEXT NULL" in sql, f"missing bespoke column in {table_name}"


def test_link_sql_nullable_sentinel_does_not_create_physical_column() -> None:
    d = _Dummy()
    sql_list, table_name = d.direct_get_direct_link_main_tables_sql(
        primary_table="agents",
        secondary_table="works",
        requested_cols={"priority", "nullable"},
        nullable_fks=True,
    )
    sql = "\n".join(sql_list)
    assert "nullable" not in sql.lower(), f"nullable should not be a physical column for {table_name}"

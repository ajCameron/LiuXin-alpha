"""
Check generated link SQL text without building a full FRBR catalogue.

Uses a minimal mixin host and asserts metadata-column fragments, one source-column
declaration and absence of a nullable pseudo-column. No SQL is executed by these
tests.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/test_link_sql_generation_requested_columns.py
"""

from __future__ import annotations

from LiuXin_alpha.databases.database_driver_plugins.SQL.utility_mixins import SQLiteTableLinkingMixin


class _Dummy(SQLiteTableLinkingMixin):
    """
    Expose the shared SQLite link SQL builder without connection or name-resolution overrides.

    Example:
        >>> isinstance(_Dummy(), SQLiteTableLinkingMixin)
        True
    """
    pass


def test_link_sql_all_includes_origin_source_policy_data() -> None:
    """
    Require origin, source, policy and data fragments when requesting all link columns.

    Example:
        >>> test_link_sql_all_includes_origin_source_policy_data()


    :return: None; failed expectations raise AssertionError.
    """
    d = _Dummy()
    sql_list, _ = d.direct_get_direct_link_main_tables_sql(
        primary_table="agents",
        secondary_table="works",
        requested_cols="all",
        nullable_fks=True,
    )
    sql = "\n".join(sql_list)
    assert "_origin" in sql
    assert "_source" in sql
    assert "_policy" in sql
    assert "_data" in sql


def test_link_sql_bespoke_columns_are_emitted_as_text() -> None:
    """
    Require nullable TEXT declarations for bespoke extra_meta and standard source fields.

    Exercises the lower-level SQL builder, whose bespoke-column support differs from
    strict FRBR TOML validation.

    Example:
        >>> test_link_sql_bespoke_columns_are_emitted_as_text()


    :return: None; failed expectations raise AssertionError.
    """
    d = _Dummy()
    sql_list, table_name = d.direct_get_direct_link_main_tables_sql(
        primary_table="agents",
        secondary_table="works",
        requested_cols={"priority", "type", "origin", "policy", "data", "extra_meta"},
        nullable_fks=True,
    )
    sql = "\n".join(sql_list)
    assert "`agent_work_link_extra_meta` TEXT NULL" in sql, f"missing bespoke column in {table_name}"
    assert "`agent_work_link_source` TEXT NULL" in sql, f"missing standard source column in {table_name}"


def test_link_sql_source_request_does_not_duplicate_standard_column() -> None:
    """
    Require exactly one source-column TEXT NULL declaration when source is explicitly requested.

    Example:
        >>> test_link_sql_source_request_does_not_duplicate_standard_column()


    :return: None; failed expectations raise AssertionError.
    """
    d = _Dummy()
    sql_list, table_name = d.direct_get_direct_link_main_tables_sql(
        primary_table="agents",
        secondary_table="works",
        requested_cols={"priority", "source"},
        nullable_fks=True,
    )
    sql = "\n".join(sql_list)
    assert sql.count("`agent_work_link_source` TEXT NULL") == 1, (
        f"source should be a single standard column for {table_name}"
    )


def test_link_sql_nullable_sentinel_does_not_create_physical_column() -> None:
    """
    Require the nullable sentinel to be absent from generated SQL text.

    Example:
        >>> test_link_sql_nullable_sentinel_does_not_create_physical_column()


    :return: None; failed expectations raise AssertionError.
    """
    d = _Dummy()
    sql_list, table_name = d.direct_get_direct_link_main_tables_sql(
        primary_table="agents",
        secondary_table="works",
        requested_cols={"priority", "nullable"},
        nullable_fks=True,
    )
    sql = "\n".join(sql_list)
    assert "nullable" not in sql.lower(), f"nullable should not be a physical column for {table_name}"

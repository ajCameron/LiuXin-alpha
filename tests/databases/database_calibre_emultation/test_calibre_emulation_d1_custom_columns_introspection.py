from __future__ import annotations

import sqlite3

from LiuXin_alpha.utils.calibre_compat.calibre_database_emulation import CalibreDB
from LiuXin_alpha.databases.database_driver_plugins.SQL.calibre_database_generator import CalibreLibraryBuilder


def test_custom_column_introspection_flags_and_tables(provision_calibre_library) -> None:
    lib = provision_calibre_library(name="lib_d1_flags")

    b = CalibreLibraryBuilder(lib.root)
    num_series = b.create_custom_column(label="cc_series", name="Series", datatype="series")
    num_int = b.create_custom_column(label="cc_int", name="Number", datatype="int")

    db = CalibreDB.from_root(lib.root)
    info = db.schema_info(include_custom_columns=True)

    by_label = {c.label: c for c in info.custom_columns}
    assert "cc_series" in by_label
    assert "cc_int" in by_label

    s = by_label["cc_series"]
    assert s.num == num_series
    assert s.normalized is True
    assert s.expects_link_table is True
    assert s.value_table == f"custom_column_{num_series}"
    assert s.link_table == f"books_custom_column_{num_series}_link"
    assert s.has_value_table is True
    assert s.has_link_table is True
    assert s.link_has_extra is True

    i = by_label["cc_int"]
    assert i.num == num_int
    assert i.normalized is False
    assert i.expects_link_table is False
    assert i.value_table == f"custom_column_{num_int}"
    assert i.link_table == f"books_custom_column_{num_int}_link"
    assert i.has_value_table is True
    # Non-normalised columns do not create the link table in Calibre.
    assert i.has_link_table is False


def test_schema_info_records_missing_custom_tables_in_best_effort(provision_calibre_library) -> None:
    lib = provision_calibre_library(name="lib_d1_missing_tables")
    b = CalibreLibraryBuilder(lib.root)
    num_series = b.create_custom_column(label="cc_series", name="Series", datatype="series")

    # Simulate a mangled DB: drop the dynamic value table.
    conn = sqlite3.connect(str(lib.root / "metadata.db"))
    try:
        conn.execute(f"DROP TABLE custom_column_{num_series}")
        conn.commit()
    finally:
        conn.close()

    db = CalibreDB.from_root(lib.root)
    info = db.schema_info(best_effort=True, include_custom_columns=True)

    codes = [i.code for i in info.issues]
    assert "missing_custom_value_table" in codes

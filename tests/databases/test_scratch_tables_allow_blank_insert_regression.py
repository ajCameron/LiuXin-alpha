from __future__ import annotations

import sqlite3


def test_all_scratch_tables_allow_blank_insert(provision_test_database) -> None:
    """Regression: any table with a *_scratch column must accept a scratch-only INSERT.

    DriverWrapper.get_blank_row() works by:
      1) INSERTing a new row with only the scratch column populated
      2) SELECTing it back
      3) later, updating FK/type/etc.

    If a table has any NOT NULL column without a DEFAULT, step (1) will fail.
    """

    provisioned = provision_test_database("test_db_13")

    conn = sqlite3.connect(str(provisioned.db_path))
    try:
        # Constant tables can intentionally be write-locked by FRBR generator triggers.
        read_only_tables = {
            str(r[0]).replace("block_insert_on_", "", 1)
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE 'block_insert_on_%';"
            ).fetchall()
        }

        tables = [
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name;"
            ).fetchall()
        ]

        for table in tables:
            if table in read_only_tables:
                continue
            cols = conn.execute(f"PRAGMA table_info(`{table}`);").fetchall()
            scratch_cols = [str(c[1]) for c in cols if str(c[1]).endswith("_scratch")]
            if not scratch_cols:
                continue

            scratch_col = scratch_cols[0]
            try:
                conn.execute(
                    f"INSERT INTO `{table}` (`{scratch_col}`) VALUES (?);",
                    ("scratch-regression",),
                )
            except sqlite3.IntegrityError as e:
                raise AssertionError(
                    f"{table}: scratch-only INSERT failed; this breaks DriverWrapper.get_blank_row()."
                ) from e

        conn.commit()
    finally:
        conn.close()

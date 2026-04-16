from __future__ import annotations

import sqlite3

from pathlib import Path
from typing import Any

from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.databases.database_driver_plugins.SQL.database_generator_frbr import database_generator as frbr_gen


class MiniDriverWrapper:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def get_allowed_tables_snapshot(self):
        return set(self._tables())

    def _tables(self) -> list[str]:
        rows = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name;"
        ).fetchall()
        return [str(r[0]) for r in rows]

    def _columns(self, table: str) -> list[str]:
        return [str(row[1]) for row in self.conn.execute(f"PRAGMA table_info(`{table}`);").fetchall()]

    def identify_table_from_row_dict(self, row_dict: dict[str, Any]):
        keys = set(row_dict.keys())
        matches = [table for table in self._tables() if keys.issubset(set(self._columns(table)))]
        if not matches:
            raise KeyError(f"Could not identify table from row_dict keys: {sorted(keys)}")
        if len(matches) == 1:
            return matches[0]
        # Prefer the table with the smallest column superset.
        matches.sort(key=lambda table: len(self._columns(table)))
        return matches[0]

    def identify_table_from_column(self, column: str, error: bool = True):
        matches = [table for table in self._tables() if column in self._columns(table)]
        if not matches:
            if error:
                raise KeyError(column)
            return None
        if len(matches) == 1:
            return matches[0]
        # Prefer non-link/non-helper tables only as a convenience.
        matches.sort(key=lambda table: (table.endswith('s') is False, len(self._columns(table))))
        return matches[0]

    def get_id_column(self, table: str) -> str:
        for row in self.conn.execute(f"PRAGMA table_info(`{table}`);").fetchall():
            if int(row[5]) == 1:
                return str(row[1])
        raise KeyError(f"No primary key id column found for {table!r}")

    def add_row(self, row_dict: dict[str, Any]):
        table = self.identify_table_from_row_dict(row_dict)
        columns = list(row_dict.keys())
        placeholders = ", ".join(["?"] * len(columns))
        quoted_cols = ", ".join(f"`{col}`" for col in columns)
        sql = f"INSERT INTO `{table}` ({quoted_cols}) VALUES ({placeholders})"
        cur = self.conn.execute(sql, [row_dict[col] for col in columns])
        self.conn.commit()
        return int(cur.lastrowid)

    def get_row_from_id(self, table: str, row_id: int):
        id_col = self.get_id_column(table)
        cur = self.conn.execute(f"SELECT * FROM `{table}` WHERE `{id_col}` = ?", (int(row_id),))
        row = cur.fetchone()
        if row is None:
            return None
        cols = self._columns(table)
        return dict(zip(cols, row, strict=True))

    def update_row(self, row_dict: dict[str, Any]):
        table = self.identify_table_from_row_dict(row_dict)
        id_col = self.get_id_column(table)
        row_id = row_dict[id_col]
        columns = [col for col in row_dict.keys() if col != id_col]
        assignments = ", ".join(f"`{col}` = ?" for col in columns)
        sql = f"UPDATE `{table}` SET {assignments} WHERE `{id_col}` = ?"
        params = [row_dict[col] for col in columns] + [row_id]
        self.conn.execute(sql, params)
        self.conn.commit()

    def get_blank_row(self, table: str):
        return {col: None for col in self._columns(table)}

    def ensure_row_has_id(self, row_dict: dict[str, Any]):
        table = self.identify_table_from_row_dict(row_dict)
        id_col = self.get_id_column(table)
        if row_dict.get(id_col) is None:
            new_id = self.add_row({k: v for k, v in row_dict.items() if k != id_col})
            row_dict = dict(row_dict)
            row_dict[id_col] = new_id
        return row_dict

    def get_interlinked_tables(self, table: str):
        return []

    def check_for_intralink_table(self, table: str):
        return False


class MiniDB:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        self.driver_wrapper = MiniDriverWrapper(conn)
        self.uuid = None

    def get_tables(self, force_refresh: bool = False):
        return self.driver_wrapper._tables()

    def get_column_headings(self, table: str):
        return self.driver_wrapper._columns(table)

    def get_row_from_id(self, table: str, row_id: int):
        row_dict = self.driver_wrapper.get_row_from_id(table, row_id)
        if row_dict is None:
            return None
        return Row(database=self, row_dict=row_dict)

    def search(self, table: str, column: str, search_term: Any):
        cur = self.conn.execute(f"SELECT * FROM `{table}` WHERE `{column}` = ?", (search_term,))
        cols = self.driver_wrapper._columns(table)
        return [Row(database=self, row_dict=dict(zip(cols, row, strict=True))) for row in cur.fetchall()]

    def delete(self, row: Row) -> None:
        table = row.table
        id_col = self.driver_wrapper.get_id_column(table)
        self.conn.execute(f"DELETE FROM `{table}` WHERE `{id_col}` = ?", (row[id_col],))
        self.conn.commit()



def build_mini_db(db_path: Path) -> MiniDB:
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON;")
    frbr_gen.create_new_database(conn)
    return MiniDB(conn)

"""Run native predicates over an explicitly supplied immutable row projection.

The owner supplies values and revision scope. This module performs no database
reads and defines no bibliographic field names or projection policy.
"""

from copy import deepcopy
from types import SimpleNamespace
from typing import Any, cast

from LiuXin_alpha.caches.query import CacheQueryEngine


class ProjectionQueryEngine(CacheQueryEngine):
    """Snapshot one projected table for the lifetime of this query engine."""

    def __init__(self, table, rows, columns, **kwargs):
        self.table_name = table
        self._rows = deepcopy(dict(rows))
        self.row_ids = frozenset(self._rows)
        self.column_headings = tuple(columns)
        self.id_column = "id"
        super().__init__(cast(Any, self), **kwargs)

    def has_main_table(self, table):
        return table == self.table_name

    def get_main_table(self, table):
        if not self.has_main_table(table):
            raise KeyError(table)
        return self

    def get_field(self, key):
        table, column = key.split(".", 1)
        if table != self.table_name or column not in self.column_headings:
            raise KeyError(key)
        return SimpleNamespace(field_key=key, table_name=table, column_name=column)

    def get_cached_value(self, row_id, key):
        field = self.get_field(key)
        return self._rows[row_id].get(field.column_name)

    def has_id(self, row_id):
        return row_id in self._rows

    def get_row_snapshot(self, row_id):
        return deepcopy(self._rows[row_id])

    def all_book_ids(self, type=frozenset):
        return type(self._rows)

    def __iter__(self):
        return iter(self._rows)

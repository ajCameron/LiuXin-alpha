"""Materialize only the data used by one live query or metadata read.

This internal execution view borrows the caller's database and transaction. It
retains point lookups (including misses), whole populations and directed link
snapshots only for that execution. It must not be retained across public calls
or advertised as a transaction snapshot independently of the caller's SQL unit.
"""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from typing import Any

from LiuXin_alpha.caches.api.storage_cache_api.storage_fields_api.base_field_api import (
    FieldBasicInterfaceAPI,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.base_table_api import (
    TableTypes,
)
from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_cache import (
    SchemaBackedStorageCache,
)
from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_fields.many_many_field import (
    SchemaBackedManyManyField,
)
from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_fields.many_one_field import (
    SchemaBackedManyOneField,
)
from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_fields.one_many_field import (
    SchemaBackedOneManyField,
)
from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_fields.one_one_field import (
    SchemaBackedSameTableField,
    SchemaBackedTwoTableOneOneField,
)
from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_tables.single_table import (
    SchemaBackedMainTableCache,
)
from LiuXin_alpha.databases.api import DatabaseAPI
from LiuXin_alpha.databases.schema_specs import StorageTableSpec


def _converted_row(
    database: DatabaseAPI, table: str, row: Mapping[str, Any]
) -> dict[str, Any]:
    """Retain the driver's declared-type policy after a portable raw row read.

    Portable macros intentionally preserve SQL values. Cache snapshots use the
    same conversion as driver bulk reads, including SQLite declared containers
    and PostgreSQL binary buffers. The driver caches the table's declarations.
    """
    headings = tuple(row)
    return database.driver._row_to_dict(
        table=table, headings=headings, row=tuple(row[column] for column in headings)
    )


class _QueryTable(SchemaBackedMainTableCache):
    """Capture individual identities until an operation needs the entire table."""

    def __init__(self, spec: StorageTableSpec, db: DatabaseAPI) -> None:
        """Retain schema and allocate a per-execution set of confirmed misses."""
        super().__init__(spec, db)
        self._missing: set[int] = set()

    def read(self, db: DatabaseAPI | None = None) -> None:
        """Read one full population using the caller's managed SQL connection."""
        if db is not None:
            self.db = db
        database = self._require_attachment()
        rows = database.macros.get_rows(self.table)
        self._replace_rows([_converted_row(database, self.table, row) for row in rows])
        self._missing.clear()

    def _all(self) -> None:
        """Materialize the population and its indexes once per execution."""
        if not self._loaded:
            self.read()

    def _one(self, row_id: int) -> None:
        """Remember a point result or confirmed miss without scanning other rows."""
        if self._loaded or row_id in self._rows_by_id or row_id in self._missing:
            return
        row = self._require_attachment().macros.get_row(self.table, row_id)
        if row is None:
            self._missing.add(row_id)
        else:
            self._rows_by_id[row_id] = deepcopy(
                _converted_row(self._require_attachment(), self.table, row)
            )

    @property
    def row_ids(self) -> tuple[int, ...]:
        """Return the complete ordered population, loading it when first requested."""
        self._all()
        return super().row_ids

    def has_id(self, table_id: int) -> bool:
        """Resolve one identity without implying that other rows were captured."""
        self._one(int(table_id))
        return super().has_id(table_id)

    def get_row_snapshot(self, table_id: int) -> dict[str, Any]:
        """Return an independent copy of the selected row, or raise for a miss."""
        self._one(int(table_id))
        return super().get_row_snapshot(table_id)

    def get_values_for(self, column: str) -> list[Any]:
        """Read the complete column population for scalar field projection."""
        self._all()
        return list(super().get_values_for(column))

    def get_unique_values(self, column: str) -> set[Any]:
        """Build and read complete value indexes when a query needs them."""
        self._all()
        return super().get_unique_values(column)

    def get_ids_for_value(self, column: str, value: Any) -> set[int]:
        """Retain the query engine's Python equality semantics over a full index."""
        self._all()
        return super().get_ids_for_value(column, value)


class DatabaseQueryView(SchemaBackedStorageCache):
    """Supply an ephemeral query read surface without an eager catalogue rebuild.

    Only schema descriptors are created initially. Point reads capture requested
    identities; filters load their actual populations; relationship projections load
    their own routes. The enclosing metadata service supplies repeatable SQL reads.
    No result, open cursor or owned connection survives the execution.
    """

    _main_table_type = _QueryTable

    def read(self, db: DatabaseAPI | None = None) -> None:
        """Discover schema without loading row data or constructing unused fields."""
        self._require_db(db)
        self.clear()
        self.read_tables(self.db)
        self._stale_link_tables.update(self._link_tables)
        self._is_loaded = self._is_initialized = True

    def _ensure_link_table_fresh(self, key: tuple[str, str]) -> None:
        """Capture one physical route and share its data with both orientations."""
        if key not in self._stale_link_tables:
            return
        table = self._link_tables[key]
        database = self._require_db()
        rows = [
            _converted_row(database, table.table, row)
            for row in database.macros.get_rows(table.table)
        ]
        for route, view in self._link_tables.items():
            if view.table == table.table:
                view._rebuild_indices(rows)
                self._stale_link_tables.discard(route)

    def get_field(
        self, name: str | FieldBasicInterfaceAPI[Any]
    ) -> FieldBasicInterfaceAPI[Any]:
        """Build only the scalar or directed relationship field a query requests."""
        key = name if isinstance(name, str) else name.field_key
        if key in self._fields:
            return self._fields[key]
        if "." not in key:
            matches = [
                f"{table.table}.{key}"
                for table in self._main_tables.values()
                if key in table.column_headings
            ]
            if len(matches) != 1:
                raise KeyError(key)
            key = matches[0]
            if key in self._fields:
                return self._fields[key]
        parts = key.split(".")
        db = self._require_db()
        field: FieldBasicInterfaceAPI[Any]
        if len(parts) == 2:
            table, column = parts
            main = self.get_main_table(table)
            if column not in main.column_headings:
                raise KeyError(key)
            field = SchemaBackedSameTableField(self, main, column, db)
        elif len(parts) == 3:
            source, target, column = parts
            main = self.get_main_table(target)
            if column not in main.column_headings or column == main.id_column:
                raise KeyError(key)
            link = self.get_link_table(source, target)
            field_type = {
                TableTypes.ONE_ONE: SchemaBackedTwoTableOneOneField,
                TableTypes.ONE_MANY: SchemaBackedOneManyField,
                TableTypes.MANY_ONE: SchemaBackedManyOneField,
            }.get(link.table_type, SchemaBackedManyManyField)
            field = field_type(
                self, source, self.get_main_table(source).id_column, target, column, db
            )
        else:
            raise KeyError(key)
        field.read(db)
        self._field_objects[key] = self._fields[key] = field
        return field

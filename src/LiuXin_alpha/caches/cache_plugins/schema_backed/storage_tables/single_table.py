"""
Concrete schema-backed implementation of the single-table cache API.
"""

from __future__ import annotations

from collections import defaultdict
from copy import deepcopy

from typing import Any, Iterable, Mapping, Optional, Sequence

from LiuXin_alpha.caches.api.storage_cache_api.storage_tables.base_table import (
    TableMetadata,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables.single_table import (
    StorageCacheSingleTableAPI,
)
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.databases.schema_specs import StorageTableSpec

from LiuXin_alpha.caches.cache_plugins.schema_backed.common import (
    _column_type_map,
    _default_value_column,
    _ensure_db,
)


class SchemaBackedMainTableCache(StorageCacheSingleTableAPI):
    """
    Generic cache for one main table.
    """

    spec: StorageTableSpec

    def __init__(self, spec: StorageTableSpec, db: Any) -> None:
        metadata = TableMetadata(
            table_name=spec.name,
            main_table=True,
            is_interlink=False,
            is_intralink=False,
        )
        super().__init__(table=spec.name, db=db, metadata=metadata)
        self.spec = spec
        self._rows_by_id: dict[int, dict[str, Any]] = {}
        self._row_order: list[int] = []
        self._value_indexes: dict[str, dict[Any, set[int]]] = {}
        self._loaded = False

    @property
    def id_column(self) -> str:
        if self.spec.id_column is None:
            raise RuntimeError(f"Table {self.table!r} does not expose an id column")
        return self.spec.id_column

    @property
    def default_value_column(self) -> Optional[str]:
        return _default_value_column(self.spec)

    @property
    def row_ids(self) -> tuple[int, ...]:
        return tuple(self._row_order)

    @property
    def column_headings(self) -> list[str]:
        return [col.name for col in self.spec.columns]

    @property
    def column_types(self) -> dict[str, str]:
        return _column_type_map(self.spec)

    def _row_from_snapshot(self, row_dict: Mapping[str, Any]) -> Row:
        return Row(database=self.db, row_dict=deepcopy(dict(row_dict)), read_only=True)

    def _refresh_indexes(self) -> None:
        value_indexes: dict[str, dict[Any, set[int]]] = {
            column: defaultdict(set)
            for column in self.column_headings
        }
        for row_id in self._row_order:
            row_dict = self._rows_by_id[row_id]
            for column in self.column_headings:
                value_indexes[column][row_dict.get(column)].add(row_id)
        self._value_indexes = {
            column: {value: set(ids) for value, ids in values.items()}
            for column, values in value_indexes.items()
        }

    def _replace_rows(self, row_dicts: Sequence[Mapping[str, Any]]) -> None:
        rows_by_id: dict[int, dict[str, Any]] = {}
        ordered_ids: list[int] = []

        sortable = []
        for index, row_dict in enumerate(row_dicts):
            row_copy = deepcopy(dict(row_dict))
            row_id = row_copy.get(self.id_column)
            if row_id is None:
                continue
            sortable.append((int(row_id), index, row_copy))

        sortable.sort(key=lambda item: (item[0], item[1]))

        for row_id, _index, row_copy in sortable:
            rows_by_id[int(row_id)] = row_copy
            ordered_ids.append(int(row_id))

        self._rows_by_id = rows_by_id
        self._row_order = ordered_ids
        self._refresh_indexes()
        self._loaded = True

    def read(self, db: Any) -> None:
        db = _ensure_db(self.db, db)
        self.db = db
        rows = db.get_all_rows(self.table, iterator_return=False)
        row_dicts = [row.row_dict for row in rows]
        self._replace_rows(row_dicts)

    def reload(self, db: Any) -> None:
        self.read(db=db)

    def linked_to(self) -> Iterable[str]:
        return tuple(self.spec.linked_tables)

    def get_values_for(self, column: str) -> Sequence[Any]:
        if column not in self._value_indexes:
            raise KeyError(column)
        return [self._rows_by_id[row_id].get(column) for row_id in self._row_order]

    def get_unique_values(self, column: str) -> set[Any]:
        if column not in self._value_indexes:
            raise KeyError(column)
        return set(self._value_indexes[column].keys())

    def get_ids_for_value(self, column: str, value: str) -> set[int]:
        if column not in self._value_indexes:
            raise KeyError(column)
        return set(self._value_indexes[column].get(value, set()))

    def get_col_value_from_id(self, table_id: int) -> Any:
        row_dict = self.get_row_snapshot(table_id)
        default_column = self.default_value_column
        if default_column is None:
            return row_dict
        return row_dict.get(default_column)

    def has_id(self, table_id: int) -> bool:
        return int(table_id) in self._rows_by_id

    def get_row_snapshot(self, table_id: int) -> dict[str, Any]:
        row_id = int(table_id)
        if row_id not in self._rows_by_id:
            raise KeyError(row_id)
        return deepcopy(self._rows_by_id[row_id])

    def get_row(self, table_id: int) -> Row:
        return self._row_from_snapshot(self.get_row_snapshot(table_id))

    def _normalize_row_payload(
        self,
        table_id_val_map: Mapping[int, Any],
        target_column: Optional[str],
    ) -> dict[int, dict[str, Any]]:
        payloads: dict[int, dict[str, Any]] = {}
        chosen_column = target_column or self.default_value_column
        if chosen_column is None:
            raise RuntimeError(f"Table {self.table!r} has no sensible default value column")

        for table_id, value in table_id_val_map.items():
            row_id = int(table_id)
            if isinstance(value, Mapping):
                payload = deepcopy(dict(value))
                payload[self.id_column] = row_id
            else:
                payload = {self.id_column: row_id, chosen_column: value}
            payloads[row_id] = payload
        return payloads

    def _refresh_ids(self, table_ids: Iterable[int]) -> None:
        db = _ensure_db(self.db)
        changed = False
        for table_id in table_ids:
            row_id = int(table_id)
            row = db.get_row_from_id(self.table, row_id)
            if row is None:
                if row_id in self._rows_by_id:
                    self._rows_by_id.pop(row_id, None)
                    self._row_order = [existing for existing in self._row_order if existing != row_id]
                    changed = True
                continue
            if row_id not in self._rows_by_id:
                self._row_order.append(row_id)
                self._row_order.sort()
            self._rows_by_id[row_id] = deepcopy(row.row_dict)
            changed = True
        if changed:
            self._refresh_indexes()

    def create(
        self,
        table_id_val_map: Mapping[int, Any],
        db: Any,
        target_column: Optional[str] = None,
        allow_case_change: bool = False,
    ) -> None:
        del allow_case_change
        self._create_to_db(table_id_val_map, db, target_column=target_column)
        self._create_to_cache(table_id_val_map, target_column=target_column)

    def _create_to_cache(
        self,
        table_id_val_map: Mapping[int, Any],
        target_column: Optional[str] = None,
        allow_case_change: bool = False,
    ) -> None:
        del target_column, allow_case_change
        self._refresh_ids(table_id_val_map.keys())

    def _create_to_db(
        self,
        table_id_val_map: Mapping[int, Any],
        db: Any,
        target_column: Optional[str] = None,
        allow_case_change: bool = False,
    ) -> None:
        del allow_case_change
        db = _ensure_db(self.db, db)
        payloads = self._normalize_row_payload(table_id_val_map, target_column)
        for payload in payloads.values():
            db.driver_wrapper.add_row(payload)

    def update(
        self,
        table_id_val_map: Mapping[int, Any],
        db: Any,
        target_column: Optional[str] = None,
        allow_case_change: bool = False,
    ) -> None:
        self._update_db(
            table_id_val_map,
            db,
            target_column=target_column,
            allow_case_change=allow_case_change,
        )
        self._update_cache(
            table_id_val_map,
            target_column=target_column,
            allow_case_change=allow_case_change,
        )

    def _update_cache(
        self,
        table_id_val_map: Mapping[int, Any],
        target_column: Optional[str] = None,
        allow_case_change: bool = False,
    ) -> None:
        del target_column, allow_case_change
        self._refresh_ids(table_id_val_map.keys())

    def _update_db(
        self,
        table_id_val_map: Mapping[int, Any],
        db: Any,
        target_column: Optional[str] = None,
        allow_case_change: bool = False,
    ) -> None:
        db = _ensure_db(self.db, db)
        payloads = self._normalize_row_payload(table_id_val_map, target_column)
        chosen_column = target_column or self.default_value_column

        for row_id, payload in payloads.items():
            if isinstance(table_id_val_map[row_id], Mapping):
                current = db.get_row_from_id(self.table, row_id)
                if current is None:
                    raise KeyError(f"No such row in {self.table!r}: {row_id}")
                merged = deepcopy(current.row_dict)
                merged.update(payload)
                db.driver_wrapper.update_row(merged)
                continue

            if chosen_column is None:
                raise RuntimeError(f"Table {self.table!r} has no sensible default value column")

            current_value = None
            if self.has_id(row_id):
                current_value = self._rows_by_id[row_id].get(chosen_column)

            new_value = payload.get(chosen_column)
            if (
                not allow_case_change
                and isinstance(current_value, str)
                and isinstance(new_value, str)
                and current_value.lower() == new_value.lower()
            ):
                continue

            db.driver_wrapper.update_column(self.table, row_id, chosen_column, new_value)

    def delete(
        self,
        table_ids: Iterable[int],
        db: Any,
    ) -> None:
        self._delete_from_db(table_ids, db)
        self._delete_from_cache(table_ids)

    def _delete_from_cache(self, table_ids: Iterable[int]) -> None:
        deleted = {int(table_id) for table_id in table_ids}
        self._rows_by_id = {
            row_id: row_dict
            for row_id, row_dict in self._rows_by_id.items()
            if row_id not in deleted
        }
        self._row_order = [row_id for row_id in self._row_order if row_id not in deleted]
        self._refresh_indexes()

    def _delete_from_db(self, table_ids: Iterable[str], db: Any) -> None:
        db = _ensure_db(self.db, db)
        ids = {int(table_id) for table_id in table_ids}
        if ids:
            db.driver_wrapper.delete_by_id(self.table, ids)


__all__ = ["SchemaBackedMainTableCache"]

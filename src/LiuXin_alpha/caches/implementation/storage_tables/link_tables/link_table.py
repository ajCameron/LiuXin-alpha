"""
Concrete schema-backed implementation of the link-table cache APIs.
"""

from __future__ import annotations

from collections import defaultdict
from copy import deepcopy

from typing import Any, Mapping, Optional, Sequence, cast

from LiuXin_alpha.caches.api.storage_cache_api.storage_tables.base_table import (
    TableMetadata,
    TableTypes,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables.link_tables.many_many_tables import (
    ManyManyLink,
    StorageCacheManyToManyLinkTable,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables.link_tables.many_one_tables import (
    ManyOneLink,
    StorageCacheManyToOneLinkTable,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables.link_tables.one_many_tables import (
    OneManyLink,
    StorageCacheOneToManyLinkTable,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables.link_tables.one_one_tables import (
    OneOneLink,
    StorageCacheOneToOneLinkTable,
)
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.databases.schema_specs import LinkCardinality, StorageLinkSpec

from LiuXin_alpha.caches.implementation.common import (
    _CachedLinkRecord,
    _column_type_map,
    _ensure_db,
)
from LiuXin_alpha.caches.implementation.storage_tables.single_table import (
    SchemaBackedMainTableCache,
)


class SchemaBackedLinkTable(
    StorageCacheOneToOneLinkTable[Any],
    StorageCacheOneToManyLinkTable,
    StorageCacheManyToOneLinkTable,
    StorageCacheManyToManyLinkTable,
):
    """
    Generic oriented cache over one database link table.
    """

    def __init__(
        self,
        *,
        db: Any,
        link_spec: StorageLinkSpec,
        src_table: SchemaBackedMainTableCache,
        dst_table: SchemaBackedMainTableCache,
        src_table_name: str,
        dst_table_name: str,
        src_link_col: str,
        dst_link_col: str,
    ) -> None:
        metadata = TableMetadata(
            table_name=link_spec.link_table,
            main_table=False,
            is_interlink=src_table_name != dst_table_name,
            is_intralink=src_table_name == dst_table_name,
        )
        super().__init__(table=link_spec.link_table, db=db, metadata=metadata)
        self.link_spec = link_spec
        self._src_table = src_table
        self._dst_table = dst_table
        self._src_table_name = src_table_name
        self._dst_table_name = dst_table_name
        self._src_link_col = src_link_col
        self._dst_link_col = dst_link_col
        self._records: list[_CachedLinkRecord] = []
        self._by_src: dict[int, list[_CachedLinkRecord]] = {}
        self._by_dst: dict[int, list[_CachedLinkRecord]] = {}
        self._by_pair: dict[tuple[int, int], list[_CachedLinkRecord]] = {}
        self._id_column: Optional[str] = None
        self._table_type = TableTypes.MANY_MANY
        self._priority = bool(link_spec.priority_link_col)
        self._typed = bool(link_spec.type_link_col)

    @property
    def primary_table(self) -> str:
        return self._src_table_name

    @property
    def secondary_table(self) -> str:
        return self._dst_table_name

    @property
    def designated_secondary_col(self) -> str:
        return self._dst_table.default_value_column or self.link_spec.secondary_id_col

    @property
    def column_headings(self) -> list[str]:
        try:
            return list(self.db.get_column_headings(self.table))
        except Exception:
            return []

    @property
    def column_types(self) -> dict[str, str]:
        spec = self.db.driver_wrapper.get_table_spec(self.table)
        return _column_type_map(spec)

    def _row_from_snapshot(self, row_dict: Mapping[str, Any]) -> Row:
        return Row(database=self.db, row_dict=deepcopy(dict(row_dict)), read_only=True)

    def _record_matches(self, record: _CachedLinkRecord, type_filter: Optional[str]) -> bool:
        if type_filter is None:
            return True
        if not self.typed:
            return False
        return record.link_type == type_filter

    def _ordered_records(
        self,
        records: Sequence[_CachedLinkRecord],
        *,
        require_ordering: bool = False,
    ) -> list[_CachedLinkRecord]:
        ordered = list(records)
        if self.priority:
            ordered.sort(
                key=lambda record: (
                    -float(record.priority) if record.priority is not None else 0.0,
                    record.sequence,
                )
            )
        elif require_ordering:
            ordered.sort(key=lambda record: record.sequence)
        return ordered

    def _to_row_dicts(self) -> list[dict[str, Any]]:
        rows = self.db.get_all_rows(self.table, iterator_return=False)
        return [deepcopy(row.row_dict) for row in rows]

    def _unique_single_columns(self) -> set[str]:
        conn = getattr(self.db, "conn", None)
        if conn is None:
            return set()

        unique_columns: set[str] = set()
        try:
            cursor = conn.cursor()
            cursor.execute(f"PRAGMA index_list('{self.table}')")
            indexes = cursor.fetchall()
            for index in indexes:
                is_unique = bool(index[2])
                if not is_unique:
                    continue
                index_name = index[1]
                cursor.execute(f"PRAGMA index_info('{index_name}')")
                columns = [row[2] for row in cursor.fetchall()]
                if len(columns) == 1:
                    unique_columns.add(columns[0])
        except Exception:
            return set()
        return unique_columns

    def _infer_table_type(self, records: Sequence[_CachedLinkRecord]) -> TableTypes:
        cardinality = self.link_spec.cardinality
        if cardinality == LinkCardinality.ONE_TO_ONE:
            return TableTypes.ONE_ONE
        if cardinality == LinkCardinality.ONE_TO_MANY:
            return TableTypes.ONE_MANY
        if cardinality == LinkCardinality.MANY_TO_ONE:
            return TableTypes.MANY_ONE
        if cardinality == LinkCardinality.MANY_TO_MANY:
            return TableTypes.MANY_MANY

        unique_columns = self._unique_single_columns()
        src_unique = self._src_link_col in unique_columns
        dst_unique = self._dst_link_col in unique_columns
        if src_unique and dst_unique:
            return TableTypes.ONE_ONE
        if src_unique:
            return TableTypes.MANY_ONE
        if dst_unique:
            return TableTypes.ONE_MANY

        src_seen: set[int] = set()
        dst_seen: set[int] = set()
        src_duplicate = False
        dst_duplicate = False

        for record in records:
            if record.src_id in src_seen:
                src_duplicate = True
            src_seen.add(record.src_id)
            if record.dst_id in dst_seen:
                dst_duplicate = True
            dst_seen.add(record.dst_id)

        if not src_duplicate and not dst_duplicate:
            return TableTypes.ONE_ONE
        if src_duplicate and not dst_duplicate:
            return TableTypes.ONE_MANY
        if not src_duplicate and dst_duplicate:
            return TableTypes.MANY_ONE
        return TableTypes.MANY_MANY

    def _rebuild_indices(self, row_dicts: Sequence[Mapping[str, Any]]) -> None:
        self._id_column = self.db.driver_wrapper.get_id_column(self.table)
        records: list[_CachedLinkRecord] = []
        for sequence, row_dict in enumerate(row_dicts):
            row_copy = deepcopy(dict(row_dict))
            src_id = row_copy.get(self._src_link_col)
            dst_id = row_copy.get(self._dst_link_col)
            if src_id is None or dst_id is None:
                continue
            row_id = row_copy.get(self._id_column) if self._id_column else None
            priority = None
            if self.link_spec.priority_link_col:
                priority_value = row_copy.get(self.link_spec.priority_link_col)
                if priority_value is not None:
                    try:
                        priority = float(priority_value)
                    except (TypeError, ValueError):
                        priority = None
            link_type = None
            if self.link_spec.type_link_col:
                raw_type = row_copy.get(self.link_spec.type_link_col)
                if raw_type is not None:
                    link_type = str(raw_type)
            records.append(
                _CachedLinkRecord(
                    src_id=int(src_id),
                    dst_id=int(dst_id),
                    row_dict=row_copy,
                    row_id=int(row_id) if row_id is not None else None,
                    link_type=link_type,
                    priority=priority,
                    sequence=sequence,
                )
            )

        by_src: dict[int, list[_CachedLinkRecord]] = defaultdict(list)
        by_dst: dict[int, list[_CachedLinkRecord]] = defaultdict(list)
        by_pair: dict[tuple[int, int], list[_CachedLinkRecord]] = defaultdict(list)

        for record in records:
            by_src[record.src_id].append(record)
            by_dst[record.dst_id].append(record)
            by_pair[(record.src_id, record.dst_id)].append(record)

        self._records = records
        self._by_src = {key: list(value) for key, value in by_src.items()}
        self._by_dst = {key: list(value) for key, value in by_dst.items()}
        self._by_pair = {key: list(value) for key, value in by_pair.items()}
        self._table_type = self._infer_table_type(records)

    def read(self, db: Any) -> None:
        db = _ensure_db(self.db, db)
        self.db = db
        self._rebuild_indices(self._to_row_dicts())

    def reload(self, db: Any) -> None:
        self.read(db)

    def _records_for_src(
        self,
        src_id: int,
        *,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> list[_CachedLinkRecord]:
        matches = [record for record in self._by_src.get(int(src_id), []) if self._record_matches(record, type_filter)]
        return self._ordered_records(matches, require_ordering=require_ordering)

    def _records_for_dst(
        self,
        dst_id: int,
        *,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> list[_CachedLinkRecord]:
        matches = [record for record in self._by_dst.get(int(dst_id), []) if self._record_matches(record, type_filter)]
        return self._ordered_records(matches, require_ordering=require_ordering)

    def _records_for_pair(
        self,
        src_id: int,
        dst_id: int,
        *,
        type_filter: Optional[str] = None,
    ) -> list[_CachedLinkRecord]:
        matches = [
            record
            for record in self._by_pair.get((int(src_id), int(dst_id)), [])
            if self._record_matches(record, type_filter)
        ]
        return self._ordered_records(matches)

    def _make_link_object(self, record: _CachedLinkRecord) -> Any:
        if self.table_type == TableTypes.ONE_ONE:
            return OneOneLink(
                src_id=record.src_id,
                dst_id=record.dst_id,
                link_row_id=record.row_id,
                link_type=record.link_type,
            )
        if self.table_type == TableTypes.ONE_MANY:
            return OneManyLink(
                src_id=record.src_id,
                dst_id=record.dst_id,
                link_row_id=record.row_id,
                link_type=record.link_type,
                priority=record.priority,
            )
        if self.table_type == TableTypes.MANY_ONE:
            return ManyOneLink(
                src_id=record.src_id,
                dst_id=record.dst_id,
                link_row_id=record.row_id,
                link_type=record.link_type,
                priority=record.priority,
            )
        return ManyManyLink(
            src_id=record.src_id,
            dst_id=record.dst_id,
            link_row_id=record.row_id,
            link_type=record.link_type,
            priority=record.priority,
        )

    def _optional_single(
        self,
        records: Sequence[_CachedLinkRecord],
        *,
        insist_on_singular: bool = True,
    ) -> Optional[_CachedLinkRecord]:
        if not records:
            return None
        if insist_on_singular and len(records) > 1:
            raise RuntimeError(f"Expected one link row, found {len(records)}")
        return records[0]

    def _src_row(self, src_id: int) -> Optional[Row]:
        if not self._src_table.has_id(src_id):
            return None
        return self._src_table.get_row(src_id)

    def _dst_row(self, dst_id: int) -> Optional[Row]:
        if not self._dst_table.has_id(dst_id):
            return None
        return self._dst_table.get_row(dst_id)

    def has_link(
        self,
        src_id: int,
        dst_id: int,
        type_filter: Optional[str] = None,
    ) -> bool:
        return bool(self._records_for_pair(src_id, dst_id, type_filter=type_filter))

    def has_src(
        self,
        dst_id: int,
        type_filter: Optional[str] = None,
    ) -> bool:
        return bool(self._records_for_dst(dst_id, type_filter=type_filter))

    def has_dst(
        self,
        src_id: int,
        type_filter: Optional[str] = None,
    ) -> bool:
        return bool(self._records_for_src(src_id, type_filter=type_filter))

    def has_dsts(
        self,
        src_id: int,
        type_filter: Optional[str] = None,
    ) -> bool:
        return self.has_dst(src_id, type_filter=type_filter)

    def has_srcs(
        self,
        dst_id: int,
        type_filter: Optional[str] = None,
    ) -> bool:
        return self.has_src(dst_id, type_filter=type_filter)

    def get_link(
        self,
        src_id: int,
        dst_id: int,
        insist_on_singular: bool = True,
        type_filter: Optional[str] = None,
    ) -> Optional[Any]:
        record = self._optional_single(
            self._records_for_pair(src_id, dst_id, type_filter=type_filter),
            insist_on_singular=insist_on_singular,
        )
        if record is None:
            return None
        return self._make_link_object(record)

    def get_links(
        self,
        src_id: int,
        dst_id: int,
    ) -> Sequence[Any]:
        return [self._make_link_object(record) for record in self._records_for_pair(src_id, dst_id)]

    def get_link_for_src(
        self,
        src_id: int,
        type_filter: Optional[str] = None,
    ) -> Optional[Any]:
        record = self._optional_single(self._records_for_src(src_id, type_filter=type_filter))
        if record is None:
            return None
        return self._make_link_object(record)

    def get_links_for_src(
        self,
        src_id: int,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[Any]:
        return [
            self._make_link_object(record)
            for record in self._records_for_src(src_id, require_ordering=require_ordering, type_filter=type_filter)
        ]

    def get_link_for_dst(
        self,
        dst_id: int,
        type_filter: Optional[str] = None,
    ) -> Optional[Any]:
        record = self._optional_single(self._records_for_dst(dst_id, type_filter=type_filter))
        if record is None:
            return None
        return self._make_link_object(record)

    def get_links_for_dst(
        self,
        dst_id: int,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[Any]:
        return [
            self._make_link_object(record)
            for record in self._records_for_dst(dst_id, require_ordering=require_ordering, type_filter=type_filter)
        ]

    def get_link_row(
        self,
        src_id: int,
        dst_id: int,
        insist_on_singular: bool = True,
        type_filter: Optional[str] = None,
    ) -> Optional[Row]:
        record = self._optional_single(
            self._records_for_pair(src_id, dst_id, type_filter=type_filter),
            insist_on_singular=insist_on_singular,
        )
        if record is None:
            return None
        return self._row_from_snapshot(record.row_dict)

    def get_link_rows(
        self,
        src_id: int,
        dst_id: int,
    ) -> Sequence[Row]:
        return [self._row_from_snapshot(record.row_dict) for record in self._records_for_pair(src_id, dst_id)]

    def get_link_row_for_src(
        self,
        src_id: int,
        type_filter: Optional[str] = None,
    ) -> Optional[Row]:
        record = self._optional_single(self._records_for_src(src_id, type_filter=type_filter))
        if record is None:
            return None
        return self._row_from_snapshot(record.row_dict)

    def get_link_rows_for_src(
        self,
        src_id: int,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[Row]:
        return [
            self._row_from_snapshot(record.row_dict)
            for record in self._records_for_src(src_id, require_ordering=require_ordering, type_filter=type_filter)
        ]

    def get_link_row_for_dst(
        self,
        dst_id: int,
        type_filter: Optional[str] = None,
    ) -> Optional[Row]:
        record = self._optional_single(self._records_for_dst(dst_id, type_filter=type_filter))
        if record is None:
            return None
        return self._row_from_snapshot(record.row_dict)

    def get_link_rows_for_dst(
        self,
        dst_id: int,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[Row]:
        return [
            self._row_from_snapshot(record.row_dict)
            for record in self._records_for_dst(dst_id, require_ordering=require_ordering, type_filter=type_filter)
        ]

    def get_src_id(
        self,
        dst_id: int,
        type_filter: Optional[str] = None,
    ) -> Optional[int]:
        record = self._optional_single(self._records_for_dst(dst_id, type_filter=type_filter))
        return None if record is None else record.src_id

    def get_src_ids(
        self,
        dst_id: int,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[int]:
        return [
            record.src_id
            for record in self._records_for_dst(dst_id, require_ordering=require_ordering, type_filter=type_filter)
        ]

    def get_src_row(
        self,
        dst_id: int,
        type_filter: Optional[str] = None,
    ) -> Optional[Row]:
        src_id = self.get_src_id(dst_id, type_filter=type_filter)
        if src_id is None:
            return None
        return self._src_row(src_id)

    def get_src_rows(
        self,
        dst_id: int,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[Row]:
        rows = []
        for src_id in self.get_src_ids(dst_id, require_ordering=require_ordering, type_filter=type_filter):
            row = self._src_row(src_id)
            if row is not None:
                rows.append(row)
        return rows

    def get_src_value(
        self,
        dst_id: int,
        src_column: str,
        type_filter: Optional[str] = None,
    ) -> Any:
        src_id = self.get_src_id(dst_id, type_filter=type_filter)
        if src_id is None:
            return None
        return self._src_table.get_row_snapshot(src_id).get(src_column)

    def get_src_values(
        self,
        dst_id: int,
        src_column: str,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[Any]:
        return [
            self._src_table.get_row_snapshot(src_id).get(src_column)
            for src_id in self.get_src_ids(dst_id, require_ordering=require_ordering, type_filter=type_filter)
        ]

    def get_src_ids_from_value(
        self,
        dst_value: Any,
        dst_column: str,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[int]:
        src_ids: list[int] = []
        for dst_id in sorted(self._dst_table.get_ids_for_value(dst_column, dst_value)):
            src_ids.extend(self.get_src_ids(dst_id, require_ordering=require_ordering, type_filter=type_filter))
        return src_ids

    def get_src_rows_from_value(
        self,
        dst_value: Any,
        dst_column: str,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[Row]:
        return [
            row
            for src_id in self.get_src_ids_from_value(
                dst_value,
                dst_column,
                require_ordering=require_ordering,
                type_filter=type_filter,
            )
            for row in [self._src_row(src_id)]
            if row is not None
        ]

    def get_dst_id(
        self,
        src_id: int,
        type_filter: Optional[str] = None,
    ) -> Optional[int]:
        record = self._optional_single(self._records_for_src(src_id, type_filter=type_filter))
        return None if record is None else record.dst_id

    def get_dst_ids(
        self,
        src_id: int,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[int]:
        return [
            record.dst_id
            for record in self._records_for_src(src_id, require_ordering=require_ordering, type_filter=type_filter)
        ]

    def get_dst_row(
        self,
        src_id: int,
        type_filter: Optional[str] = None,
    ) -> Optional[Row]:
        dst_id = self.get_dst_id(src_id, type_filter=type_filter)
        if dst_id is None:
            return None
        return self._dst_row(dst_id)

    def get_dst_rows(
        self,
        src_id: int,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[Row]:
        rows = []
        for dst_id in self.get_dst_ids(src_id, require_ordering=require_ordering, type_filter=type_filter):
            row = self._dst_row(dst_id)
            if row is not None:
                rows.append(row)
        return rows

    def get_dst_value(
        self,
        src_id: int,
        dst_column: str,
        type_filter: Optional[str] = None,
    ) -> Any:
        dst_id = self.get_dst_id(src_id, type_filter=type_filter)
        if dst_id is None:
            return None
        return self._dst_table.get_row_snapshot(dst_id).get(dst_column)

    def get_dst_values(
        self,
        src_id: int,
        dst_column: str,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[Any]:
        return [
            self._dst_table.get_row_snapshot(dst_id).get(dst_column)
            for dst_id in self.get_dst_ids(src_id, require_ordering=require_ordering, type_filter=type_filter)
        ]

    def get_dst_ids_from_value(
        self,
        src_value: Any,
        src_column: str,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[int]:
        dst_ids: list[int] = []
        for src_id in sorted(self._src_table.get_ids_for_value(src_column, src_value)):
            dst_ids.extend(self.get_dst_ids(src_id, require_ordering=require_ordering, type_filter=type_filter))
        return dst_ids

    def get_dst_rows_from_value(
        self,
        src_value: Any,
        src_column: str,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[Row]:
        return [
            row
            for dst_id in self.get_dst_ids_from_value(
                src_value,
                src_column,
                require_ordering=require_ordering,
                type_filter=type_filter,
            )
            for row in [self._dst_row(dst_id)]
            if row is not None
        ]

    def get_primary_id_secondary_value_map(self) -> dict[int, Any]:
        return {
            src_id: self.get_dst_value(src_id, self.designated_secondary_col)
            for src_id in self._by_src
            if self.get_dst_id(src_id) is not None
        }

    def get_primary_id_secondary_value_id_map(self) -> dict[int, int]:
        return {
            src_id: dst_id
            for src_id in self._by_src
            for dst_id in [self.get_dst_id(src_id)]
            if dst_id is not None
        }

    def get_secondary_id_primary_id_map(self) -> dict[int, int]:
        return {
            dst_id: src_id
            for dst_id in self._by_dst
            for src_id in [self.get_src_id(dst_id)]
            if src_id is not None
        }

    def _delete_matching_records(
        self,
        *,
        src_ids: Optional[set[int]] = None,
        dst_ids: Optional[set[int]] = None,
        row_ids: Optional[set[int]] = None,
    ) -> None:
        if self._id_column is None:
            return
        ids_to_delete: set[int] = set()
        for record in self._records:
            if row_ids is not None and record.row_id in row_ids:
                ids_to_delete.add(cast(int, record.row_id))
                continue
            if src_ids is not None and record.src_id in src_ids:
                ids_to_delete.add(cast(int, record.row_id))
                continue
            if dst_ids is not None and record.dst_id in dst_ids:
                ids_to_delete.add(cast(int, record.row_id))
                continue
        if ids_to_delete:
            self.db.driver_wrapper.delete_by_id(self.table, ids_to_delete)

    def _delete_existing_for_src(self, src_id: int) -> None:
        self._delete_matching_records(src_ids={int(src_id)})

    def _delete_existing_for_dst(self, dst_id: int) -> None:
        self._delete_matching_records(dst_ids={int(dst_id)})

    def _find_existing_pair_row(self, src_id: int, dst_id: int) -> Optional[dict[str, Any]]:
        existing = self._records_for_pair(src_id, dst_id)
        if not existing:
            return None
        return deepcopy(existing[0].row_dict)

    def _ensure_link(
        self,
        src_id: int,
        dst_id: int,
        *,
        updates: Optional[Mapping[str, Any]] = None,
    ) -> None:
        src_id = int(src_id)
        dst_id = int(dst_id)
        updates = dict(updates or {})

        if self.table_type in {TableTypes.ONE_ONE, TableTypes.MANY_ONE}:
            self._delete_existing_for_src(src_id)
        if self.table_type in {TableTypes.ONE_ONE, TableTypes.ONE_MANY}:
            self._delete_existing_for_dst(dst_id)

        current = self._find_existing_pair_row(src_id, dst_id)
        if current is None:
            payload = {
                self._src_link_col: src_id,
                self._dst_link_col: dst_id,
            }
            payload.update(updates)
            self.db.driver_wrapper.add_row(payload)
            return

        current.update(updates)
        self.db.driver_wrapper.update_row(current)

    def _set_pair_column(
        self,
        src_id: int,
        dst_id: int,
        column: Optional[str],
        value: Any,
    ) -> None:
        if column is None or column not in self.column_headings:
            return
        self._ensure_link(src_id, dst_id, updates={column: value})

    def _apply_priority_order(self, src_id: int, dst_ids: Sequence[int]) -> None:
        if self.link_spec.priority_link_col is None:
            for dst_id in dst_ids:
                self._ensure_link(src_id, int(dst_id))
            return
        total = len(dst_ids)
        for index, dst_id in enumerate(dst_ids):
            self._ensure_link(
                src_id,
                int(dst_id),
                updates={self.link_spec.priority_link_col: total - index},
            )

    def update(self, update: Any) -> Any:
        update = self.update_preflight(update)
        self.update_precheck(update)
        self.update_db(update)
        self.update_cache(update)
        return update

    def update_preflight(self, update: Any) -> Any:
        return update

    def update_precheck(self, update: Any) -> bool:
        del update
        return True

    def update_db(self, update: Any) -> bool:
        link_row_ids = {
            int(value)
            for value in getattr(update, "delete_these_link_ids", set())
            if value is not None
        }
        if link_row_ids:
            self._delete_matching_records(row_ids=link_row_ids)

        src_ids = {int(value) for value in getattr(update, "src_ids_deleted", set())}
        src_ids |= {int(value) for value in getattr(update, "delete_these_src_ids", set())}
        src_ids |= {int(value) for value in getattr(update, "delete_links_with_this_src_id", set())}
        if src_ids:
            self._delete_matching_records(src_ids=src_ids)

        dst_ids = {int(value) for value in getattr(update, "dst_ids_deleted", set())}
        dst_ids |= {int(value) for value in getattr(update, "delete_these_dst_ids", set())}
        dst_ids |= {int(value) for value in getattr(update, "delete_links_with_this_dst_id", set())}
        if dst_ids:
            self._delete_matching_records(dst_ids=dst_ids)

        for src_id, dst_id in getattr(update, "create_these_links", {}).items():
            self._ensure_link(int(src_id), int(dst_id))

        for src_id, dst_ids_for_src in getattr(update, "src_dst_priority_update", {}).items():
            self._apply_priority_order(int(src_id), [int(dst_id) for dst_id in dst_ids_for_src])

        for dst_srcs, dst_id in getattr(update, "dst_src_priority_update", {}).items():
            for src_id in dst_srcs:
                self._ensure_link(int(src_id), int(dst_id))

        for src_id, typed_values in getattr(update, "src_dst_type_update", {}).items():
            for link_type, dst_ids_for_type in typed_values.items():
                for dst_id in dst_ids_for_type:
                    self._ensure_link(
                        int(src_id),
                        int(dst_id),
                        updates={self.link_spec.type_link_col: link_type} if self.link_spec.type_link_col else None,
                    )

        for type_and_srcs, dst_id in getattr(update, "dst_src_type_update", {}).items():
            link_type, src_ids_for_type = type_and_srcs
            for src_id in src_ids_for_type:
                self._ensure_link(
                    int(src_id),
                    int(dst_id),
                    updates={self.link_spec.type_link_col: link_type} if self.link_spec.type_link_col else None,
                )

        for src_id, typed_values in getattr(update, "src_dst_priority_type_update", {}).items():
            for link_type, ordered_dsts in typed_values.items():
                total = len(ordered_dsts)
                for index, dst_id in enumerate(ordered_dsts):
                    updates = {}
                    if self.link_spec.type_link_col:
                        updates[self.link_spec.type_link_col] = link_type
                    if self.link_spec.priority_link_col:
                        updates[self.link_spec.priority_link_col] = total - index
                    self._ensure_link(int(src_id), int(dst_id), updates=updates)

        for type_and_srcs, dst_id in getattr(update, "dst_src_priority_type_update", {}).items():
            link_type, src_ids_for_type = type_and_srcs
            total = len(src_ids_for_type)
            for index, src_id in enumerate(src_ids_for_type):
                updates = {}
                if self.link_spec.type_link_col:
                    updates[self.link_spec.type_link_col] = link_type
                if self.link_spec.priority_link_col:
                    updates[self.link_spec.priority_link_col] = total - index
                self._ensure_link(int(src_id), int(dst_id), updates=updates)

        for (src_id, dst_id), priority in getattr(update, "set_link_priority", {}).items():
            self._set_pair_column(int(src_id), int(dst_id), self.link_spec.priority_link_col, priority)

        for (src_id, dst_id), link_type in getattr(update, "set_link_type", {}).items():
            self._set_pair_column(int(src_id), int(dst_id), self.link_spec.type_link_col, link_type)

        optional_columns = {
            "set_link_origin": "origin",
            "set_link_policy": "policy",
            "set_link_data": "data",
            "set_link_index": "index",
        }
        for attr_name, suffix in optional_columns.items():
            column_name = None
            for candidate in self.column_headings:
                if candidate.endswith(f"_{suffix}"):
                    column_name = candidate
                    break
            for (src_id, dst_id), value in getattr(update, attr_name, {}).items():
                self._set_pair_column(int(src_id), int(dst_id), column_name, value)

        primary_column = None
        for candidate in self.column_headings:
            if candidate.endswith("_primary"):
                primary_column = candidate
                break
        for dst_id in getattr(update, "set_these_dst_as_primary", set()):
            if primary_column is None:
                break
            for record in self._records_for_dst(int(dst_id)):
                self._set_pair_column(record.src_id, record.dst_id, primary_column, 1)
        for src_id in getattr(update, "set_these_src_as_primary", set()):
            if primary_column is None:
                break
            for record in self._records_for_src(int(src_id)):
                self._set_pair_column(record.src_id, record.dst_id, primary_column, 1)

        return True

    def update_cache(self, update: Any) -> bool:
        del update
        self.read(self.db)
        return True


__all__ = ["SchemaBackedLinkTable"]

"""
Concrete schema-backed implementation of many-to-one storage fields.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional, Sequence, Union, cast

from LiuXin_alpha.caches.api.storage_cache_api.storage_fields_api.many_one_field import (
    IndividualLinkProperties,
    ManyOneInTwoTableFieldUpdate,
    ManyToOneFieldAPI,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.single_table import (
    StorageCacheSingleTableAPI,
)

from LiuXin_alpha.caches.cache_plugins.schema_backed.common import _ensure_db
from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_fields.relation_base import (
    _SchemaBackedRelationFieldBase,
)
from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_tables.link_tables.link_table import (
    SchemaBackedLinkTable,
)

if TYPE_CHECKING:
    from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_cache import SchemaBackedStorageCache


class SchemaBackedManyOneField(
    _SchemaBackedRelationFieldBase[Any],
    ManyToOneFieldAPI[Any],
):
    """
    Field wrapper over a many-to-one relation.
    """

    def __init__(
        self,
        cache: "SchemaBackedStorageCache",
        src_table: Union[StorageCacheSingleTableAPI, str],
        src_table_id_col: str,
        dst_table: Union[StorageCacheSingleTableAPI, str],
        dst_table_cache_col: str,
        db: Any,
    ) -> None:
        self._init_relation_field(cache, db)
        ManyToOneFieldAPI.__init__(
            self,
            src_table=src_table,
            src_table_id_col=src_table_id_col,
            dst_table=dst_table,
            dst_table_cache_col=dst_table_cache_col,
            db=db,
        )

    def get_link_table(
        self,
        src_table: Union[StorageCacheSingleTableAPI, str],
        dst_table: Union[StorageCacheSingleTableAPI, str],
    ) -> SchemaBackedLinkTable:
        return cast(SchemaBackedLinkTable, self._cache.get_many_one_link_table(src_table, dst_table))

    def update(self, update: ManyOneInTwoTableFieldUpdate[Any]) -> None:
        self._db = _ensure_db(self._db)
        create_missing_links = bool(update.create_missing_links)
        create_missing_related_rows = bool(update.create_missing_related_rows)
        self._validate_create_policy(
            create_missing_links=create_missing_links,
            create_missing_related_rows=create_missing_related_rows,
        )

        raw_updates = {
            int(src_id): value
            for src_id, value in {
                **dict(update.added_maps),
                **dict(update.updated_maps),
            }.items()
        }

        deleted_src_ids = {
            int(src_id)
            for src_id in update.deleted_ids
        } | {
            int(src_id)
            for src_id, value in raw_updates.items()
            if value is None
        }

        missing_link_src_ids: list[int] = []
        dst_updates: dict[int, Any] = {}
        links_to_create: dict[int, int] = {}

        for src_id, value in raw_updates.items():
            if value is None:
                continue

            existing_dst_id = cast(Any, self.link_table).get_dst_id(int(src_id))
            if existing_dst_id is not None:
                dst_updates[int(existing_dst_id)] = value
                continue

            if not create_missing_links:
                missing_link_src_ids.append(int(src_id))
                continue

            dst_id = self._get_unique_dst_id_for_value(value)
            if dst_id is None:
                if not create_missing_related_rows:
                    missing_link_src_ids.append(int(src_id))
                    continue
                dst_id = self._create_related_dst_row(value)

            links_to_create[int(src_id)] = int(dst_id)

        if missing_link_src_ids:
            raise KeyError(
                f"Field {self.field_key!r} cannot update missing linked rows for src ids: {sorted(missing_link_src_ids)}"
            )

        if deleted_src_ids:
            self._unlink_src_ids(deleted_src_ids)

        for src_id, dst_id in links_to_create.items():
            self._create_link(src_id, dst_id)

        if dst_updates:
            self._update_dst_values(dst_updates)

        if deleted_src_ids or links_to_create or dst_updates or update.dirtied:
            self.read(self._db)

    @property
    def ids(self) -> set[int]:
        return set(self._src_to_values.keys())

    @property
    def values(self) -> list[Any]:
        return self._flattened_values()

    @property
    def values_set(self) -> set[Any]:
        return set(self.values)

    @property
    def ids_values_map(self) -> dict[int, Optional[Any]]:
        return {
            src_id: values[0] if values else None
            for src_id, values in self._src_to_values.items()
        }

    @property
    def dst_ids_values_map(self) -> dict[int, Optional[Any]]:
        return dict(self._dst_to_values)

    def get_value_from_src_id(self, src_id: int) -> Optional[Any]:
        return self._single_value_for_src_id(int(src_id))

    def get_value_from_dst_id(self, dst_id: int) -> Optional[Any]:
        return self._value_for_dst_id(int(dst_id))

    def get_dst_id_from_src_id(
        self,
        src_id: int,
        type_filter: Optional[str] = None,
    ) -> Optional[int]:
        dst_ids = self._ordered_dst_ids_for_src(int(src_id), type_filter=type_filter)
        return dst_ids[0] if dst_ids else None

    def get_src_ids_from_dst_id(
        self,
        dst_id: int,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[int]:
        return self._ordered_src_ids_for_dst(
            int(dst_id),
            require_ordering=require_ordering,
            type_filter=type_filter,
        )

    def get_src_ids_from_value(self, value: Any) -> list[int]:
        src_ids: list[int] = []
        for dst_id, dst_value in sorted(self._dst_to_values.items()):
            if dst_value == value:
                src_ids.extend(self._dst_to_src_ids.get(dst_id, ()))
        return sorted(src_ids)

    def get_dst_ids_from_value(self, value: Any) -> list[int]:
        return sorted(
            dst_id
            for dst_id, dst_value in self._dst_to_values.items()
            if dst_value == value
        )

    def get_link_properties(
        self,
        src_id: int,
        dst_id: int,
    ) -> IndividualLinkProperties:
        return cast(
            IndividualLinkProperties,
            self._build_link_properties(IndividualLinkProperties, int(src_id), int(dst_id)),
        )

    def set_link_properties(
        self,
        updated_link_properties: IndividualLinkProperties,
    ) -> None:
        self._set_link_properties(updated_link_properties)


__all__ = ["SchemaBackedManyOneField"]

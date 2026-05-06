"""
Concrete schema-backed implementation of one-to-many storage fields.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional, Sequence, Union, cast

from LiuXin_alpha.caches.api.storage_cache_api.storage_fields_api.one_many_field import (
    IndividualLinkProperties,
    OneManyInTwoTableFieldUpdate,
    OneToManyFieldAPI,
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


class SchemaBackedOneManyField(
    _SchemaBackedRelationFieldBase[Any],
    OneToManyFieldAPI[Any],
):
    """
    Field wrapper over a one-to-many relation.
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
        OneToManyFieldAPI.__init__(
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
        return cast(SchemaBackedLinkTable, self._cache.get_one_many_link_table(src_table, dst_table))

    def update(self, update: OneManyInTwoTableFieldUpdate[Any]) -> None:
        self._db = _ensure_db(self._db)

        value_updates = {
            int(src_id): tuple(values)
            for src_id, values in {
                **dict(update.added_maps),
                **dict(update.updated_maps),
            }.items()
        }
        explicit_replacements = {
            int(src_id): tuple(replacements)
            for src_id, replacements in dict(update.link_replacements).items()
        }
        overlap = set(value_updates) & set(explicit_replacements)
        if overlap:
            raise ValueError(
                f"Field {self.field_key!r} cannot mix value updates and explicit link replacements "
                f"for the same src ids: {sorted(overlap)}"
            )
        overlap = {int(src_id) for src_id in update.deleted_ids} & set(explicit_replacements)
        if overlap:
            raise ValueError(
                f"Field {self.field_key!r} cannot delete and replace links for the same src ids: {sorted(overlap)}"
            )

        self._ensure_existing_sequence_targets(value_updates)

        deleted_src_ids = {int(src_id) for src_id in update.deleted_ids}
        if deleted_src_ids:
            self._unlink_src_ids(deleted_src_ids)

        dst_updates: dict[int, Any] = {}
        for src_id, values in value_updates.items():
            for dst_id, value in zip(self._existing_ordered_dst_ids_for_src(src_id), values):
                dst_updates[int(dst_id)] = value
        if dst_updates:
            self._update_dst_values(dst_updates)

        for src_id, replacements in explicit_replacements.items():
            self._replace_links_for_src(
                int(src_id),
                replacements,
                allow_shared_dst=False,
            )

        if deleted_src_ids or dst_updates or explicit_replacements or update.dirtied:
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
    def ids_values_map(self) -> dict[int, Sequence[Optional[Any]]]:
        return {
            src_id: tuple(values)
            for src_id, values in self._src_to_values.items()
        }

    @property
    def dst_ids_values_map(self) -> dict[int, Optional[Any]]:
        return dict(self._dst_to_values)

    def get_values_from_src_id(
        self,
        src_id: int,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[Optional[Any]]:
        if require_ordering or type_filter is not None:
            return self._values_for_src_id(
                int(src_id),
                require_ordering=require_ordering,
                type_filter=type_filter,
            )
        return tuple(self._src_to_values.get(int(src_id), ()))

    def get_value_from_dst_id(self, dst_id: int) -> Optional[Any]:
        return self._value_for_dst_id(int(dst_id))

    def get_dst_ids_from_src_id(
        self,
        src_id: int,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> Sequence[int]:
        return self._ordered_dst_ids_for_src(
            int(src_id),
            require_ordering=require_ordering,
            type_filter=type_filter,
        )

    def get_src_id_from_dst_id(
        self,
        dst_id: int,
        type_filter: Optional[str] = None,
    ) -> Optional[int]:
        src_ids = self._ordered_src_ids_for_dst(int(dst_id), type_filter=type_filter)
        return src_ids[0] if src_ids else None

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


__all__ = ["SchemaBackedOneManyField"]

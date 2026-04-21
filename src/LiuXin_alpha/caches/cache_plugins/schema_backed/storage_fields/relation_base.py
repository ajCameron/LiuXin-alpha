"""
Shared helpers for schema-backed relation fields.
"""

from __future__ import annotations

import dataclasses

from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, Generic, Iterable, Optional, Sequence, TypeVar, Union, cast

from LiuXin_alpha.caches.api.storage_cache_api.storage_tables.single_table import (
    StorageCacheSingleTableAPI,
)

from LiuXin_alpha.caches.cache_plugins.schema_backed.common import (
    _canonical_field_key,
    _ensure_db,
)
from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_tables.link_tables.link_table import (
    SchemaBackedLinkTable,
)

if TYPE_CHECKING:
    from LiuXin_alpha.caches.cache_plugins.schema_backed.storage_cache import SchemaBackedStorageCache

T = TypeVar("T")


class _SchemaBackedRelationFieldBase(Generic[T]):
    """
    Common runtime helpers for relation-backed field objects.

    These helpers intentionally stay conservative: field updates may mutate
    linked values or sever links, but they do not create or delete related rows.
    """

    def _init_relation_field(
        self,
        cache: "SchemaBackedStorageCache",
        db: Any,
    ) -> None:
        self._cache = cache
        self._db = db
        self._src_to_dst_ids: dict[int, tuple[int, ...]] = {}
        self._dst_to_src_ids: dict[int, tuple[int, ...]] = {}
        self._src_to_values: dict[int, tuple[Optional[T], ...]] = {}
        self._dst_to_values: dict[int, Optional[T]] = {}

    @property
    def field_key(self) -> str:
        return _canonical_field_key(
            self.src_table_name,
            f"{self.dst_table_name}.{self.dst_table_cache_col}",
        )

    @property
    def table_name(self) -> str:
        return self.src_table_name

    @property
    def column_name(self) -> str:
        return self.dst_table_cache_col

    def get_main_table(
        self,
        name: Union[str, StorageCacheSingleTableAPI],
    ) -> StorageCacheSingleTableAPI:
        return self._cache.get_main_table(name)

    def _link_table_cache(self) -> SchemaBackedLinkTable:
        return cast(SchemaBackedLinkTable, self.link_table)

    def _value_for_dst_id(self, dst_id: int) -> Optional[T]:
        dst_id = int(dst_id)
        if self.dst_table.has_id(dst_id):
            return cast(Optional[T], self.dst_table.get_row_snapshot(dst_id).get(self.dst_table_cache_col))
        return None

    def _ordered_dst_ids_for_src(
        self,
        src_id: int,
        *,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> tuple[int, ...]:
        link_table = cast(Any, self.link_table)
        return tuple(
            int(dst_id)
            for dst_id in link_table.get_dst_ids(
                int(src_id),
                require_ordering=require_ordering,
                type_filter=type_filter,
            )
        )

    def _ordered_src_ids_for_dst(
        self,
        dst_id: int,
        *,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> tuple[int, ...]:
        link_table = cast(Any, self.link_table)
        return tuple(
            int(src_id)
            for src_id in link_table.get_src_ids(
                int(dst_id),
                require_ordering=require_ordering,
                type_filter=type_filter,
            )
        )

    def _values_for_src_id(
        self,
        src_id: int,
        *,
        require_ordering: bool = False,
        type_filter: Optional[str] = None,
    ) -> tuple[Optional[T], ...]:
        return tuple(
            self._value_for_dst_id(dst_id)
            for dst_id in self._ordered_dst_ids_for_src(
                int(src_id),
                require_ordering=require_ordering,
                type_filter=type_filter,
            )
        )

    def _single_value_for_src_id(
        self,
        src_id: int,
        *,
        type_filter: Optional[str] = None,
    ) -> Optional[T]:
        values = self._values_for_src_id(int(src_id), type_filter=type_filter)
        return values[0] if values else None

    def _read_relation_cache(self) -> None:
        src_to_dst_ids: dict[int, tuple[int, ...]] = {}
        dst_to_src_ids: dict[int, list[int]] = {}
        src_to_values: dict[int, tuple[Optional[T], ...]] = {}
        dst_to_values: dict[int, Optional[T]] = {}

        for src_id in sorted(int(row_id) for row_id in self.src_table.row_ids):
            dst_ids = self._ordered_dst_ids_for_src(src_id, require_ordering=True)
            if not dst_ids:
                continue

            values = tuple(self._value_for_dst_id(dst_id) for dst_id in dst_ids)
            src_to_dst_ids[src_id] = dst_ids
            src_to_values[src_id] = values

            for dst_id, value in zip(dst_ids, values):
                dst_to_src_ids.setdefault(dst_id, []).append(src_id)
                dst_to_values[dst_id] = value

        self._src_to_dst_ids = src_to_dst_ids
        self._src_to_values = src_to_values
        self._dst_to_src_ids = {
            dst_id: tuple(src_ids)
            for dst_id, src_ids in dst_to_src_ids.items()
        }
        self._dst_to_values = dst_to_values

    def read(self, db: Any) -> None:
        db = _ensure_db(self._db, db)
        self._db = db
        self.src_table.db = db
        self.dst_table.db = db
        self.link_table.db = db
        self._read_relation_cache()

    def refresh_ids(
        self,
        ids: Iterable[int],
        db: Any = None,
    ) -> None:
        del ids
        self.read(db)

    def remove_ids(self, ids: Iterable[int]) -> None:
        removed_ids = {int(row_id) for row_id in ids}
        if not removed_ids:
            return

        self._src_to_dst_ids = {
            src_id: dst_ids
            for src_id, dst_ids in self._src_to_dst_ids.items()
            if src_id not in removed_ids
        }
        self._src_to_values = {
            src_id: values
            for src_id, values in self._src_to_values.items()
            if src_id not in removed_ids
        }

        dst_to_src_ids: dict[int, list[int]] = {}
        dst_to_values: dict[int, Optional[T]] = {}
        for src_id, dst_ids in self._src_to_dst_ids.items():
            values = self._src_to_values.get(src_id, ())
            for dst_id, value in zip(dst_ids, values):
                dst_to_src_ids.setdefault(dst_id, []).append(src_id)
                dst_to_values[dst_id] = value

        self._dst_to_src_ids = {
            dst_id: tuple(src_ids)
            for dst_id, src_ids in dst_to_src_ids.items()
        }
        self._dst_to_values = dst_to_values

    def _flattened_values(self) -> list[Optional[T]]:
        values: list[Optional[T]] = []
        for src_id in sorted(self._src_to_values):
            values.extend(self._src_to_values[src_id])
        return values

    def _unlink_src_ids(self, src_ids: Iterable[int]) -> None:
        deleted_ids = {int(src_id) for src_id in src_ids}
        if not deleted_ids:
            return
        self._db = _ensure_db(self._db)
        cast(Any, self.link_table).update(SimpleNamespace(src_ids_deleted=deleted_ids))

    def _validate_create_policy(
        self,
        *,
        create_missing_links: bool,
        create_missing_related_rows: bool,
    ) -> None:
        if create_missing_related_rows and not create_missing_links:
            raise ValueError(
                f"Field {self.field_key!r} cannot create related rows without also creating links"
            )

    def _update_dst_values(self, dst_values_map: dict[int, Optional[T]]) -> None:
        if not dst_values_map:
            return
        self._db = _ensure_db(self._db)
        self.dst_table.update(
            {int(dst_id): value for dst_id, value in dst_values_map.items()},
            self._db,
            target_column=self.dst_table_cache_col,
        )

    def _existing_ordered_dst_ids_for_src(self, src_id: int) -> tuple[int, ...]:
        return self._ordered_dst_ids_for_src(
            int(src_id),
            require_ordering=bool(self._link_table_cache().link_spec.ordered),
        )

    def _get_unique_dst_id_for_value(self, value: T) -> Optional[int]:
        matches = sorted(int(dst_id) for dst_id in self.dst_table.get_ids_for_value(self.dst_table_cache_col, value))
        if not matches:
            return None
        if len(matches) > 1:
            raise ValueError(
                f"Field {self.field_key!r} found multiple dst rows for value {value!r}: {matches}"
            )
        return matches[0]

    def _create_related_dst_row(self, value: T) -> int:
        self._db = _ensure_db(self._db)
        driver_wrapper = self._db.driver_wrapper
        if callable(getattr(driver_wrapper, "get_blank_row", None)):
            blank_row = driver_wrapper.get_blank_row(self.dst_table_name)
            payload = dict(getattr(blank_row, "row_dict", blank_row))
            payload[self.dst_table_cache_col] = value
            driver_wrapper.update_row(payload)
            new_id = int(payload[self.dst_table.id_column])
            cast(Any, self.dst_table)._refresh_ids({new_id})
            return new_id

        new_id = driver_wrapper.add_row({self.dst_table_cache_col: value})
        if new_id is None:
            raise RuntimeError(
                f"Field {self.field_key!r} failed to create a related row for value {value!r}"
            )
        new_id = int(new_id)
        cast(Any, self.dst_table)._refresh_ids({new_id})
        return new_id

    def _create_link(self, src_id: int, dst_id: int) -> None:
        self._db = _ensure_db(self._db)
        cast(Any, self.link_table).update(
            SimpleNamespace(create_these_links={int(src_id): int(dst_id)})
        )

    def _validate_link_dst_update(self, link_update: Any) -> None:
        if str(getattr(link_update, "dst_table", self.dst_table_name)) != self.dst_table_name:
            raise ValueError(
                f"Field {self.field_key!r} received a link update for dst table "
                f"{getattr(link_update, 'dst_table', None)!r}, expected {self.dst_table_name!r}"
            )
        if (
            str(getattr(link_update, "dst_table_target_column", self.dst_table_cache_col))
            != self.dst_table_cache_col
        ):
            raise ValueError(
                f"Field {self.field_key!r} received a link update for dst column "
                f"{getattr(link_update, 'dst_table_target_column', None)!r}, "
                f"expected {self.dst_table_cache_col!r}"
            )

    def _resolve_explicit_dst_target(
        self,
        src_id: int,
        link_update: Any,
        *,
        allow_shared_dst: bool,
    ) -> int:
        self._validate_link_dst_update(link_update)

        explicit_dst_id = getattr(link_update, "dst_table_id", None)
        if explicit_dst_id is not None:
            dst_id = int(explicit_dst_id)
            if not self.dst_table.has_id(dst_id):
                raise KeyError(
                    f"Field {self.field_key!r} cannot target missing dst id {dst_id}"
                )
            if not allow_shared_dst:
                existing_src_id = cast(Any, self.link_table).get_src_id(dst_id)
                if existing_src_id is not None and int(existing_src_id) != int(src_id):
                    raise ValueError(
                        f"Field {self.field_key!r} cannot retarget dst id {dst_id} "
                        f"because it is already linked to src id {int(existing_src_id)}"
                    )
            return dst_id

        desired_value = getattr(link_update, "dst_col_val", None)
        if desired_value is not None:
            matched_dst_id = self._get_unique_dst_id_for_value(desired_value)
            if matched_dst_id is not None:
                if allow_shared_dst:
                    return matched_dst_id
                existing_src_id = cast(Any, self.link_table).get_src_id(matched_dst_id)
                if existing_src_id is None or int(existing_src_id) == int(src_id):
                    return matched_dst_id

        return self._create_related_dst_row(desired_value)

    def _link_property_updates(self, link_update: Any) -> dict[str, Any]:
        updates: dict[str, Any] = {}
        for property_name in ("priority", "type", "primary", "origin", "policy", "data", "index"):
            column_name = self._column_for_extra(property_name)
            if column_name is None or not hasattr(link_update, property_name):
                continue
            value = getattr(link_update, property_name)
            if value is None:
                continue
            updates[column_name] = value
        return updates

    def _replace_links_for_src(
        self,
        src_id: int,
        replacements: Sequence[Any],
        *,
        allow_shared_dst: bool,
    ) -> None:
        resolved: list[tuple[int, Any]] = []
        seen_dst_ids: set[int] = set()
        dst_updates: dict[int, Optional[T]] = {}

        for link_update in replacements:
            dst_id = self._resolve_explicit_dst_target(
                int(src_id),
                link_update,
                allow_shared_dst=allow_shared_dst,
            )
            if dst_id in seen_dst_ids:
                raise ValueError(
                    f"Field {self.field_key!r} cannot replace src id {int(src_id)} "
                    f"with duplicate dst id {dst_id}"
                )
            seen_dst_ids.add(dst_id)

            desired_value = cast(Optional[T], getattr(link_update, "dst_col_val", None))
            if dst_id in dst_updates and dst_updates[dst_id] != desired_value:
                raise ValueError(
                    f"Field {self.field_key!r} received conflicting values for dst id {dst_id}"
                )
            dst_updates[dst_id] = desired_value
            resolved.append((dst_id, link_update))

        self._unlink_src_ids({int(src_id)})

        if resolved:
            cast(Any, self.link_table).update(
                SimpleNamespace(
                    src_dst_priority_update={
                        int(src_id): [dst_id for dst_id, _link_update in resolved]
                    }
                )
            )

        if dst_updates:
            self._update_dst_values(dst_updates)

        for dst_id, link_update in resolved:
            property_updates = self._link_property_updates(link_update)
            if property_updates:
                self._update_link_row_columns(int(src_id), int(dst_id), property_updates)

    def _ensure_existing_singular_targets(
        self,
        src_ids: Iterable[int],
    ) -> dict[int, int]:
        mapping: dict[int, int] = {}
        missing: list[int] = []
        for src_id in sorted({int(value) for value in src_ids}):
            dst_id = cast(Any, self.link_table).get_dst_id(src_id)
            if dst_id is None:
                missing.append(src_id)
                continue
            mapping[src_id] = int(dst_id)
        if missing:
            raise KeyError(
                f"Field {self.field_key!r} cannot update missing linked rows for src ids: {missing}"
            )
        return mapping

    def _ensure_existing_sequence_targets(
        self,
        updates: dict[int, Sequence[Optional[T]]],
    ) -> dict[int, tuple[int, ...]]:
        mapping: dict[int, tuple[int, ...]] = {}
        missing: list[int] = []
        length_mismatches: list[tuple[int, int, int]] = []

        for src_id, values in updates.items():
            dst_ids = self._existing_ordered_dst_ids_for_src(src_id)
            if not dst_ids and values:
                missing.append(int(src_id))
                continue
            if len(dst_ids) != len(values):
                length_mismatches.append((int(src_id), len(dst_ids), len(values)))
                continue
            mapping[int(src_id)] = dst_ids

        if missing:
            raise KeyError(
                f"Field {self.field_key!r} cannot update missing linked rows for src ids: {sorted(missing)}"
            )
        if length_mismatches:
            mismatch_text = ", ".join(
                f"{src_id} (linked={linked_count}, values={value_count})"
                for src_id, linked_count, value_count in length_mismatches
            )
            raise ValueError(
                f"Field {self.field_key!r} requires one value per existing linked row: {mismatch_text}"
            )
        return mapping

    def _link_row_snapshot(self, src_id: int, dst_id: int) -> dict[str, Any]:
        row = cast(Any, self.link_table).get_link_row(int(src_id), int(dst_id))
        if row is None:
            raise KeyError((int(src_id), int(dst_id)))
        return dict(row.row_dict)

    def _column_for_extra(self, extra_name: str) -> Optional[str]:
        link_table = self._link_table_cache()
        if extra_name == "priority":
            return link_table.link_spec.priority_link_col
        if extra_name == "type":
            return link_table.link_spec.type_link_col

        suffixes = {
            "primary": "_primary",
            "origin": "_origin",
            "policy": "_policy",
            "data": "_data",
            "index": "_index",
        }
        suffix = suffixes.get(str(extra_name))
        if suffix is None:
            return None
        for candidate in link_table.column_headings:
            if candidate.endswith(suffix):
                return candidate
        return None

    def _update_link_row_columns(
        self,
        src_id: int,
        dst_id: int,
        updates: dict[str, Any],
    ) -> None:
        if not updates:
            return
        self._db = _ensure_db(self._db)
        row_dict = self._link_row_snapshot(src_id, dst_id)
        row_dict.update(updates)
        self._db.driver_wrapper.update_row(row_dict)
        self.link_table.read(self._db)

    def _value_from_link_property(
        self,
        row_dict: dict[str, Any],
        property_name: str,
    ) -> Any:
        column_name = self._column_for_extra(property_name)
        if column_name is None:
            return None
        return row_dict.get(column_name)

    def _build_link_properties(
        self,
        props_cls: type[Any],
        src_id: int,
        dst_id: int,
    ) -> Any:
        row_dict = self._link_row_snapshot(src_id, dst_id)
        kwargs: dict[str, Any] = {}
        for dc_field in dataclasses.fields(props_cls):
            if dc_field.name == "src_table":
                kwargs[dc_field.name] = self.src_table_name
            elif dc_field.name == "src_table_id":
                kwargs[dc_field.name] = int(src_id)
            elif dc_field.name == "dst_table":
                kwargs[dc_field.name] = self.dst_table_name
            elif dc_field.name == "dst_table_id":
                kwargs[dc_field.name] = int(dst_id)
            else:
                kwargs[dc_field.name] = self._value_from_link_property(row_dict, dc_field.name)
        return props_cls(**kwargs)

    def _set_link_properties(self, updated_link_properties: Any) -> None:
        updates: dict[str, Any] = {}
        for property_name in ("priority", "type", "primary", "origin", "policy", "data", "index"):
            column_name = self._column_for_extra(property_name)
            if column_name is None or not hasattr(updated_link_properties, property_name):
                continue
            value = getattr(updated_link_properties, property_name)
            if value is None:
                continue
            updates[column_name] = value

        self._update_link_row_columns(
            int(updated_link_properties.src_table_id),
            int(updated_link_properties.dst_table_id),
            updates,
        )
        self.read(self._db)

    def get_extra(
        self,
        src_id: int,
        dst_id: int,
        extra_type: Any,
    ) -> Optional[str | bool | int]:
        row_dict = self._link_row_snapshot(int(src_id), int(dst_id))
        column_name = self._column_for_extra(str(extra_type))
        if column_name is None:
            return None
        return cast(Optional[str | bool | int], row_dict.get(column_name))

    def set_extra(
        self,
        src_id: int,
        dst_id: int,
        extra_type: Any,
        new_extra_value: Optional[str | bool | int],
    ) -> None:
        column_name = self._column_for_extra(str(extra_type))
        if column_name is None:
            raise KeyError(str(extra_type))
        self._update_link_row_columns(int(src_id), int(dst_id), {column_name: new_extra_value})
        self.read(self._db)

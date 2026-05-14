"""Core WEMI item metadata-bundle implementation containers.

Category: core WEMI metadata bundle.
This module implements the editable metadata surface around an item, not the item
identity object and not a read-side query result.
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any, Optional

from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.item_containers.item_identity_api import ItemIdentityAPI
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.item_containers.item_metadata_api import (
    ItemMetadataAPI,
    ItemRelationKey,
    ItemRelationLink,
)
from LiuXin_alpha.metadata.containers.metadata_containers._string_formatting import (
    metadata_bundle_string,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import (
    ItemIdentity,
)


class ItemMetadata(ItemMetadataAPI):
    """
    Concrete implementation of :class:`ItemMetadataAPI`.

    Targets in relation links are usually live database :class:`Row` objects,
    but plain mappings are also supported for round-tripping/tests.
    """

    def __init__(
        self,
        *,
        item: Optional[ItemIdentityAPI] = None,
        relation_links: Optional[Mapping[str, Iterable[ItemRelationLink]]] = None,
    ) -> None:
        self._item = item
        self._relation_links: dict[ItemRelationKey, list[ItemRelationLink]] = {
            relation_key: [] for relation_key in self.RELATION_KEYS
        }
        if relation_links:
            for relation_key, links in relation_links.items():
                self.set_relation_links(relation_key, links)

    @property
    def item(self) -> Optional[ItemIdentityAPI]:
        return self._item

    @item.setter
    def item(self, value: Optional[ItemIdentityAPI]) -> None:
        self._item = value

    def get_relation_links(self, relation_key: ItemRelationKey) -> list[ItemRelationLink]:
        relation_key = self.validate_relation_name(relation_key)
        return self._relation_links[relation_key]

    def set_relation_links(self, relation_key: ItemRelationKey, links: Iterable[ItemRelationLink]) -> None:
        relation_key = self.validate_relation_name(relation_key)
        self._relation_links[relation_key] = self.validate_relation_links(relation_key, links)

    def __str__(self) -> str:
        return metadata_bundle_string(
            self,
            identity_name="item",
            relation_names=self.RELATION_KEYS,
            get_links=self.get_relation_links,
        )

    def write_to_database(
        self,
        database: Any,
        *,
        fields: Iterable[str] | None = None,
        item_id: int | None = None,
        target_row: Row | Mapping[str, Any] | None = None,
        replace: bool = False,
        mark_dirty: bool = True,
    ) -> Any:
        from LiuXin_alpha.metadata.containers.metadata_containers.liuxin_wemi_metadata_writer import (
            LiuXinWEMIMetadataWriter,
        )

        return LiuXinWEMIMetadataWriter(database).write(
            self,
            fields=fields,
            target_level="item",
            item_id=item_id,
            target_row=target_row,
            replace=replace,
            mark_dirty=mark_dirty,
        )

    @staticmethod
    def _serialize_target(target: Any) -> Any:
        if target is None:
            return None
        if isinstance(target, Row):
            return dict(target.row_dict)
        to_mapping = getattr(target, "to_mapping", None)
        if callable(to_mapping):
            return to_mapping()
        if isinstance(target, Mapping):
            return dict(target)
        return target

    @staticmethod
    def _deserialize_target(target: Any) -> Any:
        if isinstance(target, Mapping):
            if "item_id" in target or "item_manifestation_id" in target:
                return ItemIdentity.from_mapping(target)
            return dict(target)
        return target

    def to_mapping(self, include_related: bool = True) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "item": self.item.to_mapping() if self.item is not None else None,
        }
        if include_related:
            payload["relations"] = {
                relation_key: [
                    {
                        "target": self._serialize_target(link.target),
                        "priority": link.priority,
                        "primary": link.primary,
                        "type": link.type,
                        "origin": link.origin,
                        "source": link.source,
                        "policy": link.policy,
                        "data": link.data,
                        "index": link.index,
                        "edge_id": link.edge_id,
                        "cardinality": (
                            link.cardinality.value
                            if link.cardinality is not None
                            else None
                        ),
                        "extra": dict(link.extra),
                    }
                    for link in self.get_relation_links(relation_key)
                ]
                for relation_key in self.RELATION_KEYS
            }
        return payload

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "ItemMetadata":
        item_payload = payload.get("item")
        item: Optional[ItemIdentityAPI]
        if isinstance(item_payload, ItemIdentityAPI):
            item = item_payload
        elif isinstance(item_payload, Mapping):
            item = ItemIdentity.from_mapping(item_payload)
        else:
            item = None

        relation_payload = payload.get("relations") or {}
        relation_links: dict[str, list[ItemRelationLink]] = {}
        for relation_key in cls.RELATION_KEYS:
            relation_links[relation_key] = []
            for raw_link in relation_payload.get(relation_key, []):
                if isinstance(raw_link, ItemRelationLink):
                    relation_links[relation_key].append(raw_link)
                    continue
                if not isinstance(raw_link, Mapping):
                    continue
                relation_links[relation_key].append(
                    ItemRelationLink(
                        target=cls._deserialize_target(raw_link.get("target")),
                        priority=raw_link.get("priority"),
                        primary=raw_link.get("primary"),
                        type=raw_link.get("type"),
                        origin=raw_link.get("origin"),
                        source=raw_link.get("source"),
                        policy=raw_link.get("policy"),
                        data=raw_link.get("data"),
                        index=raw_link.get("index"),
                        edge_id=raw_link.get("edge_id"),
                        cardinality=raw_link.get("cardinality"),
                        extra=dict(raw_link.get("extra") or {}),
                    )
                )
        return cls(item=item, relation_links=relation_links)

    @classmethod
    def from_database(
        cls,
        database: Any,
        *,
        item_id: Optional[int] = None,
        source_row: Optional[Mapping[str, Any] | Row] = None,
    ) -> "ItemMetadata":
        from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_hydrator import (
            ItemMetadataHydrator,
        )

        hydrator = ItemMetadataHydrator(database)
        if item_id is not None:
            return hydrator.from_item_id(int(item_id))
        if source_row is not None:
            return hydrator.from_source_row(source_row)
        raise ValueError("Provide either item_id or source_row.")

__all__ = ["ItemMetadata"]

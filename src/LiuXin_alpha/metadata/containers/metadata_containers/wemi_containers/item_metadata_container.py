"""Core WEMI item metadata-bundle implementation containers.

Category: core WEMI metadata bundle.
This module implements the editable metadata surface around an item, not the item
identity object and not a read-side query result.
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any, Optional

from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.item_containers.item_identity_api import ItemIdentityAPI
from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.item_containers.item_metadata_api import (
    ItemMetadataAPI,
    ItemRelationLink,
    ItemStorageHints,
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
        self._relation_links: dict[str, list[ItemRelationLink]] = {
            relation: [] for relation in self.RELATION_KEYS
        }
        if relation_links:
            for relation, links in relation_links.items():
                self.set_relation_links(relation, links)

    @property
    def item(self) -> Optional[ItemIdentityAPI]:
        return self._item

    @item.setter
    def item(self, value: Optional[ItemIdentityAPI]) -> None:
        self._item = value

    def get_relation_links(self, relation: str) -> list[ItemRelationLink]:
        relation_key = self.validate_relation_name(relation)
        return self._relation_links[relation_key]

    def set_relation_links(self, relation: str, links: Iterable[ItemRelationLink]) -> None:
        relation_key = self.validate_relation_name(relation)
        self._relation_links[relation_key] = list(links)

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
                relation: [
                    {
                        "target": self._serialize_target(link.target),
                        "priority": link.priority,
                        "primary": link.primary,
                        "type": link.type,
                        "origin": link.origin,
                        "policy": link.policy,
                        "data": link.data,
                        "index": link.index,
                        "extra": dict(link.extra),
                    }
                    for link in self.get_relation_links(relation)
                ]
                for relation in self.RELATION_KEYS
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
        for relation in cls.RELATION_KEYS:
            relation_links[relation] = []
            for raw_link in relation_payload.get(relation, []):
                if isinstance(raw_link, ItemRelationLink):
                    relation_links[relation].append(raw_link)
                    continue
                if not isinstance(raw_link, Mapping):
                    continue
                relation_links[relation].append(
                    ItemRelationLink(
                        target=cls._deserialize_target(raw_link.get("target")),
                        priority=raw_link.get("priority"),
                        primary=raw_link.get("primary"),
                        type=raw_link.get("type"),
                        origin=raw_link.get("origin"),
                        policy=raw_link.get("policy"),
                        data=raw_link.get("data"),
                        index=raw_link.get("index"),
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

    @staticmethod
    def _first_target(links: list[ItemRelationLink]) -> Any:
        if not links:
            return None
        for link in links:
            if link.primary:
                return link.target
        return links[0].target

    @staticmethod
    def _value_from_mapping(mapping: Mapping[str, Any], keys: tuple[str, ...]) -> Any:
        for key in keys:
            value = mapping.get(key)
            if value not in (None, ""):
                return value
        return None

    @classmethod
    def _rowish_to_mapping(cls, value: Any) -> Mapping[str, Any]:
        if value is None:
            return {}
        if isinstance(value, Row):
            return value.row_dict
        if hasattr(value, "to_mapping") and callable(value.to_mapping):
            return value.to_mapping()
        if isinstance(value, Mapping):
            return value
        return {}

    @classmethod
    def _display_value(cls, value: Any) -> Optional[str]:
        mapping = cls._rowish_to_mapping(value)
        if mapping:
            candidates = (
                "agent_canonical_name",
                "agent_sort_name",
                "work_canonical_title",
                "work_title",
                "expression_title_override",
                "expression_label",
                "manifestation_edition_statement",
                "manifestation_format_detail",
                "series",
                "series_name",
                "genre",
                "subject",
                "tag",
                "label",
                "language",
                "language_name",
                "language_code",
                "folder_name",
                "folder_relpath",
                "store_name",
                "store_root_uri",
                "file_name",
                "image_name",
                "digital_asset_name",
                "digital_asset_base_name",
                "composite_digital_asset_name",
                "identifier_value",
                "annotation_selected_text",
                "annotation_note_text",
                "note",
                "comment",
            )
            display = cls._value_from_mapping(mapping, candidates)
            if display not in (None, ""):
                return str(display)
            for key, item in mapping.items():
                key_text = str(key)
                if key_text.endswith("_id") or key_text.endswith("_timestamp_ep_k"):
                    continue
                if item not in (None, ""):
                    return str(item)
        if value in (None, ""):
            return None
        return str(value)

    @classmethod
    def _link_display_values(
        cls,
        links: Iterable[ItemRelationLink],
        *,
        primary_only: bool = False,
        unique: bool = True,
    ) -> tuple[str, ...]:
        values: list[str] = []
        seen: set[str] = set()
        for link in links:
            if primary_only and not link.primary:
                continue
            display = cls._display_value(link.target)
            if not display:
                continue
            if unique and display in seen:
                continue
            seen.add(display)
            values.append(display)
        return tuple(values)

    @classmethod
    def _format_candidates_from_links(cls, links: Iterable[ItemRelationLink]) -> tuple[str, ...]:
        values: list[str] = []
        seen: set[str] = set()
        for link in links:
            mapping = cls._rowish_to_mapping(link.target)
            for key in (
                "file_extension",
                "image_extension",
                "manifestation_format_detail",
                "digital_asset_extension",
                "asset_replica_extension",
                "identifier_scheme",
            ):
                raw = mapping.get(key)
                if raw in (None, ""):
                    continue
                token = str(raw).strip().lower()
                if key == "identifier_scheme":
                    continue
                if token in seen:
                    continue
                seen.add(token)
                values.append(token.upper())
        return tuple(values)

    @classmethod
    def _preferred_filename_stem(cls, title: Optional[str], primary_agents: tuple[str, ...], source_name: Optional[str]) -> Optional[str]:
        if title and primary_agents:
            return "{} - {}".format(title, " & ".join(primary_agents))
        if title:
            return title
        if source_name:
            return Path(str(source_name)).stem
        return None

    @classmethod
    def _preferred_storage_key(cls, *relations: list[ItemRelationLink]) -> Optional[str]:
        for links in relations:
            for link in links:
                mapping = cls._rowish_to_mapping(link.target)
                for key in ("file_storage_key", "image_storage_key", "asset_replica_storage_key"):
                    value = mapping.get(key)
                    if value not in (None, ""):
                        return str(value)
        return None

    def storage_hints(self) -> ItemStorageHints:
        work = self._first_target(self.get_relation_links("works"))
        expression = self._first_target(self.get_relation_links("expressions"))
        manifestation = self._first_target(self.get_relation_links("manifestations"))

        work_map = self._rowish_to_mapping(work)
        expression_map = self._rowish_to_mapping(expression)
        manifestation_map = self._rowish_to_mapping(manifestation)
        item_map = self.item.to_mapping() if self.item is not None else {}

        title = self._value_from_mapping(
            expression_map,
            ("expression_title_override",),
        )
        if title in (None, ""):
            title = self._value_from_mapping(work_map, ("work_canonical_title", "work_title"))
        if title in (None, ""):
            title = self._value_from_mapping(item_map, ("item_source_name",))
            if title not in (None, ""):
                title = Path(str(title)).stem

        canonical_title = self._value_from_mapping(work_map, ("work_canonical_title", "work_title"))
        sort_title = self._value_from_mapping(work_map, ("work_sort_title", "work_canonical_title", "work_title"))
        subtitle = self._value_from_mapping(manifestation_map, ("manifestation_subtitle",))
        if subtitle in (None, ""):
            subtitle = self._value_from_mapping(expression_map, ("expression_subtitle",))

        agent_links = self.get_relation_links("agents")
        primary_agents = self._link_display_values(agent_links, primary_only=True)
        if not primary_agents:
            primary_agents = self._link_display_values(agent_links)

        series = self._link_display_values(self.get_relation_links("series"))
        genres = self._link_display_values(self.get_relation_links("genres"))
        subjects = self._link_display_values(self.get_relation_links("subjects"))
        languages = self._link_display_values(self.get_relation_links("languages"))
        labels = self._link_display_values(self.get_relation_links("labels"))
        tags = self._link_display_values(self.get_relation_links("tags"))

        file_links = self.get_relation_links("files")
        image_links = self.get_relation_links("images")
        digital_asset_links = self.get_relation_links("digital_assets")
        replica_links = self.get_relation_links("asset_replicas")

        attachment_roles = []
        for links in (file_links, image_links):
            for link in links:
                mapping = self._rowish_to_mapping(link.target)
                role = self._value_from_mapping(mapping, ("file_role", "image_role"))
                if role not in (None, "") and str(role) not in attachment_roles:
                    attachment_roles.append(str(role))

        digital_asset_kinds = []
        for link in digital_asset_links:
            mapping = self._rowish_to_mapping(link.target)
            kind = self._value_from_mapping(
                mapping,
                ("digital_asset_media_category", "digital_asset_mime_type", "digital_asset_extension"),
            )
            if kind not in (None, "") and str(kind) not in digital_asset_kinds:
                digital_asset_kinds.append(str(kind))

        replica_modes = []
        for link in replica_links:
            mapping = self._rowish_to_mapping(link.target)
            mode = self._value_from_mapping(mapping, ("asset_replica_mode",))
            if mode not in (None, "") and str(mode) not in replica_modes:
                replica_modes.append(str(mode))

        file_formats = []
        for token in self._format_candidates_from_links(
            self.get_relation_links("manifestations")
            + file_links
            + image_links
            + digital_asset_links
            + replica_links
        ):
            if token not in file_formats:
                file_formats.append(token)

        preferred_folder_tokens: list[str] = []
        if primary_agents:
            preferred_folder_tokens.extend(primary_agents)
        elif series:
            preferred_folder_tokens.extend(series)
        if title not in (None, ""):
            preferred_folder_tokens.append(str(title))

        preferred_filename_stem = self._preferred_filename_stem(
            None if title in (None, "") else str(title),
            primary_agents,
            self._value_from_mapping(item_map, ("item_source_name",)),
        )

        preferred_storage_key = self._preferred_storage_key(replica_links, file_links, image_links)

        extra = {
            "work_count": len(self.get_relation_links("works")),
            "expression_count": len(self.get_relation_links("expressions")),
            "manifestation_count": len(self.get_relation_links("manifestations")),
            "file_count": len(file_links),
            "image_count": len(image_links),
            "digital_asset_count": len(digital_asset_links),
            "replica_count": len(replica_links),
        }

        return ItemStorageHints(
            item_id=self.item.item_id if self.item is not None else None,
            manifestation_id=self._value_from_mapping(
                manifestation_map,
                ("manifestation_id",),
            ) or (self.item.item_manifestation_id if self.item is not None else None),
            expression_id=self._value_from_mapping(expression_map, ("expression_id",)),
            work_id=self._value_from_mapping(work_map, ("work_id",)),
            title=None if title in (None, "") else str(title),
            canonical_title=None if canonical_title in (None, "") else str(canonical_title),
            sort_title=None if sort_title in (None, "") else str(sort_title),
            subtitle=None if subtitle in (None, "") else str(subtitle),
            item_type=self.item.item_type if self.item is not None else None,
            item_location=self.item.item_location if self.item is not None else None,
            inventory_code=self.item.item_inventory_code if self.item is not None else None,
            lifecycle_status=self.item.item_lifecycle_status if self.item is not None else None,
            condition=self.item.item_condition if self.item is not None else None,
            source=self.item.item_source if self.item is not None else None,
            source_name=self.item.item_source_name if self.item is not None else None,
            source_path=self.item.item_source_path if self.item is not None else None,
            primary_agents=primary_agents,
            series=series,
            genres=genres,
            subjects=subjects,
            languages=languages,
            labels=labels,
            tags=tags,
            attachment_roles=tuple(attachment_roles),
            digital_asset_kinds=tuple(digital_asset_kinds),
            replica_modes=tuple(replica_modes),
            file_formats=tuple(file_formats),
            preferred_folder_tokens=tuple(preferred_folder_tokens),
            preferred_filename_stem=preferred_filename_stem,
            preferred_storage_key=preferred_storage_key,
            extra=extra,
        )


__all__ = ["ItemMetadata"]

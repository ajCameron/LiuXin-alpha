"""
Hydrator/factory for concrete :class:`ItemMetadata` objects.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Iterable, Optional

from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.item_containers.item_metadata_api import (
    ItemRelationLink,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import (
    ItemIdentity,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import (
    ItemMetadata,
)
from LiuXin_alpha.metadata.read_sources import metadata_read_source_from
from LiuXin_alpha.utils.adaptors import _boolish_to_bool


class ItemMetadataHydrator:
    """
    Build :class:`ItemMetadata` instances from database rows or views.

    Supported entry points:
    - item id
    - live ``items`` row
    - any mapping/row containing ``item_id`` plus optional WEMI ids such as
      ``manifestation_id``, ``expression_id``, and ``work_id``.
    """

    def __init__(self, database: Any) -> None:
        if database is None:
            raise ValueError("ItemMetadataHydrator requires a database instance.")
        self.db = metadata_read_source_from(database)
        try:
            self._tables = set(self.db.get_tables(force_refresh=False))
        except Exception:
            self._tables = set()
        try:
            self._tables_and_columns = dict(self.db.get_tables_and_columns())
        except Exception:
            self._tables_and_columns = {}

    def from_item_id(self, item_id: int) -> ItemMetadata:
        item_row = self.db.get_row_from_id("items", int(item_id))
        if item_row is None:
            raise ValueError("No item found for id {}.".format(int(item_id)))
        return self._hydrate(item_row=item_row, source_row=item_row)

    def from_source_row(self, source_row: Mapping[str, Any] | Row) -> ItemMetadata:
        ids = self._extract_known_ids(source_row)
        item_row = None
        if isinstance(source_row, Row) and source_row.table == "items":
            item_row = source_row
        elif ids["item_id"] is not None:
            item_row = self.db.get_row_from_id("items", int(ids["item_id"]))

        if item_row is None and self._looks_like_item_mapping(source_row):
            return self._hydrate(item_row=None, source_row=source_row)
        if item_row is None:
            raise ValueError("Could not resolve an item row from the supplied source row/view.")
        return self._hydrate(item_row=item_row, source_row=source_row)

    @staticmethod
    def _mapping_from(value: Mapping[str, Any] | Row | Any) -> Mapping[str, Any]:
        if isinstance(value, Row):
            return value.row_dict
        if isinstance(value, Mapping):
            return value
        return {}

    def _has_table(self, table: str) -> bool:
        return table in self._tables or table in self._tables_and_columns

    def _has_column(self, table: str, column: str) -> bool:
        return column in set(self._tables_and_columns.get(table, []))

    def _looks_like_item_mapping(self, value: Mapping[str, Any] | Row | Any) -> bool:
        mapping = self._mapping_from(value)
        return bool(mapping) and ("item_manifestation_id" in mapping or "item_id" in mapping)

    @staticmethod
    def _extract_known_ids(source_row: Mapping[str, Any] | Row | Any) -> dict[str, Optional[int]]:
        mapping = ItemMetadataHydrator._mapping_from(source_row)

        def _as_int(value: Any) -> Optional[int]:
            if value in (None, ""):
                return None
            try:
                return int(value)
            except Exception:
                return None

        return {
            "item_id": _as_int(mapping.get("item_id")),
            "manifestation_id": _as_int(mapping.get("manifestation_id") or mapping.get("item_manifestation_id") or mapping.get("book_manifestation_id")),
            "expression_id": _as_int(mapping.get("expression_id") or mapping.get("book_expression_id")),
            "work_id": _as_int(mapping.get("work_id") or mapping.get("book_work_id") or mapping.get("title_id")),
        }

    def _hydrate(self, *, item_row: Optional[Row], source_row: Mapping[str, Any] | Row) -> ItemMetadata:
        ids = self._extract_known_ids(source_row)
        source_map = self._mapping_from(source_row)

        item_container = None
        if item_row is not None:
            item_container = ItemIdentity.from_mapping(item_row.row_dict)
        elif self._looks_like_item_mapping(source_map):
            item_container = ItemIdentity.from_mapping(source_map)

        container = ItemMetadata(item=item_container)

        manifestation_rows: list[Row] = []
        expression_rows: list[Row] = []
        work_rows: list[Row] = []

        manifestation_id = None
        if item_container is not None:
            manifestation_id = item_container.item_manifestation_id
        if manifestation_id is None:
            manifestation_id = ids["manifestation_id"]

        if manifestation_id is not None:
            manifestation_row = self.db.get_row_from_id("manifestations", int(manifestation_id))
            if manifestation_row is not None:
                manifestation_rows.append(manifestation_row)
                container.add_relation_link(
                    "manifestations",
                    ItemRelationLink(
                        target=manifestation_row,
                        primary=True,
                        type="parent_manifestation",
                        extra={"source_entity_type": "item"},
                    ),
                )

        explicit_expression_id = ids["expression_id"]
        if explicit_expression_id is not None:
            expression_row = self.db.get_row_from_id("expressions", int(explicit_expression_id))
            if expression_row is not None:
                expression_rows.append(expression_row)

        if manifestation_rows:
            for manifestation_row in manifestation_rows:
                links = self._collect_interlinks_from_row(
                    manifestation_row,
                    secondary_table="expressions",
                    source_entity_type="manifestation",
                )
                if links:
                    self._append_links_unique(container, "expressions", links)
                    for link in links:
                        if isinstance(link.target, Row):
                            expression_rows.append(link.target)

        expression_rows = self._dedupe_rows(expression_rows)
        if explicit_expression_id is not None and expression_rows:
            self._mark_primary_by_row_id(container.get_relation_links("expressions"), explicit_expression_id)

        explicit_work_id = ids["work_id"]
        if explicit_work_id is not None:
            work_row = self.db.get_row_from_id("works", int(explicit_work_id))
            if work_row is not None:
                work_rows.append(work_row)

        for expression_row in expression_rows:
            links = self._collect_interlinks_from_row(
                expression_row,
                secondary_table="works",
                source_entity_type="expression",
            )
            if links:
                self._append_links_unique(container, "works", links)
                for link in links:
                    if isinstance(link.target, Row):
                        work_rows.append(link.target)

        work_rows = self._dedupe_rows(work_rows)
        if explicit_work_id is not None and work_rows:
            self._mark_primary_by_row_id(container.get_relation_links("works"), explicit_work_id)

        relation_source_rows: list[Row] = []
        if item_row is not None:
            relation_source_rows.append(item_row)
        relation_source_rows.extend(manifestation_rows)
        relation_source_rows.extend(expression_rows)
        relation_source_rows.extend(work_rows)

        multi_source_relations = (
            "agents",
            "genres",
            "subjects",
            "series",
            "tags",
            "labels",
            "languages",
            "notes",
            "comments",
            "images",
        )
        for relation in multi_source_relations:
            for source in relation_source_rows:
                self._append_links_unique(
                    container,
                    relation,
                    self._collect_interlinks_from_row(source, secondary_table=relation, source_entity_type=source.table[:-1]),
                )

        # Direct item-bound helper tables / assets.
        if item_container is not None and item_container.item_id is not None:
            item_id = int(item_container.item_id)
            self._append_links_unique(container, "files", self._collect_direct_fk_rows(
                table="files",
                fk_column="file_item_id",
                fk_value=item_id,
                type_hint="item_file",
            ))
            self._append_links_unique(container, "images", self._collect_direct_fk_rows(
                table="images",
                fk_column="image_item_id",
                fk_value=item_id,
                type_hint="item_image",
            ))
            self._append_links_unique(container, "annotations", self._collect_direct_fk_rows(
                table="annotations",
                fk_column="annotation_item_id",
                fk_value=item_id,
                type_hint="item_annotation",
            ))
            self._append_links_unique(container, "identifiers", self._collect_identifier_rows(item_id=item_id, work_rows=work_rows, expression_rows=expression_rows, manifestation_rows=manifestation_rows))

            # Managed storage graph.
            self._append_links_unique(
                container,
                "digital_assets",
                self._collect_interlinks_from_row(
                    item_row if item_row is not None else self.db.get_row_from_id("items", item_id),
                    secondary_table="digital_assets",
                    source_entity_type="item",
                ) if item_row is not None or self.db.get_row_from_id("items", item_id) is not None else [],
            )
            self._append_links_unique(
                container,
                "composite_digital_assets",
                self._collect_interlinks_from_row(
                    item_row if item_row is not None else self.db.get_row_from_id("items", item_id),
                    secondary_table="composite_digital_assets",
                    source_entity_type="item",
                ) if item_row is not None or self.db.get_row_from_id("items", item_id) is not None else [],
            )

        for digital_asset_link in container.get_relation_links("digital_assets"):
            target = digital_asset_link.target
            if not isinstance(target, Row):
                continue
            self._append_links_unique(
                container,
                "asset_replicas",
                self._collect_direct_fk_rows(
                    table="asset_replicas",
                    fk_column="asset_replica_digital_asset_id",
                    fk_value=int(target.row_id),
                    type_hint="asset_replica",
                ),
            )

        self._hydrate_folders_and_stores(container, work_rows=work_rows)
        return container

    @staticmethod
    def _row_key(row: Row | Any) -> tuple[str, int] | None:
        if not isinstance(row, Row):
            return None
        if row.table is None or row.row_id is None:
            return None
        return (str(row.table), int(row.row_id))

    def _dedupe_rows(self, rows: Iterable[Row]) -> list[Row]:
        ordered: list[Row] = []
        seen: set[tuple[str, int]] = set()
        for row in rows:
            key = self._row_key(row)
            if key is None or key in seen:
                continue
            seen.add(key)
            ordered.append(row)
        return ordered

    @staticmethod
    def _mark_primary_by_row_id(links: list[ItemRelationLink], row_id: int) -> None:
        for index, link in enumerate(links):
            target = link.target
            if isinstance(target, Row) and int(target.row_id) == int(row_id):
                links[index] = ItemRelationLink(
                    target=link.target,
                    priority=link.priority,
                    primary=True,
                    type=link.type,
                    origin=link.origin,
                    source=link.source,
                    policy=link.policy,
                    data=link.data,
                    index=link.index,
                    link_id=link.link_id,
                    cardinality=link.cardinality,
                    extra=dict(link.extra),
                )
                break

    def _append_links_unique(self, container: ItemMetadata, relation: str, links: Iterable[ItemRelationLink]) -> None:
        existing = container.get_relation_links(relation)
        seen_rows = {self._row_key(link.target) for link in existing if isinstance(link.target, Row)}
        for link in links:
            key = self._row_key(link.target)
            if key is not None and key in seen_rows:
                continue
            existing.append(link)
            if key is not None:
                seen_rows.add(key)

    def _collect_interlinks_from_row(
        self,
        source_row: Optional[Row],
        *,
        secondary_table: str,
        source_entity_type: str,
    ) -> list[ItemRelationLink]:
        if source_row is None:
            return []
        if not self._has_table(secondary_table):
            return []
        try:
            link_rows = list(self.db.get_interlink_rows(primary_row=source_row, secondary_table=secondary_table))
        except Exception:
            return []

        try:
            secondary_id_column = self.db.driver_wrapper.get_link_column(
                source_row.table,
                secondary_table,
                self.db.driver_wrapper.get_id_column(secondary_table),
            )
        except Exception:
            secondary_id_column = None

        prefix = None
        try:
            link_table = self.db.driver_wrapper.get_link_table_name(source_row.table, secondary_table)
            if link_table:
                prefix = self.db.driver_wrapper.get_column_base(link_table)
        except Exception:
            prefix = None

        out: list[ItemRelationLink] = []
        for link_row in link_rows:
            link_map = link_row.row_dict if isinstance(link_row, Row) else dict(link_row)
            target = None
            target_id = link_map.get(secondary_id_column) if secondary_id_column else None
            if target_id not in (None, ""):
                try:
                    target = self.db.get_row_from_id(secondary_table, int(target_id))
                except Exception:
                    target = None
            if target is None:
                continue

            extra = {"source_entity_type": source_entity_type}
            if prefix is not None:
                for key, value in link_map.items():
                    if not str(key).startswith(prefix + "_"):
                        continue
                    suffix = str(key)[len(prefix) + 1 :]
                    if suffix in {
                        self.db.driver_wrapper.get_id_column(source_row.table),
                        self.db.driver_wrapper.get_id_column(secondary_table),
                        "priority",
                        "primary",
                        "type",
                        "origin",
                        "source",
                        "policy",
                        "data",
                        "index",
                        "id",
                    }:
                        continue
                    extra[suffix] = value

            out.append(
                ItemRelationLink(
                    target=target,
                    priority=link_map.get(prefix + "_priority") if prefix else None,
                    primary=_boolish_to_bool(link_map.get(prefix + "_primary")) if prefix else None,
                    type=link_map.get(prefix + "_type") if prefix else None,
                    origin=link_map.get(prefix + "_origin") if prefix else None,
                    source=link_map.get(prefix + "_source") if prefix else None,
                    policy=link_map.get(prefix + "_policy") if prefix else None,
                    data=link_map.get(prefix + "_data") if prefix else None,
                    index=link_map.get(prefix + "_index") if prefix else None,
                    link_id=link_map.get(prefix + "_id") if prefix else None,
                    extra=extra,
                )
            )
        return out

    def _collect_direct_fk_rows(
        self,
        *,
        table: str,
        fk_column: str,
        fk_value: int,
        type_hint: str,
    ) -> list[ItemRelationLink]:
        if not self._has_table(table) or not self._has_column(table, fk_column):
            return []
        try:
            rows = list(self.db.search(table=table, column=fk_column, search_term=int(fk_value)))
        except Exception:
            return []
        return [
            ItemRelationLink(
                target=row,
                primary=(index == 0),
                type=type_hint,
                extra={"source_entity_type": table},
            )
            for index, row in enumerate(rows)
            if row is not None
        ]

    def _collect_identifier_rows(
        self,
        *,
        item_id: int,
        work_rows: list[Row],
        expression_rows: list[Row],
        manifestation_rows: list[Row],
    ) -> list[ItemRelationLink]:
        links: list[ItemRelationLink] = []
        if self._has_table("item_identifiers") and self._has_column("item_identifiers", "item_identifier_item_id"):
            try:
                for row in self.db.search("item_identifiers", "item_identifier_item_id", item_id):
                    links.append(ItemRelationLink(target=row, primary=None, type="item_identifier", extra={"source_entity_type": "item"}))
            except Exception:
                pass
        if self._has_table("entity_identifiers") and self._has_column("entity_identifiers", "entity_identifier_entity_type"):
            entity_targets: list[tuple[str, int]] = [("item", item_id)]
            entity_targets.extend(("manifestation", int(row.row_id)) for row in manifestation_rows if row.row_id is not None)
            entity_targets.extend(("expression", int(row.row_id)) for row in expression_rows if row.row_id is not None)
            entity_targets.extend(("work", int(row.row_id)) for row in work_rows if row.row_id is not None)
            for entity_type, entity_id in entity_targets:
                try:
                    rows = list(self.db.search("entity_identifiers", "entity_identifier_entity_id", entity_id))
                except Exception:
                    continue
                for row in rows:
                    mapping = row.row_dict if isinstance(row, Row) else dict(row)
                    if str(mapping.get("entity_identifier_entity_type")) != entity_type:
                        continue
                    links.append(
                        ItemRelationLink(
                            target=row,
                            primary=_boolish_to_bool(mapping.get("entity_identifier_is_primary")),
                            type="entity_identifier",
                            origin=mapping.get("entity_identifier_provenance"),
                            extra={"source_entity_type": entity_type},
                        )
                    )
        return links

    def _hydrate_folders_and_stores(self, container: ItemMetadata, *, work_rows: list[Row]) -> None:
        folder_rows: list[Row] = []
        store_rows: list[Row] = []

        for relation in ("files", "images", "asset_replicas"):
            for link in container.get_relation_links(relation):
                target = link.target
                if not isinstance(target, Row):
                    continue
                mapping = target.row_dict
                folder_id = mapping.get("file_folder_id") or mapping.get("image_folder_id") or mapping.get("asset_replica_folder_id")
                store_id = mapping.get("file_store_id") or mapping.get("image_store_id") or mapping.get("asset_replica_store_id")
                if folder_id not in (None, "") and self._has_table("folders"):
                    folder = self.db.get_row_from_id("folders", int(folder_id))
                    if folder is not None:
                        folder_rows.append(folder)
                if store_id not in (None, "") and self._has_table("stores"):
                    store = self.db.get_row_from_id("stores", int(store_id))
                    if store is not None:
                        store_rows.append(store)

        for work_row in work_rows:
            if self._has_table("folders"):
                folder_rows.extend(
                    [
                        link.target
                        for link in self._collect_interlinks_from_row(work_row, secondary_table="folders", source_entity_type="work")
                        if isinstance(link.target, Row)
                    ]
                )

        for folder_row in self._dedupe_rows(folder_rows):
            container.add_relation_link(
                "folders",
                ItemRelationLink(target=folder_row, primary=False, type="resolved_folder", extra={"source_entity_type": "folder"}),
            )
            store_id = folder_row.row_dict.get("folder_store_id")
            if store_id not in (None, "") and self._has_table("stores"):
                store = self.db.get_row_from_id("stores", int(store_id))
                if store is not None:
                    store_rows.append(store)

        for store_row in self._dedupe_rows(store_rows):
            container.add_relation_link(
                "stores",
                ItemRelationLink(target=store_row, primary=False, type="resolved_store", extra={"source_entity_type": "store"}),
            )


__all__ = ["ItemMetadataHydrator"]

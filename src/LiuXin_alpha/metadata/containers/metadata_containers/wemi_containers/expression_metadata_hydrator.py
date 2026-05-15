"""Hydrator/factory for concrete :class:`ExpressionMetadata` objects."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Iterable, Optional

from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.expression_containers.expression_metadata_api import (
    ExpressionRelationLink,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_container import (
    ExpressionIdentity,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_container import (
    ExpressionMetadata,
)
from LiuXin_alpha.metadata.read_sources import metadata_read_source_from
from LiuXin_alpha.utils.adaptors import _boolish_to_bool


class ExpressionMetadataHydrator:
    """
    Build :class:`ExpressionMetadata` instances from database rows or views.

    Supported entry points:
    - expression id
    - live ``expressions`` row
    - any mapping/row containing ``expression_id`` plus optional related WEMI ids
      such as ``work_id``, ``manifestation_id``, and ``item_id``.
    """

    def __init__(self, database: Any) -> None:
        if database is None:
            raise ValueError("ExpressionMetadataHydrator requires a database instance.")
        self.db = metadata_read_source_from(database)
        try:
            self._tables = set(self.db.get_tables(force_refresh=False))
        except Exception:
            self._tables = set()
        try:
            self._tables_and_columns = dict(self.db.get_tables_and_columns())
        except Exception:
            self._tables_and_columns = {}

    def from_expression_id(self, expression_id: int) -> ExpressionMetadata:
        expression_row = self.db.get_row_from_id("expressions", int(expression_id))
        if expression_row is None:
            raise ValueError(f"No expression found for id {int(expression_id)}.")
        return self._hydrate(expression_row=expression_row, source_row=expression_row)

    def from_source_row(self, source_row: Mapping[str, Any] | Row) -> ExpressionMetadata:
        ids = self._extract_known_ids(source_row)
        expression_row = None
        if isinstance(source_row, Row) and source_row.table == "expressions":
            expression_row = source_row
        elif ids["expression_id"] is not None:
            expression_row = self.db.get_row_from_id("expressions", int(ids["expression_id"]))

        if expression_row is None and self._looks_like_expression_mapping(source_row):
            return self._hydrate(expression_row=None, source_row=source_row)
        if expression_row is None:
            raise ValueError("Could not resolve an expression row from the supplied source row/view.")
        return self._hydrate(expression_row=expression_row, source_row=source_row)

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

    def _looks_like_expression_mapping(self, value: Mapping[str, Any] | Row | Any) -> bool:
        mapping = self._mapping_from(value)
        return bool(mapping) and bool(
            {"expression_id", "expression_title_override", "expression_label", "expression_work_id"} & set(mapping)
        )

    @staticmethod
    def _extract_known_ids(source_row: Mapping[str, Any] | Row | Any) -> dict[str, Optional[int]]:
        mapping = ExpressionMetadataHydrator._mapping_from(source_row)

        def _as_int(value: Any) -> Optional[int]:
            if value in (None, ""):
                return None
            try:
                return int(value)
            except Exception:
                return None

        return {
            "expression_id": _as_int(mapping.get("expression_id") or mapping.get("book_expression_id")),
            "work_id": _as_int(mapping.get("work_id") or mapping.get("expression_work_id") or mapping.get("title_id")),
            "manifestation_id": _as_int(mapping.get("manifestation_id") or mapping.get("item_manifestation_id") or mapping.get("book_manifestation_id")),
            "item_id": _as_int(mapping.get("item_id")),
        }

    def _hydrate(
        self,
        *,
        expression_row: Optional[Row],
        source_row: Mapping[str, Any] | Row,
    ) -> ExpressionMetadata:
        ids = self._extract_known_ids(source_row)
        source_map = self._mapping_from(source_row)

        expression_container = None
        if expression_row is not None:
            expression_container = ExpressionIdentity.from_mapping(expression_row.row_dict)
        elif self._looks_like_expression_mapping(source_map):
            payload = dict(source_map)
            if payload.get("expression_id") in (None, "") and ids["expression_id"] is not None:
                payload["expression_id"] = ids["expression_id"]
            expression_container = ExpressionIdentity.from_mapping(payload)

        container = ExpressionMetadata(expression=expression_container)

        expression_rows: list[Row] = []
        work_rows: list[Row] = []
        manifestation_rows: list[Row] = []
        item_rows: list[Row] = []

        if expression_row is not None:
            expression_rows.append(expression_row)

        work_id = ids["work_id"]
        if work_id is None and expression_container is not None:
            work_id = expression_container.expression_work_id
        if work_id is not None:
            work_row = self.db.get_row_from_id("works", int(work_id))
            if work_row is not None:
                work_rows.append(work_row)
                self._ensure_row_link(
                    container,
                    "works",
                    work_row,
                    type_hint="parent_work",
                    source_entity_type="expression",
                    primary=True,
                )

        if expression_row is not None:
            work_links = self._collect_interlinks_from_row(
                expression_row,
                secondary_table="works",
                source_entity_type="expression",
            )
            if work_links:
                self._append_links_unique(container, "works", work_links)
                for link in work_links:
                    if isinstance(link.target, Row):
                        work_rows.append(link.target)

        manifestation_id = ids["manifestation_id"]
        if manifestation_id is not None:
            manifestation_row = self.db.get_row_from_id("manifestations", int(manifestation_id))
            if manifestation_row is not None:
                manifestation_rows.append(manifestation_row)

        if expression_row is not None:
            manifestation_links = self._collect_interlinks_from_row(
                expression_row,
                secondary_table="manifestations",
                source_entity_type="expression",
            )
            if manifestation_links:
                self._append_links_unique(container, "manifestations", manifestation_links)
                for link in manifestation_links:
                    if isinstance(link.target, Row):
                        manifestation_rows.append(link.target)

        manifestation_rows = self._dedupe_rows(manifestation_rows)
        for manifestation_row in manifestation_rows:
            self._ensure_row_link(
                container,
                "manifestations",
                manifestation_row,
                type_hint="expression_manifestation",
                source_entity_type="expression",
            )

        item_id = ids["item_id"]
        if item_id is not None:
            item_row = self.db.get_row_from_id("items", int(item_id))
            if item_row is not None:
                item_rows.append(item_row)

        for manifestation_row in manifestation_rows:
            item_rows.extend(self._collect_item_rows_from_manifestation(manifestation_row))

        item_rows = self._dedupe_rows(item_rows)
        for item_row in item_rows:
            self._ensure_row_link(
                container,
                "items",
                item_row,
                type_hint="manifestation_item",
                source_entity_type="manifestation",
            )

        relation_source_rows: list[Row] = []
        relation_source_rows.extend(expression_rows)
        relation_source_rows.extend(work_rows)
        relation_source_rows.extend(manifestation_rows)
        relation_source_rows.extend(item_rows)

        multi_source_relations = (
            "agents",
            "titles",
            "genres",
            "tags",
            "labels",
            "languages",
            "notes",
            "comments",
        )
        for relation in multi_source_relations:
            for source in relation_source_rows:
                self._append_links_unique(
                    container,
                    relation,
                    self._collect_interlinks_from_row(
                        source,
                        secondary_table=relation,
                        source_entity_type=source.table[:-1],
                    ),
                )

        self._append_links_unique(
            container,
            "identifiers",
            self._collect_identifier_rows(
                expression_rows=expression_rows,
                work_rows=work_rows,
                manifestation_rows=manifestation_rows,
                item_rows=item_rows,
            ),
        )
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

    def _append_links_unique(
        self,
        container: ExpressionMetadata,
        relation: str,
        links: Iterable[ExpressionRelationLink],
    ) -> None:
        existing = container.get_relation_links(relation)
        seen_rows = {
            self._row_key(link.target): index
            for index, link in enumerate(existing)
            if isinstance(link.target, Row)
        }
        for link in links:
            key = self._row_key(link.target)
            if key is not None and key in seen_rows and seen_rows[key] is not None:
                existing_index = seen_rows[key]
                existing[existing_index] = self._merge_link_metadata(
                    existing[existing_index],
                    link,
                )
                continue
            existing.append(link)
            if key is not None:
                seen_rows[key] = len(existing) - 1

    @staticmethod
    def _merge_link_metadata(
        existing: ExpressionRelationLink,
        incoming: ExpressionRelationLink,
    ) -> ExpressionRelationLink:
        extra = dict(existing.extra)
        extra.update(incoming.extra)
        return ExpressionRelationLink(
            target=existing.target,
            priority=incoming.priority if incoming.priority is not None else existing.priority,
            primary=incoming.primary if incoming.primary is not None else existing.primary,
            type=incoming.type if incoming.type is not None else existing.type,
            origin=incoming.origin if incoming.origin is not None else existing.origin,
            source=incoming.source if incoming.source is not None else existing.source,
            policy=incoming.policy if incoming.policy is not None else existing.policy,
            data=incoming.data if incoming.data is not None else existing.data,
            index=incoming.index if incoming.index is not None else existing.index,
            link_id=incoming.link_id if incoming.link_id is not None else existing.link_id,
            cardinality=(
                incoming.cardinality
                if incoming.cardinality is not None
                else existing.cardinality
            ),
            extra=extra,
        )

    def _ensure_row_link(
        self,
        container: ExpressionMetadata,
        relation: str,
        row: Row,
        *,
        type_hint: str,
        source_entity_type: str,
        primary: bool | None = None,
    ) -> None:
        key = self._row_key(row)
        if key is None:
            return
        for link in container.get_relation_links(relation):
            if self._row_key(link.target) == key:
                return
        container.get_relation_links(relation).append(
            ExpressionRelationLink(
                target=row,
                primary=primary,
                type=type_hint,
                extra={"source_entity_type": source_entity_type},
            )
        )

    def _collect_interlinks_from_row(
        self,
        source_row: Optional[Row],
        *,
        secondary_table: str,
        source_entity_type: str,
    ) -> list[ExpressionRelationLink]:
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

        out: list[ExpressionRelationLink] = []
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
                ExpressionRelationLink(
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

    def _collect_item_rows_from_manifestation(self, manifestation_row: Row) -> list[Row]:
        if not self._has_table("items") or not self._has_column("items", "item_manifestation_id"):
            return []
        manifestation_id = manifestation_row.row_id
        if manifestation_id is None:
            return []
        try:
            return list(self.db.search(table="items", column="item_manifestation_id", search_term=int(manifestation_id)))
        except Exception:
            return []

    def _collect_identifier_rows(
        self,
        *,
        expression_rows: list[Row],
        work_rows: list[Row],
        manifestation_rows: list[Row],
        item_rows: list[Row],
    ) -> list[ExpressionRelationLink]:
        links: list[ExpressionRelationLink] = []
        if self._has_table("item_identifiers") and self._has_column("item_identifiers", "item_identifier_item_id"):
            for item_row in item_rows:
                if item_row.row_id is None:
                    continue
                try:
                    rows = list(self.db.search("item_identifiers", "item_identifier_item_id", int(item_row.row_id)))
                except Exception:
                    continue
                for row in rows:
                    links.append(
                        ExpressionRelationLink(
                            target=row,
                            type="item_identifier",
                            extra={"source_entity_type": "item"},
                        )
                    )

        if self._has_table("entity_identifiers") and self._has_column("entity_identifiers", "entity_identifier_entity_type"):
            entity_targets: list[tuple[str, int]] = []
            entity_targets.extend(("expression", int(row.row_id)) for row in expression_rows if row.row_id is not None)
            entity_targets.extend(("work", int(row.row_id)) for row in work_rows if row.row_id is not None)
            entity_targets.extend(("manifestation", int(row.row_id)) for row in manifestation_rows if row.row_id is not None)
            entity_targets.extend(("item", int(row.row_id)) for row in item_rows if row.row_id is not None)
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
                        ExpressionRelationLink(
                            target=row,
                            primary=_boolish_to_bool(mapping.get("entity_identifier_is_primary")),
                            type="entity_identifier",
                            origin=mapping.get("entity_identifier_provenance"),
                            extra={"source_entity_type": entity_type},
                        )
                    )
        return links


__all__ = ["ExpressionMetadataHydrator"]

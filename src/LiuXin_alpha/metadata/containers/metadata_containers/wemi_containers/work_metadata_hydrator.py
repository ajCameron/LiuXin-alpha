"""
Hydrator/factory for concrete :class:`WorkMetadataContainer` objects.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Iterable, Optional

from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.work_metadata_container_api import (
    WorkRelationLink,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import (
    WorkContainer,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import (
    WorkMetadataContainer,
)
from LiuXin_alpha.utils.adaptors import _boolish_to_bool


class WorkMetadataHydrator:
    """
    Build :class:`WorkMetadataContainer` instances from database rows or views.

    Supported entry points:
    - work id
    - live ``works`` row
    - any mapping/row containing ``work_id`` plus optional descendant ids such
      as ``expression_id``, ``manifestation_id``, and ``item_id``.
    """

    def __init__(self, database: Any) -> None:
        if database is None:
            raise ValueError("WorkMetadataHydrator requires a database instance.")
        self.db = database
        try:
            self._tables = set(self.db.get_tables(force_refresh=False))
        except Exception:
            self._tables = set()
        try:
            self._tables_and_columns = dict(self.db.get_tables_and_columns())
        except Exception:
            self._tables_and_columns = {}

    def from_work_id(self, work_id: int) -> WorkMetadataContainer:
        work_row = self.db.get_row_from_id("works", int(work_id))
        if work_row is None:
            raise ValueError("No work found for id {}.".format(int(work_id)))
        return self._hydrate(work_row=work_row, source_row=work_row)

    def from_source_row(self, source_row: Mapping[str, Any] | Row) -> WorkMetadataContainer:
        ids = self._extract_known_ids(source_row)
        work_row = None
        if isinstance(source_row, Row) and source_row.table == "works":
            work_row = source_row
        elif ids["work_id"] is not None:
            work_row = self.db.get_row_from_id("works", int(ids["work_id"]))

        if work_row is None and self._looks_like_work_mapping(source_row):
            return self._hydrate(work_row=None, source_row=source_row)
        if work_row is None:
            raise ValueError("Could not resolve a work row from the supplied source row/view.")
        return self._hydrate(work_row=work_row, source_row=source_row)

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

    def _looks_like_work_mapping(self, value: Mapping[str, Any] | Row | Any) -> bool:
        mapping = self._mapping_from(value)
        return bool(mapping) and bool(
            {"work_id", "title_id", "work_title", "work_canonical_title"} & set(mapping)
        )

    @staticmethod
    def _extract_known_ids(source_row: Mapping[str, Any] | Row | Any) -> dict[str, Optional[int]]:
        mapping = WorkMetadataHydrator._mapping_from(source_row)

        def _as_int(value: Any) -> Optional[int]:
            if value in (None, ""):
                return None
            try:
                return int(value)
            except Exception:
                return None

        return {
            "work_id": _as_int(mapping.get("work_id") or mapping.get("title_id")),
            "expression_id": _as_int(mapping.get("expression_id") or mapping.get("book_expression_id")),
            "manifestation_id": _as_int(mapping.get("manifestation_id") or mapping.get("item_manifestation_id") or mapping.get("book_manifestation_id")),
            "item_id": _as_int(mapping.get("item_id")),
        }

    def _hydrate(
        self,
        *,
        work_row: Optional[Row],
        source_row: Mapping[str, Any] | Row,
    ) -> WorkMetadataContainer:
        ids = self._extract_known_ids(source_row)
        source_map = self._mapping_from(source_row)

        work_container = None
        if work_row is not None:
            work_container = WorkContainer.from_mapping(work_row.row_dict)
        elif self._looks_like_work_mapping(source_map):
            payload = dict(source_map)
            if payload.get("work_id") in (None, "") and ids["work_id"] is not None:
                payload["work_id"] = ids["work_id"]
            work_container = WorkContainer.from_mapping(payload)

        container = WorkMetadataContainer(work=work_container)

        work_rows: list[Row] = []
        expression_rows: list[Row] = []
        manifestation_rows: list[Row] = []
        item_rows: list[Row] = []

        if work_row is not None:
            work_rows.append(work_row)

        explicit_expression_id = ids["expression_id"]
        if explicit_expression_id is not None:
            expression_row = self.db.get_row_from_id("expressions", int(explicit_expression_id))
            if expression_row is not None:
                expression_rows.append(expression_row)

        if work_row is not None:
            expression_links = self._collect_interlinks_from_row(
                work_row,
                secondary_table="expressions",
                source_entity_type="work",
            )
            if expression_links:
                self._append_links_unique(container, "expressions", expression_links)
                for link in expression_links:
                    if isinstance(link.target, Row):
                        expression_rows.append(link.target)

        expression_rows = self._dedupe_rows(expression_rows)
        for expression_row in expression_rows:
            self._ensure_row_link(
                container,
                "expressions",
                expression_row,
                type_hint="work_expression",
                source_entity_type="work",
            )

        explicit_manifestation_id = ids["manifestation_id"]
        if explicit_manifestation_id is not None:
            manifestation_row = self.db.get_row_from_id(
                "manifestations",
                int(explicit_manifestation_id),
            )
            if manifestation_row is not None:
                manifestation_rows.append(manifestation_row)

        for expression_row in expression_rows:
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

        explicit_item_id = ids["item_id"]
        if explicit_item_id is not None:
            item_row = self.db.get_row_from_id("items", int(explicit_item_id))
            if item_row is not None:
                item_rows.append(item_row)

        for manifestation_row in manifestation_rows:
            item_rows.extend(
                self._collect_item_rows_from_manifestation(manifestation_row),
            )

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
        relation_source_rows.extend(work_rows)
        relation_source_rows.extend(expression_rows)
        relation_source_rows.extend(manifestation_rows)
        relation_source_rows.extend(item_rows)

        multi_source_relations = (
            "agents",
            "genres",
            "subjects",
            "series",
            "tags",
            "labels",
            "languages",
            "ratings",
            "notes",
            "comments",
            "synopses",
            "images",
            "folders",
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

        for item_row in item_rows:
            item_id = item_row.row_id
            if item_id is None:
                continue
            self._append_links_unique(
                container,
                "files",
                self._collect_direct_fk_rows(
                    table="files",
                    fk_column="file_item_id",
                    fk_value=int(item_id),
                    type_hint="item_file",
                ),
            )
            self._append_links_unique(
                container,
                "images",
                self._collect_direct_fk_rows(
                    table="images",
                    fk_column="image_item_id",
                    fk_value=int(item_id),
                    type_hint="item_image",
                ),
            )

        self._append_links_unique(
            container,
            "identifiers",
            self._collect_identifier_rows(
                work_rows=work_rows,
                expression_rows=expression_rows,
                manifestation_rows=manifestation_rows,
                item_rows=item_rows,
            ),
        )
        self._hydrate_folders(container, work_rows=work_rows)
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
        container: WorkMetadataContainer,
        relation: str,
        links: Iterable[WorkRelationLink],
    ) -> None:
        existing = container.get_relation_links(relation)
        seen_rows = {
            self._row_key(link.target) for link in existing if isinstance(link.target, Row)
        }
        for link in links:
            key = self._row_key(link.target)
            if key is not None and key in seen_rows:
                continue
            existing.append(link)
            if key is not None:
                seen_rows.add(key)

    def _ensure_row_link(
        self,
        container: WorkMetadataContainer,
        relation: str,
        row: Row,
        *,
        type_hint: str,
        source_entity_type: str,
    ) -> None:
        key = self._row_key(row)
        if key is None:
            return
        for link in container.get_relation_links(relation):
            if self._row_key(link.target) == key:
                return
        container.add_relation_link(
            relation,
            WorkRelationLink(
                target=row,
                type=type_hint,
                extra={"source_entity_type": source_entity_type},
            ),
        )

    def _collect_interlinks_from_row(
        self,
        source_row: Optional[Row],
        *,
        secondary_table: str,
        source_entity_type: str,
    ) -> list[WorkRelationLink]:
        if source_row is None:
            return []
        if not self._has_table(secondary_table):
            return []
        try:
            link_rows = list(
                self.db.get_interlink_rows(
                    primary_row=source_row,
                    secondary_table=secondary_table,
                )
            )
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
            link_table = self.db.driver_wrapper.get_link_table_name(
                source_row.table,
                secondary_table,
            )
            if link_table:
                prefix = self.db.driver_wrapper.get_column_base(link_table)
        except Exception:
            prefix = None

        out: list[WorkRelationLink] = []
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
                        "type",
                        "origin",
                        "policy",
                        "data",
                        "primary",
                        "index",
                    }:
                        continue
                    extra[suffix] = value
                if prefix + "_primary" in link_map:
                    extra["is_primary"] = _boolish_to_bool(link_map.get(prefix + "_primary"))
                if prefix + "_index" in link_map and link_map.get(prefix + "_index") not in (None, ""):
                    extra["index"] = link_map.get(prefix + "_index")

            out.append(
                WorkRelationLink(
                    target=target,
                    priority=link_map.get(prefix + "_priority") if prefix else None,
                    type=link_map.get(prefix + "_type") if prefix else None,
                    origin=link_map.get(prefix + "_origin") if prefix else None,
                    policy=link_map.get(prefix + "_policy") if prefix else None,
                    data=link_map.get(prefix + "_data") if prefix else None,
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
            return list(
                self.db.search(
                    table="items",
                    column="item_manifestation_id",
                    search_term=int(manifestation_id),
                )
            )
        except Exception:
            return []

    def _collect_direct_fk_rows(
        self,
        *,
        table: str,
        fk_column: str,
        fk_value: int,
        type_hint: str,
    ) -> list[WorkRelationLink]:
        if not self._has_table(table) or not self._has_column(table, fk_column):
            return []
        try:
            rows = list(self.db.search(table=table, column=fk_column, search_term=int(fk_value)))
        except Exception:
            return []
        return [
            WorkRelationLink(
                target=row,
                type=type_hint,
                extra={"source_entity_type": table},
            )
            for row in rows
            if row is not None
        ]

    def _collect_identifier_rows(
        self,
        *,
        work_rows: list[Row],
        expression_rows: list[Row],
        manifestation_rows: list[Row],
        item_rows: list[Row],
    ) -> list[WorkRelationLink]:
        links: list[WorkRelationLink] = []
        if self._has_table("item_identifiers") and self._has_column(
            "item_identifiers",
            "item_identifier_item_id",
        ):
            for item_row in item_rows:
                if item_row.row_id is None:
                    continue
                try:
                    rows = list(
                        self.db.search(
                            "item_identifiers",
                            "item_identifier_item_id",
                            int(item_row.row_id),
                        )
                    )
                except Exception:
                    continue
                for row in rows:
                    links.append(
                        WorkRelationLink(
                            target=row,
                            type="item_identifier",
                            extra={"source_entity_type": "item"},
                        )
                    )

        if self._has_table("entity_identifiers") and self._has_column(
            "entity_identifiers",
            "entity_identifier_entity_type",
        ):
            entity_targets: list[tuple[str, int]] = []
            entity_targets.extend(
                ("work", int(row.row_id)) for row in work_rows if row.row_id is not None
            )
            entity_targets.extend(
                ("expression", int(row.row_id))
                for row in expression_rows
                if row.row_id is not None
            )
            entity_targets.extend(
                ("manifestation", int(row.row_id))
                for row in manifestation_rows
                if row.row_id is not None
            )
            entity_targets.extend(
                ("item", int(row.row_id)) for row in item_rows if row.row_id is not None
            )
            for entity_type, entity_id in entity_targets:
                try:
                    rows = list(
                        self.db.search(
                            "entity_identifiers",
                            "entity_identifier_entity_id",
                            entity_id,
                        )
                    )
                except Exception:
                    continue
                for row in rows:
                    mapping = row.row_dict if isinstance(row, Row) else dict(row)
                    if str(mapping.get("entity_identifier_entity_type")) != entity_type:
                        continue
                    links.append(
                        WorkRelationLink(
                            target=row,
                            type="entity_identifier",
                            origin=mapping.get("entity_identifier_provenance"),
                            extra={
                                "source_entity_type": entity_type,
                                "is_primary": _boolish_to_bool(
                                    mapping.get("entity_identifier_is_primary"),
                                ),
                            },
                        )
                    )
        return links

    def _hydrate_folders(
        self,
        container: WorkMetadataContainer,
        *,
        work_rows: list[Row],
    ) -> None:
        folder_rows: list[Row] = []

        for relation in ("files", "images"):
            for link in container.get_relation_links(relation):
                target = link.target
                if not isinstance(target, Row):
                    continue
                mapping = target.row_dict
                folder_id = mapping.get("file_folder_id") or mapping.get("image_folder_id")
                if folder_id not in (None, "") and self._has_table("folders"):
                    folder = self.db.get_row_from_id("folders", int(folder_id))
                    if folder is not None:
                        folder_rows.append(folder)

        for work_row in work_rows:
            if self._has_table("folders"):
                folder_rows.extend(
                    [
                        link.target
                        for link in self._collect_interlinks_from_row(
                            work_row,
                            secondary_table="folders",
                            source_entity_type="work",
                        )
                        if isinstance(link.target, Row)
                    ]
                )

        for folder_row in self._dedupe_rows(folder_rows):
            self._ensure_row_link(
                container,
                "folders",
                folder_row,
                type_hint="resolved_folder",
                source_entity_type="folder",
            )


__all__ = ["WorkMetadataHydrator"]

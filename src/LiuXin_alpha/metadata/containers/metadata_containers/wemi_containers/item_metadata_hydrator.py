"""
Hydrate item metadata and its WEMI, relation and managed-storage context.

The hydrator adapts a caller-owned read source, combines rows and graph hints, and
builds editable bundles without writing or closing the source.

Example:
    Exercise this contract with pytest::

        python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py
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
    Build item bundles from ids, items Rows or mappings with item identity hints.

    Hydration gathers parent WEMI rows, relation metadata, identifiers, direct assets
    and folder/store context. Cached schema snapshots gate optional lookups; each
    collector defines its own error handling.

    Example:
        Exercise this contract with pytest::

            python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py
    """

    def __init__(self, database: Any) -> None:
        """
        Adapt the caller source and cache its table and column snapshots.

        None raises ValueError. Table-list and column-map snapshot failures are
        independently replaced with empty collections; source adaptation errors propagate.

        Example:
            >>> ItemMetadataHydrator(None)
            Traceback (most recent call last):
            ...
            ValueError: ItemMetadataHydrator requires a database instance.


        :param database: Caller-owned database or compatible metadata read source; must not
            be None.
        :return: None.
        """
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
        """
        Resolve an items Row by integer id and hydrate its graph context.

        Missing rows raise ValueError; id conversion and source lookup errors propagate.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param item_id: Item row id converted with int before lookup.
        :return: Concrete ItemMetadata bundle.
        """
        item_row = self.db.get_row_from_id("items", int(item_id))
        if item_row is None:
            raise ValueError("No item found for id {}.".format(int(item_id)))
        return self._hydrate(item_row=item_row, source_row=item_row)

    def from_source_row(self, source_row: Mapping[str, Any] | Row) -> ItemMetadata:
        """
        Hydrate from a direct items Row, a resolvable id hint or an item-shaped mapping.

        A direct items Row is retained as the source. Otherwise item_id is looked up; a
        missing row may fall back to a mapping containing item_id or item_manifestation_id.
        Unresolvable sources raise ValueError, while read errors propagate.

        Example:
            Exercise the owning hydrator regression with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py::test_item_hydrator_uses_source_manifestation_id_when_item_mapping_lacks_one


        :param source_row: Row or mapping with item identity fields and optional WEMI graph
            hints.
        :return: Hydrated item bundle.
        """
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
        """
        Expose Row data or return a Mapping unchanged; use an empty mapping for other inputs.

        Example:
            >>> source = {'item_id': 2}
            >>> ItemMetadataHydrator._mapping_from(source) is source
            True
            >>> ItemMetadataHydrator._mapping_from(object())
            {}


        :param value: Row, mapping or unsupported object to inspect.
        :return: Live source mapping, or a new empty dictionary.
        """
        if isinstance(value, Row):
            return value.row_dict
        if isinstance(value, Mapping):
            return value
        return {}

    def _has_table(self, table: str) -> bool:
        """
        Check either cached table-name or table-column snapshot for a table.

        Example:
            Exercise the owning hydrator regression with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py::test_hydrators_tolerate_schema_snapshot_failures


        :param table: Exact schema table name to check.
        :return: True when either snapshot contains the table name.
        """
        return table in self._tables or table in self._tables_and_columns

    def _has_column(self, table: str, column: str) -> bool:
        """
        Check the cached column collection for a table without refreshing schema.

        Example:
            Exercise the owning hydrator regression with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py::test_item_hydrator_direct_fk_and_identifier_exception_paths


        :param table: Exact schema table name.
        :param column: Exact column name to find.
        :return: True when the column occurs in the cached table columns.
        """
        return column in set(self._tables_and_columns.get(table, []))

    def _looks_like_item_mapping(self, value: Mapping[str, Any] | Row | Any) -> bool:
        """
        Recognize item identity by key presence in a nonempty Row or mapping.

        Either item_id or item_manifestation_id qualifies even when its value is None.

        Example:
            Exercise the owning hydrator regression with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py::test_level_hydrators_accept_mapping_only_identity_payloads


        :param value: Row, mapping or unsupported object to inspect.
        :return: True for an item-shaped mapping, otherwise False.
        """
        mapping = self._mapping_from(value)
        return bool(mapping) and ("item_manifestation_id" in mapping or "item_id" in mapping)

    @staticmethod
    def _extract_known_ids(source_row: Mapping[str, Any] | Row | Any) -> dict[str, Optional[int]]:
        """
        Extract integer item and WEMI hints using truthy column-name fallbacks.

        Item uses item_id. Manifestation tries manifestation_id, item_manifestation_id then
        book_manifestation_id; expression tries expression_id then book_expression_id; work
        tries work_id, book_work_id then title_id. Failed integer conversions yield None.

        Example:
            >>> ids = ItemMetadataHydrator._extract_known_ids({'item_id': '1', 'book_manifestation_id': '2', 'title_id': 'bad'})
            >>> ids['item_id'], ids['manifestation_id'], ids['work_id']
            (1, 2, None)


        :param source_row: Row or mapping carrying graph ids; unsupported objects yield
            absent ids.
        :return: Dictionary with item_id, manifestation_id, expression_id and work_id
            entries.
        """
        mapping = ItemMetadataHydrator._mapping_from(source_row)

        def _as_int(value: Any) -> Optional[int]:
            """
            Convert a selected id hint to int, suppressing conversion exceptions.

            None and empty text are missing values. This helper is local to _extract_known_ids.

            Example:
                >>> ItemMetadataHydrator._extract_known_ids({'item_id': float('inf')})['item_id'] is None
                True


            :param value: Selected scalar id hint.
            :return: Integer id, or None when missing or conversion fails.
            """
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
        """
        Assemble item identity, WEMI graph links, metadata relations and asset/storage context.

        A resolved Row supplies identity; otherwise an item-shaped mapping does. The stored
        manifestation hint wins unless it is None. Explicit expression/work ids augment
        interlinks and are marked primary without clearing other flags. Keyed Rows are
        deduplicated and link metadata merged. Direct assets and identifiers require an item
        id; folder/store resolution runs last.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param item_row: Resolved items Row, or None to use source mapping identity fields.
        :param source_row: Original Row or mapping supplying identity and graph hints.
        :return: New concrete ItemMetadata bundle.
        """
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

        if item_row is not None:
            manifestation_links = self._collect_interlinks_from_row(
                item_row,
                secondary_table="manifestations",
                source_entity_type="item",
            )
            if manifestation_links:
                self._append_links_unique(
                    container,
                    "manifestations",
                    manifestation_links,
                )
                for link in manifestation_links:
                    if isinstance(link.target, Row):
                        manifestation_rows.append(link.target)

        manifestation_rows = self._dedupe_rows(manifestation_rows)

        explicit_expression_id = ids["expression_id"]
        if explicit_expression_id is not None:
            expression_row = self.db.get_row_from_id("expressions", int(explicit_expression_id))
            if expression_row is not None:
                expression_rows.append(expression_row)
                self._ensure_row_link(
                    container,
                    "expressions",
                    expression_row,
                    type_hint="source_expression",
                    source_entity_type="item",
                    primary=True,
                )

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
                self._ensure_row_link(
                    container,
                    "works",
                    work_row,
                    type_hint="source_work",
                    source_entity_type="item",
                    primary=True,
                )

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
        """
        Identify a concrete Row by table and integer row id.

        Non-Rows and Rows missing either component return None. Invalid row-id conversion
        propagates.

        Example:
            >>> ItemMetadataHydrator._row_key({'item_id': 2}) is None
            True


        :param row: Candidate Row whose database identity should be extracted.
        :return: Table/id tuple, or None for an unkeyed value.
        """
        if not isinstance(row, Row):
            return None
        if row.table is None or row.row_id is None:
            return None
        return (str(row.table), int(row.row_id))

    def _dedupe_rows(self, rows: Iterable[Row]) -> list[Row]:
        """
        Retain the first keyed Row for each table/id pair in input order.

        Unkeyed values are discarded and retained Rows remain shared.

        Example:
            Exercise the owning hydrator regression with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py::test_level_hydrator_row_helpers_cover_skip_and_duplicate_paths


        :param rows: Iterable of candidate Rows.
        :return: New ordered list of unique Row references.
        """
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
        """
        Replace the first matching Row-target link with a primary copy.

        Matching compares integer row ids without table names. Other primary flags remain
        unchanged. The target and metadata are retained, with a shallow copy of extra;
        invalid Row ids can raise during conversion.

        Example:
            Exercise the owning hydrator regression with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py::test_level_hydrator_row_helpers_cover_skip_and_duplicate_paths


        :param links: Mutable link list whose first matching entry should be replaced.
        :param row_id: Row id to match after integer conversion.
        :return: None.
        """
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
        """
        Append links, merging repeated keyed Row targets into the existing live bucket.

        A repeated Row key replaces its existing link with merged metadata. Unkeyed and
        non-Row targets append without deduplication. Existing duplicates are not removed;
        incoming matches use the last existing occurrence.

        Example:
            Exercise the owning hydrator regression with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py::test_level_hydrator_row_helpers_cover_skip_and_duplicate_paths


        :param container: Item bundle whose relation list is updated in place.
        :param relation: Supported relation bucket name or alias.
        :param links: Incoming links in merge order.
        :return: None.
        """
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
        existing: ItemRelationLink,
        incoming: ItemRelationLink,
    ) -> ItemRelationLink:
        """
        Merge link metadata with non-None incoming fields taking precedence.

        The existing target is retained. False, zero and empty strings count as supplied
        values. Extra mappings are shallow-merged, with incoming keys winning.

        Example:
            >>> old = ItemRelationLink(target={'work_id': 1}, primary=True, priority=4, extra={'a': 1})
            >>> new = ItemRelationLink(target={'work_id': 9}, primary=False, priority=0, extra={'b': 2})
            >>> merged = ItemMetadataHydrator._merge_link_metadata(old, new)
            >>> merged.target is old.target, merged.primary, merged.priority, merged.extra
            (True, False, 0, {'a': 1, 'b': 2})


        :param existing: Link supplying the retained target and fallback metadata.
        :param incoming: Link supplying non-None overrides and additional extra keys.
        :return: New ItemRelationLink retaining the existing target.
        """
        extra = dict(existing.extra)
        extra.update(incoming.extra)
        return ItemRelationLink(
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
        container: ItemMetadata,
        relation: str,
        row: Row,
        *,
        type_hint: str,
        source_entity_type: str,
        primary: bool | None = None,
    ) -> None:
        """
        Append a minimal link only when the Row has a key absent from the relation bucket.

        Existing matching links are left unchanged rather than merged. Unkeyed rows are
        ignored.

        Example:
            Exercise the owning hydrator regression with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py::test_level_hydrator_row_helpers_cover_skip_and_duplicate_paths


        :param container: Item bundle to update.
        :param relation: Supported relation bucket name or alias.
        :param row: Shared target Row to link.
        :param type_hint: Link type used only for a newly created link.
        :param source_entity_type: Originating WEMI level recorded in extra metadata.
        :param primary: Optional primary flag used only on insertion.
        :return: None.
        """
        key = self._row_key(row)
        if key is None:
            return
        for link in container.get_relation_links(relation):
            if self._row_key(link.target) == key:
                return
        container.get_relation_links(relation).append(
            ItemRelationLink(
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
    ) -> list[ItemRelationLink]:
        """
        Collect resolvable targets and link metadata from a source Row.

        Missing source/table or interlink-query errors return an empty list. Driver metadata
        discovers the target-id column and link prefix; failed target lookup skips that
        link. Prefix discovery failure leaves optional link fields unset. Unrecognized
        prefixed fields are retained in extra; other malformed-row or metadata errors can
        propagate.

        Example:
            Exercise the owning hydrator regression with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py::test_item_hydrator_collect_interlink_edge_paths


        :param source_row: Primary Row, or None to return no links.
        :param secondary_table: Related table whose interlinks should be read.
        :param source_entity_type: Originating WEMI level added to each link extra mapping.
        :return: New relation links in query order, without deduplication.
        """
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
        """
        Collect links for rows matching a direct foreign key.

        Missing table/column snapshots or search errors return an empty list. None rows are
        skipped, but primary is based on the original result index: only index zero is
        primary, even if it is skipped. No target type filtering occurs.

        Example:
            Exercise the owning hydrator regression with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py::test_item_hydrator_direct_fk_and_identifier_exception_paths


        :param table: Table to search.
        :param fk_column: Foreign-key column to match.
        :param fk_value: Foreign-key value converted with int inside the guarded query.
        :param type_hint: Link type assigned to each result.
        :return: New links in search order, with the table name recorded as
            source_entity_type.
        """
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
        """
        Collect item-specific and typed entity identifier links across the resolved WEMI graph.

        Item-specific results precede item, manifestation, expression and work entity
        results. Entity rows are filtered by type and carry primary/provenance metadata.
        Optional query failures are suppressed; an item-specific iterator failure can retain
        already appended links. Duplicates are left for the caller to merge.

        Example:
            Exercise the owning hydrator regression with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py::test_item_hydrator_direct_fk_and_identifier_exception_paths


        :param item_id: Item id used for item-specific and typed entity searches.
        :param work_rows: Resolved work Rows supplying typed entity ids.
        :param expression_rows: Resolved expression Rows supplying typed entity ids.
        :param manifestation_rows: Resolved manifestation Rows supplying typed entity ids.
        :return: New list of identifier links.
        """
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
        """
        Append folder/store links resolved from file, image, replica and work context.

        Only Row asset targets supply hints. Folder candidates include work interlinks;
        their folder_store_id values supply additional stores. Candidates are deduplicated
        by table/id within this call, but existing bundle links are not checked. Direct id
        conversion and lookup errors propagate.

        Example:
            Exercise the owning hydrator regression with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py::test_item_hydrator_resolves_folders_and_stores_from_files_and_work_links


        :param container: Item bundle receiving resolved folders and stores.
        :param work_rows: Resolved work Rows whose folder interlinks should be consulted.
        :return: None.
        """
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

"""
Hydrate concrete expression metadata from read-source rows and joined mappings.

The hydrator combines expression identity, WEMI context, interlink metadata and
identifier rows. It adapts a caller-owned source and does not write or close it.

Example:
    Exercise this contract with pytest::

        python -m pytest -q tests/metadata/containers/test_expression_metadata_hydrator.py
"""

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
    Build expression bundles from ids, expression Rows or mappings containing identity hints.

    Schema snapshots gate optional relation queries. Hydration combines explicit
    parent/child hints with interlinks and merges repeated Row targets; optional lookup
    failures are handled by the individual collectors.

    Example:
        Exercise this contract with pytest::

            python -m pytest -q tests/metadata/containers/test_expression_metadata_hydrator.py
    """

    def __init__(self, database: Any) -> None:
        """
        Adapt the caller source and cache its table and column snapshots.

        None raises ValueError. Table-list and column-map snapshot failures are
        independently replaced with empty collections; source adaptation errors propagate.

        Example:
            >>> ExpressionMetadataHydrator(None)
            Traceback (most recent call last):
            ...
            ValueError: ExpressionMetadataHydrator requires a database instance.


        :param database: Caller-owned database or compatible metadata read source; must not
            be None.
        :return: None.
        """
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
        """
        Resolve an expression Row by integer id and hydrate its graph context.

        A missing row raises ValueError; conversion and read-source errors propagate.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_expression_metadata_hydrator.py


        :param expression_id: Expression row id converted with int before lookup.
        :return: Concrete ExpressionMetadata bundle.
        """
        expression_row = self.db.get_row_from_id("expressions", int(expression_id))
        if expression_row is None:
            raise ValueError(f"No expression found for id {int(expression_id)}.")
        return self._hydrate(expression_row=expression_row, source_row=expression_row)

    def from_source_row(self, source_row: Mapping[str, Any] | Row) -> ExpressionMetadata:
        """
        Hydrate from an expression Row, a resolvable id hint or an expression-shaped mapping.

        Direct expression Rows are used as supplied. Otherwise the extracted id is looked
        up; a missing row can fall back to a mapping containing recognized identity keys.
        Unresolvable sources raise ValueError, while lookup exceptions propagate.

        Example:
            Exercise the owning hydrator regression with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py::test_level_hydrators_accept_mapping_only_identity_payloads


        :param source_row: Row or mapping with expression identity and optional WEMI context
            hints.
        :return: Hydrated expression bundle.
        """
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
        """
        Expose Row data or return a Mapping unchanged; use an empty mapping for other inputs.

        Example:
            >>> source = {'expression_id': 2}
            >>> ExpressionMetadataHydrator._mapping_from(source) is source
            True
            >>> ExpressionMetadataHydrator._mapping_from(object())
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

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py::test_expression_hydrator_item_and_identifier_edge_paths


        :param table: Exact schema table name.
        :param column: Exact column name to find.
        :return: True when the column occurs in the cached table columns.
        """
        return column in set(self._tables_and_columns.get(table, []))

    def _looks_like_expression_mapping(self, value: Mapping[str, Any] | Row | Any) -> bool:
        """
        Recognize a nonempty mapping by expression identity key presence.

        Any expression_id, expression_title_override, expression_label or expression_work_id
        key qualifies, even when its value is None.

        Example:
            Exercise the owning hydrator regression with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py::test_level_hydrators_accept_mapping_only_identity_payloads


        :param value: Row, mapping or other object to inspect.
        :return: True for a mapping or Row containing a recognized key.
        """
        mapping = self._mapping_from(value)
        return bool(mapping) and bool(
            {"expression_id", "expression_title_override", "expression_label", "expression_work_id"} & set(mapping)
        )

    @staticmethod
    def _extract_known_ids(source_row: Mapping[str, Any] | Row | Any) -> dict[str, Optional[int]]:
        """
        Extract integer expression, work, manifestation and item hints using truthy aliases.

        Expression uses expression_id then book_expression_id. Work uses work_id,
        expression_work_id then title_id. Manifestation uses manifestation_id,
        item_manifestation_id then book_manifestation_id. Item uses item_id. Conversion
        exceptions yield None.

        Example:
            >>> ids = ExpressionMetadataHydrator._extract_known_ids({'book_expression_id': '2', 'expression_work_id': '1', 'item_id': 'bad'})
            >>> ids['expression_id'], ids['work_id'], ids['item_id']
            (2, 1, None)


        :param source_row: Row or mapping carrying graph ids; unsupported inputs yield
            absent ids.
        :return: Dictionary with all four id keys and optional integer values.
        """
        mapping = ExpressionMetadataHydrator._mapping_from(source_row)

        def _as_int(value: Any) -> Optional[int]:
            """
            Convert a selected id hint to int, suppressing any conversion exception.

            None and empty text are missing values. This helper is local to _extract_known_ids.

            Example:
                >>> ExpressionMetadataHydrator._extract_known_ids({'item_id': float('inf')})['item_id'] is None
                True


            :param value: Selected scalar id hint.
            :return: Integer id, or None when absent or conversion fails.
            """
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
        """
        Assemble expression identity, WEMI context and related metadata from source hints and rows.

        A resolved Row supplies identity fields; otherwise recognized mapping fields do.
        Explicit work, manifestation and item hints augment expression interlinks and
        manifestation-to-item lookup. Metadata relations are collected from all resolved
        levels, then identifier rows are added. Repeated Row targets merge link metadata;
        non-Row targets are retained separately.

        Example:
            Exercise the owning hydrator regression with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py::test_expression_hydrator_uses_explicit_manifestation_and_item_ids_from_mapping


        :param expression_row: Resolved expression Row, or None to construct identity from
            the source mapping.
        :param source_row: Original Row or mapping supplying identity fields and graph
            hints.
        :return: New concrete expression metadata bundle.
        """
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
        """
        Identify a concrete Row by table and integer row id.

        Non-Rows and Rows missing either component return None. Invalid row-id conversion
        propagates.

        Example:
            >>> ExpressionMetadataHydrator._row_key({'expression_id': 2}) is None
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

    def _append_links_unique(
        self,
        container: ExpressionMetadata,
        relation: str,
        links: Iterable[ExpressionRelationLink],
    ) -> None:
        """
        Append links, merging repeated keyed Row targets into the existing live bucket.

        A repeated Row key replaces its existing link with merged metadata. Unkeyed and
        non-Row targets append without deduplication. Existing duplicates are not removed;
        incoming matches use the last existing occurrence.

        Example:
            Exercise the owning hydrator regression with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py::test_level_hydrator_row_helpers_cover_skip_and_duplicate_paths


        :param container: Expression bundle whose relation list is updated in place.
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
        existing: ExpressionRelationLink,
        incoming: ExpressionRelationLink,
    ) -> ExpressionRelationLink:
        """
        Merge link metadata with non-None incoming fields taking precedence.

        The existing target is retained. False, zero and empty strings count as supplied
        values. Extra mappings are shallow-merged, with incoming keys winning.

        Example:
            >>> old = ExpressionRelationLink(target={'work_id': 1}, primary=True, priority=4, extra={'a': 1})
            >>> new = ExpressionRelationLink(target={'work_id': 9}, primary=False, priority=0, extra={'b': 2})
            >>> merged = ExpressionMetadataHydrator._merge_link_metadata(old, new)
            >>> merged.target is old.target, merged.primary, merged.priority, merged.extra
            (True, False, 0, {'a': 1, 'b': 2})


        :param existing: Link supplying the retained target and fallback metadata.
        :param incoming: Link supplying non-None overrides and additional extra keys.
        :return: New ExpressionRelationLink retaining the existing target.
        """
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
        """
        Append a minimal link only when the Row has a key absent from the relation bucket.

        Existing matching links are left unchanged rather than merged. Unkeyed rows are
        ignored.

        Example:
            Exercise the owning hydrator regression with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py::test_level_hydrator_row_helpers_cover_skip_and_duplicate_paths


        :param container: Expression bundle to update.
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
        """
        Collect resolvable targets and link metadata from a source Row.

        Missing source/table or interlink-query errors return an empty list. Driver metadata
        discovers the target-id column and link prefix; failed target lookup skips that
        link. Prefix discovery failure leaves optional link fields unset. Unrecognized
        prefixed fields are retained in extra; other malformed-row or metadata errors can
        propagate.

        Example:
            Exercise the owning hydrator regression with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py::test_expression_hydrator_collect_interlink_edge_paths


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
        """
        Find items whose direct manifestation foreign key matches the given Row.

        Missing schema, an absent manifestation id or a search failure produces an empty
        list.

        Example:
            Exercise the owning hydrator regression with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py::test_expression_hydrator_item_and_identifier_edge_paths


        :param manifestation_row: Manifestation Row supplying the parent id.
        :return: List of search-result Rows in source order.
        """
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
        """
        Gather item-specific and typed entity identifier links for the resolved WEMI rows.

        Item-specific results come first, followed by expression, work, manifestation and
        item entity results. Entity identifiers are filtered by entity type and retain
        primary/provenance metadata. Missing optional schema and individual search failures
        are skipped; duplicates are left for the caller to merge.

        Example:
            Exercise the owning hydrator regression with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py::test_expression_hydrator_item_and_identifier_edge_paths


        :param expression_rows: Resolved expression Rows used for typed entity-identifier
            searches.
        :param work_rows: Resolved work Rows used for typed entity-identifier searches.
        :param manifestation_rows: Resolved manifestation Rows used for typed
            entity-identifier searches.
        :param item_rows: Resolved item Rows used for item-specific and typed entity
            searches.
        :return: New list of identifier relation links.
        """
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

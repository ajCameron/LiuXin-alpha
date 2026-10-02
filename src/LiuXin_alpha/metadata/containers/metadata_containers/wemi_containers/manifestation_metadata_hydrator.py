"""
Hydrate manifestation metadata with related WEMI rows, descriptive links and assets.

The hydrator adapts a caller-owned read source and builds editable bundles without
writing or closing that source. Schema snapshots gate optional relation queries.

Example:
    Exercise this contract with pytest::

        python -m pytest -q tests/metadata/containers/test_manifestation_metadata_hydrator.py
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Iterable, Optional

from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.manifestation_containers.manifestation_metadata_api import (
    ManifestationRelationLink,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_container import (
    ManifestationIdentity,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_container import (
    ManifestationMetadata,
)
from LiuXin_alpha.metadata.read_sources import metadata_read_source_from
from LiuXin_alpha.utils.adaptors import _boolish_to_bool


class ManifestationMetadataHydrator:
    """
    Build manifestation bundles from ids, manifestations Rows or identity-shaped mappings.

    Hydration follows expression, work and item context, gathers descriptive metadata
    and identifiers, and resolves linked assets and replicas. Optional collectors
    suppress some query errors; direct row lookups can still raise.

    Example:
        Exercise this contract with pytest::

            python -m pytest -q tests/metadata/containers/test_manifestation_metadata_hydrator.py
    """

    def __init__(self, database: Any) -> None:
        """
        Adapt the caller source and cache its table and column snapshots.

        None raises ValueError. Table-list and column-map snapshot failures are
        independently replaced with empty collections; source adaptation errors propagate.

        Example:
            >>> ManifestationMetadataHydrator(None)
            Traceback (most recent call last):
            ...
            ValueError: ManifestationMetadataHydrator requires a database instance.


        :param database: Caller-owned database or compatible metadata read source; must not
            be None.
        :return: None.
        """
        if database is None:
            raise ValueError("ManifestationMetadataHydrator requires a database instance.")
        self.db = metadata_read_source_from(database)
        try:
            self._tables = set(self.db.get_tables(force_refresh=False))
        except Exception:
            self._tables = set()
        try:
            self._tables_and_columns = dict(self.db.get_tables_and_columns())
        except Exception:
            self._tables_and_columns = {}

    def from_manifestation_id(self, manifestation_id: int) -> ManifestationMetadata:
        """
        Resolve a manifestations Row by integer id and hydrate its graph context.

        A missing row raises ValueError; id conversion and source lookup errors propagate.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_manifestation_metadata_hydrator.py


        :param manifestation_id: Manifestation row id converted with int before lookup.
        :return: Concrete ManifestationMetadata bundle.
        """
        manifestation_row = self.db.get_row_from_id("manifestations", int(manifestation_id))
        if manifestation_row is None:
            raise ValueError(f"No manifestation found for id {int(manifestation_id)}.")
        return self._hydrate(manifestation_row=manifestation_row, source_row=manifestation_row)

    def from_source_row(self, source_row: Mapping[str, Any] | Row) -> ManifestationMetadata:
        """
        Hydrate a direct manifestations Row, a resolvable id hint or a manifestation-shaped mapping.

        Direct Rows take precedence. Otherwise a manifestation id is looked up, with a
        mapping fallback when the row is absent. A recognized key can qualify even with a
        None value. Unresolvable inputs raise ValueError; lookup errors propagate.

        Example:
            Exercise the owning hydrator regression with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py::test_manifestation_hydrator_uses_explicit_work_and_item_ids_from_mapping


        :param source_row: Row or mapping with manifestation identity fields and optional
            expression, work or item hints.
        :return: Hydrated manifestation bundle.
        """
        ids = self._extract_known_ids(source_row)
        manifestation_row = None
        if isinstance(source_row, Row) and source_row.table == "manifestations":
            manifestation_row = source_row
        elif ids["manifestation_id"] is not None:
            manifestation_row = self.db.get_row_from_id("manifestations", int(ids["manifestation_id"]))

        if manifestation_row is None and self._looks_like_manifestation_mapping(source_row):
            return self._hydrate(manifestation_row=None, source_row=source_row)
        if manifestation_row is None:
            raise ValueError("Could not resolve a manifestation row from the supplied source row/view.")
        return self._hydrate(manifestation_row=manifestation_row, source_row=source_row)

    @staticmethod
    def _mapping_from(value: Mapping[str, Any] | Row | Any) -> Mapping[str, Any]:
        """
        Expose Row data or return a Mapping unchanged; use an empty mapping for other inputs.

        Example:
            >>> source = {'manifestation_id': 2}
            >>> ManifestationMetadataHydrator._mapping_from(source) is source
            True
            >>> ManifestationMetadataHydrator._mapping_from(object())
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

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py::test_manifestation_hydrator_direct_fk_item_and_identifier_edge_paths


        :param table: Exact schema table name.
        :param column: Exact column name to find.
        :return: True when the column occurs in the cached table columns.
        """
        return column in set(self._tables_and_columns.get(table, []))

    def _looks_like_manifestation_mapping(self, value: Mapping[str, Any] | Row | Any) -> bool:
        """
        Recognize manifestation identity by key presence in a nonempty Row or mapping.

        Any of manifestation_id, manifestation_format_detail, manifestation_carrier_type or
        manifestation_expression_id qualifies regardless of its value.

        Example:
            Exercise the owning hydrator regression with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py::test_level_hydrators_accept_mapping_only_identity_payloads


        :param value: Row, mapping or unsupported value to inspect.
        :return: True for a recognized identity mapping, otherwise False.
        """
        mapping = self._mapping_from(value)
        return bool(mapping) and bool(
            {"manifestation_id", "manifestation_format_detail", "manifestation_carrier_type", "manifestation_expression_id"} & set(mapping)
        )

    @staticmethod
    def _extract_known_ids(source_row: Mapping[str, Any] | Row | Any) -> dict[str, Optional[int]]:
        """
        Extract manifestation, expression, work and item integer hints using truthy alias fallbacks.

        Manifestation tries manifestation_id, book_manifestation_id then
        item_manifestation_id. Expression tries expression_id, manifestation_expression_id
        then book_expression_id. Work tries work_id then title_id; item uses item_id.
        Invalid conversions become None.

        Example:
            >>> ids = ManifestationMetadataHydrator._extract_known_ids({'book_manifestation_id': '7', 'manifestation_expression_id': '8', 'title_id': 'bad'})
            >>> ids['manifestation_id'], ids['expression_id'], ids['work_id']
            (7, 8, None)


        :param source_row: Row or mapping supplying graph hints; unsupported values have no
            hints.
        :return: Dictionary containing all four id keys, with None for absent or invalid
            values.
        """
        mapping = ManifestationMetadataHydrator._mapping_from(source_row)

        def _as_int(value: Any) -> Optional[int]:
            """
            Convert a selected id hint to int while suppressing conversion exceptions.

            None and empty text are missing values. This helper is local to _extract_known_ids.

            Example:
                >>> ManifestationMetadataHydrator._extract_known_ids({'item_id': float('inf')})['item_id'] is None
                True


            :param value: Scalar hint chosen by the outer helper.
            :return: Integer id, or None for missing or invalid hints.
            """
            if value in (None, ""):
                return None
            try:
                return int(value)
            except Exception:
                return None

        return {
            "manifestation_id": _as_int(mapping.get("manifestation_id") or mapping.get("book_manifestation_id") or mapping.get("item_manifestation_id")),
            "expression_id": _as_int(mapping.get("expression_id") or mapping.get("manifestation_expression_id") or mapping.get("book_expression_id")),
            "work_id": _as_int(mapping.get("work_id") or mapping.get("title_id")),
            "item_id": _as_int(mapping.get("item_id")),
        }

    def _hydrate(
        self,
        *,
        manifestation_row: Optional[Row],
        source_row: Mapping[str, Any] | Row,
    ) -> ManifestationMetadata:
        """
        Assemble manifestation identity, WEMI context, descriptive links, assets and identifiers.

        A resolved Row supplies identity before source mapping fields. Explicit expression
        hints take precedence over the stored identity hint; the direct expression link is
        primary. Work hints and expression interlinks supply works; item hints and
        manifestation foreign keys supply items. Repeated keyed Row links merge non-None
        incoming metadata. Mapping-only identities do not supply a manifestation Row for
        interlink queries. Direct lookups and malformed ids can raise.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_manifestation_metadata_hydrator.py


        :param manifestation_row: Resolved manifestations Row, or None to build identity
            from the source mapping.
        :param source_row: Original Row or mapping supplying identity and graph hints.
        :return: New concrete ManifestationMetadata bundle.
        """
        ids = self._extract_known_ids(source_row)
        source_map = self._mapping_from(source_row)

        manifestation_container = None
        if manifestation_row is not None:
            manifestation_container = ManifestationIdentity.from_mapping(manifestation_row.row_dict)
        elif self._looks_like_manifestation_mapping(source_map):
            payload = dict(source_map)
            if payload.get("manifestation_id") in (None, "") and ids["manifestation_id"] is not None:
                payload["manifestation_id"] = ids["manifestation_id"]
            manifestation_container = ManifestationIdentity.from_mapping(payload)

        container = ManifestationMetadata(manifestation=manifestation_container)

        manifestation_rows: list[Row] = []
        expression_rows: list[Row] = []
        work_rows: list[Row] = []
        item_rows: list[Row] = []

        if manifestation_row is not None:
            manifestation_rows.append(manifestation_row)

        expression_id = ids["expression_id"]
        if expression_id is None and manifestation_container is not None:
            expression_id = manifestation_container.manifestation_expression_id
        if expression_id is not None:
            expression_row = self.db.get_row_from_id("expressions", int(expression_id))
            if expression_row is not None:
                expression_rows.append(expression_row)
                self._ensure_row_link(
                    container,
                    "expressions",
                    expression_row,
                    type_hint="parent_expression",
                    source_entity_type="manifestation",
                    primary=True,
                )

        if manifestation_row is not None:
            expression_links = self._collect_interlinks_from_row(
                manifestation_row,
                secondary_table="expressions",
                source_entity_type="manifestation",
            )
            if expression_links:
                self._append_links_unique(container, "expressions", expression_links)
                for link in expression_links:
                    if isinstance(link.target, Row):
                        expression_rows.append(link.target)

        work_id = ids["work_id"]
        if work_id is not None:
            work_row = self.db.get_row_from_id("works", int(work_id))
            if work_row is not None:
                work_rows.append(work_row)

        for expression_row in expression_rows:
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

        work_rows = self._dedupe_rows(work_rows)
        for work_row in work_rows:
            self._ensure_row_link(
                container,
                "works",
                work_row,
                type_hint="expression_work",
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
        relation_source_rows.extend(manifestation_rows)
        relation_source_rows.extend(expression_rows)
        relation_source_rows.extend(work_rows)
        relation_source_rows.extend(item_rows)

        multi_source_relations = (
            "agents",
            "titles",
            "genres",
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

        for source in manifestation_rows:
            self._append_links_unique(container, "files", self._collect_interlinks_from_row(source, secondary_table="files", source_entity_type="manifestation"))
            self._append_links_unique(container, "images", self._collect_interlinks_from_row(source, secondary_table="images", source_entity_type="manifestation"))
            self._append_links_unique(container, "digital_assets", self._collect_interlinks_from_row(source, secondary_table="digital_assets", source_entity_type="manifestation"))

        for item_row in item_rows:
            if item_row.row_id is None:
                continue
            self._append_links_unique(container, "files", self._collect_direct_fk_rows(table="files", fk_column="file_item_id", fk_value=int(item_row.row_id), type_hint="item_file"))
            self._append_links_unique(container, "images", self._collect_direct_fk_rows(table="images", fk_column="image_item_id", fk_value=int(item_row.row_id), type_hint="item_image"))
            self._append_links_unique(container, "digital_assets", self._collect_interlinks_from_row(item_row, secondary_table="digital_assets", source_entity_type="item"))

        for digital_asset_link in container.get_relation_links("digital_assets"):
            target = digital_asset_link.target
            if not isinstance(target, Row):
                continue
            if target.row_id is None:
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

        self._append_links_unique(
            container,
            "identifiers",
            self._collect_identifier_rows(
                manifestation_rows=manifestation_rows,
                expression_rows=expression_rows,
                work_rows=work_rows,
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
            >>> ManifestationMetadataHydrator._row_key({'manifestation_id': 2}) is None
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
        container: ManifestationMetadata,
        relation: str,
        links: Iterable[ManifestationRelationLink],
    ) -> None:
        """
        Append links, merging repeated keyed Row targets into the existing live bucket.

        A repeated Row key replaces its existing link with merged metadata. Unkeyed and
        non-Row targets append without deduplication. Existing duplicates are not removed;
        incoming matches use the last existing occurrence.

        Example:
            Exercise the owning hydrator regression with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py::test_level_hydrator_row_helpers_cover_skip_and_duplicate_paths


        :param container: Manifestation bundle whose relation list is updated in place.
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
                existing[existing_index] = self._merge_link_metadata(existing[existing_index], link)
                continue
            existing.append(link)
            if key is not None:
                seen_rows[key] = len(existing) - 1

    @staticmethod
    def _merge_link_metadata(
        existing: ManifestationRelationLink,
        incoming: ManifestationRelationLink,
    ) -> ManifestationRelationLink:
        """
        Merge link metadata with non-None incoming fields taking precedence.

        The existing target is retained. False, zero and empty strings count as supplied
        values. Extra mappings are shallow-merged, with incoming keys winning.

        Example:
            >>> old = ManifestationRelationLink(target={'work_id': 1}, primary=True, priority=4, extra={'a': 1})
            >>> new = ManifestationRelationLink(target={'work_id': 9}, primary=False, priority=0, extra={'b': 2})
            >>> merged = ManifestationMetadataHydrator._merge_link_metadata(old, new)
            >>> merged.target is old.target, merged.primary, merged.priority, merged.extra
            (True, False, 0, {'a': 1, 'b': 2})


        :param existing: Link supplying the retained target and fallback metadata.
        :param incoming: Link supplying non-None overrides and additional extra keys.
        :return: New ManifestationRelationLink retaining the existing target.
        """
        extra = dict(existing.extra)
        extra.update(incoming.extra)
        return ManifestationRelationLink(
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
        container: ManifestationMetadata,
        relation: str,
        row: Row,
        *,
        type_hint: str,
        source_entity_type: str,
        primary: bool | None = None,
    ) -> None:
        """
        Append a typed Row link only when its table/id key is absent.

        Rows without a usable key are ignored. Existing links with that key are left
        unchanged, including their metadata. New links receive the requested primary flag
        and source entity hint; no source lookup occurs.

        Example:
            Exercise the owning hydrator regression with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py::test_level_hydrator_row_helpers_cover_skip_and_duplicate_paths


        :param container: Bundle whose live relation list receives the link.
        :param relation: Relation name accepted by the bundle.
        :param row: Row target with a table and integer-convertible id.
        :param type_hint: Type assigned to a new link.
        :param source_entity_type: Source level stored in the extra mapping.
        :param primary: Primary flag for a newly appended link, or None.
        :return: None.
        """
        key = self._row_key(row)
        if key is None:
            return
        for link in container.get_relation_links(relation):
            if self._row_key(link.target) == key:
                return
        container.get_relation_links(relation).append(
            ManifestationRelationLink(
                target=row,
                priority=None,
                primary=primary,
                type=type_hint,
                origin=None,
                policy=None,
                data=None,
                extra={"source_entity_type": source_entity_type},
            )
        )

    def _collect_interlinks_from_row(
        self,
        source_row: Optional[Row],
        *,
        secondary_table: str,
        source_entity_type: str,
    ) -> list[ManifestationRelationLink]:
        """
        Resolve interlink targets and copy link metadata into manifestation relation links.

        Missing source rows, absent tables and query/iterator errors return an empty list.
        Schema naming failures are tolerated; unresolved target ids are skipped. Known
        metadata fields are copied and other prefixed fields become extra entries. Primary
        uses bool-like conversion. Invalid link mappings or later driver column lookups can
        still raise.

        Example:
            Exercise the owning hydrator regression with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py::test_manifestation_hydrator_collect_interlink_edge_paths


        :param source_row: Source Row, or None for no links.
        :param secondary_table: Related target table to resolve through interlinks.
        :param source_entity_type: Source level recorded in each extra mapping.
        :return: New resolved links in interlink query order, without deduplication.
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

        out: list[ManifestationRelationLink] = []
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
                        "source",
                        "policy",
                        "data",
                        "primary",
                        "index",
                        "id",
                    }:
                        continue
                    extra[suffix] = value
            out.append(
                ManifestationRelationLink(
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
        Find item rows whose item_manifestation_id matches a manifestation Row id.

        Missing schema or row ids return an empty list. Integer conversion, search and
        iterator failures are suppressed; successful results retain their order without
        filtering or deduplication.

        Example:
            Exercise the owning hydrator regression with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py::test_manifestation_hydrator_direct_fk_item_and_identifier_edge_paths


        :param manifestation_row: Manifestation Row whose id selects child items.
        :return: Materialized query results, or an empty list on unsupported or failed
            lookup.
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

    def _collect_direct_fk_rows(
        self,
        *,
        table: str,
        fk_column: str,
        fk_value: int,
        type_hint: str,
    ) -> list[ManifestationRelationLink]:
        """
        Wrap non-None rows matching a direct foreign key in manifestation relation links.

        Missing table/column snapshots, integer conversion errors and query/iterator
        failures yield an empty list. Search order is preserved; no target type check,
        deduplication or primary assignment occurs.

        Example:
            Exercise the owning hydrator regression with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py::test_manifestation_hydrator_direct_fk_item_and_identifier_edge_paths


        :param table: Table to search.
        :param fk_column: Foreign-key column to match.
        :param fk_value: Foreign-key value converted with int inside the guarded query.
        :param type_hint: Type assigned to each result link.
        :return: New links with the requested type and table name as source_entity_type.
        """
        if not self._has_table(table) or not self._has_column(table, fk_column):
            return []
        try:
            rows = list(self.db.search(table=table, column=fk_column, search_term=int(fk_value)))
        except Exception:
            return []
        return [
            ManifestationRelationLink(
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
        manifestation_rows: list[Row],
        expression_rows: list[Row],
        work_rows: list[Row],
        item_rows: list[Row],
    ) -> list[ManifestationRelationLink]:
        """
        Collect item-specific identifiers followed by typed identifiers from WEMI context.

        Entity query order is manifestation, expression, work then item. Entity rows are
        filtered by entity type and carry primary/provenance metadata. Each query is
        materialized within its error guard, so iterator failures discard that query.
        Invalid outer row ids and malformed result mappings can still raise. Duplicates are
        left for the caller to merge.

        Example:
            Exercise the owning hydrator regression with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py::test_manifestation_hydrator_direct_fk_item_and_identifier_edge_paths


        :param manifestation_rows: Manifestation Rows supplying entity ids.
        :param expression_rows: Expression Rows supplying entity ids.
        :param work_rows: Work Rows supplying entity ids.
        :param item_rows: Item Rows supplying item-specific and typed entity ids.
        :return: New identifier links in query order.
        """
        links: list[ManifestationRelationLink] = []
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
                        ManifestationRelationLink(
                            target=row,
                            type="item_identifier",
                            extra={"source_entity_type": "item"},
                        )
                    )

        if self._has_table("entity_identifiers") and self._has_column("entity_identifiers", "entity_identifier_entity_type"):
            entity_targets: list[tuple[str, int]] = []
            entity_targets.extend(("manifestation", int(row.row_id)) for row in manifestation_rows if row.row_id is not None)
            entity_targets.extend(("expression", int(row.row_id)) for row in expression_rows if row.row_id is not None)
            entity_targets.extend(("work", int(row.row_id)) for row in work_rows if row.row_id is not None)
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
                        ManifestationRelationLink(
                            target=row,
                            primary=_boolish_to_bool(mapping.get("entity_identifier_is_primary")),
                            type="entity_identifier",
                            origin=mapping.get("entity_identifier_provenance"),
                            extra={"source_entity_type": entity_type},
                        )
                    )
        return links


__all__ = ["ManifestationMetadataHydrator"]

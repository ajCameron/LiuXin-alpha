"""
Hydrate editable work metadata from WEMI rows and mapping hints.

A caller-owned read source supplies identities, descriptive links, identifiers and
assets. Hydration performs reads only; optional collectors tolerate some schema and
query failures.

Example:
    Exercise this contract with pytest::

        python -m pytest -q tests/metadata/containers/test_work_metadata_hydrator.py
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Iterable, Optional

from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.work_containers.work_metadata_api import (
    WorkRelationLink,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import (
    WorkIdentity,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import (
    WorkMetadata,
)
from LiuXin_alpha.metadata.read_sources import metadata_read_source_from
from LiuXin_alpha.utils.adaptors import _boolish_to_bool


class WorkMetadataHydrator:
    """
    Build work bundles from ids, works Rows or work-shaped mappings.

    Explicit descendant hints augment traversal through expression, manifestation and
    item rows. Duplicate keyed Row links retain the first link and its metadata; unkeyed
    targets remain separate. Optional collectors suppress some failures, while direct
    lookups can raise.

    Example:
        Exercise this contract with pytest::

            python -m pytest -q tests/metadata/containers/test_work_metadata_hydrator.py
    """

    def __init__(self, database: Any) -> None:
        """
        Adapt the caller source and cache its table and column snapshots.

        None raises ValueError. Table-list and column-map snapshot failures are
        independently replaced with empty collections; source adaptation errors propagate.

        Example:
            >>> WorkMetadataHydrator(None)
            Traceback (most recent call last):
            ...
            ValueError: WorkMetadataHydrator requires a database instance.


        :param database: Caller-owned database or compatible metadata read source; must not
            be None.
        :return: None.
        """
        if database is None:
            raise ValueError("WorkMetadataHydrator requires a database instance.")
        self.db = metadata_read_source_from(database)
        try:
            self._tables = set(self.db.get_tables(force_refresh=False))
        except Exception:
            self._tables = set()
        try:
            self._tables_and_columns = dict(self.db.get_tables_and_columns())
        except Exception:
            self._tables_and_columns = {}

    def from_work_id(self, work_id: int) -> WorkMetadata:
        """
        Resolve a works Row by integer id and hydrate its context.

        Missing rows raise ValueError. Id conversion and read-source lookup errors
        propagate.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_work_metadata_hydrator.py


        :param work_id: Work row id converted with int before lookup.
        :return: New concrete WorkMetadata bundle.
        """
        work_row = self.db.get_row_from_id("works", int(work_id))
        if work_row is None:
            raise ValueError("No work found for id {}.".format(int(work_id)))
        return self._hydrate(work_row=work_row, source_row=work_row)

    def from_source_row(self, source_row: Mapping[str, Any] | Row) -> WorkMetadata:
        """
        Hydrate a direct works Row, a resolvable work id or a work-shaped mapping.

        A direct works Row takes precedence. Otherwise the extracted id is looked up, then
        recognized mappings provide a fallback if no row exists. Unresolvable inputs raise
        ValueError; lookup failures propagate.

        Example:
            Exercise the owning hydrator regression with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py::test_level_hydrators_accept_mapping_only_identity_payloads


        :param source_row: Row or mapping containing work identity fields and optional
            descendant hints.
        :return: New concrete WorkMetadata bundle.
        """
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
        """
        Expose Row data or return a Mapping unchanged; use an empty mapping for other inputs.

        Example:
            >>> source = {'manifestation_id': 2}
            >>> WorkMetadataHydrator._mapping_from(source) is source
            True
            >>> WorkMetadataHydrator._mapping_from(object())
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

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py::test_work_hydrator_direct_fk_item_and_identifier_edge_paths


        :param table: Exact schema table name.
        :param column: Exact column name to find.
        :return: True when the column occurs in the cached table columns.
        """
        return column in set(self._tables_and_columns.get(table, []))

    def _looks_like_work_mapping(self, value: Mapping[str, Any] | Row | Any) -> bool:
        """
        Recognize a nonempty Row or mapping by work-related key presence.

        Any of work_id, title_id, work_title or work_canonical_title qualifies, regardless
        of its value.

        Example:
            Exercise the owning hydrator regression with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py::test_level_hydrators_accept_mapping_only_identity_payloads


        :param value: Row, mapping or unsupported value to inspect.
        :return: True for a recognized mapping, otherwise False.
        """
        mapping = self._mapping_from(value)
        return bool(mapping) and bool(
            {"work_id", "title_id", "work_title", "work_canonical_title"} & set(mapping)
        )

    @staticmethod
    def _extract_known_ids(source_row: Mapping[str, Any] | Row | Any) -> dict[str, Optional[int]]:
        """
        Read WEMI id hints using truthy aliases before integer conversion.

        Work tries work_id then title_id; expression tries expression_id then
        book_expression_id. Manifestation tries manifestation_id, item_manifestation_id then
        book_manifestation_id. Item uses item_id alone. Invalid hints become None.

        Example:
            >>> ids = WorkMetadataHydrator._extract_known_ids({'title_id': '3', 'item_manifestation_id': '7', 'item_id': 'bad'})
            >>> ids['work_id'], ids['manifestation_id'], ids['item_id']
            (3, 7, None)


        :param source_row: Row or mapping supplying graph hints; unsupported values have no
            hints.
        :return: Dictionary of all four id keys with integer or None values.
        """
        mapping = WorkMetadataHydrator._mapping_from(source_row)

        def _as_int(value: Any) -> Optional[int]:
            """
            Convert a selected hint to int, suppressing conversion exceptions.

            None and empty text mean missing. This helper is local to _extract_known_ids.

            Example:
                >>> WorkMetadataHydrator._extract_known_ids({'item_id': float('inf')})['item_id'] is None
                True


            :param value: Scalar hint selected by the outer helper.
            :return: Integer id, or None for absent or invalid hints.
            """
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
    ) -> WorkMetadata:
        """
        Assemble work identity, descendant rows, descriptive links, assets and identifiers.

        A resolved work Row supplies identity ahead of mapping fields. Otherwise a
        recognized mapping supplies identity and a missing work_id can use the title_id
        hint. Explicit descendant hints augment work/expression interlinks and
        manifestation-to-item foreign keys. Sources contribute agents, genres, subjects,
        series, tags, labels, languages, ratings, notes, comments, synopses, images and
        folders; title relations are not collected here. Item files/images, identifiers and
        resolved folders follow. Duplicate keyed Row links keep their first metadata. Direct
        lookups and malformed ids can raise.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_work_metadata_hydrator.py


        :param work_row: Resolved works Row, or None for mapping-only identity.
        :param source_row: Original Row or mapping carrying identity and graph hints.
        :return: New concrete WorkMetadata bundle.
        """
        ids = self._extract_known_ids(source_row)
        source_map = self._mapping_from(source_row)

        work_container = None
        if work_row is not None:
            work_container = WorkIdentity.from_mapping(work_row.row_dict)
        elif self._looks_like_work_mapping(source_map):
            payload = dict(source_map)
            if payload.get("work_id") in (None, "") and ids["work_id"] is not None:
                payload["work_id"] = ids["work_id"]
            work_container = WorkIdentity.from_mapping(payload)

        container = WorkMetadata(work=work_container)

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
        """
        Identify a concrete Row by table and integer row id.

        Non-Rows and Rows missing either component return None. Invalid row-id conversion
        propagates.

        Example:
            >>> WorkMetadataHydrator._row_key({'manifestation_id': 2}) is None
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
        container: WorkMetadata,
        relation: str,
        links: Iterable[WorkRelationLink],
    ) -> None:
        """
        Append incoming links unless their keyed Row target already exists.

        A duplicate Row key is skipped entirely, leaving the first link metadata unchanged.
        Non-Row and unkeyed targets always append. The live bucket is mutated directly,
        bypassing setter validation; incoming link objects remain shared.

        Example:
            Exercise the owning hydrator regression with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py::test_level_hydrator_row_helpers_cover_skip_and_duplicate_paths


        :param container: Bundle whose live relation list is extended.
        :param relation: Supported relation name or alias.
        :param links: Iterable of links to append in order.
        :return: None.
        """
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
        container: WorkMetadata,
        relation: str,
        row: Row,
        *,
        type_hint: str,
        source_entity_type: str,
    ) -> None:
        """
        Add a typed Row link only when its table/id key is absent.

        Unkeyed rows are ignored and existing links remain unchanged. A new WorkRelationLink
        records the source level in extra and is added through the bundle's validated
        helper. No lookup or primary assignment occurs.

        Example:
            Exercise the owning hydrator regression with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py::test_level_hydrator_row_helpers_cover_skip_and_duplicate_paths


        :param container: Bundle receiving the new link.
        :param relation: Supported relation name or alias.
        :param row: Row target with a usable table/id key.
        :param type_hint: Type for a newly created link.
        :param source_entity_type: Source level stored in the extra mapping.
        :return: None.
        """
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
        """
        Resolve interlink targets and copy link metadata into work relation links.

        Missing source rows, absent tables and query/iterator errors return an empty list.
        Schema naming failures are tolerated; unresolved target ids are skipped. Known
        metadata fields are copied and other prefixed fields become extra entries. Primary
        uses bool-like conversion. Invalid link mappings or later driver column lookups can
        still raise.

        Example:
            Exercise the owning hydrator regression with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py::test_work_hydrator_collect_interlink_edge_paths


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
                WorkRelationLink(
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

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py::test_work_hydrator_direct_fk_item_and_identifier_edge_paths


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
        """
        Wrap non-None rows matching a direct foreign key in work relation links.

        Missing table/column snapshots, integer conversion errors and query/iterator
        failures yield an empty list. Search order is preserved; no target type check,
        deduplication or primary assignment occurs.

        Example:
            Exercise the owning hydrator regression with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py::test_work_hydrator_direct_fk_item_and_identifier_edge_paths


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
        """
        Collect item-specific identifiers followed by typed WEMI identifiers.

        Typed query order is work, expression, manifestation then item. Results are filtered
        by entity type and carry primary/provenance fields. Each query is materialized
        within its guard, so iteration failure discards that query. Invalid outer ids and
        malformed result mappings can raise; duplicates remain for the caller to handle.

        Example:
            Exercise the owning hydrator regression with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py::test_work_hydrator_direct_fk_item_and_identifier_edge_paths


        :param work_rows: Work Rows supplying typed entity ids.
        :param expression_rows: Expression Rows supplying typed entity ids.
        :param manifestation_rows: Manifestation Rows supplying typed entity ids.
        :param item_rows: Item Rows supplying item-specific and typed ids.
        :return: New identifier links in query order.
        """
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
                            primary=_boolish_to_bool(mapping.get("entity_identifier_is_primary")),
                            type="entity_identifier",
                            origin=mapping.get("entity_identifier_provenance"),
                            extra={"source_entity_type": entity_type},
                        )
                    )
        return links

    def _hydrate_folders(
        self,
        container: WorkMetadata,
        *,
        work_rows: list[Row],
    ) -> None:
        """
        Resolve folder Rows from linked files/images and work interlinks.

        Only Row asset targets contribute file_folder_id or image_folder_id hints. Folder
        lookup requires the cached table; id conversion and direct lookup errors propagate.
        Candidate Rows are deduplicated before missing links are added; existing folder
        metadata remains unchanged.

        Example:
            Exercise the owning hydrator regression with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py::test_work_hydrator_item_identifier_and_folder_resolution_paths


        :param container: Bundle whose asset links supply hints and folders bucket receives
            links.
        :param work_rows: Work Rows queried for folder interlinks.
        :return: None.
        """
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

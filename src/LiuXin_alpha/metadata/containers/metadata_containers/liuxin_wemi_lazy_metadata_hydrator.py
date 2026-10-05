"""
Hydrate item metadata with eager identities and deferred relation-backed fields.

Schema snapshots and selected structural links are read during construction.
Deferred loaders retain the hydrator and caller-owned database; keep that database
usable until loading is complete.

Example:
    Exercise this contract with pytest::

        python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api import (
    ExpressionRelationLink,
    ItemRelationLink,
    ManifestationRelationLink,
    WorkRelationLink,
)
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.relation_link_api import (
    select_primary_relation_link,
)
from LiuXin_alpha.metadata.containers.metadata_containers.lazy_liuxin_wemi_metadata import (
    LazyLiuXinWEMIMetadata,
)
from LiuXin_alpha.metadata.containers.metadata_containers.liuxin_wemi_metadata_hydrator import (
    LiuXinWEMIMetadataHydrator,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_container import (
    ExpressionIdentity,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_container import (
    ExpressionMetadata,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import (
    ItemIdentity,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import (
    ItemMetadata,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_container import (
    ManifestationIdentity,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_container import (
    ManifestationMetadata,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_container import (
    WorkIdentity,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import (
    WorkMetadata,
)
from LiuXin_alpha.utils.adaptors import _boolish_to_bool


class LazyLiuXinWEMIMetadataHydrator:
    """
    Resolve a selected WEMI identity chain and install deferred relation loaders.

    Preferred structural links override legacy parent-id hints. Relation buckets retain
    the graph separately from that selected chain. Schema and many query failures
    produce empty fallbacks, while malformed row payloads can still raise.

    Example:
        Exercise this contract with pytest::

            python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py
    """

    _SOURCE_ENTITY_TYPE_BY_LEVEL = {
        "work": "work",
        "expression": "expression",
        "manifestation": "manifestation",
        "item": "item",
    }
    _RELATION_LINK_CLASS_BY_LEVEL = {
        "work": WorkRelationLink,
        "expression": ExpressionRelationLink,
        "manifestation": ManifestationRelationLink,
        "item": ItemRelationLink,
    }
    _STRUCTURAL_RELATIONS_BY_LEVEL = {
        "work": frozenset({"expressions", "manifestations", "items"}),
        "expression": frozenset({"works", "manifestations", "items"}),
        "manifestation": frozenset({"expressions", "works", "items"}),
        "item": frozenset({"manifestations", "expressions", "works"}),
    }
    _LEGACY_FIELD_RELATIONS = {
        "tags": "tags",
        "labels": "labels",
        "genre": "genres",
        "subject": "subjects",
        "series": "series",
        "notes": "notes",
        "comments": "comments",
        "synopses": "synopses",
        "ratings": "ratings",
        "files": "files",
        "languages_available": "languages",
    }

    def __init__(self, database: Any) -> None:
        """
        Retain a non-None database and snapshot its tables and columns.

        A missing database raises ValueError. Each failed schema query falls back to an
        empty snapshot; the hydrator never closes the database.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py


        :param database: Caller-owned database/read source retained for metadata access; it
            is not closed here.
        :return: None.
        """
        if database is None:
            raise ValueError("LazyLiuXinWEMIMetadataHydrator requires a database instance.")
        self.db = database
        try:
            self._tables = set(self.db.get_tables(force_refresh=False))
        except Exception:
            self._tables = set()
        try:
            self._tables_and_columns = dict(self.db.get_tables_and_columns())
        except Exception:
            self._tables_and_columns = {}

    def get_lazy_liuxin_wemi_metadata(
        self,
        item_id: int | None = None,
        source_row: Mapping[str, Any] | Row | None = None,
    ) -> LazyLiuXinWEMIMetadata:
        """
        Resolve item, manifestation, expression and work identities, then defer other metadata.

        Provide item_id or source_row, otherwise ValueError is raised. An explicit item id
        wins over source hints; preferred relation targets override legacy parent ids.
        Resolved Rows take precedence over mapping-only identity fallbacks. The title is
        synchronized before return.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py


        :param item_id: Optional item row id; overrides an item id extracted from
            source_row.
        :param source_row: Optional Row or mapping containing WEMI identity fields and
            source-id hints.
        :return: Lazy slice with selected identities, structural links and deferred
            field/relation loaders.
        """
        if item_id is None and source_row is None:
            raise ValueError("Provide either item_id or source_row.")

        ids = LiuXinWEMIMetadataHydrator._extract_known_ids(source_row)
        if item_id is not None:
            ids["item_id"] = int(item_id)

        source_map = LiuXinWEMIMetadataHydrator._mapping_from(source_row)

        item_row = self._resolve_row("items", ids["item_id"])
        if item_row is None and isinstance(source_row, Row) and source_row.table == "items":
            item_row = source_row
        if item_row is not None:
            ids["item_id"] = self._prefer_id(ids["item_id"], item_row.row_id)
            ids["manifestation_id"] = self._prefer_id(
                ids["manifestation_id"],
                item_row.row_dict.get("item_manifestation_id"),
            )

        manifestation_link = None
        if item_row is not None:
            manifestation_link = self._first_relation_link(
                level="item",
                source_row=item_row,
                secondary_table="manifestations",
            )
            ids["manifestation_id"] = self._prefer_relation_link_id(
                ids["manifestation_id"],
                manifestation_link,
            )

        manifestation_row = self._resolve_row("manifestations", ids["manifestation_id"])
        if manifestation_row is None and isinstance(source_row, Row) and source_row.table == "manifestations":
            manifestation_row = source_row
        if manifestation_row is not None:
            ids["manifestation_id"] = self._prefer_id(
                ids["manifestation_id"],
                manifestation_row.row_id,
            )
            ids["expression_id"] = self._prefer_id(
                ids["expression_id"],
                manifestation_row.row_dict.get("manifestation_expression_id"),
            )

        expression_link = None
        if manifestation_row is not None:
            expression_link = self._first_relation_link(
                level="manifestation",
                source_row=manifestation_row,
                secondary_table="expressions",
            )
            ids["expression_id"] = self._prefer_relation_link_id(
                ids["expression_id"],
                expression_link,
            )

        expression_row = self._resolve_row("expressions", ids["expression_id"])
        if expression_row is None and isinstance(source_row, Row) and source_row.table == "expressions":
            expression_row = source_row
        if expression_row is not None:
            ids["expression_id"] = self._prefer_id(ids["expression_id"], expression_row.row_id)
            ids["work_id"] = self._prefer_id(
                ids["work_id"],
                expression_row.row_dict.get("expression_work_id"),
            )

        work_link = None
        if expression_row is not None:
            work_link = self._first_relation_link(
                level="expression",
                source_row=expression_row,
                secondary_table="works",
            )
            ids["work_id"] = self._prefer_relation_link_id(ids["work_id"], work_link)

        work_row = self._resolve_row("works", ids["work_id"])
        if work_row is None and isinstance(source_row, Row) and source_row.table == "works":
            work_row = source_row

        metadata = LazyLiuXinWEMIMetadata(
            work_metadata=WorkMetadata(work=self._work_identity(work_row, source_map)),
            expression_metadata=ExpressionMetadata(
                expression=self._expression_identity(expression_row, source_map),
            ),
            manifestation_metadata=ManifestationMetadata(
                manifestation=self._manifestation_identity(manifestation_row, source_map),
            ),
            item_metadata=ItemMetadata(item=self._item_identity(item_row, source_map)),
        )

        self._install_structural_links(
            metadata,
            item_row=item_row,
            manifestation_row=manifestation_row,
            expression_row=expression_row,
            work_row=work_row,
            expression_link=expression_link,
            work_link=work_link,
        )
        self._install_lazy_loaders(
            metadata,
            {
                "work": work_row,
                "expression": expression_row,
                "manifestation": manifestation_row,
                "item": item_row,
            },
        )
        metadata.sync_legacy_title_from_wemi()
        return metadata

    def get_liuxin_wemi_metadata(
        self,
        item_id: int | None = None,
        source_row: Mapping[str, Any] | Row | None = None,
    ) -> LazyLiuXinWEMIMetadata:
        """
        Build a lazy WEMI slice through the standard hydrator method name.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py


        :param item_id: Optional item row id; overrides an item id extracted from
            source_row.
        :param source_row: Optional Row or mapping containing WEMI identity fields and
            source-id hints.
        :return: Lazy slice returned by get_lazy_liuxin_wemi_metadata.
        """
        return self.get_lazy_liuxin_wemi_metadata(
            item_id=item_id,
            source_row=source_row,
        )

    def get_lazy_liuxin_metadata(
        self,
        item_id: int | None = None,
        source_row: Mapping[str, Any] | Row | None = None,
    ) -> LazyLiuXinWEMIMetadata:
        """
        Build a lazy WEMI slice through the legacy LiuXin alias.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py


        :param item_id: Optional item row id; overrides an item id extracted from
            source_row.
        :param source_row: Optional Row or mapping containing WEMI identity fields and
            source-id hints.
        :return: Lazy slice returned by get_lazy_liuxin_wemi_metadata.
        """
        return self.get_lazy_liuxin_wemi_metadata(
            item_id=item_id,
            source_row=source_row,
        )

    def get_calibre_metadata(
        self,
        item_id: int | None = None,
        source_row: Mapping[str, Any] | Row | None = None,
    ) -> Any:
        """
        Build a lazy slice and load its dependencies during Calibre conversion.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py


        :param item_id: Optional item row id; overrides an item id extracted from
            source_row.
        :param source_row: Optional Row or mapping containing WEMI identity fields and
            source-id hints.
        :return: New flat Calibre metadata object.
        """
        return self.get_lazy_liuxin_wemi_metadata(
            item_id=item_id,
            source_row=source_row,
        ).as_calibre_metadata()

    @staticmethod
    def _prefer_id(current: Any, fallback: Any) -> int | None:
        """
        Prefer a convertible current id, then try the fallback.

        Example:
            >>> LazyLiuXinWEMIMetadataHydrator._prefer_id('7', 9)
            7


        :param current: Existing id candidate to normalize.
        :param fallback: Fallback id candidate used when current cannot be converted.
        :return: Integer id, or None when neither candidate converts.
        """
        return LiuXinWEMIMetadataHydrator._prefer_id(current, fallback)

    @classmethod
    def _prefer_relation_link_id(cls, current: Any, relation_link: Any) -> int | None:
        """
        Prefer a relation target Row's id over the current source-row hint.

        Targets that are not database Rows do not override the hint.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py


        :param current: Existing id candidate to normalize.
        :param relation_link: Optional relation link whose target may be a database Row.
        :return: Target row id when available, otherwise the normalized current id.
        """
        current_id = cls._prefer_id(current, None)
        target = getattr(relation_link, "target", None)
        target_id = target.row_id if isinstance(target, Row) else None
        target_id = cls._prefer_id(target_id, None)
        if target_id is None:
            return current_id
        return target_id

    def _has_table(self, table: str) -> bool:
        """
        Test whether either cached schema snapshot contains the table.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py


        :param table: Database table name checked against the cached schema.
        :return: True when the table is known.
        """
        return table in self._tables or table in self._tables_and_columns

    def _has_column(self, table: str, column: str) -> bool:
        """
        Test for a column in the cached table-column mapping.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py


        :param table: Database table name checked against the cached schema.
        :param column: Column name checked in the cached table schema.
        :return: True when the column is listed for the table.
        """
        return column in set(self._tables_and_columns.get(table, []))

    def _resolve_row(self, table: str, row_id: int | None) -> Row | None:
        """
        Resolve a known table/id pair through the retained database.

        Missing ids, unknown tables and ordinary lookup/conversion exceptions produce None.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py


        :param table: Database table name checked against the cached schema.
        :param row_id: Optional row id to resolve; None requests no lookup.
        :return: Resolved database Row, or None.
        """
        if row_id is None or not self._has_table(table):
            return None
        try:
            return self.db.get_row_from_id(table, int(row_id))
        except Exception:
            return None

    @staticmethod
    def _work_identity(row: Row | None, source_map: Mapping[str, Any]) -> WorkIdentity | None:
        """
        Build a work identity from a resolved Row or a recognized source mapping.

        The Row wins when supplied. Otherwise the mapping must contain work_id or
        work_title; unrelated source mappings produce None.

        Example:
            >>> identity = LazyLiuXinWEMIMetadataHydrator._work_identity(None, {'work_id': 7})
            >>> identity.work_id
            7


        :param row: Resolved database Row, or None to try the fallback source mapping.
        :param source_map: Fallback mapping containing identity fields when no resolved Row
            is available.
        :return: Work identity, or None.
        """
        if row is not None:
            return WorkIdentity.from_mapping(row.row_dict)
        if "work_id" in source_map or "work_title" in source_map:
            return WorkIdentity.from_mapping(source_map)
        return None

    @staticmethod
    def _expression_identity(
        row: Row | None,
        source_map: Mapping[str, Any],
    ) -> ExpressionIdentity | None:
        """
        Build a expression identity from a resolved Row or a recognized source mapping.

        The Row wins when supplied. Otherwise the mapping must contain expression_id or
        expression_title_override; unrelated source mappings produce None.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py


        :param row: Resolved database Row, or None to try the fallback source mapping.
        :param source_map: Fallback mapping containing identity fields when no resolved Row
            is available.
        :return: Expression identity, or None.
        """
        if row is not None:
            return ExpressionIdentity.from_mapping(row.row_dict)
        if "expression_id" in source_map or "expression_title_override" in source_map:
            return ExpressionIdentity.from_mapping(source_map)
        return None

    @staticmethod
    def _manifestation_identity(
        row: Row | None,
        source_map: Mapping[str, Any],
    ) -> ManifestationIdentity | None:
        """
        Build a manifestation identity from a resolved Row or a recognized source mapping.

        The Row wins when supplied. Otherwise the mapping must contain manifestation_id or
        manifestation_format_detail; unrelated source mappings produce None.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py


        :param row: Resolved database Row, or None to try the fallback source mapping.
        :param source_map: Fallback mapping containing identity fields when no resolved Row
            is available.
        :return: Manifestation identity, or None.
        """
        if row is not None:
            return ManifestationIdentity.from_mapping(row.row_dict)
        if "manifestation_id" in source_map or "manifestation_format_detail" in source_map:
            return ManifestationIdentity.from_mapping(source_map)
        return None

    @staticmethod
    def _item_identity(row: Row | None, source_map: Mapping[str, Any]) -> ItemIdentity | None:
        """
        Build a item identity from a resolved Row or a recognized source mapping.

        The Row wins when supplied. Otherwise the mapping must contain item_id or
        item_manifestation_id; unrelated source mappings produce None.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py


        :param row: Resolved database Row, or None to try the fallback source mapping.
        :param source_map: Fallback mapping containing identity fields when no resolved Row
            is available.
        :return: Item identity, or None.
        """
        if row is not None:
            return ItemIdentity.from_mapping(row.row_dict)
        if "item_id" in source_map or "item_manifestation_id" in source_map:
            return ItemIdentity.from_mapping(source_map)
        return None

    def _install_structural_links(
        self,
        metadata: LazyLiuXinWEMIMetadata,
        *,
        item_row: Row | None,
        manifestation_row: Row | None,
        expression_row: Row | None,
        work_row: Row | None,
        expression_link: ManifestationRelationLink | None,
        work_link: ExpressionRelationLink | None,
    ) -> None:
        """
        Install selected parent links and merge persisted structural interlinks.

        The item-to-manifestation fallback is synthesized; manifestation-to-expression and
        expression-to-work use supplied preferred links when present. Existing targets are
        merged by table/id. work_row participates only as the expression parent.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py


        :param metadata: Metadata slice whose bundles or deferred loaders are being
            populated.
        :param item_row: Resolved item Row, or None.
        :param manifestation_row: Resolved manifestation Row, or None.
        :param expression_row: Resolved expression Row, or None.
        :param work_row: Resolved work Row, or None.
        :param expression_link: Preferred manifestation-to-expression link, or None for a
            synthesized fallback.
        :param work_link: Preferred expression-to-work link, or None for a synthesized
            fallback.
        :return: None.
        """
        if item_row is not None and manifestation_row is not None:
            self._add_wemi_relation_link_unique(
                metadata,
                "item",
                "manifestations",
                ItemRelationLink(
                    target=manifestation_row,
                    primary=True,
                    type="parent_manifestation",
                    extra={"source_entity_type": "item"},
                ),
            )
        if item_row is not None:
            self._add_structural_relation_links(
                metadata,
                level="item",
                relation="manifestations",
                source_row=item_row,
            )
        if manifestation_row is not None and expression_row is not None:
            self._add_wemi_relation_link_unique(
                metadata,
                "manifestation",
                "expressions",
                expression_link
                if expression_link is not None
                else ManifestationRelationLink(
                    target=expression_row,
                    primary=True,
                    type="manifestation_expression",
                    extra={"source_entity_type": "manifestation"},
                ),
            )
        if manifestation_row is not None:
            self._add_structural_relation_links(
                metadata,
                level="manifestation",
                relation="expressions",
                source_row=manifestation_row,
            )
        if expression_row is not None and work_row is not None:
            self._add_wemi_relation_link_unique(
                metadata,
                "expression",
                "works",
                work_link
                if work_link is not None
                else ExpressionRelationLink(
                    target=work_row,
                    primary=True,
                    type="expression_work",
                    extra={"source_entity_type": "expression"},
                ),
            )
        if expression_row is not None:
            self._add_structural_relation_links(
                metadata,
                level="expression",
                relation="works",
                source_row=expression_row,
            )

    def _add_structural_relation_links(
        self,
        metadata: LazyLiuXinWEMIMetadata,
        *,
        level: str,
        relation: str,
        source_row: Row,
    ) -> None:
        """
        Collect persisted structural links and merge them into the selected bucket.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py


        :param metadata: Metadata slice whose bundles or deferred loaders are being
            populated.
        :param level: WEMI level identifying the bundle to access.
        :param relation: Canonical relation bucket or target-table name.
        :param source_row: Database Row anchoring the relation lookup.
        :return: None.
        """
        for link in self._collect_relation_links(
            level=level,
            source_row=source_row,
            secondary_table=relation,
        ):
            self._add_wemi_relation_link_unique(metadata, level, relation, link)

    def _add_wemi_relation_link_unique(
        self,
        metadata: LazyLiuXinWEMIMetadata,
        level: str,
        relation: str,
        link: Any,
    ) -> None:
        """
        Merge links sharing a database Row target, or append a new target link.

        Only database Rows with table and id can be deduplicated. Matching links retain the
        existing target and receive incoming non-None metadata.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py


        :param metadata: Metadata slice whose bundles or deferred loaders are being
            populated.
        :param level: WEMI level identifying the bundle to access.
        :param relation: Canonical relation bucket or target-table name.
        :param link: Incoming relation link whose target and metadata are retained.
        :return: None.
        """
        links = list(metadata.get_wemi_relation_links(level, relation))
        key = self._row_key(link.target)
        if key is not None:
            for index, existing in enumerate(links):
                if self._row_key(existing.target) == key:
                    links[index] = self._merge_relation_link_metadata(
                        existing,
                        link,
                    )
                    metadata.set_wemi_relation_links(level, relation, links)
                    return
        links.append(link)
        metadata.set_wemi_relation_links(level, relation, links)

    @staticmethod
    def _row_key(row: Row | Any) -> tuple[str, int] | None:
        """
        Identify a database Row by its table name and integer row id.

        Example:
            >>> LazyLiuXinWEMIMetadataHydrator._row_key({'work_id': 7}) is None
            True


        :param row: Candidate target whose database identity is inspected.
        :return: Table/id pair, or None for non-Row or incomplete targets.
        """
        if not isinstance(row, Row):
            return None
        if row.table is None or row.row_id is None:
            return None
        return (str(row.table), int(row.row_id))

    @staticmethod
    def _merge_relation_link_metadata(existing: Any, incoming: Any) -> Any:
        """
        Build a replacement link retaining the existing target and concrete link class.

        Incoming non-None attributes override existing attributes; incoming extra entries
        override matching existing extra keys. Neither input link is mutated.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py


        :param existing: Existing link providing the target, class and fallback metadata.
        :param incoming: Incoming link providing non-None overrides and extra entries.
        :return: Merged link instance.
        """
        extra = dict(getattr(existing, "extra", {}) or {})
        extra.update(getattr(incoming, "extra", {}) or {})
        return type(existing)(
            target=existing.target,
            priority=(
                incoming.priority
                if incoming.priority is not None
                else existing.priority
            ),
            primary=(
                incoming.primary
                if incoming.primary is not None
                else existing.primary
            ),
            type=incoming.type if incoming.type is not None else existing.type,
            origin=(
                incoming.origin
                if incoming.origin is not None
                else existing.origin
            ),
            source=(
                incoming.source
                if incoming.source is not None
                else existing.source
            ),
            policy=(
                incoming.policy
                if incoming.policy is not None
                else existing.policy
            ),
            data=incoming.data if incoming.data is not None else existing.data,
            index=incoming.index if incoming.index is not None else existing.index,
            link_id=(
                incoming.link_id
                if incoming.link_id is not None
                else existing.link_id
            ),
            cardinality=(
                incoming.cardinality
                if incoming.cardinality is not None
                else existing.cardinality
            ),
            extra=extra,
        )

    def _install_lazy_loaders(
        self,
        metadata: LazyLiuXinWEMIMetadata,
        source_rows_by_level: Mapping[str, Row | None],
    ) -> None:
        """
        Register non-structural relation loaders and legacy field mapping loaders.

        Missing source Rows skip that level's relation loaders, while every configured
        legacy field still receives a wrapper. Identifiers and item asset replicas use
        dedicated loaders.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py


        :param metadata: Metadata slice whose bundles or deferred loaders are being
            populated.
        :param source_rows_by_level: Mapping of canonical WEMI levels to resolved source
            Rows or None.
        :return: None.
        """
        for level, source_row in source_rows_by_level.items():
            if source_row is None:
                continue
            for relation in metadata.get_wemi_metadata(level).relation_names():
                if relation in self._STRUCTURAL_RELATIONS_BY_LEVEL[level]:
                    continue
                if relation == "identifiers":
                    loader = self._make_identifier_loader(level, source_row)
                elif level == "item" and relation == "asset_replicas":
                    loader = self._make_item_asset_replicas_loader(metadata)
                else:
                    loader = self._make_relation_loader(level, source_row, relation)
                metadata.install_lazy_relation_loader(level, relation, loader)

        for field, relation in self._LEGACY_FIELD_RELATIONS.items():
            metadata.install_lazy_value_to_id(
                field,
                self._make_legacy_field_loader(metadata, field=field, relation=relation),
            )

    def _make_relation_loader(
        self,
        level: str,
        source_row: Row,
        relation: str,
    ):
        """
        Capture a deferred direct-foreign-key or interlink lookup.

        Item files, images and annotations use direct foreign keys; other buckets use
        interlink rows. No relation query runs while creating the callable.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py


        :param level: WEMI level identifying the bundle to access.
        :param source_row: Database Row anchoring the relation lookup.
        :param relation: Canonical relation bucket or target-table name.
        :return: Zero-argument relation loader.
        """
        direct_fk = self._direct_fk_spec(level=level, relation=relation)
        if direct_fk is not None:
            table, fk_column, type_hint = direct_fk
            return lambda: self._collect_direct_fk_links(
                level=level,
                table=table,
                fk_column=fk_column,
                fk_value=int(source_row.row_id),
                type_hint=type_hint,
            )

        return lambda: self._collect_relation_links(
            level=level,
            source_row=source_row,
            secondary_table=relation,
        )

    def _make_item_asset_replicas_loader(self, metadata: LazyLiuXinWEMIMetadata):
        """
        Capture a loader that follows the item's digital assets to their replicas.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py


        :param metadata: Metadata slice whose bundles or deferred loaders are being
            populated.
        :return: Zero-argument asset-replica loader retaining this hydrator and metadata
            slice.
        """
        return lambda: self._collect_item_asset_replica_links(metadata)

    def _make_identifier_loader(self, level: str, source_row: Row):
        """
        Capture a loader for observed-item and entity identifier rows.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py


        :param level: WEMI level identifying the bundle to access.
        :param source_row: Database Row anchoring the relation lookup.
        :return: Zero-argument identifier loader retaining the source Row and hydrator.
        """
        return lambda: self._collect_identifier_links(level=level, source_row=source_row)

    @staticmethod
    def _direct_fk_spec(*, level: str, relation: str) -> tuple[str, str, str] | None:
        """
        Select direct foreign-key metadata for item files, images or annotations.

        Example:
            >>> LazyLiuXinWEMIMetadataHydrator._direct_fk_spec(level='item', relation='files')
            ('files', 'file_item_id', 'item_file')


        :param level: WEMI level identifying the bundle to access.
        :param relation: Canonical relation bucket or target-table name.
        :return: Table, foreign-key column and link-type tuple, or None for other pairs.
        """
        if level == "item" and relation == "files":
            return ("files", "file_item_id", "item_file")
        if level == "item" and relation == "images":
            return ("images", "image_item_id", "item_image")
        if level == "item" and relation == "annotations":
            return ("annotations", "annotation_item_id", "item_annotation")
        return None

    def _make_legacy_field_loader(
        self,
        metadata: LazyLiuXinWEMIMetadata,
        *,
        field: str,
        relation: str,
    ):
        """
        Capture a deferred relation-to-legacy-field projection.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py


        :param metadata: Metadata slice whose bundles or deferred loaders are being
            populated.
        :param field: Legacy metadata field name; supported aliases are normalized by the
            implementation.
        :param relation: Canonical relation bucket or target-table name.
        :return: Zero-argument callable returning the slice's ordered legacy field mapping.
        """
        return lambda: metadata.lazy_legacy_terms_from_relation(
            field=field,
            relation_key=relation,
        )

    def _first_relation_link(
        self,
        *,
        level: str,
        source_row: Row,
        secondary_table: str,
    ):
        """
        Select the preferred collected link using primary and priority metadata.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py


        :param level: WEMI level identifying the bundle to access.
        :param source_row: Database Row anchoring the relation lookup.
        :param secondary_table: Target table whose related rows should be loaded.
        :return: Preferred relation link, or None when no links resolve.
        """
        links = self._collect_relation_links(
            level=level,
            source_row=source_row,
            secondary_table=secondary_table,
        )
        return select_primary_relation_link(links)

    def _collect_relation_links(
        self,
        *,
        level: str,
        source_row: Row,
        secondary_table: str,
    ) -> list[Any]:
        """
        Resolve interlink rows into typed WEMI relation links with provenance.

        Unavailable tables or failed interlink queries yield an empty list. Rows with
        unresolved targets are skipped; malformed target-id conversion can still raise.
        Recognized prefixed columns populate link metadata and remaining suffixes populate
        extra.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py


        :param level: WEMI level identifying the bundle to access.
        :param source_row: Database Row anchoring the relation lookup.
        :param secondary_table: Target table whose related rows should be loaded.
        :return: Resolved links in database result order.
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

        link_class = self._RELATION_LINK_CLASS_BY_LEVEL[level]
        out: list[Any] = []
        for link_row in link_rows:
            link_map = link_row.row_dict if isinstance(link_row, Row) else dict(link_row)
            target = None
            target_id = link_map.get(secondary_id_column) if secondary_id_column else None
            if target_id not in (None, ""):
                target = self._resolve_row(secondary_table, int(target_id))
            if target is None:
                continue

            extra = {"source_entity_type": self._SOURCE_ENTITY_TYPE_BY_LEVEL[level]}
            if prefix is not None:
                extra.update(
                    self._extra_from_link_map(
                        source_table=source_row.table,
                        secondary_table=secondary_table,
                        prefix=prefix,
                        link_map=link_map,
                    )
                )

            out.append(
                link_class(
                    target=target,
                    priority=link_map.get(prefix + "_priority") if prefix else None,
                    primary=(
                        _boolish_to_bool(link_map.get(prefix + "_primary"))
                        if prefix
                        else None
                    ),
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

    def _collect_direct_fk_links(
        self,
        *,
        level: str,
        table: str,
        fk_column: str,
        fk_value: int,
        type_hint: str,
    ) -> list[Any]:
        """
        Search a target table by foreign key and wrap each row as a relation link.

        Missing schema or failed queries yield an empty list. The first returned row is
        marked primary, with no explicit sorting added here.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py


        :param level: WEMI level identifying the bundle to access.
        :param table: Database table name checked against the cached schema.
        :param fk_column: Foreign-key column to match against the source id.
        :param fk_value: Source id converted to int for the search.
        :param type_hint: Relation type recorded on every generated link.
        :return: Typed links in search-result order.
        """
        if not self._has_table(table) or not self._has_column(table, fk_column):
            return []
        try:
            rows = list(
                self.db.search(
                    table=table,
                    column=fk_column,
                    search_term=int(fk_value),
                )
            )
        except Exception:
            return []

        link_class = self._RELATION_LINK_CLASS_BY_LEVEL[level]
        return [
            link_class(
                target=row,
                primary=(index == 0),
                type=type_hint,
                extra={"source_entity_type": table},
            )
            for index, row in enumerate(rows)
        ]

    def _collect_identifier_links(
        self,
        *,
        level: str,
        source_row: Row,
    ) -> list[Any]:
        """
        Collect item-observed identifiers and entity identifiers matching the WEMI level.

        Missing schema or failed searches yield no rows for that branch. Entity identifiers
        preserve primary/provenance fields; duplicate targets are not removed here.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py


        :param level: WEMI level identifying the bundle to access.
        :param source_row: Database Row anchoring the relation lookup.
        :return: Typed identifier links, with item-observed rows before entity rows.
        """
        links: list[Any] = []
        link_class = self._RELATION_LINK_CLASS_BY_LEVEL[level]

        if (
            level == "item"
            and source_row.row_id is not None
            and self._has_table("item_identifiers")
            and self._has_column("item_identifiers", "item_identifier_item_id")
        ):
            try:
                item_identifier_rows = list(
                    self.db.search(
                        table="item_identifiers",
                        column="item_identifier_item_id",
                        search_term=int(source_row.row_id),
                    )
                )
            except Exception:
                item_identifier_rows = []
            for row in item_identifier_rows:
                links.append(
                    link_class(
                        target=row,
                        type="item_identifier",
                        extra={"source_entity_type": "item"},
                    )
                )

        if (
            source_row.row_id is not None
            and self._has_table("entity_identifiers")
            and self._has_column("entity_identifiers", "entity_identifier_entity_id")
            and self._has_column("entity_identifiers", "entity_identifier_entity_type")
        ):
            try:
                entity_identifier_rows = list(
                    self.db.search(
                        table="entity_identifiers",
                        column="entity_identifier_entity_id",
                        search_term=int(source_row.row_id),
                    )
                )
            except Exception:
                entity_identifier_rows = []
            for row in entity_identifier_rows:
                mapping = row.row_dict if isinstance(row, Row) else dict(row)
                if str(mapping.get("entity_identifier_entity_type", "")).strip().lower() != level:
                    continue
                links.append(
                    link_class(
                        target=row,
                        primary=_boolish_to_bool(mapping.get("entity_identifier_is_primary")),
                        type="entity_identifier",
                        origin=mapping.get("entity_identifier_provenance"),
                        extra={"source_entity_type": level},
                    )
                )

        return links

    def _collect_item_asset_replica_links(
        self,
        metadata: LazyLiuXinWEMIMetadata,
    ) -> list[ItemRelationLink]:
        """
        Read item digital-asset links and collect replicas by asset foreign key.

        Reading digital assets may itself hydrate a pending bucket. Non-Row and id-less
        targets are skipped; results are concatenated without deduplication.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py


        :param metadata: Metadata slice whose bundles or deferred loaders are being
            populated.
        :return: Item relation links for the resolved replicas.
        """
        links: list[ItemRelationLink] = []
        for digital_asset_link in metadata.get_wemi_relation_links("item", "digital_assets"):
            target = digital_asset_link.target
            if not isinstance(target, Row) or target.row_id is None:
                continue
            links.extend(
                self._collect_direct_fk_links(
                    level="item",
                    table="asset_replicas",
                    fk_column="asset_replica_digital_asset_id",
                    fk_value=int(target.row_id),
                    type_hint="asset_replica",
                )
            )
        return links

    def _extra_from_link_map(
        self,
        *,
        source_table: str,
        secondary_table: str,
        prefix: str,
        link_map: Mapping[str, Any],
    ) -> dict[str, Any]:
        """
        Collect unmodeled interlink columns after removing the relation prefix.

        Known metadata suffixes and driver-reported id-column names are omitted. Failed
        id-column discovery uses an empty exclusion set.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py


        :param source_table: Source table of the interlink.
        :param secondary_table: Target table whose related rows should be loaded.
        :param prefix: Interlink column base used to recognize relation metadata.
        :param link_map: Column-keyed interlink row values.
        :return: New mapping from remaining suffixes to their original values.
        """
        try:
            skipped_id_columns = {
                self.db.driver_wrapper.get_id_column(source_table),
                self.db.driver_wrapper.get_id_column(secondary_table),
            }
        except Exception:
            skipped_id_columns = set()

        skipped_suffixes = {
            "priority",
            "primary",
            "type",
            "origin",
            "source",
            "policy",
            "data",
            "index",
            "id",
        }
        out: dict[str, Any] = {}
        for key, value in link_map.items():
            key_text = str(key)
            if not key_text.startswith(prefix + "_"):
                continue
            suffix = key_text[len(prefix) + 1 :]
            if suffix in skipped_suffixes or suffix in skipped_id_columns:
                continue
            out[suffix] = value
        return out


__all__ = ["LazyLiuXinWEMIMetadataHydrator"]

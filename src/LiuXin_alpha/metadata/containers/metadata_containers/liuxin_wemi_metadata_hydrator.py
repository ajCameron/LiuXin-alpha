"""
Compose level-specific hydrators into eager item-centered WEMI metadata.

A read-source adapter lets the same orchestration work with databases and supported
loaded caches. The caller retains ownership of the underlying source.

Example:
    Exercise this contract with pytest::

        python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api import (
    ExpressionIdentityAPI,
    ItemIdentityAPI,
    ManifestationIdentityAPI,
    WorkIdentityAPI,
)
from LiuXin_alpha.metadata.api.from_database_api.metadata_hydrator_api import (
    HydratableMetadataKind,
    HydratedMetadataAPI,
    MetadataHydratorAPI,
)
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.relation_link_api import (
    select_primary_relation_link,
)
from LiuXin_alpha.metadata.read_sources import metadata_read_source_from
from LiuXin_alpha.metadata.containers.metadata_containers.liuxin_wemi_metadata import (
    LiuXinWEMIMetadata,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_container import (
    ExpressionMetadata,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_hydrator import (
    ExpressionMetadataHydrator,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import (
    ItemMetadata,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_hydrator import (
    ItemMetadataHydrator,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_container import (
    ManifestationMetadata,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_hydrator import (
    ManifestationMetadataHydrator,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import (
    WorkMetadata,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_hydrator import (
    WorkMetadataHydrator,
)


class LiuXinWEMIMetadataHydrator(MetadataHydratorAPI):
    """
    Hydrate identities, individual bundles or a complete eager WEMI slice.

    Specialized hydrators own table and relation queries. This class chooses the
    preferred identity chain, retains complete bundle relations and synchronizes
    supported legacy fields.

    Example:
        Exercise this contract with pytest::

            python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py
    """

    def __init__(self, database: Any) -> None:
        """
        Adapt a non-None database/read source and construct four specialized hydrators.

        None raises ValueError; adapter or hydrator initialization failures propagate. The
        underlying source is not closed here.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py


        :param database: Caller-owned database or supported metadata read source shared by
            all level hydrators.
        :return: None.
        """
        if database is None:
            raise ValueError("LiuXinWEMIMetadataHydrator requires a database instance.")
        self.db = metadata_read_source_from(database)
        self._work_hydrator = WorkMetadataHydrator(self.db)
        self._expression_hydrator = ExpressionMetadataHydrator(self.db)
        self._manifestation_hydrator = ManifestationMetadataHydrator(self.db)
        self._item_hydrator = ItemMetadataHydrator(self.db)

    def get_work_identity(self, work_id: int) -> WorkIdentityAPI:
        """
        Hydrate a work bundle and require its identity to be present.

        An empty identity raises ValueError; lower-level hydration failures propagate.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py


        :param work_id: Work row id converted with int before hydration.
        :return: Work identity from the hydrated bundle.
        """
        metadata = self.get_work_metadata(work_id)
        if metadata.work is None:
            raise ValueError("No work identity found for id {}.".format(int(work_id)))
        return metadata.work

    def get_work_metadata(self, work_id: int) -> WorkMetadata:
        """
        Delegate work row hydration to the specialized hydrator.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py


        :param work_id: Work row id converted with int before hydration.
        :return: Hydrated work metadata bundle.
        """
        return self._work_hydrator.from_work_id(int(work_id))

    def get_expression_identity(self, expression_id: int) -> ExpressionIdentityAPI:
        """
        Hydrate a expression bundle and require its identity to be present.

        An empty identity raises ValueError; lower-level hydration failures propagate.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py


        :param expression_id: Expression row id converted with int before hydration.
        :return: Expression identity from the hydrated bundle.
        """
        metadata = self.get_expression_metadata(expression_id)
        if metadata.expression is None:
            raise ValueError(
                "No expression identity found for id {}.".format(
                    int(expression_id),
                )
            )
        return metadata.expression

    def get_expression_metadata(self, expression_id: int) -> ExpressionMetadata:
        """
        Delegate expression row hydration to the specialized hydrator.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py


        :param expression_id: Expression row id converted with int before hydration.
        :return: Hydrated expression metadata bundle.
        """
        return self._expression_hydrator.from_expression_id(int(expression_id))

    def get_manifestation_identity(
        self,
        manifestation_id: int,
    ) -> ManifestationIdentityAPI:
        """
        Hydrate a manifestation bundle and require its identity to be present.

        An empty identity raises ValueError; lower-level hydration failures propagate.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py


        :param manifestation_id: Manifestation row id converted with int before hydration.
        :return: Manifestation identity from the hydrated bundle.
        """
        metadata = self.get_manifestation_metadata(manifestation_id)
        if metadata.manifestation is None:
            raise ValueError(
                "No manifestation identity found for id {}.".format(
                    int(manifestation_id),
                )
            )
        return metadata.manifestation

    def get_manifestation_metadata(self, manifestation_id: int) -> ManifestationMetadata:
        """
        Delegate manifestation row hydration to the specialized hydrator.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py


        :param manifestation_id: Manifestation row id converted with int before hydration.
        :return: Hydrated manifestation metadata bundle.
        """
        return self._manifestation_hydrator.from_manifestation_id(int(manifestation_id))

    def get_item_identity(self, item_id: int) -> ItemIdentityAPI:
        """
        Hydrate a item bundle and require its identity to be present.

        An empty identity raises ValueError; lower-level hydration failures propagate.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py


        :param item_id: Item row id converted with int before hydration.
        :return: Item identity from the hydrated bundle.
        """
        metadata = self.get_item_metadata(item_id=item_id)
        if metadata.item is None:
            raise ValueError("No item identity found for id {}.".format(int(item_id)))
        return metadata.item

    def get_item_metadata(
        self,
        item_id: int | None = None,
        source_row: Mapping[str, Any] | Row | None = None,
    ) -> ItemMetadata:
        """
        Hydrate an item by explicit id, otherwise from a supplied Row or mapping.

        An explicit id takes precedence. Omitting both inputs raises ValueError.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py


        :param item_id: Optional item row id; overrides an item id extracted from
            source_row.
        :param source_row: Database Row or source mapping supplying identity fields and
            row-id hints.
        :return: Hydrated item metadata bundle.
        """
        if item_id is not None:
            return self._item_hydrator.from_item_id(int(item_id))
        if source_row is not None:
            return self._item_hydrator.from_source_row(source_row)
        raise ValueError("Provide either item_id or source_row.")

    def get_liuxin_wemi_metadata(
        self,
        item_id: int | None = None,
        source_row: Mapping[str, Any] | Row | None = None,
    ) -> LiuXinWEMIMetadata:
        """
        Build an eager item slice and synchronize supported legacy metadata.

        Hydrate the item first, then choose manifestation, expression and work identities
        using preferred relation ids before source-row hints. Full relation buckets remain
        attached to their bundles. Missing parent levels can become empty bundles; other
        hydration failures propagate.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py


        :param item_id: Optional item row id; overrides an item id extracted from
            source_row.
        :param source_row: Database Row or source mapping supplying identity fields and
            row-id hints.
        :return: Complete WEMI slice with title, terms and supported external identifiers
            synchronized.
        """
        if item_id is None and source_row is None:
            raise ValueError("Provide either item_id or source_row.")

        item_metadata = self.get_item_metadata(item_id=item_id, source_row=source_row)
        ids = self._extract_known_ids(source_row)
        if item_id is not None:
            ids["item_id"] = int(item_id)

        if item_metadata.item is not None:
            ids["item_id"] = self._prefer_id(ids["item_id"], item_metadata.item.item_id)
            ids["manifestation_id"] = self._prefer_id(
                ids["manifestation_id"],
                item_metadata.item.item_manifestation_id,
            )

        ids["manifestation_id"] = self._prefer_id(
            item_metadata.primary_manifestation_id,
            ids["manifestation_id"],
        )
        manifestation_metadata = self._get_manifestation_metadata_or_empty(
            ids["manifestation_id"],
            source_row,
        )

        if manifestation_metadata.manifestation is not None:
            ids["manifestation_id"] = self._prefer_id(
                ids["manifestation_id"],
                manifestation_metadata.manifestation.manifestation_id,
            )
            ids["expression_id"] = self._prefer_id(
                ids["expression_id"],
                manifestation_metadata.manifestation.manifestation_expression_id,
            )

        ids["expression_id"] = self._prefer_id(
            self._first_id(
                manifestation_metadata.primary_expression_id,
                item_metadata.primary_expression_id,
            ),
            ids["expression_id"],
        )
        expression_metadata = self._get_expression_metadata_or_empty(
            ids["expression_id"],
            source_row,
        )

        if expression_metadata.expression is not None:
            ids["expression_id"] = self._prefer_id(
                ids["expression_id"],
                expression_metadata.expression.expression_id,
            )
            ids["work_id"] = self._prefer_id(
                ids["work_id"],
                expression_metadata.expression.expression_work_id,
            )

        ids["work_id"] = self._prefer_id(
            self._first_id(
                expression_metadata.primary_work_id,
                manifestation_metadata.primary_work_id,
                item_metadata.primary_work_id,
            ),
            ids["work_id"],
        )
        work_metadata = self._get_work_metadata_or_empty(ids["work_id"], source_row)

        metadata = LiuXinWEMIMetadata(
            work_metadata=work_metadata,
            expression_metadata=expression_metadata,
            manifestation_metadata=manifestation_metadata,
            item_metadata=item_metadata,
        )
        metadata.sync_legacy_title_from_wemi()
        metadata.sync_legacy_tags_from_wemi()
        metadata.sync_legacy_labels_from_wemi()
        metadata.sync_legacy_genres_from_wemi()
        metadata.sync_legacy_subjects_from_wemi()
        metadata.sync_legacy_series_from_wemi()
        metadata.sync_legacy_identifiers_from_wemi()
        return metadata

    def hydrate_metadata(
        self,
        kind: HydratableMetadataKind,
        *,
        work_id: int | None = None,
        expression_id: int | None = None,
        manifestation_id: int | None = None,
        item_id: int | None = None,
        source_row: Mapping[str, Any] | Row | None = None,
    ) -> HydratedMetadataAPI:
        """
        Dispatch a normalized metadata-kind name to a bundle or compatibility view.

        Supported kinds are work, expression, manifestation, item, liuxin_wemi, liuxin and
        calibre. Corresponding explicit ids precede source rows; unknown or insufficient
        requests raise ValueError.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py


        :param kind: Metadata kind name, stripped and lowercased before dispatch.
        :param work_id: Optional work id used only by the work branch.
        :param expression_id: Optional expression id used only by the expression branch.
        :param manifestation_id: Optional manifestation id used only by the manifestation
            branch.
        :param item_id: Optional item row id; overrides an item id extracted from
            source_row.
        :param source_row: Database Row or source mapping supplying identity fields and
            row-id hints.
        :return: Requested bundle, complete slice, live LiuXin view or converted Calibre
            object.
        """
        normalized_kind = str(kind).strip().lower()
        if normalized_kind == "work":
            if work_id is not None:
                return self.get_work_metadata(int(work_id))
            if source_row is not None:
                return self._work_hydrator.from_source_row(source_row)
        elif normalized_kind == "expression":
            if expression_id is not None:
                return self.get_expression_metadata(int(expression_id))
            if source_row is not None:
                return self._expression_hydrator.from_source_row(source_row)
        elif normalized_kind == "manifestation":
            if manifestation_id is not None:
                return self.get_manifestation_metadata(int(manifestation_id))
            if source_row is not None:
                return self._manifestation_hydrator.from_source_row(source_row)
        elif normalized_kind == "item":
            return self.get_item_metadata(item_id=item_id, source_row=source_row)
        elif normalized_kind == "liuxin_wemi":
            return self.get_liuxin_wemi_metadata(item_id=item_id, source_row=source_row)
        elif normalized_kind == "liuxin":
            return self.get_liuxin_wemi_metadata(
                item_id=item_id,
                source_row=source_row,
            ).as_liuxin_metadata()
        elif normalized_kind == "calibre":
            return self.get_liuxin_wemi_metadata(
                item_id=item_id,
                source_row=source_row,
            ).as_calibre_metadata()

        raise ValueError(
            "Could not hydrate metadata kind {!r} from the supplied ids/source row.".format(
                kind,
            )
        )

    @staticmethod
    def _mapping_from(value: Mapping[str, Any] | Row | None) -> Mapping[str, Any]:
        """
        Expose row_dict for a database Row, retain a Mapping, or return an empty mapping.

        Example:
            >>> payload = {"item_id": 7}
            >>> LiuXinWEMIMetadataHydrator._mapping_from(payload) is payload
            True


        :param value: Row, mapping or unsupported/None source value.
        :return: Existing mapping without copying, or a new empty dictionary.
        """
        if isinstance(value, Row):
            return value.row_dict
        if isinstance(value, Mapping):
            return value
        return {}

    @classmethod
    def _extract_known_ids(
        cls,
        source_row: Mapping[str, Any] | Row | None,
    ) -> dict[str, int | None]:
        """
        Collect WEMI id hints using direct ids, parent ids and legacy title/book aliases.

        Alias selection uses truthiness before int conversion, so zero hints fall through to
        later aliases. Unconvertible selected values become None.

        Example:
            >>> LiuXinWEMIMetadataHydrator._extract_known_ids({"title_id": "7"})["work_id"]
            7


        :param source_row: Database Row or source mapping supplying identity fields and
            row-id hints.
        :return: Dictionary with work_id, expression_id, manifestation_id and item_id.
        """
        mapping = cls._mapping_from(source_row)
        return {
            "work_id": cls._as_int(
                mapping.get("work_id")
                or mapping.get("expression_work_id")
                or mapping.get("title_id"),
            ),
            "expression_id": cls._as_int(
                mapping.get("expression_id")
                or mapping.get("manifestation_expression_id")
                or mapping.get("book_expression_id"),
            ),
            "manifestation_id": cls._as_int(
                mapping.get("manifestation_id")
                or mapping.get("item_manifestation_id")
                or mapping.get("book_manifestation_id"),
            ),
            "item_id": cls._as_int(mapping.get("item_id")),
        }

    @staticmethod
    def _as_int(value: Any) -> int | None:
        """
        Convert a nonempty value to int, tolerating common conversion failures.

        Example:
            >>> LiuXinWEMIMetadataHydrator._as_int("bad") is None
            True


        :param value: Candidate integer-like value; normal int coercion applies, including
            bool.
        :return: Integer value, or None for None, empty text or failed conversion.
        """
        if value in (None, ""):
            return None
        try:
            return int(value)
        except (TypeError, ValueError, OverflowError):
            return None

    @classmethod
    def _prefer_id(cls, current: Any, fallback: Any) -> int | None:
        """
        Prefer a convertible current id over the fallback, including zero.

        Example:
            >>> LiuXinWEMIMetadataHydrator._prefer_id(0, 7)
            0


        :param current: Preferred id candidate.
        :param fallback: Candidate used only when current cannot be converted.
        :return: First convertible id, or None.
        """
        current_id = cls._as_int(current)
        if current_id is not None:
            return current_id
        return cls._as_int(fallback)

    @classmethod
    def _first_id(cls, *values: Any) -> int | None:
        """
        Return the first candidate accepted by the integer conversion helper.

        Example:
            >>> LiuXinWEMIMetadataHydrator._first_id(None, "bad", "7", 9)
            7


        :param values: Id candidates in precedence order.
        :return: First convertible id, or None.
        """
        for value in values:
            value_id = cls._as_int(value)
            if value_id is not None:
                return value_id
        return None

    def _get_work_metadata_or_empty(
        self,
        work_id: int | None,
        source_row: Mapping[str, Any] | Row | None,
    ) -> WorkMetadata:
        """
        Hydrate a work by id, then try a source row, otherwise return an empty bundle.

        A supplied id bypasses the empty fallback and propagates failures. Only ValueError
        from the source-row attempt is suppressed.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py


        :param work_id: Optional work id preferred over source_row.
        :param source_row: Database Row or source mapping supplying identity fields and
            row-id hints.
        :return: Hydrated or empty work bundle.
        """
        if work_id is not None:
            return self.get_work_metadata(int(work_id))
        if source_row is not None:
            try:
                return self._work_hydrator.from_source_row(source_row)
            except ValueError:
                pass
        return WorkMetadata()

    def _get_expression_metadata_or_empty(
        self,
        expression_id: int | None,
        source_row: Mapping[str, Any] | Row | None,
    ) -> ExpressionMetadata:
        """
        Hydrate a expression by id, then try a source row, otherwise return an empty bundle.

        A supplied id bypasses the empty fallback and propagates failures. Only ValueError
        from the source-row attempt is suppressed.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py


        :param expression_id: Optional expression id preferred over source_row.
        :param source_row: Database Row or source mapping supplying identity fields and
            row-id hints.
        :return: Hydrated or empty expression bundle.
        """
        if expression_id is not None:
            return self.get_expression_metadata(int(expression_id))
        if source_row is not None:
            try:
                return self._expression_hydrator.from_source_row(source_row)
            except ValueError:
                pass
        return ExpressionMetadata()

    def _get_manifestation_metadata_or_empty(
        self,
        manifestation_id: int | None,
        source_row: Mapping[str, Any] | Row | None,
    ) -> ManifestationMetadata:
        """
        Hydrate a manifestation by id, then try a source row, otherwise return an empty bundle.

        A supplied id bypasses the empty fallback and propagates failures. Only ValueError
        from the source-row attempt is suppressed.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py


        :param manifestation_id: Optional manifestation id preferred over source_row.
        :param source_row: Database Row or source mapping supplying identity fields and
            row-id hints.
        :return: Hydrated or empty manifestation bundle.
        """
        if manifestation_id is not None:
            return self.get_manifestation_metadata(int(manifestation_id))
        if source_row is not None:
            try:
                return self._manifestation_hydrator.from_source_row(source_row)
            except ValueError:
                pass
        return ManifestationMetadata()

    @classmethod
    def _first_relation_target_id(
        cls,
        metadata: WorkMetadata | ExpressionMetadata | ManifestationMetadata | ItemMetadata,
        relation: str,
        id_column: str,
    ) -> int | None:
        """
        Try preferred links in primary/priority order until a target id can be read.

        An unsupported relation or exhausted bucket yields None. The local candidate list is
        shortened without removing stored links.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_hydrator_edge_cases.py


        :param metadata: WEMI bundle supplying the requested relation bucket.
        :param relation: Relation bucket name supported by the bundle.
        :param id_column: Target identity field to read as an integer.
        :return: First usable preferred target id, or None.
        """
        try:
            links = list(metadata.get_relation_links(relation))
        except KeyError:
            return None
        while links:
            link = select_primary_relation_link(links)
            if link is None:
                return None
            target_id = cls._target_id(link.target, id_column)
            if target_id is not None:
                return target_id
            links.remove(link)
        return None

    @classmethod
    def _target_id(cls, target: Any, id_column: str) -> int | None:
        """
        Read an id from a Row, mapping or identity attribute.

        A Row falls back from a false column value to row_id; mappings and other objects
        have no generic row-id fallback.

        Example:
            >>> LiuXinWEMIMetadataHydrator._target_id({"work_id": "7"}, "work_id")
            7


        :param target: Row, mapping or object with the requested identity field.
        :param id_column: Identity field name to inspect.
        :return: Converted id, or None.
        """
        if isinstance(target, Row):
            return cls._as_int(target.row_dict.get(id_column) or target.row_id)
        if isinstance(target, Mapping):
            return cls._as_int(target.get(id_column))
        return cls._as_int(getattr(target, id_column, None))


__all__ = ["LiuXinWEMIMetadataHydrator"]

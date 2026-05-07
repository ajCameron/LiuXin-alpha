"""Central hydrator for item-centred LiuXin/WEMI metadata slices."""

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
    Compose specialised WEMI hydrators into complete item metadata slices.

    This is the store-facing hydrator. Level-specific table/link logic remains
    in the existing W/E/M/I hydrators; this class only orchestrates them.
    """

    def __init__(self, database: Any) -> None:
        if database is None:
            raise ValueError("LiuXinWEMIMetadataHydrator requires a database instance.")
        self.db = metadata_read_source_from(database)
        self._work_hydrator = WorkMetadataHydrator(self.db)
        self._expression_hydrator = ExpressionMetadataHydrator(self.db)
        self._manifestation_hydrator = ManifestationMetadataHydrator(self.db)
        self._item_hydrator = ItemMetadataHydrator(self.db)

    def get_work_identity(self, work_id: int) -> WorkIdentityAPI:
        metadata = self.get_work_metadata(work_id)
        if metadata.work is None:
            raise ValueError("No work identity found for id {}.".format(int(work_id)))
        return metadata.work

    def get_work_metadata(self, work_id: int) -> WorkMetadata:
        return self._work_hydrator.from_work_id(int(work_id))

    def get_expression_identity(self, expression_id: int) -> ExpressionIdentityAPI:
        metadata = self.get_expression_metadata(expression_id)
        if metadata.expression is None:
            raise ValueError(
                "No expression identity found for id {}.".format(
                    int(expression_id),
                )
            )
        return metadata.expression

    def get_expression_metadata(self, expression_id: int) -> ExpressionMetadata:
        return self._expression_hydrator.from_expression_id(int(expression_id))

    def get_manifestation_identity(
        self,
        manifestation_id: int,
    ) -> ManifestationIdentityAPI:
        metadata = self.get_manifestation_metadata(manifestation_id)
        if metadata.manifestation is None:
            raise ValueError(
                "No manifestation identity found for id {}.".format(
                    int(manifestation_id),
                )
            )
        return metadata.manifestation

    def get_manifestation_metadata(self, manifestation_id: int) -> ManifestationMetadata:
        return self._manifestation_hydrator.from_manifestation_id(int(manifestation_id))

    def get_item_identity(self, item_id: int) -> ItemIdentityAPI:
        metadata = self.get_item_metadata(item_id=item_id)
        if metadata.item is None:
            raise ValueError("No item identity found for id {}.".format(int(item_id)))
        return metadata.item

    def get_item_metadata(
        self,
        item_id: int | None = None,
        source_row: Mapping[str, Any] | Row | None = None,
    ) -> ItemMetadata:
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
            ids["manifestation_id"],
            self._first_relation_target_id(
                item_metadata,
                "manifestations",
                "manifestation_id",
            ),
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
            ids["expression_id"],
            self._first_relation_target_id(
                item_metadata,
                "expressions",
                "expression_id",
            ),
        )
        ids["expression_id"] = self._prefer_id(
            ids["expression_id"],
            self._first_relation_target_id(
                manifestation_metadata,
                "expressions",
                "expression_id",
            ),
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
            ids["work_id"],
            self._first_relation_target_id(item_metadata, "works", "work_id"),
        )
        ids["work_id"] = self._prefer_id(
            ids["work_id"],
            self._first_relation_target_id(expression_metadata, "works", "work_id"),
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
        if value in (None, ""):
            return None
        try:
            return int(value)
        except (TypeError, ValueError, OverflowError):
            return None

    @classmethod
    def _prefer_id(cls, current: Any, fallback: Any) -> int | None:
        current_id = cls._as_int(current)
        if current_id is not None:
            return current_id
        return cls._as_int(fallback)

    def _get_work_metadata_or_empty(
        self,
        work_id: int | None,
        source_row: Mapping[str, Any] | Row | None,
    ) -> WorkMetadata:
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
        try:
            links = metadata.get_relation_links(relation)
        except KeyError:
            return None
        for link in sorted(
            links,
            key=lambda relation_link: not bool(relation_link.primary),
        ):
            target_id = cls._target_id(link.target, id_column)
            if target_id is not None:
                return target_id
        return None

    @classmethod
    def _target_id(cls, target: Any, id_column: str) -> int | None:
        if isinstance(target, Row):
            return cls._as_int(target.row_dict.get(id_column) or target.row_id)
        if isinstance(target, Mapping):
            return cls._as_int(target.get(id_column))
        return cls._as_int(getattr(target, id_column, None))


__all__ = ["LiuXinWEMIMetadataHydrator"]

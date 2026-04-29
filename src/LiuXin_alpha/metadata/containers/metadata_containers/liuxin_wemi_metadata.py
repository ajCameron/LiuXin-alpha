"""Item-centered LiuXin metadata slice with an attached WEMI stack."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from copy import deepcopy
from typing import Any, ClassVar, Literal, TypeAlias, cast

from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api import (
    ExpressionIdentityAPI,
    ExpressionRelationLink,
    ExpressionRelationTarget,
    ItemIdentityAPI,
    ItemRelationLink,
    ItemRelationTarget,
    ManifestationIdentityAPI,
    ManifestationRelationLink,
    ManifestationRelationTarget,
    RelationEdgeID,
    WorkIdentityAPI,
    WorkRelationLink,
    WorkRelationTarget,
)
from LiuXin_alpha.metadata.containers.calibre_like_book_metadata import (
    CalibreLikeLiuXinBookMetaData,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.expression_metadata_container import (
    ExpressionMetadata,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_metadata_container import (
    ItemMetadata,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.manifestation_metadata_container import (
    ManifestationMetadata,
)
from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.work_metadata_container import (
    WorkMetadata,
)


WemiLevel: TypeAlias = Literal["work", "expression", "manifestation", "item"]
WemiMetadataBundle: TypeAlias = (
    WorkMetadata
    | ExpressionMetadata
    | ManifestationMetadata
    | ItemMetadata
)
WemiIdentity: TypeAlias = (
    WorkIdentityAPI
    | ExpressionIdentityAPI
    | ManifestationIdentityAPI
    | ItemIdentityAPI
)
WemiRelationLink: TypeAlias = (
    WorkRelationLink
    | ExpressionRelationLink
    | ManifestationRelationLink
    | ItemRelationLink
)
WemiRelationTarget: TypeAlias = (
    WorkRelationTarget
    | ExpressionRelationTarget
    | ManifestationRelationTarget
    | ItemRelationTarget
)


class LiuXinWEMIMetadata(CalibreLikeLiuXinBookMetaData):
    """
    Complete metadata slice for one item.

    Legacy LiuXin/Calibre fields stay on the inherited ``_data`` surface. The
    WEMI stack is composed as four metadata bundles so callers can still reach
    identities, relation links, link ids, and provenance without flattening the
    model for sidecar storage.
    """

    SIDECAR_SCHEMA_NAME: ClassVar[str] = "liuxin_wemi_item_metadata"
    SIDECAR_SCHEMA_VERSION: ClassVar[int] = 1

    _LEVELS: ClassVar[tuple[WemiLevel, ...]] = (
        "work",
        "expression",
        "manifestation",
        "item",
    )
    _LEVEL_ALIASES: ClassVar[dict[str, WemiLevel]] = {
        "w": "work",
        "e": "expression",
        "m": "manifestation",
        "i": "item",
    }
    _METADATA_STORAGE_BY_LEVEL: ClassVar[dict[WemiLevel, str]] = {
        "work": "_work_metadata",
        "expression": "_expression_metadata",
        "manifestation": "_manifestation_metadata",
        "item": "_item_metadata",
    }
    _METADATA_STORAGE_BY_ATTRIBUTE: ClassVar[dict[str, str]] = {
        "work_metadata": "_work_metadata",
        "expression_metadata": "_expression_metadata",
        "manifestation_metadata": "_manifestation_metadata",
        "item_metadata": "_item_metadata",
    }
    _IDENTITY_ATTRIBUTE_BY_LEVEL: ClassVar[dict[WemiLevel, str]] = {
        "work": "work",
        "expression": "expression",
        "manifestation": "manifestation",
        "item": "item",
    }
    _DATABASE_ID_ALIASES: ClassVar[dict[str, str]] = {
        "work": "work_id",
        "expression": "expression_id",
        "manifestation": "manifestation_id",
        "item": "item_id",
    }

    def __init__(
        self,
        title: str | None = None,
        authors: str | list[str] | tuple[str, ...] | None = None,
        other: CalibreLikeLiuXinBookMetaData | None = None,
        *,
        work_metadata: WorkMetadata | None = None,
        expression_metadata: ExpressionMetadata | None = None,
        manifestation_metadata: ManifestationMetadata | None = None,
        item_metadata: ItemMetadata | None = None,
    ) -> None:
        object.__setattr__(self, "_work_metadata", WorkMetadata())
        object.__setattr__(self, "_expression_metadata", ExpressionMetadata())
        object.__setattr__(self, "_manifestation_metadata", ManifestationMetadata())
        object.__setattr__(self, "_item_metadata", ItemMetadata())

        super().__init__(title=title, authors=authors, other=other)

        if work_metadata is not None:
            object.__setattr__(self, "_work_metadata", work_metadata)
        if expression_metadata is not None:
            object.__setattr__(self, "_expression_metadata", expression_metadata)
        if manifestation_metadata is not None:
            object.__setattr__(
                self,
                "_manifestation_metadata",
                manifestation_metadata,
            )
        if item_metadata is not None:
            object.__setattr__(self, "_item_metadata", item_metadata)

    def __setattr__(self, key: str, value: Any) -> None:
        normalized_key = key.lower().strip()
        metadata_storage = self._METADATA_STORAGE_BY_ATTRIBUTE.get(normalized_key)
        if metadata_storage is not None:
            object.__setattr__(
                self,
                metadata_storage,
                self._coerce_metadata_bundle(normalized_key, value),
            )
            return

        if normalized_key in self._IDENTITY_ATTRIBUTE_BY_LEVEL:
            metadata = self.get_wemi_metadata(cast(WemiLevel, normalized_key))
            setattr(metadata, normalized_key, value)
            return

        super().__setattr__(key, value)

    @classmethod
    def _coerce_metadata_bundle(cls, attribute: str, value: Any) -> WemiMetadataBundle:
        if value is not None:
            return value

        if attribute == "work_metadata":
            return WorkMetadata()
        if attribute == "expression_metadata":
            return ExpressionMetadata()
        if attribute == "manifestation_metadata":
            return ManifestationMetadata()
        if attribute == "item_metadata":
            return ItemMetadata()
        raise KeyError(f"Unknown WEMI metadata attribute: {attribute!r}")

    @classmethod
    def normalize_wemi_level(cls, level: str) -> WemiLevel:
        normalized = str(level).strip().lower()
        normalized = cls._LEVEL_ALIASES.get(normalized, normalized)
        if normalized not in cls._LEVELS:
            raise KeyError(
                "Unknown WEMI level {!r}. Expected one of {}.".format(
                    level,
                    ", ".join(cls._LEVELS),
                )
            )
        return cast(WemiLevel, normalized)

    @property
    def liuxin(self) -> "LiuXinWEMIMetadata":
        return self

    @property
    def calibre(self) -> Any:
        return self.to_calibre()

    @property
    def work_metadata(self) -> WorkMetadata:
        return object.__getattribute__(self, "_work_metadata")

    @work_metadata.setter
    def work_metadata(self, value: WorkMetadata | None) -> None:
        object.__setattr__(
            self,
            "_work_metadata",
            self._coerce_metadata_bundle("work_metadata", value),
        )

    @property
    def expression_metadata(self) -> ExpressionMetadata:
        return object.__getattribute__(self, "_expression_metadata")

    @expression_metadata.setter
    def expression_metadata(self, value: ExpressionMetadata | None) -> None:
        object.__setattr__(
            self,
            "_expression_metadata",
            self._coerce_metadata_bundle("expression_metadata", value),
        )

    @property
    def manifestation_metadata(self) -> ManifestationMetadata:
        return object.__getattribute__(self, "_manifestation_metadata")

    @manifestation_metadata.setter
    def manifestation_metadata(self, value: ManifestationMetadata | None) -> None:
        object.__setattr__(
            self,
            "_manifestation_metadata",
            self._coerce_metadata_bundle("manifestation_metadata", value),
        )

    @property
    def item_metadata(self) -> ItemMetadata:
        return object.__getattribute__(self, "_item_metadata")

    @item_metadata.setter
    def item_metadata(self, value: ItemMetadata | None) -> None:
        object.__setattr__(
            self,
            "_item_metadata",
            self._coerce_metadata_bundle("item_metadata", value),
        )

    @property
    def work(self) -> WorkIdentityAPI | None:
        return self.work_metadata.work

    @work.setter
    def work(self, value: WorkIdentityAPI | None) -> None:
        self.work_metadata.work = value

    @property
    def expression(self) -> ExpressionIdentityAPI | None:
        return self.expression_metadata.expression

    @expression.setter
    def expression(self, value: ExpressionIdentityAPI | None) -> None:
        self.expression_metadata.expression = value

    @property
    def manifestation(self) -> ManifestationIdentityAPI | None:
        return self.manifestation_metadata.manifestation

    @manifestation.setter
    def manifestation(self, value: ManifestationIdentityAPI | None) -> None:
        self.manifestation_metadata.manifestation = value

    @property
    def item(self) -> ItemIdentityAPI | None:
        return self.item_metadata.item

    @item.setter
    def item(self, value: ItemIdentityAPI | None) -> None:
        self.item_metadata.item = value

    @property
    def wemi_stack(self) -> dict[WemiLevel, WemiMetadataBundle]:
        return {
            "work": self.work_metadata,
            "expression": self.expression_metadata,
            "manifestation": self.manifestation_metadata,
            "item": self.item_metadata,
        }

    @property
    def wemi_identities(self) -> dict[WemiLevel, WemiIdentity | None]:
        return {
            "work": self.work,
            "expression": self.expression,
            "manifestation": self.manifestation,
            "item": self.item,
        }

    @property
    def database_ids(self) -> dict[str, int | None]:
        work = self.work
        expression = self.expression
        manifestation = self.manifestation
        item = self.item
        return {
            "work_id": getattr(work, "work_id", None),
            "expression_id": getattr(expression, "expression_id", None),
            "expression_work_id": getattr(expression, "expression_work_id", None),
            "manifestation_id": getattr(
                manifestation,
                "manifestation_id",
                None,
            ),
            "manifestation_expression_id": getattr(
                manifestation,
                "manifestation_expression_id",
                None,
            ),
            "item_id": getattr(item, "item_id", None),
            "item_manifestation_id": getattr(item, "item_manifestation_id", None),
        }

    @property
    def relation_edge_ids(self) -> dict[WemiLevel, dict[str, tuple[RelationEdgeID, ...]]]:
        return {
            level: {
                relation: tuple(
                    link.edge_id
                    for link in metadata.get_relation_links(relation)
                    if link.edge_id is not None
                )
                for relation in metadata.relation_names()
            }
            for level, metadata in self.wemi_stack.items()
        }

    @property
    def titles(self) -> tuple[str, ...]:
        seen: set[str] = set()
        titles: list[str] = []
        for value in self._iter_title_candidates():
            if value is None:
                continue
            title = str(value).strip()
            if not title or title in seen:
                continue
            titles.append(title)
            seen.add(title)
        return tuple(titles)

    @property
    def canonical_title(self) -> str | None:
        work = self.work
        for value in (
            getattr(work, "work_canonical_title", None),
            getattr(work, "work_title", None),
            self.get("title", None),
        ):
            if value and str(value).strip():
                return str(value).strip()
        return None

    @property
    def display_title(self) -> str | None:
        titles = self.titles
        return titles[0] if titles else self.canonical_title

    @property
    def sort_title(self) -> str | None:
        work = self.work
        for value in (
            getattr(work, "work_sort_title", None),
            self.get("title_sort", None),
            self.display_title,
        ):
            if value and str(value).strip():
                return str(value).strip()
        return None

    def _iter_title_candidates(self) -> Iterable[str | None]:
        legacy_title = self.get("title", None)
        work = self.work
        expression = self.expression
        item = self.item

        yield legacy_title
        yield getattr(work, "work_canonical_title", None)
        yield getattr(work, "work_title", None)
        yield getattr(expression, "expression_title_override", None)
        yield getattr(expression, "expression_subtitle", None)
        yield getattr(expression, "expression_label", None)
        yield getattr(item, "item_source_name", None)

    def as_liuxin_metadata(self) -> "LiuXinWEMIMetadata":
        return self

    def as_calibre_metadata(self) -> Any:
        return self.to_calibre()

    def get_wemi_metadata(self, level: str) -> WemiMetadataBundle:
        normalized_level = self.normalize_wemi_level(level)
        return object.__getattribute__(
            self,
            self._METADATA_STORAGE_BY_LEVEL[normalized_level],
        )

    def get_wemi_identity(self, level: str) -> WemiIdentity | None:
        normalized_level = self.normalize_wemi_level(level)
        identity_attr = self._IDENTITY_ATTRIBUTE_BY_LEVEL[normalized_level]
        return getattr(self.get_wemi_metadata(normalized_level), identity_attr)

    def get_database_id(self, name: str) -> int | None:
        normalized_name = self._DATABASE_ID_ALIASES.get(
            str(name).strip().lower(),
            str(name).strip().lower(),
        )
        try:
            return self.database_ids[normalized_name]
        except KeyError as exc:
            raise KeyError(f"Unknown WEMI database id name: {name!r}") from exc

    def get_wemi_relation_links(
        self,
        level: str,
        relation: str,
    ) -> list[WemiRelationLink]:
        return self.get_wemi_metadata(level).get_relation_links(relation)

    def set_wemi_relation_links(
        self,
        level: str,
        relation: str,
        links: Iterable[WemiRelationLink],
    ) -> None:
        self.get_wemi_metadata(level).set_relation_links(relation, links)

    def add_wemi_relation_link(
        self,
        level: str,
        relation: str,
        link: WemiRelationLink,
    ) -> None:
        self.get_wemi_metadata(level).add_relation_link(relation, link)

    def get_wemi_related(
        self,
        level: str,
        relation: str,
    ) -> list[WemiRelationTarget]:
        return self.get_wemi_metadata(level).get_related(relation)

    def set_wemi_related(
        self,
        level: str,
        relation: str,
        values: Iterable[WemiRelationTarget],
    ) -> None:
        self.get_wemi_metadata(level).set_related(relation, values)

    def add_wemi_related(
        self,
        level: str,
        relation: str,
        value: WemiRelationTarget,
    ) -> None:
        self.get_wemi_metadata(level).add_related(relation, value)

    def get_wemi_relation_edge_ids(
        self,
        level: str,
        relation: str,
    ) -> tuple[RelationEdgeID, ...]:
        return tuple(
            link.edge_id
            for link in self.get_wemi_relation_links(level, relation)
            if link.edge_id is not None
        )

    def sync_legacy_title_from_wemi(self) -> str | None:
        title = self.canonical_title
        if title is not None:
            self.title = title
        return title

    def to_wemi_mapping(self, include_related: bool = True) -> dict[str, Any]:
        return {
            "work": self.work_metadata.to_mapping(include_related=include_related),
            "expression": self.expression_metadata.to_mapping(
                include_related=include_related,
            ),
            "manifestation": self.manifestation_metadata.to_mapping(
                include_related=include_related,
            ),
            "item": self.item_metadata.to_mapping(include_related=include_related),
        }

    def to_sidecar_mapping(
        self,
        include_related: bool = True,
        include_legacy: bool = True,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "schema": self.SIDECAR_SCHEMA_NAME,
            "schema_version": self.SIDECAR_SCHEMA_VERSION,
            "database_ids": self.database_ids,
            "relation_edge_ids": self.relation_edge_ids,
            "titles": list(self.titles),
            "wemi": self.to_wemi_mapping(include_related=include_related),
        }
        if include_legacy:
            payload["liuxin"] = self.get_data(rtn_deepcopy=True)
        return payload

    def to_mapping(
        self,
        include_related: bool = True,
        include_legacy: bool = True,
    ) -> dict[str, Any]:
        return self.to_sidecar_mapping(
            include_related=include_related,
            include_legacy=include_legacy,
        )

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "LiuXinWEMIMetadata":
        wemi_payload = payload.get("wemi", payload)
        if not isinstance(wemi_payload, Mapping):
            wemi_payload = {}

        metadata = cls(
            work_metadata=cls._bundle_from_mapping(
                WorkMetadata,
                wemi_payload.get("work"),
            ),
            expression_metadata=cls._bundle_from_mapping(
                ExpressionMetadata,
                wemi_payload.get("expression"),
            ),
            manifestation_metadata=cls._bundle_from_mapping(
                ManifestationMetadata,
                wemi_payload.get("manifestation"),
            ),
            item_metadata=cls._bundle_from_mapping(
                ItemMetadata,
                wemi_payload.get("item"),
            ),
        )

        liuxin_payload = payload.get("liuxin")
        if isinstance(liuxin_payload, Mapping):
            object.__getattribute__(metadata, "_data").update(
                deepcopy(dict(liuxin_payload)),
            )

        return metadata

    @classmethod
    def from_sidecar_mapping(cls, payload: Mapping[str, Any]) -> "LiuXinWEMIMetadata":
        return cls.from_mapping(payload)

    @classmethod
    def from_database(
        cls,
        database: Any,
        *,
        item_id: int | None = None,
        source_row: Mapping[str, Any] | Row | None = None,
    ) -> "LiuXinWEMIMetadata":
        from LiuXin_alpha.metadata.containers.metadata_containers.liuxin_wemi_metadata_hydrator import (
            LiuXinWEMIMetadataHydrator,
        )

        return LiuXinWEMIMetadataHydrator(database).get_liuxin_wemi_metadata(
            item_id=item_id,
            source_row=source_row,
        )

    @staticmethod
    def _bundle_from_mapping(
        bundle_type: type[WemiMetadataBundle],
        payload: Any,
    ) -> WemiMetadataBundle:
        if isinstance(payload, bundle_type):
            return payload
        if isinstance(payload, Mapping):
            return bundle_type.from_mapping(payload)
        return bundle_type()

    def deepcopy_metadata(self) -> "LiuXinWEMIMetadata":
        metadata = type(self)(
            work_metadata=deepcopy(self.work_metadata),
            expression_metadata=deepcopy(self.expression_metadata),
            manifestation_metadata=deepcopy(self.manifestation_metadata),
            item_metadata=deepcopy(self.item_metadata),
        )
        object.__setattr__(
            metadata,
            "_data",
            deepcopy(object.__getattribute__(self, "_data")),
        )
        return metadata

    def __deepcopy__(self, memo: dict[int, Any]) -> "LiuXinWEMIMetadata":
        existing = memo.get(id(self))
        if existing is not None:
            return existing

        metadata = type(self)(
            work_metadata=deepcopy(self.work_metadata, memo),
            expression_metadata=deepcopy(self.expression_metadata, memo),
            manifestation_metadata=deepcopy(self.manifestation_metadata, memo),
            item_metadata=deepcopy(self.item_metadata, memo),
        )
        memo[id(self)] = metadata
        object.__setattr__(
            metadata,
            "_data",
            deepcopy(object.__getattribute__(self, "_data"), memo),
        )
        return metadata

    def smart_update(
        self,
        other: CalibreLikeLiuXinBookMetaData,
        replace_metadata: bool = False,
    ) -> None:
        super().smart_update(other, replace_metadata=replace_metadata)

        if not isinstance(other, LiuXinWEMIMetadata):
            return

        for storage_name in self._METADATA_STORAGE_BY_LEVEL.values():
            current_bundle = object.__getattribute__(self, storage_name)
            incoming_bundle = object.__getattribute__(other, storage_name)
            if replace_metadata or not self._metadata_bundle_has_content(current_bundle):
                object.__setattr__(self, storage_name, deepcopy(incoming_bundle))

    @staticmethod
    def _metadata_bundle_has_content(bundle: WemiMetadataBundle) -> bool:
        for identity_attr in ("work", "expression", "manifestation", "item"):
            if hasattr(bundle, identity_attr) and getattr(bundle, identity_attr) is not None:
                return True

        for relation in bundle.relation_names():
            if bundle.get_relation_links(relation):
                return True
        return False


LiuXinWEMI = LiuXinWEMIMetadata


__all__ = [
    "LiuXinWEMI",
    "LiuXinWEMIMetadata",
    "WemiIdentity",
    "WemiLevel",
    "WemiMetadataBundle",
    "WemiRelationLink",
    "WemiRelationTarget",
]

"""API contract for item-centered LiuXin/WEMI metadata slices."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import Literal, Protocol, Self, TypeAlias

from LiuXin_alpha.metadata.api.calibre_metadata_api import CalibreMetadataAPI
from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api import (
    ExpressionIdentityAPI,
    ExpressionMetadataAPI,
    ExpressionRelationLink,
    ExpressionRelationTarget,
    ItemIdentityAPI,
    ItemMetadataAPI,
    ItemRelationLink,
    ItemRelationTarget,
    ManifestationIdentityAPI,
    ManifestationMetadataAPI,
    ManifestationRelationLink,
    ManifestationRelationTarget,
    MetadataRecord,
    RelationEdgeID,
    WorkIdentityAPI,
    WorkMetadataAPI,
    WorkRelationLink,
    WorkRelationTarget,
)
from LiuXin_alpha.metadata.api.liuxin_metadata_api import (
    LiuXinFieldMapping,
    LiuXinMetadataAPI,
)


WemiLevel: TypeAlias = Literal["work", "expression", "manifestation", "item"]
WemiDatabaseIDName: TypeAlias = Literal[
    "work",
    "work_id",
    "expression",
    "expression_id",
    "expression_work_id",
    "manifestation",
    "manifestation_id",
    "manifestation_expression_id",
    "item",
    "item_id",
    "item_manifestation_id",
]
WemiMetadataBundleAPI: TypeAlias = (
    WorkMetadataAPI
    | ExpressionMetadataAPI
    | ManifestationMetadataAPI
    | ItemMetadataAPI
)
WemiIdentityAPI: TypeAlias = (
    WorkIdentityAPI
    | ExpressionIdentityAPI
    | ManifestationIdentityAPI
    | ItemIdentityAPI
)
WemiRelationTargetAPI: TypeAlias = (
    WorkRelationTarget
    | ExpressionRelationTarget
    | ManifestationRelationTarget
    | ItemRelationTarget
)
WemiRelationLinkAPI: TypeAlias = (
    WorkRelationLink
    | ExpressionRelationLink
    | ManifestationRelationLink
    | ItemRelationLink
)
WemiMetadataStack: TypeAlias = Mapping[WemiLevel, WemiMetadataBundleAPI]
WemiIdentityStack: TypeAlias = Mapping[WemiLevel, WemiIdentityAPI | None]
WemiIdentityIDMap: TypeAlias = Mapping[str, int | None]
WemiRelationEdgeIDMap: TypeAlias = Mapping[
    WemiLevel,
    Mapping[str, tuple[RelationEdgeID, ...]],
]
WemiMetadataRecordMap: TypeAlias = Mapping[WemiLevel, MetadataRecord]
LiuXinWEMISidecarValue: TypeAlias = (
    str
    | int
    | Sequence[str]
    | WemiIdentityIDMap
    | WemiRelationEdgeIDMap
    | LiuXinFieldMapping
    | WemiMetadataRecordMap
)
LiuXinWEMISidecarMapping: TypeAlias = Mapping[str, LiuXinWEMISidecarValue]


class LiuXinWEMIMetadataAPI(LiuXinMetadataAPI, Protocol):
    """
    Structural API for a complete item metadata slice.

    The slice keeps legacy LiuXin/Calibre-compatible fields available while
    exposing the full W/E/M/I bundle chain and its database identity fields.
    """

    work_metadata: WorkMetadataAPI
    expression_metadata: ExpressionMetadataAPI
    manifestation_metadata: ManifestationMetadataAPI
    item_metadata: ItemMetadataAPI

    @property
    def liuxin(self) -> LiuXinMetadataAPI: ...

    @property
    def calibre(self) -> CalibreMetadataAPI: ...

    @property
    def work(self) -> WorkIdentityAPI | None: ...

    @work.setter
    def work(self, value: WorkIdentityAPI | None) -> None: ...

    @property
    def expression(self) -> ExpressionIdentityAPI | None: ...

    @expression.setter
    def expression(self, value: ExpressionIdentityAPI | None) -> None: ...

    @property
    def manifestation(self) -> ManifestationIdentityAPI | None: ...

    @manifestation.setter
    def manifestation(self, value: ManifestationIdentityAPI | None) -> None: ...

    @property
    def item(self) -> ItemIdentityAPI | None: ...

    @item.setter
    def item(self, value: ItemIdentityAPI | None) -> None: ...

    @property
    def wemi_stack(self) -> WemiMetadataStack: ...

    @property
    def wemi_identities(self) -> WemiIdentityStack: ...

    @property
    def database_ids(self) -> WemiIdentityIDMap: ...

    @property
    def relation_edge_ids(self) -> WemiRelationEdgeIDMap: ...

    @property
    def titles(self) -> tuple[str, ...]: ...

    @property
    def canonical_title(self) -> str | None: ...

    @property
    def display_title(self) -> str | None: ...

    @property
    def sort_title(self) -> str | None: ...

    @classmethod
    def from_mapping(cls, payload: Mapping[str, LiuXinWEMISidecarValue]) -> Self: ...

    @classmethod
    def from_sidecar_mapping(
        cls,
        payload: Mapping[str, LiuXinWEMISidecarValue],
    ) -> Self: ...

    def as_liuxin_metadata(self) -> LiuXinMetadataAPI: ...

    def as_calibre_metadata(self) -> CalibreMetadataAPI: ...

    def get_wemi_metadata(self, level: WemiLevel) -> WemiMetadataBundleAPI: ...

    def get_wemi_identity(self, level: WemiLevel) -> WemiIdentityAPI | None: ...

    def get_database_id(self, name: WemiDatabaseIDName) -> int | None: ...

    def get_wemi_relation_links(
        self,
        level: WemiLevel,
        relation: str,
    ) -> list[WemiRelationLinkAPI]: ...

    def set_wemi_relation_links(
        self,
        level: WemiLevel,
        relation: str,
        links: Iterable[WemiRelationLinkAPI],
    ) -> None: ...

    def add_wemi_relation_link(
        self,
        level: WemiLevel,
        relation: str,
        link: WemiRelationLinkAPI,
    ) -> None: ...

    def get_wemi_related(
        self,
        level: WemiLevel,
        relation: str,
    ) -> list[WemiRelationTargetAPI]: ...

    def set_wemi_related(
        self,
        level: WemiLevel,
        relation: str,
        values: Iterable[WemiRelationTargetAPI],
    ) -> None: ...

    def add_wemi_related(
        self,
        level: WemiLevel,
        relation: str,
        value: WemiRelationTargetAPI,
    ) -> None: ...

    def get_wemi_relation_edge_ids(
        self,
        level: WemiLevel,
        relation: str,
    ) -> tuple[RelationEdgeID, ...]: ...

    def to_wemi_mapping(self, include_related: bool = True) -> WemiMetadataRecordMap: ...

    def to_sidecar_mapping(
        self,
        include_related: bool = True,
        include_legacy: bool = True,
    ) -> LiuXinWEMISidecarMapping: ...

    def to_mapping(
        self,
        include_related: bool = True,
        include_legacy: bool = True,
    ) -> LiuXinWEMISidecarMapping: ...


LiuXinWEMIAPI: TypeAlias = LiuXinWEMIMetadataAPI


__all__ = [
    "LiuXinWEMIAPI",
    "LiuXinWEMIMetadataAPI",
    "LiuXinWEMISidecarMapping",
    "LiuXinWEMISidecarValue",
    "WemiDatabaseIDName",
    "WemiIdentityAPI",
    "WemiIdentityIDMap",
    "WemiIdentityStack",
    "WemiLevel",
    "WemiMetadataBundleAPI",
    "WemiMetadataRecordMap",
    "WemiMetadataStack",
    "WemiRelationEdgeIDMap",
    "WemiRelationLinkAPI",
    "WemiRelationTargetAPI",
]

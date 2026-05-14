"""API contract for item-centered LiuXin/WEMI metadata slices.

Category: high-level metadata slice API.
This module defines the sidecar-oriented metadata surface for a single item
while keeping the LiuXin/Calibre compatibility and full W/E/M/I stack visible.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import TYPE_CHECKING, Literal, Protocol, Self, TypeAlias

from LiuXin_alpha.metadata.api.containers_api.calibre_metadata_api import CalibreMetadataAPI
from LiuXin_alpha.metadata.api.containers_api.calibre_metadata_api.calibre_metadata_types import (
    CalibrePath,
)
from LiuXin_alpha.metadata.api.containers_api.metadata_write_api import (
    MetadataWriteDatabaseAPI,
    MetadataWriteReportAPI,
    MetadataWriteTargetRow,
)
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
    SupportsRowMapping,
)
from LiuXin_alpha.metadata.api.containers_api.liuxin_metadata_api import (
    LiuXinFieldMapping,
    LiuXinFieldValue,
    LiuXinMetadataAPI,
)

if TYPE_CHECKING:
    from LiuXin_alpha.metadata.api.from_database_api.metadata_read_source_api import (
        MetadataReadSourceAPI,
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
OPFMetadataSource: TypeAlias = CalibrePath | bytes
LazyLegacyTermValue: TypeAlias = str | int | float | bool | None
LazyLegacyTermMapping: TypeAlias = Mapping[str, LazyLegacyTermValue]
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
LazyHydratedFieldValue: TypeAlias = LiuXinFieldValue | LazyLegacyTermMapping
LazyLegacyValueLoaderAPI: TypeAlias = Callable[[], LazyLegacyTermMapping]
WemiRelationLoaderAPI: TypeAlias = Callable[[], Iterable[WemiRelationLinkAPI]]


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
    def liuxin(self) -> LiuXinMetadataAPI:
        """
        LiuXin-compatible metadata view for legacy callers.

        :return:
        """

    @property
    def calibre(self) -> CalibreMetadataAPI:
        """
        Calibre-compatible metadata view for plugin callers.

        :return:
        """

    @property
    def work(self) -> WorkIdentityAPI | None:
        """
        Work identity row at the top of this item metadata slice.

        :return:
        """

    @work.setter
    def work(self, value: WorkIdentityAPI | None) -> None:
        """
        Set the work identity row for this slice.

        :param value:
        :return:
        """

    @property
    def expression(self) -> ExpressionIdentityAPI | None:
        """
        Expression identity row for this item's realised work.

        :return:
        """

    @expression.setter
    def expression(self, value: ExpressionIdentityAPI | None) -> None:
        """
        Set the expression identity row for this slice.

        :param value:
        :return:
        """

    @property
    def manifestation(self) -> ManifestationIdentityAPI | None:
        """
        Manifestation identity row for this item's edition or format.

        :return:
        """

    @manifestation.setter
    def manifestation(self, value: ManifestationIdentityAPI | None) -> None:
        """
        Set the manifestation identity row for this slice.

        :param value:
        :return:
        """

    @property
    def item(self) -> ItemIdentityAPI | None:
        """
        Item identity row represented by this complete metadata slice.

        :return:
        """

    @item.setter
    def item(self, value: ItemIdentityAPI | None) -> None:
        """
        Set the item identity row for this slice.

        :param value:
        :return:
        """

    @property
    def wemi_stack(self) -> WemiMetadataStack:
        """
        All W/E/M/I metadata bundles keyed by level.

        :return:
        """

    @property
    def wemi_identities(self) -> WemiIdentityStack:
        """
        All W/E/M/I identity rows keyed by level.

        :return:
        """

    @property
    def database_ids(self) -> WemiIdentityIDMap:
        """
        Fundamental database ids and parent foreign keys for this slice.

        :return:
        """

    @property
    def relation_edge_ids(self) -> WemiRelationEdgeIDMap:
        """
        Persisted link-table edge ids grouped by WEMI level and relation key.

        :return:
        """

    @property
    def titles(self) -> tuple[str, ...]:
        """
        Convenience title strings derived from legacy and WEMI title fields.

        :return:
        """

    @property
    def canonical_title(self) -> str | None:
        """
        Best canonical title for the represented item.

        :return:
        """

    @property
    def display_title(self) -> str | None:
        """
        Preferred human-facing display title for the represented item.

        :return:
        """

    @property
    def sort_title(self) -> str | None:
        """
        Preferred sort title for the represented item.

        :return:
        """

    def sync_legacy_title_from_wemi(self) -> str | None:
        """
        Populate the legacy title field from the WEMI title surface.

        :return:
        """

    def sync_legacy_tags_from_wemi(self) -> tuple[str, ...]:
        """
        Populate legacy tags from WEMI tag relation targets.

        :return:
        """

    def sync_legacy_labels_from_wemi(self) -> tuple[str, ...]:
        """
        Populate legacy labels from WEMI label relation targets.

        :return:
        """

    def sync_legacy_genres_from_wemi(self) -> tuple[str, ...]:
        """
        Populate legacy genre values from WEMI genre relation targets.

        :return:
        """

    def sync_legacy_subjects_from_wemi(self) -> tuple[str, ...]:
        """
        Populate legacy subject values from WEMI subject relation targets.

        :return:
        """

    def sync_legacy_series_from_wemi(self) -> tuple[str, ...]:
        """
        Populate legacy series values from WEMI series relation targets.

        :return:
        """

    def sync_legacy_identifiers_from_wemi(self) -> tuple[tuple[str, str], ...]:
        """
        Populate legacy external identifiers from WEMI identifier relation targets.

        :return:
        """

    @classmethod
    def from_mapping(cls, payload: Mapping[str, LiuXinWEMISidecarValue]) -> Self:
        """
        Build a metadata slice from a sidecar-compatible mapping.

        :param payload:
        :return:
        """

    @classmethod
    def from_sidecar_mapping(
        cls,
        payload: Mapping[str, LiuXinWEMISidecarValue],
    ) -> Self:
        """
        Build a metadata slice from a sidecar mapping.

        :param payload:
        :return:
        """

    @classmethod
    def from_database(
        cls,
        database: MetadataReadSourceAPI,
        *,
        item_id: int | None = None,
        source_row: MetadataRecord | SupportsRowMapping | None = None,
    ) -> Self:
        """
        Build a complete item metadata slice from a database/read source.

        :param database:
        :param item_id:
        :param source_row:
        :return:
        """

    @classmethod
    def from_opf(
        cls,
        source: OPFMetadataSource,
        *,
        database: MetadataReadSourceAPI | None = None,
        item_id: int | None = None,
        source_row: MetadataRecord | SupportsRowMapping | None = None,
        replace_metadata: bool = False,
    ) -> Self:
        """
        Build a complete item metadata slice from OPF metadata.

        :param source:
        :param database:
        :param item_id:
        :param source_row:
        :param replace_metadata:
        :return:
        """

    def as_liuxin_metadata(self) -> LiuXinMetadataAPI:
        """
        Return the LiuXin-compatible metadata view.

        :return:
        """

    def as_calibre_metadata(self) -> CalibreMetadataAPI:
        """
        Return the Calibre-compatible metadata view.

        :return:
        """

    def pretty_string(
        self,
        *,
        include_empty: bool = False,
        include_relations: bool = True,
        include_legacy: bool = True,
    ) -> str:
        """
        Return a compact human-readable metadata summary.

        :param include_empty:
        :param include_relations:
        :param include_legacy:
        :return:
        """

    def to_pretty_string(
        self,
        *,
        include_empty: bool = False,
        include_relations: bool = True,
        include_legacy: bool = True,
    ) -> str:
        """
        Return a compact human-readable metadata summary.

        :param include_empty:
        :param include_relations:
        :param include_legacy:
        :return:
        """

    def write_to_database(
        self,
        database: MetadataWriteDatabaseAPI,
        *,
        fields: Iterable[str] | None = None,
        target_level: WemiLevel | None = "work",
        item_id: int | None = None,
        target_row: MetadataWriteTargetRow | None = None,
        replace: bool = False,
        mark_dirty: bool = True,
    ) -> MetadataWriteReportAPI:
        """
        Persist supported relation-backed metadata changes.

        :param database:
        :param fields:
        :param target_level:
        :param item_id:
        :param target_row:
        :param replace:
        :param mark_dirty:
        :return:
        """

    def __str__(self) -> str:
        """
        Return the default human-readable metadata summary.

        :return:
        """

    def get_wemi_metadata(self, level: WemiLevel) -> WemiMetadataBundleAPI:
        """
        Get the metadata bundle for a WEMI level.

        :param level:
        :return:
        """

    def get_wemi_identity(self, level: WemiLevel) -> WemiIdentityAPI | None:
        """
        Get the identity row for a WEMI level.

        :param level:
        :return:
        """

    def get_database_id(self, name: WemiDatabaseIDName) -> int | None:
        """
        Get one fundamental WEMI database id or parent foreign key.

        :param name:
        :return:
        """

    def get_wemi_relation_links(
        self,
        level: WemiLevel,
        relation_key: str,
    ) -> list[WemiRelationLinkAPI]:
        """
        Get relation links for one relation key on one WEMI bundle.

        :param level:
        :param relation_key: normalized relation bucket key from the selected
            bundle's ``RELATION_KEYS``.
        :return:
        """

    def set_wemi_relation_links(
        self,
        level: WemiLevel,
        relation_key: str,
        links: Iterable[WemiRelationLinkAPI],
    ) -> None:
        """
        Replace relation links for one relation key on one WEMI bundle.

        :param level:
        :param relation_key: normalized relation bucket key from the selected
            bundle's ``RELATION_KEYS``.
        :param links:
        :return:
        """

    def add_wemi_relation_link(
        self,
        level: WemiLevel,
        relation_key: str,
        link: WemiRelationLinkAPI,
    ) -> None:
        """
        Add one relation link to one WEMI bundle.

        :param level:
        :param relation_key: normalized relation bucket key from the selected
            bundle's ``RELATION_KEYS``.
        :param link:
        :return:
        """

    def get_wemi_related(
        self,
        level: WemiLevel,
        relation_key: str,
    ) -> list[WemiRelationTargetAPI]:
        """
        Get relation targets for one relation key on one WEMI bundle.

        :param level:
        :param relation_key: normalized relation bucket key from the selected
            bundle's ``RELATION_KEYS``.
        :return:
        """

    def set_wemi_related(
        self,
        level: WemiLevel,
        relation_key: str,
        values: Iterable[WemiRelationTargetAPI],
    ) -> None:
        """
        Replace relation targets for one relation key on one WEMI bundle.

        :param level:
        :param relation_key: normalized relation bucket key from the selected
            bundle's ``RELATION_KEYS``.
        :param values:
        :return:
        """

    def add_wemi_related(
        self,
        level: WemiLevel,
        relation_key: str,
        value: WemiRelationTargetAPI,
    ) -> None:
        """
        Add one relation target to one WEMI bundle.

        :param level:
        :param relation_key: normalized relation bucket key from the selected
            bundle's ``RELATION_KEYS``.
        :param value:
        :return:
        """

    def get_wemi_relation_edge_ids(
        self,
        level: WemiLevel,
        relation_key: str,
    ) -> tuple[RelationEdgeID, ...]:
        """
        Get persisted edge ids for one WEMI relation key.

        :param level:
        :param relation_key: normalized relation bucket key from the selected
            bundle's ``RELATION_KEYS``.
        :return:
        """

    def to_wemi_mapping(self, include_related: bool = True) -> WemiMetadataRecordMap:
        """
        Serialize the WEMI stack to mapping form.

        :param include_related:
        :return:
        """

    def to_sidecar_mapping(
        self,
        include_related: bool = True,
        include_legacy: bool = True,
    ) -> LiuXinWEMISidecarMapping:
        """
        Serialize this item metadata slice for sidecar storage.

        :param include_related:
        :param include_legacy:
        :return:
        """

    def to_mapping(
        self,
        include_related: bool = True,
        include_legacy: bool = True,
    ) -> LiuXinWEMISidecarMapping:
        """
        Serialize this metadata slice to its standard mapping form.

        :param include_related:
        :param include_legacy:
        :return:
        """


class LazyLiuXinWEMIMetadataAPI(LiuXinWEMIMetadataAPI, Protocol):
    """
    Structural API for lazy item-centred LiuXin/WEMI metadata slices.

    Lazy slices expose the same complete WEMI metadata surface as the eager
    object while allowing relation-backed legacy fields to be materialized on
    first read.
    """

    def install_lazy_value_to_id(
        self,
        field: str,
        loader: LazyLegacyValueLoaderAPI,
    ) -> None:
        """
        Install a lazy loader for a legacy value-to-id metadata field.

        :param field:
        :param loader:
        :return:
        """

    def install_lazy_relation_loader(
        self,
        level: WemiLevel,
        relation_key: str,
        loader: WemiRelationLoaderAPI,
    ) -> None:
        """
        Install a lazy loader for one WEMI relation key.

        :param level:
        :param relation_key: normalized relation bucket key from the selected
            bundle's ``RELATION_KEYS``.
        :param loader:
        :return:
        """

    def hydrate_field(self, field: str) -> LazyHydratedFieldValue:
        """
        Materialize one lazy legacy field and return its hydrated value.

        :param field:
        :return:
        """

    def force_hydrate(
        self,
        fields: Iterable[str] | None = None,
    ) -> Self:
        """
        Materialize all lazy fields, or only the supplied fields.

        :param fields:
        :return:
        """

    def lazy_fields(self) -> tuple[str, ...]:
        """
        Return legacy field names that are still lazy.

        :return:
        """

    def is_lazy_field_loaded(self, field: str) -> bool:
        """
        Return whether one lazy legacy field has been materialized.

        :param field:
        :return:
        """

    def lazy_legacy_terms_from_relation(
        self,
        *,
        field: str,
        relation_key: str,
    ) -> LazyLegacyTermMapping:
        """
        Build a legacy field mapping from one relation key across the WEMI stack.

        :param field:
        :param relation_key: normalized relation bucket key used to read target
            values from every WEMI bundle that exposes it.
        :return:
        """


LiuXinWEMIAPI: TypeAlias = LiuXinWEMIMetadataAPI
LazyLiuXinWEMIAPI: TypeAlias = LazyLiuXinWEMIMetadataAPI


__all__ = [
    "LazyHydratedFieldValue",
    "LazyLegacyTermMapping",
    "LazyLegacyTermValue",
    "LazyLegacyValueLoaderAPI",
    "LazyLiuXinWEMIAPI",
    "LazyLiuXinWEMIMetadataAPI",
    "LiuXinWEMIAPI",
    "LiuXinWEMIMetadataAPI",
    "LiuXinWEMISidecarMapping",
    "LiuXinWEMISidecarValue",
    "OPFMetadataSource",
    "WemiDatabaseIDName",
    "WemiIdentityAPI",
    "WemiIdentityIDMap",
    "WemiIdentityStack",
    "WemiLevel",
    "WemiMetadataBundleAPI",
    "WemiMetadataRecordMap",
    "WemiMetadataStack",
    "WemiRelationLoaderAPI",
    "WemiRelationEdgeIDMap",
    "WemiRelationLinkAPI",
    "WemiRelationTargetAPI",
]

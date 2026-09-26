"""
Define structural contracts for item metadata with a complete WEMI stack.

Legacy LiuXin fields coexist with identities, relation links, read-only projections
and sidecar serialization. Lazy implementations add explicit hydration controls;
these protocols perform no database work themselves.

Example:
    Exercise this contract with pytest::

        python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py
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
    ExpressionRelationKey,
    ExpressionRelationLink,
    ExpressionRelationTarget,
    ItemIdentityAPI,
    ItemMetadataAPI,
    ItemRelationKey,
    ItemRelationLink,
    ItemRelationTarget,
    ManifestationIdentityAPI,
    ManifestationMetadataAPI,
    ManifestationRelationKey,
    ManifestationRelationLink,
    ManifestationRelationTarget,
    MetadataRecord,
    MetadataTextViewAPI,
    MetadataValuesViewAPI,
    RelationLinkID,
    WemiIdentityAPI,
    WorkIdentityAPI,
    WorkMetadataAPI,
    WorkRelationKey,
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
WemiRelationKeyAPI: TypeAlias = (
    WorkRelationKey
    | ExpressionRelationKey
    | ManifestationRelationKey
    | ItemRelationKey
)
WemiMetadataStack: TypeAlias = Mapping[WemiLevel, WemiMetadataBundleAPI]
WemiIdentityStack: TypeAlias = Mapping[WemiLevel, WemiIdentityAPI | None]
WemiIdentityIDMap: TypeAlias = Mapping[str, int | None]
WemiRelationLinkIDMap: TypeAlias = Mapping[
    WemiLevel,
    Mapping[WemiRelationKeyAPI, tuple[RelationLinkID, ...]],
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
    | WemiRelationLinkIDMap
    | LiuXinFieldMapping
    | WemiMetadataRecordMap
)
LiuXinWEMISidecarMapping: TypeAlias = Mapping[str, LiuXinWEMISidecarValue]
LazyHydratedFieldValue: TypeAlias = LiuXinFieldValue | LazyLegacyTermMapping
LazyLegacyValueLoaderAPI: TypeAlias = Callable[[], LazyLegacyTermMapping]
WemiRelationLoaderAPI: TypeAlias = Callable[[], Iterable[WemiRelationLinkAPI]]


class LiuXinWEMIMetadataAPI(LiuXinMetadataAPI, Protocol):
    """
    Describe a complete item slice with legacy fields and four WEMI bundles.

    Bundle and identity access exposes the implementation's current objects. Calibre
    conversion creates a flat compatibility view and cannot preserve every relation link
    or its provenance.

    Example:
        Exercise this contract with pytest::

            python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py
    """

    work_metadata: WorkMetadataAPI
    expression_metadata: ExpressionMetadataAPI
    manifestation_metadata: ManifestationMetadataAPI
    item_metadata: ItemMetadataAPI

    @property
    def liuxin(self) -> LiuXinMetadataAPI:
        """
        Expose the legacy LiuXin-compatible view of this slice.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :return: The current metadata object through its legacy interface.
        """

    @property
    def calibre(self) -> CalibreMetadataAPI:
        """
        Convert this slice to a Calibre-compatible metadata view.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :return: New flat Calibre metadata object.
        """

    @property
    def work(self) -> WorkIdentityAPI | None:
        """
        Expose the identity row for the conceptual work.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :return: Current work identity, or None.
        """

    @work.setter
    def work(self, value: WorkIdentityAPI | None) -> None:
        """
        Replace the work identity on its existing metadata bundle.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :param value: Work identity to retain, or None to clear it.
        :return: None.
        """

    @property
    def expression(self) -> ExpressionIdentityAPI | None:
        """
        Expose the identity row for the realization of the work.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :return: Current expression identity, or None.
        """

    @expression.setter
    def expression(self, value: ExpressionIdentityAPI | None) -> None:
        """
        Replace the expression identity on its existing metadata bundle.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :param value: Expression identity to retain, or None to clear it.
        :return: None.
        """

    @property
    def manifestation(self) -> ManifestationIdentityAPI | None:
        """
        Expose the identity row for the edition or format.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :return: Current manifestation identity, or None.
        """

    @manifestation.setter
    def manifestation(self, value: ManifestationIdentityAPI | None) -> None:
        """
        Replace the manifestation identity on its existing metadata bundle.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :param value: Manifestation identity to retain, or None to clear it.
        :return: None.
        """

    @property
    def item(self) -> ItemIdentityAPI | None:
        """
        Expose the identity row for the represented item.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :return: Current item identity, or None.
        """

    @item.setter
    def item(self, value: ItemIdentityAPI | None) -> None:
        """
        Replace the item identity on its existing metadata bundle.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :param value: Item identity to retain, or None to clear it.
        :return: None.
        """

    @property
    def wemi_stack(self) -> WemiMetadataStack:
        """
        Collect work, expression, manifestation and item metadata bundles.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :return: Level-keyed mapping containing the current bundles.
        """

    @property
    def wemi_identities(self) -> WemiIdentityStack:
        """
        Collect the identity objects from all four WEMI levels.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :return: Level-keyed mapping of current identities, using None for missing
            identities.
        """

    @property
    def database_ids(self) -> WemiIdentityIDMap:
        """
        Collect database row ids and legacy parent-id hints.

        Parent-id hints are source-row values; relation links describe the complete graph.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :return: Mapping of named ids, with None for unavailable values.
        """

    @property
    def relation_link_ids(self) -> WemiRelationLinkIDMap:
        """
        Collect persisted relation-link ids by level and relation key.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :return: Nested mapping of tuples containing non-None link ids.
        """

    @property
    def values(self) -> MetadataValuesViewAPI:
        """
        Expose structured, read-only projections across the WEMI stack.

        Lazy dependencies may require an explicit load call before a projection can be read.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :return: Structured projection view backed by this slice.
        """

    @property
    def text(self) -> MetadataTextViewAPI:
        """
        Expose display/export text projections across the WEMI stack.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :return: Text projection view backed by the structured values view.
        """

    def load(self, *fields: str) -> Self:
        """
        Load dependencies for requested projection fields.

        Eager implementations may return immediately. Lazy implementations load requested
        dependencies, or all pending dependencies when no fields are supplied.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :param fields: Projection field names; no names requests every pending dependency.
        :return: This metadata slice.
        """

    @property
    def titles(self) -> tuple[str, ...]:
        """
        Collect distinct nonblank title candidates from legacy fields and WEMI identities.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :return: Ordered tuple of title strings.
        """

    @property
    def canonical_title(self) -> str | None:
        """
        Choose the canonical work title, falling back to the work title and legacy title.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :return: First nonblank title after trimming, or None.
        """

    @property
    def display_title(self) -> str | None:
        """
        Choose the first available title candidate for display.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :return: Display title, falling back to the canonical title, or None.
        """

    @property
    def sort_title(self) -> str | None:
        """
        Choose a work sort title, legacy title_sort or display title.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :return: First nonblank sort candidate after trimming, or None.
        """

    def sync_legacy_title_from_wemi(self) -> str | None:
        """
        Copy the canonical WEMI title into the legacy title field when available.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :return: Selected title, or None when no title is available.
        """

    def sync_legacy_tags_from_wemi(self) -> tuple[str, ...]:
        """
        Add supported WEMI tags to their legacy metadata fields.

        Existing values are retained; case-insensitive duplicate values are omitted.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :return: Tuple of newly added term strings in stack traversal order.
        """

    def sync_legacy_labels_from_wemi(self) -> tuple[str, ...]:
        """
        Add supported WEMI labels to their legacy metadata fields.

        Existing values are retained; case-insensitive duplicate values are omitted.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :return: Tuple of newly added term strings in stack traversal order.
        """

    def sync_legacy_genres_from_wemi(self) -> tuple[str, ...]:
        """
        Add supported WEMI genres to their legacy metadata fields.

        Existing values are retained; case-insensitive duplicate values are omitted.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :return: Tuple of newly added term strings in stack traversal order.
        """

    def sync_legacy_subjects_from_wemi(self) -> tuple[str, ...]:
        """
        Add supported WEMI subjects to their legacy metadata fields.

        Existing values are retained; case-insensitive duplicate values are omitted.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :return: Tuple of newly added term strings in stack traversal order.
        """

    def sync_legacy_series_from_wemi(self) -> tuple[str, ...]:
        """
        Add supported WEMI series to their legacy metadata fields.

        Existing values are retained; case-insensitive duplicate values are omitted.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :return: Tuple of newly added term strings in stack traversal order.
        """

    def sync_legacy_identifiers_from_wemi(self) -> tuple[tuple[str, str], ...]:
        """
        Add supported WEMI identifiers to their legacy metadata fields.

        Existing values are retained; case-insensitive duplicate values are omitted.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :return: Tuple of newly saved scheme/value pairs.
        """

    @classmethod
    def from_mapping(cls, payload: Mapping[str, LiuXinWEMISidecarValue]) -> Self:
        """
        Build a slice from a sidecar mapping or a mapping of WEMI levels.

        The concrete implementation copies legacy data and uses empty bundles for absent
        levels.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :param payload: Sidecar mapping with optional wemi and liuxin entries, or a mapping
            of WEMI levels.
        :return: New metadata slice.
        """

    @classmethod
    def from_sidecar_mapping(
        cls,
        payload: Mapping[str, LiuXinWEMISidecarValue],
    ) -> Self:
        """
        Build a slice using the standard mapping deserializer.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :param payload: Sidecar mapping with optional wemi and liuxin entries, or a mapping
            of WEMI levels.
        :return: New metadata slice.
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
        Hydrate an item slice from a database and item/source-row hints.

        At least one of item_id and source_row is required; the caller owns the database
        lifetime.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :param database: Caller-owned database/read source retained for metadata access; it
            is not closed here.
        :param item_id: Optional item row id; overrides an item id extracted from
            source_row.
        :param source_row: Database Row or source mapping supplying identity fields and
            row-id hints.
        :return: Hydrated metadata slice.
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
        Import OPF metadata, optionally merging it with a database-backed item slice.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :param source: OPF bytes or a filesystem path accepted by the OPF reader.
        :param database: Optional caller-owned database for loading the item slice before
            merging OPF fields.
        :param item_id: Optional item row id; overrides an item id extracted from
            source_row.
        :param source_row: Database Row or source mapping supplying identity fields and
            row-id hints.
        :param replace_metadata: Replace supported existing metadata when True; otherwise
            use the family merge rules.
        :return: Metadata slice containing the imported fields.
        """

    def as_liuxin_metadata(self) -> LiuXinMetadataAPI:
        """
        Expose this slice through the legacy LiuXin interface.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :return: This metadata object.
        """

    def as_calibre_metadata(self) -> CalibreMetadataAPI:
        """
        Convert supported WEMI values into a flat Calibre metadata object.

        The concrete implementation projects a copy so legacy field storage is preserved.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :return: New Calibre-compatible metadata object.
        """

    def pretty_string(
        self,
        *,
        include_empty: bool = False,
        include_relations: bool = True,
        include_legacy: bool = True,
    ) -> str:
        """
        Render titles, ids, bundle identities and optional relation/legacy summaries.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :param include_empty: Include empty mapping entries in the diagnostic summary when
            True.
        :param include_relations: Include relation counts in the diagnostic summary when
            True.
        :param include_legacy: Include the legacy LiuXin field mapping when True.
        :return: Human-readable multiline metadata description.
        """

    def to_pretty_string(
        self,
        *,
        include_empty: bool = False,
        include_relations: bool = True,
        include_legacy: bool = True,
    ) -> str:
        """
        Render the same diagnostic summary as pretty_string.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :param include_empty: Include empty mapping entries in the diagnostic summary when
            True.
        :param include_relations: Include relation counts in the diagnostic summary when
            True.
        :param include_legacy: Include the legacy LiuXin field mapping when True.
        :return: Human-readable multiline metadata description.
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
        Persist supported relation-backed metadata through the WEMI writer.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :param database: Caller-owned writable database passed to the WEMI writer.
        :param fields: Optional relation-field names; None uses the writer default field
            set.
        :param target_level: Destination WEMI level, defaulting to work; None allows writer
            resolution.
        :param item_id: Optional item row id; overrides an item id extracted from
            source_row.
        :param target_row: Optional explicit target Row or row mapping.
        :param replace: Replace existing supported relations when True; otherwise add
            missing values.
        :param mark_dirty: Mark changed metadata dirty when True.
        :return: Write report describing persisted, skipped and failed operations.
        """

    def __str__(self) -> str:
        """
        Render the default human-readable metadata summary.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :return: Diagnostic metadata description.
        """

    def get_wemi_metadata(self, level: WemiLevel) -> WemiMetadataBundleAPI:
        """
        Select the metadata bundle for one WEMI level.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :param level: WEMI level identifying the bundle to access.
        :return: Current bundle object.
        """

    def get_wemi_identity(self, level: WemiLevel) -> WemiIdentityAPI | None:
        """
        Select the identity attached to one WEMI bundle.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :param level: WEMI level identifying the bundle to access.
        :return: Current identity, or None.
        """

    def get_database_id(self, name: WemiDatabaseIDName) -> int | None:
        """
        Look up a row id or legacy source-row parent-id hint.

        The concrete implementation accepts short level aliases and raises KeyError for
        unknown names.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :param name: Row-id name, parent-id hint name, or short WEMI level alias.
        :return: Stored id, or None for a known but unset id.
        """

    def get_wemi_relation_links(
        self,
        level: WemiLevel,
        relation_key: WemiRelationKeyAPI,
    ) -> list[WemiRelationLinkAPI]:
        """
        Read links from one relation bucket on a WEMI bundle.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :param level: WEMI level identifying the bundle to access.
        :param relation_key: normalized relation bucket key from the selected bundle's
            ``RELATION_KEYS``.
        :return: List of relation links; lazy implementations may materialize the bucket.
        """

    def set_wemi_relation_links(
        self,
        level: WemiLevel,
        relation_key: WemiRelationKeyAPI,
        links: Iterable[WemiRelationLinkAPI],
    ) -> None:
        """
        Replace the links in one relation bucket.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :param level: WEMI level identifying the bundle to access.
        :param relation_key: normalized relation bucket key from the selected bundle's
            ``RELATION_KEYS``.
        :param links: Iterable of relation links replacing the selected bucket.
        :return: None.
        """

    def add_wemi_relation_link(
        self,
        level: WemiLevel,
        relation_key: WemiRelationKeyAPI,
        link: WemiRelationLinkAPI,
    ) -> None:
        """
        Append a link to one relation bucket.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :param level: WEMI level identifying the bundle to access.
        :param relation_key: normalized relation bucket key from the selected bundle's
            ``RELATION_KEYS``.
        :param link: Relation link to add or designate as preferred.
        :return: None.
        """

    def get_primary_wemi_relation_link(
        self,
        level: WemiLevel,
        relation_key: WemiRelationKeyAPI,
    ) -> WemiRelationLinkAPI | None:
        """
        Select the preferred link using the bundle's primary/priority rules.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :param level: WEMI level identifying the bundle to access.
        :param relation_key: normalized relation bucket key from the selected bundle's
            ``RELATION_KEYS``.
        :return: Preferred relation link, or None for an empty bucket.
        """

    def set_primary_wemi_relation_link(
        self,
        level: WemiLevel,
        relation_key: WemiRelationKeyAPI,
        link: WemiRelationLinkAPI,
    ) -> None:
        """
        Designate a link as primary within the selected relation bucket.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :param level: WEMI level identifying the bundle to access.
        :param relation_key: normalized relation bucket key from the selected bundle's
            ``RELATION_KEYS``.
        :param link: Relation link to add or designate as preferred.
        :return: None.
        """

    def get_wemi_related(
        self,
        level: WemiLevel,
        relation_key: WemiRelationKeyAPI,
    ) -> list[WemiRelationTargetAPI]:
        """
        Project target objects from a relation bucket.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :param level: WEMI level identifying the bundle to access.
        :param relation_key: normalized relation bucket key from the selected bundle's
            ``RELATION_KEYS``.
        :return: List of relation targets.
        """

    def get_primary_wemi_related(
        self,
        level: WemiLevel,
        relation_key: WemiRelationKeyAPI,
    ) -> WemiRelationTargetAPI | None:
        """
        Project the target of the preferred relation link.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :param level: WEMI level identifying the bundle to access.
        :param relation_key: normalized relation bucket key from the selected bundle's
            ``RELATION_KEYS``.
        :return: Preferred target, or None for an empty bucket.
        """

    def set_wemi_related(
        self,
        level: WemiLevel,
        relation_key: WemiRelationKeyAPI,
        values: Iterable[WemiRelationTargetAPI],
    ) -> None:
        """
        Replace a relation bucket using target objects.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :param level: WEMI level identifying the bundle to access.
        :param relation_key: normalized relation bucket key from the selected bundle's
            ``RELATION_KEYS``.
        :param values: Iterable of relation targets replacing the selected bucket.
        :return: None.
        """

    def add_wemi_related(
        self,
        level: WemiLevel,
        relation_key: WemiRelationKeyAPI,
        value: WemiRelationTargetAPI,
    ) -> None:
        """
        Add a target object to the selected relation bucket.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :param level: WEMI level identifying the bundle to access.
        :param relation_key: normalized relation bucket key from the selected bundle's
            ``RELATION_KEYS``.
        :param value: Relation target to append to the selected bucket.
        :return: None.
        """

    def get_wemi_relation_link_ids(
        self,
        level: WemiLevel,
        relation_key: WemiRelationKeyAPI,
    ) -> tuple[RelationLinkID, ...]:
        """
        Read the persisted ids of links in a relation bucket.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :param level: WEMI level identifying the bundle to access.
        :param relation_key: normalized relation bucket key from the selected bundle's
            ``RELATION_KEYS``.
        :return: Tuple of non-None link ids in bucket order.
        """

    def to_wemi_mapping(self, include_related: bool = True) -> WemiMetadataRecordMap:
        """
        Serialize each WEMI bundle under its level name.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :param include_related: Include relation targets and link payloads in each
            serialized bundle when True.
        :return: Mapping of WEMI levels to serialized bundle records.
        """

    def to_sidecar_mapping(
        self,
        include_related: bool = True,
        include_legacy: bool = True,
    ) -> LiuXinWEMISidecarMapping:
        """
        Serialize a sidecar with schema, ids, titles and WEMI bundle records.

        Legacy fields are deep-copied when included. This constructs a mapping without
        writing a file.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :param include_related: Include relation targets and link payloads in each
            serialized bundle when True.
        :param include_legacy: Include the legacy LiuXin field mapping when True.
        :return: Sidecar-compatible mapping.
        """

    def to_mapping(
        self,
        include_related: bool = True,
        include_legacy: bool = True,
    ) -> LiuXinWEMISidecarMapping:
        """
        Serialize using the standard sidecar mapping format.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :param include_related: Include relation targets and link payloads in each
            serialized bundle when True.
        :param include_legacy: Include the legacy LiuXin field mapping when True.
        :return: Sidecar-compatible mapping.
        """


class LazyLiuXinWEMIMetadataAPI(LiuXinWEMIMetadataAPI, Protocol):
    """
    Describe a WEMI slice with deferred legacy mappings and relation buckets.

    Explicit load calls prepare projection dependencies. Implementations may also
    hydrate through legacy field reads and copying; callers must keep backing resources
    available until loading completes.

    Example:
        Exercise this contract with pytest::

            python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py
    """

    def install_lazy_value_to_id(
        self,
        field: str,
        loader: LazyLegacyValueLoaderAPI,
    ) -> None:
        """
        Replace a legacy field with a deferred value-to-id mapping.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :param field: Legacy metadata field name; supported aliases are normalized by the
            implementation.
        :param loader: Zero-argument callable returning the field's value-to-id mapping when
            first accessed.
        :return: None.
        """

    def install_lazy_relation_loader(
        self,
        level: WemiLevel,
        relation_key: WemiRelationKeyAPI,
        loader: WemiRelationLoaderAPI,
    ) -> None:
        """
        Register a deferred loader for one validated WEMI relation bucket.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :param level: WEMI level identifying the bundle to access.
        :param relation_key: normalized relation bucket key from the selected bundle's
            ``RELATION_KEYS``.
        :param loader: Zero-argument callable returning relation links when the bucket is
            first accessed.
        :return: None.
        """

    def hydrate_field(self, field: str) -> LazyHydratedFieldValue:
        """
        Materialize one legacy field, normalizing supported aliases.

        Identifier hydration synchronizes WEMI identifier relations. A materialized lazy
        mapping replaces its wrapper in legacy storage.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :param field: Legacy metadata field name; supported aliases are normalized by the
            implementation.
        :return: Hydrated field value, or None for an absent ordinary field.
        """

    def force_hydrate(
        self,
        fields: Iterable[str] | None = None,
    ) -> Self:
        """
        Materialize selected legacy fields, or every deferred legacy field by default.

        This does not guarantee every unrelated WEMI relation loader has run; load without
        arguments requests all dependencies.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :param fields: Legacy field names to materialize; None selects all deferred legacy
            fields and pending identifiers.
        :return: This metadata slice.
        """

    def lazy_fields(self) -> tuple[str, ...]:
        """
        List legacy fields still represented by lazy wrappers and pending identifiers.

        A wrapper can have loaded internally and still appear until hydrate_field replaces
        it.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :return: Tuple of deferred-wrapper field names, plus identifiers while
            synchronization is pending.
        """

    def is_lazy_field_loaded(self, field: str) -> bool:
        """
        Inspect a legacy wrapper's load flag or identifier synchronization state.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :param field: Legacy metadata field name; supported aliases are normalized by the
            implementation.
        :return: True when the field is loaded or has no deferred wrapper; False when still
            pending.
        """

    def lazy_legacy_terms_from_relation(
        self,
        *,
        field: str,
        relation_key: WemiRelationKeyAPI,
    ) -> LazyLegacyTermMapping:
        """
        Collect legacy terms from one relation bucket across all WEMI levels.

        Ordinary terms retain the first case-insensitive occurrence and its row id. Rating
        fields use source-to-rating values instead.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_wemi_metadata_api.py


        :param field: Legacy metadata field name; supported aliases are normalized by the
            implementation.
        :param relation_key: normalized relation bucket key from the selected bundle's
            ``RELATION_KEYS``.
        :return: Mapping of display terms to row ids, or rating sources to rating values.
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
    "WemiRelationLinkIDMap",
    "WemiRelationKeyAPI",
    "WemiRelationLinkAPI",
    "WemiRelationTargetAPI",
]

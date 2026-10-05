
"""
Describe the legacy title-oriented LiuXin metadata interface and its supporting row/database shapes.

The container presents a combined book view with multi-value creators, identifiers
and relation ids rather than individual WEMI entities. Protocol methods declare
interfaces without providing behavior or runtime validation.

Example:
    Exercise the owning behavior with pytest::

        python -m pytest -q tests/metadata/api/test_liuxin_metadata_api.py
"""


from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Protocol, Sequence, Mapping, Self, Iterable

from LiuXin_alpha.metadata.api.containers_api.calibre_metadata_api import (
    CalibreCloseableAPI,
    CalibreFilePayload,
    CalibreIdentifierMapping,
    CalibreIdentifierSnapshot,
    CalibreIdentifierValue,
    CalibreMetadataAPI,
    CalibreMetadataInputAPI,
    CalibrePath,
    CalibreUserMetadata,
)
from LiuXin_alpha.metadata.api.containers_api.liuxin_metadata_api.liuxin_metadata_types import (
    LiuXinValueToID,
    LiuXinCreatorMapping,
    LiuXinPayloadToID,
    LiuXinFieldValue,
    LiuXinRatingMapping,
    LiuXinCreatorDump,
    LiuXinFieldKeys,
    LiuXinFieldMapping,
)
from LiuXin_alpha.metadata.api.containers_api.metadata_write_api import (
    MetadataWriteDatabaseAPI,
    MetadataWriteReportAPI,
    MetadataWriteTargetRow,
)


class LiuXinMetadataDatabaseAPI(Protocol):
    """
    Declare the table-categorization and display-column methods of the legacy database contract.

    This small protocol does not enumerate every capability of concrete hydration paths;
    the legacy factory also uses linked-row lookup and a driver wrapper.

    Example:
        Exercise the owning behavior with pytest::

            python -m pytest -q tests/metadata/api/test_liuxin_metadata_api.py
    """

    def get_categorized_tables(self) -> Mapping[str, Sequence[str]]:
        """
        Return database table names grouped by their schema categories.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_metadata_api.py


        :return: Category-name-to-table-name-sequence mapping.
        """
        ...

    def get_display_column(self, table: str) -> str:
        """
        Resolve the display-value column name for a database table.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_metadata_api.py


        :param table: Database table name to inspect.
        :return: Column name used to display rows of the table.
        """
        ...


class LiuXinTitleRowAPI(Protocol):
    """
    Describe a title-row object with a database reference and keyed column access.

    The contract supplies no row storage or loading implementation.

    Example:
        Exercise the owning behavior with pytest::

            python -m pytest -q tests/metadata/api/test_liuxin_metadata_api.py
    """

    db: LiuXinMetadataDatabaseAPI

    def __getitem__(self, item: str) -> LiuXinFieldValue:
        """
        Retrieve a named column from the title row.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_metadata_api.py


        :param item: Exact database column name.
        :return: Column value in the supported LiuXin field-value shapes.
        """
        ...


class LiuXinMetadataAPI(Protocol):
    """
    Specify legacy extended book metadata with creators, identifiers, optional row ids, and payloads.

    Implementing objects provide storage, normalization, copying, cleanup and adapters.
    This protocol is not runtime-checkable; its declared types do not validate inputs or
    repair legacy container differences.

    Example:
        >>> from LiuXin_alpha.metadata.containers.calibre_like_book_metadata import CalibreLikeLiuXinBookMetaData
        >>> book: LiuXinMetadataAPI = CalibreLikeLiuXinBookMetaData('Example', ['Writer'])
        >>> book.get_authors_copy()
        ['Writer']
    """

    title: str | None
    title_sort: str | None
    authors: LiuXinValueToID
    creator_sort: str | None
    creators: LiuXinCreatorMapping
    identifiers: CalibreIdentifierSnapshot
    internal_identifiers: CalibreIdentifierSnapshot
    comments: LiuXinValueToID
    cover_data: LiuXinPayloadToID
    custom_field_keys: Sequence[str]
    custom_fields: Mapping[str, LiuXinFieldValue]
    device_collections: Sequence[str]
    doc_type: str | None
    genre: LiuXinValueToID
    filename: Sequence[str]
    filepath: Sequence[CalibrePath]
    files: LiuXinPayloadToID
    imprint: LiuXinValueToID
    language: str | None
    labels: LiuXinValueToID
    languages: Sequence[str]
    languages_available: LiuXinValueToID
    last_modified: datetime | None
    metadata_date: datetime | None
    metadata_language: str | None
    notes: LiuXinValueToID
    program_str: str
    pubdate: datetime | None
    publisher: LiuXinValueToID
    publication_tye: str | None
    ratings: LiuXinRatingMapping
    rights: str | None
    series: LiuXinValueToID
    series_index: Mapping[str, str | int | float | None]
    subject: LiuXinValueToID
    synopses: LiuXinValueToID
    tags: LiuXinValueToID
    timestamp: datetime | None
    user_metadata: CalibreUserMetadata
    wordcount: int | None

    @classmethod
    def from_calibre(cls, calibre_md: CalibreMetadataInputAPI) -> Self:
        """
        Construct this metadata family from a Calibre-readable source.

        Only supported fields are imported; additional source attributes may be used when
        available.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_metadata_api.py


        :param calibre_md: Calibre-readable metadata source exposing title, authors, and
            identifiers.
        :return: New instance of the implementing class.
        """

    def setattr(self, key: str, value: LiuXinFieldValue) -> None:
        """
        Assign one field through the implementing container's normal metadata setter.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_metadata_api.py


        :param key: Metadata storage key.
        :param value: Value to store using the operation's field rules.
        :return: None.
        """

    def get(
        self,
        field: str,
        default: LiuXinFieldValue = None,
    ) -> LiuXinFieldValue:
        """
        Read a field with a caller-supplied fallback for an unavailable value.

        Copying and null/default handling follow the concrete container's read policy.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_metadata_api.py


        :param field: Metadata field name.
        :param default: Fallback used by the implementation for a missing value.
        :return: Field value or fallback.
        """

    def get_extra(
        self,
        field: str,
        default: LiuXinFieldValue = None,
    ) -> LiuXinFieldValue:
        """
        Read a custom-field auxiliary value using the container's compatibility policy.

        The legacy LiuXin container returns the whole user-metadata entry and can signal an
        undefined field with AttributeError.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_metadata_api.py


        :param field: Metadata field name.
        :param default: Fallback used by the implementation for a missing value.
        :return: Custom-field result or fallback where the implementation supports it.
        """

    def read_creators(self, creators_dict: Mapping[str, str | Sequence[str]]) -> None:
        """
        Read role-to-name values through the container's ordinary creator assignment path.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_metadata_api.py


        :param creators_dict: Role-to-creator mapping, in the form required by this
            operation.
        :return: None.
        """

    def direct_get(self, item: str) -> LiuXinFieldValue:
        """
        Read an exact internal metadata entry without the normal field conversion layer.

        The legacy implementation exposes mutable storage and signals unknown names with
        AttributeError.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_metadata_api.py


        :param item: Exact field or column lookup name.
        :return: Stored field value; callers must account for possible shared mutable data.
        """

    def write_to_database(
        self,
        database: MetadataWriteDatabaseAPI,
        *,
        fields: Iterable[str] | None = None,
        target_level: str = "work",
        item_id: int | None = None,
        target_row: MetadataWriteTargetRow | None = None,
        replace: bool = False,
        mark_dirty: bool = True,
    ) -> MetadataWriteReportAPI:
        """
        Persist supported relation-backed fields through the metadata writer.

        The implementation resolves the WEMI target from explicit row/item information or
        available database ids and reports changes, skips, and errors.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_metadata_api.py


        :param database: Caller-owned database used by the WEMI writer for persistence; this
            method does not close it.
        :param fields: Optional relation-field iterable; None selects writer defaults.
        :param target_level: WEMI level for the target relations, defaulting to work.
        :param item_id: Optional LiuXin item id used to resolve the write target.
        :param target_row: Optional explicit row or mapping identifying the write target.
        :param replace: Whether to replace selected existing relations.
        :param mark_dirty: Whether the writer should mark changed metadata dirty.
        :return: MetadataWriteReportAPI describing the attempted persistence.
        """

    def to_opf_bytes(self, *, default_lang: str | None = None) -> bytes:
        """
        Serialize this metadata as an OPF document.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_metadata_api.py


        :param default_lang: Optional default language forwarded to OPF serialization.
        :return: Serialized OPF bytes.
        """

    def write_to_opf(
        self,
        path: CalibrePath,
        *,
        default_lang: str | None = None,
    ) -> Path:
        """
        Serialize this metadata to an OPF destination path.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_metadata_api.py


        :param path: Destination OPF path; the shared adapter creates parents and overwrites
            the file.
        :param default_lang: Optional default language forwarded to OPF serialization.
        :return: Path of the written OPF file.
        """

    def __getitem__(self, item: str) -> LiuXinFieldValue:
        """
        Retrieve an exact stored metadata key using mapping-style access.

        The legacy container returns the live value and signals missing keys with KeyError.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_metadata_api.py


        :param item: Exact field or column lookup name.
        :return: Stored field value, possibly shared.
        """

    def __iter__(self) -> Iterable[str]:
        """
        Iterate top-level metadata storage names.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_metadata_api.py


        :return: Iterable of field keys.
        """

    def nullify(self, field: str) -> None:
        """
        Reset a field or supported grouped creator/identifier collection to its null state.

        Recognized fields and unknown-key handling are defined by the implementation.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_metadata_api.py


        :param field: Metadata field name.
        :return: None.
        """

    def get_identifiers(self) -> CalibreIdentifierSnapshot:
        """
        Return an external-identifier snapshot keyed by scheme.

        The value shape may be a string or collection; consumers should not assume a single
        identifier per scheme.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_metadata_api.py


        :return: Identifier snapshot; the legacy LiuXin implementation returns copied sets.
        """

    def get_internal_identifiers(self) -> CalibreIdentifierSnapshot:
        """
        Return an internal-identifier snapshot separately from external bibliographic identifiers.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_metadata_api.py


        :return: Internal scheme-to-values snapshot.
        """

    def read_identifiers(self, identifiers: CalibreIdentifierMapping) -> None:
        """
        Read external identifiers through the implementing container's identifier setter.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_metadata_api.py


        :param identifiers: Scheme-to-value mapping using the supported string, sequence,
            set, or value-to-id forms.
        :return: None.
        """

    def set_identifiers(self, identifiers: CalibreIdentifierMapping) -> None:
        """
        Apply a mapping of external identifiers according to the container's update policy.

        The legacy LiuXin implementation adds strings/collections and only uses its optional
        update flag to replace OrderedDict values; it does not clear unmentioned schemes.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_metadata_api.py


        :param identifiers: Scheme-to-value mapping using the supported string, sequence,
            set, or value-to-id forms.
        :return: None.
        """

    def set_identifier(self, typ: str, val: CalibreIdentifierValue) -> None:
        """
        Set an identifier under a normalized scheme, or clear that scheme for a supported null value.

        Concrete containers differ in accepted value shapes and deletion rules.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_metadata_api.py


        :param typ: Identifier scheme name or supported alias.
        :param val: Identifier or field value accepted by the implementing container.
        :return: None.
        """

    def has_identifier(self, typ: str) -> bool:
        """
        Test whether the requested identifier scheme has a stored value.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_metadata_api.py


        :param typ: Identifier scheme name or supported alias.
        :return: Whether the scheme has identifier content.
        """

    def get_authors_copy(self) -> list[str]:
        """
        Copy author names without their database row information.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_metadata_api.py


        :return: Independent list of author names.
        """

    def get_creators_dump(self) -> LiuXinCreatorDump:
        """
        Return creator-role mappings including each name's row identifier information.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_metadata_api.py


        :return: Role-to-value-to-id snapshot.
        """

    def direct_add(
        self,
        key: str,
        value: LiuXinFieldValue,
        key_check: bool = True,
    ) -> None:
        """
        Insert or replace an internal value while bypassing normal field conversion.

        This operation may retain the supplied object directly; key_check requests
        existing-key validation.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_metadata_api.py


        :param key: Metadata storage key.
        :param value: Value to store using the operation's field rules.
        :param key_check: Whether direct insertion must reject keys absent from storage.
        :return: None.
        """

    def add_cover(
        self,
        data: CalibreFilePayload,
        typ: str = "path",
        cover_id: int | None = None,
    ) -> None:
        """
        Validate and add a cover payload with its type marker and optional database id.

        Legacy implementations consume readable inputs without closing the original stream;
        explicit cleanup registration controls later ownership.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_metadata_api.py


        :param data: File/cover path, bytes, or binary-readable payload.
        :param typ: Payload type marker, defaulting to path.
        :param cover_id: Optional database id associated with the cover payload.
        :return: None.
        """

    def add_file(
        self,
        data: CalibreFilePayload,
        typ: str = "path",
        file_id: int | None = None,
    ) -> None:
        """
        Add a file payload with a type marker and optional database id.

        Legacy implementations consume readable inputs to bytes without closing the original
        stream. Register that stream separately when the metadata owner should close it.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_metadata_api.py


        :param data: File/cover path, bytes, or binary-readable payload.
        :param typ: Payload type marker, defaulting to path.
        :param file_id: Optional database id associated with the file payload.
        :return: None.
        """

    def record_path_and_file_name(self, file_path: CalibrePath) -> None:
        """
        Record an original path and basename as metadata history without adding a file payload.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_metadata_api.py


        :param file_path: Original path to record along with its basename.
        :return: None.
        """

    def set_doc_type(self, doc_type: str) -> None:
        """
        Validate and store the document-type label for this metadata container.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_metadata_api.py


        :param doc_type: Document type label to validate and store.
        :return: None.
        """

    def add_creators(self, creators: Mapping[str, str | Sequence[str]]) -> None:
        """
        Add creator names grouped by role.

        The legacy implementation stops after a single string without ampersands; list
        values support processing multiple roles in one call.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_metadata_api.py


        :param creators: Role-to-name-or-name-sequence mapping.
        :return: None.
        """

    def update_creators(self, creators_dict: LiuXinCreatorDump) -> None:
        """
        Merge already normalized role-to-name/id mappings into creator storage.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_metadata_api.py


        :param creators_dict: Role-to-creator mapping, in the form required by this
            operation.
        :return: None.
        """

    def add_identifiers(self, identifiers: CalibreIdentifierMapping) -> None:
        """
        Add external identifiers from a scheme mapping.

        The legacy implementation returns after a scalar string entry; iterable entries
        allow later schemes to be processed.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_metadata_api.py


        :param identifiers: Scheme-to-value mapping using the supported string, sequence,
            set, or value-to-id forms.
        :return: None.
        """

    def add_internal_identifiers(self, identifiers: CalibreIdentifierMapping) -> None:
        """
        Add internal identifiers from a scheme mapping.

        Internal scheme validation and accepted payload forms belong to the implementation.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_metadata_api.py


        :param identifiers: Scheme-to-value mapping using the supported string, sequence,
            set, or value-to-id forms.
        :return: None.
        """

    def __unicode__(self) -> str:
        """
        Render a Unicode diagnostic description for legacy compatibility.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_metadata_api.py


        :return: Metadata description string.
        """

    def __str__(self) -> str:
        """
        Render a human-readable diagnostic summary of the metadata.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_metadata_api.py


        :return: Metadata description string.
        """

    @staticmethod
    def standard_field_keys() -> LiuXinFieldKeys:
        """
        Return the metadata family's standard field names, including unset fields.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_metadata_api.py


        :return: Set of standard field names.
        """

    def user_metadata_keys(self) -> LiuXinFieldKeys:
        """
        Return the lookup names of user-defined fields on this container.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_metadata_api.py


        :return: Set of user-metadata names.
        """

    def all_field_keys(self) -> LiuXinFieldKeys:
        """
        Collect recognized top-level fields, creator roles and identifier schemes.

        The legacy implementation does not expand nested user-metadata keys.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_metadata_api.py


        :return: Set of recognized field names.
        """

    def all_set_fields(self) -> LiuXinFieldMapping:
        """
        Return the legacy compatibility field selection.

        The concrete implementation delegates to all_non_none_fields, which currently
        selects null fields despite the method names.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_metadata_api.py


        :return: Selected field-to-value mapping.
        """

    def all_non_none_fields(self) -> LiuXinFieldMapping:
        """
        Return the legacy compatibility field selection.

        The concrete implementation retains fields satisfying is_null; callers must not
        infer non-null filtering from the name.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_metadata_api.py


        :return: Selected field-to-value mapping.
        """

    def is_null(self, field: str) -> bool:
        """
        Test whether a field is absent, false or equal to its metadata default.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_metadata_api.py


        :param field: Legacy metadata field name; supported aliases are normalized by the
            implementation.
        :return: True for null/default fields, including failed legacy lookups.
        """

    def dict_add(self, more_metadata: LiuXinMetadataAPI) -> None:
        """
        Add fields absent from this container using another container's data.

        Existing keys remain unchanged, even when their values are empty.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_metadata_api.py


        :param more_metadata: Compatible metadata container whose copied data supplies
            absent keys.
        :return: None.
        """

    def get_all_attr(self, copy: bool = True) -> LiuXinFieldMapping:
        """
        Return the stored field mapping using the requested copy policy.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_metadata_api.py


        :param copy: Return independent copies when True; return the live field mapping when
            False.
        :return: Field-to-value mapping; mutations affect the container when copy is False.
        """

    def get_data(self, rtn_deepcopy: bool = True) -> LiuXinFieldMapping:
        """
        Expose the stored metadata as a mapping, deep-copying it by default.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_metadata_api.py


        :param rtn_deepcopy: Deep-copy the stored field mapping when True; otherwise expose
            the live mapping.
        :return: Copied or live field-to-value mapping.
        """

    def deepcopy_metadata(self) -> Self:
        """
        Create an independent metadata container of the same concrete family.

        The legacy implementation copies field storage and starts with an empty cleanup
        registry.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_metadata_api.py


        :return: Independent metadata container.
        """

    def smart_update(
        self,
        other: LiuXinMetadataAPI,
        replace_metadata: bool = False,
    ) -> None:
        """
        Merge another compatible container using field-specific replacement rules.

        Empty source values need not clear existing values; the legacy implementation merges
        mappings and deduplicates list entries when replacement is disabled.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_metadata_api.py


        :param other: Compatible metadata container supplying fields to merge.
        :param replace_metadata: Replace supported existing metadata when True; otherwise
            use the family merge rules.
        :return: None.
        """

    def clean(self) -> None:
        """
        Normalize stored titles, creators, tags and supported identifiers in place.

        Normalization follows the concrete metadata family and can collapse duplicate keys.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_metadata_api.py


        :return: None.
        """

    def get_all_user_metadata(self, make_copy: bool) -> CalibreUserMetadata:
        """
        Return user-defined field descriptors with an explicit copy policy.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_metadata_api.py


        :param make_copy: Deep-copy user metadata when True; otherwise expose its stored
            mapping.
        :return: User-metadata mapping, copied when requested.
        """

    def from_title_row(self, title_row: LiuXinTitleRowAPI) -> None:
        """
        Populate legacy metadata from a title Row and its linked database rows.

        The row supplies the database context; relation lookup and conversion errors
        propagate. This call does not close the database.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_metadata_api.py


        :param title_row: Title Row with row_dict and a database capable of resolving linked
            metadata rows.
        :return: None.
        """

    def finalize(self) -> Self:
        """
        Apply final compatibility conversions and return the metadata container.

        This normalizes cached Calibre fields and tags without preventing later mutation.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_metadata_api.py


        :return: This metadata container after normalization.
        """

    def to_calibre(self) -> CalibreMetadataAPI:
        """
        Create a flat Calibre-compatible metadata object.

        The legacy conversion is lossy: multiple values and database relation provenance
        cannot all be represented.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_metadata_api.py


        :return: New Calibre metadata object.
        """

    def register_file_for_cleanup(self, file_pointer: CalibreCloseableAPI) -> None:
        """
        Retain a closeable object for later cleanup.

        Registration does not close the object or deduplicate earlier registrations.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_metadata_api.py


        :param file_pointer: Closeable object retained in the cleanup registry.
        :return: None.
        """

    def close_cleanup_files(self) -> None:
        """
        Call close on each registered cleanup object.

        The legacy implementation ignores AttributeError, propagates other failures, and
        retains the registry after closing.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_metadata_api.py


        :return: None.
        """

    @staticmethod
    def explain_field(key: str) -> str:
        """
        Look up a human-readable explanation for an exact metadata field key.

        Unknown keys raise ValueError in the legacy implementation.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/api/test_liuxin_metadata_api.py


        :param key: Exact metadata field key whose explanation is requested.
        :return: Field explanation string.
        """

"""
Describe the richer LiuXin book-metadata surface used by Calibre compatibility callers.

This protocol combines a minimal readable source with multi-value identifiers,
value-to-id collections, persistence, cleanup, and conversion methods. Method bodies
are declarations only; documented legacy differences describe the existing container
rather than runtime enforcement.

Example:
    Exercise the owning behavior with pytest::

        python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py
"""
from __future__ import annotations

from pathlib import Path
from typing import Protocol, Mapping, Sequence, Iterable, AbstractSet, Self, Set

from LiuXin_alpha.metadata.api.containers_api.metadata_write_api import (
    MetadataWriteDatabaseAPI,
    MetadataWriteReportAPI,
    MetadataWriteTargetRow,
)
from LiuXin_alpha.metadata.api.containers_api.calibre_metadata_api.calibre_metadata_input_api import (
    CalibreMetadataInputAPI,
)
from LiuXin_alpha.metadata.api.containers_api.calibre_metadata_api.calibre_metadata_types import (
    CalibreCloseableAPI,
    CalibreFieldMapping,
    CalibreFieldValue,
    CalibreFilePayload,
    CalibreIdentifierMapping,
    CalibreIdentifierSnapshot,
    CalibreIdentifierValue,
    CalibrePath,
    CalibreUserMetadata,
    CalibreValueToID,
)
class CalibreLikeBookMetadataAPI(CalibreMetadataInputAPI, Protocol):
    """
    Specify the extended Calibre-like container with relation-valued fields and metadata operations.

    Implementers provide their own storage and behavior; the protocol is not
    runtime-checkable. The legacy CalibreLikeLiuXinBookMetaData implements this surface,
    with method-specific limitations documented below.

    Example:
        >>> from LiuXin_alpha.metadata.containers.calibre_like_book_metadata import CalibreLikeLiuXinBookMetaData
        >>> book: CalibreLikeBookMetadataAPI = CalibreLikeLiuXinBookMetaData('Example')
        >>> book.get('title')
        'Example'
    """

    creator_sort: str | None
    identifiers: CalibreIdentifierSnapshot
    internal_identifiers: CalibreIdentifierSnapshot
    creators: Mapping[str, Sequence[str]]
    languages: Sequence[str] | None
    labels: CalibreValueToID
    tags: CalibreValueToID

    def setattr(self, key: str, value: CalibreFieldValue) -> None:
        """
        Assign one field through the implementing container's normal metadata setter.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :param key: Metadata storage key.
        :param value: Value to store using the operation's field rules.
        :return: None.
        """
        ...

    def get(
        self,
        field: str,
        default: CalibreFieldValue = None,
    ) -> CalibreFieldValue:
        """
        Read a field with a caller-supplied fallback for an unavailable value.

        Copying and null/default handling follow the concrete container's read policy.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :param field: Metadata field name.
        :param default: Fallback used by the implementation for a missing value.
        :return: Field value or fallback.
        """

    def get_extra(
        self,
        field: str,
        default: CalibreFieldValue = None,
    ) -> CalibreFieldValue:
        """
        Read a custom-field auxiliary value using the container's compatibility policy.

        The legacy LiuXin container returns the whole user-metadata entry and can signal an
        undefined field with AttributeError.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :param field: Metadata field name.
        :param default: Fallback used by the implementation for a missing value.
        :return: Custom-field result or fallback where the implementation supports it.
        """

    def direct_get(self, item: str) -> CalibreFieldValue:
        """
        Read an exact internal metadata entry without the normal field conversion layer.

        The legacy implementation exposes mutable storage and signals unknown names with
        AttributeError.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :param item: Exact field or column lookup name.
        :return: Stored field value; callers must account for possible shared mutable data.
        """
        ...

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

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


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
        ...

    def to_opf_bytes(self, *, default_lang: str | None = None) -> bytes:
        """
        Serialize this metadata as an OPF document.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :param default_lang: Optional default language forwarded to OPF serialization.
        :return: Serialized OPF bytes.
        """
        ...

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

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :param path: Destination OPF path; the shared adapter creates parents and overwrites
            the file.
        :param default_lang: Optional default language forwarded to OPF serialization.
        :return: Path of the written OPF file.
        """
        ...

    def nullify(self, field: str) -> None:
        """
        Reset a field or supported grouped creator/identifier collection to its null state.

        Recognized fields and unknown-key handling are defined by the implementation.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :param field: Metadata field name.
        :return: None.
        """
        ...

    def direct_add(
        self,
        key: str,
        value: CalibreFieldValue,
        key_check: bool = True,
    ) -> None:
        """
        Insert or replace an internal value while bypassing normal field conversion.

        This operation may retain the supplied object directly; key_check requests
        existing-key validation.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :param key: Metadata storage key.
        :param value: Value to store using the operation's field rules.
        :param key_check: Whether direct insertion must reject keys absent from storage.
        :return: None.
        """
        ...

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

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :param data: File/cover path, bytes, or binary-readable payload.
        :param typ: Payload type marker, defaulting to path.
        :param file_id: Optional database id associated with the file payload.
        :return: None.
        """
        ...

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

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :param data: File/cover path, bytes, or binary-readable payload.
        :param typ: Payload type marker, defaulting to path.
        :param cover_id: Optional database id associated with the cover payload.
        :return: None.
        """
        ...

    def record_path_and_file_name(self, file_path: CalibrePath) -> None:
        """
        Record an original path and basename as metadata history without adding a file payload.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :param file_path: Original path to record along with its basename.
        :return: None.
        """
        ...

    def register_file_for_cleanup(self, file_pointer: CalibreCloseableAPI) -> None:
        """
        Register a resource for the metadata owner's later cleanup.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :param file_pointer: Resource whose close method should be called during cleanup.
        :return: None.
        """
        ...

    def close_cleanup_files(self) -> None:
        """
        Attempt to close registered cleanup resources according to the implementation's policy.

        The legacy registry is retained after closing, so repeated calls can close resources
        again.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :return: None.
        """
        ...

    def get_identifiers(self) -> CalibreIdentifierSnapshot:
        """
        Return an external-identifier snapshot keyed by scheme.

        The value shape may be a string or collection; consumers should not assume a single
        identifier per scheme.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :return: Identifier snapshot; the legacy LiuXin implementation returns copied sets.
        """
        ...

    def get_internal_identifiers(self) -> CalibreIdentifierSnapshot:
        """
        Return an internal-identifier snapshot separately from external bibliographic identifiers.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :return: Internal scheme-to-values snapshot.
        """
        ...

    def read_identifiers(self, identifiers: CalibreIdentifierMapping) -> None:
        """
        Read external identifiers through the implementing container's identifier setter.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :param identifiers: Scheme-to-value mapping using the supported string, sequence,
            set, or value-to-id forms.
        :return: None.
        """
        ...

    def set_identifiers(
        self,
        identifiers: CalibreIdentifierMapping,
        update: bool = True,
    ) -> None:
        """
        Apply a mapping of external identifiers according to the container's update policy.

        The legacy LiuXin implementation adds strings/collections and only uses its optional
        update flag to replace OrderedDict values; it does not clear unmentioned schemes.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :param identifiers: Scheme-to-value mapping using the supported string, sequence,
            set, or value-to-id forms.
        :param update: Whether supported identifier containers merge with existing values.
        :return: None.
        """
        ...

    def set_identifier(self, typ: str, val: CalibreIdentifierValue) -> None:
        """
        Set an identifier under a normalized scheme, or clear that scheme for a supported null value.

        Concrete containers differ in accepted value shapes and deletion rules.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :param typ: Identifier scheme name or supported alias.
        :param val: Identifier or field value accepted by the implementing container.
        :return: None.
        """
        ...

    def has_identifier(self, typ: str) -> bool:
        """
        Test whether the requested identifier scheme has a stored value.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :param typ: Identifier scheme name or supported alias.
        :return: Whether the scheme has identifier content.
        """
        ...

    def add_identifiers(self, identifiers: CalibreIdentifierMapping) -> None:
        """
        Add external identifiers from a scheme mapping.

        The legacy implementation returns after a scalar string entry; iterable entries
        allow later schemes to be processed.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :param identifiers: Scheme-to-value mapping using the supported string, sequence,
            set, or value-to-id forms.
        :return: None.
        """
        ...

    def add_internal_identifiers(self, identifiers: CalibreIdentifierMapping) -> None:
        """
        Add internal identifiers from a scheme mapping.

        Internal scheme validation and accepted payload forms belong to the implementation.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :param identifiers: Scheme-to-value mapping using the supported string, sequence,
            set, or value-to-id forms.
        :return: None.
        """
        ...

    def standard_field_keys(self) -> Set[str]:
        """
        Return the metadata family's standard field names, including unset fields.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :return: Set of standard field names.
        """
        ...

    def user_metadata_keys(self) -> Set[str]:
        """
        Return the lookup names of user-defined fields on this container.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :return: Set of user-metadata names.
        """
        ...

    def all_field_keys(self) -> Set[str]:
        """
        Return all field names recognized by this metadata container.

        The legacy LiuXin implementation combines top-level keys, creator roles and schemes
        without expanding nested user metadata.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :return: Set of recognized field names.
        """
        ...

    def all_set_fields(self) -> CalibreFieldMapping:
        """
        Expose the container's compatibility field/value selection.

        The legacy LiuXin implementation delegates to all_non_none_fields, whose current
        predicate selects null fields despite these method names.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :return: Field-to-value mapping produced by the implementation.
        """
        ...

    def all_non_none_fields(self) -> CalibreFieldMapping:
        """
        Expose the container's compatibility field/value selection.

        The legacy LiuXin implementation currently retains fields satisfying is_null, so
        callers must not infer non-null filtering from this method name.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :return: Selected field-to-value mapping.
        """
        ...

    def is_null(self, field: str) -> bool:
        """
        Test whether a field is absent or has the container's null/default value.

        Legacy containers also treat false numeric values and lookup failures as null.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :param field: Metadata field name.
        :return: Whether the field is considered unset.
        """
        ...

    def get_all_attr(self, copy: bool = True) -> CalibreFieldMapping:
        """
        Return the raw metadata mapping, optionally copying its values.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :param copy: Whether to return independent copied data instead of live storage.
        :return: Copied or live field mapping according to copy.
        """
        ...

    def get_data(self, rtn_deepcopy: bool = True) -> CalibreFieldMapping:
        """
        Return the raw metadata mapping with an explicit deep-copy choice.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :param rtn_deepcopy: Whether to return independent copied data instead of live
            storage.
        :return: Copied or live field mapping according to rtn_deepcopy.
        """
        ...

    def deepcopy_metadata(self) -> Self:
        """
        Clone stored metadata into an independent object of the implementing family.

        Legacy clones create an empty resource-cleanup registry.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :return: Metadata clone with the same interface.
        """
        ...

    def smart_update(
        self,
        other: CalibreLikeBookMetadataAPI,
        replace_metadata: bool = False,
    ) -> None:
        """
        Merge another compatible metadata object using field-specific null and replacement rules.

        Details differ between metadata families; replacement need not clear every empty
        source field.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :param other: Compatible metadata object supplying update values.
        :param replace_metadata: Whether to request replacement instead of collection
            merging where supported.
        :return: None.
        """
        ...

    def clean(self) -> None:
        """
        Normalize the container's supported metadata fields in place.

        The legacy implementation title-cases names/title/tags and rekeys an existing isbn10
        store.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :return: None.
        """
        ...

    def get_all_user_metadata(self, make_copy: bool) -> CalibreUserMetadata:
        """
        Return user-defined field descriptors, optionally making independent copies.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :param make_copy: True for independent copied metadata; False to expose the stored
            mapping.
        :return: User-metadata mapping with the requested copy policy.
        """
        ...

    def finalize(self) -> None:
        """
        Apply the metadata family's final normalization and compatibility conversions.

        This protocol declares a side-effect-only call. The legacy LiuXin concrete method
        returns itself and leaves later mutation enabled.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :return: None in the declared protocol; callers should not depend on a concrete
            chaining return.
        """
        ...

    def to_calibre(self) -> CalibreMetadataAPI:
        """
        Project extended metadata into the Calibre-compatible book interface.

        Multiple values and relation ids may be collapsed or omitted by conversion.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :return: CalibreMetadataAPI-compatible projection.
        """
        ...

    def __str__(self) -> str:
        """
        Render a human-readable diagnostic summary of the metadata.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :return: Metadata description string.
        """
        ...

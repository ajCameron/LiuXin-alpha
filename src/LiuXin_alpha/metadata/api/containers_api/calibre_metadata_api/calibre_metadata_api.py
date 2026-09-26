
"""
Describe mutable Calibre metadata for import/export and plugin compatibility.

The interface extends the minimal readable contract with field descriptors, custom
values, identifiers, copying, and OPF/database persistence. Methods declare expected
implementing behavior rather than providing it.

Example:
    Exercise the owning behavior with pytest::

        python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py
"""


from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Protocol, Mapping, Sequence, AbstractSet, Iterable, Self, Set

from LiuXin_alpha.metadata.api.containers_api.metadata_write_api import (
    MetadataWriteDatabaseAPI,
    MetadataWriteReportAPI,
    MetadataWriteTargetRow,
)
from LiuXin_alpha.metadata.api.containers_api.calibre_metadata_api.calibre_metadata_input_api import (
    CalibreMetadataInputAPI,
)
from LiuXin_alpha.metadata.api.containers_api.calibre_metadata_api.calibre_metadata_types import (
    CalibreCoverData,
    CalibreFieldDescriptor,
    CalibreFieldMapping,
    CalibreFieldValue,
    CalibreIdentifierMapping,
    CalibreIdentifierSnapshot,
    CalibrePath,
    CalibreUserMetadata,
)


class CalibreMetadataAPI(CalibreMetadataInputAPI, Protocol):
    """
    Specify standard Calibre book attributes plus custom-field and persistence methods.

    Standard values are exposed as attributes; descriptors and user columns are accessed
    through the method interface. Concrete behavior is supplied by calibreMetadata or
    compatible adapters. This protocol is not runtime-checkable.

    Example:
        >>> from LiuXin_alpha.metadata.book.base import calibreMetadata
        >>> book: CalibreMetadataAPI = calibreMetadata('Example', ['Writer'])
        >>> book.set_identifier('isbn', '9780306406157')
        >>> book.has_identifier('isbn')
        True
    """

    title_sort: str | None
    author_sort: str | None
    author_sort_map: Mapping[str, str] | None
    tags: Sequence[str] | None
    comments: str | None
    languages: Sequence[str] | None
    identifiers: CalibreIdentifierSnapshot
    publisher: str | None
    pubdate: datetime | None
    timestamp: datetime | None
    last_modified: datetime | None
    rights: str | None
    series: str | None
    series_index: float | int | None
    rating: float | int | None
    cover: CalibrePath | None
    cover_data: CalibreCoverData
    book_producer: str | None
    application_id: str | int | None
    db_id: int | None
    uuid: str | None
    formats: Sequence[str] | None

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

    def has_key(self, key: str) -> bool:
        """
        Test membership in the implementing book's direct metadata store.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :param key: Metadata storage key.
        :return: Whether the storage key exists.
        """

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
        Read an auxiliary value for a defined custom field.

        The Calibre implementation supplies the default for a missing extra but signals an
        undefined custom field with AttributeError.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :param field: Metadata field name.
        :param default: Fallback used by the implementation for a missing value.
        :return: Stored custom extra or default.
        """

    def set(
        self,
        field: str,
        val: CalibreFieldValue,
        extra: CalibreFieldValue = None,
    ) -> None:
        """
        Assign a metadata value and optional custom-field extra through normal setter rules.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :param field: Metadata field name.
        :param val: Identifier or field value accepted by the implementing container.
        :param extra: Optional custom-field auxiliary value, such as a series index.
        :return: None.
        """

    def get_identifiers(self) -> CalibreIdentifierSnapshot:
        """
        Read an independent snapshot of the Calibre identifier mapping.

        The concrete Calibre book container returns one cleaned string per scheme; adapters
        using broader snapshot shapes must document their value policy.

        Example:
            Exercise the concrete API with pytest::

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :return: Scheme-to-identifier snapshot; the Calibre implementation returns a copied
            dictionary of strings.
        """

    def set_identifiers(self, identifiers: CalibreIdentifierMapping) -> None:
        """
        Replace the Calibre identifier mapping from the supplied scheme/value entries.

        Concrete Calibre storage expects scalar identifier strings; broader shared alias
        shapes need an appropriate adapter.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :param identifiers: Scheme-to-value mapping using the supported string, sequence,
            set, or value-to-id forms.
        :return: None.
        """

    def set_identifier(self, typ: str, val: str | None) -> None:
        """
        Assign one identifier, removing it when the implementation receives an empty value.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :param typ: Identifier scheme name.
        :param val: Identifier string or None to remove the scheme.
        :return: None.
        """

    def has_identifier(self, typ: str) -> bool:
        """
        Test whether the requested identifier scheme has a stored value.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :param typ: Identifier scheme name or supported alias.
        :return: Whether the scheme has identifier content.
        """

    def standard_field_keys(self) -> Set[str]:
        """
        Return the metadata family's standard field names, including unset fields.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :return: Set of standard field names.
        """

    def custom_field_keys(self) -> Iterable[str]:
        """
        Iterate lookup names for the book's defined custom columns.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :return: Iterable of custom field names.
        """

    def all_field_keys(self) -> Set[str]:
        """
        Collect standard and custom lookup names, including unset fields.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :return: Set of known field names.
        """

    def metadata_for_field(self, key: str) -> CalibreFieldDescriptor | None:
        """
        Read the standard or custom descriptor for a field without requesting a copy.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :param key: Metadata storage key.
        :return: Descriptor mapping or None when unknown.
        """

    def all_non_none_fields(self) -> CalibreFieldMapping:
        """
        Collect standard and custom values that are not None.

        The Calibre implementation retains empty containers and other false values and may
        evaluate custom composites.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :return: Field-to-value mapping, including custom series index entries where
            supported.
        """

    def get_standard_metadata(
        self,
        field: str,
        make_copy: bool,
    ) -> CalibreFieldDescriptor | None:
        """
        Read a standard field descriptor with a caller-selected copy policy.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :param field: Metadata field name.
        :param make_copy: True for independent copied metadata; False to expose the stored
            mapping.
        :return: Descriptor or None for an unknown/non-field entry.
        """

    def get_all_standard_metadata(
        self,
        make_copy: bool,
    ) -> Mapping[str, CalibreFieldDescriptor]:
        """
        Read the standard field registry or independent field-descriptor copies.

        The Calibre implementation can include non-field entries when returning its live
        registry.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :param make_copy: True for independent copied metadata; False to expose the stored
            mapping.
        :return: Mapping of standard names to descriptors according to implementation and
            copy policy.
        """

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

    def get_user_metadata(
        self,
        field: str,
        make_copy: bool,
    ) -> CalibreFieldDescriptor | None:
        """
        Read one custom column descriptor with the requested copy policy.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :param field: Metadata field name.
        :param make_copy: True for independent copied metadata; False to expose the stored
            mapping.
        :return: Descriptor or None if undefined.
        """

    def set_all_user_metadata(self, metadata: CalibreUserMetadata) -> None:
        """
        Replace custom-column descriptors and values from a complete mapping.

        Concrete setters supply missing default value slots and may retain nested objects by
        reference.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :param metadata: Lookup-name-to-custom-descriptor mapping.
        :return: None.
        """

    def set_user_metadata(
        self,
        field: str,
        metadata: CalibreFieldDescriptor | None,
    ) -> None:
        """
        Install one custom-column descriptor under its lookup name.

        The Calibre implementation requires a non-None name to start with # and handles a
        None descriptor diagnostically.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :param field: Metadata field name.
        :param metadata: Custom descriptor mapping, or None for the implementation's
            absent-descriptor policy.
        :return: None.
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

    def to_opf_bytes(self, *, default_lang: str | None = None) -> bytes:
        """
        Serialize this metadata as an OPF document.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


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

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :param path: Destination OPF path; the shared adapter creates parents and overwrites
            the file.
        :param default_lang: Optional default language forwarded to OPF serialization.
        :return: Path of the written OPF file.
        """

    def deepcopy_metadata(self) -> Self:
        """
        Clone stored metadata into an independent object of the implementing family.

        Legacy clones create an empty resource-cleanup registry.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :return: Metadata clone with the same interface.
        """

    def __str__(self) -> str:
        """
        Render a human-readable diagnostic summary of the metadata.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :return: Metadata description string.
        """

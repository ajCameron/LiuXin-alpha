
"""
Metadata container focused around the title as the fundamental object.

The aim here is "calibre metadata plus".
Focussed around the single title of the work.
Provides title focused metadata methods.

The concept of a "title" in WEMI is derivwd from components from
 - work
 - expression
 - manifestation
 - item
Together into a single string.

You can think of this as the entire metadata for a manifestation.
An actual thing which exists and you can hand to people.
"""


from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Protocol, Sequence, Mapping, Self, Iterable

from LiuXin_alpha.metadata.api import (
    LiuXinValueToID,
    LiuXinCreatorMapping,
    CalibreIdentifierSnapshot,
    LiuXinPayloadToID,
    LiuXinFieldValue,
    CalibrePath,
    LiuXinRatingMapping,
    CalibreUserMetadata,
    CalibreMetadataInputAPI,
    MetadataWriteDatabaseAPI,
    MetadataWriteTargetRow,
    MetadataWriteReportAPI,
    CalibreIdentifierMapping,
    CalibreIdentifierValue,
    LiuXinCreatorDump,
    CalibreFilePayload,
    LiuXinFieldKeys,
    LiuXinFieldMapping,
    LiuXinTitleRowAPI,
    CalibreMetadataAPI,
    CalibreCloseableAPI)


class LiuXinMetadataAPI(Protocol):
    """
    Structural API for ``LiuXin.metadata.metadata.MetaData``.

    This captures the pre-alpha extended metadata object: Calibre-compatible
    enough for plugin workflows, but richer around creators, ids, database row
    ids, original files, covers, and title-row hydration.
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
        Factory method to produce a LiuXin metadata object from a calibre one.

        :param calibre_md:
        :return:
        """

    def setattr(self, key: str, value: LiuXinFieldValue) -> None:
        """
        Set aan attribute for the metadata object.

        :param key:
        :param value:
        :return:
        """

    def get(
        self,
        field: str,
        default: LiuXinFieldValue = None,
    ) -> LiuXinFieldValue:
        """
        Get the current metadata value from the container.

        :param field:
        :param default:
        :return:
        """

    def get_extra(
        self,
        field: str,
        default: LiuXinFieldValue = None,
    ) -> LiuXinFieldValue:
        """
        Get the extra value for the given value for the container.

        :param field:
        :param default:
        :return:
        """

    def read_creators(self, creators_dict: Mapping[str, str | Sequence[str]]) -> None:
        """
        Read the creators content from a creators dictionary.

        :param creators_dict:
        :return:
        """

    def direct_get(self, item: str) -> LiuXinFieldValue:
        """
        Get the value of an item from the metadata.

        :param item:
        :return:
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
        Write metadata from the object out to the database.

        :param database:
        :param fields:
        :param target_level:
        :param item_id:
        :param target_row:
        :param replace:
        :param mark_dirty:
        :return:
        """

    def to_opf_bytes(self, *, default_lang: str | None = None) -> bytes:
        """
        Render the metadata object out to an OPF file, in the form of bytes.

        :param default_lang:
        :return:
        """

    def write_to_opf(
        self,
        path: CalibrePath,
        *,
        default_lang: str | None = None,
    ) -> Path:
        """
        Write the metadata object out to an OPF file.

        :param path:
        :param default_lang:
        :return:
        """

    def __getitem__(self, item: str) -> LiuXinFieldValue:
        """
        Retrieve a metadata value from the container.

        :param item:
        :return:
        """

    def __iter__(self) -> Iterable[str]:
        """
        Iterate over the keys in the container.

        :return:
        """

    def nullify(self, field: str) -> None:
        """
        Set a field value to null in the container.

        :param field:
        :return:
        """

    def get_identifiers(self) -> CalibreIdentifierSnapshot:
        """
        Return the identifiers stored within the metadata container.

        Contains both the internal and external identifiers.

        :return:
        """

    def get_internal_identifiers(self) -> CalibreIdentifierSnapshot:
        """
        Return the internal identifiers stored for this item in the metadata container.

        These could include
         - system assigned UUID(s)
         - system assigned ID(s)
         - hashes
        :return:
        """

    def read_identifiers(self, identifiers: CalibreIdentifierMapping) -> None:
        """
        Read an identifiers container into the metadata container.

        Identifiers are appending to the existing, stored identifier.
        :param identifiers:
        :return:
        """

    def set_identifiers(self, identifiers: CalibreIdentifierMapping) -> None:
        """
        Completely replace the stored identifiers in the object with new ones.

        :param identifiers:
        :return:
        """

    def set_identifier(self, typ: str, val: CalibreIdentifierValue) -> None:
        """
        Set the primary identifier of the given type.

        :param typ:
        :param val:
        :return:
        """

    def has_identifier(self, typ: str) -> bool:
        """
        Checks to see if the container has any instances of the given identifier.

        :param typ:
        :return:
        """

    def get_authors_copy(self) -> list[str]:
        """
        Convenience method - gets a list of the authors of the container.

        :return:
        """

    def get_creators_dump(self) -> LiuXinCreatorDump:
        """
        Get all the creators in the form of a mapping of mappings.

        :return:
        """

    def direct_add(
        self,
        key: str,
        value: LiuXinFieldValue,
        key_check: bool = True,
    ) -> None:
        """
        Directly add a key-value to the metadata.

        :param key:
        :param value:
        :param key_check:
        :return:
        """

    def add_cover(
        self,
        data: CalibreFilePayload,
        typ: str = "path",
        cover_id: int | None = None,
    ) -> None:
        """
        We're adding a cover object to the metadata container.

        :param data:
        :param typ:
        :param cover_id:
        :return:
        """

    def add_file(
        self,
        data: CalibreFilePayload,
        typ: str = "path",
        file_id: int | None = None,
    ) -> None:
        """
        Add a file to the metadata container.

        :param data:
        :param typ:
        :param file_id:
        :return:
        """

    def record_path_and_file_name(self, file_path: CalibrePath) -> None:
        """
        Add a file path to the metadata container.

        :param file_path:
        :return:
        """

    def set_doc_type(self, doc_type: str) -> None:
        """
        Set the document type of the container.

        :param doc_type:
        :return:
        """

    def add_creators(self, creators: Mapping[str, str | Sequence[str]]) -> None:
        """
        Add creators from a dict to the metadata container.

        :param creators:
        :return:
        """

    def update_creators(self, creators_dict: LiuXinCreatorDump) -> None:
        """
        Update the creators dict using an input container.

        :param creators_dict:
        :return:
        """

    def add_identifiers(self, identifiers: CalibreIdentifierMapping) -> None:
        """
        Add identifiers to the metadata container from an identifier mapping.

        Identifiers can be of internal or external type.
        :param identifiers:
        :return:
        """

    def add_internal_identifiers(self, identifiers: CalibreIdentifierMapping) -> None:
        """
        Add internal identifiers to the metadata container.

        :param identifiers:
        :return:
        """

    def __unicode__(self) -> str:
        """
        Unicode representation of the object.

        :return:
        """

    def __str__(self) -> str:
        """
        String representation of the object.

        :return:
        """

    # Todo: These might want to live over in surfaces
    def to_html(self) -> str:
        """
        HTML representation of the object.

        :return:
        """

    # Todo: Likewise this is an interface thing
    def format_series_index(self, val: str | int | float | None = None) -> str:
        """
        Take a series index and render it as a human-readable string.

        :param val:
        :return:
        """

    @staticmethod
    def standard_field_keys() -> LiuXinFieldKeys:
        """
        Standard field keys for this class of metadata.

        :return:
        """

    def user_metadata_keys(self) -> LiuXinFieldKeys:
        """
        User set metadata keys for this class of metadata.

        :return:
        """

    def all_field_keys(self) -> LiuXinFieldKeys:
        """
        All recognized field keys for this class of metadata.

        :return:
        """

    def all_set_fields(self) -> LiuXinFieldMapping:
        """
        All fields which have been set for this metadata object.

        :return:
        """

    def all_non_none_fields(self) -> LiuXinFieldMapping:
        """
        All fields which do not have a value which is identifiably zero.

        :return:
        """

    def is_null(self, field: str) -> bool:
        """
        Check if a given field is null.

        :param field:
        :return:
        """

    def dict_add(self, more_metadata: LiuXinMetadataAPI) -> None:
        """
        Add metadata from a dictionary to the container.

        :param more_metadata:
        :return:
        """

    def get_all_attr(self, copy: bool = True) -> LiuXinFieldMapping:
        """
        Get all attributes of the container.

        :param copy:
        :return:
        """

    def get_data(self, rtn_deepcopy: bool = True) -> LiuXinFieldMapping:
        """
        Get all the metadata from the container as a dictionary.

        :param rtn_deepcopy:
        :return:
        """

    def deepcopy_metadata(self) -> Self:
        """
        Return a deep copy of the container.

        :return:
        """

    def smart_update(
        self,
        other: LiuXinMetadataAPI,
        replace_metadata: bool = False,
    ) -> None:
        """
        Preform a smart update of the container.

        :param other:
        :param replace_metadata:
        :return:
        """

    def clean(self) -> None:
        """
        Preform a clean of the metadata.

        Does things like
         - normalize identifiers
         - normalize and dedupe tags
        :return:
        """

    def get_all_user_metadata(self, make_copy: bool) -> CalibreUserMetadata:
        """
        Return all user metadata from the container.

        :param make_copy:
        :return:
        """

    def from_title_row(self, title_row: LiuXinTitleRowAPI) -> None:
        """
        Populate this object from a title row.

        :param title_row:
        :return:
        """

    def finalize(self) -> Self:
        """
        Bring the metadata into final form.

        Last method which should be called before a write out.
        :return:
        """

    def to_calibre(self) -> CalibreMetadataAPI:
        """
        Transform this metadata object into a CalibreMetadataObject.

        :return:
        """

    def register_file_for_cleanup(self, file_pointer: CalibreCloseableAPI) -> None:
        """
        Register a file pointer for cleanup.

        :param file_pointer:
        :return:
        """

    def close_cleanup_files(self) -> None:
        """
        Go through and close all the cleanup file pointers.

        :return:
        """

    @staticmethod
    def explain_field(key: str) -> str:
        """
        String explanation of the field.

        :param key:
        :return:
        """

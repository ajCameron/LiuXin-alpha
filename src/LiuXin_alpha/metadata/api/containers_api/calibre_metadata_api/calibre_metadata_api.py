
"""
LiuXin uses a lot of components from calibre - this is calibre metadata shaped object for those components.
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
    Structural API for Calibre's mutable ``Metadata``/``MetaInformation`` shape.

    This is the plugin-facing compatibility surface: standard fields are exposed
    as attributes, while custom fields and identifiers use the method interface.
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
        Checks whether a given field is null.

        :param field:
        :return:
        """

    def has_key(self, key: str) -> bool:
        """
        Tests if the metadata object has the given key.

        :param key:
        :return:
        """

    def get(
        self,
        field: str,
        default: CalibreFieldValue = None,
    ) -> CalibreFieldValue:
        """
        Get a value for the given field.

        :param field:
        :param default:
        :return:
        """

    def get_extra(
        self,
        field: str,
        default: CalibreFieldValue = None,
    ) -> CalibreFieldValue:
        """
        Get the extra value for the given field.

        :param field:
        :param default:
        :return:
        """

    def set(
        self,
        field: str,
        val: CalibreFieldValue,
        extra: CalibreFieldValue = None,
    ) -> None:
        """
        Set and value (and optional extra) for the given field.

        :param field:
        :param val:
        :param extra:
        :return:
        """

    def get_identifiers(self) -> CalibreIdentifierSnapshot:
        """
        Returns all the identifiers for the given metadata object.

        :return:
        """

    def set_identifiers(self, identifiers: CalibreIdentifierMapping) -> None:
        """
        Write an entire identifiers container object into the metadata object.

        :param identifiers:
        :return:
        """

    def set_identifier(self, typ: str, val: str | None) -> None:
        """
        Set the primary identifier for the given type with the given value.

        :param typ:
        :param val:
        :return:
        """

    def has_identifier(self, typ: str) -> bool:
        """
        Tests to see if we have an actual identifier for the given type.

        :param typ:
        :return:
        """

    def standard_field_keys(self) -> Set[str]:
        """
        Returns the current standard field keys.

        :return:
        """

    def custom_field_keys(self) -> Iterable[str]:
        """
        Keys for all custom fields.

        :return:
        """

    def all_field_keys(self) -> Set[str]:
        """
        Keys for all fields - custom and standard.

        :return:
        """

    def metadata_for_field(self, key: str) -> CalibreFieldDescriptor | None:
        """
        Meatdata for a given, named field.

        :param key:
        :return:
        """

    def all_non_none_fields(self) -> CalibreFieldMapping:
        """
        All fields which are not none.

        :return:
        """

    def get_standard_metadata(
        self,
        field: str,
        make_copy: bool,
    ) -> CalibreFieldDescriptor | None:
        """
        Return the value for the given standard field.

        :param field:
        :param make_copy:
        :return:
        """

    def get_all_standard_metadata(
        self,
        make_copy: bool,
    ) -> Mapping[str, CalibreFieldDescriptor]:
        """
        Return a mapping of all standard fields to their values.

        :param make_copy:
        :return:
        """

    def get_all_user_metadata(self, make_copy: bool) -> CalibreUserMetadata:
        """
        Return all user set metadata.

        :param make_copy:
        :return:
        """

    def get_user_metadata(
        self,
        field: str,
        make_copy: bool,
    ) -> CalibreFieldDescriptor | None:
        """
        Return the value for the given user metadata field.

        :param field:
        :param make_copy:
        :return:
        """

    def set_all_user_metadata(self, metadata: CalibreUserMetadata) -> None:
        """
        Entirely replace all user metadata fields with the new values.

        :param metadata:
        :return:
        """

    def set_user_metadata(
        self,
        field: str,
        metadata: CalibreFieldDescriptor | None,
    ) -> None:
        """
        Set a user side metadata field.

        :param field:
        :param metadata:
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
        Write metadata out to the database.

        :param database: Database object to write the metadata out to.
        :param fields: Fields to write to the database - if not all.
        :param target_level:
        :param item_id:
        :param target_row:
        :param replace:
        :param mark_dirty:
        :return:
        """

    def to_opf_bytes(self, *, default_lang: str | None = None) -> bytes:
        """
        Dump the metadata object out to OPF bytes.

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
        Write a metadata object out to an OPF file on disk.

        :param path:
        :param default_lang:
        :return:
        """

    def deepcopy_metadata(self) -> Self:
        """
        Deepcopy of self.

        :return:
        """

    def __str__(self) -> str:
        """
        String representation of the calibre metadata object.

        :return:
        """

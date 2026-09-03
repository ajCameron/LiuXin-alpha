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
    """LiuXin's richer Calibre-like book metadata container shape."""

    creator_sort: str | None
    identifiers: CalibreIdentifierSnapshot
    internal_identifiers: CalibreIdentifierSnapshot
    creators: Mapping[str, Sequence[str]]
    languages: Sequence[str] | None
    labels: CalibreValueToID
    tags: CalibreValueToID

    def setattr(self, key: str, value: CalibreFieldValue) -> None: ...

    def get(
        self,
        field: str,
        default: CalibreFieldValue = None,
    ) -> CalibreFieldValue:
        """
        Retrieve the value for a given field.

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
        Retrive the extra value for a field - if it exists.

        :param field:
        :param default:
        :return:
        """

    def direct_get(self, item: str) -> CalibreFieldValue: ...

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
    ) -> MetadataWriteReportAPI: ...

    def to_opf_bytes(self, *, default_lang: str | None = None) -> bytes: ...

    def write_to_opf(
        self,
        path: CalibrePath,
        *,
        default_lang: str | None = None,
    ) -> Path: ...

    def nullify(self, field: str) -> None: ...

    def direct_add(
        self,
        key: str,
        value: CalibreFieldValue,
        key_check: bool = True,
    ) -> None: ...

    def add_file(
        self,
        data: CalibreFilePayload,
        typ: str = "path",
        file_id: int | None = None,
    ) -> None: ...

    def add_cover(
        self,
        data: CalibreFilePayload,
        typ: str = "path",
        cover_id: int | None = None,
    ) -> None: ...

    def record_path_and_file_name(self, file_path: CalibrePath) -> None: ...

    def register_file_for_cleanup(self, file_pointer: CalibreCloseableAPI) -> None: ...

    def close_cleanup_files(self) -> None: ...

    def get_identifiers(self) -> CalibreIdentifierSnapshot: ...

    def get_internal_identifiers(self) -> CalibreIdentifierSnapshot: ...

    def read_identifiers(self, identifiers: CalibreIdentifierMapping) -> None: ...

    def set_identifiers(
        self,
        identifiers: CalibreIdentifierMapping,
        update: bool = True,
    ) -> None: ...

    def set_identifier(self, typ: str, val: CalibreIdentifierValue) -> None: ...

    def has_identifier(self, typ: str) -> bool: ...

    def add_identifiers(self, identifiers: CalibreIdentifierMapping) -> None: ...

    def add_internal_identifiers(self, identifiers: CalibreIdentifierMapping) -> None: ...

    def standard_field_keys(self) -> Set[str]: ...

    def user_metadata_keys(self) -> Set[str]: ...

    def all_field_keys(self) -> Set[str]: ...

    def all_set_fields(self) -> CalibreFieldMapping: ...

    def all_non_none_fields(self) -> CalibreFieldMapping: ...

    def is_null(self, field: str) -> bool: ...

    def get_all_attr(self, copy: bool = True) -> CalibreFieldMapping: ...

    def get_data(self, rtn_deepcopy: bool = True) -> CalibreFieldMapping: ...

    def deepcopy_metadata(self) -> Self: ...

    def smart_update(
        self,
        other: CalibreLikeBookMetadataAPI,
        replace_metadata: bool = False,
    ) -> None: ...

    def clean(self) -> None: ...

    def get_all_user_metadata(self, make_copy: bool) -> CalibreUserMetadata: ...

    def finalize(self) -> None: ...

    def to_calibre(self) -> CalibreMetadataAPI: ...

    def __str__(self) -> str: ...

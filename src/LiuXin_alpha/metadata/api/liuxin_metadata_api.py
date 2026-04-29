"""API contracts for legacy LiuXin extended metadata objects."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence, Set
from datetime import datetime
from typing import Protocol, Self, TypeAlias

from LiuXin_alpha.metadata.api.calibre_metadata_api import (
    CalibreCloseableAPI,
    CalibreFieldDescriptor,
    CalibreFilePayload,
    CalibreIdentifierMapping,
    CalibreIdentifierSnapshot,
    CalibreIdentifierValue,
    CalibreMetadataAPI,
    CalibreMetadataInputAPI,
    CalibrePath,
    CalibreUserMetadata,
)


LiuXinRowID: TypeAlias = int | None
LiuXinScalar: TypeAlias = str | int | float | bool | bytes | datetime | None
LiuXinScalarSequence: TypeAlias = Sequence[LiuXinScalar]
LiuXinStringSet: TypeAlias = set[str] | frozenset[str]
LiuXinValueToID: TypeAlias = Mapping[str, LiuXinRowID]
LiuXinPayloadKey: TypeAlias = tuple[str, CalibreFilePayload]
LiuXinPayloadToID: TypeAlias = Mapping[LiuXinPayloadKey, LiuXinRowID]
LiuXinRatingValue: TypeAlias = int | float
LiuXinRatingMapping: TypeAlias = Mapping[str, LiuXinRatingValue]
LiuXinCreatorMapping: TypeAlias = Mapping[str, Sequence[str]]
LiuXinCreatorDump: TypeAlias = Mapping[str, LiuXinValueToID]
LiuXinFieldValue: TypeAlias = (
    LiuXinScalar
    | LiuXinScalarSequence
    | LiuXinStringSet
    | LiuXinValueToID
    | LiuXinPayloadToID
    | LiuXinRatingMapping
    | CalibreIdentifierSnapshot
    | CalibreUserMetadata
)
LiuXinFieldMapping: TypeAlias = Mapping[str, LiuXinFieldValue]
LiuXinFieldKeys: TypeAlias = Set[str]


class LiuXinMetadataDatabaseAPI(Protocol):
    """Database methods used by legacy ``MetaData.from_title_row``."""

    def get_categorized_tables(self) -> Mapping[str, Sequence[str]]: ...

    def get_display_column(self, table: str) -> str: ...


class LiuXinTitleRowAPI(Protocol):
    """Database row shape accepted by legacy ``MetaData.from_title_row``."""

    db: LiuXinMetadataDatabaseAPI

    def __getitem__(self, item: str) -> LiuXinFieldValue: ...


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
    def from_calibre(cls, calibre_md: CalibreMetadataInputAPI) -> Self: ...

    def setattr(self, key: str, value: LiuXinFieldValue) -> None: ...

    def get(
        self,
        field: str,
        default: LiuXinFieldValue = None,
    ) -> LiuXinFieldValue: ...

    def get_extra(
        self,
        field: str,
        default: LiuXinFieldValue = None,
    ) -> LiuXinFieldValue: ...

    def read_creators(self, creators_dict: Mapping[str, str | Sequence[str]]) -> None: ...

    def direct_get(self, item: str) -> LiuXinFieldValue: ...

    def __getitem__(self, item: str) -> LiuXinFieldValue: ...

    def __iter__(self) -> Iterable[str]: ...

    def nullify(self, field: str) -> None: ...

    def get_identifiers(self) -> CalibreIdentifierSnapshot: ...

    def get_internal_identifiers(self) -> CalibreIdentifierSnapshot: ...

    def read_identifiers(self, identifiers: CalibreIdentifierMapping) -> None: ...

    def set_identifiers(self, identifiers: CalibreIdentifierMapping) -> None: ...

    def set_identifier(self, typ: str, val: CalibreIdentifierValue) -> None: ...

    def has_identifier(self, typ: str) -> bool: ...

    def get_authors_copy(self) -> list[str]: ...

    def get_creators_dump(self) -> LiuXinCreatorDump: ...

    def direct_add(
        self,
        key: str,
        value: LiuXinFieldValue,
        key_check: bool = True,
    ) -> None: ...

    def add_cover(
        self,
        data: CalibreFilePayload,
        typ: str = "path",
        cover_id: int | None = None,
    ) -> None: ...

    def add_file(
        self,
        data: CalibreFilePayload,
        typ: str = "path",
        file_id: int | None = None,
    ) -> None: ...

    def record_path_and_file_name(self, file_path: CalibrePath) -> None: ...

    def set_doc_type(self, doc_type: str) -> None: ...

    def add_creators(self, creators: Mapping[str, str | Sequence[str]]) -> None: ...

    def update_creators(self, creators_dict: LiuXinCreatorDump) -> None: ...

    def add_identifiers(self, identifiers: CalibreIdentifierMapping) -> None: ...

    def add_internal_identifiers(self, identifiers: CalibreIdentifierMapping) -> None: ...

    def __unicode__(self) -> str: ...

    def to_html(self) -> str: ...

    def format_series_index(self, val: str | int | float | None = None) -> str: ...

    @staticmethod
    def standard_field_keys() -> LiuXinFieldKeys: ...

    def user_metadata_keys(self) -> LiuXinFieldKeys: ...

    def all_field_keys(self) -> LiuXinFieldKeys: ...

    def all_set_fields(self) -> LiuXinFieldMapping: ...

    def all_non_none_fields(self) -> LiuXinFieldMapping: ...

    def is_null(self, field: str) -> bool: ...

    def dict_add(self, more_metadata: LiuXinMetadataAPI) -> None: ...

    def get_all_attr(self, copy: bool = True) -> LiuXinFieldMapping: ...

    def get_data(self, rtn_deepcopy: bool = True) -> LiuXinFieldMapping: ...

    def deepcopy_metadata(self) -> Self: ...

    def smart_update(
        self,
        other: LiuXinMetadataAPI,
        replace_metadata: bool = False,
    ) -> None: ...

    def clean(self) -> None: ...

    def get_all_user_metadata(self, make_copy: bool) -> CalibreUserMetadata: ...

    def from_title_row(self, title_row: LiuXinTitleRowAPI) -> None: ...

    def finalize(self) -> Self: ...

    def to_calibre(self) -> CalibreMetadataAPI: ...

    def register_file_for_cleanup(self, file_pointer: CalibreCloseableAPI) -> None: ...

    def close_cleanup_files(self) -> None: ...

    @staticmethod
    def explain_field(key: str) -> str: ...


LiuXinMetaInformationAPI: TypeAlias = LiuXinMetadataAPI


__all__ = [
    "LiuXinCreatorDump",
    "LiuXinCreatorMapping",
    "LiuXinFieldKeys",
    "LiuXinFieldMapping",
    "LiuXinFieldValue",
    "LiuXinMetadataAPI",
    "LiuXinMetadataDatabaseAPI",
    "LiuXinMetaInformationAPI",
    "LiuXinPayloadKey",
    "LiuXinPayloadToID",
    "LiuXinRatingMapping",
    "LiuXinRatingValue",
    "LiuXinRowID",
    "LiuXinScalar",
    "LiuXinScalarSequence",
    "LiuXinStringSet",
    "LiuXinTitleRowAPI",
    "LiuXinValueToID",
]

"""API contracts for Calibre-shaped book metadata objects.

Category: high-level metadata compatibility API.
This module defines the subset of Calibre's mutable book metadata surface used
by LiuXin import/export and plugin-facing workflows.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence, Set
from datetime import datetime
from os import PathLike
from typing import Protocol, Self, TypeAlias, runtime_checkable


CalibrePath: TypeAlias = str | PathLike[str]


@runtime_checkable
class CalibreBinaryReadableAPI(Protocol):
    """Readable binary payload accepted by Calibre-style file and cover APIs."""

    def read(self, n: int = -1) -> bytes: ...


@runtime_checkable
class CalibreCloseableAPI(Protocol):
    """Closeable resource accepted by Calibre-style cleanup paths."""

    def close(self) -> None: ...


CalibreFilePayload: TypeAlias = CalibrePath | bytes | CalibreBinaryReadableAPI
CalibreCoverData: TypeAlias = tuple[str | None, CalibreFilePayload | None]

CalibreMetadataScalar: TypeAlias = str | int | float | bool | bytes | datetime | None
CalibreMetadataSequence: TypeAlias = Sequence[CalibreMetadataScalar]
CalibreMetadataSet: TypeAlias = set[str] | frozenset[str]
CalibreValueToID: TypeAlias = Mapping[str, int | None]
CalibrePayloadToID: TypeAlias = Mapping[CalibreCoverData, int | None]
CalibreIdentifierValue: TypeAlias = (
    str
    | Sequence[str]
    | set[str]
    | frozenset[str]
    | CalibreValueToID
    | None
)
CalibreIdentifierMapping: TypeAlias = Mapping[str, CalibreIdentifierValue]
CalibreIdentifierSnapshotValue: TypeAlias = str | Sequence[str] | set[str] | frozenset[str]
CalibreIdentifierSnapshot: TypeAlias = Mapping[str, CalibreIdentifierSnapshotValue]
CalibreDescriptorValue: TypeAlias = (
    CalibreMetadataScalar
    | CalibreMetadataSequence
    | Mapping[str, CalibreMetadataScalar]
)
CalibreFieldDescriptor: TypeAlias = Mapping[str, CalibreDescriptorValue]
CalibreUserMetadata: TypeAlias = Mapping[str, CalibreFieldDescriptor]
CalibreFieldValue: TypeAlias = (
    CalibreMetadataScalar
    | CalibreMetadataSequence
    | CalibreMetadataSet
    | CalibreFilePayload
    | CalibreValueToID
    | CalibrePayloadToID
    | CalibreIdentifierSnapshot
    | CalibreCoverData
    | CalibreUserMetadata
)
CalibreFieldMapping: TypeAlias = Mapping[str, CalibreFieldValue]


class CalibreMetadataInputAPI(Protocol):
    """Minimum metadata object shape that can be read from Calibre adapters."""

    title: str | None
    authors: Sequence[str] | None

    def get_identifiers(self) -> CalibreIdentifierSnapshot | CalibreIdentifierMapping: ...


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

    def is_null(self, field: str) -> bool: ...

    def has_key(self, key: str) -> bool: ...

    def get(
        self,
        field: str,
        default: CalibreFieldValue = None,
    ) -> CalibreFieldValue: ...

    def get_extra(
        self,
        field: str,
        default: CalibreFieldValue = None,
    ) -> CalibreFieldValue: ...

    def set(
        self,
        field: str,
        val: CalibreFieldValue,
        extra: CalibreFieldValue = None,
    ) -> None: ...

    def get_identifiers(self) -> CalibreIdentifierSnapshot: ...

    def set_identifiers(self, identifiers: CalibreIdentifierMapping) -> None: ...

    def set_identifier(self, typ: str, val: str | None) -> None: ...

    def has_identifier(self, typ: str) -> bool: ...

    def standard_field_keys(self) -> Set[str]: ...

    def custom_field_keys(self) -> Iterable[str]: ...

    def all_field_keys(self) -> Set[str]: ...

    def metadata_for_field(self, key: str) -> CalibreFieldDescriptor | None: ...

    def all_non_none_fields(self) -> CalibreFieldMapping: ...

    def get_standard_metadata(
        self,
        field: str,
        make_copy: bool,
    ) -> CalibreFieldDescriptor | None: ...

    def get_all_standard_metadata(
        self,
        make_copy: bool,
    ) -> Mapping[str, CalibreFieldDescriptor]: ...

    def get_all_user_metadata(self, make_copy: bool) -> CalibreUserMetadata: ...

    def get_user_metadata(
        self,
        field: str,
        make_copy: bool,
    ) -> CalibreFieldDescriptor | None: ...

    def set_all_user_metadata(self, metadata: CalibreUserMetadata) -> None: ...

    def set_user_metadata(
        self,
        field: str,
        metadata: CalibreFieldDescriptor | None,
    ) -> None: ...

    def deepcopy_metadata(self) -> Self: ...

    def __str__(self) -> str: ...


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
    ) -> CalibreFieldValue: ...

    def get_extra(
        self,
        field: str,
        default: CalibreFieldValue = None,
    ) -> CalibreFieldValue: ...

    def direct_get(self, item: str) -> CalibreFieldValue: ...

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


__all__ = [
    "CalibreBinaryReadableAPI",
    "CalibreCloseableAPI",
    "CalibreCoverData",
    "CalibreDescriptorValue",
    "CalibreFieldDescriptor",
    "CalibreFieldMapping",
    "CalibreFieldValue",
    "CalibreFilePayload",
    "CalibreIdentifierMapping",
    "CalibreIdentifierSnapshot",
    "CalibreIdentifierSnapshotValue",
    "CalibreIdentifierValue",
    "CalibreLikeBookMetadataAPI",
    "CalibreMetadataAPI",
    "CalibreMetadataInputAPI",
    "CalibreMetadataScalar",
    "CalibreMetadataSequence",
    "CalibreMetadataSet",
    "CalibrePayloadToID",
    "CalibrePath",
    "CalibreUserMetadata",
    "CalibreValueToID",
]

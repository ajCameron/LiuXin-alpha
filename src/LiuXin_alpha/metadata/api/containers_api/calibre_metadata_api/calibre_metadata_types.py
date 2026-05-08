from __future__ import annotations

from datetime import datetime
from os import PathLike
from typing import TypeAlias, runtime_checkable, Protocol, Sequence, Mapping

CalibrePath: TypeAlias = str | PathLike[str]


@runtime_checkable
class CalibreBinaryReadableAPI(Protocol):
    """
    Readable binary payload accepted by Calibre-style file and cover APIs.
    """

    def read(self, n: int = -1) -> bytes:
        """
        Returns the binary object as bytes.

        :param n:
        :return:
        """


@runtime_checkable
class CalibreCloseableAPI(Protocol):
    """Closeable resource accepted by Calibre-style cleanup paths."""

    def close(self) -> None:
        """
        Supports closing the file.

        :return:
        """


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

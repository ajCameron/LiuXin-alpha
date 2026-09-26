"""
Define bounded field, identifier, path, payload, and custom-descriptor types for Calibre APIs.

Aliases describe accepted shapes rather than coercing data. Binary-readable and
closeable protocols support resource handoff and cleanup; runtime protocol checks
inspect member presence only.

Example:
    Exercise the owning behavior with pytest::

        python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py
"""
from __future__ import annotations

from datetime import datetime
from os import PathLike
from typing import TypeAlias, runtime_checkable, Protocol, Sequence, Mapping

CalibrePath: TypeAlias = str | PathLike[str]


@runtime_checkable
class CalibreBinaryReadableAPI(Protocol):
    """
    Describe a binary stream that supports read with an optional byte limit.

    This runtime-checkable protocol does not promise seek or close methods.

    Example:
        >>> from io import BytesIO
        >>> isinstance(BytesIO(b'abc'), CalibreBinaryReadableAPI)
        True
    """

    def read(self, n: int = -1) -> bytes:
        """
        Read binary payload bytes from the resource's current position.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :param n: Maximum byte count; -1 requests all remaining input.
        :return: Bytes read, potentially fewer than requested or empty at end of input.
        """


@runtime_checkable
class CalibreCloseableAPI(Protocol):
    """
    Describe a resource with a close method for metadata cleanup paths.

    Runtime conformance does not guarantee idempotent closing or transfer ownership
    automatically.

    Example:
        >>> from io import BytesIO
        >>> stream = BytesIO()
        >>> isinstance(stream, CalibreCloseableAPI)
        True
        >>> stream.close()
    """

    def close(self) -> None:
        """
        Release the resource according to its implementation's close semantics.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/api/test_calibre_metadata_api.py


        :return: None.
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

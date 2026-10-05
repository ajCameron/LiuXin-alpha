"""
Define delivery-target values and a minimal acquisition reader without importing application implementations.

The frozen records retain supplied fields without runtime validation. A stored
resource delegates each read to its borrowed reader; it adds no caching, decoding,
streaming, retry, or ownership of the underlying Core/storage lifecycle.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol


class AcquisitionReader(Protocol):
    """
    Describe a structural provider of acquisition metadata and fully materialized bytes.

    CoreSurfaceModel implements this port, but callers need not depend on that
    concrete model. The protocol defines no runtime validation or implementation.

    Example:
        >>> metadata, content = reader.acquisition_read("file", 7)  # doctest: +SKIP
    """

    def acquisition_read(
        self, kind: str, resource_id: int
    ) -> tuple[Mapping[str, object], bytes]:
        """
        Read one identified acquisition resource and return its metadata alongside its content.

        Kind validation, resource resolution, and read failures belong to the
        implementation; the contract does not describe a lazy byte stream.

        Example:
            >>> metadata, content = reader.acquisition_read("image", 7)  # doctest: +SKIP


        :param kind: Acquisition resource category understood by the provider, such as file or image.
        :param resource_id: Identifier within that resource category.
        :return: Metadata mapping and complete bytes payload, with provider errors left visible.
        """
        ...


@dataclass(frozen=True)
class ResolvedFileTarget:
    """
    Carry a delivery mode, location, and suggested download name without constructing an HTTP response.

    Fields are frozen but not validated: a mode such as local or redirect does
    not prove location readability, URL safety, or filename suitability. The
    serving adapter interprets them and supplies any required checks.

    Example:
        >>> target = ResolvedFileTarget("redirect", "https://example.invalid/book", "雪.epub")
        >>> (target.mode, target.download_name)
        ('redirect', '雪.epub')
    """

    mode: str
    location: str
    download_name: str


@dataclass(frozen=True)
class CoreStoredFile:
    """
    Bind a borrowed acquisition reader to one resource kind and identifier for repeated byte reads.

    ``model``, ``kind``, and ``resource_id`` are retained unchanged. Freezing these
    attributes neither freezes the reader nor caches the resource's content.

    Example:
        >>> stored = CoreStoredFile(reader, "file", 7)  # doctest: +SKIP
        >>> content = stored.read_bytes()  # doctest: +SKIP
    """

    model: AcquisitionReader
    kind: str
    resource_id: int

    def read_bytes(self) -> bytes:
        """
        Request the bound resource on every call and return the reader's payload unchanged.

        Metadata is discarded. No byte conversion, copy, local file access,
        validation, or retry is added; reader and unpacking failures propagate.

        Example:
            >>> content = stored.read_bytes()  # doctest: +SKIP


        :return: The exact payload object supplied as the reader's second result, expected to be bytes.
        """
        _resource, payload = self.model.acquisition_read(self.kind, self.resource_id)
        return payload

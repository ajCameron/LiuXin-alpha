"""Driver-local keys and file information."""

from __future__ import annotations

import dataclasses

from datetime import datetime
from typing import TypeAlias

from LiuXin_alpha.storage.api2.models import Digest


@dataclasses.dataclass(slots=True, frozen=True)
class DriverKey:
    """Opaque concrete-object key understood only by one store driver.

    Higher layers may persist or compare the value, but must not parse it or
    infer directory semantics from it.

    Example:
        >>> key = DriverKey("objects/ab/payload")
        >>> str(key)
        'objects/ab/payload'
    """

    value: str

    def __post_init__(self) -> None:
        """Reject empty keys and embedded NUL characters.

        Example:
            >>> DriverKey("")
            Traceback (most recent call last):
            ...
            ValueError: driver key must not be empty.
        """
        if not self.value:
            raise ValueError("driver key must not be empty.")
        if "\x00" in self.value:
            raise ValueError("driver key must not contain NUL characters.")

    def __str__(self) -> str:
        """Return the persistable opaque key value.

        Example:
            >>> str(DriverKey("object-42"))
            'object-42'
        """
        return self.value


DriverKeyInput: TypeAlias = DriverKey | str


@dataclasses.dataclass(slots=True, frozen=True)
class DriverFileInfo:
    """Authoritative driver-local information for one stored object.

    The configured store wrapper converts this value into ``FileInfo`` by
    pairing ``key`` with its own ``store_ref``.

    Example:
        >>> info = DriverFileInfo(DriverKey("objects/42"), size=4, version="v2")
        >>> (str(info.key), info.size)
        ('objects/42', 4)
    """

    key: DriverKey
    size: int
    modified_at: datetime | None = None
    digest: Digest | None = None
    version: str | None = None
    metadata: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        """Reject negative object sizes and duplicate metadata keys.

        Example:
            >>> DriverFileInfo(DriverKey("bad"), size=-1)
            Traceback (most recent call last):
            ...
            ValueError: driver file size must not be negative.
        """
        if self.size < 0:
            raise ValueError("driver file size must not be negative.")
        metadata_keys = tuple(key for key, _value in self.metadata)
        if len(metadata_keys) != len(set(metadata_keys)):
            raise ValueError("driver metadata keys must be unique.")


__all__ = ["DriverFileInfo", "DriverKey", "DriverKeyInput"]

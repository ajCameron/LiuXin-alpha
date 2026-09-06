"""Portable acquisition targets and the narrow byte-reading surface contract."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol


class AcquisitionReader(Protocol):
    """Read resource metadata and bytes without exposing a concrete Core model."""

    def acquisition_read(
        self, kind: str, resource_id: int
    ) -> tuple[Mapping[str, object], bytes]: ...


@dataclass(frozen=True)
class ResolvedFileTarget:
    """A surface delivery target, independent of HTTP application construction."""

    mode: str
    location: str
    download_name: str


@dataclass(frozen=True)
class CoreStoredFile:
    """Read one Core-backed acquisition resource through its injected reader."""

    model: AcquisitionReader
    kind: str
    resource_id: int

    def read_bytes(self) -> bytes:
        """Return exactly the bytes supplied by the acquisition reader."""
        _resource, payload = self.model.acquisition_read(self.kind, self.resource_id)
        return payload

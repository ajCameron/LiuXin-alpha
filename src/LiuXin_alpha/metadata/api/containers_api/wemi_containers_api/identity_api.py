"""Shared WEMI identity API contract.

Category: core WEMI identity object.
This module defines the small surface shared by work, expression,
manifestation, and item identity rows.
"""
from __future__ import annotations

import abc
from typing import ClassVar, Self

from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.relation_target_api import (
    MetadataRecord,
    MutableMetadataRecord,
)


class WemiIdentityAPI(abc.ABC):
    """Common contract for one WEMI identity row."""

    WEMI_LEVEL: ClassVar[str]
    SOURCE_TABLE: ClassVar[str]
    ID_FIELD: ClassVar[str]

    @property
    @abc.abstractmethod
    def id(self) -> int | None:
        """Primary row ID for this WEMI identity."""

    @id.setter
    @abc.abstractmethod
    def id(self, value: int | None) -> None:
        """Set the primary row ID for this WEMI identity."""

    @classmethod
    @abc.abstractmethod
    def from_mapping(cls, row: MetadataRecord) -> Self:
        """Build an identity object from a mapping keyed by database columns."""

    @abc.abstractmethod
    def to_mapping(self) -> MutableMetadataRecord:
        """Convert this identity row to a mapping keyed by database columns."""

    def __str__(self) -> str:
        return f"{self.__class__.__name__}()"


__all__ = ["WemiIdentityAPI"]

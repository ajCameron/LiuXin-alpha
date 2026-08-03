"""Contracts for coordinated semantic Catalog mutations."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from ..common import CreatedWemiStack
from .metadata_writer import MetadataWriterAPI
from .mutation_policy import MutationPolicyAPI


@runtime_checkable
class CatalogMutationsAPI(Protocol):
    """Grouped mutation API exposed by the Catalog facade.

    ``policy`` is a side-effect-free preflight surface. ``writer`` performs
    multi-table attachment and merge operations transactionally.
    """

    writer: MetadataWriterAPI
    policy: MutationPolicyAPI


__all__ = [
    "CatalogMutationsAPI",
    "CreatedWemiStack",
    "MetadataWriterAPI",
    "MutationPolicyAPI",
]

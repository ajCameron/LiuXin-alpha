"""Mutation policy API contracts for the catalog layer."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from .metadata_writer import MetadataWriterAPI
from .mutation_policy import MutationPolicyAPI


@runtime_checkable
class CatalogMutationsAPI(Protocol):
    """Grouped mutation API exposed by the catalog facade."""

    writer: MetadataWriterAPI
    policy: MutationPolicyAPI


__all__ = ["CatalogMutationsAPI", "MetadataWriterAPI", "MutationPolicyAPI"]

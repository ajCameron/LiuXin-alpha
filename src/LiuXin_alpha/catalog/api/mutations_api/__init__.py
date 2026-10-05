"""
Export contracts for coordinated Catalog writes and preliminary mutation policy.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from ..common import CreatedWemiStack
from .metadata_writer import MetadataWriterAPI
from .mutation_policy import MutationPolicyAPI


@runtime_checkable
class CatalogMutationsAPI(Protocol):
    """
    Describe grouped policy and writer services exposed by Catalog.

    Policy reads do not reserve entities. Transaction support depends on the
    writer operation and supplied macro handle; attachment/merge permit a
    legacy fallback without a transaction.

    Example:
        A consumer can inspect mutations.policy and perform a supported change
        through mutations.writer.
    """

    writer: MetadataWriterAPI
    policy: MutationPolicyAPI


__all__ = [
    "CatalogMutationsAPI",
    "CreatedWemiStack",
    "MetadataWriterAPI",
    "MutationPolicyAPI",
]

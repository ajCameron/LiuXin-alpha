"""
Compose and export Catalog mutation policy and coordinated writer services.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ..api.common import CreatedWemiStack, DatabaseHandle
from .metadata_writer import MetadataWriter
from .mutation_policy import MutationPolicy


@dataclass(slots=True)
class CatalogMutations:
    """
    Create policy and writer services sharing the Catalog database and repositories.

    Example:
        The writer retains the same policy object exposed as mutations.policy.
    """

    db: DatabaseHandle
    repositories: Any
    writer: MetadataWriter = field(init=False)
    policy: MutationPolicy = field(init=False)

    def __post_init__(self) -> None:
        """
        Construct a policy and inject it into a fresh MetadataWriter.

        Example:
            Manual reinvocation replaces both objects without querying or validating the database.


        :return: None; replaces policy/writer service fields.
        """
        self.policy = MutationPolicy(self.db, self.repositories)
        self.writer = MetadataWriter(self.db, self.repositories, self.policy)


__all__ = [
    "CatalogMutations",
    "CreatedWemiStack",
    "MetadataWriter",
    "MutationPolicy",
]

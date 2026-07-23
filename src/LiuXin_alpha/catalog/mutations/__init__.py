"""
Mutation/policy implementations for the catalog layer.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ..api.common import DatabaseHandle
from .metadata_writer import CreatedWemiStack, MetadataWriter
from .mutation_policy import MutationPolicy


@dataclass(slots=True)
class CatalogMutations:
    """
    Grouped mutation services exposed by `Catalog.mutations`.
    """

    db: DatabaseHandle
    repositories: Any
    writer: MetadataWriter = field(init=False)
    policy: MutationPolicy = field(init=False)

    def __post_init__(self) -> None:
        """
        Called to pass the db class on.

        :return:
        """
        self.policy = MutationPolicy(self.db, self.repositories)
        self.writer = MetadataWriter(self.db, self.repositories, self.policy)


__all__ = [
    "CatalogMutations",
    "CreatedWemiStack",
    "MetadataWriter",
    "MutationPolicy",
]

"""
Mutation/policy implementations for the catalog layer.

Perhaps "storage" would be a more natural name for this, but it clashes with the "storage" top level module.
So here we are.

"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ..api.common import DatabaseHandle
from .metadata_writer import MetadataWriter
from .mutation_policy import MutationPolicy


@dataclass(slots=True)
class CatalogWriting:
    """
    Grouped writing services exposed by `Catalog.write`.
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


__all__ = ["CatalogWriting", "MetadataWriter", "MutationPolicy"]

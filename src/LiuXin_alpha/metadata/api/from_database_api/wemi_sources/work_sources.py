"""Work-facing metadata source contracts.

These APIs describe read-side database access for core work identity and
work metadata bundles. They are source-layer contracts, not metadata
containers themselves.
"""

from __future__ import annotations

import abc
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api import DatabaseAPI
    from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.work_containers.work_identity_api import WorkIdentityAPI
    from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.work_containers.work_metadata_api import WorkMetadataAPI
    from LiuXin_alpha.metadata.metadata_types import WorkID


class WorkMetadataGetterAPI(abc.ABC):
    """Read work identities and work metadata bundles from the database."""

    db: 'DatabaseAPI'

    def __init__(self, db: 'DatabaseAPI') -> None:
        self.db = db

    @abc.abstractmethod
    def get_work_identity(self, work_id: 'WorkID') -> 'WorkIdentityAPI':
        """Get the narrow identity container for one work."""

    @abc.abstractmethod
    def get_work_metadata(self, work_id: 'WorkID') -> 'WorkMetadataAPI':
        """Get the editable metadata bundle for one work."""

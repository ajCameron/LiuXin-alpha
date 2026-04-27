"""Manifestation-facing metadata source contracts.

These APIs describe read-side database access for core manifestation
identity and manifestation metadata bundles.
"""

from __future__ import annotations

import abc
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api import DatabaseAPI
    from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.manifestation_containers.manifestation_identity_api import ManifestationIdentityAPI
    from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.manifestation_containers.manifestation_metadata_api import ManifestationMetadataAPI
    from LiuXin_alpha.metadata.metadata_types import ManifestationID


class ManifestationMetadataGetterAPI(abc.ABC):
    """Read manifestation identities and manifestation metadata bundles from the database."""

    db: 'DatabaseAPI'

    def __init__(self, db: 'DatabaseAPI') -> None:
        self.db = db

    @abc.abstractmethod
    def get_manifestation_identity(self, manifestation_id: 'ManifestationID') -> 'ManifestationIdentityAPI':
        """Get the narrow identity container for one manifestation."""

    @abc.abstractmethod
    def get_manifestation_metadata(self, manifestation_id: 'ManifestationID') -> 'ManifestationMetadataAPI':
        """Get the editable metadata bundle for one manifestation."""

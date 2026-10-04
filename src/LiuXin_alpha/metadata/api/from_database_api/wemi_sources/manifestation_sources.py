"""
Define database-backed manifestation identity and metadata-bundle getters.

The module makes ordering, fallback, ownership and optional-integration behavior
explicit for callers.

Example:
    Exercise manifestation sources with the owning regression module::

        python -m pytest -q tests/metadata/containers/test_manifestation_metadata_hydrator.py
"""

from __future__ import annotations

import abc
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api import DatabaseAPI
    from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.manifestation_containers.manifestation_identity_api import ManifestationIdentityAPI
    from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.manifestation_containers.manifestation_metadata_api import ManifestationMetadataAPI
    from LiuXin_alpha.metadata.metadata_types import ManifestationID


class ManifestationMetadataGetterAPI(abc.ABC):
    """
    Contract manifestation identity and editable metadata-bundle reads.

    Example:
        Exercise ManifestationMetadataGetterAPI with the owning regression module::

            python -m pytest -q tests/metadata/containers/test_manifestation_metadata_hydrator.py
    """

    db: 'DatabaseAPI'

    def __init__(self, db: 'DatabaseAPI') -> None:
        """
        Bind a manifestation metadata getter to its database dependency.

        Example:
            Exercise ManifestationMetadataGetterAPI.  init   with the owning regression module::

                python -m pytest -q tests/metadata/containers/test_manifestation_metadata_hydrator.py


        :param db: Database dependency used by inherited or typed metadata getters.
        :return: None.
        """
        self.db = db

    @abc.abstractmethod
    def get_manifestation_identity(self, manifestation_id: 'ManifestationID') -> 'ManifestationIdentityAPI':
        """
        Return the narrow identity container for one manifestation.

        Example:
            Exercise ManifestationMetadataGetterAPI.get manifestation identity with the owning regression module::

                python -m pytest -q tests/metadata/containers/test_manifestation_metadata_hydrator.py


        :param manifestation_id: Manifestation identifier used to load identity or metadata.
        :return: The normalized row, metadata object or value described above.
        """

    @abc.abstractmethod
    def get_manifestation_metadata(self, manifestation_id: 'ManifestationID') -> 'ManifestationMetadataAPI':
        """
        Return the editable metadata bundle for one manifestation.

        Example:
            Exercise ManifestationMetadataGetterAPI.get manifestation metadata with the owning regression module::

                python -m pytest -q tests/metadata/containers/test_manifestation_metadata_hydrator.py


        :param manifestation_id: Manifestation identifier used to load identity or metadata.
        :return: The normalized row, metadata object or value described above.
        """

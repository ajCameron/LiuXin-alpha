"""
Define database-backed work identity and metadata-bundle getters.

The module makes ordering, fallback, ownership and optional-integration behavior
explicit for callers.

Example:
    Exercise work sources with the owning regression module::

        python -m pytest -q tests/metadata/containers/test_work_metadata_hydrator.py
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
    """
    Contract work identity and editable metadata-bundle reads.

    Example:
        Exercise WorkMetadataGetterAPI with the owning regression module::

            python -m pytest -q tests/metadata/containers/test_work_metadata_hydrator.py
    """

    db: 'DatabaseAPI'

    def __init__(self, db: 'DatabaseAPI') -> None:
        """
        Bind a work metadata getter to its database dependency.

        Example:
            Exercise WorkMetadataGetterAPI.  init   with the owning regression module::

                python -m pytest -q tests/metadata/containers/test_work_metadata_hydrator.py


        :param db: Database dependency used by inherited or typed metadata getters.
        :return: None.
        """
        self.db = db

    @abc.abstractmethod
    def get_work_identity(self, work_id: 'WorkID') -> 'WorkIdentityAPI':
        """
        Return the narrow identity container for one work.

        Example:
            Exercise WorkMetadataGetterAPI.get work identity with the owning regression module::

                python -m pytest -q tests/metadata/containers/test_work_metadata_hydrator.py


        :param work_id: Work identifier used to load identity or metadata.
        :return: The normalized row, metadata object or value described above.
        """

    @abc.abstractmethod
    def get_work_metadata(self, work_id: 'WorkID') -> 'WorkMetadataAPI':
        """
        Return the editable metadata bundle for one work.

        Example:
            Exercise WorkMetadataGetterAPI.get work metadata with the owning regression module::

                python -m pytest -q tests/metadata/containers/test_work_metadata_hydrator.py


        :param work_id: Work identifier used to load identity or metadata.
        :return: The normalized row, metadata object or value described above.
        """

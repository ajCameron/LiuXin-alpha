"""
Define database-backed expression identity and metadata-bundle getters.

The module makes ordering, fallback, ownership and optional-integration behavior
explicit for callers.

Example:
    Exercise expression sources with the owning regression module::

        python -m pytest -q tests/metadata/containers/test_expression_metadata_hydrator.py
"""

from __future__ import annotations

import abc
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api import DatabaseAPI
    from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.expression_containers.expression_identity_api import ExpressionIdentityAPI
    from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.expression_containers.expression_metadata_api import ExpressionMetadataAPI
    from LiuXin_alpha.metadata.metadata_types import ExpressionID


class ExpressionMetadataGetterAPI(abc.ABC):
    """
    Contract expression identity and editable metadata-bundle reads.

    Example:
        Exercise ExpressionMetadataGetterAPI with the owning regression module::

            python -m pytest -q tests/metadata/containers/test_expression_metadata_hydrator.py
    """

    db: 'DatabaseAPI'

    def __init__(self, db: 'DatabaseAPI') -> None:
        """
        Bind an expression metadata getter to its database dependency.

        Example:
            Exercise ExpressionMetadataGetterAPI.  init   with the owning regression module::

                python -m pytest -q tests/metadata/containers/test_expression_metadata_hydrator.py


        :param db: Database dependency used by inherited or typed metadata getters.
        :return: None.
        """
        self.db = db

    @abc.abstractmethod
    def get_expression_identity(self, expression_id: 'ExpressionID') -> 'ExpressionIdentityAPI':
        """
        Return the narrow identity container for one expression.

        Example:
            Exercise ExpressionMetadataGetterAPI.get expression identity with the owning regression module::

                python -m pytest -q tests/metadata/containers/test_expression_metadata_hydrator.py


        :param expression_id: Expression identifier used to load identity or metadata.
        :return: The normalized row, metadata object or value described above.
        """

    @abc.abstractmethod
    def get_expression_metadata(self, expression_id: 'ExpressionID') -> 'ExpressionMetadataAPI':
        """
        Return the editable metadata bundle for one expression.

        Example:
            Exercise ExpressionMetadataGetterAPI.get expression metadata with the owning regression module::

                python -m pytest -q tests/metadata/containers/test_expression_metadata_hydrator.py


        :param expression_id: Expression identifier used to load identity or metadata.
        :return: The normalized row, metadata object or value described above.
        """

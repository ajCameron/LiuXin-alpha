"""Expression-facing metadata source contracts.

These APIs describe read-side database access for core expression identity
and expression metadata bundles.
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
    """Read expression identities and expression metadata bundles from the database."""

    db: 'DatabaseAPI'

    def __init__(self, db: 'DatabaseAPI') -> None:
        self.db = db

    @abc.abstractmethod
    def get_expression_identity(self, expression_id: 'ExpressionID') -> 'ExpressionIdentityAPI':
        """Get the narrow identity container for one expression."""

    @abc.abstractmethod
    def get_expression_metadata(self, expression_id: 'ExpressionID') -> 'ExpressionMetadataAPI':
        """Get the editable metadata bundle for one expression."""

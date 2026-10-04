"""
Define database-backed item identity and metadata-bundle getters with optional preloaded rows.

The module makes ordering, fallback, ownership and optional-integration behavior
explicit for callers.

Example:
    Exercise items sources with the owning regression module::

        python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py
"""

from __future__ import annotations

import abc
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api import DatabaseAPI
    from LiuXin_alpha.databases.row import Row
    from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.relation_target_api import MetadataRecord
    from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.item_containers.item_identity_api import ItemIdentityAPI
    from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.item_containers.item_metadata_api import ItemMetadataAPI
    from LiuXin_alpha.metadata.metadata_types import ItemID


class ItemMetadataGetterAPI(abc.ABC):
    """
    Contract item identity and editable metadata-bundle reads.

    Example:
        Exercise ItemMetadataGetterAPI with the owning regression module::

            python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py
    """

    db: 'DatabaseAPI'

    def __init__(self, db: 'DatabaseAPI') -> None:
        """
        Bind an item metadata getter to its database dependency.

        Example:
            Exercise ItemMetadataGetterAPI.  init   with the owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param db: Database dependency used by inherited or typed metadata getters.
        :return: None.
        """
        self.db = db

    @abc.abstractmethod
    def get_item_identity(self, item_id: 'ItemID') -> 'ItemIdentityAPI':
        """
        Return the narrow identity container for one item.

        Example:
            Exercise ItemMetadataGetterAPI.get item identity with the owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param item_id: Item identifier used to locate the item-centred WEMI slice.
        :return: The normalized row, metadata object or value described above.
        """

    @abc.abstractmethod
    def get_item_metadata(
        self,
        item_id: 'ItemID' | None = None,
        source_row: 'MetadataRecord' | 'Row' | None = None,
    ) -> 'ItemMetadataAPI':
        """
        Return item metadata using an item id or a row that already carries WEMI context.

        Example:
            Exercise ItemMetadataGetterAPI.get item metadata with the owning regression module::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param item_id: Item identifier used to locate the item-centred WEMI slice.
        :param source_row: Optional preloaded row carrying item and related WEMI
            identifiers.
        :return: The normalized row, metadata object or value described above.
        """

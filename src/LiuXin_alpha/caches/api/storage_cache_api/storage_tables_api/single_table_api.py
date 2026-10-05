"""
Specify raw reads from a single cached table.

This API adds relationship discovery, column values and value-to-ID
lookups to the common table lifecycle. It deliberately supplies no public
mutation contract: application writes use the composed Cache/Catalog path.
"""

from __future__ import annotations

import abc
from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Any

from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.base_table_api import (
    StorageCacheBaseTableAPI,
)

if TYPE_CHECKING:
    from LiuXin_alpha.databases.db_types import MainTableID


class StorageCacheSingleTableAPI(StorageCacheBaseTableAPI):
    """
    Expose raw column values and value-index lookups for one cached table.

    Concrete backends own row storage, value equality, ordering and refresh.
    The inherited database reference is borrowed; its release follows backend
    lifecycle.

    Example:
        >>> StorageCacheSingleTableAPI.__name__
        'StorageCacheSingleTableAPI'
    """

    # -----------------
    # - LINK PROPERTIES

    @abc.abstractmethod
    def linked_to(self) -> Iterable[str]:
        """
        Enumerate table names known to be linked to this table.

        Abstract contract; this does not require that every named relation
        currently contains physical links.

        Example:
            An empty books-to-tags association can still make tags a linked table.


        :return: Iterable of linked table names, according to backend metadata.
        """

    # --------------
    # - READ METHODS

    @abc.abstractmethod
    def get_values_for(self, column: str) -> Sequence[Any]:
        """
        Read all values for one column in the table's storage order.

        Abstract contract. This is not an application sort or filtered view;
        copy ownership and row alignment depend on the implementation.

        Example:
            Two rows storing the same title can contribute two equal values.


        :param column: Column name interpreted by the backend.
        :return: Sequence of raw column values, potentially including duplicates and None.
        """

    @abc.abstractmethod
    def get_unique_values(self, column: str) -> set[Any]:
        """
        Collect distinct cached values for one column.

        Abstract contract; this does not declare or enforce a database uniqueness constraint.

        Example:
            Two equal stored titles can produce one set member.


        :param column: Column whose known values are requested.
        :return: Set of values under the backend's equality/hash semantics.
        """

    @abc.abstractmethod
    def get_ids_for_value(self, column: str, value: str) -> set[int]:
        """
        Find row identities matching a value in one cached column.

        Abstract contract. Matching normalization, hashability requirements and
        missing-column errors belong to the implementation.

        Example:
            A shared title value can match IDs {1, 2}.


        :param column: Column used for matching.
        :param value: Lookup value; concrete backends define accepted types despite the string annotation.
        :return: Set of matching integer row IDs.
        """

    @abc.abstractmethod
    def get_col_value_from_id(self, table_id: MainTableID) -> Any:
        """
        Read the table's designated value for one row identity.

        Abstract contract; the backend selects a meaningful default column and
        defines missing-ID behavior.

        Example:
            A table whose default column is title returns that title for a row ID.


        :param table_id: Row identity interpreted by the backend.
        :return: Default-column value or another backend-defined fallback representation.
        """

    # Storage components intentionally expose no public database mutation
    # contract. Application writes enter through the composed Cache facade,
    # delegate semantic persistence to Catalog, and then reconcile storage.

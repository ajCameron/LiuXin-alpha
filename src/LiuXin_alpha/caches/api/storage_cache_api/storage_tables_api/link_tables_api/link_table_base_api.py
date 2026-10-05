

"""
Define directed link-table metadata and endpoint mapping contracts.

Concrete link caches supply cardinality state and endpoint names.
Priority/type flags default false; simple accessors expose them without
schema lookup or validation. The base also requires mappings for callers
that expect singular endpoints.
"""


from __future__ import annotations

import abc
from typing import TYPE_CHECKING, Generic, TypeVar

from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.base_table_api import (
    StorageCacheBaseTableAPI,
)

if TYPE_CHECKING:
    from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.base_table_api import (
        TableTypes,
    )

T = TypeVar("T")



class StorageCacheLinkTableBaseAPI(StorageCacheBaseTableAPI, Generic[T]):
    """
    Describe a directed association view over cached physical link rows.

    Inherit table identity/database metadata and expose stored cardinality,
    priority and typed flags. Concrete subclasses initialize _table_type and
    implement endpoint names and mappings. These properties do not validate
    data or discover schema on their own.

    Example:
        A books-to-tags view uses books as primary and tags as secondary,
        regardless of how physical link columns are ordered.
    """

    _table_type: TableTypes
    _priority = False
    _typed = False

    @property
    def table_type(self) -> TableTypes:
        """
        Expose the stored relation cardinality.

        Concrete initialization must assign this attribute or provide a class default.

        Example:
            A one-to-one table subclass can inherit _table_type = TableTypes.ONE_ONE.


        :return: The current _table_type value without coercion or inference.
        """
        return self._table_type

    @property
    def priority(self) -> bool:
        """
        Expose the stored priority-support flag.

        No schema probe or ordering operation is performed.

        Example:
            A priority-enabled concrete view can sort links by its priority column.


        :return: The _priority value, defaulting to False on this base.
        """
        return self._priority

    @property
    def typed(self) -> bool:
        """
        Expose the stored link-type-support flag.

        No type-column validation or link filtering is performed.

        Example:
            A typed view can accept a link-type restriction in supported getters.


        :return: The _typed value, defaulting to False on this base.
        """
        return self._typed

    @property
    @abc.abstractmethod
    def primary_table(self) -> str:
        """
        Identify the source endpoint table for this directed view.

        Abstract contract; this is a logical orientation rather than SQL column position.

        Example:
            The primary table of a books-to-tags view is books.


        :return: Source/primary table name supplied by the backend.
        """

    @property
    @abc.abstractmethod
    def secondary_table(self) -> str:
        """
        Identify the destination endpoint table for this directed view.

        Abstract contract; a reverse view swaps the endpoint roles.

        Example:
            The secondary table of a books-to-tags view is tags.


        :return: Destination/secondary table name supplied by the backend.
        """

    @property
    @abc.abstractmethod
    def designated_secondary_col(self) -> str:
        """
        Choose the destination column used by convenience value projections.

        Abstract contract. A concrete view can use its destination table's
        default value column or a schema fallback.

        Example:
            A books-to-tags view can designate tag_name as its destination value.


        :return: Destination-column name supplied by the backend.
        """

    @abc.abstractmethod
    def get_primary_id_secondary_value_id_map(self) -> dict[int, int]:
        """
        Map known source IDs to their destination identities.

        Abstract contract. This scalar mapping cannot represent several
        destinations per source; the backend must define how incompatible data fails.

        Example:
            A one-to-one books-to-covers view can expose {1: 10, 2: 11}.


        :return: Dictionary of source ID to destination ID.
        """

    @abc.abstractmethod
    def get_secondary_id_primary_id_map(self) -> dict[int, int]:
        """
        Map known destination IDs to their source identities.

        Abstract contract. Shared destinations need backend-specific ambiguity
        handling because the return shape has only one source per key.

        Example:
            The reverse of {1: 10, 2: 11} can be represented as {10: 1, 11: 2}.


        :return: Dictionary of destination ID to source ID.
        """

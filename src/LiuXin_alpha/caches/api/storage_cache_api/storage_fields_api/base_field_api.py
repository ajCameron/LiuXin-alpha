"""
Define shared field lifecycle contracts and declared storage shapes.

Basic field operations resolve tables and maintain value projections.
Class attributes declare scalar/relation shape and mutation capabilities;
these declarations do not enforce behavior or validate update payloads.
Owner-row creation/deletion belongs to table/database APIs.
"""

from __future__ import annotations

import abc
from typing import (
    TYPE_CHECKING,
    ClassVar,
    Generic,
    Iterable,
    Literal,
    Optional,
    TypeVar,
    Union,
)

from LiuXin_alpha.databases.api import DatabaseAPI

if TYPE_CHECKING:
    from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.single_table_api import (
        StorageCacheSingleTableAPI,
    )
    from LiuXin_alpha.catalog.api.field_metadata_api import FieldMetadataAPI
    from LiuXin_alpha.databases.db_types import MainTableID, MainTableName

T = TypeVar("T")


# Todo: We should include metadata...
class FieldBasicInterfaceAPI(abc.ABC, Generic[T]):
    """
    Define field loading and cache-maintenance hooks without owner-row deletion.

    Concrete fields provide read, table resolution, ID refresh and removal.
    The base declares generic shape and false mutation capability flags, but
    provides no runtime guard or automatic metadata initialization. A field
    update must preserve owner-row lifecycle even if it mutates related rows.

    Example:
        >>> FieldBasicInterfaceAPI.deletes_owner_rows
        False
    """

    metadata: "FieldMetadataAPI"

    name: Union[Literal["text"],
                Literal["series"],
                Literal["datetime"],
                Literal["int"],
                Literal["float"],
                Literal["bool"],
                Literal["comments"],
                Literal["rating"],
                Literal["enumeration"],
                Literal["composite"],
                Literal["title"],
                Literal["author_sort"],
                Literal["authors"],
                Literal["timestamp"],
                Literal["last_modified"],
                Literal["series_index"],
                Literal["languages"],
                Literal["identifiers"]]

    #: High-level storage/behavior category for the field.
    field_storage_shape: ClassVar[str] = "generic"

    #: Whether this field mutates relationships/link rows as part of updates.
    mutates_links: ClassVar[bool] = False

    #: Whether this field may create related rows/values as part of updates.
    creates_related_rows: ClassVar[bool] = False

    #: Whether this field may delete related rows/values as part of updates
    #: or cleanup.
    deletes_related_rows: ClassVar[bool] = False

    #: Fields must not delete owner rows; keep this explicit.
    deletes_owner_rows: ClassVar[bool] = False

    @abc.abstractmethod
    def read(self, db: "DatabaseAPI") -> None:
        """
        Populate the field from its backend's selected storage state.

        Abstract contract. A field may project already loaded table snapshots
        instead of fetching database rows itself.

        Example:
            A schema scalar field copies its owner table's loaded column into a value map.


        :param db: Database reference used by the concrete implementation.
        :return: None; the backend initializes or rebuilds field state.
        """

    @abc.abstractmethod
    def get_main_table(
        self,
        name: Union[MainTableName, "StorageCacheSingleTableAPI"],
    ) -> "StorageCacheSingleTableAPI":
        """
        Resolve a field's owner or endpoint table through its cache.

        Abstract contract; reference validation and freshness are backend-specific.

        Example:
            A relation constructor resolves both books and tags through this hook.


        :param name: Main-table name or table API reference accepted by the backend.
        :return: Resolved main-table cache object.
        """

    @abc.abstractmethod
    def refresh_ids(
        self,
        ids: Iterable["MainTableID"],
        db: Optional["DatabaseAPI"] = None,
    ) -> None:
        """
        Refresh field state after selected owner IDs may have changed.

        Abstract contract. The ID hint need not bound work: some implementations
        rebuild all projections or require table data to be refreshed first.

        Example:
            A scalar backend can reread one changed title while a relation backend
            can rebuild its projection for the same hint.


        :param ids: Owner row identities indicating changed or removed state.
        :param db: Optional database override interpreted by the backend.
        :return: None; the backend repairs or rebuilds the field projection.
        """

    @abc.abstractmethod
    def remove_ids(self, ids: Iterable["MainTableID"]) -> None:
        """
        Handle owner IDs removed elsewhere without deleting those database rows.

        Abstract contract. Backends may evict local entries or rebuild from
        refreshed tables; this is not the owner-row deletion surface.

        Example:
            Notify a field after its owning database rows have already been removed.


        :param ids: Owner identities no longer needed in the field projection.
        :return: None; the concrete field updates its cached view.
        """


class ScalarFieldBasicInterfaceAPI(FieldBasicInterfaceAPI[T], abc.ABC):
    """
    Declare a scalar value stored directly on its owning row.

    Set field_storage_shape to scalar and leave all link/related-row/owner-row
    mutation flags false. Concrete updates can change or clear column values
    without deleting owner rows; lifecycle hooks remain abstract.

    Example:
        >>> ScalarFieldBasicInterfaceAPI.field_storage_shape
        'scalar'
    """

    field_storage_shape: ClassVar[str] = "scalar"
    mutates_links: ClassVar[bool] = False
    creates_related_rows: ClassVar[bool] = False
    deletes_related_rows: ClassVar[bool] = False
    deletes_owner_rows: ClassVar[bool] = False


class RelationFieldBasicInterfaceAPI(FieldBasicInterfaceAPI[T], abc.ABC):
    """
    Declare a field whose values are reached through relationships.

    Set relation shape and mutates_links=True. Related-row creation/deletion
    and owner-row deletion flags default false; these are class declarations,
    not enforcement of concrete update policy. Backends define permitted
    related-row operations while preserving owner rows.

    Example:
        >>> RelationFieldBasicInterfaceAPI.mutates_links
        True
    """

    field_storage_shape: ClassVar[str] = "relation"
    mutates_links: ClassVar[bool] = True
    creates_related_rows: ClassVar[bool] = False
    deletes_related_rows: ClassVar[bool] = False
    deletes_owner_rows: ClassVar[bool] = False

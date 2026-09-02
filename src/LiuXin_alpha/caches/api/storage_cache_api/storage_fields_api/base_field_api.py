"""Field-level cache contracts for scalar values and table relations."""

from __future__ import annotations

import abc
from typing import ClassVar, Generic, TYPE_CHECKING, Union, TypeVar, Iterable, Optional, Literal

from LiuXin_alpha.databases.api import DatabaseAPI

if TYPE_CHECKING:
    from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.single_table import (
        StorageStorageCacheSingleTableAPI,
    )
    from LiuXin_alpha.databases.db_types import MainTableName, MainTableID
    from LiuXin_alpha.catalog.api.field_metadata_api import FieldMetadataAPI

T = TypeVar("T")


# Todo: We should include metadata...
class FieldBasicInterfaceAPI(abc.ABC, Generic[T]):
    """
    Basic interface for the field system.

    Field objects may mutate values, relationships, and related rows depending
    on their concrete storage model. They must not implicitly delete owner rows;
    owner-row lifecycle belongs to table/cache/database APIs.
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
        Read off the database into the internal cache.

        :param db:
        :return:
        """

    @abc.abstractmethod
    def get_main_table(
        self,
        name: Union[MainTableName, "StorageStorageCacheSingleTableAPI"],
    ) -> "StorageStorageCacheSingleTableAPI":
        """
        Get the cached table.

        :param name:
        :return:
        """

    @abc.abstractmethod
    def refresh_ids(
        self,
        ids: Iterable["MainTableID"],
        db: Optional["DatabaseAPI"] = None,
    ) -> None:
        """
        Refresh the cache entries for the specified owning row ids.

        This is intended for incremental cache maintenance after external row
        or field changes.

        :param ids:
        :param db:
        :return:
        """

    @abc.abstractmethod
    def remove_ids(self, ids: Iterable["MainTableID"]) -> None:
        """
        Remove the specified owning row ids from the in-memory field cache.

        This is intended for cases where rows were deleted elsewhere and the
        field object is being notified after the fact.

        :param ids:
        :return:
        """


class ScalarFieldBasicInterfaceAPI(FieldBasicInterfaceAPI[T], abc.ABC):
    """
    Field whose value lives directly on the owner row.

    Scalar fields mutate column values and clear/nullify those values, but do
    not create/delete related rows or mutate link tables.
    """

    field_storage_shape: ClassVar[str] = "scalar"
    mutates_links: ClassVar[bool] = False
    creates_related_rows: ClassVar[bool] = False
    deletes_related_rows: ClassVar[bool] = False
    deletes_owner_rows: ClassVar[bool] = False


class RelationFieldBasicInterfaceAPI(FieldBasicInterfaceAPI[T], abc.ABC):
    """
    Field backed by relationships to other rows/tables.

    Relation fields may mutate link rows and, where a specific implementation
    opts in, create or delete related rows/values. They still must not delete
    owner rows implicitly.
    """

    field_storage_shape: ClassVar[str] = "relation"
    mutates_links: ClassVar[bool] = True
    creates_related_rows: ClassVar[bool] = False
    deletes_related_rows: ClassVar[bool] = False
    deletes_owner_rows: ClassVar[bool] = False

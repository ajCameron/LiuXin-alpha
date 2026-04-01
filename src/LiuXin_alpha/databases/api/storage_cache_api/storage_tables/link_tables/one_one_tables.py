
"""
Represents a one-to-one table in the cache.
"""

import abc

from typing import Generic, Optional, TypeVar, overload, TYPE_CHECKING

from LiuXin_alpha.databases.api.storage_cache_api.storage_tables.link_tables.link_table_base import StorageCacheLinkTableBaseAPI, TableTypes, T

if TYPE_CHECKING:

    from LiuXin_alpha.databases.api.storage_cache_api.storage_tables.table_updates import (
        OneOneInterLinkTableUpdate,
        OneOneInterLinkTableUpdateResults)

    from LiuXin_alpha.databases.db_types import SrcTableID, DstTableID


class StorageCacheOneToOneLinkTableAPI(StorageCacheLinkTableBaseAPI):
    """
    Represents data that is unique per table, but stored in another table.

    Also supports cases where the mapping is not actually 1-1
    (e.g. size and timestamp - which might be the same - but it's unlikely - also we don't care that much).
    This generally involved reading something from the db's "meta" view - where all information about each of the books
    is aggregated.
    """
    table_type: TableTypes = TableTypes.ONE_ONE

    _typed: bool = False
    _priority: bool = False

    @abc.abstractmethod
    def update(
        self,
        update: OneOneInterLinkTableUpdate,
    ) -> OneOneInterLinkTableUpdateResults:
        """
        Preform an update of the database and cache.

        This goes in the following order.

        - update_preflight - brings the update object into standard form
        - update_precheck - checks the update is actually valid
        (these two should be done with a lock)
        - update_cache - updates this object
        - update_db - write the update out to the db

        :param update:
        :return result:
        """

    @abc.abstractmethod
    def update_preflight(
        self,
        update: OneOneInterLinkTableUpdate,
    ) -> OneOneInterLinkTableUpdate:
        """
        Bring the update into a form where it can be more easily written out to the database.

        :param update:
        :return updated_update: The update once it's been brought into normal form.
        """

    @abc.abstractmethod
    def update_precheck(
        self,
        update: OneOneInterLinkTableUpdate,
    ) -> bool:
        """
        Check that an update is of a valid form before writing it out to the cache and the database.

        :param update:

        :return : Does the proposed update pass or not?
        """

    @abc.abstractmethod
    def update_db(
        self,
        update: OneOneInterLinkTableUpdate,
    ) -> bool:
        """
        Preform an update on the database itself.

        We expect a valid update form.
        Which we will then write out to the database.

        :param primary_id_secondary_id_map:
        :param secondary_id_map_update:
        :return:
        """

    @abc.abstractmethod
    def update_cache(
        self,
        update: OneOneInterLinkTableUpdate
    ) -> bool:
        """
        Preform an update on the database itself.

        :param primary_id_secondary_id_map:
        :param secondary_id_map_update:
        :return:
        """

    @abc.abstractmethod
    def get_primary_id_secondary_value_map(self) -> dict[int, T]:
        """
        Get a map keyed with the primary id and the value from the secondary table.

        :return:
        """


class StorageCacheItemCalibreUUIDTableAPI(StorageCacheLinkTableBaseAPI):
    """
    Represents a calibre uuid linked to its item.

    The closest direct analogue to a calibre book is the item at the end of the WEMI stack.
    This links calibre uuids to those items.
    """

    @abc.abstractmethod
    def lookup_by_uuid(self, uuid: str) -> int:
        """
        Lookup an item id by its uuid and return the id of the item as an int.

        :param uuid:
        :return:
        """

# Todo: We want a best_get or similar option on many_many and one_many so they can pretend to be one-one
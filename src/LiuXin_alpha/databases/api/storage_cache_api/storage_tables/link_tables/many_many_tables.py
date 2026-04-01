
"""
Many-to-many tables link many items to many others.
"""
from __future__ import annotations

import abc
import dataclasses

from  typing import TYPE_CHECKING

from LiuXin_alpha.databases.api.storage_cache_api.storage_tables.link_tables.getter_mixins import \
    StorageCacheGetterMixinAPI
from LiuXin_alpha.databases.api.storage_cache_api.storage_tables.link_tables.link_table_base import StorageCacheLinkTableBaseAPI
from LiuXin_alpha.databases.api.storage_cache_api.storage_tables.link_tables.one_many_tables import OneManyLink

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.storage_cache_api.storage_tables.table_updates import (
        ManyManyInterlinkTableUpdate,
        ManyManyInterLinkTableUpdateResults)


@dataclasses.dataclass(slots=True)
class ManyManyLink(OneManyLink):
    """
    Represents an element in a many-to-many link.
    """


class StorageCacheManyToManyLinkTable(StorageCacheLinkTableBaseAPI, StorageCacheGetterMixinAPI):
    """
    Represents a Many-to-many table that links many items to many others.

    These are very common - the most basic example would be tags.
    (Which is an unordered link).
    """
    @abc.abstractmethod
    def update(
        self,
        update: ManyManyInterlinkTableUpdate
    ) -> ManyManyInterLinkTableUpdateResults:
        """
        Preform an update of the database and cache.

        This goes in the following order.

        - update_preflight - brings the update object into standard form
        - update_precheck - checks the update is actually valid
        (these two should be done with a lock)
        - update_cache - updates this object
        - update_db - write the update out to the db

        :param update:
        :return:
        """

    @abc.abstractmethod
    def update_preflight(
        self,
        update: ManyManyInterlinkTableUpdate
    ) -> ManyManyInterlinkTableUpdate:
        """
        Bring the update into a form where it can be more easily written out to the database.

        :param update:
        :return:
        """

    @abc.abstractmethod
    def update_precheck(
        self,
        update: ManyManyInterlinkTableUpdate
    ) -> bool:
        """
        Check that an update is of a valid form before writing it out to the cache and the database.

        :param update: OneManyInterlinkTableUpdate - containing
        :return:
        """

    @abc.abstractmethod
    def update_db(
        self,
        update: ManyManyInterlinkTableUpdate
    ) -> bool:
        """
        Preform an update on the database itself.

        :param update:
        :return:
        """

    @abc.abstractmethod
    def update_cache(
        self,
        update: ManyManyInterlinkTableUpdate
    ) -> bool:
        """
        Preform an update on the database itself.

        :param update:
        :return:
        """

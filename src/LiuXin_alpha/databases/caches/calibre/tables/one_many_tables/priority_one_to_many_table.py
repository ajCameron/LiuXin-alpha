from collections import defaultdict
from copy import deepcopy

from typing import TypeVar, Optional, Union, Any, Iterable

from LiuXin.customize.cache.base_tables import ONE_MANY

from LiuXin.databases.caches.calibre.tables.one_many_tables.one_to_many_table import CalibreOneToManyTable
from LiuXin.databases.db_types import SrcTableID, DstTableID, MetadataDict, InterLinkTableName

from LiuXin.exceptions import InvalidCacheUpdate

from LiuXin.utils.lx_libraries.liuxin_six import iteritems
from LiuXin.utils.general_ops.python_tools import element_to_front

from past.builtins import basestring


T = TypeVar("T")


# ----------------------------------------------------------------------------------------------------------------------
#
# - ONE TO MANY TABLES


# Todo: Need to be able to deal with being fed a custom column
from LiuXin.utils.lx_libraries.liuxin_six import dict_iteritems as iteritems


class CalibrePriorityOneToManyTable(CalibreOneToManyTable[T]):
    """
    For the case where one, and only one, book is linked to many items and the items are linked to no other books.

    Priority information is also provided - so there is an order in which the elements are linked to the book.
    """

    _table_type = ONE_MANY

    _priority = True
    _typed = False

    def __init__(
        self,
        name: str,
        metadata: MetadataDict,
        link_table: Optional[InterLinkTableName] = None,
        custom: Optional[bool] = False,
    ) -> None:
        """
        Preform startup for a PriorityOneToManyTable.

        The table has the concept of priority as well
        :param name:
        :param metadata:
        :param link_table:
        :param custom:
        """
        super(CalibrePriorityOneToManyTable, self).__init__(
            name=name, metadata=metadata, link_table=link_table, custom=custom
        )

        # MAPS
        # - id_map - Keyed with the id of the object in the target table and valued with it's value
        # - book_col_map - Keyed with the book id and valued with the result from the other table in the form of a list
        # - col_book_map - Keyed with the item id and valued with the result from the books table - which should be a
        #                  single item (as there is only one book linked to each item)

    def _book_col_map_factory(self) -> dict[SrcTableID, list[DstTableID, ...]]:
        """
        Part of producing nested default dicts.

        This is a priority table - so it has the concept of order - requiring data to be stored in a list.
        However it does not have the concept of type, so it isn't necessary to nest that in a type dict.
        :return:
        """
        return defaultdict(list)

    # ------------------------------------------------------------------------------------------------------------------
    #
    # - READ METHODS

    def read_maps(self, db, type_filter: Optional[str] = None) -> None:
        """
        Preform a read into the internal caches.

        :param db:
        :param type_filter: Either a string or None
        :return None: All changes are made internally
        """
        assert type_filter is None, "type_filter is no longer in use"

        # Todo: Rename this to something more useful - characterize_link_table
        self.set_link_table(db=db, set_type=True)

        stmt = "SELECT {}, {} FROM {} ORDER BY {} DESC;".format(
            self.link_table_bt_id_column,
            self.link_table_table_id_column,
            self.link_table,
            self.link_table_priority_col,
        )

        for bt_id, other_id in db.driver_wrapper.execute(stmt):
            self.book_col_map[bt_id].append(other_id)
            self.col_book_map[other_id] = bt_id

        for book_id in self.seen_book_ids:
            if book_id not in self.book_col_map:
                self.book_col_map[book_id] = []

        for item_id in self.id_map.keys():
            if item_id not in self.col_book_map:
                self.col_book_map[item_id] = None

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - UPDATE
    def update_precheck(
        self, book_id_item_id_map: dict[SrcTableID, list[Union[DstTableID, T]]], id_map_update: dict[DstTableID, T]
    ) -> None:
        """
        Check that an update is of a valid form before writing it out to the cache and the database.

        Called when you know the ids you want to assign to the book after the update. Checks those ids are valid.
        No changes will be made to the :param book_id_item_id_map: (e.g. if the map is valued with tuples - not lists
        as expected - this will not be corrected.
        Raised InvalidCacheUpdate if the cache update is invalid in some way.
        :param book_id_item_id_map: Keyed with the ids of the books to update and valued with the
        :param id_map_update:
        :return None: An error is raised if the update does not meet spec
        """
        for update_id, update_val in iteritems(id_map_update):
            assert isinstance(update_id, int), self._id_map_not_keyed_with_int(id_map_update)
            if update_val is None:
                continue
            assert isinstance(update_val, basestring), self._id_map_not_valued_with_str(id_map_update)

        seen_ids = set()
        for book_id, book_vals in iteritems(book_id_item_id_map):

            if book_vals is None:
                continue

            if not isinstance(book_vals, list):
                raise InvalidCacheUpdate("Update must be valued with a list")

            # Strings can overlap - but ids cannot - so filter out the strings
            new_book_vals = set([bv for bv in book_vals if not isinstance(bv, basestring)])

            # Check to ensure that all the item entries for a specific book are different - this is supposed to be a
            # ONE to MANY field, after all
            seen_ids_after = seen_ids.union(set(new_book_vals))
            if len(seen_ids_after) - len(seen_ids) != len(set(new_book_vals)):
                raise InvalidCacheUpdate("overlap between book values - {}".format(book_id_item_id_map))
            seen_ids = seen_ids_after

    @staticmethod
    def _id_map_not_keyed_with_int(id_map: Any) -> str:
        """
        Error message.

        One of the keys of the id map is not an int - and so cannot be an id in the item table.
        :param id_map: The map which caused the check to fail
        :return:
        """
        err_msg = [
            "id_map has bad form - one of the keys was not a int",
            "id_map: {}".format(id_map),
        ]
        return "\n".join(err_msg)

    # Todo: Generalize to any data type
    @staticmethod
    def _id_map_not_valued_with_str(id_map: Any) -> str:
        """
        Error message.

        One of the values of the id map is not a string.
        :param id_map: The map which caused the check to fail
        :return:
        """
        err_msg = [
            "id_map has bad form - one of the vals was not a string",
            "id_map: {}".format(id_map),
        ]
        return "\n".join(err_msg)

    def update_cache(
        self,
        book_id_val_map: dict[SrcTableID, Optional[list[Union[DstTableID, T], ...]]],
        id_map: dict[DstTableID, Optional[T]] = None,
    ) -> None:
        """
        Preform a cache update from a book_id_val_map.

        :param book_id_val_map:
        :param id_map:
        :return None: All changes are made internally
        """
        if self.val_unique:
            self.update_precheck(book_id_val_map, id_map)
        else:
            self.update_precheck_unique(book_id_val_map, id_map)

        self.internal_update_cache(book_id_val_map, id_map)

    def internal_update_cache(
        self, book_id_item_id_map: dict[SrcTableID, list[DstTableID, ...]], id_map_update: dict[DstTableID, T]
    ) -> tuple[dict[SrcTableID, T], set[DstTableID]]:
        """
        Update cache with some additional information provided in the return.

        Used in write when it needs to know some info about the cache before writing out to the database.

        :param book_id_item_id_map:
        :param id_map_update: Dictionary used to directly update the id_map
        :return:
        """
        self.id_map.update(id_map_update)

        # Update the book -> col and col -> book maps
        deleted = set()
        updated = {}

        for book_id, link_val in iteritems(book_id_item_id_map):

            # Acquire a list of all the items originally linked to the book
            old_item_ids = self.book_col_map.get(book_id, set())

            # We are nullifying - remove the links
            if link_val is None:

                self.book_col_map[book_id] = []
                for item_id in old_item_ids:
                    self.col_book_map[item_id] = None

                deleted.add(book_id)
                continue

            # Nullify the existing links
            for old_item_id in deepcopy(old_item_ids):
                # Remove the item from the col_book_map
                try:
                    old_item_book_id = self.col_book_map[old_item_id]
                except KeyError:
                    continue
                if old_item_book_id is None:
                    continue

                # Note that the item is gone from the col book map as well
                self.book_col_map[old_item_book_id].remove(old_item_id)

                # Nullify after the value has been used above
                self.col_book_map[old_item_id] = None

            for new_item_id in link_val:
                # Remove the item from the col_book_map
                try:
                    new_item_book_id = self.col_book_map[new_item_id]
                except KeyError:
                    continue
                if new_item_book_id is None:
                    continue

                # Note that the item is gone from the col book map as well
                self.book_col_map[new_item_book_id].remove(new_item_id)

                # Nullify after the value has been used above
                self.col_book_map[new_item_id] = None

            # We are updating - break the old link and write a new one back out
            self.book_col_map[book_id] = link_val
            for item_id in link_val:
                self.col_book_map[item_id] = book_id

            updated[book_id] = link_val

        return updated, deleted

    def update_preflight(
        self,
        book_id_item_id_map: dict[SrcTableID, Union[str, SrcTableID, Iterable[DstTableID]]],
        id_map_update: dict[DstTableID, Optional[T]] = None,
        dirtied: Optional[set[DstTableID]] = None,
    ) -> tuple[dict[SrcTableID, Optional[list[DstTableID]]], dict[DstTableID, Optional[T]]]:
        """
        Bring the update into a form where it can be more easily written out to the database.

        :param book_id_item_id_map:
        :param id_map_update:
        :return:
        """
        id_map_update = dict() if id_map_update is None else id_map_update
        clean_book_id_item_id_map: dict[SrcTableID, Optional[list[DstTableID]]] = defaultdict(list)

        for book_id, book_vals in iteritems(book_id_item_id_map):

            if isinstance(book_vals, dict):
                raise NotImplementedError

            # The given book values to update with have to be ordered
            if isinstance(book_vals, set):
                raise NotImplementedError

            # Unmatched string - just add it into the set
            if isinstance(book_vals, (basestring, int)):
                existing_vals = deepcopy(self.book_col_map[book_id])
                if book_vals not in existing_vals:
                    clean_book_id_item_id_map[book_id] = [
                        book_vals,
                    ] + existing_vals
                else:
                    # Todo: Make sure this is used everywhere it should be
                    clean_book_id_item_id_map[book_id] = element_to_front(existing_vals, book_vals)

            elif book_vals is None:
                clean_book_id_item_id_map[book_id] = None

            elif hasattr(book_vals, "__iter__"):
                clean_book_id_item_id_map[book_id] = list(book_vals)

            else:
                raise NotImplementedError

        return clean_book_id_item_id_map, id_map_update

    def update_precheck_unique(
        self,
        book_id_item_id_map: dict[SrcTableID, Union[str, DstTableID, list[DstTableID, ...]]],
        id_map_update: dict[DstTableID, Optional[T]],
    ) -> None:
        """
        Check that an update is of a valid form before writing it out to the cache and the database.

        Called when you know the ids you want to assign to the book after the update. Checks those ids are valid.
        No changes will be made to the :param book_id_item_id_map: (e.g. if the map is valued with tuples - not lists
        as expected - this will not be corrected.
        Raised InvalidCacheUpdate if the cache update is invalid in some way.

        :param book_id_item_id_map: Keyed with the ids of the books to update and valued with the
        :param id_map_update:
        :return None: No return - will just error if a test fails
        """
        seen_ids = set()
        seen_strs = set()
        for book_id, book_vals in iteritems(book_id_item_id_map):

            # Todo: Need a mechanism to add books - and there needs to be one for each table
            if book_id not in self.book_col_map.keys():
                raise InvalidCacheUpdate("Cannot update - book_id not known")

            if book_vals is None:
                continue

            if isinstance(book_vals, (int, basestring)):
                continue

            if not isinstance(book_vals, list):
                raise InvalidCacheUpdate(self._malformed_update_error(book_id_item_id_map, book_id, book_vals))

            # Strings cannot overlap - neither can ids so filter out the strings
            # (at the point where this is called an attempt should have already been made to match the strs)
            new_book_id_vals = set([bv for bv in book_vals if not isinstance(bv, basestring)])

            # Check to ensure that all the item entries for a specific book are different - this is a unique one to
            # many field after all
            seen_ids_after = seen_ids.union(set(new_book_id_vals))
            if len(seen_ids_after) - len(seen_ids) != len(set(new_book_id_vals)):
                raise InvalidCacheUpdate("overlap between book values on ids- {}".format(book_id_item_id_map))
            seen_ids = seen_ids_after

            # Strings cannot overlap - neither can ids so filter out the strings
            # (at the point where this is called an attempt should have already been made to match the strs)
            new_book_str_vals = set([bv for bv in book_vals if isinstance(bv, basestring)])

            # Check to ensure that all the item entries for a specific book are different - this is a unique one to
            # many field after all
            seen_strs_after = seen_strs.union(set(new_book_str_vals))
            if len(seen_strs_after) - len(seen_strs) != len(set(new_book_str_vals)):
                raise InvalidCacheUpdate("overlap between book values on strs - {}".format(book_id_item_id_map))
            seen_strs = seen_strs_after

    def update_preflight_unique(
        self,
        book_id_item_id_map: dict[SrcTableID, Optional[Union[list[Union[DstTableID, str], ...], str]]],
        id_map_update: Optional[dict[DstTableID, Optional[T]]] = None,
        dirtied: set[SrcTableID] = None,
    ) -> tuple[dict[SrcTableID, list[DstTableID, ...]], dict[DstTableID, T]]:
        """
        Bring the update into a form where it can be more easily written out to the database.

        :param book_id_item_id_map:
        :param id_map_update:
        :return:
        """
        id_map_update = dict() if id_map_update is None else id_map_update
        clean_book_id_item_id_map = defaultdict(list)

        for book_id, book_vals in iteritems(book_id_item_id_map):
            if book_vals is None:
                clean_book_id_item_id_map[book_id] = []
                continue

            # Unmatched string - just add it into the set
            if isinstance(book_vals, (basestring, int)):
                # If it isn't deepcopied then changing the list changes the cache - so keep a copy for reference
                old_vals = deepcopy(self.book_col_map[book_id])

                match_book_val = self._match_item_val(book_vals)
                if match_book_val in old_vals:
                    clean_book_id_item_id_map[book_id] = element_to_front(old_vals, match_book_val)
                else:
                    clean_book_id_item_id_map[book_id] = [
                        self._match_item_val(match_book_val),
                    ] + old_vals

            elif isinstance(book_vals, (list,)):
                # Process the set - if any of the strings in it can be matched to an existing id, then they should be
                # If not, then they'll be created later
                clean_id_list = []
                for cand_item_id in book_vals:
                    if isinstance(cand_item_id, int):
                        clean_id_list.append(cand_item_id)
                    elif isinstance(cand_item_id, basestring):
                        # Todo: This is a heinous hack - which will also be quite slow
                        clean_id_list.append(self._match_item_val(cand_item_id))
                    else:
                        raise NotImplementedError

                clean_book_id_item_id_map[book_id] = clean_id_list

            else:
                raise InvalidCacheUpdate("book_vals invalid - expected set, basestring or int")

        # Check the final product of the matching is valid
        self.update_precheck_unique(clean_book_id_item_id_map, id_map_update)

        return clean_book_id_item_id_map, id_map_update

    def update_db(
        self, book_id_to_val_map: dict[SrcTableID, Optional[list[DstTableID, ...]]], db, allow_case_change: bool = False
    ) -> bool:
        """
        Write out an update to the database - which eventually uses the write methods.

        :param book_id_to_val_map: By this point we should be dealing with a well formed book_id_to_val_map dict.
        :param db: The database to write the changes out to
        :param allow_case_change: In the case the data value is a string
        :return status: Did the write out complete?
        """

        return super(CalibrePriorityOneToManyTable, self).update_db(
            book_id_to_val_map=book_id_to_val_map,
            db=db,
            allow_case_change=allow_case_change,
        )

    #
    # ------------------------------------------------------------------------------------------------------------------

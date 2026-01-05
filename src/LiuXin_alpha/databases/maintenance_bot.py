# Todo: Move maintenance bot over into the core?

# Provides a facility to do maintenance tasks in the background while the database is live.
# (Also provides for metadata backup tasks)

# Tasks which the bot can preform
# 1 - Update the metadata caches - metadata stored in folders in the memory stores
# 2 - Preforms renaming tasks - bringing the name of covers, files, folders into line with their db mandated ones
# 3 - Consolidate folders - except when it's marked that there should be multiple by the user
# 4 - Add to metadata using the internet
# 5 - Update some fields of the database which cannot be done with triggers

# A bot which trawls the database - updating metadata, moving folders into the right place, generally making sure that
# the database conforms to the instructions given it and all metadata is present and correct
# This occurs in the background (if enable) as long as LiuXin is online

# Todo: maintenance bot
# 1) Put any files (which aren't flagged as moved by the user) where they should be
# 2) Ensure one folder per author, series, title (where these exists - if not overriden by user)
# 3) Update metadata - add to metadata by using the internet (with use-throttling, so this bot doesn;t DDOS any service)
# 4) Update some fields of the database which are two laborious or involvved to easily update with triggers.

import pprint
import queue as Queue
import time
import threading
import weakref
from collections import defaultdict
from copy import deepcopy

from typing import Iterable, Optional

from LiuXin_alpha.constants import VERBOSE_DEBUG
from LiuXin_alpha.databases.database import Database

# from LiuXin.databases.database import DatabasePing
# from LiuXin.databases.row import Row

from LiuXin_alpha.errors import InputIntegrityError

from LiuXin_alpha.utils.language_tools.lx_name_manip import author_to_author_sort

from LiuXin_alpha.utils.logging import LiuXin_debug_print, default_log
from LiuXin_alpha.utils.localization import trans as _

from LiuXin_alpha.databases.api import DatabaseAPI, DatabaseMaintainerAPI, MaintenanceBotAPI

from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode

__author__ = "Cameron"


# Todo: Load this out of continue_state, or preferences
DATABASE_CHECKED = False


class Maintainer(DatabaseMaintainerAPI):
    """
    Interface between the database and the maintenance bot.
    """
    db: DatabaseAPI

    def __init__(self, db: DatabaseAPI) -> None:
        """
        Attach the database to the maintainer which will work on it.

        :param db:
        """
        super().__init__(db=db)

        self.main_table_dirtied_queue = Queue.Queue()
        self.interlink_dirtied_queue = Queue.Queue()

        self.maintainer = MaintenanceBot(
            db=self.db,
            dirtied_main_queue=self.main_table_dirtied_queue,
            dirtied_interlink_queue=self.interlink_dirtied_queue,
        )
        self.maintainer.start()

    def dirty_record(self, table: str, row_id: int) -> None:
        """
        Notify the maintenance bot that a change has occurred to the table (put it in the maintain queue).

        :param table:
        :param row_id:
        :return:
        """
        self.main_table_dirtied_queue.put((table, row_id), block=False)

    def new_dirty_record(self, table: str, row_id: int) -> None:
        """
        Replacement for the dirty record method for testing.

        :param table:
        :param row_id:
        :return:
        """
        print("NEW TABLE", table, "NEW ID", row_id)

    def dirty_interlink_record(
            self, update_type: str, table1: str, table2: str, table1_id: int, table2_id: int
    ) -> None:
        """
        Notify the maintenance bot that an interlink record has been changed.

        Used for updating the books_aggregate table when stuff happens to the relevant other tables.
        :param update_type:
        :param table1:
        :param table2:
        :param table1_id:
        :param table2_id:
        :return:
        """
        self.interlink_dirtied_queue.put((update_type, table1, table2, table1_id, table2_id))

    def clean(self, table: str, item_ids: Iterable[int]) -> None:
        """
        Clean the relevant table of the relevant item_ids

        :param table:
        :param item_ids:
        :return:
        """
        pass

    def merge(self, table: str, item_1_id: int, item_2_id: int) -> None:
        """
        Consider merging two items on the database.

        :param table:
        :param item_1_id:
        :param item_2_id: All the item 2 ids will be repointed to item_1_id - then it'll be deleted
        :return:
        """
        for main_table in self.db.main_tables:

            if main_table == table:
                continue

            link_table = self.db.driver_wrapper.get_link_table_name(table1=table, table2=main_table)
            if link_table is None:
                continue
            if link_table not in self.db.interlink_tables:
                continue

            self._do_merge_one_table(
                src_table=main_table,
                dst_table=table,
                link_table=link_table,
                item_1_id=item_1_id,
                item_2_id=item_2_id,
            )

        item_2_row = self.db.get_row_from_id(table, row_id=item_2_id)
        self.db.delete(item_2_row)

    def _do_merge_one_table(
            self, src_table: str, dst_table: str, link_table: str, item_1_id: int, item_2_id: int
    ) -> None:
        """
        Merge two tags linked to the given table.

        :param src_table:
        :param dst_table:
        :param link_table:
        :param item_1_id:
        :param item_2_id:
        :return:
        """
        dst_table_id_col = self.db.driver_wrapper.get_id_column(dst_table)
        link_table_tag_id_col = self.db.driver_wrapper.get_link_column(
            table1=dst_table, table2=src_table, column_type=dst_table_id_col
        )
        link_table_id_col = self.db.driver_wrapper.get_id_column(link_table)

        # Retrieve the links to do a repoint on
        affect_link_ids = self.db.macros.get_values_one_condition(
            table=link_table,
            rtn_column=link_table_id_col,
            cond_column=link_table_tag_id_col,
            value=item_2_id,
            default_value=(),
        )

        for link_table_id in affect_link_ids:

            link_table_row = self.db.get_row_from_id(link_table, link_table_id)

            link_table_row[link_table_tag_id_col] = item_1_id
            try:
                link_table_row.sync()
            except:
                # Link already exists - no repoint is needed - just remove the extraneous link
                self.db.delete(link_table_row)


# ----------------------------------------------------------------------------------------------------------------------
# - THE MAINTENANCE BOT ITSELF
# ----------------------------------------------------------------------------------------------------------------------


class MaintenanceBot(threading.Thread, MaintenanceBotAPI):
    """
    Continuously checks the database and generates some of the computationally expensive derived quantities.

    As an alternative to using triggers - for things which can be done later.
    """

    def __init__(
        self,
        db: Database,
        dirtied_main_queue,
        dirtied_interlink_queue,
        interval: int = 2,
        scheduling_interval: float = 0.5,
    ) -> None:
        """
        The Maintenance bot exist to do maintenance work to the database.

        :param db:
        :param dirtied_main_queue:
        :param dirtied_interlink_queue:
        :param interval:
        :param scheduling_interval:
        """

        threading.Thread.__init__(self)
        self.daemon = True
        try:
            self.__db = weakref.ref(db)
        except TypeError:
            # Hopefully thrown as we already have a weakref to the db
            self.__db = db
        self.keep_running = True

        # The queues used to instruct the class to do stuff
        self.main_table_queue = dirtied_main_queue
        self.interlink_queue = dirtied_interlink_queue

        # Controls the behavior of the thread
        self.interval = interval
        self.scheduling_interval = scheduling_interval

    def stop(self) -> None:
        """
        Preform thread shutdown.

        :return:
        """
        self.keep_running = False

    def rename_item(
            self,
            item_id: int,
            table: str,
            value: bool,
            now: bool = True,
            db: Optional[DatabaseAPI] = None) -> None:
        """
        Register a rename action has occurred on an item.

        :param item_id:
        :param table:
        :param value: The item value will be renamed to this
        :param now:
        :param db:
        :return:
        """
        if table == "creators":
            creator_row = db.get_row_from_id("creators", row_id=item_id)
            creator_row["creator"] = value
            creator_row.sync()

        else:

            pass

    def run(self):
        while self.keep_running:

            try:
                time.sleep(self.interval)  # Limit to one record to update every two seconds
                try:
                    table, row_id = self.main_table_queue.get_nowait()
                except Queue.Empty:
                    continue
                print("table", table, "row_id", row_id)
            except Exception as e:
                debug_str = "Happens during interpreter shutdown - MetadataBackupThread has exited as expected"
                default_log.log_exception(debug_str, e, "DEBUG")
                break

            if not self.keep_running:
                break


# ----------------------------------------------------------------------------------------------------------------------
# - TRIGGERS TO UPDATE THE TITLES_AGGREGATE TABLE - INTENDED TO BE RUN IN A SEPERATE THREAD
# ----------------------------------------------------------------------------------------------------------------------


def run_ta_updates(ta_row_id_list, database_driver):
    """
    Launches the ta_trigger thread.

    :param ta_row_id_list:
    :param database:
    :return:
    """
    ta_row_id_list = deepcopy(ta_row_id_list)
    database = Database()
    ta_trigger_thread = threading.Thread(name="ta_update_thread", target=ta_trigger, args=(ta_row_id_list, database))
    ta_trigger_thread.setDaemon(True)
    LiuXin_debug_print("run_ta_update starting")
    ta_trigger_thread.start()
    LiuXin_debug_print("run_ta_update away")


def ta_trigger(ta_row_id_list, database):
    """
    Takes a list of ta_row_ids. Fills in the derived quantities which have not already been filled in.
    :param ta_row_id_list:
    :param database:
    :return:
    """
    if VERBOSE_DEBUG:
        LiuXin_debug_print("ta_trigger_started.")

    populate_ta_creators_tags(ta_row_id_list, database)
    populate_ta_title_tags(ta_row_id_list, database)
    populate_ta_series_tags(ta_row_id_list, database)
    populate_ta_series_aggregate(ta_row_id_list, database)
    populate_ta_genre_aggregate(ta_row_id_list, database)
    populate_ta_identifiers_aggregate(ta_row_id_list, database)
    populate_ta_publishers_aggregate(ta_row_id_list, database)


# Implements the logic of a trigger in Python.
# While much less efficient, does allow for multi-threading.
# Which should hopefully make it faster/allow it to be done later
def populate_ta_creators_tags(ta_row_id_list, database):
    """
    Populates the ta_creators_tags field.
    Meant to be run in a separate thread, so as to not way the database down further with more triggers.
    :param ta_row_id_list: A list of title/ta ids
    :return None: All changes are made internally to the database.
    """
    # If the database is None, using the default database
    if database is None:
        database = Database()
    ta_row_id_list = deepcopy(ta_row_id_list)

    # This should be the id of a title in the titles table.
    # - use it to find all the creator ids associated with this title.
    # - then find all the tags associated to those creators.
    # - put all of them in a single set, and write that set out to the database
    for title_row_id in ta_row_id_list:

        # Accumulating the creators linked to the title
        creator_title_row_dicts = database.search_table_row_dict(
            table="creator_title_links",
            column="creator_title_link_title_id",
            search_term=title_row_id,
        )
        creator_ids = set()
        for link_row_dict in creator_title_row_dicts:
            creator_ids.add(link_row_dict["creator_title_link_creator_id"])

        # Accumulating the tags linked to all those creators
        tags = set()
        for creator_id in creator_ids:

            tag_ids = set()
            creator_tag_row_dicts = database.search_table_row_dict(
                table="creator_tag_links",
                column="creator_tag_link_creator_id",
                search_term=creator_id,
            )

            for creator_tag_link in creator_tag_row_dicts:
                tag_ids.add(creator_tag_link["creator_tag_link_tag_id"])

            for tag_id in tag_ids:
                tag_row_dict = database.get_row_from_id(table="tags", row_id=tag_id, row_dict_return=True)
                tags.add(tag_row_dict["tag"])

        ta_update_dict = {"ta_title_id": title_row_id, "ta_creators_tags": tags}
        database.update_row_dict(ta_update_dict)


# Populates a list of the tags associated to a title
def populate_ta_title_tags(ta_row_id_list, database):
    """
    Takes a list of title_ids - searches the database for tags linked to those titles. Builds a set of all these tags
    and updates the ta_tags column with them.
    :param ta_row_id_list:
    :param database:
    :return:
    """
    if database is None:
        database = Database()
    ta_row_id_list = deepcopy(ta_row_id_list)

    # Should be a list of ids of a title in the titles table
    # - use it to find all the tags associated with this title
    # - build a set of these tags and write it to the database
    for title_row_id in ta_row_id_list:

        tag_ids = set()
        tag_title_row_dicts = database.search_table_row_dict(
            table="tag_title_links",
            column="tag_title_link_title_id",
            search_term=title_row_id,
        )
        for tag_title_link in tag_title_row_dicts:
            tag_ids.add(tag_title_link["tag_title_link_tag_id"])

        tags = set()
        for tag_id in tag_ids:
            tag_row_dict = database.get_row_from_id(table="tags", row_id=tag_id, row_dict_return=True)
            tags.add(tag_row_dict["tag"])

        ta_update_dict = {"ta_title_id": title_row_id, "ta_tags": tags}
        database.update_row_dict(ta_update_dict)


def populate_ta_series_tags(ta_row_id_list, database):
    """
    Populates the ta_series_tags column.
    This is populated by scanning all the series above this one in the series tree - finding all the tags assocated with
    them and building a set of all of them.
    :param ta_row_id_list:
    :param database:
    :return:
    """
    if database is None:
        database = Database()
    ta_row_id_list = deepcopy(ta_row_id_list)

    # Should be a list of ids to the titles table
    # - uses it to find the liner row index associated with every series associated to the title
    # - builds a set of these tags and writes them into the database
    for title_row_id in ta_row_id_list:

        # Builds a collection of all series_ids associated with this title_row_id
        series_ids = set()
        series_title_link_dicts = database.search_table_row_dict(
            table="series_title_links",
            column="series_title_link_title_id",
            search_term=title_row_id,
        )
        for link_dict in series_title_link_dicts:
            series_ids.add(link_dict["series_title_link_series_id"])

        # Uses those series ids to build a linear list of rows back to the root series. Gets the ids of every series in
        # that linear list - these will be used to populate the full tags fields
        all_series_ids = set()
        for series_id in series_ids:
            all_series_ids.add(series_id)
            current_series_row_dict = database.get_row_from_id(table="series", row_id=series_id)
            linear_row_list = database.get_linear_row_index(start_row=current_series_row_dict, row_dict_return=True)
            for series_row in linear_row_list:
                all_series_ids.add(series_row["series_id"])

        # Iterates through the series_ids and collects all the tag_ids associated with them
        tag_ids = set()
        for series_id in all_series_ids:

            series_tag_link_dicts = database.search_table_row_dict(
                table="series_tag_links",
                column="series_tag_link_series_id",
                search_term=series_id,
            )
            for link in series_tag_link_dicts:
                tag_ids.add(link["series_tag_link_tag_id"])

        # Takes the tag_ids and retrieves the actual tags - saves them in a set and updates the database
        tags = set()
        for tag_id in tag_ids:
            tag_row = database.get_row_from_id(table="tags", row_id=tag_id, row_dict_return=True)
            tags.add(tag_row["tag"])

        # If there are no tags set, then there is no reason to continue
        if tags:
            ta_update_dict = {"ta_title_id": title_row_id, "ta_series_tags": tags}
            database.update_row_dict(ta_update_dict)


def populate_ta_series_aggregate(ta_row_id_list, database):
    """
    Builds a series aggregate - a string containing all the series the title row associated with that id is linked to.
    :param ta_row_id_list: A list of title_ids in the title_aggregate table to update
    :param database:
    :return None: Changes are made purely internally to the database
    """
    if database is None:
        database = Database()
    ta_row_id_list = deepcopy(ta_row_id_list)

    # Should be a list of ids in the title table
    # - Gets a string representation of every series tree the title is linked to. Puts them together to produce a
    # representation of every series the title is linked to
    for title_row_id in ta_row_id_list:

        series_title_link_dicts = database.search_table_row_dict(
            table="series_title_links",
            column="series_title_link_title_id",
            search_term=title_row_id,
        )
        series_title_link_dicts = sorted(series_title_link_dicts, key=lambda x: x["series_title_link_priority"])

        series_ids = []
        for link_dict in series_title_link_dicts:
            series_ids.append(link_dict["series_title_link_series_id"])

        series_reps = []
        for series_id in series_ids:

            series_row_dict = database.get_row_from_id(table="series", row_id=series_id, row_dict_return=True)
            series_linear_column_list = database.get_linear_index_of_columns(
                start_row=series_row_dict, display_column="series"
            )
            series_linear_str = ":".join(series_linear_column_list)
            series_reps.append(series_linear_str)

        full_series_str = " & ".join(series_reps)

        if full_series_str:
            ta_update_dict = {
                "ta_title_id": title_row_id,
                "ta_series_aggregate": full_series_str,
            }
            database.update_row_dict(ta_update_dict)


def populate_ta_genre_aggregate(ta_row_id_list, database):
    """
    Builds a genre aggregate - the genre table has a tree like structure with genres and sub genres. Additionally
    :param ta_row_id_list:
    :param database:
    :return:
    """
    if database is None:
        database = Database()
    ta_row_id_list = deepcopy(ta_row_id_list)
    # Should be a list of the ides in the title table
    # - Produces a string representation of every genre tree that the title is linked to (ordered by priority)
    # - Then updates the given title_ids in the title aggregate table with that information
    for title_row_id in ta_row_id_list:

        genre_title_link_dicts = database.search_table_row_dict(
            table="genre_title_links",
            column="genre_title_link_title_id",
            search_term=title_row_id,
        )
        genre_title_link_dicts = sorted(genre_title_link_dicts, key=lambda x: x["genre_title_link_priority"])

        genre_ids = []
        for link_dict in genre_title_link_dicts:
            genre_ids.append(link_dict["genre_title_link_genre_id"])

        genre_reps = []
        for genre_id in genre_ids:
            genre_row_dict = database.get_row_from_id(table="genres", row_id=genre_id, row_dict_return=True)
            genre_linear_column_list = database.get_linear_index_of_columns(
                start_row=genre_row_dict, display_column="genre"
            )
            genre_linear_str = ":".join(genre_linear_column_list)
            genre_reps.append(genre_linear_str)

        full_genre_str = " & ".join(genre_reps)

        if full_genre_str:
            ta_update_dict = {
                "ta_title_id": title_row_id,
                "ta_genre_aggregate": full_genre_str,
            }
            database.update_row_dict(ta_update_dict)


def populate_ta_identifiers_aggregate(ta_row_id_list, database):
    """
    Builds a genre aggregate - the genre table has a tree like structure with genres and sub genres. Additionally
    :param ta_row_id_list:
    :param database:
    :return:
    """
    if database is None:
        database = Database()
    ta_row_id_list = deepcopy(ta_row_id_list)

    # - Searchs the identifier_title link table for any instance of the given title_id
    # - filters the results and converts them into a dictionary
    # - writes the dictionary back out to the database
    for title_id in ta_row_id_list:

        identifier_ids = set()
        id_title_links = database.search_table_row_dict(
            table="identifier_title_links",
            column="identifier_title_link_title_id",
            search_term=title_id,
        )
        for link_row in id_title_links:
            identifier_ids.add(link_row["identifier_title_link_identifier_id"])

        title_ids_dict = dict()
        for identifier_id in identifier_ids:
            identifier_row_dict = database.get_row_from_id(
                table="identifiers", row_id=identifier_id, row_dict_return=True
            )
            identifier_type = deepcopy(identifier_row_dict["identifier_type"])
            identifier_type = identifier_type.upper().strip()
            if identifier_type not in title_ids_dict:
                title_ids_dict[identifier_type] = set()
                title_ids_dict[identifier_type].add(identifier_row_dict["identifier"])
            else:
                title_ids_dict[identifier_type].add(identifier_row_dict["identifier"])

        if title_ids_dict:
            ta_update_dict = {
                "ta_title_id": title_id,
                "ta_identifiers": title_ids_dict,
            }
            database.update_row_dict(ta_update_dict)


def populate_ta_publishers_aggregate(ta_row_id_list, database):
    """
    Builds a genre aggregate - the genre table has a tree like structure with genres and sub genres. Additionally
    :param ta_row_id_list:
    :param database:
    :return:
    """
    if database is None:
        database = Database()
    ta_row_id_list = deepcopy(ta_row_id_list)
    # Should be a list of the ides in the title table
    # - Produces a string representation of every publisher tree that the title is linked to (ordered by priority)
    # - Then updates the given title_ids in the title aggregate table with that information
    for title_row_id in ta_row_id_list:

        publisher_title_link_dicts = database.search_table_row_dict(
            table="publisher_title_links",
            column="publisher_title_link_title_id",
            search_term=title_row_id,
        )
        publisher_title_link_dicts = sorted(
            publisher_title_link_dicts,
            key=lambda x: x["publisher_title_link_priority"],
        )

        publisher_ids = []
        for link_dict in publisher_title_link_dicts:
            publisher_ids.append(link_dict["publisher_title_link_publisher_id"])

        publisher_reps = []
        for publisher_id in publisher_ids:
            publisher_row_dict = database.get_row_from_id(table="publishers", row_id=publisher_id, row_dict_return=True)
            publisher_linear_column_list = database.get_linear_index_of_columns(
                start_row=publisher_row_dict, display_column="publisher"
            )
            publisher_linear_str = ":".join(publisher_linear_column_list)
            publisher_reps.append(publisher_linear_str)

        full_publisher_str = " & ".join(publisher_reps)

        if full_publisher_str:
            ta_update_dict = {
                "ta_title_id": title_row_id,
                "ta_publishers": full_publisher_str,
            }
            database.update_row_dict(ta_update_dict)


def ensure_creators_sort(creator_rows):
    """
    Make sure some sort of creator sort field is set for every row in the given creator_rows itterable.

    :param creator_rows:
    :return:
    """
    for row in creator_rows:
        if six_unicode(row["creator_sort"]).lower().strip() == "none":
            row["creator_sort"] = author_to_author_sort(row["creator"])
            row.sync()
    return creator_rows


def clean(db, table, item_ids=None):
    """
    Remove any unused entries from the database.

    If item_ids are provided only removes items which are in that set and unused.

    :param db: The database to preform the clean in
    :param table: The table to attempt to clean of unused ids
    :param item_ids: If provided restricts the clean to these ids
    :return:
    """
    if item_ids is not None:
        raise NotImplemented

    # If the table is a book or title then it isn't suitable for cleaning
    if table in ["books", "titles"]:
        return
    main_tables = deepcopy(db.main_tables)
    try:
        main_tables.remove(table)
    except KeyError:
        err_str = "table not in main tables - table: {}".format(table)
        raise KeyError(err_str)

    # For every item in the given table need to remove it if and only if it isn't in use (linked to) by another table
    for target_table in main_tables:
        link_table = db.driver_wrapper.get_link_table_name(table1=table, table2=target_table)
        if not link_table:
            continue

    raise NotImplementedError


def direct_merge(self, table, main_id, target_ids):
    """
    Merge all the given target_ids into the main_id
    :param table:
    :param main_id:
    :param target_ids:
    :return:
    """
    raise NotImplementedError


# Todo: Come back and code to deal with large database
def fix_duplicates(db, table, column, comparison="nocase"):
    """
    Remove all the entries which differ only according to the comparison.
    :param db: The database to fix
    :param table: The table in the database to fix
    :param column: The column in the table in the database to fix
    :param comparison: The comparison method used to identify and remove the duplicated
    :return:
    """
    dupe_dictionary = find_duplicates(db, table, column, comparison)

    # Get some basic information about the table that's being worked on
    intralinkable = db.driver_wrapper.check_for_intralink_table(table)
    interlink_tables = db.driver_wrapper.get_interlinked_tables(table)
    parent_column = db.driver_wrapper.get_parent_column(table)

    for match_set in dupe_dictionary.values():
        if len(match_set) == 1:
            continue

        # Select the smallest element in the match set - all other entries in the table will be directed to point to it
        o_match_set = deepcopy(match_set)
        match_set = sorted(match_set)
        target_id = match_set[0]
        match_set = match_set[1:]
        for match_id in match_set:
            for linked_table in interlink_tables:
                _do_one_table_link_update(db, table, linked_table, target_id, match_id)

        # If the table is intralinkable then update all the intralink rows - see the explanation above
        # _do_intralink_merge
        # Every row intralinked to both rows being merged needed to be found
        # target_row_primary - rows with the target row being primary
        # target_row_secondary - rows with the target row being secondary
        if intralinkable:

            intralink_primary_col = db.driver_wrapper.get_intralink_column(table, "primary_id")
            intralink_secondary_col = db.driver_wrapper.get_intralink_column(table, "secondary_id")

            target_row = db.get_row_from_id(table, target_id)

            target_row_primary = db.get_intralink_rows(row=target_row, primary=True, secondary=False)
            target_row_secondary = db.get_intralink_rows(row=target_row, primary=False, secondary=True)
            if not target_row_primary and not target_row_secondary:
                continue

            # Remove any links between the target row and the match set - relations between two rows stop mattering
            # when those two rows become one
            target_row_primary_ids = set([r[intralink_secondary_col] for r in target_row_primary])
            target_row_primary_ids = set([r_id for r_id in target_row_primary_ids if r_id not in o_match_set])
            target_row_secondary_ids = set([r[intralink_primary_col] for r in target_row_secondary])
            target_row_secondary_ids = set([r_id for r_id in target_row_secondary_ids if r_id not in o_match_set])
            target_row_intralinked_ids = target_row_primary_ids.union(target_row_secondary_ids)
            target_intralink_rows_dict = dict((r[intralink_primary_col], r) for r in target_row_secondary)
            target_intralink_rows_dict.update(dict((r[intralink_secondary_col], r) for r in target_row_primary))

            for match_id in match_set:

                match_row = db.get_row_from_id(table, match_id)

                match_row_primary = db.get_intralink_rows(row=match_row, primary=True, secondary=False)
                match_row_secondary = db.get_intralink_rows(row=match_row, primary=False, secondary=True)
                if not match_row_primary and not match_row_secondary:
                    continue

                match_row_primary_ids = set([r[intralink_secondary_col] for r in match_row_primary])
                match_row_primary_ids = set([r_id for r_id in match_row_primary_ids if r_id not in o_match_set])
                match_row_secondary_ids = set([r[intralink_primary_col] for r in match_row_secondary])
                match_row_secondary_ids = set([r_id for r_id in match_row_secondary_ids if r_id not in o_match_set])
                match_row_intralink_ids = match_row_primary_ids.union(match_row_secondary_ids)
                match_intralink_rows_dict = dict((r[intralink_primary_col], r) for r in match_row_secondary)
                match_intralink_rows_dict.update((r[intralink_secondary_col], r) for r in match_row_primary)

                # Deal with the rows which are intralinked to the match row, but not the title row
                for row_id in deepcopy(match_row_intralink_ids - target_row_intralinked_ids):
                    match_row_intralink_ids.remove(row_id)
                    intralink_row = match_intralink_rows_dict[row_id]
                    repoint_intralink_row(
                        db,
                        table=table,
                        intralink_row=intralink_row,
                        old_id=match_id,
                        new_id=target_id,
                    )

                # Deal with rows which are intralinked to both the match row and the title row
                for row_id in deepcopy(match_row_intralink_ids.intersection(target_row_intralinked_ids)):
                    match_row_intralink_ids.remove(row_id)
                    match_intralink_row = match_intralink_rows_dict[row_id]
                    target_intralink_row = target_intralink_rows_dict[row_id]
                    _do_intralink_merge(db, table, target_intralink_row, match_intralink_row)

        # If the table has a tree like structure then go through and change all the child rows for each of the match_ids
        # to point at the new row
        if parent_column:
            for match_id in match_set:
                match_id_parent_rows = db.driver_wrapper.search(table=table, column=parent_column, search_term=match_id)
                for matched_row in match_id_parent_rows:
                    matched_row[parent_column] = target_id
                    db.driver_wrapper.update_row(matched_row)

        # Delete every row in the match set
        for match_id in match_set:
            match_row = db.get_row_from_id(table, match_id)
            db.delete(match_row)

    return True


def repoint_intralink_row(db, table, intralink_row, old_id, new_id):
    """
    Repoint an intralink row to reference a new_id instead of an old_id.
    :param db: The database we're working in
    :param intralink_row: The interlink row to update
    :param old_id: Any mentions of this id will be changed to the nw_id
    :param new_id:
    :return:
    """
    # Check to see which of the entries is the one being repointed - either the primary or the secondary must be
    # changed
    intralink_primary_col = db.driver_wrapper.get_intralink_column(table, "primary_id")
    if six_unicode(intralink_row[intralink_primary_col]) == six_unicode(old_id):
        intralink_row[intralink_primary_col] = new_id
        intralink_row.sync()
        return intralink_row

    intralink_secondary_col = db.driver_wrapper.get_intralink_column(table, "secondary_id")
    if six_unicode(intralink_row[intralink_secondary_col]) == six_unicode(old_id):
        intralink_row[intralink_secondary_col] = new_id
        intralink_row.sync()
        return intralink_row

    # Todo: Calling log_variables with a tuple with only one element produces unhelpful, bad outcomes
    info_str = "Neither the old_id or the new_id could be matched - consider sources of error"
    info_str = default_log.log_variables(
        info_str,
        "INFO",
        ("db", db),
        ("table", table),
        ("intralink_row", intralink_row),
        ("old_id", old_id),
        ("new_id", new_id),
        ("intralink_primary_col", intralink_primary_col),
        ("intralink_secondary_col", intralink_secondary_col),
    )
    raise InputIntegrityError(info_str)


# 1) All intralink rows that connect the two rows that are being merged should be deleted
# 2) Intralink rows which connect other titles to the two titles being merged should be considered
#    - Those that are only linked to the row being merged into the main row should be redirected to point to the main
#      row instead.
#    - Those that are linked to both - should be filtered into those with a differing kind of link and those with the
#      same kind of link. The differing kind should just be rediretced - after all two titles can be related to each
#      other in two different ways. The non-differing kind should be merged into each other.
def _do_intralink_merge(db, table, primary_intralink_row, secondary_intralink_row):
    """
    Two intralink rows will be merged - it is assumed that the two intralink rows both link to the same row in the table
    in the same way. Fpr example they could both be links indicating that the two title rows being merged are different
    from a third row.
    primary and secondary will remain unchanged from the primary_intralink_row.
    :param db: The database to work with
    :param table: The table we're working with
    :param primary_intralink_row: The secondary intralink row will be merged into this primary row
    :param secondary_intralink_row: This row will be merged into the primary
    :return:
    """
    column_headings = set([r for r in primary_intralink_row.keys()])

    primary_link_col = db.driver_wrapper.get_intralink_column(table, column_type="primary_id")
    column_headings.remove(primary_link_col)
    secondary_link_col = db.driver_wrapper.get_intralink_column(table, column_type="secondary_id")
    column_headings.remove(secondary_link_col)
    intralink_table_id_col = db.driver_wrapper.get_id_column(primary_intralink_row.table)
    column_headings.remove(intralink_table_id_col)

    # Retrieve the datestamp - convert to int and compare
    datestamp_column = db.driver_wrapper.get_datestamp_column(primary_intralink_row.table)
    try:
        primary_timestamp = int(primary_intralink_row[datestamp_column])
        secondary_timestamp = int(secondary_intralink_row[datestamp_column])
    except Exception as e:
        debug_str = "Trying to coerce the primary or the secondary row timestamp to int failed in _do_intralink_merge\n"
        default_log.log_exception(
            debug_str,
            e,
            "DEBUG",
            ("primary_row", primary_intralink_row),
            ("secondary_row", secondary_intralink_row),
        )
        primary_first = True
    else:
        if primary_timestamp < secondary_timestamp:
            primary_first = True
        else:
            primary_first = False
    column_headings.discard(datestamp_column)

    # Todo: Add triggers to fix the whole datestamp on update thing
    # Smart update the rows
    for column_heading in column_headings:
        if not primary_first and six_unicode(secondary_intralink_row[column_heading]).lower() != "none":
            primary_intralink_row[column_heading] = secondary_intralink_row[column_heading]
        elif six_unicode(primary_intralink_row[column_heading]).lower() == "none":
            primary_intralink_row[column_heading] = secondary_intralink_row[column_heading]

    # Data from the link rows has been merged - sync and return
    primary_intralink_row.sync()
    return primary_intralink_row


def _smart_merge_rows(db, primary_row, secondary_row):
    """
    Smart merge two rows using the following algorith,
    1) If the entry in the primary row is None, and the entry in the secondary is not, then use the entry in the
       secondary
    2) If both are non trivial then use the newest one, as determined by the datestamp
    :param db:
    :param primary_row:
    :param secondary_row:
    :return:
    """
    table = primary_row.table
    table_id_column = db.driver_wrapper.get_id_column(table)
    column_headings = set([r for r in primary_row.keys()])

    # Retrieving, converting and comparing the datestamp - this is in UNIXEPOCH
    datestamp_column = db.driver_wrapper.get_datestamp_column(table)
    try:
        primary_timestamp = int(primary_row[datestamp_column])
        secondary_timestamp = int(secondary_row[datestamp_column])
    except Exception as e:
        debug_str = "Trying to coerce the primary or the secondary row timestamp to int failed - in _smart_merge_rows\n"
        default_log.log_exception(
            debug_str,
            e,
            "DEBUG",
            ("primary_row", primary_row),
            ("secondary_row", secondary_row),
        )
        primary_first = True
    else:
        if primary_timestamp < secondary_timestamp:
            primary_first = True
        else:
            primary_first = False
    column_headings.discard(datestamp_column)
    column_headings.discard(db.driver_wrapper.get_id_column(table))

    # The id itself should remain untouched - so removing it from the column headings
    column_headings.discard(table_id_column)

    # Todo: Add triggers to fix the whole datestamp on update thing
    # Smart update the rows
    for column_heading in column_headings:
        if not primary_first and six_unicode(secondary_row[column_heading]).lower() != "none":
            primary_row[column_heading] = secondary_row[column_heading]
        elif six_unicode(primary_row[column_heading]).lower() == "none":
            primary_row[column_heading] = secondary_row[column_heading]

    # Data from the link rows has been merged - sync and return
    primary_row.sync()
    return primary_row


def _do_one_table_link_update(db, src_table, dst_table, src_table_id_1, src_table_id_2):
    """
    Update every link between the given src_table and the given dest table.
    The row with src_table_id_2 will end up merged with the row with src_table_id_1.
    :param src_table: The source table - the table containing the rows to merge
    :param dst_table: The dst_table - the table linked to the table containing the rows
    :param src_table_id_1: The primary id - the secondary id will end up being changed to the primary id
    :param src_table_id_2: The secondary_id - will be changed to the primary id.
    :return:
    """
    # 1) Retrieve all rows which reference either the primary or the secondary id
    # 2) For all the links which don't already exist in the primary links, just update the link
    # 3) For the links which exist with both the primary and the secondary id, smart merge the two - smart merge
    #    i) - If one is None, and the other is not, then use the one which is not None
    #    ii) - If both are not trivial then use the newest one, as determined by the datestamp
    id_1_row = db.get_row_from_id(table=src_table, row_id=src_table_id_1)
    id_2_row = db.get_row_from_id(table=src_table, row_id=src_table_id_2)
    id_1_link_rows = db.get_interlink_rows(primary_row=id_1_row, secondary_table=dst_table)
    id_2_link_rows = db.get_interlink_rows(primary_row=id_2_row, secondary_table=dst_table)

    # Check for the rows that don't already exist in the first set of links - these can be simple updated
    src_id_col = db.driver_wrapper.get_id_column(src_table)
    src_table_id_col = db.driver_wrapper.get_link_column(table1=src_table, table2=dst_table, column_type=src_id_col)
    dst_id_col = db.driver_wrapper.get_id_column(dst_table)
    dst_table_id_col = db.driver_wrapper.get_link_column(table1=src_table, table2=dst_table, column_type=dst_id_col)
    id_1_link_row_dst_table_ids = set([r[dst_table_id_col] for r in id_1_link_rows])
    id_2_link_row_dst_table_ids = set([r[dst_table_id_col] for r in id_2_link_rows])
    simple_update_ids = id_2_link_row_dst_table_ids - id_1_link_row_dst_table_ids
    for link_row in id_2_link_rows:
        if link_row[dst_table_id_col] in simple_update_ids:
            link_row[src_table_id_col] = src_table_id_1
            link_row.sync()

    # Check for rows that exist in the first and the second - these will need to be merged
    id_1_link_rows_dictionary = dict((r[dst_table_id_col], r) for r in id_1_link_rows)
    id_2_link_rows_dictionary = dict((r[dst_table_id_col], r) for r in id_2_link_rows)
    merge_update_ids = id_1_link_row_dst_table_ids.intersection(id_2_link_row_dst_table_ids)
    for merge_id in merge_update_ids:
        primary_row = id_1_link_rows_dictionary[merge_id]
        secondary_row = id_2_link_rows_dictionary[merge_id]
        _smart_merge_link_rows(db, src_table, dst_table, primary_row, secondary_row)


def _smart_merge_link_rows(db, src_table, dst_table, primary_row, secondary_row):
    """
    Smart merge two link rows using the following algorithm,
    1) If the entry in the primary row is None, and the entry in the secondary is not, then use the entry in the
       secondary
    2) If both are non trivial then use the newest one, as determined by the datestamp
    :param db: THe database we'retrying to update
    :param src_table: The source table
    :param dst_table: The destination table - SHOULD NOT BE THE SAME AS THE SRC_TABLE - USE MERGE INTRALINK ROWS INSTEAD-
    :param primary_row:
    :param secondary_row:
    :return:
    """
    column_headings = set([r for r in primary_row.keys()])
    table = primary_row.table

    src_id_col = db.driver_wrapper.get_link_column(
        table1=src_table,
        table2=dst_table,
        column_type=db.driver_wrapper.get_id_column(src_table),
    )
    dst_id_col = db.driver_wrapper.get_link_column(
        table1=src_table,
        table2=dst_table,
        column_type=db.driver_wrapper.get_id_column(dst_table),
    )

    # Retrieving, converting and comparing the datestamp - this is in UNIXEPOCH
    datestamp_column = db.driver_wrapper.get_link_column(table1=src_table, table2=dst_table, column_type="datestamp")
    try:
        primary_timestamp = int(primary_row[datestamp_column])
        secondary_timestamp = int(secondary_row[datestamp_column])
    except Exception as e:
        debug_str = (
            "Trying to coerce the primary or the secondary row timestamp to int failed -" " in _smart_merge_link_rows\n"
        )
        default_log.log_exception(
            debug_str,
            e,
            "DEBUG",
            ("primary_row", primary_row),
            ("secondary_row", secondary_row),
        )
        primary_first = True
    else:
        if primary_timestamp < secondary_timestamp:
            primary_first = True
        else:
            primary_first = False
    column_headings.discard(datestamp_column)
    column_headings.discard(db.driver_wrapper.get_id_column(table))

    # The actual link should remain untouched - so remove the two columns that constitute it from the column headings
    # before doing the smart update
    column_headings.discard(src_id_col)
    column_headings.discard(dst_id_col)

    # Todo: Add triggers to fix the whole datestamp on update thing
    # Smart update the rows
    for column_heading in column_headings:
        if not primary_first and six_unicode(secondary_row[column_heading]).lower() != "none":
            primary_row[column_heading] = secondary_row[column_heading]
        elif six_unicode(primary_row[column_heading]).lower() == "none":
            primary_row[column_heading] = secondary_row[column_heading]

    # Data from the link rows has been merged - sync and return
    primary_row.sync()
    return primary_row


def find_duplicates(db, table, column, comparison="nocase"):
    """
    Find duplicates in a given column in a given table in the given database using the given comparison method.
    If comparison is "nocase" defaults to icu_lower.
    :param db: The database to search in
    :param table: The table in the database to search
    :param column: The column in the table in the database
    :param comparison: The comparison method used to match the values - can be a callable which accepts a single string
                       and returns a single string
    :return dupe_dict: Keyed with the hashed value and valued with a set of the ids in the table which mapped to that
                      value.
    """
    dupe_dict = defaultdict(set)

    if comparison == "nocase" or not comparison:
        from LiuXin.utils.icu import lower as icu_lower

        comparison_func = icu_lower
    else:
        comparison_func = comparison

    for table_row in db.driver_wrapper.get_all_rows(table=table):
        table_id_column = db.driver_wrapper.get_id_column(table=table)
        row_id = table_row[table_id_column]
        row_value = table_row[column]
        hashed_row_value = comparison_func(row_value)
        dupe_dict[hashed_row_value].add(int(row_id))

    return dupe_dict


# ----------------------------------------------------------------------------------------------------------------------
#
# - STARTUP TASKS START HERE


def do_pre_view_startup_tasks(db, custom_columns=None):
    """
    Takes a database - preforms the startup tasks on it.
    These are all the tasks that have to be completed before creating the meta2 view
    :param db: Currently assumes it's SQL compatible
    :param custom_columns: A LiuXin.library.custom_columns object which represents the custom columns on this database
                           If not provided then no custom columns will be included in the meta2 view (which is the basis
                           for the main view and searchable parameters of the library).
    :return:
    """
    create_creator_insert_update_trigger(db)
    direct_ensure_creators_sort(db)
    direct_create_tag_browser_news(db)


def do_view_startup_tasks(db, view_metadata):
    """

    :param db:
    :param custom_columns:
    :param update_field_metadata:
    :param view_metadata:
    :return:
    """
    pass


def create_creator_insert_update_trigger(db):
    """
    Creates an author insert trigger on the database - this ensures that the creator_sort field in the creator table is
    set to be something after an insert on the creator's table (if a value is already set, then the trigger should
    ignore it).
    Likewise after an update checks to see if the sort field has been nullified - if it has replaces it with the auto
    generated field.
    :param db:
    :return:
    """
    # The second trigger works as follows - when a change is made to the creator (creator name) the creator_sort field
    # is set to that value BEFORE the update takes effect. If the new row specifies what the creator_sort should be,
    # that value is used. If not the creator_sort is left unchanged, set to author_to_author_sort(NEW.creator)
    # NOTE: If you update the database with a row dict with creator_sort set to None, this will currently override this
    # and set the creator_sort to be None. Instead del row_dict["creator_sort"]
    # These used to be temp triggers - but are needed on all database access - so just checking and refreshing them
    sql = """
    DROP TRIGGER IF EXISTS creator_insert_trg;

        CREATE TRIGGER creator_insert_trg
            AFTER INSERT ON creators
            BEGIN
            UPDATE creators SET creator_sort=author_to_author_sort(NEW.creator) WHERE creator_id=NEW.creator_id;
        END;

    DROP TRIGGER IF EXISTS creator_update_trg;
        CREATE TRIGGER creator_update_trg
            BEFORE UPDATE ON creators
            BEGIN
            UPDATE creators SET creator_sort=author_to_author_sort(NEW.creator)
            WHERE creator_id=NEW.creator_id AND creator <> NEW.creator;
        END;
    """
    db.driver.direct_executescript(sql)


def direct_ensure_creators_sort(db):
    """
    Makes sure that every row in the creators table has some sort of creator_sort set.
    :param db:
    :return:
    """
    sql = "UPDATE creators SET creator_sort = author_to_author_sort(creator) WHERE creator_sort IS NULL;"
    db.driver.direct_execute(sql)


def direct_set_original_one_row_creator_sort(db, creator_row_id):
    """
    Update the creator sort of a particular creator row to set it to the raw value generator from author_to_author_sort.
    :param db:
    :param creator_row_id:
    :return:
    """
    sql = "UPDATE creators SET creator_sort = author_to_author_sort(creator) WHERE creator_id = ?;"
    db.driver.direct_execute(sql, creator_row_id)


def direct_create_tag_browser_news(db):
    """
    Creates the tag_browser_news view - which is used for viewing books which have been tagged as news.
    :param db:
    :return:
    """
    sql = """
    CREATE VIEW IF NOT EXISTS tag_browser_news AS SELECT DISTINCT
        tag_id as id,
        tag as name,
        (SELECT COUNT(tag_title_links.tag_title_link_title_id) FROM tag_title_links
        WHERE tag_title_link_tag_id=x.tag_id) count,
        (0) as avg_rating,
        tag as sort
        FROM tags as x WHERE tag != "{0}" AND tag_id IN
        (SELECT DISTINCT tag_title_link_tag_id FROM tag_title_links WHERE tag_title_link_title_id IN
            (SELECT DISTINCT tag_title_link_title_id FROM tag_title_links WHERE tag_title_link_tag_id IN
                (SELECT tag_id FROM tags WHERE tag = "{0}")));""".format(
        _("News")
    )
    db.driver.direct_execute(sql)


# Todo: This should be a macro
# Todo: Add the concept of a file path to the folder store logic
def direct_create_meta_2_view(db, custom_columns=None, update_field_metadata=False):
    """
    Creates the meta_2 view - which is used to drive the primary books table from calibre.
    :param db:
    :param custom_columns: A LiuXin.library.custom_columns object to represent the custom columns on the database.
    :param update_field_metadata: Should the field_metadata object in the custom columns be updated as well?
    :return:
    """
    template = """\
                (SELECT {query} FROM books_{table}_link AS link INNER JOIN
                    {table} ON(link.{link_col}={table}.id) WHERE link.book=books.id)
                    {col}
                """
    # The spacing before some fields is purely for readability and has no effect on function.
    columns = [
        "               book_id AS id",
        """
               (SELECT title as title
                FROM titles
                WHERE titles.title_id = books.book_id)title""",
        """
               (SELECT GROUP_CONCAT(creator) AS author
                FROM creators
                INNER JOIN creator_title_links
                ON creators.creator_id = creator_title_links.creator_title_link_creator_id
                WHERE creator_title_links.creator_title_link_title_id = books.book_id
                GROUP BY creator_title_links.creator_title_link_title_id
                AND creator_title_links.creator_title_link_type = 'author')authors""",
        "\n               book_datestamp AS timestamp",
        """
               (SELECT SUM(file_size) AS uncompressed_size
                FROM files
                INNER JOIN file_folder_links
                ON files.file_id = file_folder_links.file_folder_link_file_id
                INNER JOIN book_folder_links
                ON file_folder_links.file_folder_link_folder_id = book_folder_links.book_folder_link_folder_id
                WHERE book_folder_links.book_folder_link_book_id = books.book_id
                GROUP BY book_folder_links.book_folder_link_book_id) size""",
        """
               (SELECT MAX(rating) AS rating
                FROM ratings
                INNER JOIN rating_title_links
                ON ratings.rating_id = rating_title_links.rating_title_link_rating_id
                WHERE rating_title_links.rating_title_link_title_id = books.book_id
                GROUP BY rating_title_links.rating_title_link_title_id)rating
               """,
        """
              (SELECT main_tags FROM meta WHERE id=books.book_id
               UNION ALL
               SELECT creator_tags FROM meta WHERE id=books.book_id
               UNION ALL
               SELECT series_tags FROM meta WHERE id=books.book_id) tags
               """,
        """
              (SELECT GROUP_CONCAT(synopsis) AS text
               FROM synopses
               INNER JOIN synopsis_title_links
               ON synopses.synopsis_id = synopsis_title_links.synopsis_title_link_synopsis_id
               WHERE synopsis_title_links.synopsis_title_link_title_id = books.book_id
               GROUP BY synopsis_title_links.synopsis_title_link_title_id) comments
               """,
        """
              (SELECT series AS series_name
               FROM meta
               WHERE meta.id = books.book_id) series
               """,
        """
              (SELECT GROUP_CONCAT(publisher) AS publisher
               FROM publishers
               INNER JOIN publisher_title_links
               ON publishers.publisher_id = publisher_title_links.publisher_title_link_publisher_id
               WHERE publisher_title_links.publisher_title_link_title_id = books.book_id
               GROUP BY publisher_title_links.publisher_title_link_title_id
               ORDER BY publisher_title_links.publisher_title_link_priority) publisher
               """,
        """
              (SELECT series_index AS series_index
               FROM meta
               WHERE meta.id = books.book_id) series_index
               """,
        """
              (SELECT sort AS sort
               FROM meta
               WHERE meta.id = books.book_id) sort
               """,
        """
              (SELECT author_sort AS author_sort
               FROM meta
               WHERE meta.id = books.book_id) author_sort
               """,
        """
              (SELECT GROUP_CONCAT(file_extension) AS file_formats
               FROM files
               INNER JOIN file_folder_links
               ON files.file_id = file_folder_links.file_folder_link_file_id
               INNER JOIN book_folder_links
               ON file_folder_links.file_folder_link_folder_id = book_folder_links.book_folder_link_folder_id
               WHERE book_folder_links.book_folder_link_book_id = books.book_id
               GROUP BY book_folder_links.book_folder_link_book_id) formats
               """,
        """
              (SELECT GROUP_CONCAT(file_path) AS file_path
               FROM files
               INNER JOIN file_folder_links
               ON files.file_id = file_folder_links.file_folder_link_file_id
               INNER JOIN book_folder_links
               ON file_folder_links.file_folder_link_folder_id = book_folder_links.book_folder_link_folder_id
               WHERE book_folder_links.book_folder_link_book_id = books.book_id
               GROUP BY book_folder_links.book_folder_link_book_id) formats
               """,
        "\n               book_pubdate AS pubdate",
        "\n               book_uuid AS uuid",
        "\n               book_has_cover AS has_cover",
        """
              (SELECT aum_sortconcat(link.creator_title_link_id, creators.creator, creators.creator_sort,
               link.creator_title_link_type)
               FROM creator_title_links AS link
               INNER JOIN creators
               ON (link.creator_title_link_creator_id = creators.creator_id)
               WHERE link.creator_title_link_title_id = books.book_id) au_map
               """,
        "\n               book_last_modified AS last_modified",
        """
              (SELECT identifiers_concat(identifier_type, identifier) FROM identifier_title_links AS link
               INNER JOIN identifiers
               ON (link.identifier_title_link_identifier_id = identifiers.identifier_id)
               WHERE link.identifier_title_link_title_id = books.book_id) identifiers
               """,
        """
              (SELECT sortconcat(link.language_title_link_id, languages.language_code)
               FROM language_title_links AS link
               INNER JOIN languages
               ON (link.language_title_link_language_id = languages.language_id)
               WHERE link.language_title_link_title_id = books.book_id) languages
               """,
    ]

    lines = []
    for col in columns:
        line = col
        if isinstance(col, tuple):
            line = template.format(col=col[0], table=col[1], link_col=col[2], query=col[3])
        lines.append(line)

    if custom_columns is not None:
        # the map is labelled with numbers - custom col labels are numbers (the id in the custom_columns table) and
        # valued with a list of the lines needed to bring information for that custom column into meta2
        custom_map = custom_columns.custom_columns_in_meta()

        # As custom columns are deleted the sequence of ids may develop holes
        custom_cols = list(sorted(custom_map.keys()))

        # Extending the list of lines with the lines that form the view for each of the custom columns
        lines.extend([custom_map[x] for x in custom_cols])
    else:
        custom_map = None

    try:
        script = """
            DROP VIEW IF EXISTS meta2;
            CREATE VIEW meta2 AS
            SELECT
            {0}
            FROM books;
            """.format(
            ", \n".join(lines)
        )
    except TypeError:
        err_str = "Error while trying to construct the SQL Script using a format\n"
        err_str = default_log.log_variables(err_str, "ERROR", ("lines", pprint.pformat(lines)))
        raise TypeError(err_str)

    default_log.info(script)
    db.driver.direct_executescript(script)

    if custom_columns is not None:
        custom_columns.update_field_map_from_custom_columns_in_meta(
            lines=custom_map, update_field_metadata=update_field_metadata
        )

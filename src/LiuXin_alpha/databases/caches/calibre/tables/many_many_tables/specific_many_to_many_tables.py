"""
ManyToMany tables are items which are linked to many other items and visa versa.

E.g. "tags" and "titles" - many "tags" can be assigned to a "title" and visa versa.
"""

import re
from collections import defaultdict, OrderedDict

from typing import TypeVar, Optional, Union, Iterable, Literal

from LiuXin.customize.cache.base_tables import (
    BaseCreatorsTable,
    BaseFormatsTable,
)
from LiuXin.databases.caches.calibre.tables.many_many_tables.many_to_many_table import CalibreManyToManyTable
from LiuXin.databases.caches.calibre.tables.many_many_tables.priority_many_to_many_table import (
    CalibrePriorityManyToManyTable,
)
from LiuXin.databases.db_types import (
    MetadataDict,
    InterLinkTableName,
    SrcTableID,
    DstTableID,
    SpecificFormat,
    GenericFormat,
)

from LiuXin.exceptions import (
    DatabaseIntegrityError,
)

from LiuXin.metadata import author_to_author_sort

from LiuXin.preferences import preferences

from LiuXin.utils.lx_libraries.liuxin_six import (
    dict_iteritems as iteritems,
    dict_itervalues as itervalues,
)
from LiuXin.utils.logger import default_log

from past.builtins import basestring

T = TypeVar("T")


class CalibreAuthorsTable(CalibrePriorityManyToManyTable[T], BaseCreatorsTable):
    """
    Represents the authors' subset of the creators table, and the link between that and books.
    """

    _priority: bool = True
    _typed: bool = False

    def __init__(self, name: str, metadata: MetadataDict, link_table: Optional[InterLinkTableName] = None) -> None:
        """
        Initialize the authors table.


        :param name:
        :param metadata:
        :param link_table:
        """

        CalibrePriorityManyToManyTable.__init__(self, name, metadata, link_table)

        self.name = "creators"
        # Todo: Check that this is set when subtables are created
        self.table_type_filter = "authors"

        # Set the link table for the authors - which is a subset for the creators table
        self.link_table = "creator_title_links"
        self.link_table_bt_id_column = "creator_title_link_title_id"
        self.link_table_table_id_column = "creator_title_link_creator_id"
        self.link_table_type = "creator_title_link_type"

        self.alink_map: dict[DstTableID, str] = {}
        self.asort_map: dict[DstTableID, str] = {}

    def update_preflight(
        self,
        book_id_item_id_map: dict[SrcTableID, Union[DstTableID, list[DstTableID, ...], tuple[DstTableID, ...], str]],
        id_map_update: dict[DstTableID, str],
        dirtied: Optional[set[SrcTableID]] = None,
    ) -> tuple[dict[SrcTableID, Iterable[DstTableID]], dict[DstTableID, str]]:
        """
        Transforms :param book_id_item_id_map: into a form which can be written out to the database.

        :param book_id_item_id_map:
        :param id_map_update:
        :param dirtied: The books which are affected by this update
        :return:
        """
        dirtied = set() if dirtied is None else dirtied

        # Standardize the book_id_item_id_map
        clean_book_id_item_id_map = dict()
        for book_id, book_vals in iteritems(book_id_item_id_map):

            if isinstance(book_vals, int):
                clean_book_id_item_id_map[book_id] = [
                    book_vals,
                ] + list(self.book_col_map[book_id])

            elif isinstance(book_vals, (list, tuple)):
                clean_book_id_item_id_map[book_id] = book_vals

            elif isinstance(book_vals, basestring):

                # If the value for the book_id_item_id_map is a string, then check the id_map_update for it
                # Then look up the id and treat it as if we've been given an id
                if book_vals in id_map_update.values():
                    book_val_id = None
                    for item_id, str_val in iteritems(id_map_update):
                        if str_val == book_vals:
                            book_val_id = item_id
                            break
                    assert book_val_id is not None
                    clean_book_id_item_id_map[book_id] = [
                        book_val_id,
                    ] + list(self.book_col_map[book_id])
                else:
                    clean_book_id_item_id_map[book_id] = [
                        book_vals,
                    ] + list(self.book_col_map[book_id])

            else:
                err_msg = [
                    "Unexpected form of the value of the update preflight",
                    "book_id_item_id_map: {}".format(book_id_item_id_map),
                    "id_map_update: {}".format(id_map_update),
                    "book_id: {}".format(book_id),
                    "book_vals: {}".format(book_vals),
                ]
                raise NotImplementedError("\n".join(err_msg))

        return clean_book_id_item_id_map, id_map_update

    def read_id_maps(self, db) -> None:
        """
        Load the data off the database.

        Also populates the alink and asort maps at the same time.
        :param db:
        :return:
        """
        self.alink_map = link_map = {}
        self.asort_map = sort_map = {}
        self.id_map = im = {}
        us = self.unserialize

        for aid, name, sort, link in db.macros.read_creator_with_sort_and_link():
            name = us(name)
            im[aid] = name
            sort_map[aid] = sort or author_to_author_sort(name)
            link_map[aid] = link

    def read_maps(self, db, type_filter: str = "authors") -> None:
        """
        Read the maps, filtering to remove any creator who doesn't have the role of author in a given work.

        Specialized to actually read off the creators table rather than the authors table - which does not exist in the
        new schema.
        :param db: The database to read off
        :param type_filter: The filter to use - "authors" by default, but this could be used to view other types of
                            creator just as well.
        :return:
        """
        book_col_map = defaultdict(list)
        col_book_map = self.col_book_map

        # Get the id column for the target table
        idcol = db.driver_wrapper.get_id_column("creators")

        # Get the name of the link table and the relevant columns
        link_table_name = db.driver_wrapper.get_link_table_name(table1="titles", table2="creators")
        link_table_book = db.driver_wrapper.get_link_column(table1="titles", table2="creators", column_type="title_id")
        link_table_other = db.driver_wrapper.get_link_column(table1="titles", table2="creators", column_type=idcol)
        link_table_priority = db.driver_wrapper.get_link_column(
            table1="titles", table2="creators", column_type="priority"
        )

        if type_filter is None:
            stmt = self.selectq_desc.format(link_table_book, link_table_other, link_table_name, link_table_priority)
        else:
            link_table_type = db.driver_wrapper.get_link_column(table1="titles", table2="creators", column_type="type")
            stmt = self.selectq_filter_desc.format(
                link_table_book,
                link_table_other,
                link_table_name,
                link_table_priority,
                link_table_type,
                type_filter,
            )

        # Preform the search and cache the results
        for book_id, item_id in db.driver_wrapper.execute(stmt):
            col_book_map[item_id].append(book_id)
            book_col_map[book_id].append(item_id)

        # Convert everything into a tuple
        self.book_col_map = {k: tuple(v) for k, v in iteritems(book_col_map)}

        # Read the ids from the books table - so that we know what ids are valid for the table
        title_query = db.driver_wrapper.execute("SELECT title_id FROM titles")
        self.seen_book_ids = set([tr[0] for tr in title_query])

    def set_sort_names(self, aus_map: dict[DstTableID, str], db) -> dict[DstTableID, str]:
        """
        Update the database with the given author_sort map

        :param aus_map: An author_sort map
        :param db:
        :return aus_map: A processed author sort map - as it will actually be written into the database
        """
        # Preprocess before writing out to the database
        aus_map = {aid: (a or "").strip() for aid, a in iteritems(aus_map)}
        aus_map = {aid: a for aid, a in iteritems(aus_map) if a != self.asort_map.get(aid, None)}

        # Update the cache
        self.asort_map.update(aus_map)

        # Write the changes out to the database
        db.macros.update_creator_sorts([(v, k) for k, v in iteritems(aus_map)])

        return aus_map

    def set_links(self, link_map: dict[DstTableID, str], db) -> dict[DstTableID, str]:
        """
        NOTE: THIS DOES NOT UPDATE THE LINKS BETWEEN CREATOR AND BOOKS, DESPITE THE CONFUSING NAME.

        This uses the link_map (keyed with the creator_id, valued with the value that the creator_link will have) to
        update the creators table with new links.
        :param link_map:
        :param db:
        :return link_map: With the standard transforms done on the values
                          The link map as it will actually appear on the database.
        """
        link_map = {author_id: (l or "").strip() for author_id, l in iteritems(link_map)}
        link_map = {aid: l for aid, l in iteritems(link_map) if l != self.alink_map.get(aid, None)}
        self.alink_map.update(link_map)

        db.macros.update_creator_links([(v, k) for k, v in iteritems(link_map)])
        return link_map

    def remove_books(self, book_ids: Iterable[SrcTableID], db) -> set[DstTableID]:
        """
        Remove books from this cache.

        :param book_ids:
        :param db:
        :return clean:
        """
        clean = CalibreManyToManyTable.remove_books(self, book_ids, db)

        for item_id in clean:
            self.alink_map.pop(item_id, None)
            self.asort_map.pop(item_id, None)

        return clean

    def rename_item(self, item_id: DstTableID, new_name: str, db) -> tuple[set[SrcTableID], DstTableID]:
        """
        Rename items in the authors table.

        :param item_id:
        :param new_name:
        :param db:
        :return ret:
        """
        ret = CalibreManyToManyTable.rename_item(self, item_id, new_name, db)

        # If something has gone wrong - renaming a thing which no longer exists? Scrub the entry and continue
        if item_id not in self.id_map:
            self.alink_map.pop(item_id, None)
            self.asort_map.pop(item_id, None)
        else:
            # Was a simple rename, update the author sort value
            self.set_sort_names({item_id: author_to_author_sort(new_name)}, db)

        return ret

    def remove_items(
        self, item_ids: Iterable[DstTableID], db, restrict_to_book_ids: Iterable[SrcTableID] = None
    ) -> None:
        """
        Remove items by ids from the cache and the database.

        :param item_ids:
        :param db:
        :param restrict_to_book_ids:
        :return:
        """
        raise ValueError("Direct removal of authors is not allowed")


class CalibreFormatsTable(CalibreManyToManyTable, BaseFormatsTable):
    """
    Contains informaiton about the formats contained in the database table.

    The Formats table contains the following maps.
    It's a ManyMany table as many book can have files of a particular format - this table can easily answer the question
    "How many books have an epub file?".
    It can also answer "Where are all the epub files associated with this book?"
    It also has aspects of a ManyOne table - as it also provides locations for the format files.
    fname_map - A dictionary of dictionaries keyed with the book_id, then keyed with the format and finally valued
                with the name of the file.
    book_file_map - A dictionary of dictionaries keyed with the book_id, then keyed with the format and finally valued
                    with the id of the file
    size_map - A dictionary of dictionaries keyed with the book_id, then keyed with the format and finally valued
               with the size of that format.
    col_book_map - A dictionary keyed with the format and valued with a set of book_ids with that format
    book_col_map - A dictionary keyed with the book_id and valued with a tuple of the formats that book has
    book_col_count_map - A dictionary of dictionaries - keyed with the book_id, then keyed with the format and
                         valued with the count
    book_file_loc_map - A dictionary of dictionaries keyed with the book_id, then keyed with the format and finally
                        valued with the location of that file.
    book_fmts_map - Keyed with the id of the book and valued with a set of all the available fornat types (e.g a set
                    of EPUB, MOBI e.t.c - not a set of the fmt_priorities (which contains information as to the format
                    and the priority of that format in the book - e.g. a string of the form EPUB_1)
    If adding more maps, remember to also add them to the remove_books function.
    """

    _priority: bool = True
    _typed: bool = False

    def __init__(self, name: str, metadata: MetadataDict, link_table: InterLinkTableName = None) -> None:
        """
        Initialize the formats table.

        Data is not read into the database at this point.
        :param name:
        :param metadata:
        :param link_table:
        """

        CalibreManyToManyTable.__init__(self, name, metadata, link_table)

        self.fname_map = defaultdict(dict)
        self.size_map = defaultdict(dict)
        self.col_book_map = None
        self.book_col_count_map = None
        self.book_file_map = None
        self.book_file_loc_map = None
        self.book_fmts_map = None

        self.size_mode: Literal["sum", "max", "min"] = "max"
        self.__parse_size_mode()

        # Needed in order to calculate the location of each of the files
        self.fsm = None

    def __parse_size_mode(self) -> None:
        """
        Parse the preferences to determine how the size of the book should be calculated

        :return None: Changes are made internally.
        """
        pref_size_mode = preferences["book_size_display_mode"]
        if pref_size_mode.lower() not in ["sum", "max", "min"]:
            wrn_str = "Unable to parse preferences:book_size while creating the size table.\n"
            wrn_str += "preferences:book_size - {}".format(pref_size_mode)
            wrn_str += "defaulting to max.\n"
            default_log.warn(wrn_str)
            self.size_mode = "sum"
        else:
            self.size_mode = pref_size_mode.lower()

    def read_id_maps(self, db) -> None:
        """
        Id maps are not needed in this case.

        :param db:
        :return:
        """
        pass

    def fix_case_duplicates(self, db) -> None:
        """
        A nonsensical thing to do in this context.

        :param db:
        :return:
        """
        pass

    # Todo: Deal with unattatched folder stores
    def read_maps(self, db, type_filter: Optional[str] = None) -> None:
        """
        Create the format maps. These are described in detail in the class docstring

        :param db:
        :param type_filter:
        :return:
        """
        assert self.fsm is not None, "Cannot load - fsm is None"

        self.fname_map = fnm = defaultdict(OrderedDict)
        self.size_map = sm = defaultdict(OrderedDict)
        self.col_book_map = col_book_map = defaultdict(set)
        book_col_map = defaultdict(list)
        book_col_count_map = defaultdict(dict)
        book_file_map = defaultdict(OrderedDict)
        book_file_loc_map = defaultdict(dict)
        book_fmts_map = defaultdict(set)

        seen_books = self.seen_books

        for (
            book_id,
            file_id,
            fmt,
            file_name,
            file_size,
        ) in db.macros.read_book_id_with_file_id_file_ext_file_name_and_file_size():

            seen_books.add(book_id)

            if fmt is None:
                continue

            # Record the general stats for each file type
            fmt = fmt.upper()
            if fmt.startswith("."):
                fmt = fmt[1:]

            # Record that format in the book_fmts_map
            book_fmts_map[book_id].add(fmt)

            # Note that this book has a specific fmt in the col_book_map
            col_book_map[fmt].add(book_id)

            # Generate the priority fmt - use it to record the specific stats for this file
            if fmt in book_col_count_map[book_id].keys():
                book_col_count_map[book_id][fmt] += 1
                fmt_name = fmt + "_{}".format(book_col_count_map[book_id][fmt])
            else:
                book_col_count_map[book_id][fmt] = 1
                fmt_name = fmt + "_1"

            book_col_map[book_id].append(fmt_name)
            fnm[book_id][fmt_name] = file_name
            sm[book_id][fmt_name] = file_size
            book_file_map[book_id][fmt_name] = file_id

            # Record the file location - if it can be found
            file_row = db.get_row_from_id("files", file_id)
            file_loc = self.fsm.get_loc(asset_row=file_row)

            book_file_loc_map[book_id][fmt_name] = file_loc

        self.col_book_map = col_book_map
        self.book_col_count_map = book_col_count_map
        # Freeze the values for the book_col_map as a tuple to make them harder to accidentally modify
        self.book_col_map = {k: tuple(sorted(v)) for k, v in iteritems(book_col_map)}
        self.book_file_map = book_file_map
        self.book_file_loc_map = book_file_loc_map
        self.book_fmts_map = book_fmts_map

    # Todo: Should also mark the associated books for removal from the folder stores
    def remove_books(self, book_ids: Iterable[SrcTableID], db) -> set[DstTableID]:
        """
        Remove the specified books from the cache.

        :param book_ids:
        :param db:
        :return clean: The format file ids to be removed
        """
        clean = CalibreManyToManyTable.remove_books(self, book_ids, db)

        for book_id in book_ids:
            self.fname_map.pop(book_id, None)
            self.book_file_map.pop(book_id, None)
            self.size_map.pop(book_id, None)

            for fmt_set in itervalues(self.col_book_map):
                fmt_set.discard(book_id)

            self.book_col_map.pop(book_id, None)
            self.book_col_count_map.pop(book_id, None)
            self.book_file_loc_map.pop(book_id, None)
            self.book_fmts_map.pop(book_id, None)

        return clean

    def set_fname(self, book_id: SrcTableID, fmt: SpecificFormat, fname: str, db) -> None:
        """
        Changes the file_name for the given format of the given file.

        A note on formats.
        calibre assumes that each book will have at most one book of each format. Thus formats are things like EPUB,
        MOBI e.t.c - thus the fmts for each book look something like EPUB_1, MOBI_1 e.t.c.
        So calling this method with something like EPUB will fail with an AttributeError
        :param book_id:
        :param fmt:
        :param fname:
        :param db:
        :return:
        """
        # Change the filename in the cache
        self.fname_map[book_id][fmt] = fname

        # Lookup the file_id for the file which corresponds to that format in that book
        file_id = self.book_file_map[book_id][fmt]
        db.macros.set_file_name(file_id, fname)

        # Todo: Notify the maintainer that the file name has changed - so it can change the file on disk

    def remove_formats(
        self, formats_map: dict[DstTableID, Iterable[Union[SpecificFormat, GenericFormat]]], db
    ) -> Union[int, dict[DstTableID, int]]:
        """
        Takes a format map - keyed with the book_id and valued with the formats to remove.

        Removes those formats first from the cache and then from the database.
        The formats are expected to be LiuXin formats - thus things like EPUB_2.
        format priorities will be updated to take account of the removal of the old formats.
        :param formats_map:
        :param db:
        :return:
        """
        # Don't even bother trying to modify the cache in place - just remove the specified files and then reload from
        # the db
        # Build a set of file ids to be cleaned from the database
        clean = set()
        for book_id, fmts in iteritems(formats_map):

            for fmt in fmts:
                clean.add(self.book_file_map[book_id].pop(fmt, None))

        # Filter None out, if it's been introduced to the clean set
        clean.discard(None)

        # Todo: Need to mark these for deletion by the folder store manager
        # Discard the unused files from the link table - then discard the unused files from the files table
        # Should overcome any problems with the foreign key constraint
        db.macros.delete_files_by_id([file_id for file_id in clean])

        for book_id, fmts in iteritems(formats_map):
            self.reload_book_from_db(db=db, book_id=book_id)

        # Todo: This probably won't do what it's intended to do at the moment - fix later
        def zero_max(book_id):
            try:
                return max(itervalues(self.size_map[book_id]))
            except ValueError:
                return 0

        # Todo: This should depend on the selected size mode
        # Return a map to be used to update the book size table
        return {book_id: zero_max(book_id) for book_id in formats_map}

    def remove_items(
        self, item_ids: Iterable[DstTableID], db, restrict_to_book_ids: Iterable[SrcTableID] = None
    ) -> bool:
        """
        Attempts to remove items from the cache by id.

        Not currently supported.
        You need to remove the format from the book directly instead.
        :param item_ids:
        :param db:
        :param restrict_to_book_ids:
        :return:
        """
        raise NotImplementedError("Cannot delete a format directly - must remove it from the book")

    def rename_item(self, item_id: DstTableID, new_name: str, db) -> bool:
        """
        Directly rename an item in the formats table.

        Will currently fail - the concept is nonsensical in
        :param item_id:
        :param new_name:
        :param db:
        :return status: Did the rename go through?
        """
        raise NotImplementedError("Meaningless to rename formats in the format table")

    def reload_book_from_db(self, db, book_id: DstTableID) -> None:
        """
        Reload information about a book from the db.

        The file store is the final arbitrator of what is true or not.
        But it can be useful to reload to check that the database hasn't mutated.
        :param db: The database to reload from
        :param book_id:
        :return:
        """
        book_id = int(book_id)

        # Todo: Enforce that a file and it's backup are always linked to the same book
        # Todo: Make sure that, when moving a book, any backups of it are moved as well

        # Reset those caches that won't just be overwritten later
        for base_fmt in self.col_book_map:
            self.col_book_map[base_fmt].discard(book_id)
        self.book_col_map[book_id] = list()
        self.fname_map[book_id] = OrderedDict()
        self.size_map[book_id] = OrderedDict()
        self.book_file_map[book_id] = OrderedDict()
        self.book_file_loc_map[book_id] = dict()
        self.book_fmts_map[book_id] = set()
        self.book_col_count_map[book_id] = dict()

        # Build the backup dictionary for the given book - this is keyed with the id of the file, and valued with the
        # id of that file that file backs up (this is because one file could have multiple backups - at some point in
        # the future).
        backup_dict = dict()
        for primary_file_id, secondary_file_id in db.macros.read_file_backups_for_book(book_id):
            backup_dict[int(secondary_file_id)] = int(primary_file_id)

        # Read all the data from the database first - will be processed into the caches in a second
        book_data_dict = OrderedDict()
        book_backup_data_dict = OrderedDict()

        # Expect each file id to turn up once and only once - so key off it
        for (
            file_id,
            fmt,
            file_name,
            file_size,
        ) in db.macros.read_file_properties_for_book(book_id):

            if fmt is None:
                continue

            # Standardize the fmt for later use
            fmt = fmt.upper()
            if fmt.startswith("."):
                fmt = fmt[1:]

            # Split the books data down into the original files and any backups that they might have
            if file_id not in backup_dict.keys():
                # File is NOT a backup of another file
                book_data_dict[file_id] = (fmt, file_name, file_size)
            else:
                book_backup_data_dict[file_id] = (fmt, file_name, file_size)

        # Process the files that ARE NOT backups
        specific_book_fmt_count_map = dict()
        file_id_priority_fmt_map = dict()
        for file_id in book_data_dict:

            file_fmt, file_name, file_size = book_data_dict[file_id]

            # Record the format in the book_fmts_map
            self.book_fmts_map[book_id].add(file_fmt)

            # Note that this book has a specific fmt in the col_book_map
            self.col_book_map[file_fmt].add(book_id)

            # Generate the priority fmt - use it to record the specific stats for this file
            if file_fmt in specific_book_fmt_count_map.keys():
                specific_book_fmt_count_map[file_fmt] += 1
                file_fmt_name = file_fmt + "_{}".format(specific_book_fmt_count_map[file_fmt])
            else:
                specific_book_fmt_count_map[file_fmt] = 1
                file_fmt_name = file_fmt + "_1"

            # Will be used later when adding the backups
            file_id_priority_fmt_map[file_id] = file_fmt_name

            # Use the fmt_priority to note the rest of the information about that file
            self.book_col_map[book_id].append(file_fmt_name)
            self.fname_map[book_id][file_fmt_name] = file_name
            self.size_map[book_id][file_fmt_name] = file_size
            self.book_file_map[book_id][file_fmt_name] = file_id

            # Record the file locations - if they can be found
            file_row = db.get_row_from_id("files", file_id)
            try:
                file_loc = self.fsm.get_loc(asset_row=file_row)
            except:
                file_loc = None
            self.book_file_loc_map[book_id][file_fmt_name] = file_loc

            # Record the file in the fmts count

        # Process the files that are backups
        # Todo: Make sure that the restriction of one backup per file is enforced somewhere else
        backed_up_files = set()
        for backup_file_id in backup_dict:

            original_file_id = backup_dict[backup_file_id]
            if original_file_id in backed_up_files:
                raise DatabaseIntegrityError

            file_fmt, file_name, file_size = book_backup_data_dict[original_file_id]

            fmt_priority_name = "ORIGINAL_{}".format(file_id_priority_fmt_map[original_file_id])

            # Use the fmt_priority to note the rest of the information about that file
            self.book_col_map[book_id].append(fmt_priority_name)
            self.fname_map[book_id][fmt_priority_name] = file_name
            self.size_map[book_id][fmt_priority_name] = file_size
            self.book_file_map[book_id][fmt_priority_name] = backup_file_id

            # Record the file locations - if they can be found
            file_row = db.get_row_from_id("files", original_file_id)
            try:
                file_loc = self.fsm.get_loc(asset_row=file_row)
            except:
                file_loc = None
            self.book_file_loc_map[book_id][fmt_priority_name] = file_loc

        # Freeze the values for the book_col_map as a tuple to make them harder to accidentally modify
        self.book_col_map[book_id] = tuple(sorted(self.book_col_map[book_id]))
        self.book_col_count_map[book_id] = specific_book_fmt_count_map

    def update_fmt(
        self, book_id: SrcTableID, fmt: Union[SpecificFormat, GenericFormat], fname: str, size: int, db
    ) -> None:
        """
        Update the metadata for the particular format for this particular book.

        :param book_id: The id of the book to work on
        :param fmt: The format in the book to work on.
                    If the given fmt is not a priority fmt, then it'll be assumed that the fmt to update is the highest
                    priority file of that fmt associated with the book.
                    But please try and use the specific format where possible to cut down on potential confusion.
        :param fname: The updated name for the format
        :param size: The updated size for the format
        :param db: The database to apply the changes to
        :return fmt_new_size: The new size of the format after the changes have taken effect
        """
        base_format = fmt.split("_")[0]
        if not self.check_fmt_is_priority_fmt(fmt):
            fmt += "_1"

        # Retrieve the list of available formats from the book_col_map - add the newly available format and re-add the
        # list
        # Todo: What's the point of this? We're doing an update...
        fmts = list(self.book_col_map.get(book_id, []))
        try:
            fmts.remove(fmt)
        except ValueError:
            pass
        fmts.append(fmt)
        self.book_col_map[book_id] = tuple(sorted(fmts))

        # If an entry for that particular format already exists, add to it - if not, KeyError then create it
        try:
            self.col_book_map[base_format].add(book_id)
        except KeyError:
            self.col_book_map[fmt] = {book_id}

        # Change the cached values for the file metadata
        if fname is not None:
            self.fname_map[book_id][fmt] = fname
        self.size_map[book_id][fmt] = size

        # Counts dictionary should be left unchanged, as should the book_file_map (the id of the file shouldn't have
        # changed

        # Look up the id of the actual file - use this to update the files table on the database - if it is found, then
        # update the record - if it isn't found then insert the record and continue
        try:
            file_id = self.book_file_map[book_id][fmt]
        except KeyError:
            priority_fmt = fmt + "_1"

            if priority_fmt in self.book_file_map[book_id].keys():
                file_id = self.book_file_map[book_id][priority_fmt]
            else:
                err_str = "KeyError while trying to read the file_id"
                err_str = default_log.log_variables(
                    err_str,
                    "ERROR",
                    ("fmt", fmt),
                    ("book_id_book_file_map", self.book_file_map[book_id]),
                )
                raise KeyError(err_str)

        if fname is not None:
            db.macros.set_file_size_and_name(file_id=file_id, size=size, fname=fname)
        else:
            db.macros.set_file_size(file_id=file_id, size=size)

    def get_last_priority_fmt(self, book_id: SrcTableID, fmt: GenericFormat) -> SpecificFormat:
        """
        Return the highest priority fmt for the title - needed when adding a fmt to the end of the priority stack.

        :param book_id:
        :param fmt:
        :return:
        """
        base_fmt = self.prep_base_fmt(fmt)
        in_use_fmts = set([fn for fn in self.fname_map[book_id].keys()])
        for i in range(1, 10000):
            test_fmt = "{}_{}".format(base_fmt, i + 1)
            if test_fmt not in in_use_fmts:
                return "{}_{}".format(base_fmt, i)
        raise KeyError("Cannot get_last_priority_fmt - out of range")

    def get_all_priority_fmts(self, book_id: SrcTableID, fmt: GenericFormat) -> Iterable[SpecificFormat]:
        """
        Return all the priority fmts corresponding to a given base fmt.

        :return:
        """
        base_fmt = self.prep_base_fmt(fmt)
        in_use_fmts = set([fn for fn in self.fname_map[book_id].keys()])

        fmt_re = r"^{}_[0-9]+$".format(base_fmt)
        fmt_pat = re.compile(fmt_re, re.I)

        found_fmts = set()
        for priority_fmt in in_use_fmts:

            if fmt_pat.match(priority_fmt):
                found_fmts.add(priority_fmt)

        return found_fmts

"""
Tables are one level of abstraction below fields - they represent links between two database tables.

(Is it a bad name for this object - kinda. Yes. But changing it breaks calibre compatibility too badly).

Tables represent links between the titles (or books) and various resources (e.g. series, tags for titles and formats
for books).
There are various types of table - named after the type of link
- one_to_one
- - items like titles or a book's uuid - only one of them can be assigned to a book or a title at any one time
- one_to_many
- - items like comments or notes. Many of them can be assigned to a title at any one time, but each of them is not
    shared between titles
- - items linked to titles in this way can be unique or not unique.
- - - e.g. many identifiers can be uniquely assigned to one title (or one book)
- - - e.g. many comments can be assigned to a given title - but the comments might not be unique (if they where you
      could never assign the same comment to two books)
- many_to_one
- - many books can be assigned to one item. E.g. the physical location of books in a library - many books can be on
    one shelf, but no book can be on two shelves at the same time
- many_to_many
- - Many of these can be linked to many titles. E.g. tags - one book can have many tags and one tag can be associated
    with many books
- - Also true for series - one book can be in many series and one series can have many titles

As elsewhere, as we're using the same naming as calibre, these classes assume that you'd only ever care about a table
in the context of it's link to books.
"""

import re

from typing import Optional, Callable, Any, Generic, TypeVar, Iterable, Mapping, Union

from LiuXin_alpha.errors import DatabaseIntegrityError, InputIntegrityError

from LiuXin_alpha.preferences import preferences

from LiuXin_alpha.utils.libraries.calibre_date import c_parse
from LiuXin_alpha.databases.db_types import (
    MetadataDict,
    SrcTableID,
    DstTableID,
    DataTypes,
    TableTypes,
    MainTableName,
    InterLinkTableName,
    TableColumnName,
    UUIDStr,
    SpecificFormat,
    GenericFormat,
    MetadataDisplayDict,
)
from LiuXin_alpha.interfaces.field_metadata import calibre_name_to_liuxin_name
from LiuXin_alpha.utils.logging import default_log

ONE_ONE, MANY_ONE, MANY_MANY, ONE_MANY = range(4)

null = object()

# ----------------------------------------------------------------------------------------------------------------------
#
# - ONE TO ONE TABLE BASES

T = TypeVar("T")


class BaseTable(Generic[T]):
    """
    Base class for any table like implementation in any cache.
    """

    def __init__(self, name: str, metadata: MetadataDict, link_table=None, custom: bool = False) -> None:
        """
        Start up the table.

        :param name: The name of the table
        :param metadata: A metadata object with, at least,
        :param link_table:
        :param custom: Is this a custom table?
        :return:
        """
        # Todo: This is a HEINOUS hack - need to sort the metadata later
        if name != "publisher":
            self.name: str = name
        else:
            self.name: str = "publishers"
        self.metadata = metadata

        self.sort_alpha: bool = metadata.get("is_multiple", False) and metadata.get("display", {}).get(
            "sort_alpha", False
        )

        # self.unserialize() provides methods maps values from the db to python objects
        self.unserialize: Optional[Callable[[Any,], Any]] = {
            "datetime": c_parse,
            "bool": bool,
        }.get(metadata["datatype"], None)

        # Legacy
        if name == "authors":
            self.unserialize = lambda x: x.replace("|", ",") if x else ""

        self.custom: bool = custom

        self.main_table_name: Optional[str] = None
        self.auxiliary_table_name: Optional[str] = None

        # LiuXin specific properties
        self.lx_table_name: Optional[str] = None
        self.table_id_col: Optional[str] = None
        self.linked_to: Optional[str] = None
        self.link_table: Optional[str] = None
        self.link_table_bt_id_column: Optional[str] = None
        self.link_table_table_id_column: Optional[str] = None
        self.link_table_priority_col: Optional[str] = None
        self.link_table_type_col: Optional[str] = None

    def remove_books(self, book_ids: Iterable[SrcTableID], db) -> set[DstTableID]:
        """
        Remove books from the table.

        Should be called when books are removed (this is an element of a cache, and it needs to be updated).
        :param book_ids:
        :param db:
        :return:
        """
        return set()

    def fix_link_table(self, db) -> None:
        """
        LiuXin compatibility method - called to set the link table for this table.

        :param db:
        :return:
        """
        pass

    def fix_case_duplicates(self, db) -> None:
        """
        If this table contains entries that differ only by case, then merge those entries.

        This can happen in databases created with old versions of calibre and non-ascii values, since sqlite's
        NOCASE only works with ascii text.
        :param db: The database containing the table
        :return:
        """
        pass

    def set_link_tables(self, db, set_priority: bool = True, set_type: bool = True) -> None:
        """
        For comparability reasons it is sometimes desirable to have a ManyToOne table appear as a OneToOne table.

        If this is the case then data such as a link table is needed.
        :param db: The database to read the link types from
        :param set_priority: Set the priority column for the link table
        :param set_type: Set the type column for the link table
        :return:
        """
        # Characterize the table which is being linked to
        table_name = self.name
        table_name = calibre_name_to_liuxin_name(table_name)

        # Infer which of "books" and "titles" the table is linked to
        title_cand = db.driver_wrapper.get_link_table_name(table1=table_name, table2="titles")
        if title_cand:
            self.linked_to = "titles"
            self.link_table = title_cand
            self.link_table_bt_id_column = db.driver_wrapper.get_interlink_column(
                table1=table_name, table2="titles", column_type="title_id"
            )
            table_id_col = db.driver_wrapper.get_id_column(table_name)
            self.link_table_table_id_column = db.driver_wrapper.get_interlink_column(
                table1=table_name, table2="titles", column_type=table_id_col
            )

            self.table_id_col = table_id_col
            self.lx_table_name = table_name
            # Todo: Return now?

        book_cand = db.driver_wrapper.get_link_table_name(table1=table_name, table2="books")
        if book_cand:
            self.linked_to = "books"
            self.link_table = book_cand
            self.link_table_bt_id_column = db.driver_wrapper.get_interlink_column(
                table1=table_name, table2="books", column_type="book_id"
            )
            table_id_col = db.driver_wrapper.get_id_column(table_name)
            self.link_table_table_id_column = db.driver_wrapper.get_interlink_column(
                table1=table_name, table2="books", column_type=table_id_col
            )

            self.table_id_col = table_id_col
            self.lx_table_name = table_name

        # Inference has failed - abort
        # NOTE: driver_wrapper.get_link_table_name() returns False (not None) when missing
        if not book_cand and not title_cand:
            return

        # Extra safety: don't proceed unless inference actually set a target
        if not self.linked_to:
            return

        if set_priority:

            try:
                self.link_table_priority_col = db.driver_wrapper.get_interlink_column(
                    table1=table_name, table2=self.linked_to, column_type="priority"
                )
            except (DatabaseIntegrityError, InputIntegrityError):
                pass

        if set_type:

            try:
                self.link_table_type_col = db.driver_wrapper.get_interlink_column(
                    table1=table_name, table2=self.linked_to, column_type="type"
                )
            except (DatabaseIntegrityError, InputIntegrityError):
                pass

    def update_db(self, book_id_to_val_map: Mapping[SrcTableID, Any], db, allow_case_change: bool = False) -> bool:
        """
        Method for writing updates out to the database.

        (specifically the links between this table and another - data for which should be contained in metadata).

        There is a similar upate_db method in each of the fields - mostly that method should just call this one, however
        that method is there for if you want to override update behavior at the field level.
        :param book_id_to_val_map:
        :param db:
        :param allow_case_change:
        :return status: Did the update actually go through?
        """
        return self.writer.set_books(book_id_to_val_map, db, allow_case_change=allow_case_change)


# Todo: set the generic based off the datatype
class BaseVirtualTable(BaseTable[T]):
    """
    Used for fields that only exist in memory e.g ondevice.
    """

    def __init__(self, name: str, table_type: TableTypes = ONE_ONE, datatype: DataTypes = "text") -> None:
        """


        :param name:
        :param table_type:
        :param datatype:
        """

        metadata: MetadataDict = {"datatype": datatype, "table": name}
        self.table_type = table_type
        BaseTable.__init__(self, name, metadata)


# Todo: This seems like a problem which smarter men than I have solved - a pythonic caching layer over a db
class BaseOneToOneTable(BaseTable[T]):
    """
    Serves as a generic base for OneToOneTables in the cache.

    Inherits from base table.
    If you need to inherit all tables in your cache from a customized base table, that's a valid thing to do - consider
    multiple inheritance and consulting https://rhettinger.wordpress.com/2011/05/26/super-considered-super/

    Represents data that is unique per book - assigned to it in a 1-1 mapping - e.g. uuid, timestamp, size e.t.c.
    This generally involved reading something from the db's "meta" view - where all information about each of the books
    is aggregated.
    """

    # Todo: Load the valid main tables in for typing purporses from a json?
    # Todo: You should NOT be able to alter this at runtime
    table_type: TableTypes = ONE_ONE

    def __init__(
        self,
        name: MainTableName,
        metadata: MetadataDict,
        link_table: Optional[InterLinkTableName] = None,
        custom: bool = False,
    ) -> None:
        """
        Setup for a OneToOne table - a value which is singular for a "book".

        :param name:
        :param metadata:
        :param link_table: If applicable, the table linking this table to the titles or books table
                           Or whatever other table this table is linked to.
        :param custom:
        """
        BaseTable.__init__(self, name, metadata, link_table, custom=custom)

        self.linked_to: Optional[MainTableName] = None
        self.link_table: Optional[InterLinkTableName] = None
        self.link_table_bt_id_column: Optional[TableColumnName] = None
        self.link_table_table_id_column: Optional[TableColumnName] = None
        self.link_table_priority_col: Optional[TableColumnName] = None


class BasePathTable(BaseOneToOneTable[T]):
    """
    Contains a Location object for every book folder on the database.
    Each book_id has a tuple of the Locations of the folders associated with it.
    """

    def set_path(self, book_id: SrcTableID, path: str, db) -> bool:
        """
        Update the cache with the path - a specialized write which just does this.

        :param book_id: The id of the book to update the path for in the cache.
        :param path: The path string to write out to the book
        :param db: Database to update.
        :return status: Did the cache update go through?
        """
        raise NotImplementedError

    @staticmethod
    def set_db_path(book_id: SrcTableID, path: str, db) -> bool:
        """
        Set the override path for the book in the database.

        :param book_id: The id of the book to updte the path for on the database
        :param path: The path to write out to the database
        :param db: The database to update
        :return status: Did the db write go through?
        """
        # Todo: This should be a macro itself - no sql outside the drivers
        return db.macros.execute("UPDATE books SET book_paths=? WHERE book_id=?", (path, book_id))


class BaseSizeTable(BaseOneToOneTable[T]):
    def update_sizes(self, size_map: Mapping[SrcTableID, int]) -> bool:
        """
        Update the cache when changes occur to the overall size of the files stored in the folder store manager.

        :param size_map: Keyed with the id of the book and valued with the new size.
        :return status: Did the update go through successfully?
        """
        raise NotImplementedError("You must implement this class!")

    def _parse_size_mode(self) -> None:
        """
        Parse the preferences to determine how the size of the book should be calculated

        Internally sets the size_mode
        :return:
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


class BaseUUIDTable(BaseOneToOneTable[UUIDStr]):
    """
    Stores the 1-1 correspondence between books and uuids.

    May also store a cache of uuid values which can be used to more quickly look up a book from it's uuid.
    """

    def update_uuid_cache(self, book_id_val_map: Mapping[SrcTableID, UUIDStr]) -> bool:
        """
        Updates the uuid cache - used when changes occur to the uuid assigned to a book

        Mostly these changes should be the addition of more books to the database.
        But you can also manually update this - i.e. to bring it into line with another database.
        :param book_id_val_map: Keyed with the id for the books to update and valued with their new UUIDs
        :return:
        """
        raise NotImplementedError

    def remove_books(self, book_ids: Iterable[SrcTableID], db) -> bool:
        """
        Remove books from the cache - doesn't clear them from the database.

        :param book_ids: The ids to remove from the table
        :param db: The database to preform the writes to
        :return status: Did the books get removed?
        """
        raise NotImplementedError

    def lookup_by_uuid(self, uuid: UUIDStr) -> SrcTableID:
        """
        Reverse lookup - provides the book which corresponds to that UUID.

        :param uuid: The uuid to search for in the books table.
        :return:
        """
        raise NotImplementedError


class BaseCompositeTable(BaseOneToOneTable[T]):
    """
    Composite tables contain data form multiple different tables.

    As such, updates may be somewhat complicated.
    """

    def __init__(
        self, name: str, metadata: MetadataDict, link_table: InterLinkTableName = None, custom: bool = False
    ) -> None:
        """
        Setup for a Composite table - a table which contains data from multiple different tables.

        :param name:
        :param metadata:
        :param link_table: If applicable, the table linking this table to the titles or books table
        :param custom:
        """
        BaseOneToOneTable.__init__(self, name=name, metadata=metadata, link_table=link_table, custom=custom)

        self.composite_template: Optional[list[str]] = None
        self.contains_html: bool = False
        self.make_category: bool = False
        self.composite_sort: bool = False
        self.use_decorations: bool = False

    def read(self, db) -> None:
        """
        Because the values for composite caches tend to be generated on the fly minimal actual reading is needed.

        Sets interval values of this table.
        :param db:
        :return:
        """

        d: MetadataDisplayDict = self.metadata["display"]
        self.composite_template: list[str] = ["composite_template"]
        self.contains_html: bool = d.get("contains_html", False)
        self.make_category: bool = d.get("make_category", False)
        self.composite_sort: bool = d.get("composite_sort", False)
        self.use_decorations: bool = d.get("use_decorations", False)


#
# ----------------------------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------
#
# - MANY TO MANY TABLES


class BaseManyToOneTable(BaseTable[T]):

    table_type: TableTypes = MANY_ONE

    def __init__(
        self, name: MainTableName, metadata: MetadataDict, link_table: InterLinkTableName = None, custom: bool = False
    ) -> None:
        """
        Startup a ManyToOneTable - includes the link_table and if the Table is custom (which may effect how the table
        behaves in some circumstances).
        :param name: The name of the table
        :param metadata: The metadata associated with the table - how it should display and other properties
        :param link_table: THe table linking this table to either the books or titles field
        :param custom: Is this table a custom table
        """
        super(BaseManyToOneTable, self).__init__(name, metadata, link_table, custom=custom)

    def fix_link_table(self, db) -> bool:
        """
        Originally removed any items from the table which where not linked to the book

        this functionality is not provided here, as
        1) The Metadata might be assigned to things which are not books, and thus be still in use
        2) Metadata should be properly deleted, with the library delete methods that will be made available
        3) Just because a resource is currently not in use does not mean it won't be used again.
        :param db: Database to apply the changes to
        :return status: Did the changes go through as expected?
        """
        pass

    def fix_case_duplicates(self, db) -> bool:
        """
        Originally intended to merge any items from the table which only differed up to a change of case.

        Will have to be handled a bit more carefully - as most objects in LiuXin are defined with more than just a name.
        But may of them should not, in principle, just differ by the case of their name.
        :param db: The database to apply the changes to
        :return status: Did the changes go through as expected?
        """
        pass

    def remove_items(self, item_ids: Iterable[DstTableID], db) -> set[SrcTableID]:
        """
        Remove items from the table, updating the cache and then the link row

        :param item_ids: Ids of the things which this table is linked to - which will be removed
        :param db: The database to apply the changes to.
        :return affected_books: Books that where affected by removing the items
        """
        raise NotImplementedError

    def rename_item(self, item_id: DstTableID, new_name: str, db) -> bool:
        """
        Change the column value for the item_id to the value given by new_name

        :param item_id: The item to update
        :param new_name: The value to change the column to
        :param db: The database to preform the change in
        :return status: Did the operation succeed?
        """
        raise NotImplementedError


class BaseRatingTable(BaseManyToOneTable[T]):
    """
    Base for the rating table - which stores the ratings of a work.
    """

    def __init__(
        self, name: MainTableName, metadata: MetadataDict, link_table: InterLinkTableName = None, custom: bool = False
    ) -> None:
        """
        Start up the ratings table - which stores the rating information for the books.

        :param name:
        :param metadata:
        :param link_table:
        :param custom:
        """
        super(BaseRatingTable, self).__init__(name, metadata, link_table, custom)

        # By, default ratings pretend to be a ManyToOne table - many titles linked to one rating
        # Actually, in LiuXin, it's a typed ManyToMany table - many titles linked to many ratings with different types
        self.type_filter = "calibre"


#
# ----------------------------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------
#
# - MANY TO MANY TABLES


# Todo: store the link type in table metadata
class BaseManyToManyTable(BaseManyToOneTable[T]):
    """
    Represents data that has a many-to-many mapping with books.

    i.e. each book can have more than one value and each value can be mapped to more than one book.
    i.e. "tags" or "authors"/"creators" linked to "titles".
    i.e. But also "series"
    In LiuXin most of the tables are linked together with ManyToMany links
    """

    table_type: TableTypes = MANY_MANY

    do_clean_on_remove: bool = True


class BaseTypedManyToManyTable(BaseManyToManyTable):
    """
    Represents a MantToMany field with a type - e.g. creators - which have various types which might be of interest.
    """

    @property
    def seen_types(self) -> set[str]:
        """
        A set of all the types which have been used on the table.

        :return:
        """
        raise NotImplementedError


class BaseCreatorsTable(BaseTypedManyToManyTable):
    """
    Represents the creators associated with a title - with some additional methods for the creators table.
    """

    def set_sort_names(self, aus_map: Mapping[SrcTableID, str], db) -> Mapping[SrcTableID, str]:
        """
        Update the database with the given author_sort map

        :param aus_map: An author_sort map
        :param db: The database to write the changes out to
        :return aus_map: A processed author sort map - as it will actually be written into the database.
        """
        raise NotImplementedError

    def set_links(self, link_map: Mapping[SrcTableID, str], db) -> Mapping[SrcTableID, str]:
        """
        NOTE: THIS DOES NOT UPDATE THE LINKS BETWEEN CREATOR AND BOOKS, DESPITE THE CONFUSING NAME.

        This uses the link_map (keyed with the creator_id, valued with the value that the creator_link will have) to
        update the creators table with new links.
        :param link_map:
        :param db:
        :return link_map: With the standard transforms done on the values
        """
        raise NotImplementedError

    def remove_books(self, book_ids: Iterable[SrcTableID], db) -> bool:
        """
        Remove books from this cache.

        :param book_ids:
        :param db:
        :return status: Did the remove go through?
        """
        raise NotImplementedError


class BaseCoversTable(BaseManyToManyTable[T]):
    """
    Basis for the covers table - contains information as to the covers linked to titles.
    """

    do_clean_on_remove: bool = False


class BaseFormatsTable(BaseManyToManyTable[T]):
    """
    Basis for the formats table = contains information as to the files linked to a book.
    """

    do_clean_on_remove: bool = False

    def set_fname(self, book_id: SrcTableID, fmt: str, fname: str, db) -> bool:
        """
        Changes the file_name for the given format of the given file.

        :param book_id: id of the book to update the file name for
        :param fmt: fmt string for the book to trigger update - needs to be a LiuXin priority format e.g. MOBI_1
        :param fname: The new name of the format
        :param db: The database to preform the update in
        :return status: Did the update go through?
        """
        raise NotImplementedError

    def remove_formats(
        self, formats_map: Mapping[SrcTableID, Iterable[Union[SpecificFormat, GenericFormat]]], db
    ) -> bool:
        """
        Takes a format map - keyed with the book_id and valued with the formats to remove.

        Removes those formats first from the cache and then from the database.
        The formats are expected to be LiuXin formats - thus things like EPUB_2.

        If a SpecificFormat is passed that format, and only that format, will be removed.
        If a GenericFormat is passed then all formats of that type will be removed.

        format priorities will be updated to take account of the removal of the old formats.
        :param formats_map: Keyed with the id of the book and valued with the formats to remove.
        :param db:
        :return status: Did the remove go through?
        """
        raise NotImplementedError

    def reload_book_from_db(self, db, book_id: SrcTableID) -> bool:
        """
        Reload information about a book from the db.

        :param db: The database to reload from
        :param book_id:
        :return status: Did the reload go through?
        """
        raise NotImplementedError

    def update_fmt(self, book_id: SrcTableID, fmt: SpecificFormat, fname: str, size: int, db) -> int:
        """
        Update the metadata for the particular format for this particular book.

        :param book_id: The id of the book to work on
        :param fmt: The format in the book to work on.
                    If the given fmt is not a priority fmt, then it'll be assumed that the fmt to update is the highest
                    priority file of that fmt associated with the book
        :param fname: The updated name for the format
        :param size: The updated size for the format
        :param db: The database to apply the changes to
        :return fmt_new_size: The new size of the format after the changes have taken effect
        """
        raise NotImplementedError

    def get_last_priority_fmt(self, book_id: SrcTableID, fmt: GenericFormat) -> SpecificFormat:
        """
        Return the highest priority fmt for the title - needed when adding a fmt to the end of the priority stack.

        :param book_id: Book id to get the highest priority format for
        :param fmt:
        :return:
        """
        raise NotImplementedError

    def get_all_priority_fmts(self, book_id: SrcTableID, fmt: GenericFormat) -> Iterable[SpecificFormat]:
        """
        Return all the priority fmts corresponding to a given GenericFormat.

        E.g. a call of "EPUB" would yield "EPUB_1", "EPUB_2" e.t.c.
        :return:
        """
        raise NotImplementedError

    @staticmethod
    def check_fmt_is_priority_fmt(fmt: SpecificFormat) -> bool:
        """
        Checks that the given fmt is a priority fmt (fmt of the form, e.g. EPUB_1)

        Does not check the cache or database.
        Just checks that the string has the right format.
        :param fmt:
        :return status: Does the string have the right format or not?
        """
        num_regex = re.match(r"([A-Z0-9_]+)_[0-9]+$", fmt)
        if num_regex:
            return True
        else:
            return False

    @staticmethod
    def stand_fmt(fmt: str) -> str:
        """
        Bring a fmt into standard form.

        No checking of the cache or database is preformed.
        :param fmt:
        :return:
        """
        fmt = fmt.upper()
        if fmt.startswith("."):
            return fmt[1:]
        return fmt

    @staticmethod
    def prep_base_fmt(fmt: str) -> str:
        """
        Prepare the format for inclusion in the book_fmts_map.

        If it has a priority number, strip it. If it has a leading dot remove it. If it starts with the word ORIGINAL
        then remove it - should be left with just the base format without anything else on it.
        :return:
        """
        # Upper case and strip any preceding .
        fmt = fmt.upper()
        if fmt.startswith("."):
            fmt = fmt[1:]

        # If the fmt ends with a number, then remove it
        num_regex = re.match(r"([A-Z0-9]+)_[0-9]+$", fmt)
        if num_regex:
            return num_regex.group(1)
        else:
            if "_" in fmt:
                fmt_tokens = fmt.split("_")
                if len(fmt_tokens) == 3:
                    return fmt_tokens[-2]
                elif len(fmt_tokens) == 2:
                    return fmt_tokens[-1]
                else:
                    raise NotImplementedError("This position should never be reached")
            else:
                return fmt


class BaseIdentifiersTable(BaseManyToManyTable[T]):
    """
    Basis for the identifiers table - which sis an unordered typed table.
    """

    pass


#
# ----------------------------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------
#
# - BASE PROPERTY
# Properties are used to more fully characterize the link between two assets
# e.g. the index of a series in a series_title_link is a property which should be so characterized


class BaseLinkAttributeTable(Generic[T]):
    """
    Represents a property (attribute) of a link between two assets
    """

    def __init__(
        self,
        name: str,
        link_table_name: InterLinkTableName,
        link_table: BaseTable,
        main_table: MainTableName,
        auxiliary_table: MainTableName,
    ) -> None:
        """
        Startup. Stores the name of the property this class represents as well as the underlying table.

        :param name: Name of the property
        :param link_table_name: The property is defined in the following link table
        :param link_table: Table class representing the underlying link table
        :param main_table: The name of the main table
        :param auxiliary_table: The name of the auxiliary table
        """
        self.name = name
        self.link_table_name = link_table_name
        self.link_table = link_table

        # Characterize the fields for easier access
        self.main_table = main_table
        self.auxiliary_table = auxiliary_table

        # Characterize the link table - these should be set by the containing table before trying to read the property
        # information from the database into this table.
        self.property_column: Optional[TableColumnName] = None
        self.main_id_col: Optional[TableColumnName] = None
        self.auxiliary_id_col: Optional[TableColumnName] = None

    # ------------------------------------------------------------------------------------------------------------------
    #
    # - STARTUP METHODS

    def read(self, db) -> None:
        """
        Preforms a read of information from the database into this table.

        After this method has been called, the table should be populated with data.
        :param db:
        :return:
        """
        raise NotImplementedError("Need to either actually do the ")

    def set_link_properties(self, db) -> None:
        """
        Set the characteristics of the link table.

        Needs the main and auxiliary tables to be set.
        :param db: The database to set the properties from.
        :return:
        """
        # Link table name
        self.link_table_name = db.driver_wrapper.get_link_table_name(self.main_table, self.auxiliary_table)

        # Property column
        self.property_column = db.driver_wrapper.get_interlink_column(self.main_table, self.auxiliary_table, self.name)

        # Main id column
        main_table_id_col = db.driver_wrapper.get_id_column(self.main_table)
        self.main_id_col = db.driver_wrapper.get_interlink_column(
            self.main_table, self.auxiliary_table, main_table_id_col
        )

        # Auxiliary id column
        aux_id_col = db.driver_wrapper.get_id_column(self.auxiliary_table)
        self.auxiliary_id_col = db.driver_wrapper.get_interlink_column(
            self.main_table, self.auxiliary_table, aux_id_col
        )

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - API METHODS

    @staticmethod
    def _property_adapter(link_attr: Any) -> T:
        """
        Used when reading properties off the database - affects how the data is locally stored for purposes of sorting.

        Unless overridden will just return the identity.
        :param link_attr:
        :return:
        """
        return link_attr

    def get_property(self, main_id: SrcTableID, auxiliary_id: DstTableID) -> Optional[T]:
        """
        Return the property for a given title_id and object_id.

        :param main_id:
        :param auxiliary_id:
        :return:
        """
        raise NotImplementedError("Need to specify to a cache type")

    def get_sorted_auxiliary_vals(self, main_id: SrcTableID) -> Iterable[DstTableID]:
        """
        Return a set of right ids sorted in some way.

        The exact form this takes can depend on the nature of the property. By default it's just a sort by the value of
        the property.
        :param main_id:
        :return:
        """
        raise NotImplementedError("Need to specify to a cache type")

    def get_sorted_main_values(self, auxiliary_id: DstTableID) -> Iterable[SrcTableID]:
        """
        Return a set of left ids sorted in some way.

        The exact form this takes can depend on the nature of the property.
        By default it's just a sort by the value of the property.
        :param auxiliary_id:
        :return:
        """
        raise NotImplementedError("Need to specify to a cache type")

    #
    # ------------------------------------------------------------------------------------------------------------------

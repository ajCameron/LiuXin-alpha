"""
Model cached tables, relations and normalized value mappings.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise base tables through a consuming regression::

        python -m pytest -q tests/customize/test_customize_base.py
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
from LiuXin_alpha.catalog.field_metadata import calibre_name_to_liuxin_name
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

    Example:
        Exercise BaseTable through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
    """

    def __init__(self, name: str, metadata: MetadataDict, link_table=None, custom: bool = False) -> None:
        """
        Start up the table.

        Example:
            Exercise BaseTable.  init   through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param metadata: Value supplied for metadata under the utility contract.
        :param link_table: Value supplied for link table under the utility contract.
        :param custom: Value supplied for custom under the utility contract.
        :return: None; validated state is stored on the receiving object.
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

        Example:
            Exercise BaseTable.remove books through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_ids: Book identities included in the batched read operation.
        :param db: Value supplied for db under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return set()

    def fix_link_table(self, db) -> None:
        """
        LiuXin compatibility method - called to set the link table for this table.

        Example:
            Exercise BaseTable.fix link table through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param db: Value supplied for db under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pass

    def fix_case_duplicates(self, db) -> None:
        """
        If this table contains entries that differ only by case, then merge those entries.

        Example:
            Exercise BaseTable.fix case duplicates through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param db: Value supplied for db under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pass

    def set_link_tables(self, db, set_priority: bool = True, set_type: bool = True) -> None:
        """
        For comparability reasons it is sometimes desirable to have a ManyToOne table appear as a OneToOne table.

        Example:
            Exercise BaseTable.set link tables through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param db: Value supplied for db under the utility contract.
        :param set_priority: Value supplied for set priority under the utility contract.
        :param set_type: Value supplied for set type under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
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

        Example:
            Exercise BaseTable.update db through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_id_to_val_map: Value supplied for book id to val map under the utility
            contract.
        :param db: Value supplied for db under the utility contract.
        :param allow_case_change: Value supplied for allow case change under the utility
            contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.writer.set_books(book_id_to_val_map, db, allow_case_change=allow_case_change)


# Todo: set the generic based off the datatype
class BaseVirtualTable(BaseTable[T]):
    """
    Used for fields that only exist in memory e.g ondevice.

    Example:
        Exercise BaseVirtualTable through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
    """

    def __init__(self, name: str, table_type: TableTypes = ONE_ONE, datatype: DataTypes = "text") -> None:
        """
        Initialize and validate the basevirtualtable state.

        Example:
            Exercise BaseVirtualTable.  init   through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param table_type: Value supplied for table type under the utility contract.
        :param datatype: Value supplied for datatype under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """

        metadata: MetadataDict = {"datatype": datatype, "table": name}
        self.table_type = table_type
        BaseTable.__init__(self, name, metadata)


# Todo: This seems like a problem which smarter men than I have solved - a pythonic caching layer over a db
class BaseOneToOneTable(BaseTable[T]):
    """
    Serves as a generic base for OneToOneTables in the cache.

    Example:
        Exercise BaseOneToOneTable through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
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

        Example:
            Exercise BaseOneToOneTable.  init   through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param metadata: Value supplied for metadata under the utility contract.
        :param link_table: Value supplied for link table under the utility contract.
        :param custom: Value supplied for custom under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        BaseTable.__init__(self, name, metadata, link_table, custom=custom)

        self.linked_to: Optional[MainTableName] = None
        self.link_table: Optional[InterLinkTableName] = None
        self.link_table_bt_id_column: Optional[TableColumnName] = None
        self.link_table_table_id_column: Optional[TableColumnName] = None
        self.link_table_priority_col: Optional[TableColumnName] = None


class BasePathTable(BaseOneToOneTable[T]):
    """
    Contains a Location object for every book folder on the database. Each book_id has a tuple of the Locations of the folders associated with it.

    Example:
        Exercise BasePathTable through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
    """

    def set_path(self, book_id: SrcTableID, path: str, db) -> bool:
        """
        Update the cache with the path - a specialized write which just does this.

        Example:
            Exercise BasePathTable.set path through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_id: Value supplied for book id under the utility contract.
        :param path: Filesystem path read, written, normalized or validated by the
            operation.
        :param db: Value supplied for db under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @staticmethod
    def set_db_path(book_id: SrcTableID, path: str, db) -> bool:
        """
        Set the override path for the book in the database.

        Example:
            Exercise BasePathTable.set db path through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_id: Value supplied for book id under the utility contract.
        :param path: Filesystem path read, written, normalized or validated by the
            operation.
        :param db: Value supplied for db under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        # Todo: This should be a macro itself - no sql outside the drivers
        return db.macros.execute("UPDATE books SET book_paths=? WHERE book_id=?", (path, book_id))


class BaseSizeTable(BaseOneToOneTable[T]):
    """
    Provide the basesizetable contract for validated ebook processing.

    Example:
        Exercise BaseSizeTable through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
    """
    def update_sizes(self, size_map: Mapping[SrcTableID, int]) -> bool:
        """
        Update the cache when changes occur to the overall size of the files stored in the folder store manager.

        Example:
            Exercise BaseSizeTable.update sizes through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param size_map: Value supplied for size map under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError("You must implement this class!")

    def _parse_size_mode(self) -> None:
        """
        Parse the preferences to determine how the size of the book should be calculated

        Example:
            Exercise BaseSizeTable. parse size mode through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
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

    Example:
        Exercise BaseUUIDTable through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
    """

    def update_uuid_cache(self, book_id_val_map: Mapping[SrcTableID, UUIDStr]) -> bool:
        """
        Updates the uuid cache - used when changes occur to the uuid assigned to a book

        Example:
            Exercise BaseUUIDTable.update uuid cache through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_id_val_map: Value supplied for book id val map under the utility
            contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def remove_books(self, book_ids: Iterable[SrcTableID], db) -> bool:
        """
        Remove books from the cache - doesn't clear them from the database.

        Example:
            Exercise BaseUUIDTable.remove books through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_ids: Book identities included in the batched read operation.
        :param db: Value supplied for db under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def lookup_by_uuid(self, uuid: UUIDStr) -> SrcTableID:
        """
        Reverse lookup - provides the book which corresponds to that UUID.

        Example:
            Exercise BaseUUIDTable.lookup by uuid through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param uuid: Value supplied for uuid under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError


class BaseCompositeTable(BaseOneToOneTable[T]):
    """
    Composite tables contain data form multiple different tables.

    Example:
        Exercise BaseCompositeTable through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
    """

    def __init__(
        self, name: str, metadata: MetadataDict, link_table: InterLinkTableName = None, custom: bool = False
    ) -> None:
        """
        Setup for a Composite table - a table which contains data from multiple different tables.

        Example:
            Exercise BaseCompositeTable.  init   through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param metadata: Value supplied for metadata under the utility contract.
        :param link_table: Value supplied for link table under the utility contract.
        :param custom: Value supplied for custom under the utility contract.
        :return: None; validated state is stored on the receiving object.
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

        Example:
            Exercise BaseCompositeTable.read through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param db: Value supplied for db under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
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

    """
    Provide the basemanytoonetable contract for validated ebook processing.

    Example:
        Exercise BaseManyToOneTable through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
    """
    table_type: TableTypes = MANY_ONE

    def __init__(
        self, name: MainTableName, metadata: MetadataDict, link_table: InterLinkTableName = None, custom: bool = False
    ) -> None:
        """
        Startup a ManyToOneTable - includes the link_table and if the Table is custom (which may effect how the table behaves in some circumstances).

        Example:
            Exercise BaseManyToOneTable.  init   through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param metadata: Value supplied for metadata under the utility contract.
        :param link_table: Value supplied for link table under the utility contract.
        :param custom: Value supplied for custom under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        super(BaseManyToOneTable, self).__init__(name, metadata, link_table, custom=custom)

    def fix_link_table(self, db) -> bool:
        """
        Originally removed any items from the table which where not linked to the book

        Example:
            Exercise BaseManyToOneTable.fix link table through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param db: Value supplied for db under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pass

    def fix_case_duplicates(self, db) -> bool:
        """
        Originally intended to merge any items from the table which only differed up to a change of case.

        Example:
            Exercise BaseManyToOneTable.fix case duplicates through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param db: Value supplied for db under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pass

    def remove_items(self, item_ids: Iterable[DstTableID], db) -> set[SrcTableID]:
        """
        Remove items from the table, updating the cache and then the link row

        Example:
            Exercise BaseManyToOneTable.remove items through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param item_ids: Value supplied for item ids under the utility contract.
        :param db: Value supplied for db under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def rename_item(self, item_id: DstTableID, new_name: str, db) -> bool:
        """
        Change the column value for the item_id to the value given by new_name

        Example:
            Exercise BaseManyToOneTable.rename item through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param item_id: Value supplied for item id under the utility contract.
        :param new_name: Value supplied for new name under the utility contract.
        :param db: Value supplied for db under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError


class BaseRatingTable(BaseManyToOneTable[T]):
    """
    Base for the rating table - which stores the ratings of a work.

    Example:
        Exercise BaseRatingTable through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
    """

    def __init__(
        self, name: MainTableName, metadata: MetadataDict, link_table: InterLinkTableName = None, custom: bool = False
    ) -> None:
        """
        Start up the ratings table - which stores the rating information for the books.

        Example:
            Exercise BaseRatingTable.  init   through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param metadata: Value supplied for metadata under the utility contract.
        :param link_table: Value supplied for link table under the utility contract.
        :param custom: Value supplied for custom under the utility contract.
        :return: None; validated state is stored on the receiving object.
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

    Example:
        Exercise BaseManyToManyTable through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
    """

    table_type: TableTypes = MANY_MANY

    do_clean_on_remove: bool = True


class BaseTypedManyToManyTable(BaseManyToManyTable):
    """
    Represents a MantToMany field with a type - e.g. creators - which have various types which might be of interest.

    Example:
        Exercise BaseTypedManyToManyTable through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
    """

    @property
    def seen_types(self) -> set[str]:
        """
        A set of all the types which have been used on the table.

        Example:
            Exercise BaseTypedManyToManyTable.seen types through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError


class BaseCreatorsTable(BaseTypedManyToManyTable):
    """
    Represents the creators associated with a title - with some additional methods for the creators table.

    Example:
        Exercise BaseCreatorsTable through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
    """

    def set_sort_names(self, aus_map: Mapping[SrcTableID, str], db) -> Mapping[SrcTableID, str]:
        """
        Update the database with the given author_sort map

        Example:
            Exercise BaseCreatorsTable.set sort names through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param aus_map: Value supplied for aus map under the utility contract.
        :param db: Value supplied for db under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def set_links(self, link_map: Mapping[SrcTableID, str], db) -> Mapping[SrcTableID, str]:
        """
        NOTE: THIS DOES NOT UPDATE THE LINKS BETWEEN CREATOR AND BOOKS, DESPITE THE CONFUSING NAME.

        Example:
            Exercise BaseCreatorsTable.set links through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param link_map: Value supplied for link map under the utility contract.
        :param db: Value supplied for db under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def remove_books(self, book_ids: Iterable[SrcTableID], db) -> bool:
        """
        Remove books from this cache.

        Example:
            Exercise BaseCreatorsTable.remove books through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_ids: Book identities included in the batched read operation.
        :param db: Value supplied for db under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError


class BaseCoversTable(BaseManyToManyTable[T]):
    """
    Basis for the covers table - contains information as to the covers linked to titles.

    Example:
        Exercise BaseCoversTable through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
    """

    do_clean_on_remove: bool = False


class BaseFormatsTable(BaseManyToManyTable[T]):
    """
    Basis for the formats table = contains information as to the files linked to a book.

    Example:
        Exercise BaseFormatsTable through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
    """

    do_clean_on_remove: bool = False

    def set_fname(self, book_id: SrcTableID, fmt: str, fname: str, db) -> bool:
        """
        Changes the file_name for the given format of the given file.

        Example:
            Exercise BaseFormatsTable.set fname through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_id: Value supplied for book id under the utility contract.
        :param fmt: Date, number or template format specification.
        :param fname: Value supplied for fname under the utility contract.
        :param db: Value supplied for db under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def remove_formats(
        self, formats_map: Mapping[SrcTableID, Iterable[Union[SpecificFormat, GenericFormat]]], db
    ) -> bool:
        """
        Takes a format map - keyed with the book_id and valued with the formats to remove.

        Example:
            Exercise BaseFormatsTable.remove formats through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param formats_map: Value supplied for formats map under the utility contract.
        :param db: Value supplied for db under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def reload_book_from_db(self, db, book_id: SrcTableID) -> bool:
        """
        Reload information about a book from the db.

        Example:
            Exercise BaseFormatsTable.reload book from db through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param db: Value supplied for db under the utility contract.
        :param book_id: Value supplied for book id under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def update_fmt(self, book_id: SrcTableID, fmt: SpecificFormat, fname: str, size: int, db) -> int:
        """
        Update the metadata for the particular format for this particular book.

        Example:
            Exercise BaseFormatsTable.update fmt through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_id: Value supplied for book id under the utility contract.
        :param fmt: Date, number or template format specification.
        :param fname: Value supplied for fname under the utility contract.
        :param size: Value supplied for size under the utility contract.
        :param db: Value supplied for db under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def get_last_priority_fmt(self, book_id: SrcTableID, fmt: GenericFormat) -> SpecificFormat:
        """
        Return the highest priority fmt for the title - needed when adding a fmt to the end of the priority stack.

        Example:
            Exercise BaseFormatsTable.get last priority fmt through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_id: Value supplied for book id under the utility contract.
        :param fmt: Date, number or template format specification.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def get_all_priority_fmts(self, book_id: SrcTableID, fmt: GenericFormat) -> Iterable[SpecificFormat]:
        """
        Return all the priority fmts corresponding to a given GenericFormat.

        Example:
            Exercise BaseFormatsTable.get all priority fmts through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_id: Value supplied for book id under the utility contract.
        :param fmt: Date, number or template format specification.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @staticmethod
    def check_fmt_is_priority_fmt(fmt: SpecificFormat) -> bool:
        """
        Checks that the given fmt is a priority fmt (fmt of the form, e.g. EPUB_1)

        Example:
            Exercise BaseFormatsTable.check fmt is priority fmt through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param fmt: Date, number or template format specification.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
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

        Example:
            Exercise BaseFormatsTable.stand fmt through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param fmt: Date, number or template format specification.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        fmt = fmt.upper()
        if fmt.startswith("."):
            return fmt[1:]
        return fmt

    @staticmethod
    def prep_base_fmt(fmt: str) -> str:
        """
        Prepare the format for inclusion in the book_fmts_map.

        Example:
            Exercise BaseFormatsTable.prep base fmt through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param fmt: Date, number or template format specification.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
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

    Example:
        Exercise BaseIdentifiersTable through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
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

    Example:
        Exercise BaseLinkAttributeTable through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
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

        Example:
            Exercise BaseLinkAttributeTable.  init   through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param link_table_name: Value supplied for link table name under the utility
            contract.
        :param link_table: Value supplied for link table under the utility contract.
        :param main_table: Value supplied for main table under the utility contract.
        :param auxiliary_table: Value supplied for auxiliary table under the utility
            contract.
        :return: None; validated state is stored on the receiving object.
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

        Example:
            Exercise BaseLinkAttributeTable.read through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param db: Value supplied for db under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError("Need to either actually do the ")

    def set_link_properties(self, db) -> None:
        """
        Set the characteristics of the link table.

        Example:
            Exercise BaseLinkAttributeTable.set link properties through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param db: Value supplied for db under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
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

        Example:
            Exercise BaseLinkAttributeTable. property adapter through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param link_attr: Value supplied for link attr under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return link_attr

    def get_property(self, main_id: SrcTableID, auxiliary_id: DstTableID) -> Optional[T]:
        """
        Return the property for a given title_id and object_id.

        Example:
            Exercise BaseLinkAttributeTable.get property through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param main_id: Value supplied for main id under the utility contract.
        :param auxiliary_id: Value supplied for auxiliary id under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError("Need to specify to a cache type")

    def get_sorted_auxiliary_vals(self, main_id: SrcTableID) -> Iterable[DstTableID]:
        """
        Return a set of right ids sorted in some way.

        Example:
            Exercise BaseLinkAttributeTable.get sorted auxiliary vals through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param main_id: Value supplied for main id under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError("Need to specify to a cache type")

    def get_sorted_main_values(self, auxiliary_id: DstTableID) -> Iterable[SrcTableID]:
        """
        Return a set of left ids sorted in some way.

        Example:
            Exercise BaseLinkAttributeTable.get sorted main values through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param auxiliary_id: Value supplied for auxiliary id under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError("Need to specify to a cache type")

    #
    # ------------------------------------------------------------------------------------------------------------------

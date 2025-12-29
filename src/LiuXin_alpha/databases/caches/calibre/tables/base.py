"""
This provides bse classes to model tables.

This is one step of abstraction under fields.
It's the fundamental object of the cache - the place where the data is actually stored.
Though it tends to be _accessed_ through the fields.
"""

from __future__ import unicode_literals

from typing import Optional, TypeVar, Mapping, Union, Iterable

from LiuXin.customize.cache.base_tables import BaseTable, BaseVirtualTable
from LiuXin.databases.db_types import (
    MetadataDict,
    MainTableName,
    InterLinkTableName,
    SrcTableID,
    DstTableID,
    TableColumnName,
)
from LiuXin.databases.field_metadata import calibre_name_to_liuxin_name

from LiuXin.exceptions import DatabaseIntegrityError

null = object()

T = TypeVar("T")


# Todo: Once everything is tested, fix the class hierarchy
# Todo: Rename these damn things. They are NOT tables. They are the data from tables in other tables.
# CalibreLinkedTableData?
class CalibreBaseTable(BaseTable[T]):
    """
    All Calibre tables should descend from this one.
    """

    # Characterize the class
    _priority: bool = False
    _typed: bool = False

    @property
    def priority(self):
        return self._priority

    @priority.setter
    def priority(self, value):
        raise AttributeError("Cannot change priority property of this class")

    @property
    def typed(self):
        return self._typed

    @typed.setter
    def typed(self, value):
        raise AttributeError("Cannot change priority property of this class")

    def __init__(
        self,
        name: MainTableName,
        metadata: MetadataDict,
        link_table: Optional[InterLinkTableName] = None,
        custom: bool = False,
    ) -> None:
        """
        Startup a calibre table.

        :param name: The name of the table
        :param metadata: A metadata object with, at least,
        :param link_table: Optional explicit name of the link table - otherwise it will be inferred from metadata.
        :param custom: Is this a custom table?
        :return:
        """
        super(CalibreBaseTable, self).__init__(name=name, metadata=metadata, link_table=link_table, custom=custom)

        # Used to actually preform writes to the database - should be set at the same time as the writer method in the
        # field containing this table
        self.writer = None

    # ------------------------------------------------------------------------------------------------------------------
    #
    # - STARTUP METHODS
    def read(self, db) -> None:
        """
        Load the table with data from the database.

        :param db:
        :return:
        """
        raise NotImplementedError("read methods have to be defined on a per table basis")

    def read_link_attributes(self, db) -> None:
        """
        Startup task - reads the properties of the given link table.

        :param db:
        :return:
        """
        raise NotImplementedError("Has to be implemented on a by table basis")

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - UPDATE METHODS

    def update(
        self,
        book_id_val_map: Mapping[SrcTableID, Union[T, Iterable[T]]],
        db,
        id_map: Optional[Mapping[DstTableID, T]] = None,
        allow_case_change: bool = False,
    ) -> None:
        """
        Update both the cache and the database at the same time.

        As a rule, when updating this class - you should call _this_ method.
        :param book_id_val_map:
        :param db:
        :param id_map:
        :param allow_case_change:
        :return status: Did the update go through?
        """
        raise NotImplementedError

    # Todo: Standardize naming between the update_cache and update_db methods
    def update_cache(
        self,
        book_id_val_map: Mapping[SrcTableID, Union[T, Iterable[T]]],
        id_map: Optional[Mapping[DstTableID, T]] = None,
    ) -> bool:
        """
        Preform an internal update of the data cached in this table.

        :param book_id_val_map:
        :param id_map:
        :return:
        """
        raise NotImplementedError

    def update_db(
        self, book_id_to_val_map: Mapping[SrcTableID, Union[T, Iterable[T]]], db, allow_case_change: bool = False
    ) -> bool:
        """
        Method for updating the database

        (specifically the links between this table and another - data for which should be contained in metadata).
        There is a similar upate_db method in each of the fields - mostly that method should just call this one, however
        that method is there for if you want to override update behavior at the field level.
        :param book_id_to_val_map:
        :param db:
        :param allow_case_change:
        :return status: Did the update to the db go through?
        """
        return self.writer.set_books(book_id_to_val_map, db, allow_case_change=allow_case_change)

    #
    # ------------------------------------------------------------------------------------------------------------------


class CalibreVirtualTable(BaseVirtualTable[T]):

    pass


class MultiTableMixin:
    """
    Provides some helpful methods for tables which use a link table.
    Including an __init__ witth variables to characterize the link table and methods to set properties of that link.
    """

    # Todo: Should only be able to access these in a scope that makes sense
    # Todo: These probably need to be moved over into macros
    selectq: str = "SELECT {0}, {1} FROM {2} ORDER BY {3} Asc;"
    selectq_filter: str = 'SELECT {0}, {1} FROM {2} WHERE {4} = "{5}" ORDER BY {3} Asc;'

    selectq_desc: str = "SELECT {0}, {1} FROM {2} ORDER BY {3} Desc;"
    selectq_filter_desc: str = 'SELECT {0}, {1} FROM {2} WHERE {4} = "{5}" ORDER BY {3} Desc;'

    selectq_no_priority: str = "SELECT {0}, {1} FROM {2};"
    selectq_filter_no_priority: str = 'SELECT {0}, {1} FROM {2} WHERE {4} = "{5}";'

    custom_selectq_old: str = "SELECT book, {0} FROM {1} ORDER BY id"
    custom_selectq: str = "SELECT {lt_col}_book, {lt_col}_{lt_link_col} FROM {lt} ORDER BY {lt_col}_id"

    def __init__(self, name: MainTableName, custom: bool, link_table: Optional[InterLinkTableName]) -> None:
        """
        Preform startup for the variables needed to characterize the other table.
        """
        self.name: MainTableName = name

        # The name of the table under LiuXin (as some tables names are different between LiuXin and calibre)
        self.lx_table_name: Optional[MainTableName] = None

        # The id column for the table being accessed
        self.table_id_col: Optional[MainTableName] = None

        # The table this table is linked to - choices should be "titles" or "books"
        self.linked_to: Optional[MainTableName] = None
        self.link_table: Optional[InterLinkTableName] = None if not custom else link_table

        # Link table book or title column - the column in the link table containing either the title or book id
        # depending on which of those two columns this table is linked to
        self.link_table_bt_id_column: Optional[TableColumnName] = None

        # Link table table column - the column in the link table containing the id of the table that is being linked to
        self.link_table_table_id_column: Optional[TableColumnName] = None

        # Link table priority column - the column in the link table noting the priority of the link represented by an
        # entry on the table
        self.link_table_priority_col: Optional[TableColumnName] = None

        # Type column - when the type of a link is set
        self.link_table_type: Optional[TableColumnName] = None

    def set_link_table(self, db, set_type: bool = False) -> None:
        """
        Set the value for self.link_table and self.link_table_bt_column -

        :param db: The database to be used to calculate the link table.
        :param set_type: If True, will attempt to set the type column along with everything else
        :return:
        """
        if hasattr(self, "custom") and self.custom:

            self.linked_to = "books"
            # Todo: Replace this with the db method for getting a link table
            try:
                self.link_table = "{}_{}_link".format("books", self.metadata["table"])
            except KeyError:
                self.link_table = None

        else:
            # Todo: This should all rely on self.metadata["table"] - check that we can
            table_name = self.name
            table_name = calibre_name_to_liuxin_name(table_name)

            if table_name.startswith("#"):
                table_name = self.metadata["table"]

            title_cand = db.driver_wrapper.get_link_table_name(table1=table_name, table2="titles")
            book_cand = db.driver_wrapper.get_link_table_name(table1=table_name, table2="books")
            if title_cand:
                self.linked_to = "titles"
                self.link_table = title_cand
                self.link_table_bt_id_column = db.driver_wrapper.get_interlink_column(
                    table1=table_name, table2="titles", column_type="title_id"
                )

            elif book_cand:
                self.linked_to = "books"
                self.link_table = book_cand
                self.link_table_bt_id_column = db.driver_wrapper.get_interlink_column(
                    table1=table_name, table2="books", column_type="book_id"
                )

            else:

                return

            table_id_col = db.driver_wrapper.get_id_column(table_name)

            self.link_table_table_id_column = db.driver_wrapper.get_interlink_column(
                table1=table_name, table2=self.linked_to, column_type=table_id_col
            )

            # Todo: Shouldn't even be trying to do this unless we're in an appropriate table type
            try:
                self.link_table_priority_col = db.driver_wrapper.get_interlink_column(
                    table1=table_name, table2=self.linked_to, column_type="priority"
                )
            except DatabaseIntegrityError:
                pass

            # Todo: Shouldn't even be trying to do this unless we're in an appropriate table type
            if set_type:
                self.link_table_type = db.driver_wrapper.get_interlink_column(
                    table1=table_name, table2=self.linked_to, column_type="type"
                )

            self.table_id_col = table_id_col
            self.lx_table_name = table_name
            return

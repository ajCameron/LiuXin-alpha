
"""
Container for a
"""

import datetime
import pprint
from copy import deepcopy

from typing import Optional, Union, Iterator, Any

from LiuXin_alpha.errors import DatabaseDriverError, RowReadOnlyError

from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode
from LiuXin_alpha.utils.logging import default_log

from LiuXin_alpha.databases.api import DatabaseAPI, RowAPI


class Row(RowAPI):
    """
    Contains a row off the database.
    """

    def __init__(self, database: DatabaseAPI, row_dict: Optional[dict[str, str]] = None, read_only: bool = False) -> None:
        """
        Represents a single row from the LiuXin database.

        :param database: A LiuXin database object
        :param row_dict: Keyed with the column names and valued with their values.
        :param read_only: If True then the row is loaded in read only mode
        :return:
        """
        super().__init__(database=database, row_dict=row_dict, read_only=read_only)

        self.read_only = read_only

        # Preform checking on the inputs
        if database is None:
            err_str = "Row called without a DatabasePing"
            err_str = default_log.log_variables(err_str, "ERROR", ("row_dict", row_dict), ("database", database))
            raise DatabaseDriverError(err_str)
        self.db = database

        # Copy the given row_dict into the local row_dict
        local_row_dict = dict()
        if row_dict is not None:
            local_row_dict = deepcopy(row_dict)
        self.int_row_dict = local_row_dict

        # Properties that will be read off the database/derived from the row
        self.table = None
        self.allowed_tables = None
        self.row_id = None
        self.self_linkable = False
        self.linkable_tables = []
        self.allowed_columns = set()

        self.refresh_db_properties()

        if self.read_only:
            self.sync = self.no_sync

    def make_read_only(self):
        """
        Makes the row read only.

        :return:
        """
        self.read_only = True
        self.sync = self.no_sync

    def refresh_db_properties(self) -> None:
        """
        Read the properties for the row off the database.

        :return:
        """
        row_dict = object.__getattribute__(self, "int_row_dict")
        if not row_dict:
            allowed_tables = set([t for t in self.db.get_tables_and_columns().keys()])
            object.__setattr__(self, "allowed_tables", allowed_tables)
            return None

        table = self.db.driver_wrapper.identify_table_from_row_dict(row_dict)
        object.__setattr__(self, "table", table)

        allowed_tables = set([t for t in self.db.get_tables_and_columns().keys()])
        object.__setattr__(self, "allowed_tables", allowed_tables)

        row_id = self.db.driver_wrapper.get_id_from_row(row_dict)
        if row_id != 0:
            row_id = row_id if row_id else None
        elif row_id is None:
            pass
        else:
            row_id = 0

        object.__setattr__(self, "row_id", row_id)

        self_linkable = True if self.db.driver_wrapper.check_for_intralink_table(table) else False
        object.__setattr__(self, "self_linkable", self_linkable)

        linkable_tables = self.db.driver_wrapper.get_interlinked_tables(table)
        object.__setattr__(self, "linkable_tables", linkable_tables)

        allowed_columns = self.db.get_column_headings(table)
        object.__setattr__(self, "allowed_columns", allowed_columns)

    @property
    def row_dict(self):
        return self.int_row_dict

    @row_dict.setter
    def row_dict(self, val):
        self.int_row_dict = deepcopy(val)

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - OUTPUT OPTIONS START HERE
    #
    # ----------------------------------------------------------------------------------------------------------------------

    def __unicode__(self):
        """
        Unicode representation of the row.
        :return:
        """
        info_str = "LiuXin Row Object\n"

        info_str += "row_dict: \n" + pprint.pformat(object.__getattribute__(self, "row_dict")) + "\n"

        info_str += "table: " + six_unicode(object.__getattribute__(self, "table")) + "\n"
        info_str += "allowed_tables: " + pprint.pformat(object.__getattribute__(self, "allowed_tables")) + "\n"
        info_str += "row_id: " + six_unicode(object.__getattribute__(self, "row_id")) + "\n"
        info_str += "self_linkable: " + six_unicode(object.__getattribute__(self, "self_linkable")) + "\n"
        info_str += "linkable_tables: " + six_unicode(object.__getattribute__(self, "linkable_tables")) + "\n"
        info_str += "allowed_columns: " + six_unicode(object.__getattribute__(self, "allowed_columns")) + "\n"

        return info_str

    def __str__(self):
        return self.__unicode__().encode("utf-8")

    def __repr__(self):

        rtn_str = "|LX ROW OBJECT - DatabasePing {0} - Table {1} - Id {2}|".format(
            repr(self.db),
            object.__getattribute__(self, "table"),
            six_unicode(object.__getattribute__(self, "row_id")),
        )
        return rtn_str

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - I/O METHODS START HERE
    #
    # ----------------------------------------------------------------------------------------------------------------------

    def __setitem__(self, key: str, value: Union[str, int, float, datetime.datetime]) -> None:
        """
        Allows a dictionary like interface to the row.

        :param key:
        :param value:
        :return:
        """
        row_dict = object.__getattribute__(self, "int_row_dict")
        target_table = self.db.driver_wrapper.identify_table_from_column(key, error=False)
        if target_table is None:
            err_str = "Cannot set item - does not correspond to a column heading from any table in this database"
            err_str = default_log.log_variables(err_str, "ERROR", ("db", self.db), ("key", key), ("value", value))
            raise KeyError(err_str)

        # If the row_dict has nothing in it add the value and proceed
        if not row_dict:
            row_dict[key] = value
            self.refresh_db_properties()
            return None

        # Check to make sure the key is on the list of allowed column headings
        allowed_cols = object.__getattribute__(self, "allowed_columns")
        if key not in allowed_cols:
            err_str = "Cannot set item - key is not one of the column headings allowed for this table."
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("db", self.db),
                ("key", key),
                ("value", value),
                ("allowed_cols", allowed_cols),
            )
            raise KeyError(err_str)

        row_dict[key] = value
        return None

    def __getitem__(self, item: str) -> Union[str, int, float, datetime.datetime]:
        """
        Allows a dictionary like interface to the row.

        :param item:
        :return:
        """
        row_dict = object.__getattribute__(self, "int_row_dict")
        if item in row_dict:
            return row_dict[item]

        allowed_columns = object.__getattribute__(self, "allowed_columns")
        if item in allowed_columns:
            row_dict[item] = None
            return row_dict[item]

        err_str = "item couldn't be found in the row_dict, and wasn't a recognized column heading for this table"
        err_str = default_log.log_variables(
            err_str,
            "ERROR",
            ("item", item),
            ("row_dict", row_dict),
            ("allowed_columns", allowed_columns),
        )
        raise KeyError(err_str)

    # ---------------------------
    #
    # - UPDATE METHODS START HERE

    def update_and_check(self) -> None:
        """
        Updates the metadata stored about the row in the class.

        :return:
        """
        self.refresh_db_properties()

    def load_row_from_id(self, row_id: int = None, table: str = None) -> None:
        """
        If an id is present, load or reload the row_dict from it.

        :param row_id: The id of the row to load - if None, tries to use the id already present
        :param table: The name of the table to load the row from
        :return:
        """
        if row_id is not None:
            object.__setattr__(self, "row_id", row_id)
        if table is not None:
            object.__setattr__(self, "table", table)

        row_id = object.__getattribute__(self, "row_id")
        table = object.__getattribute__(self, "table")
        if row_id is None or table is None:
            err_str = "Unable to load_from_id  - id or table has yet to be set."
            default_log.error(err_str)
            raise TypeError(err_str)

        row_dict = self.db.driver_wrapper.get_row_from_id(table=table, row_id=row_id)
        object.__setattr__(self, "row_dict", row_dict)

        self.refresh_db_properties()

    def load_blank_row(self, table: Optional[str] = None) -> None:
        """
        Load a blank row off the given database - will block if the table or row_dict fields are already full.

        :param table:
        :return:
        """
        if table is not None:
            object.__setattr__(self, "table", table)

        blank_row_dict = self.db.driver_wrapper.get_blank_row(object.__getattribute__(self, "table"))
        object.__setattr__(self, "int_row_dict", blank_row_dict)

        self.refresh_db_properties()

    def ensure_row_has_id(self) -> None:
        """
        Makes sure that the row_dict has an id in it.

        :return:
        """
        new_row_dict = self.db.driver_wrapper.ensure_row_has_id(object.__getattribute__(self, "row_dict"))
        new_id = self.db.driver_wrapper.get_id_from_row(new_row_dict)

        object.__setattr__(self, "row_dict", new_row_dict)
        object.__setattr__(self, "row_id", new_id)

    def sync(self) -> None:
        """
        Sync the current contents of the row to the database.

        :return:
        """
        if self.row_id is None:
            self.ensure_row_has_id()

        row_dict = object.__getattribute__(self, "int_row_dict")
        if row_dict:
            self.db.driver_wrapper.update_row(row_dict)

    def no_sync(self) -> None:
        """
        Method to replace sync if we're in read only mode.

        :return:
        """
        raise RowReadOnlyError("You cannot sync this row - we're in read only mode.")

    # ---------------------------
    # -------------------------------
    # - COMPARISON METHODS START HERE

    def __hash__(self) -> int:
        """
        A hash for the row based on the table, id and database - will fail unless all three of these are filled.
        :return:
        """
        uuid = self.db.uuid
        row_id = object.__getattribute__(self, "row_id")
        table = object.__getattribute__(self, "table")
        return hash((uuid, row_id, table))

    def __eq__(self, other: RowAPI) -> bool:
        """
        Uses the hash function to test equality.

        :param other:
        :return:
        """
        self_hash = self.__hash__()
        other_hash = hash(other)
        if self_hash == other_hash:
            return True
        else:
            return False

    # -------------------------------
    # -----------------------------------------------
    #
    # - DICTIONARY EMULATION MAGIC METHODS START HERE

    def keys(self) -> None:
        """
        Returns the keys from the row_dict dictionary.

        :return:
        """
        row_dict = object.__getattribute__(self, "int_row_dict")
        return row_dict.keys()

    def __iter__(self) -> Iterator[str]:
        """
        Allows use of the in statement in content of a for loop.

        Iterates over all the column headings in the row.
        If the row has been loaded from the database then all column headings will be set - including if the row is
        black. If the row is being constructed rom the invididual keys, only the keys that have been set will be
        returned.
        :return:
        """
        row_dict = object.__getattribute__(self, "int_row_dict")
        keys_list = row_dict.keys()
        for key in keys_list:
            yield key

    def __contains__(self, item: str) -> bool:
        """
        Allows use of the in statement - returns true if the item is in the row_dict - false otherwise.

        :param item:
        :return:
        """
        row_dict = object.__getattribute__(self, "int_row_dict")
        if item in row_dict.keys():
            return True
        else:
            return False

    # -----------------------------------------------
    # ------------------------
    #
    # - COPY MAGIC STARTS HERE

    def __deepcopy__(self, memo: dict[Any, Any]) -> RowAPI:
        """
        Allows for deep copying.

        :param memo:
        :return:
        """
        # if memo:
        #     info_str = "Row __deepcopy__ passed a non-trivial memo"
        #     default_log.log_variables(info_str, "INFO", ("memo", memo))
        row_dict = object.__getattribute__(self, "int_row_dict")
        new_row_dict = deepcopy(row_dict)
        return Row(database=self.db, row_dict=new_row_dict)

    # ------------------------

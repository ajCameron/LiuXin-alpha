"""Driver-wrapper API contracts."""

from __future__ import annotations

import abc

from typing import Any, Iterable, Iterator, Optional, Union, TYPE_CHECKING, Literal

from LiuXin_alpha.databases.api.driver_wrapper_api.mixins.schema_introspection_api import SchemaIntrospectionAPI

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api.database_api import DatabaseAPI
    from LiuXin_alpha.databases.api.macros_api import MacrosAPI
    from LiuXin_alpha.databases.api.row_api import RowAPI


class DatabaseDriverWrapperAPI(SchemaIntrospectionAPI):
    """
    API contract for driver wrappers sitting between Database and DatabaseDriver.

    DB and higher classes use the Row classes.
    The driver, and lower, uses row_dicts.
    This class serves as a bridge between them.

    Eventually, we might want the database to talk to multiple drivers at the same time.
    This layer is also to make that easier - when the time comes.
    """

    def __init__(self, db: Optional["DatabaseAPI"] = None, macros: Optional["MacrosAPI"] = None) -> None:
        """
        Startup the driver wrapper.

        :param db:
        :param macros:
        """
        self.db: Optional["DatabaseAPI"] = db
        if macros is not None:
            self.set_macros(macros)
        try:
            super().__init__(db, macros)  # type: ignore[misc]
        except TypeError:
            try:
                super().__init__()  # type: ignore[misc]
            except TypeError:
                pass

    @abc.abstractmethod
    def __del__(self) -> None:
        """
        Break cycles in the driver and shut down.

        :return:
        """

    @abc.abstractmethod
    def _canonicalise_cc_in_table(self, in_table: str) -> str:
        """
        Produce a canonical CC name for a custom column in a table.

        :param in_table:
        :return:
        """

    @abc.abstractmethod
    def _get_custom_column_row(self, in_table: str, cc_name: str) -> "RowAPI":
        """
        Get the row representing a custom column.

        :param in_table:
        :param cc_name:
        :return:
        """

    @abc.abstractmethod
    def _walk(self, start_row: "RowAPI", table: str, table_id_col: str, table_parent_col: str) -> Iterable["RowAPI"]:
        """
        Front end for the tree walk method.

        :param start_row:
        :param table:
        :param table_id_col:
        :param table_parent_col:
        :return:
        """

    @abc.abstractmethod
    def add_multiple_rows(self, row_dict_list: list[dict[str, Any]]):
        """
        Add multiple rows to the database.

        :param row_dict_list:
        :return:
        """

    @abc.abstractmethod
    def add_row(self, row_dict: dict[str, Any]):
        """
        Add a single row to the database.

        :param row_dict:
        :return:
        """

    @abc.abstractmethod
    def break_cycles(self) -> None:
        """
        Used as part of the shutdown to kill stale driver connections.

        :return:
        """

    @abc.abstractmethod
    def check_for_intralink_table(self, table_name: str) -> Union[str, Literal[False]]:
        """
        Check to see if the given table supports one (or more) intralink tables.

        :param table_name:
        :return:
        """

    @abc.abstractmethod
    def clear(self, target_table: str) -> None:
        """
        Delete every row of the given target_table.

        :param target_table:
        :return:
        """

    @abc.abstractmethod
    def close(self) -> None:
        """
        Shut down the driver.

        :return:
        """

    @abc.abstractmethod
    def complete_row(self, partial_row: dict[str, Any]) -> dict[str, Any]:
        """
        Take a partial row - which should include the id value - and retrieve the rest of the values for it.

        :param partial_row:
        :return:
        """

    # Todo: Issue deprecitation warnings from this - we want it gone
    @property
    @abc.abstractmethod
    def conn(self):
        """
        Get the underlying connection to the database

        :return:
        """

    @abc.abstractmethod
    def create_custom_column(
            self,
            name: str,
            datatype: str = 'text',
            is_multiple: bool = False,
            label: Optional[str] = None,
            editable: bool = True,
            display: Optional[str] = None,
            in_table: str = 'books',
            table=None, make_category=None):
        """
        Create a custom column in the database attatched to a table.

        :param name:
        :param datatype:
        :param is_multiple:
        :param label:
        :param editable:
        :param display:
        :param in_table:
        :param table:
        :param make_category:
        :return:
        """

    @abc.abstractmethod
    def create_new_main_table(
            self,
            table_name: str,
            column_headings: Optional[Iterable[str]] = None,
            link_to: Optional[Union[str, Iterable[str]]] = None,
            link_type: Optional[Iterable[str]] = None,
            link_properties: Optional[Iterable[str]] = None):
        """
        Create a new main table in the database.

        :param table_name:
        :param column_headings:
        :param link_to:
        :param link_type:
        :param link_properties:
        :return:
        """

    @staticmethod
    @abc.abstractmethod
    def custom_table_names(num: int, in_table: str = 'books') -> str:
        """
        Get a custom table name.

        :param num:
        :param in_table:
        :return:
        """

    # Todo: Rename for greater clarity.
    # Todo: Might want to return the affected ids
    @abc.abstractmethod
    def delete(
            self,
            target_table: str,
            column: str,
            value: Any) -> None:
        """
        Delete rows by column value.

        :param target_table:
        :param column:
        :param value:
        :return:
        """

    @abc.abstractmethod
    def delete_by_id(self, target_table: str, row_id: int) -> None:
        """
        Delete an entire row by id.

        :param target_table:
        :param row_id:
        :return:
        """

    @abc.abstractmethod
    def delete_custom_column(self, num: str) -> None:
        """
        Delete a custom column specified by its num (id).

        :param num:
        :return:
        """

    @abc.abstractmethod
    def deleted_marked_custom_columns(self) -> None:
        """
        Actually delete the custom columns which have been marked for delete.

        :return:
        """

    @abc.abstractmethod
    def get_custom_tables(self) -> set[str]:
        """
        Get whatever custom tables exist.

        :return:
        """

    @abc.abstractmethod
    def direct_get_custom_extra(self, link_table: str, index: int) -> Any:
        """
        Get the value of the extra field for the custom link table.

        :param link_table:
        :param index:
        :return:
        """

    @abc.abstractmethod
    def direct_get_custom_id_val_pairs(self, table: str) -> Iterable[tuple[int, Any]]:
        """
        Get the custom_id value pairs for a table.

        :param table:
        :return:
        """

    @abc.abstractmethod
    def dirty_record(self, table: str, row_id: int, reason: str) -> None:
        """
        Note that a record has been dirtied.

        :param table:
        :param row_id:
        :param reason:
        :return:
        """

    @abc.abstractmethod
    def drop_all_triggers(self) -> None:
        """
        Drop all the triggers off the database.

        USE WITH CARE!
        :return:
        """

    @abc.abstractmethod
    def drop_triggers(self, triggers: list[str]) -> None:
        """
        Drop a specified list of triggers from the database.

        :param triggers:
        :return:
        """

    @abc.abstractmethod
    def ensure_row_has_id(self, row_dict: dict[str, Any]) -> dict[str, Any]:
        """
        Ensure a row dict has an id.

        :param row_dict:
        :return:
        """

    @abc.abstractmethod
    def execute(self, sql: str, values=None):
        """
        Execute SQL on the database.

        :param sql:
        :param values:
        :return:
        """

    @abc.abstractmethod
    def executemany(self, sql: list[str], values = None) -> None:
        """
        Execute a list of SQL statements on the database.

        :param sql:
        :param values:
        :return:
        """

    @abc.abstractmethod
    def executescript(self, sqlscript: str) -> None:
        """
        Execute a SQL script, which can include multiple statements.

        :param sqlscript:
        :return:
        """

    @abc.abstractmethod
    def get(self, *args, **kw):
        """
        Front end for the SQL connection get method.

        :param args:
        :param kw:
        :return:
        """

    @abc.abstractmethod
    def get_all_hashes(self) -> Iterable[str]:
        """
        Get all the hashes stored in the database.

        :return:
        """

    @abc.abstractmethod
    def get_all_rows(
            self,
            table: str,
            sort_column: Optional[str] = None,
            reverse: bool = False) -> Iterator[dict[str, Any]]:
        """
        Get all the rows off the database.

        :param table:
        :param sort_column:
        :param reverse:
        :return:
        """

    @abc.abstractmethod
    def get_blank_row(self, table: str) -> dict[str, Any]:
        """
        Get a blank row from the database.

        :param table:
        :return:
        """

    @abc.abstractmethod
    def get_column_base(self, table_name: str) -> str:
        """
        Get the base name of a column.

        :param table_name:
        :return:
        """

    @abc.abstractmethod
    def get_column_headings(self, table: str) -> list[str]:
        """
        Get the column headings for a particular table.

        :param table:
        :return:
        """

    # Todo: This needs to go. No direct connections if we can help it.
    @abc.abstractmethod
    def get_connection(self) -> Any:
        """
        Get a direct connection to the database.

        :return:
        """

    @abc.abstractmethod
    def get_datestamp_column(self, table: str) -> str:
        """
        Get the datestamp column for a particular table.

        :param table:
        :return:
        """

    @abc.abstractmethod
    def get_dirtied_count(self) -> int:
        """
        Get the number of dirtied records on the database.

        :return:
        """

    @abc.abstractmethod
    def get_display_column(self, table_name: str) -> str:
        """
        Get the display column for a particular table.

        :param table_name:
        :return:
        """

    @abc.abstractmethod
    def get_highest_id(self, target_table: str) -> int:
        """
        Get the highest id for a particular table.

        :param target_table:
        :return:
        """

    @abc.abstractmethod
    def get_id_column(self, table: str) -> str:
        """
        Get the id column from the database.

        :param table:
        :return:
        """

    @abc.abstractmethod
    def get_id_from_row(self, row_dict: dict[str, Any]) -> Optional[int]:
        """
        Extract and return the id from a given row.

        :param row_dict:
        :return:
        """

    @abc.abstractmethod
    def get_interlink_column(self, table1: str, table2: str, column_type: str) -> str:
        """
        Get the column in the interlink table defined by table1, table2 and the column type.

        :param table1:
        :param table2:
        :param column_type:
        :return:
        """

    @abc.abstractmethod
    def get_interlinked_tables(self, table_name: str) -> set[str]:
        """
        Get all the tables interlinked to the given table.

        :param table_name:
        :return:
        """

    @abc.abstractmethod
    def get_intralink_column(self, table: str, column_type: str) -> str:
        """
        Get a column name in the intralink table linking the table back to itself.

        :param table:
        :param column_type:
        :return:
        """

    @abc.abstractmethod
    def get_linear_row_list(self, start_row: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Get a linear row list from a tree structure in the database.

        :param start_row:
        :return:
        """

    @abc.abstractmethod
    def get_link_column(self, table1: str, table2: str, column_type: str) -> str:
        """
        Get a link column in the link table linking the two tables - no guarantee there is one.

        :param table1:
        :param table2:
        :param column_type:
        :return:
        """

    @abc.abstractmethod
    def get_link_table_name(self, table1: str, table2: str) -> str:
        """
        Get the link table name - no guarantee there is one.

        :param table1:
        :param table2:
        :return:
        """

    @abc.abstractmethod
    def get_parent_column(self, table_name: str) -> str:
        """
        Get the parent column from the database.

        :param table_name:
        :return:
        """

    @abc.abstractmethod
    def get_random_row(self, table: str, direct_access: bool = False) -> dict[str, Any]:
        """
        Get a random row from the database.

        :param table:
        :param row_dict:
        :param direct_access:
        :return:
        """

    @abc.abstractmethod
    def get_record_count(self, target_table: str) -> int:
        """
        Get the raw record count for the target table.

        :param target_table:
        :return:
        """

    @abc.abstractmethod
    def get_relation_type(self, name: str) -> Union[Literal["table"], Literal["view"]]:
        """
        Get the type of the name (e.g. 'table' or 'view').

        :param name:
        :return:
        """

    @abc.abstractmethod
    def get_row_from_id(self, table: str, row_id: int) -> dict[str, Any]:
        """
        Retrieve and return a row_dict directly from the database.

        :param table:
        :param row_id:
        :return:
        """

    @abc.abstractmethod
    def get_scratch_column(self, table: str) -> str:
        """
        Get the scratch column for the given table.

        :param table:
        :return:
        """

    @abc.abstractmethod
    def get_tables(self, force_refresh: bool=False) -> set[str]:
        """
        Return a set of all the tables in the database.

        :param force_refresh:
        :return:
        """

    @abc.abstractmethod
    def get_tables_and_columns(self) -> dict[str, set[str]]:
        """
        Get a dict keyed with the table names and valued with the columns in them.

        :return:
        """

    @abc.abstractmethod
    def get_triggers(self) -> list[str]:
        """
        Return all the triggers declared on the database.

        :return:
        """

    @abc.abstractmethod
    def get_uuid(self) -> str:
        """
        Get the UUID of the database.

        :return:
        """

    @abc.abstractmethod
    def get_view_column_headings(self, view: str) -> list[str]:
        """
        Get the column headings for a particular view.

        :param view:
        :return:
        """

    # Todo: A ViewRowAPI
    @abc.abstractmethod
    def get_view_row_from_id(self, view: str, row_id: int) -> dict[str, Any]:
        """
        Get a view row from a particular view and id.

        :param view:
        :param row_id:
        :return:
        """

    # Todo: Just error - don't given an option
    @abc.abstractmethod
    def identify_table_from_column(self, column_heading: str, error: bool = True) -> Optional[str]:
        """
        Identify a table from the column.

        Thanks to the schema, every column should appear in one, and only one table.
        As such, this can deterministically pin a column to a table.
        :param column_heading:
        :param error:
        :return:
        """

    @abc.abstractmethod
    def identify_table_from_row_dict(self, row_dict: dict[str, Any]) -> str:
        """
        Identify a table from the row dict.

        :param row_dict:
        :return:
        """

    @abc.abstractmethod
    def is_view(self, name: str) -> bool:
        """
        Check to see if the name corresponds to a known view in the database.

        :param name:
        :return:
        """

    @abc.abstractmethod
    def link_main_tables(
            self,
            primary_table: str,
            secondary_table: str,
            link_type: Union[Literal["one_one", "many_one", "one_many", "many_many"]],
            link_properties: Optional[Iterable[str]] = None) -> None:
        """
        Form a link from the primary table to the secondary table.

        :param primary_table:
        :param secondary_table:
        :param link_type:
        :param link_properties:
        :return:
        """

    @property
    @abc.abstractmethod
    def macros(self) -> "MacrosAPI":
        """
        Get the macros class for this database.

        :return:
        """

    # Todo: Check if it's it's null value, or NULL
    @abc.abstractmethod
    def nullify_column(self, table: str, row_id: int, column: str) -> None:
        """
        Set a particular cell in the database to it's null value.

        :param table:
        :param row_id:
        :param column:
        :return:
        """

    @abc.abstractmethod
    def read_metadata(self, field: str) -> Any:
        """
        Read metadata stored in the metadata table of the database.

        :param field:
        :return:
        """

    @abc.abstractmethod
    def search(self, table: str, column: str, search_term: Any) -> dict[str, Any]:
        """
        Execute a search query against the database.

        :param table:
        :param column:
        :param search_term:
        :return:
        """

    @abc.abstractmethod
    def set_custom_column_metadata(
            self,
            num: int,
            name: Optional[str] = None,
            label: Optional[str] = None,
            is_editable: Optional[bool] = None,
            display: Optional[str] = None,
            in_table: Optional[str] = None) -> None:
        """
        Set metadata for a custom column - writing to the custom columns table.

        :param num:
        :param name:
        :param label:
        :param is_editable:
        :param display:
        :param in_table:
        :return:
        """

    @abc.abstractmethod
    def set_full_column(self, table: str) -> None:
        """
        Set the full column entries for the given table with tree like structure.

        :param table:
        :return:
        """

    @abc.abstractmethod
    def set_macros(self, new_macros: MacrosAPI) -> None:
        """
        Set the underlying macros class for this method.

        :param new_macros:
        :return:
        """

    @abc.abstractmethod
    def set_tree_ids(self, table: str) -> None:
        """
        Set the tree ids for the given table.

        :param table:
        :return:
        """

    @abc.abstractmethod
    def set_uuid(self, new_force_value: Optional[str] = None) -> None:
        """
        Set the database UUID.

        :param new_force_value:
        :return:
        """

    @abc.abstractmethod
    def shell(self) -> None:
        """
        Drop into a database shell.

        :return:
        """

    # Todo: I thiiinkk we know that it's string or number for all database entries
    @abc.abstractmethod
    def update_column(
            self,
            table: str,
            row_id: int,
            column: str,
            new_value: Any) -> None:
        """
        Update a speicifc entry in a specific table and column.

        :param table:
        :param row_id:
        :param column:
        :param new_value:
        :return:
        """

    @abc.abstractmethod
    def update_columns(
            self,
            values_map: dict[int, Any],
            field: Optional[str] = None,
            table: Optional[str] = None) -> None:
        """
        Write a values map out to the database.

        :param values_map:
        :param field:
        :param table:
        :return:
        """

    # Todo: Where we say "num" in ref to custom columns, change it to cc_id or similar
    @abc.abstractmethod
    def update_custom_column(self, in_table: str, cc_name: str, value: Any) -> None:
        """
        Update a custom column in the database.

        :param in_table:
        :param cc_name:
        :param value:
        :return:
        """

    @abc.abstractmethod
    def update_row(self, row_dict: dict[str, Any]) -> None:
        """
        Update a row on the database from a row dict.

        :param row_dict:
        :return:
        """

    @property
    @abc.abstractmethod
    def user_version(self) -> str:
        """
        Get the user version of the database.

        :return:
        """

    @abc.abstractmethod
    def walk(self, start_row: dict[str, Any]) -> Iterable[dict[str, Any]]:
        """
        Walk a tree like structure and yield every row in it.

        :param start_row:
        :return:
        """

    @abc.abstractmethod
    def write_metadata(self, field: str, value: Any) -> None:
        """
        Write metadata to the database.

        :param field:
        :param value:
        :return:
        """

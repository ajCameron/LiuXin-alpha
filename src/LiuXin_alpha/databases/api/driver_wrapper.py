"""Driver-wrapper API contracts."""

from __future__ import annotations

import abc

from typing import Any, Iterable, Iterator, Optional, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database import DatabaseAPI
    from LiuXin_alpha.databases.api.macros import MacrosAPI
    from LiuXin_alpha.databases.api.row import RowAPI


class DatabaseDriverWrapperAPI(abc.ABC):
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
    def check_for_intralink_table(self, table_name: str) -> bool:
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
    def create_custom_column(self, name, datatype='text', is_multiple=False, label=None, editable=True, display=None, in_table='books', table=None, make_category=None):
        ...

    @abc.abstractmethod
    def create_new_main_table(self, table_name, column_headings=None, link_to=None, link_type=None, link_properties=None):
        ...

    @staticmethod
    @abc.abstractmethod
    def custom_table_names(num, in_table='books'):
        ...

    @abc.abstractmethod
    def delete(self, target_table, column, value):
        ...

    @abc.abstractmethod
    def delete_by_id(self, target_table, row_id):
        ...

    @abc.abstractmethod
    def delete_custom_column(self, num):
        ...

    @abc.abstractmethod
    def deleted_marked_custom_columns(self):
        ...

    @property
    @abc.abstractmethod
    def direct_custom_tables(self):
        ...

    @abc.abstractmethod
    def direct_get_custom_extra(self, link_table, index):
        ...

    @abc.abstractmethod
    def direct_get_custom_id_val_pairs(self, table):
        ...

    @abc.abstractmethod
    def dirty_record(self, table, row_id, reason):
        ...

    @abc.abstractmethod
    def drop_all_triggers(self):
        ...

    @abc.abstractmethod
    def drop_triggers(self, triggers):
        ...

    @abc.abstractmethod
    def ensure_row_has_id(self, row_dict):
        ...

    @abc.abstractmethod
    def execute(self, sql, values=None):
        ...

    @abc.abstractmethod
    def executemany(self, sql, values=None):
        ...

    @abc.abstractmethod
    def executescript(self, sqlscript):
        ...

    @abc.abstractmethod
    def get(self, *args, **kw):
        ...

    @abc.abstractmethod
    def get_all_hashes(self):
        ...

    @abc.abstractmethod
    def get_all_rows(self, table, sort_column=None, reverse=False):
        ...

    @abc.abstractmethod
    def get_blank_row(self, table):
        ...

    @abc.abstractmethod
    def get_column_base(self, table_name):
        ...

    @abc.abstractmethod
    def get_column_headings(self, table):
        ...

    @abc.abstractmethod
    def get_connection(self):
        ...

    @abc.abstractmethod
    def get_datestamp_column(self, table):
        ...

    @abc.abstractmethod
    def get_dirtied_count(self):
        ...

    @abc.abstractmethod
    def get_display_column(self, table_name):
        ...

    @abc.abstractmethod
    def get_highest_id(self, target_table):
        ...

    @abc.abstractmethod
    def get_id_column(self, table):
        ...

    @abc.abstractmethod
    def get_id_from_row(self, row_dict):
        ...

    @abc.abstractmethod
    def get_interlink_column(self, table1, table2, column_type):
        ...

    @abc.abstractmethod
    def get_interlinked_tables(self, table_name):
        ...

    @abc.abstractmethod
    def get_intralink_column(self, table, column_type):
        ...

    @abc.abstractmethod
    def get_linear_row_list(self, start_row):
        ...

    @abc.abstractmethod
    def get_link_column(self, table1, table2, column_type):
        ...

    @abc.abstractmethod
    def get_link_table_name(self, table1, table2):
        ...

    @abc.abstractmethod
    def get_parent_column(self, table_name):
        ...

    @abc.abstractmethod
    def get_random_row(self, table, row_dict=None, direct_access=False):
        ...

    @abc.abstractmethod
    def get_record_count(self, target_table):
        ...

    @abc.abstractmethod
    def get_relation_type(self, name: str) -> Optional[str]:
        ...

    @abc.abstractmethod
    def get_row_from_id(self, table, row_id):
        ...

    @abc.abstractmethod
    def get_scratch_column(self, table):
        ...

    @abc.abstractmethod
    def get_tables(self, force_refresh: bool=False):
        ...

    @abc.abstractmethod
    def get_tables_and_columns(self):
        ...

    @abc.abstractmethod
    def get_triggers(self):
        ...

    @abc.abstractmethod
    def get_uuid(self):
        ...

    @abc.abstractmethod
    def get_view_column_headings(self, view):
        ...

    @abc.abstractmethod
    def get_view_row_from_id(self, view, row_id):
        ...

    @abc.abstractmethod
    def identify_table_from_column(self, column_heading, error=True):
        ...

    @abc.abstractmethod
    def identify_table_from_row_dict(self, row_dict):
        ...

    @abc.abstractmethod
    def is_view(self, name: str) -> bool:
        ...

    @abc.abstractmethod
    def link_main_tables(self, primary_table, secondary_table, link_type, link_properties=None):
        ...

    @property
    @abc.abstractmethod
    def macros(self) -> MacrosAPI:
        ...

    @abc.abstractmethod
    def nullify_column(self, table, row_id, column):
        ...

    @abc.abstractmethod
    def read_metadata(self, field):
        ...

    @abc.abstractmethod
    def search(self, table, column, search_term):
        ...

    @abc.abstractmethod
    def set_custom_column_metadata(self, num, name=None, label=None, is_editable=None, display=None, in_table=None):
        ...

    @abc.abstractmethod
    def set_full_column(self, table):
        ...

    @abc.abstractmethod
    def set_macros(self, new_macros: MacrosAPI) -> None:
        ...

    @abc.abstractmethod
    def set_tree_ids(self, table):
        ...

    @abc.abstractmethod
    def set_uuid(self, new_force_value=None):
        ...

    @abc.abstractmethod
    def shell(self):
        ...

    @abc.abstractmethod
    def update_column(self, table, row_id, column, new_value):
        ...

    @abc.abstractmethod
    def update_columns(self, values_map, field=None, table=None):
        ...

    @abc.abstractmethod
    def update_custom_column(self, in_table, cc_name, value):
        ...

    @abc.abstractmethod
    def update_row(self, row_dict):
        ...

    @property
    @abc.abstractmethod
    def user_version(self):
        ...

    @abc.abstractmethod
    def walk(self, start_row):
        ...

    @abc.abstractmethod
    def write_metadata(self, field, value):
        ...

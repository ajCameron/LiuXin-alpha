"""Low-level database driver API contracts."""

from __future__ import annotations

from typing import Optional, Union, Any, Sequence, Iterator, Iterable

import abc

from LiuXin_alpha.databases.api.macros_api import MacrosAPI
from LiuXin_alpha.databases.api.driver_api.mixins.tree_mixin_api import DriverTreeMixinAPI
from LiuXin_alpha.databases.api.driver_api.mixins.metadata_mixin_api import DriverMetadataMixinAPI
from LiuXin_alpha.databases.api.driver_api.mixins.interlink_mixin_api import DriverInterlinkMixinAPI
from LiuXin_alpha.databases.api.driver_api.mixins.properties_mixin_api import DriverDatabasePropertiesMixinAPI
from LiuXin_alpha.databases.api.driver_api.mixins.names_mixin_api import DriverNamesMixinAPI
from LiuXin_alpha.databases.api.driver_api.mixins.intralink_mixin_api import DriverIntralinkMixinAPI
from LiuXin_alpha.databases.api.driver_api.mixins.search_mixin_api import DriverSearchMixinAPI
from LiuXin_alpha.databases.api.driver_api.mixins.add_mixin_api import DriverAddMixinAPI
from LiuXin_alpha.databases.api.driver_api.mixins.delete_mixin_api import DriverDeleteMixinAPI
from LiuXin_alpha.databases.api.driver_api.mixins.custom_columns_mixin_api import DriverCustomColumnsMixinAPI
from LiuXin_alpha.databases.api.driver_api.mixins.tables_mixin_api import DriverTablesMixinAPI
from LiuXin_alpha.databases.api.driver_api.mixins.triggers_mixin_api import DriverTriggersMixinAPI
from LiuXin_alpha.databases.api.driver_api.mixins.new_books_compressed_files_mixin_api import DriverNewBooksCompressedFilesMixinAPI
from LiuXin_alpha.databases.api.driver_api.mixins.view_mixin_api import DriverViewMixinAPI
from LiuXin_alpha.databases.api.driver_api.mixins.update_mixin_api import DriverUpdateMixinAPI


class DatabaseDriverAPI(
    DriverTreeMixinAPI,
    DriverMetadataMixinAPI,
    DriverInterlinkMixinAPI,
    DriverIntralinkMixinAPI,
    DriverCustomColumnsMixinAPI,
    DriverDatabasePropertiesMixinAPI,
    DriverNamesMixinAPI,
    DriverSearchMixinAPI,
    DriverAddMixinAPI,
    DriverDeleteMixinAPI,
    DriverTablesMixinAPI,
    DriverTriggersMixinAPI,
    DriverNewBooksCompressedFilesMixinAPI,
    DriverViewMixinAPI,
    DriverUpdateMixinAPI,
):
    """API contract for low-level SQL drivers."""

    @abc.abstractmethod
    def __init__(self, db_metadata, db=None, set_conn=True, dirty_records_queue=None) -> None:
        """
        Startup the driver.

        :param db_metadata:
        :param db: Link back to the database containing this driver.
        :param set_conn:
        :param dirty_records_queue:
        """

    @abc.abstractmethod
    def _canonicalise_table_name_for_cache(self, table: str) -> str:
        """
        Bring the table name into standard form for the cache.

        Used for compatibility and do some "common sense" name mangling.
        :param table:
        :return:
        """

    @abc.abstractmethod
    def _close_all_open_connections(self) -> str:
        """
        Used when shutting the database down to make sure there are no stale connections to the file on disk.

        :return:
        """

    @abc.abstractmethod
    def _coerce_db_value(self, value: Any, declared_type: Any) -> Any:
        """
        Attempt, via several fallbacks, to bring a database value into properly stored form.

        :param value:
        :param declared_type:
        :return:
        """

    @abc.abstractmethod
    def _coerce_untyped_value(self, value: Any) -> Any:
        """
        Attempt the same with an untyped value.

        :param value:
        :return:
        """

    @staticmethod
    @abc.abstractmethod
    def _normalize_declared_type(declared_type: Any) -> str:
        """
        Bring the declared datatype of a column into normal form.

        :param declared_type:
        :return:
        """

    @abc.abstractmethod
    def _register_open_connection(self, conn: Any) -> None:
        """
        Register a connection to the database for later cleanup.

        :param conn:
        :return:
        """

    @abc.abstractmethod
    def _row_to_dict(
            self,
            *,
            table: Optional[str] = None,
            headings: Sequence[Any],
            row: Sequence[Any]) -> dict[Any, Any]:
        """
        Take a raw row off the database, apply the adaptors and give you back a dict.

        :param table:
        :param headings:
        :param row:
        :return:
        """

    @staticmethod
    @abc.abstractmethod
    def _sanitize_embedded_nul_text(*, target_table: str, row_dict: dict) -> None:
        """
        Try and sanitize every entry that might be null in row_dict from a target table.

        :param target_table: The table the row is in.
        :param row_dict: The row off the database.
        :return None: The row_dict itself will be mutated.
        """

    # Todo: Probably not an API level function
    @classmethod
    @abc.abstractmethod
    def _sqlite_affinity(cls, declared_type: str) -> str:
        """
        Produces the SQLite type from the declared type.

        ("int" becomes "INTEGER" - hopefully).
        :param declared_type:
        :return:
        """

    # Todo: See below?
    @abc.abstractmethod
    def _zero_prop_cache(self) -> None:
        """
        Zero the properties cache - which will trigger a reload on next call.

        :return:
        """

    @abc.abstractmethod
    def call_after_table_changes(self) -> None:
        """
        Callback method to announce that a change has been made to a table.

        :return:
        """

    @abc.abstractmethod
    def close(self) -> None:
        """
        Close the driver and shut down all open connections.

        :return:
        """

    @abc.abstractmethod
    def direct_backup(self, path=None):
        """
        Backup the DatabasePing.

        :param path: The path to back up the database to - if none is provided, autogenerated
        :return:
        """

    @abc.abstractmethod
    def direct_create_new_database(self) -> None:
        """
        Call to create a new database with the given metadata.

        :return:
        """

    @abc.abstractmethod
    def direct_execute(self, sql: str, values: Optional[tuple[Any, ...]] = None) -> None:
        """
        Directly execute SQL on the database.

        USE WITH CARE! NO GUARDRAILS.
        :param sql:
        :param values:
        :return:
        """

    @abc.abstractmethod
    def direct_executemany(
            self,
            sql: str,
            values: Optional[tuple[tuple[Any, ...], ...]] = None) -> None:
        """
        Calls executemany on the conn to the database.

        :param sql:
        :param values:
        :return:
        """

    @abc.abstractmethod
    def direct_executescript(self, sqlscript: str) -> None:
        """
        Directly execute an SQL script on the database.

        :param sqlscript:
        :return:
        """

    @abc.abstractmethod
    def direct_get_db_unique_id(self) -> str:
        """
        Direct get a unique id for use on the database.

        :return:
        """

    @abc.abstractmethod
    def direct_get_null_row(self, table: str) -> dict[str, Any]:
        """
        Direct get a null row from the database.

        :param table:
        :return:
        """

    @abc.abstractmethod
    def direct_has_null_row(self, table: str) -> bool:
        """
        Check to see if the given table has the given null row.

        :param table:
        :return:
        """

    # Todo: This is a macro
    @abc.abstractmethod
    def direct_run_ta_update(self, ta_row_id):
        ...

    @abc.abstractmethod
    def direct_self_delete(self) -> None:
        """
        Delete the underlying database file.

        :return:
        """

    @abc.abstractmethod
    def direct_update_null_row(
            self,
            table: str,
            updates: Optional[dict[str, Any]] = None,
            **fields: Any) -> bool:
        """
        Update one of the null rows on the database.

        :param table:
        :param updates:
        :param fields:
        :return:
        """

    @abc.abstractmethod
    def dirty_record(self, table: str, table_id: int, reason: str) -> None:
        """
        Record that a record has been dirited.

        :param table:
        :param table_id:
        :param reason:
        :return:
        """

    @abc.abstractmethod
    def dump_and_restore(self, callback=lambda x: x, sql: Optional[str] = None) -> None:
        """
        Dump the database to SQL and then reload.

        :param callback:
        :param sql:
        :return:
        """

    @abc.abstractmethod
    def execute_sql(self, sql: str, values=None):
        """
        Execute SQL on the database.

        :param sql:
        :param values:
        :return:
        """

    @abc.abstractmethod
    def executescript(self, script: Union[str, list[str]]) -> None:
        """
        Execute SQL on the database as a script.

        :param script:
        :return:
        """

    @abc.abstractmethod
    def exists(self) -> bool:
        """
        Check to see if the database exists on disc.

        :return:
        """


    # Todo: Need a protocol for this
    # Todo: direct_*
    @abc.abstractmethod
    def get_connection(self) -> Any:
        """
        Get a connection to the database.

        :return:
        """

    @abc.abstractmethod
    def get_id_from_row_dict(self, row_dict: dict[str, Any]) -> int:
        """
        Return the id from the given row dict.

        :param row_dict:
        :return:
        """

    @abc.abstractmethod
    def identify_table_from_column(
            self,
            column_heading: str,
            headings_and_columns: Optional[dict[str, set[str]]] = None,
            print_error: bool = True) -> str:
        """
        Identify a table from a column.

        :param column_heading:
        :param headings_and_columns:
        :param print_error:
        :return:
        """

    @abc.abstractmethod
    def identify_table_from_row(self, row_dict: dict[str, Any]) -> str:
        """
        Take a row dict and identify a table from it.

        :param row_dict:
        :return:
        """

    @abc.abstractmethod
    def iterator_return(self,
                        stmt: str,
                        headings: Iterable[str],
                        table: Optional[str] = None,
                        bindings = None) -> Iterator[dict[str, Any]]:
        """
        Execute a statement and return the results of that statement.

        :param stmt:
        :param headings:
        :param table:
        :param bindings:
        :return:
        """


    @property
    @abc.abstractmethod
    def macros(self) -> "MacrosAPI":
        """
        Front end for the macros class which will be loaded into this driver.

        :return:
        """

    @abc.abstractmethod
    def make_scratch(self) -> None:
        """
        Trabsform the database into a scratch version of itself.

        :return:
        """

    @abc.abstractmethod
    def refresh(self, reconnect: bool = False) -> None:
        """
        Refresh the database - with optional reconnect.

        :param reconnect:
        :return:
        """

    @abc.abstractmethod
    def reopen(self) -> None:
        """
        Reopen the database connections.

        :return:
        """

    @abc.abstractmethod
    def shell(self) -> None:
        """
        Drop into a shell to execute SQL directly on the database.

        :return:
        """

    @abc.abstractmethod
    def simple_print_progress_handler(self) -> None:
        """
        A progress handler which just prints the occasional transaction count.

        :return:
        """

    @abc.abstractmethod
    def sql_dump(self):
        """
        Dump the database to SQL.

        :return:
        """

    @abc.abstractmethod
    def zero_prop_cache(self) -> None:
        """
        Zero out the internal properties cache for this class.

        :return:
        """

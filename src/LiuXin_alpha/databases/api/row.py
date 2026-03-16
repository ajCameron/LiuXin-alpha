"""
Base database API contracts.

APIs exist mostly for type checking and object verification.
LiuXin operates a plugin system and, as such, strict verification is a necessity.
As such, all relevant (and many irrelevant APIs) are explicitely declared for checking.
"""

from __future__ import annotations

import abc
import datetime

from typing import Any, Iterator, Optional, Union


# Todo: This does not need to be here.
# Todo: Rename to "DatabaseGeneratorAPI"


class RowAPI(abc.ABC):
    """
    API for a row off the database.

    This is the minimum contract that anything representing a row should meet.
    If you're employing a database WITHOUT something recognizable, in abstract, as a row...
    Well. Can you not?
    """

    @classmethod
    @abc.abstractmethod
    def from_idless_row_dict(cls,
                             database: "DatabaseAPI",
                             row_dict: dict[str, Any],
                             *,
                             table: Optional[str] = None,
                             read_only: bool = False,
                             reload_from_db: bool = True) -> "RowAPI":
        """
        Given a row without a valid ID column, starts up the class from it.

        :param database:
        :param row_dict:
        :param table:
        :param read_only:
        :param reload_from_db:
        :return:
        """

    def __init__(
            self,
            database: "DatabaseAPI",
            row_dict: Optional[dict[str, str]] = None,
            read_only: bool=False) -> None:
        """
        Startup the row with information off the database.

        :param database:
        :param row_dict:
        :param read_only:
        """
        self.db = database
        self.read_only = read_only
        self.int_row_dict = dict(row_dict or {})

    @abc.abstractmethod
    def __contains__(self, item: str) -> bool:
        """
        Checks if the given column is in this row.

        :param item:
        :return:
        """

    @abc.abstractmethod
    def __deepcopy__(self, memo: dict[Any, Any]) -> RowAPI:
        """
        Allows deep copying.

        :param memo:
        :return:
        """

    @abc.abstractmethod
    def __eq__(self, other: RowAPI) -> bool:
        """
        Implements equality.

        :param other:
        :return:
        """

    @abc.abstractmethod
    def __getitem__(self, item: str) -> Union[str, int, float, datetime.datetime]:
        """
        dict like interface.

        :param item:
        :return:
        """

    # Todo: There's probably a standard way to do this we should just use
    #       So that two different row implementations have the same hash for the same row
    @abc.abstractmethod
    def __hash__(self) -> str:
        """
        Used to uniquely identify a row.

        :return:
        """

    # Todo: Does this mean column headings, or values, or tuples? Not clear. Follow dict.
    #       Dict's print "column headings" - so do that
    @abc.abstractmethod
    def __iter__(self) -> Iterator[str]:
        """
        Iterate over the row.

        :return:
        """

    # Todo: Iterkeys and itervalues

    # Todo: Actually make this an ASCII rep of the object?
    @abc.abstractmethod
    def __repr__(self) -> str:
        """
        String representation of the row.

        :return:
        """

    @abc.abstractmethod
    def __setitem__(self, key: str, value: Union[str, int, float, datetime.datetime]) -> None:
        """
        Allows a dict-like set interface.

        :param key:
        :param value:
        :return:
        """

    @abc.abstractmethod
    def __str__(self) -> str:
        """
        String representation of the row.

        :return:
        """

    @abc.abstractmethod
    def __unicode__(self) -> str:
        """
        String representation of the row.

        :return:
        """

    # Todo: "name" should be "column_name
    # Todo: The return is going to be Literal strings - work out what they are
    @staticmethod
    @abc.abstractmethod
    def _best_effort_sqlite_object_type(database: "DatabaseAPI", name: str) -> Optional[str]:
        """
        Does its best to work out the SQLite type of the entry in the row.

        :param database:
        :param name:
        :return:
        """


    @abc.abstractmethod
    def to_jsonable(
        self,
        *,
        include_values: bool = True,
        max_cols: int = 50,
        max_text: int = 500,
        include_db_uuid: bool = True,
    ) -> dict[str, Any]:
        """
        Return a JSON-serializable representation of this Row.

        This is intentionally *lossy* and bounded so it is safe for logs and reports.
        Implementations should only return JSON primitives (plus lists/dicts thereof).

        :param include_values:
        :param max_cols:
        :param max_text:
        :param include_db_uuid:
        :return:
        """
        ...

    @abc.abstractmethod
    def ensure_row_has_id(self) -> None:
        """
        Check that this Row has an ID.

        :return:
        """

    @abc.abstractmethod
    def keys(self) -> None:
        """
        Dict like interface - returns the column headings for the table.

        :return:
        """

    # Todo: Corresponding factory method?
    @abc.abstractmethod
    def load_blank_row(self, table: Optional[str] = None) -> None:
        """
        Load a blank row from the internal database from the given table.

        :param table:
        :return:
        """

    @abc.abstractmethod
    def load_row_from_id(self, row_id: int=None, table: str=None) -> None:
        """
        Load a row from the internal database from the given table.

        :param row_id:
        :param table:
        :return:
        """

    @abc.abstractmethod
    def make_read_only(self) -> None:
        """
        Make the row read only.

        :return:
        """

    @abc.abstractmethod
    def no_sync(self) -> None:
        """
        Method used to replace sync when we're in read only mode.

        :return:
        """

    @abc.abstractmethod
    def refresh_db_properties(self) -> None:
        """
        Refresh the internally cached db properties of this row.

        :return:
        """

    # Todo: We can _probably_ tighten this Any - with abuse of protocols and overloads perhaps for all rows?
    @property
    @abc.abstractmethod
    def row_dict(self) -> dict[str, Any]:
        """
        Return this row as a dictionary of values.

        :return:
        """

    @abc.abstractmethod
    def sync(self) -> None:
        """
        Sync changes made to this row back to the database.

        :return:
        """

    @property
    @abc.abstractmethod
    def table(self) -> str:
        """
        What table is this row in?

        :return:
        """

    @abc.abstractmethod
    def update_and_check(self) -> None:
        """
        Preform a check and update pass - ensure the row is still valid.

        :return:
        """

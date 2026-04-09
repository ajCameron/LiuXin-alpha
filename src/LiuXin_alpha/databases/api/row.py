"""
Base database API contracts.

APIs exist mostly for type checking and object verification.
LiuXin operates a plugin system and, as such, strict verification is a necessity.
As such, all relevant (and many irrelevant APIs) are explicitely declared for checking.
"""

from __future__ import annotations

import abc
import datetime

from typing import Any, Iterator, Optional, Union, TYPE_CHECKING

from LiuXin_alpha.errors import NoSuchPropertyForLinkException

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api import DatabaseAPI
    from LiuXin_alpha.databases.db_types import MainTableName, InterlinkTableID



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
            *,
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

    @property
    @abc.abstractmethod
    def row_id(self) -> Optional[int]:
        """
        Return this row as a dictionary of values.

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


# Todo: ensure driver has a direct_get_allowed_types method.
class InterlinkRowAPI(RowAPI):
    """
    Specialized row for an interlink row.
    """
    src_table: MainTableName
    dst_table: MainTableName

    # Properties of the link
    _has_priority: bool = False
    priority_col: str

    _has_primary: bool = False
    primary_col: str

    _has_type: bool = False
    _allowed_types: list[str]
    type_col: str

    _has_origin: bool = False
    origin_col: str

    _has_policy: bool = False
    policy_col: str

    _has_data: bool = False
    data_col: str

    _has_index: bool = False
    index_col: str

    def __init__(
            self,
            database: "DatabaseAPI",
            row_dict: Optional[dict[str, str]] = None,
            *,
            read_only: bool=False,
            src_table: MainTableName,
            dst_table: MainTableName
    ) -> None:
        """
        Startup the row with information off the database.

        :param database:
        :param row_dict:
        :param read_only:
        :param src_table:
        :param dst_table:
        """
        super().__init__(database=database, row_dict=row_dict, read_only=read_only)

        self.src_table = src_table
        self.dst_table = dst_table

    @property
    def priority(self) -> int:
        """
        Return the priority of this link.

        :return:
        """
        if not self._has_priority:
            raise NoSuchPropertyForLinkException(f"{self.table} does not support priority.")
        return int(self.row_dict.get(self.priority_col))

    @priority.setter
    def priority(self, new_priority: int) -> None:
        """
        Return the priority of this link.

        :return:
        """
        if not self._has_priority:
            raise NoSuchPropertyForLinkException(f"{self.table} does not support priority.")
        self.row_dict[self.priority_col] = int(new_priority)

    @property
    def primary(self) -> bool:
        """
        Return if this link is primary or not

        :return:
        """
        if not self._has_primary:
            raise NoSuchPropertyForLinkException(f"{self.table} does not support primary.")
        return bool(self.row_dict.get(self.primary_col))

    @primary.setter
    def primary(self, new_primary: bool) -> None:
        """
        Set if the link is primary or not.

        :param new_primary:
        :return:
        """
        if not self._has_primary:
            raise NoSuchPropertyForLinkException(f"{self.table} does not support primary.")
        self.row_dict[self.primary_col] = new_primary

    @property
    @abc.abstractmethod
    def type(self) -> str:
        """
        Return the type of this link.

        :return:
        """
        if not self._has_type:
            raise NoSuchPropertyForLinkException(f"{self.table} does not support type.")
        return str(self.row_dict.get(self.type_col))

    @type.setter
    def type(self, new_type: str) -> None:
        """
        Try and set the type of this link.

        :param new_type:
        :return:
        """
        if not self._has_type:
            raise NoSuchPropertyForLinkException(f"{self.table} does not support type.")

        assert new_type in self._allowed_types, f"{new_type = } not in allowed_types = {self._allowed_types}"

    @property
    def origin(self) -> str:
        """
        Return the origin of this link.

        :return:
        """
        if not self._has_origin:
            raise NoSuchPropertyForLinkException(f"{self.table} does not support origin.")

        return str(self.row_dict.get(self.origin_col))

    @origin.setter
    def origin(self, new_origin: str) -> None:
        """
        Set the origin of this link.

        :param new_origin:
        :return:
        """
        if not self._has_origin:
            raise NoSuchPropertyForLinkException(f"{self.table} does not support origin.")

        self.row_dict[self.origin_col] = str(new_origin)

    @property
    def policy(self) -> str:
        """
        Return the policy of this link.

        :return:
        """
        if not self._has_policy:
            raise NoSuchPropertyForLinkException(f"{self.table} does not support policy.")

        return str(self.row_dict.get(self.policy_col))

    @policy.setter
    def policy(self, new_policy: str) -> None:
        """
        Set the policy for the link row - if supported.

        :param new_policy:
        :return:
        """
        if not self._has_policy:
            raise NoSuchPropertyForLinkException(f"{self.table} does not support policy.")

        self.row_dict[self.policy_col] = str(new_policy)

    @property
    def data(self) -> str:
        """
        Return the data of this link.

        :return:
        """
        if not self._has_data:
            raise NoSuchPropertyForLinkException(f"{self.table} does not support data.")

        return str(self.row_dict.get(self.data_col))

    @data.setter
    def data(self, new_data: str) -> None:
        """
        Set the data for this link.

        :param new_data:
        :return:
        """
        if not self._has_data:
            raise NoSuchPropertyForLinkException(f"{self.table} does not support data.")

        self.row_dict[self.data_col] = str(new_data)

    @property
    def index(self) -> str:
        """
        Return the index of this link.

        :return:
        """
        if not self._has_index:
            raise NoSuchPropertyForLinkException(f"{self.table} does not support index.")

        return str(self.row_dict.get(self.index_col))

    @index.setter
    def index(self, new_index: str) -> None:
        """
        Set the index of this link.

        :param new_index:
        :return:
        """
        if not self._has_index:
            raise NoSuchPropertyForLinkException(f"{self.table} does not support index.")

        self.row_dict[self.index_col] = str(new_index)

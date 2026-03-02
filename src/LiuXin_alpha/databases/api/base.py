"""Base database API contracts."""

from __future__ import annotations

import abc
import datetime
import sqlite3

from typing import Any, Iterable, Iterator, Optional, Union

class DatabaseBuilderAPI(abc.ABC):
    """API for the fundamental database builder class."""

    @abc.abstractmethod
    def __init__(self, conn: Any) -> None:
        """
        Startup the databsse builder class.

        :param conn:
        """

    @abc.abstractmethod
    def _bad_link_type_error(self, link_type: str) -> str:
        """
        The link type is not valid for the current table.

        :param link_type:
        :return:
        """

    @abc.abstractmethod
    def _build_allowed_types_table_interlink(self, for_table, allowed_types):
        ...

    @abc.abstractmethod
    def _build_interlink_table_sqlite(self, table1: str, table2: str, requested_cols: Optional[Union[str, list[str]]]=None, allowed_types: Optional[Iterable[str]]=None, override_restriction_sql: Optional[str]=None) -> list[str]:
        ...

    @staticmethod
    @abc.abstractmethod
    def _canonicalize_link_type(link_type: str) -> str:
        ...

    @abc.abstractmethod
    def _get_direct_link_main_tables_sqlite(self, primary_table: str, secondary_table: str, link_type: str='many_many', requested_cols: str='all', index_both: bool=True, allowed_types: Optional[Iterable[str]]=None, one_link_with_one_type: bool=True, override_restriction_sql: Optional[str]=None, nullable_fks: bool=True) -> tuple[list[str], Union[str, LiteralString]]:
        ...

    @staticmethod
    @abc.abstractmethod
    def _get_link_table_name_col_name(primary_table: str, secondary_table: str) -> tuple[str, str]:
        ...

    @abc.abstractmethod
    def _lock_table_read_only(self, table: str, *, message: str) -> None:
        ...

    @abc.abstractmethod
    def apply_interlink_constraints_from_spec(self) -> None:
        ...

    @abc.abstractmethod
    def build_allowed_types_table_interlink(self, for_table: str, allowed_types: Optional[Iterable[str]]=None) -> list[str]:
        ...

    @abc.abstractmethod
    def build_allowed_types_table_intralink(self, for_table: str, allowed_types: Optional[Iterable[str]]=None) -> list[str]:
        ...

    @abc.abstractmethod
    def build_interlink_table_sqlite(self, table1: str, table2: str, requested_cols: Optional[Union[str, Iterable[str]]]=None, allowed_types: Optional[Iterable[str]]=None, nullable_fks: bool=True) -> list[str]:
        ...

    @abc.abstractmethod
    def build_intralink_table_sqlite(self, name: str, **kwargs: Any) -> list[str]:
        ...

    @abc.abstractmethod
    def create_aggregate_tables(self) -> None:
        ...

    @abc.abstractmethod
    def create_interlink_table(self, table1: str, table2: str, connection: sqlite3.Connection) -> None:
        ...

    @abc.abstractmethod
    def create_interlink_types_reference_table(self, interlink_table_name: str, interlink_column_base: str, allowed_types: list[str], connection: sqlite3.Connection) -> None:
        ...

    @abc.abstractmethod
    def create_intralink_table(self, table_name: str, connection: sqlite3.Connection) -> None:
        ...

    @abc.abstractmethod
    def create_main_tables(self) -> None:
        ...

    @abc.abstractmethod
    def create_main_triggers(self) -> None:
        ...

    @staticmethod
    @abc.abstractmethod
    def direct_get_column_base(table_name: str) -> str:
        ...

    @abc.abstractmethod
    def direct_get_tables(self) -> set[str]:
        ...

    @abc.abstractmethod
    def direct_link_main_tables(self, primary_table, secondary_table, link_type='many_many', requested_cols='all', index_both=True, allowed_types=None, override_restriction_sql=None, nullable_fks: bool=True):
        ...

    @abc.abstractmethod
    def extract_main_tables(self, interlink_request: str) -> Optional[list[str]]:
        ...

    @staticmethod
    @abc.abstractmethod
    def get_allowed_types_table_name(for_table: str) -> str:
        ...

    @abc.abstractmethod
    def get_allowed_types_table_name_intralinks(self, for_table: str) -> str:
        ...

    @abc.abstractmethod
    def get_interlink_constraint(self, link_pair: list[str]) -> dict[str, str]:
        ...

    @staticmethod
    @abc.abstractmethod
    def get_interlink_name(link_pair: list[str]) -> str:
        ...

    @staticmethod
    @abc.abstractmethod
    def get_interlink_table_name(table1: str, table2: str) -> tuple[str, str]:
        ...

    @abc.abstractmethod
    def get_requested_interlink_tables(self) -> set[tuple[str, str]]:
        ...

    @abc.abstractmethod
    def get_requested_intralink_tables(self) -> set[str]:
        ...

    @abc.abstractmethod
    def lock_constant_tables(self) -> None:
        ...

    @abc.abstractmethod
    def match_to_table_name(self, candidate_name: str) -> Optional[str]:
        ...

    @abc.abstractmethod
    def materialize_interlink_type_reference_tables(self) -> None:
        ...

    @abc.abstractmethod
    def run(self) -> None:
        ...

    @abc.abstractmethod
    def sanity_check_interlink_inputs(self) -> None:
        ...

    @abc.abstractmethod
    def sanity_check_intralink_inputs(self) -> None:
        ...

    @abc.abstractmethod
    def seed_constant_tables(self) -> None:
        ...

    @abc.abstractmethod
    def seed_languages_table(self) -> None:
        ...

    @abc.abstractmethod
    def set_database_version(self) -> None:
        ...

    @abc.abstractmethod
    def validate_allowed_type_val_dict(self) -> None:
        ...

    @abc.abstractmethod
    def validate_interlink_table_column_requests(self) -> None:
        ...

    @abc.abstractmethod
    def validate_interlink_table_constraints(self) -> None:
        ...


class RowAPI(abc.ABC):
    """API for a row off the database."""

    def __init__(self, database: "DatabaseAPI", row_dict: Optional[dict[str, str]]=None, read_only: bool=False) -> None:
        self.db = database
        self.read_only = read_only
        self.int_row_dict = dict(row_dict or {})

    @abc.abstractmethod
    def __contains__(self, item: str) -> bool:
        ...

    @abc.abstractmethod
    def __deepcopy__(self, memo: dict[Any, Any]) -> RowAPI:
        ...

    @abc.abstractmethod
    def __eq__(self, other: RowAPI) -> bool:
        ...

    @abc.abstractmethod
    def __getitem__(self, item: str) -> Union[str, int, float, datetime.datetime]:
        ...

    @abc.abstractmethod
    def __hash__(self) -> int:
        ...

    @abc.abstractmethod
    def __iter__(self) -> Iterator[str]:
        ...

    @abc.abstractmethod
    def __repr__(self):
        ...

    @abc.abstractmethod
    def __setitem__(self, key: str, value: Union[str, int, float, datetime.datetime]) -> None:
        ...

    @abc.abstractmethod
    def __str__(self):
        ...

    @abc.abstractmethod
    def __unicode__(self):
        ...

    @staticmethod
    @abc.abstractmethod
    def _best_effort_sqlite_object_type(database: "DatabaseAPI", name: str) -> Optional[str]:
        ...


    @abc.abstractmethod
    def to_jsonable(
        self,
        *,
        include_values: bool = True,
        max_cols: int = 50,
        max_text: int = 500,
        include_db_uuid: bool = True,
    ) -> dict[str, Any]:
        """Return a JSON-serializable representation of this Row.

        This is intentionally *lossy* and bounded so it is safe for logs and reports.
        Implementations should only return JSON primitives (plus lists/dicts thereof).
        """
        ...

    @abc.abstractmethod
    def ensure_row_has_id(self) -> None:
        ...

    @classmethod
    @abc.abstractmethod
    def from_idless_row_dict(cls, database: "DatabaseAPI", row_dict: dict[str, Any], *, table: Optional[str]=None, read_only: bool=False, reload_from_db: bool=True) -> 'Row':
        ...

    @abc.abstractmethod
    def keys(self) -> None:
        ...

    @abc.abstractmethod
    def load_blank_row(self, table: Optional[str] = None) -> None:
        ...

    @abc.abstractmethod
    def load_row_from_id(self, row_id: int=None, table: str=None) -> None:
        ...

    @abc.abstractmethod
    def make_read_only(self):
        ...

    @abc.abstractmethod
    def no_sync(self) -> None:
        ...

    @abc.abstractmethod
    def refresh_db_properties(self) -> None:
        ...

    @property
    @abc.abstractmethod
    def row_dict(self):
        ...

    @abc.abstractmethod
    def sync(self) -> None:
        ...

    @property
    @abc.abstractmethod
    def table(self) -> str:
        ...

    @abc.abstractmethod
    def update_and_check(self) -> None:
        ...

"""
API contracts for Database mixin surfaces.

Modularized, as these mixins are used in multiple different places.
"""

from __future__ import annotations

import abc

from typing import Any, Iterable, Iterator, Optional, Union

from LiuXin_alpha.databases.api.row import RowAPI


class DatabaseRatingMixinAPI(abc.ABC):
    """
    Typed API for ``DatabaseRatingMixin``.
    """

    @abc.abstractmethod
    def check_rating_table(self) -> None:
        """
        Ensure canonical rows exist in ``ratings`` and repair malformed entries.

        :return:
        """


class DatabaseNullRowsMixinAPI(abc.ABC):
    """
    Typed API for ``DatabaseNullRowsMixin``.
    """

    @abc.abstractmethod
    def ensure_null_rows(self) -> None:
        """
        Ensure required sentinel/null rows exist for schema-specific tables.

        :return:
        """


class DatabaseMetadataMixinAPI(abc.ABC):
    """
    Typed API for ``DatabaseMetadataMixin``.
    """

    @property
    @abc.abstractmethod
    def uuid(self) -> str:
        """
        Return the uuid for the database.

        :return:
        """

    @uuid.setter
    @abc.abstractmethod
    def uuid(self, value: str) -> None:
        """
        Set the uuid for the database.

        :param value:
        :return:
        """

    @property
    @abc.abstractmethod
    def library_id(self) -> str:
        """
        Get the library id for the database.

        :return:
        """

    @library_id.setter
    @abc.abstractmethod
    def library_id(self, value: str) -> None:
        """
        Set the library id for the database.

        :param value:
        :return:
        """

    @property
    @abc.abstractmethod
    def database_version(self) -> str:
        """
        Get the current database version.

        :return:
        """

    @database_version.setter
    @abc.abstractmethod
    def database_version(self, value: str) -> None:
        """
        Set the database version for the database.

        :param value:
        :return:
        """

    @abc.abstractmethod
    def get_tables(self, force_refresh: bool = False) -> Iterable[str]:
        """
        Get all the tables in the database.

        :param force_refresh: Bypass and refresh the cache
        :return:
        """

    @abc.abstractmethod
    def get_column_headings(self, table: str) -> list[str]:
        """
        Get the column headings for the table of the database.

        :param table:
        :return:
        """

    @abc.abstractmethod
    def get_view_column_headings(self, view: str) -> list[str]:
        """
        Get the column headings for a view.

        :param view:
        :return:
        """

    @abc.abstractmethod
    def get_tables_and_columns(self) -> dict[str, list[str]]:
        """
        Get all the tables and columns for the database.

        :return:
        """

    @abc.abstractmethod
    def get_record_count(self, target_table: str) -> int:
        """
        Get the raw record count for the table.

        :param target_table:
        :return:
        """

    @abc.abstractmethod
    def get_max(self, column: str) -> Any:
        """
        Return the max value for the given column.

        :param column:
        :return:
        """

    @abc.abstractmethod
    def get_min(self, column: str) -> Any:
        """
        Return the min value for the given column.

        :param column:
        :return:
        """

    @abc.abstractmethod
    def row_counts(self) -> str:
        """
        Get the raw record count for the table.

        :return:
        """

class DatabaseDirtiedRecordsMixinAPI(abc.ABC):
    """
    Typed API for ``DatabaseDirtiedRecordsMixin``.

    This defines the interface for dealing with the dirtied records on the database.
    """

    @property
    @abc.abstractmethod
    def metadata_dirtied_table(self) -> str:
        """
        Return the name for the metadata dirty table.

        :return:
        """

    @abc.abstractmethod
    def get_dirtied_count(self, *, include_persisted: bool = False) -> int:
        """
        Get the dirtied record count for the database.

        :param include_persisted:
        :return:
        """

    @abc.abstractmethod
    def dirty_record(self, table: str, row_id: int, reason: str = "") -> None:
        """
        Note that a record in a given table has been dirtied.

        :param table:
        :param row_id:
        :param reason:
        :return:
        """

    # Todo: Counts per table would be good/interesting?

    @abc.abstractmethod
    def get_persisted_dirtied_count(self) -> int:
        """
        Get the record count for the records marked persistently dirtied.

        :return:
        """

    @abc.abstractmethod
    def persist_dirtied_records(self, *, limit: Optional[int] = None) -> int:
        """Drain dirtied-record events from the in-memory queue into ``metadata_dirtied_table``.

        This is intended to be called from a single controlling thread (e.g. a maintenance loop) to avoid
        cross-thread SQLite connection use. Returns the number of persisted events.

        :param limit:
        :return:
        """

    @abc.abstractmethod
    def get_write_telemetry_snapshot(self, *, recent_limit: int = 8) -> dict[str, Any]:
        """
        Return a lightweight live snapshot of observed database write activity.

        :param recent_limit:
        :return:
        """


class DatabaseSearchMixinAPI(abc.ABC):
    """
    Typed API for ``DatabaseSearchMixin``.

    API for preforming searches on the database.
    """

    @abc.abstractmethod
    def search(self, table: str, column: str, search_term: Any) -> list["RowAPI"]:
        """
        Search in a single column in a single table.

        :param table:
        :param column:
        :param search_term:
        :return:
        """

    @abc.abstractmethod
    def multi_column_search(self, search_index: Any, iterator_return: bool = False) -> Any:
        """
        Search in multiple columns in a single table.

        :param search_index:
        :param iterator_return:
        :return:
        """

    @abc.abstractmethod
    def get_unique(self, target_column: str) -> Any:
        """
        Return all the unique values for the given column.

        :param target_column:
        :return:
        """

    @abc.abstractmethod
    def get_values_set(self, target_column: str, iterator_return: bool = False) -> Any:
        """
        Return a set of values for the given column.

        :param target_column:
        :param iterator_return:
        :return:
        """

    @abc.abstractmethod
    def get_row_from_id(self, table: str, row_id: int) -> Optional["RowAPI"]:
        """
        Get a row from the given table by id.

        :param table:
        :param row_id:
        :return:
        """

    @abc.abstractmethod
    def get_random_row(self, table: str) -> "RowAPI":
        """
        Get a random row off the database.

        :param table:
        :return:
        """

    # Todo: Split this down into iterator and list

    @abc.abstractmethod
    def get_all_rows(
        self,
        table: str,
        iterator_return: bool = True,
        sort_column: Optional[str] = None,
        reverse: bool = False,
    ) -> Union[list["RowAPI"], Iterator["RowAPI"]]:
        """
        Get all rows from the database.

        :param table:
        :param iterator_return:
        :param sort_column:
        :param reverse:
        :return:
        """

    # Todo: Add chunk size
    @abc.abstractmethod
    def chunk_iterator(self, column: str, target_table: Optional[str] = None) -> Iterator[list["RowAPI"]]:
        """
        Iterate over all rows in the database in chunks.

        :param column:
        :param target_table:
        :return:
        """


class DatabaseInterlinkRowsMixinAPI(abc.ABC):
    """Typed API for ``DatabaseInterlinkRowsMixin``."""

    @abc.abstractmethod
    def get_interlink_row(
        self,
        primary_row: "RowAPI",
        secondary_row: "RowAPI",
        onelink: bool = True,
    ) -> Optional[Union["RowAPI", list["RowAPI"]]]:
        """
        Return the interlink row - if there is one - linking the two given rows.

        :param primary_row:
        :param secondary_row:
        :param onelink:
        :return:
        """

    # Todo: get_interlink_rows and get_interlinked_rows seem similar
    @abc.abstractmethod
    def get_interlink_rows(self, primary_row: "RowAPI", secondary_table: str) -> list["RowAPI"]:
        """
        Return all the interlink rows between the primary row and an entire secondary table.

        :param primary_row:
        :param secondary_table:
        :return:
        """

    @abc.abstractmethod
    def get_interlinked_rows(
        self,
        primary_row: Optional["RowAPI"] = None,
        secondary_table: Optional[str] = None,
        type_filter: Optional[str] = None,
        **kwargs: Any,
    ) -> list["RowAPI"]:
        """
        Get the interlinked rows between the primary row and an entire secondary table.

        :param primary_row:
        :param secondary_table:
        :param type_filter:
        :param kwargs:
        :return:
        """

    @abc.abstractmethod
    def get_interlink_values(self, target_row: "RowAPI", secondary_column: str) -> set[Any]:
        """
        Get the interlink values from the secondary table for the target_row.

        :param target_row:
        :param secondary_column:
        :return:
        """

    @abc.abstractmethod
    def interlink_rows(
        self,
        primary_row: "RowAPI",
        secondary_row: "RowAPI",
        priority: Optional[Union[int, float, str]] = "highest",
        type: Optional[str] = None,
        **col_value_pairs: Any,
    ) -> "RowAPI":
        ...

    @abc.abstractmethod
    def dupe_interlinks(
        self,
        src_row: "RowAPI",
        dst_row: "RowAPI",
        swap_priorities: bool = False,
        restrict_to_tables: Optional[Iterable[str]] = None,
        force_priority: Optional[Union[int, float, str]] = None,
    ) -> None:
        """
        Dupe the interlinks from one row onto another.

        :param src_row:
        :param dst_row:
        :param swap_priorities:
        :param restrict_to_tables:
        :param force_priority:
        :return:
        """

    @abc.abstractmethod
    def swap_priorities(self, src_row: "RowAPI", dst_row_1: "RowAPI", dst_row_2: "RowAPI") -> None:
        """
        Swap the priorities of two rows linked to the src_row.

        :param src_row:
        :param dst_row_1:
        :param dst_row_2:
        :return:
        """

    @abc.abstractmethod
    def update_interlink(
        self,
        primary_row: "RowAPI",
        secondary_row: "RowAPI",
        priority: Optional[Union[int, float, str]] = "unchanged",
        **col_value_pairs: Any,
    ) -> "RowAPI":
        """
        Update the interlink values of the link between two rows.

        :param primary_row:
        :param secondary_row:
        :param priority:
        :param col_value_pairs:
        :return:
        """

    @abc.abstractmethod
    def update_interlink_priority(
            self,
            primary_row: "RowAPI",
            secondary_table: str,
            ordered_ids: Iterable[int]) -> None:
        """
        Update the priority of an interlink connecting two rows.

        :param primary_row:
        :param secondary_table:
        :param ordered_ids:
        :return:
        """

    @abc.abstractmethod
    def unlink_interlink(self, primary_row: "RowAPI", secondary_row: "RowAPI") -> None:
        """
        Unlink two interlinked rows.

        :param primary_row:
        :param secondary_row:
        :return:
        """

    @abc.abstractmethod
    def unlink_all(self, primary_row: "RowAPI", secondary_table: str, type_filter: Optional[str] = None) -> None:
        """
        Unlink all rows connecting the primary row and the secondary table.

        :param primary_row:
        :param secondary_table:
        :param type_filter:
        :return:
        """


class DatabaseIntralinkRowsMixinAPI(abc.ABC):
    """
    Typed API for ``DatabaseIntralinkRowsMixin``.

    This is responsible for dealing with intalink rows - rows which link a table back to itself.
    """

    @abc.abstractmethod
    def intralink_rows(self, primary_row: "RowAPI", secondary_row: "RowAPI", link_type: str) -> "RowAPI":
        """
        Make a link between the primary row and the secondary rows with given type.

        :param primary_row:
        :param secondary_row:
        :param link_type:
        :return:
        """

    @abc.abstractmethod
    def get_intralink_row(self, primary_row: "RowAPI", secondary_row: "RowAPI") -> Optional["RowAPI"]:
        """
        Get the interlink row - if any - linking the primary row and the secondary row.

        :param primary_row:
        :param secondary_row:
        :return:
        """

    # Todo: Merge with the method below
    @abc.abstractmethod
    def get_intralink_rows(
        self,
        row: "RowAPI",
        primary: bool = True,
        secondary: bool = True,
        link_type_filter: Optional[str] = None,
    ) -> list["RowAPI"]:
        """
        Get all rows intralinked to the primary row.

        :param row:
        :param primary:
        :param secondary:
        :param link_type_filter:
        :return:
        """

    @abc.abstractmethod
    def get_intralinked_rows(
        self,
        primary_row: Optional["RowAPI"],
        secondary_row: Optional["RowAPI"],
    ) -> list["RowAPI"]:
        """
        Get the intralink rows with a type filter.

        :param primary_row:
        :param secondary_row:
        :return:
        """

    @abc.abstractmethod
    def unlinked_intralink(self, primary_row: Optional["RowAPI"], secondary_row: Optional["RowAPI"]) -> None:
        """
        Unlink an interlink between the primary row and the secondary row - if there is one.

        :param primary_row:
        :param secondary_row:
        :return:
        """

    # Todo: unlink_all_intralinks


class DatabaseTreeMixinAPI(abc.ABC):
    """
    Typed API for ``DatabaseTreeMixin``.

    Methods for dealing with trees in the database.
    """

    @abc.abstractmethod
    def get_root_row(self, start_row: "RowAPI") -> "RowAPI":
        """
        Get the root row of a tree the start_row is in.

        :param start_row:
        :return:
        """

    # Todo: Replace this with "get_root_row"
    @abc.abstractmethod
    def get_root_series(self, start_row: "RowAPI") -> "RowAPI":
        """
        Get the root series of the series tree we're in.

        :param start_row:
        :return:
        """

    @abc.abstractmethod
    def get_children(self, src_row: "RowAPI") -> list["RowAPI"]:
        """
        Get all the child rows of the tree we're in.

        :param src_row:
        :return:
        """

    @abc.abstractmethod
    def get_linear_row_list(self, start_row: "RowAPI") -> list["RowAPI"]:
        """
        Get all the rows in the tree we're in as a list.

        :param start_row:
        :return:
        """

    @abc.abstractmethod
    def get_all_tree_rows(self, start_row: "RowAPI", back_iterate: bool = True) -> set["RowAPI"]:
        """
        Get all the rows in the tree the start_row is in - from the start row down.

        :param start_row:
        :param back_iterate:
        :return:
        """

    @abc.abstractmethod
    def walk(self, start_row: "RowAPI") -> Iterator["RowAPI"]:
        """
        Walk a tree from the start_row down.

        :param start_row:
        :return:
        """

    @abc.abstractmethod
    def search_tree(self, root_row: "RowAPI", for_ids: Iterable[int]) -> set[int]:
        """
        Search a tree rooted in the root for any instances of the given ids.

        :param root_row:
        :param for_ids:
        :return:
        """

    @abc.abstractmethod
    def nest_rows(self, parent_row: "RowAPI", child_rows: Union["RowAPI", Iterable["RowAPI"]]) -> None:
        """
        Place the child rows under the parent row.

        :param parent_row:
        :param child_rows:
        :return:
        """

    # Todo: The parent_row should be the root_row
    @abc.abstractmethod
    def delete_tree(self, parent_row: "RowAPI") -> None:
        """
        Delete an entire tree

        :param parent_row:
        :return:
        """


class DatabaseTriggerHelpersAPI(abc.ABC):
    """
    Typed API for trigger helper passthroughs exposed by ``Database``.

    Helper to deal with triggers.
    """

    @abc.abstractmethod
    def get_triggers(self) -> list[str]:
        """
        Return all the triggers on the database.

        :return:
        """

    @abc.abstractmethod
    def drop_triggers(self, triggers: list[str]) -> bool:
        """
        Drop the given triggers from the database.

        :param triggers:
        :return:
        """

    @abc.abstractmethod
    def drop_all_triggers(self) -> Any:
        """
        Drop all triggers from the database.

        :return:
        """

"""API contracts for Database mixin surfaces."""

from __future__ import annotations

import abc

from typing import Any, Iterable, Iterator, Optional, Union

class DatabaseRatingMixinAPI(abc.ABC):
    """Typed API for ``DatabaseRatingMixin``."""

    @abc.abstractmethod
    def check_rating_table(self) -> None:
        """Ensure canonical rows exist in ``ratings`` and repair malformed entries."""

class DatabaseNullRowsMixinAPI(abc.ABC):
    """Typed API for ``DatabaseNullRowsMixin``."""

    @abc.abstractmethod
    def ensure_null_rows(self) -> None:
        """Ensure required sentinel/null rows exist for schema-specific tables."""

class DatabaseMetadataMixinAPI(abc.ABC):
    """Typed API for ``DatabaseMetadataMixin``."""

    @property
    @abc.abstractmethod
    def uuid(self) -> str:
        ...

    @uuid.setter
    @abc.abstractmethod
    def uuid(self, value: str) -> None:
        ...

    @property
    @abc.abstractmethod
    def library_id(self) -> str:
        ...

    @library_id.setter
    @abc.abstractmethod
    def library_id(self, value: str) -> None:
        ...

    @property
    @abc.abstractmethod
    def database_version(self) -> str:
        ...

    @database_version.setter
    @abc.abstractmethod
    def database_version(self, value: str) -> None:
        ...

    @abc.abstractmethod
    def get_tables(self, force_refresh: bool = False) -> Iterable[str]:
        ...

    @abc.abstractmethod
    def get_column_headings(self, table: str) -> list[str]:
        ...

    @abc.abstractmethod
    def get_view_column_headings(self, view: str) -> list[str]:
        ...

    @abc.abstractmethod
    def get_tables_and_columns(self) -> dict[str, list[str]]:
        ...

    @abc.abstractmethod
    def get_record_count(self, target_table: str) -> int:
        ...

    @abc.abstractmethod
    def get_max(self, column: str) -> Any:
        ...

    @abc.abstractmethod
    def get_min(self, column: str) -> Any:
        ...

    @abc.abstractmethod
    def row_counts(self) -> str:
        ...

class DatabaseDirtiedRecordsMixinAPI(abc.ABC):
    """Typed API for ``DatabaseDirtiedRecordsMixin``."""

    @property
    @abc.abstractmethod
    def metadata_dirtied_table(self) -> str:
        ...

    @abc.abstractmethod
    def get_dirtied_count(self, *, include_persisted: bool = False) -> int:
        ...

    @abc.abstractmethod
    def dirty_record(self, table: str, row_id: int, reason: str = "") -> None:
        ...

    @abc.abstractmethod
    def get_persisted_dirtied_count(self) -> int:
        ...

    @abc.abstractmethod
    def persist_dirtied_records(self, *, limit: Optional[int] = None) -> int:
        ...

class DatabaseSearchMixinAPI(abc.ABC):
    """Typed API for ``DatabaseSearchMixin``."""

    @abc.abstractmethod
    def search(self, table: str, column: str, search_term: Any) -> list["RowAPI"]:
        ...

    @abc.abstractmethod
    def multi_column_search(self, search_index: Any, iterator_return: bool = False) -> Any:
        ...

    @abc.abstractmethod
    def get_unique(self, target_column: str) -> Any:
        ...

    @abc.abstractmethod
    def get_values_set(self, target_column: str, iterator_return: bool = False) -> Any:
        ...

    @abc.abstractmethod
    def get_row_from_id(self, table: str, row_id: int) -> Optional["RowAPI"]:
        ...

    @abc.abstractmethod
    def get_random_row(self, table: str) -> "RowAPI":
        ...

    @abc.abstractmethod
    def get_all_rows(
        self,
        table: str,
        iterator_return: bool = True,
        sort_column: Optional[str] = None,
        reverse: bool = False,
    ) -> Union[list["RowAPI"], Iterator["RowAPI"]]:
        ...

    @abc.abstractmethod
    def chunk_iterator(self, column: str, target_table: Optional[str] = None) -> Iterator[list["RowAPI"]]:
        ...

class DatabaseInterlinkRowsMixinAPI(abc.ABC):
    """Typed API for ``DatabaseInterlinkRowsMixin``."""

    @abc.abstractmethod
    def get_interlink_row(
        self,
        primary_row: "RowAPI",
        secondary_row: "RowAPI",
        onelink: bool = True,
    ) -> Optional[Union["RowAPI", list["RowAPI"]]]:
        ...

    @abc.abstractmethod
    def get_interlink_rows(self, primary_row: "RowAPI", secondary_table: str) -> list["RowAPI"]:
        ...

    @abc.abstractmethod
    def get_interlinked_rows(
        self,
        target_row: Optional["RowAPI"] = None,
        secondary_table: Optional[str] = None,
        type_filter: Optional[str] = None,
        **kwargs: Any,
    ) -> list["RowAPI"]:
        ...

    @abc.abstractmethod
    def get_interlink_values(self, target_row: "RowAPI", secondary_column: str) -> set[Any]:
        ...

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
        ...

    @abc.abstractmethod
    def swap_priorities(self, src_row: "RowAPI", dst_row_1: "RowAPI", dst_row_2: "RowAPI") -> None:
        ...

    @abc.abstractmethod
    def update_interlink(
        self,
        primary_row: "RowAPI",
        secondary_row: "RowAPI",
        priority: Optional[Union[int, float, str]] = "unchanged",
        **col_value_pairs: Any,
    ) -> "RowAPI":
        ...

    @abc.abstractmethod
    def update_interlink_priority(self, primary_row: "RowAPI", secondary_table: str, ordered_ids: Iterable[int]) -> None:
        ...

    @abc.abstractmethod
    def unlink_interlink(self, primary_row: "RowAPI", secondary_row: "RowAPI") -> None:
        ...

    @abc.abstractmethod
    def unlink_all(self, primary_row: "RowAPI", secondary_table: str, type_filter: Optional[str] = None) -> None:
        ...

class DatabaseIntralinkRowsMixinAPI(abc.ABC):
    """Typed API for ``DatabaseIntralinkRowsMixin``."""

    @abc.abstractmethod
    def intralink_rows(self, primary_row: "RowAPI", secondary_row: "RowAPI", link_type: str) -> "RowAPI":
        ...

    @abc.abstractmethod
    def get_intralink_row(self, primary_row: "RowAPI", secondary_row: "RowAPI") -> Optional["RowAPI"]:
        ...

    @abc.abstractmethod
    def get_intralink_rows(
        self,
        row: "RowAPI",
        primary: bool = True,
        secondary: bool = True,
        link_type_filter: Optional[str] = None,
    ) -> list["RowAPI"]:
        ...

    @abc.abstractmethod
    def get_intralinked_rows(
        self,
        primary_row: Optional["RowAPI"],
        secondary_row: Optional["RowAPI"],
    ) -> list["RowAPI"]:
        ...

    @abc.abstractmethod
    def unlinked_intralink(self, primary_row: Optional["RowAPI"], secondary_row: Optional["RowAPI"]) -> None:
        ...

class DatabaseTreeMixinAPI(abc.ABC):
    """Typed API for ``DatabaseTreeMixin``."""

    @abc.abstractmethod
    def get_root_row(self, start_row: "RowAPI") -> "RowAPI":
        ...

    @abc.abstractmethod
    def get_root_series(self, start_row: "RowAPI") -> "RowAPI":
        ...

    @abc.abstractmethod
    def get_children(self, src_row: "RowAPI") -> list["RowAPI"]:
        ...

    @abc.abstractmethod
    def get_linear_row_list(self, start_row: "RowAPI") -> list["RowAPI"]:
        ...

    @abc.abstractmethod
    def get_all_tree_rows(self, start_row: "RowAPI", back_iterate: bool = True) -> set["RowAPI"]:
        ...

    @abc.abstractmethod
    def walk(self, start_row: "RowAPI") -> Iterator["RowAPI"]:
        ...

    @abc.abstractmethod
    def search_tree(self, root_row: "RowAPI", for_ids: Iterable[int]) -> set[int]:
        ...

    @abc.abstractmethod
    def nest_rows(self, parent_row: "RowAPI", child_rows: Union["RowAPI", Iterable["RowAPI"]]) -> None:
        ...

    @abc.abstractmethod
    def delete_tree(self, parent_row: "RowAPI") -> None:
        ...

class DatabaseTriggerHelpersAPI(abc.ABC):
    """Typed API for trigger helper passthroughs exposed by ``Database``."""

    @abc.abstractmethod
    def get_triggers(self) -> Any:
        ...

    @abc.abstractmethod
    def drop_triggers(self, triggers: Any) -> Any:
        ...

    @abc.abstractmethod
    def drop_all_triggers(self) -> Any:
        ...

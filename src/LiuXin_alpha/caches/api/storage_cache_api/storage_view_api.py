
"""
Cache views are ordered projections over cached ids and fields.

A cache view is not a database view.
It is a stateful in-memory object that presents cached ids as rows and cached fields as columns, with support for
sorting, filtering, searching, and refresh.
"""



from __future__ import annotations

import abc
import dataclasses
from collections.abc import Iterable, Iterator, Sequence
from typing import TYPE_CHECKING, Any, Optional

from LiuXin_alpha.databases.db_types import MainTableID, MainTableName

if TYPE_CHECKING:
    from LiuXin_alpha.caches.api.storage_cache_api.storage_fields.base_field import (
        FieldBasicInterfaceAPI,
    )


@dataclasses.dataclass(frozen=True, slots=True)
class CacheViewColumnSpec:
    """
    Declarative description of one column in a cache view.
    """

    key: str
    label: str
    field_key: Optional[str] = None
    sortable: bool = True
    searchable: bool = True
    visible: bool = True


@dataclasses.dataclass(frozen=True, slots=True)
class CacheViewSortSpec:
    """
    Description of one sort component.
    """

    key: str
    ascending: bool = True


@dataclasses.dataclass(frozen=True, slots=True)
class CacheViewSpec:
    """
    Declarative metadata for a cache view.

    The spec describes what the view is over. It does not store the mutable
    runtime state of the live view.
    """

    name: str
    base_table: MainTableName
    id_column: str = "id"
    columns: tuple[CacheViewColumnSpec, ...] = ()
    default_sort: tuple[CacheViewSortSpec, ...] = (CacheViewSortSpec("id", True),)

    def __post_init__(self) -> None:
        if self.default_sort == (CacheViewSortSpec("id", True),) and self.id_column != "id":
            object.__setattr__(self, "default_sort", (CacheViewSortSpec(self.id_column, True),))


@dataclasses.dataclass(slots=True)
class CacheViewState:
    """
    Mutable runtime state for a live cache view.
    """

    all_ids: tuple[MainTableID, ...] = ()
    visible_ids: tuple[MainTableID, ...] = ()
    full_map_is_sorted: bool = False
    sort_history: list[CacheViewSortSpec] = dataclasses.field(default_factory=list)

    search_restriction: str = ""
    search_restriction_name: str = ""
    search_restriction_count: int = 0

    base_restriction: str = ""
    base_restriction_name: str = ""


class CacheViewRowAPI(abc.ABC):
    """
    Row-like wrapper over one cached id in a view.
    """

    row_id: MainTableID
    column_count: int

    @abc.abstractmethod
    def __getitem__(self, obj: int | slice) -> Any:
        """
        Get one cell or a slice of cells from the row.
        """

    @abc.abstractmethod
    def __len__(self) -> int:
        """
        Return the number of visible columns in the row.
        """

    @abc.abstractmethod
    def __iter__(self) -> Iterator[Any]:
        """
        Iterate over the visible cells in the row.
        """


class CacheViewAPI(abc.ABC):
    """
    Ordered projection over cached ids and fields.

    A cache view owns ordering, filtering, and row/column access over cached
    data. It does not own the underlying storage schema.
    """

    spec: CacheViewSpec
    state: CacheViewState

    @property
    @abc.abstractmethod
    def field_map(self) -> dict[int, str]:
        """
        Mapping from column index to field key.
        """

    @property
    @abc.abstractmethod
    def fields(self) -> dict[str, "FieldBasicInterfaceAPI[Any]"]:
        """
        Cached field objects keyed by field key.
        """

    @property
    def column_count(self) -> int:
        """
        Number of columns visible in this view.
        """
        return len(self.field_map)

    # ------------------
    # - ID / ROW ACCESS

    @abc.abstractmethod
    def count(self) -> int:
        """
        Number of ids currently visible in the view.
        """

    @abc.abstractmethod
    def has_id(self, row_id: MainTableID) -> bool:
        """
        Return True if the given id is present in the current view.
        """

    @abc.abstractmethod
    def iter_all_ids(self) -> Iterator[MainTableID]:
        """
        Iterate over all ids in the view before filtering.
        """

    @abc.abstractmethod
    def iter_visible_ids(self) -> Iterator[MainTableID]:
        """
        Iterate over ids currently visible in the view.
        """

    @abc.abstractmethod
    def row_for_id(self, row_id: MainTableID) -> CacheViewRowAPI:
        """
        Return the row object for the given id.
        """

    @abc.abstractmethod
    def index_to_id(self, index: int) -> MainTableID:
        """
        Return the row id at the given visible index.
        """

    @abc.abstractmethod
    def id_to_index(self, row_id: MainTableID) -> int:
        """
        Return the visible index for the given row id.
        """

    @abc.abstractmethod
    def __getitem__(self, index: int) -> CacheViewRowAPI:
        """
        Return the row at the given visible index.
        """

    @abc.abstractmethod
    def __len__(self) -> int:
        """
        Alias for count().
        """

    @abc.abstractmethod
    def __iter__(self) -> Iterator[CacheViewRowAPI]:
        """
        Iterate over visible rows.
        """

    # ------------------
    # - CELL ACCESS

    @abc.abstractmethod
    def value_for(
        self,
        row_id: MainTableID,
        field_key: str,
        default_value: Any = None,
    ) -> Any:
        """
        Return the value for one field on one row id.
        """

    @abc.abstractmethod
    def value_for_index(
        self,
        index: int,
        field_key: str,
        default_value: Any = None,
    ) -> Any:
        """
        Return the value for one field on one visible row index.
        """

    @abc.abstractmethod
    def row_values_for_id(self, row_id: MainTableID) -> Sequence[Any]:
        """
        Return all visible column values for the given row id.
        """

    @abc.abstractmethod
    def get_field(self, key_or_index: str | int) -> "FieldBasicInterfaceAPI[Any]":
        """
        Return the field object for the given field key or column index.
        """

    # ------------------
    # - SORT / SEARCH / FILTER

    @abc.abstractmethod
    def sort(self, fields: Sequence[CacheViewSortSpec], subsort: bool = False) -> None:
        """
        Sort the view by the given fields.
        """

    @abc.abstractmethod
    def search(self, query: str, return_ids: bool = False) -> Optional[tuple[MainTableID, ...]]:
        """
        Search within the current view.

        If return_ids is True, return matching ids instead of applying the
        filter to the live visible id set.
        """

    @abc.abstractmethod
    def set_search_restriction(self, restriction: str, name: str = "") -> None:
        """
        Set the active search restriction for the view.
        """

    @abc.abstractmethod
    def set_base_restriction(self, restriction: str, name: str = "") -> None:
        """
        Set the base restriction for the view.
        """

    @abc.abstractmethod
    def search_restriction_applied(self) -> bool:
        """
        Return True if a search or base restriction is currently active.
        """

    # ------------------
    # - REFRESH / MUTATION

    @abc.abstractmethod
    def refresh(self, field: Optional[str] = None, ascending: bool = True) -> None:
        """
        Refresh the whole view from the underlying cache.
        """

    @abc.abstractmethod
    def refresh_ids(self, ids: Iterable[MainTableID]) -> None:
        """
        Refresh the given ids in-place where possible.
        """

    @abc.abstractmethod
    def remove_ids(self, ids: Iterable[MainTableID]) -> None:
        """
        Remove the given ids from the view.
        """

    @abc.abstractmethod
    def add_ids(self, ids: Iterable[MainTableID]) -> None:
        """
        Add the given ids to the view.
        """

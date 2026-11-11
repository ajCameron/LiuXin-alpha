"""Declare connection-aware reads, bounded scans and optional diagnostics."""

from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator, Mapping, Sequence
from typing import Any

from LiuXin_alpha.databases.db_types.backend_capabilities import BackendCapabilities
from LiuXin_alpha.databases.db_types.driver_types import (
    QueryConnection,
    RowPage,
    SQLParameters,
    SQLRow,
)
from LiuXin_alpha.databases.db_types.query_observation import QueryObserver


class QueryOperationsAPI(ABC):
    """Read results without completing a borrowed transaction or retaining cursors.

    Raw SQL is trusted input. These methods do not parse or forbid mutations;
    borrow macros.read_transaction() for backend-enforced read-only snapshots.
    """

    @abstractmethod
    def fetch_one(
        self,
        sql: str,
        values: SQLParameters | None = None,
        *,
        connection: QueryConnection | None = None,
    ) -> SQLRow | None:
        """Fetch one row or None, preferring explicit, active macro then primary handles."""

    @abstractmethod
    def fetch_all(
        self,
        sql: str,
        values: SQLParameters | None = None,
        *,
        connection: QueryConnection | None = None,
    ) -> list[SQLRow]:
        """Fetch all result rows and close the cursor on every path."""

    @abstractmethod
    def fetch_scalar(
        self,
        sql: str,
        values: SQLParameters | None = None,
        *,
        connection: QueryConnection | None = None,
    ) -> Any:
        """Fetch a single-column first row; no row and SQL NULL both return None."""

    @abstractmethod
    def scan_page(
        self,
        table: str,
        *,
        columns: Sequence[str] | None = None,
        where: Mapping[str, Any] | None = None,
        batch_size: int = 1000,
        after_id: int | None = None,
        connection: QueryConnection | None = None,
    ) -> RowPage:
        """Read a bounded ID-ordered batch; the caller owns any snapshot isolation."""

    @abstractmethod
    def iter_row_batches(
        self,
        table: str,
        *,
        columns: Sequence[str] | None = None,
        where: Mapping[str, Any] | None = None,
        batch_size: int = 1000,
        after_id: int | None = None,
        connection: QueryConnection | None = None,
    ) -> Iterator[RowPage]:
        """Scan with bounded pages and one schema lookup; no owned handle crosses yield."""

    @abstractmethod
    def get_backend_capabilities(
        self, *, connection: QueryConnection | None = None
    ) -> BackendCapabilities:
        """Inspect supported backend facilities and current limits without committing."""

    @abstractmethod
    def set_query_observers(self, observers: Iterable[QueryObserver] = ()) -> None:
        """Replace synchronous observers; an empty collection disables diagnostics."""

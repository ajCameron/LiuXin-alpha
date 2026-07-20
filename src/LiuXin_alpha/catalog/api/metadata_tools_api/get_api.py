"""Getter API contracts for catalog metadata tools."""

from __future__ import annotations

from typing import Literal, Protocol, TypeAlias, overload, runtime_checkable

from LiuXin_alpha.catalog.api.metadata_tools_api.common import RowValue
from LiuXin_alpha.databases.api import DatabaseAPI, RowAPI

SeriesGetterResult: TypeAlias = (
    list[tuple[RowAPI, RowAPI]]
    | list[tuple[RowValue, RowValue]]
    | tuple[RowAPI, RowAPI]
    | tuple[RowValue, RowAPI]
    | None
)


@runtime_checkable
class BackendGetterAPI(Protocol):
    """Convenience metadata read helpers for linked resource rows."""

    db: DatabaseAPI

    @overload
    def comment(self, resource_row: RowAPI, all: Literal[True], rows: Literal[True] = True) -> list[RowAPI]:
        ...

    @overload
    def comment(self, resource_row: RowAPI, all: Literal[True], rows: Literal[False]) -> list[str]:
        ...

    @overload
    def comment(self, resource_row: RowAPI, all: Literal[False] = False, rows: Literal[True] = True) -> RowAPI | None:
        ...

    @overload
    def comment(self, resource_row: RowAPI, all: Literal[False], rows: Literal[False]) -> None:
        ...

    def comment(
        self,
        resource_row: RowAPI,
        all: bool = False,
        rows: bool = True,
    ) -> RowAPI | list[RowAPI] | list[str] | None:
        """Return comment rows or comment values linked to a resource."""
        ...

    @overload
    def series(
        self,
        resource_row: RowAPI,
        all: Literal[True],
        rows: Literal[True] = True,
    ) -> list[tuple[RowAPI, RowAPI]]:
        ...

    @overload
    def series(
        self,
        resource_row: RowAPI,
        all: Literal[True],
        rows: Literal[False],
    ) -> list[tuple[RowValue, RowValue]]:
        ...

    @overload
    def series(
        self,
        resource_row: RowAPI,
        all: Literal[False] = False,
        rows: Literal[True] = True,
    ) -> tuple[RowAPI, RowAPI] | None:
        ...

    @overload
    def series(
        self,
        resource_row: RowAPI,
        all: Literal[False],
        rows: Literal[False],
    ) -> tuple[RowValue, RowAPI] | None:
        ...

    def series(
        self,
        resource_row: RowAPI,
        all: bool = False,
        rows: bool = True,
    ) -> SeriesGetterResult:
        """Return series links and series rows or values for a resource."""
        ...

    @overload
    def synopsis(self, resource_row: RowAPI, all: Literal[True], rows: Literal[True] = True) -> list[RowAPI]:
        ...

    @overload
    def synopsis(self, resource_row: RowAPI, all: Literal[True], rows: Literal[False]) -> list[str]:
        ...

    @overload
    def synopsis(self, resource_row: RowAPI, all: Literal[False] = False, rows: Literal[True] = True) -> RowAPI | None:
        ...

    @overload
    def synopsis(self, resource_row: RowAPI, all: Literal[False], rows: Literal[False]) -> None:
        ...

    def synopsis(
        self,
        resource_row: RowAPI,
        all: bool = False,
        rows: bool = True,
    ) -> RowAPI | list[RowAPI] | list[str] | None:
        """Return synopsis rows or synopsis values linked to a resource."""
        ...


__all__ = ["BackendGetterAPI", "SeriesGetterResult"]

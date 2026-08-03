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
    """Read linked metadata from a database resource ``RowAPI``.

    ``all=False`` returns one preferred/first result; ``all=True`` returns a
    list. ``rows=True`` returns database rows, while ``rows=False`` projects
    scalar values where the helper supports them.

    Example::

        comment_rows = getter.comment(work_row, all=True)
        comment_text = getter.comment(work_row, all=True, rows=False)
        series_links = getter.series(work_row, all=True)
    """

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
        """Return linked Comments according to ``all``/``rows`` mode.

        With ``all=False, rows=False`` the legacy helper returns ``None`` rather
        than a single scalar; request ``all=True`` for comment text values.
        """
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
        """Return linked Series together with their series-position metadata.

        ``all=False`` selects the preferred first link; ``all=True`` returns
        every linked Series. With ``rows=True``, each result pairs the Series
        row with its link/index row. With ``rows=False``, it projects legacy
        scalar values instead. The single scalar form retains its link row as
        the second tuple member for compatibility.

        :param resource_row: Database row whose linked Series are requested.
        :param all: Return all links instead of the preferred first link.
        :param rows: Return database rows rather than projected values.
        :return: One pair, a list of pairs, or ``None`` according to the mode.

        Example::

            first_series, index_link = getter.series(work_row)
            series_values = getter.series(
                work_row,
                all=True,
                rows=False,
            )
        """
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
        """Return linked Synopsis rows or projected text.

        ``all=False`` returns the preferred first row (or ``None``);
        ``all=True`` returns a list. Text projection is only available in the
        all-results form: the legacy ``all=False, rows=False`` combination
        returns ``None``.

        :param resource_row: Database row whose linked Synopses are requested.
        :param all: Return all linked Synopses instead of the preferred first.
        :param rows: Return database rows; with ``all=True``, ``False`` returns
            Synopsis strings.
        :return: A row, list of rows, list of strings, or ``None`` by mode.

        Example::

            synopsis_rows = getter.synopsis(work_row, all=True)
            synopsis_texts = getter.synopsis(
                work_row,
                all=True,
                rows=False,
            )
        """
        ...


__all__ = ["BackendGetterAPI", "SeriesGetterResult"]

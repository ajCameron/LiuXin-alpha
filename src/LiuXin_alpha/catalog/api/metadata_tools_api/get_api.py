"""
Describe legacy getter overloads and their asymmetric projection modes.
"""

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
    """
    Retain a database handle for legacy row-oriented relationship reads.

    Result order comes from the database. First-result operations catch KeyError
    but not IndexError, so an empty list can raise instead of returning None.
    No encompassing read transaction is opened.

    Example:
        Request ``getter.comment(resource_row, all=True, rows=False)`` for text values.
    """

    db: DatabaseAPI

    @overload
    def comment(self, resource_row: RowAPI, all: Literal[True], rows: Literal[True] = True) -> list[RowAPI]:
        """
        Read linked Comments in the legacy all/rows mode.

        With all=False and rows=False, return None after fetching links. A first
        row KeyError is suppressed; an empty list's IndexError propagates. All-text
        projection errors and database failures propagate.

        Example:
            ``getter.comment(resource_row, all=True, rows=False)`` returns stored comment
            text values, including an empty list when there are no links.


        :param resource_row: Resource Row used as the primary endpoint.
        :param all: True selects all results; false selects the first row when rows=True.
        :param rows: True returns Rows; false returns text only in the all-results mode.
        :return: All linked Rows, a list of comment values, one Row, or None by mode.
        """

        ...

    @overload
    def comment(self, resource_row: RowAPI, all: Literal[True], rows: Literal[False]) -> list[str]:
        """
        Read linked Comments in the legacy all/rows mode.

        With all=False and rows=False, return None after fetching links. A first
        row KeyError is suppressed; an empty list's IndexError propagates. All-text
        projection errors and database failures propagate.

        Example:
            ``getter.comment(resource_row, all=True, rows=False)`` returns stored comment
            text values, including an empty list when there are no links.


        :param resource_row: Resource Row used as the primary endpoint.
        :param all: True selects all results; false selects the first row when rows=True.
        :param rows: True returns Rows; false returns text only in the all-results mode.
        :return: All linked Rows, a list of comment values, one Row, or None by mode.
        """

        ...

    @overload
    def comment(self, resource_row: RowAPI, all: Literal[False] = False, rows: Literal[True] = True) -> RowAPI | None:
        """
        Read linked Comments in the legacy all/rows mode.

        With all=False and rows=False, return None after fetching links. A first
        row KeyError is suppressed; an empty list's IndexError propagates. All-text
        projection errors and database failures propagate.

        Example:
            ``getter.comment(resource_row, all=True, rows=False)`` returns stored comment
            text values, including an empty list when there are no links.


        :param resource_row: Resource Row used as the primary endpoint.
        :param all: True selects all results; false selects the first row when rows=True.
        :param rows: True returns Rows; false returns text only in the all-results mode.
        :return: All linked Rows, a list of comment values, one Row, or None by mode.
        """

        ...

    @overload
    def comment(self, resource_row: RowAPI, all: Literal[False], rows: Literal[False]) -> None:
        """
        Read linked Comments in the legacy all/rows mode.

        With all=False and rows=False, return None after fetching links. A first
        row KeyError is suppressed; an empty list's IndexError propagates. All-text
        projection errors and database failures propagate.

        Example:
            ``getter.comment(resource_row, all=True, rows=False)`` returns stored comment
            text values, including an empty list when there are no links.


        :param resource_row: Resource Row used as the primary endpoint.
        :param all: True selects all results; false selects the first row when rows=True.
        :param rows: True returns Rows; false returns text only in the all-results mode.
        :return: All linked Rows, a list of comment values, one Row, or None by mode.
        """

        ...

    def comment(
        self,
        resource_row: RowAPI,
        all: bool = False,
        rows: bool = True,
    ) -> RowAPI | list[RowAPI] | list[str] | None:
        """
        Read linked Comments in the legacy all/rows mode.

        With all=False and rows=False, return None after fetching links. A first
        row KeyError is suppressed; an empty list's IndexError propagates. All-text
        projection errors and database failures propagate.

        Example:
            ``getter.comment(resource_row, all=True, rows=False)`` returns stored comment
            text values, including an empty list when there are no links.


        :param resource_row: Resource Row used as the primary endpoint.
        :param all: True selects all results; false selects the first row when rows=True.
        :param rows: True returns Rows; false returns text only in the all-results mode.
        :return: All linked Rows, a list of comment values, one Row, or None by mode.
        """
        ...

    @overload
    def series(
        self,
        resource_row: RowAPI,
        all: Literal[True],
        rows: Literal[True] = True,
    ) -> list[tuple[RowAPI, RowAPI]]:
        """
        Read linked Series together with their link rows or index values.

        The single values mode deliberately retains the Series Row as the second
        member. Empty-list IndexError is not caught. The single-row mode still
        looks up the index column even when returning Rows, so column discovery
        can fail independently. Each linked Series requires a separate link read.

        Example:
            Unpack ``link_row, series_row = getter.series(resource_row)``; the link
            row precedes the Series row.


        :param resource_row: Resource Row whose Series links are requested.
        :param all: True returns a list; false selects the database's first linked Series.
        :param rows: True returns (link Row, Series Row); false projects index values.
        :return: All rows: list of (link Row, Series Row); all values: list of (index, Series text).
            Single rows: (link Row, Series Row); single values: (index, Series Row);
            None only for a caught KeyError in first-row/link retrieval.
        """

        ...

    @overload
    def series(
        self,
        resource_row: RowAPI,
        all: Literal[True],
        rows: Literal[False],
    ) -> list[tuple[RowValue, RowValue]]:
        """
        Read linked Series together with their link rows or index values.

        The single values mode deliberately retains the Series Row as the second
        member. Empty-list IndexError is not caught. The single-row mode still
        looks up the index column even when returning Rows, so column discovery
        can fail independently. Each linked Series requires a separate link read.

        Example:
            Unpack ``link_row, series_row = getter.series(resource_row)``; the link
            row precedes the Series row.


        :param resource_row: Resource Row whose Series links are requested.
        :param all: True returns a list; false selects the database's first linked Series.
        :param rows: True returns (link Row, Series Row); false projects index values.
        :return: All rows: list of (link Row, Series Row); all values: list of (index, Series text).
            Single rows: (link Row, Series Row); single values: (index, Series Row);
            None only for a caught KeyError in first-row/link retrieval.
        """

        ...

    @overload
    def series(
        self,
        resource_row: RowAPI,
        all: Literal[False] = False,
        rows: Literal[True] = True,
    ) -> tuple[RowAPI, RowAPI] | None:
        """
        Read linked Series together with their link rows or index values.

        The single values mode deliberately retains the Series Row as the second
        member. Empty-list IndexError is not caught. The single-row mode still
        looks up the index column even when returning Rows, so column discovery
        can fail independently. Each linked Series requires a separate link read.

        Example:
            Unpack ``link_row, series_row = getter.series(resource_row)``; the link
            row precedes the Series row.


        :param resource_row: Resource Row whose Series links are requested.
        :param all: True returns a list; false selects the database's first linked Series.
        :param rows: True returns (link Row, Series Row); false projects index values.
        :return: All rows: list of (link Row, Series Row); all values: list of (index, Series text).
            Single rows: (link Row, Series Row); single values: (index, Series Row);
            None only for a caught KeyError in first-row/link retrieval.
        """

        ...

    @overload
    def series(
        self,
        resource_row: RowAPI,
        all: Literal[False],
        rows: Literal[False],
    ) -> tuple[RowValue, RowAPI] | None:
        """
        Read linked Series together with their link rows or index values.

        The single values mode deliberately retains the Series Row as the second
        member. Empty-list IndexError is not caught. The single-row mode still
        looks up the index column even when returning Rows, so column discovery
        can fail independently. Each linked Series requires a separate link read.

        Example:
            Unpack ``link_row, series_row = getter.series(resource_row)``; the link
            row precedes the Series row.


        :param resource_row: Resource Row whose Series links are requested.
        :param all: True returns a list; false selects the database's first linked Series.
        :param rows: True returns (link Row, Series Row); false projects index values.
        :return: All rows: list of (link Row, Series Row); all values: list of (index, Series text).
            Single rows: (link Row, Series Row); single values: (index, Series Row);
            None only for a caught KeyError in first-row/link retrieval.
        """

        ...

    def series(
        self,
        resource_row: RowAPI,
        all: bool = False,
        rows: bool = True,
    ) -> SeriesGetterResult:
        """
        Read linked Series together with their link rows or index values.

        The single values mode deliberately retains the Series Row as the second
        member. Empty-list IndexError is not caught. The single-row mode still
        looks up the index column even when returning Rows, so column discovery
        can fail independently. Each linked Series requires a separate link read.

        Example:
            Unpack ``link_row, series_row = getter.series(resource_row)``; the link
            row precedes the Series row.


        :param resource_row: Resource Row whose Series links are requested.
        :param all: True returns a list; false selects the database's first linked Series.
        :param rows: True returns (link Row, Series Row); false projects index values.
        :return: All rows: list of (link Row, Series Row); all values: list of (index, Series text).
            Single rows: (link Row, Series Row); single values: (index, Series Row);
            None only for a caught KeyError in first-row/link retrieval.
        """
        ...

    @overload
    def synopsis(self, resource_row: RowAPI, all: Literal[True], rows: Literal[True] = True) -> list[RowAPI]:
        """
        Read linked Synopses in the legacy all/rows mode.

        With all=False and rows=False, return None after fetching links. A first
        row KeyError is suppressed; an empty list's IndexError propagates. All-text
        projection errors and database failures propagate.

        Example:
            ``getter.synopsis(resource_row, all=True, rows=False)`` returns stored synopsis
            text values, including an empty list when there are no links.


        :param resource_row: Resource Row used as the primary endpoint.
        :param all: True selects all results; false selects the first row when rows=True.
        :param rows: True returns Rows; false returns text only in the all-results mode.
        :return: All linked Rows, a list of synopsis values, one Row, or None by mode.
        """

        ...

    @overload
    def synopsis(self, resource_row: RowAPI, all: Literal[True], rows: Literal[False]) -> list[str]:
        """
        Read linked Synopses in the legacy all/rows mode.

        With all=False and rows=False, return None after fetching links. A first
        row KeyError is suppressed; an empty list's IndexError propagates. All-text
        projection errors and database failures propagate.

        Example:
            ``getter.synopsis(resource_row, all=True, rows=False)`` returns stored synopsis
            text values, including an empty list when there are no links.


        :param resource_row: Resource Row used as the primary endpoint.
        :param all: True selects all results; false selects the first row when rows=True.
        :param rows: True returns Rows; false returns text only in the all-results mode.
        :return: All linked Rows, a list of synopsis values, one Row, or None by mode.
        """

        ...

    @overload
    def synopsis(self, resource_row: RowAPI, all: Literal[False] = False, rows: Literal[True] = True) -> RowAPI | None:
        """
        Read linked Synopses in the legacy all/rows mode.

        With all=False and rows=False, return None after fetching links. A first
        row KeyError is suppressed; an empty list's IndexError propagates. All-text
        projection errors and database failures propagate.

        Example:
            ``getter.synopsis(resource_row, all=True, rows=False)`` returns stored synopsis
            text values, including an empty list when there are no links.


        :param resource_row: Resource Row used as the primary endpoint.
        :param all: True selects all results; false selects the first row when rows=True.
        :param rows: True returns Rows; false returns text only in the all-results mode.
        :return: All linked Rows, a list of synopsis values, one Row, or None by mode.
        """

        ...

    @overload
    def synopsis(self, resource_row: RowAPI, all: Literal[False], rows: Literal[False]) -> None:
        """
        Read linked Synopses in the legacy all/rows mode.

        With all=False and rows=False, return None after fetching links. A first
        row KeyError is suppressed; an empty list's IndexError propagates. All-text
        projection errors and database failures propagate.

        Example:
            ``getter.synopsis(resource_row, all=True, rows=False)`` returns stored synopsis
            text values, including an empty list when there are no links.


        :param resource_row: Resource Row used as the primary endpoint.
        :param all: True selects all results; false selects the first row when rows=True.
        :param rows: True returns Rows; false returns text only in the all-results mode.
        :return: All linked Rows, a list of synopsis values, one Row, or None by mode.
        """

        ...

    def synopsis(
        self,
        resource_row: RowAPI,
        all: bool = False,
        rows: bool = True,
    ) -> RowAPI | list[RowAPI] | list[str] | None:
        """
        Read linked Synopses in the legacy all/rows mode.

        With all=False and rows=False, return None after fetching links. A first
        row KeyError is suppressed; an empty list's IndexError propagates. All-text
        projection errors and database failures propagate.

        Example:
            ``getter.synopsis(resource_row, all=True, rows=False)`` returns stored synopsis
            text values, including an empty list when there are no links.


        :param resource_row: Resource Row used as the primary endpoint.
        :param all: True selects all results; false selects the first row when rows=True.
        :param rows: True returns Rows; false returns text only in the all-results mode.
        :return: All linked Rows, a list of synopsis values, one Row, or None by mode.
        """
        ...


__all__ = ["BackendGetterAPI", "SeriesGetterResult"]

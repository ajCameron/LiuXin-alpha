"""
Specify column discovery and identifier lookup for database views.

The shared SQL implementation inspects view headings and searches a conventional
identifier column. Missing-row behavior differs from ordinary table row lookup;
these hooks expose read operations only.
"""

from __future__ import annotations

import abc

from typing import Any, Callable, Dict, List, Optional, Tuple, Union, Iterable

# Todo: I suspect this is used EVERYWHERE. So let's try and dry out the code base.
class DriverViewMixinAPI(abc.ABC):
    """
    Specify column discovery and identifier lookup for database views.

    The shared SQL implementation inspects view headings and searches a conventional
    identifier column. Missing-row behavior differs from ordinary table row lookup;
    these hooks expose read operations only. Abstract members must be implemented by a
    backend; their empty bodies return None when called directly.

    Example:
        >>> import inspect
        >>> inspect.isabstract(DriverViewMixinAPI)
        True
    """

    @abc.abstractmethod
    def direct_get_view_column_headings(self, view: str) -> list[str]:
        """
        Read relation column names in the order returned by PRAGMA TABLE_INFO.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Interpolate the supplied name directly, without canonicalization, quoting or
        identifier validation. Tables also work, and an unknown relation normally produces
        an empty list. Only column names are retained from the PRAGMA rows. SQL errors
        propagate. This helper does not explicitly close its cursor or connection, even
        after successful iteration.

        Example:
            >>> driver.direct_get_view_column_headings("sample_view")  # doctest: +SKIP


        :param view: Trusted relation spelling suitable for direct PRAGMA interpolation; no
            check distinguishes tables from views.
        :return: A fresh list of column names, possibly empty.
        """

    @abc.abstractmethod
    def direct_get_view_row_dict_from_id(self, view: str, row_id: int) -> Optional[dict[str, Any]]:
        """
        Retrieve at most one view row by a bound, text-coerced id value.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Convert both arguments with force_unicode, currently str. Interpolate the view name
        and query a column literally named id, binding the id as data. Fetch headings
        through direct_get_view_column_headings and convert every match through _row_to_dict
        with the view as its declared-type lookup key. Conversion may therefore turn numeric
        cells into ints or floats.

        Consume all matches before deciding the result. Log and return None for zero rows;
        log and raise DatabaseIntegrityError for multiple rows. Close this query's
        connection on these paths and on success. Query, heading, conversion or logging
        errors propagate without guaranteed cleanup; the headings helper's separate
        connection is not explicitly closed here.

        Example:
            >>> driver.direct_get_view_row_dict_from_id("sample_view", 1)  # doctest: +SKIP


        :param view: Trusted view spelling suitable for direct SQL interpolation; converted
            to text without identifier validation or quoting.
        :param row_id: Identifier converted to text and bound to the id predicate; the
            annotated int type is not enforced at runtime.
        :return: A new heading-to-converted-value dictionary, or None if no row matches.
            Duplicate matches raise DatabaseIntegrityError.
        """


"""
Discover compatibility views and read their headings and row dictionaries.

The host provides a driver, table discovery and relation-type lookup. Discovery
filters the names visible through get_tables(), so views suppressed by backend
compatibility-name rules remain hidden here.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:

    from LiuXin_alpha.databases.api.row_api import ViewRowAPI


# Todo: Need to actually type this mess
class DriverWrapperViewMixin:
    """
    Discover compatibility views and read their headings and row dictionaries.

    These methods expose read operations; they neither create views nor make them
    writable.

    Example:
        >>> wrapper.get_views()  # doctest: +SKIP
    """

    def get_views(self, force_refresh: bool = False):
        """
        Filter discovered relation names to those reported as views.

        Passes force_refresh to get_tables(), then keeps each name whose get_relation_type()
        result equals view. Preserves discovery order; unavailable or failed type lookups
        are excluded.

        Example:
            >>> wrapper.get_views()  # doctest: +SKIP


        :param force_refresh: Whether get_tables() should refresh discovery and wrapper
            caches.
        :return: List of discovered view names.
        """
        names = self.get_tables(force_refresh=force_refresh)
        return [
            name
            for name in names
            if self.get_relation_type(name) == "view"
        ]

    def is_view(self, name: str) -> bool:
        """
        Check whether relation discovery reports a view for this name.

        Unknown names and swallowed relation-lookup errors produce False.

        Example:
            >>> wrapper.is_view("note")  # doctest: +SKIP


        :param name: Schema object name to inspect.
        :return: True only when get_relation_type(name) equals view.
        """
        return self.get_relation_type(name) == "view"

    def get_view_column_headings(self, view: str) -> list[str]:
        """
        Read relation column names in the order returned by PRAGMA TABLE_INFO.

        Delegates to the driver's direct_get_view_column_headings hook. The following
        backend details describe the shared SQL/SQLite implementation; backend errors
        propagate.

        Interpolate the supplied name directly, without canonicalization, quoting or
        identifier validation. Tables also work, and an unknown relation normally produces
        an empty list. Only column names are retained from the PRAGMA rows. SQL errors
        propagate. This helper does not explicitly close its cursor or connection, even
        after successful iteration.

        Example:
            >>> wrapper.get_view_column_headings("sample_view")  # doctest: +SKIP


        :param view: Trusted relation spelling suitable for direct PRAGMA interpolation; no
            check distinguishes tables from views.
        :return: A fresh list of column names, possibly empty.
        """
        return self.driver.direct_get_view_column_headings(view)

    # Todo: Need a method to get the name of all the views for a database
    def get_view_row_from_id(self, view: str, row_id: int) -> "ViewRowAPI":
        """
        Retrieve at most one view row by a bound, text-coerced id value.

        Delegates to the driver's direct_get_view_row_dict_from_id hook. The following
        backend details describe the shared SQL/SQLite implementation; backend errors
        propagate.

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
            >>> wrapper.get_view_row_from_id("sample_view", 1)  # doctest: +SKIP


        :param view: Trusted view spelling suitable for direct SQL interpolation; converted
            to text without identifier validation or quoting.
        :param row_id: Identifier converted to text and bound to the id predicate; the
            annotated int type is not enforced at runtime.
        :return: A new heading-to-converted-value dictionary, or None if no row matches.
            Duplicate matches raise DatabaseIntegrityError.
        """
        return self.driver.direct_get_view_row_dict_from_id(view, row_id)

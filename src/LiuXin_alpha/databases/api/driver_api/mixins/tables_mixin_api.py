
"""
Specify creation of conventionally named main tables.

The shared SQL implementation accepts a column-specification mapping despite the
historical iterable annotation. Creating a table and refreshing schema caches are
backend responsibilities, not behavior provided by these stubs.
"""

import abc

from typing import Iterable, Any, Optional, Union


class DriverTablesMixinAPI(abc.ABC):
    """
    Specify creation of conventionally named main tables.

    The shared SQL implementation accepts a column-specification mapping despite the
    historical iterable annotation. Creating a table and refreshing schema caches are
    backend responsibilities, not behavior provided by these stubs. Abstract members
    must be implemented by a backend; their empty bodies return None when called
    directly.

    Example:
        >>> import inspect
        >>> inspect.isabstract(DriverTablesMixinAPI)
        True
    """
    @abc.abstractmethod
    def direct_create_main_table(
            self,
            table_name: str,
            column_headings: Optional[Iterable[str]] = None,
            index_on: Optional[Union[str, Iterable[str]]] = 'all',
            default_datatype: str = 'TEXT',
            default_unique: bool = False) -> None:
        """
        Create a conventionally named main table and invalidate schema caches.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        With no headings, create one default data column and its index; only index_on="all"
        is implemented in that branch. Explicit headings must be a mapping to datatype
        specification mappings; that branch creates no indexes. default_unique is unused.
        Trusted names/types become SQL syntax; repeated default creation may fail on the
        existing index even though the table uses IF NOT EXISTS.

        Example:
            >>> driver.direct_create_main_table("examples", {"name": {"datatype": "TEXT"}})  # doctest: +SKIP


        :param table_name: Trusted plural table name; its singular form prefixes generated
            columns.
        :param column_headings: Optional suffix-to-specification mapping; each spec may
            contain a ``datatype`` key.
        :param index_on: Use ``all`` for the default layout; ignored for explicit column
            specifications.
        :param default_datatype: Trusted SQL type used for the default column or a spec
            missing datatype.
        :param default_unique: Accepted but currently unused; no uniqueness constraint is
            generated from it.
        :return: ``None``.
        """

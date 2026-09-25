
"""
Specify physical custom-value tables and their relationship layouts.

These hooks concern storage tables, not the separate Calibre-style custom-column
definition registry. Shared SQL builders distinguish inline values, owned multiple
values and reusable linked values; unsupported normalization modes remain explicit.
"""

import abc

from typing import Iterable, Any


class DriverCustomColumnsMixinAPI(abc.ABC):
    """
    Specify physical custom-value tables and their relationship layouts.

    These hooks concern storage tables, not the separate Calibre-style custom-column
    definition registry. Shared SQL builders distinguish inline values, owned multiple
    values and reusable linked values; unsupported normalization modes remain explicit.
    Abstract members must be implemented by a backend; their empty bodies return None
    when called directly.

    Example:
        >>> import inspect
        >>> inspect.isabstract(DriverCustomColumnsMixinAPI)
        True
    """

    @staticmethod
    @abc.abstractmethod
    def direct_get_custom_column_table_name(table: str, column_name: str) -> str:
        """
        Combine table and label into the custom_column$ naming convention without validation.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Example:
            >>> driver.direct_get_custom_column_table_name("works", "note")  # doctest: +SKIP


        :param table: Target table spelling inserted into the name.
        :param column_name: Custom label inserted into the name.
        :return: Storage-table name containing both supplied components.
        """

    @abc.abstractmethod
    def direct_create_custom_column(
            self,
            in_table: str,
            column_name: str,
            data_type: str = 'TEXT',
            multi: bool = False) -> None:
        """
        Dispatch physical custom-column creation by relationship type.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Falsy multi selects inline one-to-one; True selects many_many. Other supported
        values are one_many and many_one. Reject custom_columns as a target by assertion and
        unknown modes with NotImplementedError. Despite the annotation, return the created
        table name from the selected helper.

        Example:
            >>> driver.direct_create_custom_column("works", "note")  # doctest: +SKIP


        :param in_table: Existing target table, not a table-category label.
        :param column_name: Custom-column label used to derive the storage name.
        :param data_type: SQL type forwarded to one-to-one/one-to-many; only one-to-one
            currently uses it.
        :param multi: Falsy for one-to-one, True for many_many, or a supported relationship
            string.
        :return: Created custom-value table name.
        """

    @abc.abstractmethod
    def direct_create_many_many_custom_column(
            self,
            target_table: str,
            custom_column_name: str
    ) -> str:
        """
        Create shared custom values and strict many-to-many links without optional columns.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Build the main table with default settings and notify the host of schema changes.

        Example:
            >>> driver.direct_create_many_many_custom_column("works", "note")  # doctest: +SKIP


        :param target_table: Existing main table to which the custom values belong.
        :param custom_column_name: Custom-column label used in its generated storage-table
            name.
        :return: Created custom-value table name.
        """

    @abc.abstractmethod
    def direct_create_many_to_one_custom_column(
            self,
            target_table: str,
            custom_column_name: str
    ) -> str:
        """
        Create reusable custom values with at most one linked value per parent.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Build the main table with default settings, then many_one links and refresh schema
        state. No unused-value cleanup trigger is installed here.

        Example:
            >>> driver.direct_create_many_to_one_custom_column("works", "note")  # doctest: +SKIP


        :param target_table: Existing main table to which the custom values belong.
        :param custom_column_name: Custom-column label used in its generated storage-table
            name.
        :return: Created custom-value table name.
        """

    @abc.abstractmethod
    def direct_create_one_to_many_custom_column(
            self,
            target_table: str,
            custom_column_name: str,
            datatype: str = 'TEXT') -> str:
        """
        Create a value table and exclusive one-to-many links plus an unlink cleanup trigger.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        The trigger deletes the custom value whenever its link is removed. The datatype
        argument is currently ignored: main-table creation uses its own default type.

        Example:
            >>> driver.direct_create_one_to_many_custom_column("works", "note")  # doctest: +SKIP


        :param target_table: Existing main table to which the custom values belong.
        :param custom_column_name: Custom-column label used in its generated storage-table
            name.
        :param datatype: Requested SQL type; only the inline one-to-one builder uses it.
        :return: Created custom-value table name.
        """

    @abc.abstractmethod
    def direct_create_one_to_one_custom_column(
            self,
            target_table: str,
            custom_column_name: str,
            datatype: str = 'TEXT',
            normalized: bool = False) -> str:
        """
        Create an inline custom-value table with a unique parent reference and cascade FK.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Assert label/target validity and cache nonexistence, create lookup/value indexes,
        then invoke schema-change/cache hooks. The parent-reference column has TEXT
        affinity. Normalized mode is unsupported and raises NotImplementedError.

        Example:
            >>> driver.direct_create_one_to_one_custom_column("works", "note")  # doctest: +SKIP


        :param target_table: Existing main table to which the custom values belong.
        :param custom_column_name: Custom-column label used in its generated storage-table
            name.
        :param datatype: Requested SQL type; only the inline one-to-one builder uses it.
        :param normalized: Must be False; the normalized link-table variant is not
            implemented.
        :return: Created storage-table name.
        """

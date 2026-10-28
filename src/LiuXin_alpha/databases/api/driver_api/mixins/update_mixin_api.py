
"""
Specify bulk value updates and individual row-dictionary writes.

Shared SQL scalar updates require a field and infer its table; individual row writes
require an ID. Both paths own their operation transactions and roll back complete
batches on failure. These abstract stubs have no implementation.
"""

import abc

from typing import Iterable, Any


class DriverUpdateMixinAPI(abc.ABC):
    """
    Specify bulk value updates and individual row-dictionary writes.

    Shared SQL scalar updates require a field and infer its table; individual row writes
    require an ID. Both paths own their operation transactions and roll back complete
    batches on failure. Abstract members must be implemented by a backend; their empty
    bodies return None when called directly.

    Example:
        >>> import inspect
        >>> inspect.isabstract(DriverUpdateMixinAPI)
        True
    """
    @abc.abstractmethod
    def direct_update_columns(
            self,
            id_values_map: dict[str, Any],
            field: str = None,
            table: str = None) -> None:
        """
        Update one field for each ID as an atomic batch, preserving derived keys.

        In shared SQL, infer the table from the field; a conflicting table hint warns
        and the inferred table wins. Empty mappings are no-ops and dictionary-valued
        updates remain unsupported. A failed update rolls back earlier rows in the
        batch. Any owned SQLite connection is closed; PostgreSQL retains its primary
        connection.

        Example:
            Updating tag_phash alone rederives each key from the stored tag text.

        :param id_values_map: Mapping from row IDs to replacement scalar values.
        :param field: Column to update.
        :param table: Optional expected table name; PostgreSQL uses it as the table
            hint.
        :return: None for shared SQL; True for PostgreSQL.
        """

    @abc.abstractmethod
    def direct_update_row_dict(self, row_dict: dict[str, Any]) -> None:
        """
        Update the supplied columns of an identified row without mutating the input.

        Literal text "None" stays text; Python None requests SQL NULL. Derived-key-only
        updates read the omitted display value on the write connection and recompute the
        key. A missing ID raises an input/row integrity error. Shared SQL commits only
        on success, rolls back errors and always closes its owned connection; PostgreSQL
        retains its primary connection. Backend exceptions propagate.

        Example:
            Updating only tag_phash recomputes it from the stored tag text.

        :param row_dict: Column/value mapping including the row ID and optional table
            hint.
        :return: True for an ID-only no-op; shared SQL otherwise returns None,
            PostgreSQL True.
        """

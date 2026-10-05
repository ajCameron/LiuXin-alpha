
"""
Retain the legacy table-hash entry point as an MD5 fingerprint alias.

HashTablesMacrosMixin delegates table reading and canonical serialization to a host
fingerprint_table implementation. In the SQL macro composition that implementation is
supplied by SQLPortableMacrosMixin; no hashing or query logic is duplicated here.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Iterable

if TYPE_CHECKING:

    from LiuXin_alpha.databases.api.database_api import DatabaseAPI



class HashTablesMacrosMixin:
    """
    Adapt legacy hash_table calls to the host's table fingerprint service.

    The host must provide fingerprint_table. This mixin neither initializes a database
    nor owns connections, and its db annotation does not create an instance attribute.

    Example:
        ``SQLiteDatabaseMacros(db).hash_table("works", ("work_id", "work_title"))``
        uses the composed portable fingerprint implementation with MD5.
    """

    db: "DatabaseAPI"

    def hash_table(self, target_table: str, columns: Iterable[str]) -> str:
        """
        Request an MD5 fingerprint over the selected columns of the entire table.

        Materialize columns once as a tuple and delegate with algorithm="md5". The SQL
        portable implementation validates table/columns, includes their names in the
        serialized header and orders rows by a recognized ID column, otherwise by
        selected columns. This wrapper adds no filter or custom order. An empty
        selection or unsupported column propagates the delegated validation error. The
        digest is a change-detection fingerprint, not a uniqueness guarantee.

        Example:
            On a configured macro facade, ``macros.hash_table("works",
            ("work_id", "work_title"))`` returns a 32-character hexadecimal digest
            from the portable implementation.


        :param target_table: Table name forwarded to the host fingerprint
            implementation.
        :param columns: Nonempty iterable of column names in serialization order,
            consumed into a tuple.
        :return: The delegated fingerprint string, normally the hexadecimal MD5 digest.
        """
        return self.fingerprint_table(
            target_table,
            columns=tuple(columns),
            algorithm="md5",
        )

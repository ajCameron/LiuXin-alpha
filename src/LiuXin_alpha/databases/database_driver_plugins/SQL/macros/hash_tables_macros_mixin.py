
"""
Hash methods - to monitor tables for changes.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Iterable

if TYPE_CHECKING:

    from LiuXin_alpha.databases.api.database_api import DatabaseAPI



class HashTablesMacrosMixin:
    """
    Hash methods - which create gists and hashes of tables.
    """

    db: "DatabaseAPI"

    def hash_table(self, target_table: str, columns: Iterable[str]) -> str:
        """
        Construct a hash of the given table using the given columns.

        Used to tag snapshots of the current state of the db.
        :param target_table:
        :param columns:
        :return:
        """
        return self.fingerprint_table(
            target_table,
            columns=tuple(columns),
            algorithm="md5",
        )

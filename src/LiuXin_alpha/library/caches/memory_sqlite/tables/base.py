"""
Define shared cache-table persistence and lookup behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise base through a consuming regression::

        python -m pytest -q tests/library/test_unified_library.py
"""
from __future__ import unicode_literals

from LiuXin_alpha.library.caches import BaseTable

from LiuXin_alpha.exceptions import DatabaseIntegrityError

null = object()


class SQLiteBaseTable(BaseTable):
    """
    Provide the sqlitebasetable contract for validated ebook processing.

    Example:
        Exercise SQLiteBaseTable through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """
    def __init__(self, memory_db, name, metadata, link_table=None, custom=False):

        """
        Initialize and validate the sqlitebasetable state.

        Example:
            Exercise SQLiteBaseTable.  init   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param memory_db: Value supplied for memory db under the utility contract.
        :param name: Field, file, function or resource name addressed by the operation.
        :param metadata: Value supplied for metadata under the utility contract.
        :param link_table: Value supplied for link table under the utility contract.
        :param custom: Value supplied for custom under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.memory_db = memory_db

        super(BaseTable).__init__(name=name, metadata=metadata, link_table=link_table, custom=custom)


"""
Coordinate in-memory SQLite cache operations.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise cache through a consuming regression::

        python -m pytest -q tests/library/test_unified_library.py
"""

from LiuXin_alpha.customize.cache import BaseCache

from LiuXin_alpha.library.caches.utils import api
from LiuXin_alpha.library.caches.memory_sqlite import in_memory_db_factory


class SQLiteCache(BaseCache):
    """
    An in-memory cache of the metadata.db file. This class also serves as a threadsafe API for accessing the database. The in-memory cache is, as the database, maintained in normal form for performance.

    Example:
        Exercise SQLiteCache through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """

    def __init__(self, backend):
        """
        Initialize and validate the sqlitecache state.

        Example:
            Exercise SQLiteCache.  init   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param backend: Value supplied for backend under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        super(SQLiteCache, self).__init__(backend=backend)

        self.memory_db = None

    def read_database_to_memory_sqlite(self):
        """
        Preform a read of the database into memory.

        Example:
            Exercise SQLiteCache.read database to memory sqlite through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.memory_db = in_memory_db_factory(self.backend)

    @api
    def init(self):
        """
        Preform initialization tasks needed to read data and startup the cache.

        Example:
            Exercise SQLiteCache.init through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._backend_read_data()

        self.init_called = True

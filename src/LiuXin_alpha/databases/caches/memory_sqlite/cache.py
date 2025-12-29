from LiuXin.databases.caches.base.cache import BaseCache
from LiuXin.databases.caches.utils import api, read_api, write_api
from LiuXin.databases.caches.memory_sqlite import in_memory_db_factory


class SQLiteCache(BaseCache):
    """
    An in-memory cache of the metadata.db file.
    This class also serves as a threadsafe API for accessing the database.
    The in-memory cache is, as the database, maintained in normal form for performance.

    SQLITE is simply used as a way to read and write from metadata.db robustly.
    All table reading/sorting/searching/caching logic is re-implemented. This was necessary for maximum performance and
    flexibility.
    """

    def __init__(self, backend):
        super(SQLiteCache, self).__init__(backend=backend)

        self.memory_db = None

    def read_database_to_memory_sqlite(self):
        """
        Preform a read of the database into memory.
        :return:
        """
        self.memory_db = in_memory_db_factory(self.backend)

    @api
    def init(self):
        """
        Preform initialization tasks needed to read data and startup the cache.
        :return:
        """
        self._backend_read_data()

        self.init_called = True

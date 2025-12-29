from __future__ import unicode_literals

from LiuXin.databases.caches.base.tables import BaseTable

from LiuXin.exceptions import DatabaseIntegrityError

null = object()


class SQLiteBaseTable(BaseTable):
    def __init__(self, memory_db, name, metadata, link_table=None, custom=False):

        self.memory_db = memory_db

        super(BaseTable).__init__(name=name, metadata=metadata, link_table=link_table, custom=custom)

# Todo: Not actually used - kept around for inspiration - either use or remove

# one_many_single_link_table_cache is used to store one to many information about objects on the database
# Each of the elements can be linked to multiple of the other elements

import shutil
from collections import defaultdict

try:
    from LiuXin.customize.ui import run_plugins_on_import
except ImportError:

    def run_plugins_on_import(file):
        return file


from LiuXin.metadata.book.base import calibreMetadata as Metadata

# Todo: This should be over in preferences
from LiuXin.utils.config.config_base import prefs
from LiuXin.utils.ptempfiles import (
    PersistentTemporaryFile,
)


def run_import_plugins(path_or_stream, fmt):
    fmt = fmt.lower()
    if hasattr(path_or_stream, "seek"):
        path_or_stream.seek(0)
        pt = PersistentTemporaryFile("_import_plugin." + fmt)
        shutil.copyfileobj(path_or_stream, pt, 1024**2)
        pt.close()
        path = pt.name
    else:
        path = path_or_stream
    return run_plugins_on_import(path)


def _add_newbook_tag(mi):
    """
    Apply the new book tags (if any) to the given metadata.
    :param mi:
    :return:
    """
    tags = prefs["new_book_tags"]
    if tags:
        if isinstance(mi, Metadata):
            mi.tags = tags
            return

        for tag in [t.strip() for t in tags]:
            if tag:
                if not mi.tags:
                    mi.tags = [tag]
                elif tag not in mi.tags:
                    mi.tags.append(tag)


def api(f):
    f.is_cache_api = True
    return f


def read_api(f):
    f = api(f)
    f.is_read_api = True
    return f


def write_api(f):
    f = api(f)
    f.is_read_api = False
    return f


class OneManyExclusiveLinkTableCache(object):
    """
    Used to store one to one information about rows on the database.
    Only a single element can be stored for each row on the table, but each element of the table can be linked to
    multiple elements.
    """

    def __init__(self, table_name, column_name=None, default_val=None):
        """
        Initialize a cache from a table.
        Loading data from the table is optional - you can just start the cache without any table data.
        :param table_name: The name of a table on the database - pass None if the cache doesn't represent any table on
                           the database.
        :param column_name: The name of a column - data will be read onto the database from this column (if provided)
        :param default_val: Default value to return if there is no existing value for the given row in the cache.
        """
        self.table_name = table_name
        self.column_name = column_name

        self.default_val = default_val
        self.id_val_map = defaultdict(default_factory=self.__default_factory)

    # ------------------------------------------------------------------------------------------------------------------
    #
    # - BASIC ACCESS METHODS
    def from_query(self, query):
        self.id_val_map = dict(query)

    def get_entry(self, item):
        """
        Returns an entry from the table - if there isn't anything to return then return the default value.
        :param item:
        :return:
        """
        return self.id_val_map[item]

    def __getitem__(self, item):
        return self.get_entry(item)

    def set_entry(self, key, value):
        """
        Sets an entry from the table.
        :param key:
        :param value:
        :return:
        """
        self.id_val_map[key] = value

    def __setitem__(self, key, value):
        self.set_entry(key, value)

    #
    # ------------------------------------------------------------------------------------------------------------------
    def __default_factory(self):
        return self.default_val

    def load(self):
        """
        Preform load - reading data of the table - if required.
        :return:
        """
        pass


# one_one_table_cache is used to store one to one information about objects on the database
# Only a single element can be stored for each row on the table


class OneOneTableCache(object):
    """
    Used to store one to one information about rows on the database.
    Only a single element can be stored for each row on the table.
    """

    def __init__(self, table_name, column_name=None, default_val=None):
        """
        Initialize a cache from a table.
        Loading data from the table is optional - you can just start the cache without any table data.
        :param table_name:
        :param column_name:
        :param default_val:
        """
        self.id_val_map = dict()


class LazySortMap(object):
    """
    Used when sorting the database - sort values are only retrieved when required.
    """

    __slots__ = ("default_sort_key", "sort_key_func", "id_map", "cache")

    def __init__(self, default_sort_key, sort_key_func, id_map):
        self.default_sort_key = default_sort_key
        self.sort_key_func = sort_key_func
        self.id_map = id_map
        self.cache = {None: default_sort_key}

    def __call__(self, item_id):
        try:
            return self.cache[item_id]
        except KeyError:
            try:
                val = self.cache[item_id] = self.sort_key_func(self.id_map[item_id])
            except KeyError:
                val = self.cache[item_id] = self.default_sort_key
            return val

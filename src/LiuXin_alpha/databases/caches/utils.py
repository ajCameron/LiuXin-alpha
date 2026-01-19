
"""
Generic tools common for all cache objects.
"""


# one_many_single_link_table_cache is used to store one to many information about objects on the database
# Each of the elements can be linked to multiple of the other elements

from collections import defaultdict



from LiuXin.metadata.book.base import calibreMetadata as Metadata


from LiuXin_alpha.utils.logging import default_log

try:
    from LiuXin_alpha.customize.ui import run_plugins_on_import
except ImportError:

    default_log.exception('LiuXin_alpha.customize.ui - cannot import run_plugins_on_import')

    def run_plugins_on_import(file):
        return file


try:
    from LiuXin_alpha.customize.ui import run_plugins_on_postimport
except ImportError:

    default_log.exception('LiuXin_alpha.customize.ui - cannot import run_plugins_on_postimport')

    def run_plugins_on_postimport(file):
        return file


try:
    from LiuXin_alpha.customize.ui import run_plugins_on_postadd
except ImportError:

    default_log.exception('LiuXin_alpha.customize.ui - cannot import run_plugins_on_postadd')

    def run_plugins_on_postadd(file, *args, **kwargs):
        return file


try:
    from LiuXin_alpha.customize.ui import run_import_plugins
except ImportError:

    default_log.exception('LiuXin_alpha.customize.ui - cannot import run_import_plugins')

    def run_plugins_on_postadd(file, *args, **kwargs):
        return file


def _add_newbook_tag(mi):
    """
    Apply the new book tags (if any) to the given metadata.
    :param mi:
    :return:
    """
    from LiuXin_alpha.preferences import preferences as prefs

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



__all__ = ['run_plugins_on_import', 'run_plugins_on_postimport', 'run_plugins_on_postadd', 'run_import_plugins']



"""
Provide shared library-cache utility operations.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise utils through a consuming regression::

        python -m pytest -q tests/library/test_unified_library.py
"""


# one_many_single_link_table_cache is used to store one-to-many information about objects on the database
# Each of the elements can be linked to multiple of the other elements

from collections import defaultdict

from LiuXin_alpha.utils.logging import default_log

try:
    from LiuXin_alpha.customize.ui import run_plugins_on_import
except ImportError:

    default_log.exception('LiuXin_alpha.customize.ui - cannot import run_plugins_on_import')

    def run_plugins_on_import(file):
        """
        Perform the run plugins on import operation under explicit file-format and conversion rules.

        Example:
            Exercise run plugins on import through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param file: Value supplied for file under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return file


try:
    from LiuXin_alpha.customize.ui import run_plugins_on_postimport
except ImportError:

    default_log.exception('LiuXin_alpha.customize.ui - cannot import run_plugins_on_postimport')

    def run_plugins_on_postimport(file):
        """
        Perform the run plugins on postimport operation under explicit file-format and conversion rules.

        Example:
            Exercise run plugins on postimport through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param file: Value supplied for file under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return file


try:
    from LiuXin_alpha.customize.ui import run_plugins_on_postadd
except ImportError:

    default_log.exception('LiuXin_alpha.customize.ui - cannot import run_plugins_on_postadd')

    def run_plugins_on_postadd(file, *args, **kwargs):
        """
        Perform the run plugins on postadd operation under explicit file-format and conversion rules.

        Example:
            Exercise run plugins on postadd through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param file: Value supplied for file under the utility contract.
        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return file


try:
    from LiuXin_alpha.customize.ui import run_import_plugins
except ImportError:

    default_log.exception('LiuXin_alpha.customize.ui - cannot import run_import_plugins')

    def run_import_plugins(file, *args, **kwargs):
        """
        Perform the run import plugins operation under explicit file-format and conversion rules.

        Example:
            Exercise run import plugins through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param file: Value supplied for file under the utility contract.
        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return file


def _add_newbook_tag(mi):
    """
    Apply the new book tags (if any) to the given metadata.

    Example:
        Exercise  add newbook tag through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py


    :param mi: Metadata object exposed to the template function.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.metadata.book.base import calibreMetadata as Metadata

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
    """
    Perform the api operation under explicit file-format and conversion rules.

    Example:
        Exercise api through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py


    :param f: Value supplied for f under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    f.is_cache_api = True
    return f


def read_api(f):
    """
    Read api under the format's safety and compatibility rules.

    Example:
        Exercise read api through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py


    :param f: Value supplied for f under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    f = api(f)
    f.is_read_api = True
    return f


def write_api(f):
    """
    Write api under the format's safety and compatibility rules.

    Example:
        Exercise write api through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py


    :param f: Value supplied for f under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    f = api(f)
    f.is_read_api = False
    return f


class OneManyExclusiveLinkTableCache(object):
    """
    Used to store one to one information about rows on the database. Only a single element can be stored for each row on the table, but each element of the table can be linked to multiple elements.

    Example:
        Exercise OneManyExclusiveLinkTableCache through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """

    def __init__(self, table_name, column_name=None, default_val=None):
        """
        Initialize a cache from a table.

        Example:
            Exercise OneManyExclusiveLinkTableCache.  init   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param table_name: Value supplied for table name under the utility contract.
        :param column_name: Value supplied for column name under the utility contract.
        :param default_val: Value supplied for default val under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.table_name = table_name
        self.column_name = column_name

        self.default_val = default_val
        self.id_val_map = defaultdict(default_factory=self.__default_factory)

    # ------------------------------------------------------------------------------------------------------------------
    #
    # - BASIC ACCESS METHODS
    def from_query(self, query):
        """
        Perform the from query operation under explicit file-format and conversion rules.

        Example:
            Exercise OneManyExclusiveLinkTableCache.from query through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param query: Search expression parsed or evaluated by the utility.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.id_val_map = dict(query)

    def get_entry(self, item):
        """
        Returns an entry from the table - if there isn't anything to return then return the default value.

        Example:
            Exercise OneManyExclusiveLinkTableCache.get entry through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param item: Value supplied for item under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.id_val_map[item]

    def __getitem__(self, item):
        """
        Perform the getitem operation under explicit file-format and conversion rules.

        Example:
            Exercise OneManyExclusiveLinkTableCache.  getitem   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param item: Value supplied for item under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.get_entry(item)

    def set_entry(self, key, value):
        """
        Sets an entry from the table.

        Example:
            Exercise OneManyExclusiveLinkTableCache.set entry through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param key: Metadata, identifier or local-variable key.
        :param value: Value normalized, stored, formatted or returned.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.id_val_map[key] = value

    def __setitem__(self, key, value):
        """
        Perform the setitem operation under explicit file-format and conversion rules.

        Example:
            Exercise OneManyExclusiveLinkTableCache.  setitem   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param key: Metadata, identifier or local-variable key.
        :param value: Value normalized, stored, formatted or returned.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.set_entry(key, value)

    #
    # ------------------------------------------------------------------------------------------------------------------
    def __default_factory(self):
        """
        Perform the default factory operation under explicit file-format and conversion rules.

        Example:
            Exercise OneManyExclusiveLinkTableCache.  default factory through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.default_val

    def load(self):
        """
        Preform load - reading data of the table - if required.

        Example:
            Exercise OneManyExclusiveLinkTableCache.load through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pass


# one_one_table_cache is used to store one to one information about objects on the database
# Only a single element can be stored for each row on the table


class OneOneTableCache(object):
    """
    Used to store one to one information about rows on the database. Only a single element can be stored for each row on the table.

    Example:
        Exercise OneOneTableCache through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """

    def __init__(self, table_name, column_name=None, default_val=None):
        """
        Initialize a cache from a table.

        Example:
            Exercise OneOneTableCache.  init   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param table_name: Value supplied for table name under the utility contract.
        :param column_name: Value supplied for column name under the utility contract.
        :param default_val: Value supplied for default val under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.id_val_map = dict()


class LazySortMap(object):
    """
    Used when sorting the database - sort values are only retrieved when required.

    Example:
        Exercise LazySortMap through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """

    __slots__ = ("default_sort_key", "sort_key_func", "id_map", "cache")

    def __init__(self, default_sort_key, sort_key_func, id_map):
        """
        Initialize and validate the lazysortmap state.

        Example:
            Exercise LazySortMap.  init   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param default_sort_key: Value supplied for default sort key under the utility
            contract.
        :param sort_key_func: Value supplied for sort key func under the utility contract.
        :param id_map: Value supplied for id map under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.default_sort_key = default_sort_key
        self.sort_key_func = sort_key_func
        self.id_map = id_map
        self.cache = {None: default_sort_key}

    def __call__(self, item_id):
        """
        Perform the call operation under explicit file-format and conversion rules.

        Example:
            Exercise LazySortMap.  call   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param item_id: Value supplied for item id under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            return self.cache[item_id]
        except KeyError:
            try:
                val = self.cache[item_id] = self.sort_key_func(self.id_map[item_id])
            except KeyError:
                val = self.cache[item_id] = self.default_sort_key
            return val



__all__ = ['run_plugins_on_import', 'run_plugins_on_postimport', 'run_plugins_on_postadd', 'run_import_plugins']


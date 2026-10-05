#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

"""
Model in-memory SQLite cached fields.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise fields through a consuming regression::

        python -m pytest -q tests/library/test_unified_library.py
"""
from __future__ import unicode_literals, division, absolute_import, print_function

from collections import defaultdict
from threading import Lock

from LiuXin_alpha.utils.libraries.liuxin_six import iteritems

from LiuXin_alpha.customize.cache.base_tables import null

from LiuXin_alpha.utils.localization import trans as _


from LiuXin_alpha.library.caches import BaseField
from LiuXin_alpha.library.caches import BaseOneToOneField
from LiuXin_alpha.library.caches import BaseCompositeField
from LiuXin_alpha.library.caches import BaseOnDeviceField


class SQLiteField(BaseField):
    """
    Represents a field of the books/titles table.

    Example:
        Exercise SQLiteField through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """

    pass


class SQLiteOneToOneField(SQLiteField, BaseOneToOneField):
    """
    A 1-1 mapping exists between books and these fields. (E.g. the uuid of a book).

    Example:
        Exercise SQLiteOneToOneField through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """

    def for_book(self, book_id, default_value=None):
        """
        Return the table value for the book.

        Example:
            Exercise SQLiteOneToOneField.for book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.table.get_value(rid=book_id, default_value=default_value)

    def __iter__(self):
        # Todo: This is stupid - call self.table.table self.table.name
        """
        Perform the iter operation under explicit file-format and conversion rules.

        Example:
            Exercise SQLiteOneToOneField.  iter   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: An iterator yielding the normalized values described above.
        """
        unique_book_ids = self.table.memory_db.macros.get_unique_value(table=self.table.table, column=self.table.id_col)
        for book_id in unique_book_ids:
            yield book_id


class SQLiteCompositeField(SQLiteField, BaseCompositeField):
    """
    A composite field uses data from other fields to produce a composite value.

    Example:
        Exercise SQLiteCompositeField through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """

    pass


class SQLiteOnDeviceField(BaseOnDeviceField):
    """
    Provide the sqliteondevicefield contract for validated ebook processing.

    Example:
        Exercise SQLiteOnDeviceField through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """
    def __init__(self, name, table, bools_are_tristate):

        """
        Initialize and validate the sqliteondevicefield state.

        Example:
            Exercise SQLiteOnDeviceField.  init   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param table: Value supplied for table under the utility contract.
        :param bools_are_tristate: Value supplied for bools are tristate under the utility
            contract.
        :return: None; validated state is stored on the receiving object.
        """
        super(SQLiteOnDeviceField, self).__init__(name, table, bools_are_tristate)

        self.cache = {}
        self._lock = Lock()

    def clear_caches(self, book_ids=None):
        """
        Perform the clear caches operation under explicit file-format and conversion rules.

        Example:
            Exercise SQLiteOnDeviceField.clear caches through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_ids: Book identities included in the batched read operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        with self._lock:
            if book_ids is None:
                self.cache.clear()
            else:
                for book_id in book_ids:
                    self.cache.pop(book_id, None)

    def book_on_device(self, book_id):
        """
        Perform the book on device operation under explicit file-format and conversion rules.

        Example:
            Exercise SQLiteOnDeviceField.book on device through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        with self._lock:
            ans = self.cache.get(book_id, null)
        if ans is null and callable(self.book_on_device_func):
            ans = self.book_on_device_func(book_id)
            with self._lock:
                self.cache[book_id] = ans
        return None if ans is null else ans

    def set_book_on_device_func(self, func):
        """
        Set book on device func under the format's safety and compatibility rules.

        Example:
            Exercise SQLiteOnDeviceField.set book on device func through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param func: Value supplied for func under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.book_on_device_func = func

    def for_book(self, book_id, default_value=None):
        """
        Perform the for book operation under explicit file-format and conversion rules.

        Example:
            Exercise SQLiteOnDeviceField.for book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        loc = []
        count = 0
        on = self.book_on_device(book_id)
        if on is not None:
            m, a, b, count = on[:4]
            if m is not None:
                loc.append(_("Main"))
            if a is not None:
                loc.append(_("Card A"))
            if b is not None:
                loc.append(_("Card B"))
        return ", ".join(loc) + ((" (%s books)" % count) if count > 1 else "")

    def iter_searchable_values(self, get_metadata, candidates, default_value=None):
        """
        Iterate over searchable values under the format's safety and compatibility rules.

        Example:
            Exercise SQLiteOnDeviceField.iter searchable values through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param get_metadata: Value supplied for get metadata under the utility contract.
        :param candidates: Value supplied for candidates under the utility contract.
        :param default_value: Value supplied for default value under the utility contract.
        :return: An iterator yielding the normalized values described above.
        """
        val_map = defaultdict(set)
        for book_id in candidates:
            val_map[self.for_book(book_id, default_value=default_value)].add(book_id)
        for val, book_ids in iteritems(val_map):
            yield val, book_ids


def sqlite_create_field(name, table, bools_are_tristate):
    """
    Takes a table field and the other properties needed to instantiate it - constructs the Table object and returns it.

    Example:
        Exercise sqlite create field through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py


    :param name: Field, file, function or resource name addressed by the operation.
    :param table: Value supplied for table under the utility contract.
    :param bools_are_tristate: Value supplied for bools are tristate under the utility
        contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    pass

    # cls = {
    #     ONE_ONE: CalibreOneToOneField,
    #     ONE_MANY: CalibreOneToManyField,
    #     MANY_ONE: CalibreManyToOneField,
    #     MANY_MANY: CalibreManyToManyField,
    # }[table.table_type]
    #
    # if name == 'authors':
    #     cls = CalibreAuthorsField
    # elif name in ["comments", "publisher"]:
    #     cls = CalibreOneToOneField
    # elif name == 'ondevice':
    #     cls = CalibreOnDeviceField
    # elif name == 'formats':
    #     cls = CalibreFormatsField
    # elif name == 'identifiers':
    #     cls = CalibreIdentifiersField
    # elif name == 'tags':
    #     cls = CalibreTagsField
    # elif name in ('cover', 'covers'):
    #     cls = CalibreCoversField
    # elif name == "languages":
    #     cls = CalibreLanguagesField
    # elif table.metadata['datatype'] == 'composite':
    #     cls = CalibreCompositeField
    # elif table.metadata['datatype'] == 'series':
    #     cls = CalibreSeriesField
    # return cls(name, table, bools_are_tristate)

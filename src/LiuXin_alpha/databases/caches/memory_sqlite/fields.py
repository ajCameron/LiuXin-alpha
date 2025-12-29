#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

from __future__ import unicode_literals, division, absolute_import, print_function

from collections import defaultdict
from threading import Lock

from six import iteritems

from LiuXin.databases.caches.base.tables import (
    ONE_ONE,
    MANY_ONE,
    MANY_MANY,
    ONE_MANY,
    null,
)

from LiuXin.utils.localization import _


from LiuXin.databases.caches.base.fields import BaseField
from LiuXin.databases.caches.base.fields import BaseOneToOneField
from LiuXin.databases.caches.base.fields import BaseCompositeField
from LiuXin.databases.caches.base.fields import BaseOnDeviceField


class SQLiteField(BaseField):
    """
    Represents a field of the books/titles table.
    """

    pass


class SQLiteOneToOneField(SQLiteField, BaseOneToOneField):
    """
    A 1-1 mapping exists between books and these fields. (E.g. the uuid of a book).
    """

    def for_book(self, book_id, default_value=None):
        """
        Return the table value for the book.
        :param book_id:
        :param default_value:
        :return:
        """
        return self.table.get_value(rid=book_id, default_value=default_value)

    def __iter__(self):
        # Todo: This is stupid - call self.table.table self.table.name
        unique_book_ids = self.table.memory_db.macros.get_unique_value(table=self.table.table, column=self.table.id_col)
        for book_id in unique_book_ids:
            yield book_id


class SQLiteCompositeField(SQLiteField, BaseCompositeField):
    """
    A composite field uses data from other fields to produce a composite value.
    """

    pass


class SQLiteOnDeviceField(BaseOnDeviceField):
    def __init__(self, name, table, bools_are_tristate):

        super(SQLiteOnDeviceField, self).__init__(name, table, bools_are_tristate)

        self.cache = {}
        self._lock = Lock()

    def clear_caches(self, book_ids=None):
        with self._lock:
            if book_ids is None:
                self.cache.clear()
            else:
                for book_id in book_ids:
                    self.cache.pop(book_id, None)

    def book_on_device(self, book_id):
        with self._lock:
            ans = self.cache.get(book_id, null)
        if ans is null and callable(self.book_on_device_func):
            ans = self.book_on_device_func(book_id)
            with self._lock:
                self.cache[book_id] = ans
        return None if ans is null else ans

    def set_book_on_device_func(self, func):
        self.book_on_device_func = func

    def for_book(self, book_id, default_value=None):
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
        val_map = defaultdict(set)
        for book_id in candidates:
            val_map[self.for_book(book_id, default_value=default_value)].add(book_id)
        for val, book_ids in iteritems(val_map):
            yield val, book_ids


def sqlite_create_field(name, table, bools_are_tristate):
    """
    Takes a table field and the other properties needed to instantiate it - constructs the Table object and returns it.
    :param name:
    :param table:
    :param bools_are_tristate:
    :return:
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

#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

from __future__ import unicode_literals, division, absolute_import, print_function

import operator
import weakref
from functools import partial
from builtins import map as imap

from six import iteritems
from six import iterkeys
from six import itervalues

from LiuXin.databases.write import uniq

from LiuXin.metadata import title_sort

from LiuXin.utils.config.config_base import tweaks, prefs
from LiuXin.utils.lx_libraries.liuxin_six import memory_range
from LiuXin.utils.logger import default_log

from LiuXin.utils.lx_libraries.liuxin_six import six_unicode
from LiuXin.utils.lx_libraries.liuxin_six import six_zip as izip


__license__ = "GPL v3"
__copyright__ = "2011, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"


class ViewMetadata(object):
    """
    Stored metadata about a view - the FIELD_MAP needed to located the positions of each of the columns in it and the
    lines with form the SQL statement to construct it.
    """

    def __init__(self, FIELD_MAP, sql_lines, custom_columns, field_metadata):
        """
        Stores all information about the view to be created.

        :param FIELD_MAP: A dictionary keyed with the position of the column in the view and valued with the label of
                          that column.
        :param sql_lines: The sql which will be used to actually create the view
        :param custom_columns: A CustomColumns object which stores information on the custom columns to be included in
                               the view.
        :param field_metadata: An object which stores metadata about the individual fields in the view.
        :return:
        """
        self.FIELD_MAP = FIELD_MAP
        self.sql_lines = sql_lines
        self.custom_columns = custom_columns
        self.field_metadata = field_metadata


def sanitize_sort_field_name(field_metadata, field):
    """

    :param field_metadata:
    :param field:
    :return:
    """
    field = field_metadata.search_term_to_field_key(field.lower().strip())
    # translate some fields to their hidden equivalent
    field = {"title": "sort", "authors": "author_sort"}.get(field, field)
    return field


class CalibreMarkedVirtualField(object):
    def __init__(self, marked_ids):
        self.marked_ids = marked_ids

    def iter_searchable_values(self, get_metadata, candidates, default_value=None):
        for book_id in candidates:
            yield self.marked_ids.get(book_id, default_value), {book_id}

    def sort_keys_for_books(self, get_metadata, lang_map):
        g = self.marked_ids.get
        return lambda book_id: g(book_id, None)


class TableRow(object):
    def __init__(self, book_id, view):
        self.book_id = book_id
        self.view = weakref.ref(view)
        self.column_count = view.column_count

    def __getitem__(self, obj):
        view = self.view()
        if isinstance(obj, slice):
            return [view._field_getters[c](self.book_id) for c in memory_range(*obj.indices(len(view._field_getters)))]
        else:
            return view._field_getters[obj](self.book_id)

    def __len__(self):
        return self.column_count

    def __iter__(self):
        for i in memory_range(self.column_count):
            yield self[i]


def format_is_multiple(x, sep=",", repl=None):
    """
    Provides a format for display if the value has multiple components.
    :param x:
    :param sep:
    :param repl:
    :return:
    """
    if not x:
        return None
    if repl is not None:
        x = (y.replace(sep, repl) for y in x)
    return sep.join(x)


def format_identifiers(x):
    """
    Make a string representation of a set of identifiers.
    :param x:
    :return:
    """
    if not x:
        return None
    return ",".join("%s:%s" % (k, v) for k, v in iteritems(x))


class CalibreView(object):
    """
    A table view of the database, with rows and columns (some of which are made of formatted selections from others rows
    and columns).
    Used to actually display contents of the books/titles combined tables.
    """

    def __init__(self, cache):
        """
        Needs to be able to thread safely read/write the database - so runs off a cache.
        :param cache:
        :return:
        """
        self.cache = cache

        self.marked_ids = {}
        self.marked_listeners = {}

        self.search_restriction_book_count = 0
        self.search_restriction = self.base_restriction = ""
        self.search_restriction_name = self.base_restriction_name = ""

        self._field_getters = {}
        self.column_count = len(cache.FIELD_MAP)

        for col, idx in iteritems(cache.FIELD_MAP):
            label, fmt = col, lambda x: x
            func = {
                "id": self._get_id,
                "au_map": self.get_author_data,
                "ondevice": self.get_ondevice,
                "marked": self.get_marked,
                "series_sort": self.get_series_sort,
            }.get(col, self._get)

            if isinstance(col, int):
                label = self.cache.backend.custom_column_num_map[col]["label"]
                label = self.cache.backend.field_metadata.custom_field_prefix + label

            if label.endswith("_index"):
                try:
                    num = int(label.partition("_")[0])
                except ValueError:
                    pass  # series_index
                else:
                    label = self.cache.backend.custom_column_num_map[num]["label"]
                    label = self.cache.backend.field_metadata.custom_field_prefix + label + "_index"

            fm = self.field_metadata[label]
            if label == "authors":
                fmt = partial(format_is_multiple, repl="|")
            elif label in {"tags", "languages", "formats"}:
                fmt = format_is_multiple
            elif label == "cover":
                fmt = bool
            elif label == "identifiers":
                fmt = format_identifiers
            elif fm["datatype"] == "text" and fm["is_multiple"]:
                sep = fm["is_multiple"]["cache_to_list"]
                if sep not in {"&", "|"}:
                    sep = "|"
                fmt = partial(format_is_multiple, sep=sep)

            self._field_getters[idx] = partial(func, label, fmt=fmt) if func == self._get else func

        self._map = tuple(sorted(self.cache.all_book_ids()))
        self._map_filtered = tuple(self._map)
        self.full_map_is_sorted = True
        self.sort_history = [("id", True)]

    def add_marked_listener(self, func):
        """
        Add a listener function to the view. Miantains a weakref so that it won;t block if this is deleted.
        :param func:
        :return:
        """
        self.marked_listeners[id(func)] = weakref.ref(func)

    def add_to_sort_history(self, items):
        """
        Add to the top of the current sort history.
        :param items:
        :return:
        """
        self.sort_history = uniq((list(items) + list(self.sort_history)), operator.itemgetter(0))[
            : tweaks["maximum_resort_levels"]
        ]

    def count(self):
        """
        The number of items in the current view.
        :return:
        """
        return len(self._map)

    def get_property(self, id_or_index, index_is_id=False, loc=-1):
        """
        Get the given property of the book from either it's id or some other unique id - given by the location.
        :param id_or_index: The unique thing to search with
        :param index_is_id: Is the id the index?
        :param loc: By default -1 - the id of the book
        :return:
        """
        book_id = id_or_index if index_is_id else self._map_filtered[id_or_index]
        return self._field_getters[loc](book_id)

    def sanitize_sort_field_name(self, field):
        """
        Sanitize the name for the sort field.
        :param field:
        :return:
        """
        return sanitize_sort_field_name(self.field_metadata, field)

    @property
    def field_metadata(self):
        """
        Returns the field metadata from the cache object.
        :return:
        """
        return self.cache.field_metadata

    def _get_id(self, idx, index_is_id=True):
        """
        If the index_is_id, returns the index if it exists in the view, or raise an exception if it isn't.
        If not index_is_id tries to find the book from the given data and return the id identified from that identifier.
        :param idx:
        :param index_is_id:
        :return:
        """
        if index_is_id and not self.cache.has_id(idx):
            raise IndexError("No book with id %s present" % idx)
        return idx if index_is_id else self.index_to_id(idx)

    def has_id(self, book_id):
        """
        Uses the cache has_id method to check if the given book_id is currently in the cache.
        :param book_id:
        :return:
        """
        return self.cache.has_id(book_id)

    def __getitem__(self, row):
        return TableRow(self._map_filtered[row], self)

    def __len__(self):
        return len(self._map_filtered)

    def __iter__(self):
        for book_id in self._map_filtered:
            yield TableRow(book_id, self)

    def iterall(self):
        for book_id in self.iterallids():
            yield TableRow(book_id, self)

    def iterallids(self):
        for book_id in sorted(self._map):
            yield book_id

    def tablerow_for_id(self, book_id):
        return TableRow(book_id, self)

    def get_field_map_field(self, row, col, index_is_id=True):
        """
        Supports the legacy FIELD_MAP interface for getting metadata. Do not use
        in new code.
        """
        getter = self._field_getters[col]
        return getter(row, index_is_id=index_is_id)

    def index_to_id(self, idx):
        """
        Return the id of a book from the given index.
        :param idx:
        :return:
        """
        return self._map_filtered[idx]

    def id_to_index(self, book_id):
        """
        Get the id from the given book_id.
        :param book_id:
        :return:
        """
        return self._map_filtered.index(book_id)

    row = index_to_id

    def index(self, book_id, cache=False):
        x = self._map if cache else self._map_filtered
        return x.index(book_id)

    def _get(self, field, idx, index_is_id=True, default_value=None, fmt=lambda x: x):
        """
        Get a field value from an index with a given default value and the option of a format function to transform the
        output.
        :param field:
        :param idx:
        :param index_is_id:
        :param default_value:
        :param fmt:
        :return:
        """
        id_ = idx if index_is_id else self.index_to_id(idx)
        if index_is_id and not self.cache.has_id(id_):
            raise IndexError("No book with id %s present" % idx)
        return fmt(self.cache.field_for(field, id_, default_value=default_value))

    def get_series_sort(self, idx, index_is_id=True, default_value=""):
        """
        Get the series sort index for the particular book.
        :param idx:
        :param index_is_id:
        :param default_value:
        :return:
        """
        if default_value:
            info_str = "an unexpected default_value was provided"
            default_log.log_variables(info_str, "INFO", ("default_value", default_value))

        book_id = idx if index_is_id else self.index_to_id(idx)
        with self.cache.safe_read_lock:
            lang_map = self.cache.fields["languages"].book_value_map
            lang = lang_map.get(book_id, None) or None
            if lang:
                lang = lang[0]
            return title_sort(
                self.cache._field_for("series", book_id, default_value=""),
                order=tweaks["title_series_sorting"],
                lang=lang,
            )

    def get_ondevice(self, idx, index_is_id=True, default_value=""):
        """
        Is a book with the given id or index on the currently connected device?
        :param idx:
        :param index_is_id:
        :param default_value:
        :return:
        """
        id_ = idx if index_is_id else self.index_to_id(idx)
        return self.cache.field_for("ondevice", id_, default_value=default_value)

    def get_marked(self, idx, index_is_id=True, default_value=None):
        id_ = idx if index_is_id else self.index_to_id(idx)
        return self.marked_ids.get(id_, default_value)

    def get_author_data(self, idx, index_is_id=True, default_value=None):
        """
        Return a serialized string of author data for writing into a file.
        :param idx: The index or id of the book
        :param index_is_id: Is the given index an id
        :param default_value: In the event of the author data not being retrievable return this.
        :return:
        """
        id_ = idx if index_is_id else self.index_to_id(idx)
        with self.cache.safe_read_lock:
            ids = self.cache._field_ids_for("authors", id_)
            adata = self.cache._author_data(ids)
            ans = [
                ":::".join((adata[aid]["name"], adata[aid]["sort"], adata[aid]["link"])) for aid in ids if aid in adata
            ]
        return ":#:".join(ans) if ans else default_value

    def _do_sort(self, ids_to_sort, fields=(), subsort=False):
        fields = [(sanitize_sort_field_name(self.field_metadata, x), bool(y)) for x, y in fields]
        keys = self.field_metadata.sortable_field_keys()
        fields = [x for x in fields if x[0] in keys]
        if subsort and "sort" not in [x[0] for x in fields]:
            fields += [("sort", True)]
        if not fields:
            fields = [("timestamp", False)]

        return self.cache.multisort(
            fields,
            ids_to_sort=ids_to_sort,
            virtual_fields={"marked": CalibreMarkedVirtualField(self.marked_ids)},
        )

    def multisort(self, fields=None, subsort=False, only_ids=None):
        if fields is None:
            fields = []
        sorted_book_ids = self._do_sort(self._map if only_ids is None else only_ids, fields=fields, subsort=subsort)
        if only_ids is None:
            self._map = tuple(sorted_book_ids)
            self.full_map_is_sorted = True
            self.add_to_sort_history(fields)
            if len(self._map_filtered) == len(self._map):
                self._map_filtered = tuple(self._map)
            else:
                fids = frozenset(self._map_filtered)
                self._map_filtered = tuple(i for i in self._map if i in fids)
        else:
            smap = {book_id: i for i, book_id in enumerate(sorted_book_ids)}
            only_ids.sort(key=smap.get)

    def incremental_sort(self, fields=(), subsort=False):
        if len(self._map) == len(self._map_filtered):
            return self.multisort(fields=fields, subsort=subsort)

        self._map_filtered = tuple(self._do_sort(self._map_filtered, fields=fields, subsort=subsort))
        self.full_map_is_sorted = False
        self.add_to_sort_history(fields)

    def search(self, query, return_matches=False, sort_results=True):
        ans = self.search_getting_ids(
            query,
            self.search_restriction,
            set_restriction_count=True,
            sort_results=sort_results,
        )
        if return_matches:
            return ans
        self._map_filtered = tuple(ans)

    def _build_restriction_string(self, restriction):
        if self.base_restriction:
            if restriction:
                return "(%s) and (%s)" % (self.base_restriction, restriction)
            else:
                return self.base_restriction
        else:
            return restriction

    def search_getting_ids(
        self,
        query,
        search_restriction,
        set_restriction_count=False,
        use_virtual_library=True,
        sort_results=True,
    ):
        """
        Search the cache with the given query - return the results as an ordered list.
        :param query: Query string - will be parsed and applied to the entries in the cache.
        :param search_restriction: A restriction to apply the search - overridden if you choose to use a virtual
                                   library.
        :param set_restriction_count:
        :param use_virtual_library: If True then applies the virtual library restriction to the search
        :param sort_results:
        :return:
        """
        if use_virtual_library:
            search_restriction = self._build_restriction_string(search_restriction)
        if not query or not query.strip():
            q = search_restriction
        else:
            q = query
            if search_restriction:
                q = "(%s) and (%s)" % (search_restriction, query)
        if not q:
            if set_restriction_count:
                self.search_restriction_book_count = len(self._map)
            rv = list(self._map)
            if sort_results and not self.full_map_is_sorted:
                rv = self._do_sort(rv, fields=self.sort_history)
                self._map = tuple(rv)
                self.full_map_is_sorted = True
            return rv
        matches = self.cache.search(
            query,
            search_restriction,
            virtual_fields={"marked": CalibreMarkedVirtualField(self.marked_ids)},
        )
        if len(matches) == len(self._map):
            rv = list(self._map)
        else:
            rv = [x for x in self._map if x in matches]

        if sort_results and not self.full_map_is_sorted:
            # We need to sort the search results
            if matches.issubset(frozenset(self._map_filtered)):
                rv = [x for x in self._map_filtered if x in matches]
            else:
                rv = self._do_sort(rv, fields=self.sort_history)
            if len(matches) == len(self._map):
                # Sort complete - update internal map
                self._map = tuple(rv)
                self.full_map_is_sorted = True
        if set_restriction_count and q == search_restriction:
            self.search_restriction_book_count = len(rv)
        return rv

    def get_search_restriction(self):
        return self.search_restriction

    def set_search_restriction(self, s):
        self.search_restriction = s

    def get_base_restriction(self):
        return self.base_restriction

    def set_base_restriction(self, s):
        self.base_restriction = s

    def get_base_restriction_name(self):
        return self.base_restriction_name

    def set_base_restriction_name(self, s):
        self.base_restriction_name = s

    def get_search_restriction_name(self):
        return self.search_restriction_name

    def set_search_restriction_name(self, s):
        self.search_restriction_name = s

    def search_restriction_applied(self):
        return bool(self.search_restriction) or bool(self.base_restriction)

    def get_search_restriction_book_count(self):
        return self.search_restriction_book_count

    def change_search_locations(self, newlocs):
        self.cache.change_search_locations(newlocs)

    def set_marked_ids(self, id_dict):
        """
        ids in id_dict are "marked". They can be searched for by using the search term ``marked:true``. Pass in an empty
        dictionary or set to clear marked ids.

        :param id_dict: Either a dictionary mapping ids to values or a set of ids. In the latter case, the value is set
        to 'true' for all ids. If a mapping is provided, then the search can be used to search for particular values:
        ``marked:value``
        """
        old_marked_ids = set(self.marked_ids)
        if not hasattr(id_dict, "items"):
            # Simple list. Make it a dict of string 'true'
            self.marked_ids = dict.fromkeys(id_dict, "true")
        else:
            # Ensure that all the items in the dict are text
            self.marked_ids = dict(izip(iterkeys(id_dict), imap(six_unicode, itervalues(id_dict))))

        # This invalidates all searches in the cache even though the cache may be shared by multiple views. This is not
        # ideal, but...
        cmids = set(self.marked_ids)
        self.cache.clear_search_caches(old_marked_ids | cmids)
        if old_marked_ids != cmids:
            for funcref in itervalues(self.marked_listeners):
                func = funcref()
                if func is not None:
                    func(old_marked_ids, cmids)

    def toggle_marked_ids(self, book_ids):
        book_ids = set(book_ids)
        mids = set(self.marked_ids)
        common = mids.intersection(book_ids)
        self.set_marked_ids((mids | book_ids) - common)

    def refresh(self, field=None, ascending=True, clear_caches=True, do_search=True):
        self._map = tuple(sorted(self.cache.all_book_ids()))
        self._map_filtered = tuple(self._map)
        self.full_map_is_sorted = True
        self.sort_history = [("id", True)]
        if clear_caches:
            self.cache.clear_caches()
        if field is not None:
            self.sort(field, ascending)
        if do_search and (self.search_restriction or self.base_restriction):
            self.search("", return_matches=False)

    def refresh_ids(self, ids):
        self.cache.clear_caches(book_ids=ids)
        try:
            return list(map(self.id_to_index, ids))
        except ValueError:
            pass
        return None

    def remove(self, book_id):
        try:
            self._map = tuple(bid for bid in self._map if bid != book_id)
        except ValueError:
            pass
        try:
            self._map_filtered = tuple(bid for bid in self._map_filtered if bid != book_id)
        except ValueError:
            pass

    def books_deleted(self, ids):
        for book_id in ids:
            self.remove(book_id)

    def books_added(self, ids):
        ids = tuple(ids)
        self._map = ids + self._map
        self._map_filtered = ids + self._map_filtered
        if prefs["mark_new_books"]:
            self.toggle_marked_ids(ids)

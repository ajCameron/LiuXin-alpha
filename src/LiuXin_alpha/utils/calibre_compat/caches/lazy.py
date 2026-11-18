#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

"""
Provide lazy wrappers for library metadata and query results.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise lazy through a consuming regression::

        python -m pytest -q tests/library/test_unified_library.py
"""

from __future__ import division, absolute_import, print_function, annotations

import weakref
from collections.abc import MutableMapping, MutableSequence
from copy import deepcopy

from LiuXin_alpha.utils.localization import trans as _

from LiuXin_alpha.metadata.book.base import (
    calibreMetadata,
    SIMPLE_GET,
    NULL_VALUES,

)
from LiuXin_alpha.metadata.book import TOP_LEVEL_IDENTIFIERS, ALL_METADATA_FIELDS

from LiuXin_alpha.metadata.book.formatter import SafeFormat

from LiuXin_alpha.utils.date import utcnow
from LiuXin_alpha.utils.logging import default_log

# Py2/Py3 compatibility layer
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode, dict_iterkeys as iterkeys

__license__ = "GPL v3"
__copyright__ = "2012, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"


def resolved(f):
    """
    Decorator to call _resolve on access.

    Example:
        Exercise resolved through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py


    :param f: Value supplied for f under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    def wrapper(self, *args, **kwargs):
        """
        Perform the wrapper operation under explicit file-format and conversion rules.

        Example:
            Exercise resolved.wrapper through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param self: Value supplied for self under the utility contract.
        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if getattr(self, "_must_resolve", True):
            self._resolve()
            self._must_resolve = False
        return f(self, *args, **kwargs)

    return wrapper


class MutableBase:
    """
    Mutable base class.

    Example:
        Exercise MutableBase through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """
    def __int__(self) -> None:
        """
        Perform the int operation under explicit file-format and conversion rules.

        Example:
            Exercise MutableBase.  int   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._values = []

    def __str__(self):
        """
        Perform the str operation under explicit file-format and conversion rules.

        Example:
            Exercise MutableBase.  str   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return str(self._values)

    def __repr__(self):
        """
        Perform the repr operation under explicit file-format and conversion rules.

        Example:
            Exercise MutableBase.  repr   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return repr(self._values)

    def __unicode__(self):
        """
        Perform the unicode operation under explicit file-format and conversion rules.

        Example:
            Exercise MutableBase.  unicode   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return six_unicode(self._values)

    def __len__(self):
        """
        Perform the len operation under explicit file-format and conversion rules.

        Example:
            Exercise MutableBase.  len   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return len(self._values)

    def __iter__(self):
        """
        Perform the iter operation under explicit file-format and conversion rules.

        Example:
            Exercise MutableBase.  iter   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return iter(self._values)

    def __contains__(self, key):
        """
        Perform the contains operation under explicit file-format and conversion rules.

        Example:
            Exercise MutableBase.  contains   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param key: Metadata, identifier or local-variable key.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return key in self._values

    def __getitem__(self, fmt):
        """
        Perform the getitem operation under explicit file-format and conversion rules.

        Example:
            Exercise MutableBase.  getitem   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param fmt: Date, number or template format specification.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self._values[fmt]

    def __setitem__(self, key, val):
        """
        Perform the setitem operation under explicit file-format and conversion rules.

        Example:
            Exercise MutableBase.  setitem   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param key: Metadata, identifier or local-variable key.
        :param val: Template or metadata value evaluated by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._values[key] = val

    def __delitem__(self, key):
        """
        Perform the delitem operation under explicit file-format and conversion rules.

        Example:
            Exercise MutableBase.  delitem   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param key: Metadata, identifier or local-variable key.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        del self._values[key]


class FormatMetadata(MutableBase, MutableMapping):
    """
    Maps available formats to the ids that correspond to them.

    Example:
        Exercise FormatMetadata through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """
    def __init__(self, db, id_, formats):
        """
        Initialize and validate the formatmetadata state.

        Example:
            Exercise FormatMetadata.  init   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param db: Value supplied for db under the utility contract.
        :param id_: Value supplied for id under the utility contract.
        :param formats: Value supplied for formats under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self._dbwref = weakref.ref(db)
        self._id = id_
        self._formats = formats

    def _resolve(self):
        """
        Perform the resolve operation under explicit file-format and conversion rules.

        Example:
            Exercise FormatMetadata. resolve through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        db = self._dbwref()
        self._values = {}
        for f in self._formats:
            try:
                self._values[f] = db.format_metadata(self._id, f)
            except Exception as e:
                err_str = "Attempted call to format metadata failed"
                default_log.log_exception(err_str, e, "DEBUG")


class FormatsList(MutableBase, MutableSequence):
    """
    Provide the formatslist contract for validated ebook processing.

    Example:
        Exercise FormatsList through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """
    def __init__(self, formats, format_metadata):
        """
        Initialize and validate the formatslist state.

        Example:
            Exercise FormatsList.  init   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param formats: Value supplied for formats under the utility contract.
        :param format_metadata: Value supplied for format metadata under the utility
            contract.
        :return: None; validated state is stored on the receiving object.
        """
        self._formats = formats
        self._format_metadata = format_metadata

    def _resolve(self):
        """
        Perform the resolve operation under explicit file-format and conversion rules.

        Example:
            Exercise FormatsList. resolve through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._values = [f for f in self._formats if f in self._format_metadata]

    def insert(self, idx, val):
        """
        Perform the insert operation under explicit file-format and conversion rules.

        Example:
            Exercise FormatsList.insert through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param idx: Value supplied for idx under the utility contract.
        :param val: Template or metadata value evaluated by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._values.insert(idx, val)


# }}}

# Lazy metadata getters {{{
ga = object.__getattribute__
sa = object.__setattr__


def simple_getter(field, default_value=None):
    """
    Returns a function which serves as a simplified retrieval method for a given field.

    Example:
        Exercise simple getter through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py


    :param field: Metadata or template field addressed by the operation.
    :param default_value: Value supplied for default value under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """

    def func(dbref, book_id, cache):
        """
        Perform the func operation under explicit file-format and conversion rules.

        Example:
            Exercise simple getter.func through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param dbref: Value supplied for dbref under the utility contract.
        :param book_id: Value supplied for book id under the utility contract.
        :param cache: Value supplied for cache under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            return cache[field]
        except KeyError:
            db = dbref()
            cache[field] = ret = db.field_for(field, book_id, default_value=default_value)
            return ret

    return func


def pp_getter(field, postprocess, default_value=None):
    """
    A getter with the option to postprocess the result of the get.

    Example:
        Exercise pp getter through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py


    :param field: Metadata or template field addressed by the operation.
    :param postprocess: Value supplied for postprocess under the utility contract.
    :param default_value: Value supplied for default value under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """

    def func(dbref, book_id, cache):
        """
        Perform the func operation under explicit file-format and conversion rules.

        Example:
            Exercise pp getter.func through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param dbref: Value supplied for dbref under the utility contract.
        :param book_id: Value supplied for book id under the utility contract.
        :param cache: Value supplied for cache under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            return cache[field]
        except KeyError:
            db = dbref()
            cache[field] = ret = postprocess(db.field_for(field, book_id, default_value=default_value))
            return ret

    return func


def adata_getter(field):
    """
    Perform the adata getter operation under explicit file-format and conversion rules.

    Example:
        Exercise adata getter through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py


    :param field: Metadata or template field addressed by the operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    def func(dbref, book_id, cache):
        """
        Perform the func operation under explicit file-format and conversion rules.

        Example:
            Exercise adata getter.func through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param dbref: Value supplied for dbref under the utility contract.
        :param book_id: Value supplied for book id under the utility contract.
        :param cache: Value supplied for cache under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            author_ids, adata = cache["adata"]
        except KeyError:
            db = dbref()
            with db.safe_read_lock:
                author_ids = db._field_ids_for("authors", book_id)
                adata = db._author_data(author_ids)
            cache["adata"] = (author_ids, adata)
        k = "sort" if field == "author_sort_map" else "link"
        return {adata[i]["name"]: adata[i][k] for i in author_ids}

    return func


def dt_getter(field):
    """
    Perform the dt getter operation under explicit file-format and conversion rules.

    Example:
        Exercise dt getter through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py


    :param field: Metadata or template field addressed by the operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    def func(dbref, book_id, cache):
        """
        Perform the func operation under explicit file-format and conversion rules.

        Example:
            Exercise dt getter.func through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param dbref: Value supplied for dbref under the utility contract.
        :param book_id: Value supplied for book id under the utility contract.
        :param cache: Value supplied for cache under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            return cache[field]
        except KeyError:
            db = dbref()
            cache[field] = ret = db.field_for(field, book_id, default_value=utcnow())
            return ret

    return func


def item_getter(field, default_value=None, key=0):
    """
    Perform the item getter operation under explicit file-format and conversion rules.

    Example:
        Exercise item getter through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py


    :param field: Metadata or template field addressed by the operation.
    :param default_value: Value supplied for default value under the utility contract.
    :param key: Metadata, identifier or local-variable key.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    def func(dbref, book_id, cache):
        """
        Perform the func operation under explicit file-format and conversion rules.

        Example:
            Exercise item getter.func through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param dbref: Value supplied for dbref under the utility contract.
        :param book_id: Value supplied for book id under the utility contract.
        :param cache: Value supplied for cache under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            return cache[field]
        except KeyError:
            db = dbref()
            ret = cache[field] = db.field_for(field, book_id, default_value=default_value)
            try:
                return ret[key]
            except (IndexError, KeyError):
                return default_value

    return func


def fmt_getter(field):
    """
    Perform the fmt getter operation under explicit file-format and conversion rules.

    Example:
        Exercise fmt getter through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py


    :param field: Metadata or template field addressed by the operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    def func(dbref, book_id, cache):
        """
        Perform the func operation under explicit file-format and conversion rules.

        Example:
            Exercise fmt getter.func through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param dbref: Value supplied for dbref under the utility contract.
        :param book_id: Value supplied for book id under the utility contract.
        :param cache: Value supplied for cache under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            format_metadata = cache["format_metadata"]
        except KeyError:
            db = dbref()
            format_metadata = {}
            for fmt in db.formats(book_id, verify_formats=False):
                m = db.format_metadata(book_id, fmt)
                if m:
                    format_metadata[fmt] = m
        if field == "formats":
            return sorted(format_metadata) or None
        return format_metadata

    return func


def approx_fmts_getter(dbref, book_id, cache):
    """
    Perform the approx fmts getter operation under explicit file-format and conversion rules.

    Example:
        Exercise approx fmts getter through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py


    :param dbref: Value supplied for dbref under the utility contract.
    :param book_id: Value supplied for book id under the utility contract.
    :param cache: Value supplied for cache under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    try:
        return cache["formats"]
    except KeyError:
        db = dbref()
        cache["formats"] = ret = list(db.field_for("formats", book_id))
        return ret


def series_index_getter(field="series"):
    """
    Perform the series index getter operation under explicit file-format and conversion rules.

    Example:
        Exercise series index getter through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py


    :param field: Metadata or template field addressed by the operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    def func(dbref, book_id, cache):
        """
        Perform the func operation under explicit file-format and conversion rules.

        Example:
            Exercise series index getter.func through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param dbref: Value supplied for dbref under the utility contract.
        :param book_id: Value supplied for book id under the utility contract.
        :param cache: Value supplied for cache under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            series = getters[field](dbref, book_id, cache)
        except KeyError:
            series = custom_getter(field, dbref, book_id, cache)
        if series:
            try:
                return cache[field + "_index"]
            except KeyError:
                db = dbref()
                cache[field + "_index"] = ret = db.field_for(field + "_index", book_id, default_value=1.0)
                return ret

    return func


def has_cover_getter(dbref, book_id, cache):
    """
    Return whether has cover getter holds for the supplied ebook data.

    Example:
        Exercise has cover getter through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py


    :param dbref: Value supplied for dbref under the utility contract.
    :param book_id: Value supplied for book id under the utility contract.
    :param cache: Value supplied for cache under the utility contract.
    :return: True when the documented condition holds; otherwise False.
    """
    try:
        return cache["has_cover"]
    except KeyError:
        db = dbref()
        cache["has_cover"] = ret = _("Yes") if db.field_for("cover", book_id, default_value=False) else ""
        return ret


def fmt_custom(x):
    """
    Perform the fmt custom operation under explicit file-format and conversion rules.

    Example:
        Exercise fmt custom through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py


    :param x: Value supplied for x under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return list(x) if isinstance(x, tuple) else x


def custom_getter(field, dbref, book_id, cache):
    """
    Perform the custom getter operation under explicit file-format and conversion rules.

    Example:
        Exercise custom getter through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py


    :param field: Metadata or template field addressed by the operation.
    :param dbref: Value supplied for dbref under the utility contract.
    :param book_id: Value supplied for book id under the utility contract.
    :param cache: Value supplied for cache under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    try:
        return cache[field]
    except KeyError:
        db = dbref()
        cache[field] = ret = fmt_custom(db.field_for(field, book_id))
        return ret


def composite_getter(mi, field, dbref, book_id, cache, formatter, template_cache):
    """
    Perform the composite getter operation under explicit file-format and conversion rules.

    Example:
        Exercise composite getter through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py


    :param mi: Metadata object exposed to the template function.
    :param field: Metadata or template field addressed by the operation.
    :param dbref: Value supplied for dbref under the utility contract.
    :param book_id: Value supplied for book id under the utility contract.
    :param cache: Value supplied for cache under the utility contract.
    :param formatter: Template formatter supplying evaluation services and context.
    :param template_cache: Value supplied for template cache under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    try:
        return cache[field]
    except KeyError:
        cache[field] = "RECURSIVE_COMPOSITE FIELD (Metadata) " + field
        try:
            db = dbref()
            with db.safe_read_lock:
                try:
                    fo = db.fields[field]
                except KeyError:
                    ret = cache[field] = _("Invalid field: %s") % field
                else:
                    ret = cache[field] = fo._render_composite_with_cache(book_id, mi, formatter, template_cache)
        except Exception as e:
            err_str = "Error while running composite_getter"
            default_log.log_exception(err_str, e, "INFO")
            return "ERROR WHILE EVALUATING: %s" % field
        return ret


def virtual_libraries_getter(dbref, book_id, cache):
    """
    Perform the virtual libraries getter operation under explicit file-format and conversion rules.

    Example:
        Exercise virtual libraries getter through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py


    :param dbref: Value supplied for dbref under the utility contract.
    :param book_id: Value supplied for book id under the utility contract.
    :param cache: Value supplied for cache under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    try:
        return cache["virtual_libraries"]
    except KeyError:
        db = dbref()
        vls = db.virtual_libraries_for_books((book_id,))[book_id]
        ret = cache["virtual_libraries"] = ", ".join(vls)
        return ret


def user_categories_getter(proxy_metadata):
    """
    Perform the user categories getter operation under explicit file-format and conversion rules.

    Example:
        Exercise user categories getter through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py


    :param proxy_metadata: Value supplied for proxy metadata under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    cache = ga(proxy_metadata, "_cache")
    try:
        return cache["user_categories"]
    except KeyError:
        db = ga(proxy_metadata, "_db")()
        book_id = ga(proxy_metadata, "_book_id")
        ret = cache["user_categories"] = db.user_categories_for_books((book_id,), {book_id: proxy_metadata})[book_id]
        return ret


# Keyed with the field name and valued with the getter to retrieve that field value
getters = {
    "title": simple_getter("title", _("Unknown")),
    "title_sort": simple_getter("sort", _("Unknown")),
    "authors": pp_getter("authors", list, (_("Unknown"),)),
    "author_sort": simple_getter("author_sort", _("Unknown")),
    "uuid": simple_getter("uuid", "dummy"),
    "book_size": simple_getter("size", 0),
    "ondevice_col": simple_getter("ondevice", ""),
    "languages": pp_getter("languages", list),
    "language": item_getter("languages", default_value=NULL_VALUES["language"]),
    "db_approx_formats": approx_fmts_getter,
    "has_cover": has_cover_getter,
    "tags": pp_getter("tags", list, (_("Unknown"),)),
    "series_index": series_index_getter(),
    "application_id": lambda x, book_id, y: book_id,
    "id": lambda x, book_id, y: book_id,
    "virtual_libraries": virtual_libraries_getter,
}

for local_field in ("comments", "publisher", "identifiers", "series", "rating"):
    getters[local_field] = simple_getter(local_field)

for local_field in ("author_sort_map", "author_link_map"):
    getters[local_field] = adata_getter(local_field)

for local_field in ("timestamp", "pubdate", "last_modified"):
    getters[local_field] = dt_getter(local_field)

for local_field in TOP_LEVEL_IDENTIFIERS:
    getters[local_field] = item_getter("identifiers", key=local_field)

for local_field in ("formats", "format_metadata"):
    getters[local_field] = fmt_getter(local_field)


class ProxyMetadata(calibreMetadata):
    """
    Provide the proxymetadata contract for validated ebook processing.

    Example:
        Exercise ProxyMetadata through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """
    def __init__(self, db, book_id, formatter=None):
        """
        Initialize and validate the proxymetadata state.

        Example:
            Exercise ProxyMetadata.  init   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param db: Value supplied for db under the utility contract.
        :param book_id: Value supplied for book id under the utility contract.
        :param formatter: Template formatter supplying evaluation services and context.
        :return: None; validated state is stored on the receiving object.
        """
        sa(self, "template_cache", db.formatter_template_cache)
        sa(self, "formatter", SafeFormat() if formatter is None else formatter)
        sa(self, "_db", weakref.ref(db))
        sa(self, "_book_id", book_id)
        sa(self, "_cache", {"cover_data": (None, None), "device_collections": []})
        sa(self, "_user_metadata", db.field_metadata)

    def __getattribute__(self, field):
        """
        Perform the getattribute operation under explicit file-format and conversion rules.

        Example:
            Exercise ProxyMetadata.  getattribute   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param field: Metadata or template field addressed by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        getter = getters.get(field, None)
        if getter is not None:
            return getter(ga(self, "_db"), ga(self, "_book_id"), ga(self, "_cache"))
        if field in SIMPLE_GET:
            if field == "user_categories":
                return user_categories_getter(self)
            return ga(self, "_cache").get(field, None)
        try:
            return ga(self, field)
        except AttributeError:
            pass
        um = ga(self, "_user_metadata")
        d = um.get(field, None)
        if d is not None:
            dt = d["datatype"]
            if dt != "composite":
                if field.endswith("_index") and dt == "float":
                    return series_index_getter(field[:-6])(ga(self, "_db"), ga(self, "_book_id"), ga(self, "_cache"))
                return custom_getter(field, ga(self, "_db"), ga(self, "_book_id"), ga(self, "_cache"))
            return composite_getter(
                self,
                field,
                ga(self, "_db"),
                ga(self, "_book_id"),
                ga(self, "_cache"),
                ga(self, "formatter"),
                ga(self, "template_cache"),
            )

        try:
            return ga(self, "_cache")[field]
        except KeyError:
            raise AttributeError("Metadata object has no attribute named: %r" % field)

    def __setattr__(self, field, val, extra=None):
        """
        Perform the setattr operation under explicit file-format and conversion rules.

        Example:
            Exercise ProxyMetadata.  setattr   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param field: Metadata or template field addressed by the operation.
        :param val: Template or metadata value evaluated by the operation.
        :param extra: Value supplied for extra under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        cache = ga(self, "_cache")
        cache[field] = val
        if extra is not None:
            cache[field + "_index"] = val

    def get_user_metadata(self, field, make_copy=False):
        """
        Return user metadata under the format's safety and compatibility rules.

        Example:
            Exercise ProxyMetadata.get user metadata through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param field: Metadata or template field addressed by the operation.
        :param make_copy: Value supplied for make copy under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        um = ga(self, "_user_metadata")
        try:
            ans = um[field]
        except KeyError:
            pass
        else:
            if make_copy:
                ans = deepcopy(ans)
            return ans

    def get_extra(self, field, default=None):
        """
        Return extra under the format's safety and compatibility rules.

        Example:
            Exercise ProxyMetadata.get extra through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param field: Metadata or template field addressed by the operation.
        :param default: Value supplied for default under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        um = ga(self, "_user_metadata")
        if field + "_index" in um:
            try:
                return getattr(self, field + "_index")
            except AttributeError:
                return default
        raise AttributeError("Metadata object has no attribute named: " + repr(field))

    def custom_field_keys(self):
        """
        Perform the custom field keys operation under explicit file-format and conversion rules.

        Example:
            Exercise ProxyMetadata.custom field keys through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        um = ga(self, "_user_metadata")
        return iter(um.custom_field_keys())

    def get_standard_metadata(self, field, make_copy=False):
        """
        Return standard metadata under the format's safety and compatibility rules.

        Example:
            Exercise ProxyMetadata.get standard metadata through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param field: Metadata or template field addressed by the operation.
        :param make_copy: Value supplied for make copy under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        field_metadata = ga(self, "_user_metadata")
        if field in field_metadata and field_metadata[field]["kind"] == "field":
            if make_copy:
                return deepcopy(field_metadata[field])
            return field_metadata[field]
        return None

    def all_field_keys(self):
        """
        Perform the all field keys operation under explicit file-format and conversion rules.

        Example:
            Exercise ProxyMetadata.all field keys through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        um = ga(self, "_user_metadata")
        return frozenset(ALL_METADATA_FIELDS.union(iterkeys(um)))

    def _proxy_metadata(self):
        """
        Perform the proxy metadata operation under explicit file-format and conversion rules.

        Example:
            Exercise ProxyMetadata. proxy metadata through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self

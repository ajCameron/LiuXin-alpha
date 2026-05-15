#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

import copy
import traceback

from LiuXin_alpha.constants import VERBOSE_DEBUG as DEBUG

from LiuXin_alpha.utils.calibre_compat.metadata.calibre_metadata_constants import (
    SC_COPYABLE_FIELDS,
    SC_FIELDS_COPY_NOT_NULL,
    STANDARD_METADATA_FIELDS,
    TOP_LEVEL_IDENTIFIERS,
    ALL_METADATA_FIELDS,
)

from LiuXin_alpha.surfaces.field_metadata import FieldMetadata

from LiuXin_alpha.utils.logging import prints
from LiuXin_alpha.utils.text.icu import sort_key, lower as icu_lower
from LiuXin_alpha.utils.localization import trans as _

# Py2/Py3 comparability layer
from LiuXin_alpha.utils.libraries.liuxin_six import (
    dict_iteritems as iteritems, dict_iterkeys as iterkeys,
    six_unicode, basestring)


# Special sets used to optimize the performance of getting and setting
# attributes on Metadata objects
SIMPLE_GET = frozenset(STANDARD_METADATA_FIELDS - TOP_LEVEL_IDENTIFIERS)
SIMPLE_SET = frozenset(SIMPLE_GET - {"identifiers"})

__license__ = "GPL v3"
__copyright__ = "2010, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"


def human_readable(size, precision=2):
    """
    Convert a size in bytes into megabytes.
    :param size:
    :param precision:
    :return:
    """
    return ("%." + str(precision) + "f" + "MB") % ((size / (1024.0 * 1024.0)),)


NULL_VALUES = {
    "user_metadata": {},
    "cover_data": (None, None),
    "tags": [],
    "identifiers": {},
    "languages": [],
    "device_collections": [],
    "author_sort_map": {},
    "authors": [_("Unknown")],
    "author_sort": _("Unknown"),
    "title": _("Unknown"),
    "user_categories": {},
    "author_link_map": {},
    "language": "und",
}

field_metadata = FieldMetadata()


def reset_field_metadata():
    """
    Ensures that the global field_metadata object is set and is an empty instance of FieldMetadata
    :return:
    """
    global field_metadata
    field_metadata = FieldMetadata()


def ck(typ):
    return icu_lower(typ).strip().replace(":", "").replace(",", "")


def cv(val):
    return val.strip().replace(",", "|")


# Todo: Include all calibre fields, marked as such
class calibreMetadata(object):
    """
    A class representing some of the metadata for a book. The various standard metadata fields are available as
    attributes of this object. You can also stick arbitrary attributes onto this object.

    Metadata from custom columns should be accessed via the get() method, passing in the lookup name for the column,
    for example: "#mytags".

    Use the :meth:`is_null` method to test if a field is null.

    This object also has functions to format fields into strings.

    The list of standard metadata fields grows with time is in
    :data:`STANDARD_METADATA_FIELDS`.

    Please keep the method based API of this class to a minimum. Every method becomes a reserved field name.

    This is the original calibre version. It cannot handle as much ambiguity as the LiuXin metadata object (located at
    LiuXin.metadata.metadata) - but a number of the methods ported from calibre need this - it's also made available to
    make porting calibre plugins more painless.
    """

    __name__ = "calibre Metadata object"

    def __init__(
        self,
        title,
        authors=(_("Unknown"),),
        other=None,
        template_cache=None,
        formatter=None,
    ):
        """
        Generate a new metadata object.
        :param title:
        :param authors:
        :param other:
        :param template_cache:
        :param formatter:
        """
        _data = copy.deepcopy(NULL_VALUES)
        _data.pop("language")
        object.__setattr__(self, "_data", _data)
        if other is not None:
            self.smart_update(other)
        else:
            if title:
                self.title = title
            if authors:
                # List of strings or []
                self.author = list(authors) if authors else []  # Needed for backward compatibility
                self.authors = list(authors) if authors else []

        from LiuXin_alpha.metadata.book.formatter import SafeFormat

        self.formatter = SafeFormat() if formatter is None else formatter
        self.template_cache = template_cache

    def is_null(self, field):
        """
        Return True if the value of field is null in this object.
        'null' means it is unknown or evaluates to False. So a title of
        _('Unknown') is null or a language of 'und' is null.

        Be careful with numeric fields since this will return True for zero as
        well as None.

        Also returns True if the field does not exist.
        """
        try:
            null_val = NULL_VALUES.get(field, None)
            val = getattr(self, field, None)
            return not val or val == null_val
        except:
            return True

    def __getattribute__(self, field):

        _data = object.__getattribute__(self, "_data")

        if field in SIMPLE_GET:
            return _data.get(field, None)

        if field in TOP_LEVEL_IDENTIFIERS:
            return _data.get("identifiers").get(field, None)

        if field == "language":
            try:
                return _data.get("languages", [])[0]
            except:
                return NULL_VALUES["language"]

        try:
            return object.__getattribute__(self, field)
        except AttributeError:
            pass

        if field in iterkeys(_data["user_metadata"]):
            d = _data["user_metadata"][field]
            val = d["#value#"]
            if d["datatype"] != "composite":
                return val
            if val is None:
                d["#value#"] = "RECURSIVE_COMPOSITE FIELD (Metadata) " + field
                val = d["#value#"] = self.formatter.safe_format(
                    d["display"]["composite_template"],
                    self,
                    _("TEMPLATE ERROR"),
                    self,
                    column_name=field,
                    template_cache=self.template_cache,
                ).strip()
            return val

        if field.startswith("#") and field.endswith("_index"):
            try:
                return self.get_extra(field[:-6])
            except:
                pass

        raise AttributeError("Metadata object has no attribute named: " + repr(field))

    def __setattr__(self, field, val, extra=None):
        # Never want trailing whitespace to hang around
        if isinstance(val, basestring):
            val = val.strip()

        _data = object.__getattribute__(self, "_data")
        if field in SIMPLE_SET:
            if val is None:
                val = copy.copy(NULL_VALUES.get(field, None))
            _data[field] = val
        elif field in TOP_LEVEL_IDENTIFIERS:
            field, val = self._clean_identifier(field, val)
            identifiers = _data["identifiers"]
            identifiers.pop(field, None)
            if val:
                identifiers[field] = val
        elif field == "identifiers":
            if not val:
                val = copy.copy(NULL_VALUES.get("identifiers", None))
            self.set_identifiers(val)
        elif field == "language":
            langs = []
            if val and val.lower() != "und":
                langs = [val]
            _data["languages"] = langs
        elif field in iterkeys(_data["user_metadata"]):
            _data["user_metadata"][field]["#value#"] = val
            _data["user_metadata"][field]["#extra#"] = extra
        else:
            # You are allowed to stick arbitrary attributes onto this object as
            # long as they don't conflict with global or user metadata names
            # Don't abuse this privilege
            self.__dict__[field] = val

    def set_attr(self, field, val, extra=None):
        """
        Convenience front end for __setattr__.
        :param field:
        :param val:
        :param extra:
        :return:
        """
        self.__setattr__(field, val, extra)

    def __iter__(self):
        return iterkeys(object.__getattribute__(self, "_data"))

    def has_key(self, key):
        return key in object.__getattribute__(self, "_data")

    def deepcopy(self, class_generator=lambda: calibreMetadata(None)):
        """
        Do not use this method unless you know what you are doing, if you
        want to create a simple clone of this object, use :meth:`deepcopy_metadata`
        instead. Class_generator must be a function that returns an instance
        of Metadata or a subclass of it.
        :param class_generator:
        :return:
        """
        m = class_generator()
        if not isinstance(m, calibreMetadata):
            return None
        object.__setattr__(m, "__dict__", copy.deepcopy(self.__dict__))
        return m

    def deepcopy_metadata(self):
        m = calibreMetadata(None)
        object.__setattr__(m, "_data", copy.deepcopy(object.__getattribute__(self, "_data")))
        return m

    def get_data(self):
        """
        Returns the _data dictionary - mostly used for diagnostics.
        :return:
        """
        return copy.deepcopy(object.__getattribute__(self, "_data"))

    def get(self, field, default=None):
        try:
            return self.__getattribute__(field)
        except AttributeError:
            return default

    def get_extra(self, field, default=None):
        _data = object.__getattribute__(self, "_data")
        if field in iterkeys(_data["user_metadata"]):
            try:
                return _data["user_metadata"][field]["#extra#"]
            except:
                return default
        raise AttributeError("Metadata object has no attribute named: " + repr(field))

    def set(self, field, val, extra=None):
        self.__setattr__(field, val, extra)

    @classmethod
    def from_opf(cls, source):
        from LiuXin_alpha.metadata.opf_tools import calibre_metadata_from_opf

        metadata = calibre_metadata_from_opf(source)
        if cls is calibreMetadata:
            return metadata
        return cls(metadata.title, metadata.authors, other=metadata)

    def to_opf_bytes(self, *, default_lang=None):
        from LiuXin_alpha.metadata.opf_tools import metadata_to_opf_bytes

        return metadata_to_opf_bytes(self, default_lang=default_lang)

    def write_to_opf(self, path, *, default_lang=None):
        from LiuXin_alpha.metadata.opf_tools import metadata_to_opf_file

        return metadata_to_opf_file(self, path, default_lang=default_lang)

    def write_to_database(
        self,
        database,
        *,
        fields=None,
        target_level="work",
        item_id=None,
        target_row=None,
        replace=False,
        mark_dirty=True,
    ):
        """
        Persist supported relation-backed fields through the WEMI metadata writer.

        Calibre-shaped metadata can only identify the target database row when
        ``item_id``/``target_row`` is supplied or ``db_id``/``application_id``
        contains the LiuXin item id.
        """
        from LiuXin_alpha.metadata.containers.metadata_containers.liuxin_wemi_metadata_writer import (
            LiuXinWEMIMetadataWriter,
        )

        return LiuXinWEMIMetadataWriter(database).write(
            self,
            fields=fields,
            target_level=target_level,
            item_id=item_id,
            target_row=target_row,
            replace=replace,
            mark_dirty=mark_dirty,
        )

    def get_identifiers(self):
        """
        Return a copy of the identifiers dictionary.
        The dict is small, and the penalty for using a reference where a copy is needed is large.
        Also, we don't want any manipulations of the returned dict to show up in the book.
        """
        ans = object.__getattribute__(self, "_data")["identifiers"]
        if not ans:
            ans = {}
        return copy.deepcopy(ans)

    def _clean_identifier(self, typ, val):
        if typ:
            typ = ck(typ)
        if val:
            val = cv(val)
        return typ, val

    def set_identifiers(self, identifiers):
        """
        Set all identifiers. Note that if you previously set ISBN, calling this method will delete it.
        :param identifiers:
        """
        cleaned = {ck(k): cv(v) for k, v in iteritems(identifiers) if k and v}
        object.__getattribute__(self, "_data")["identifiers"] = cleaned

    def set_identifier(self, typ, val):
        """
        If val is empty, deletes identifier of type typ
        :param typ:
        :param val:
        :return:
        """
        typ, val = self._clean_identifier(typ, val)
        if not typ:
            return
        identifiers = object.__getattribute__(self, "_data")["identifiers"]

        identifiers.pop(typ, None)
        if val:
            identifiers[typ] = val

    def has_identifier(self, typ):
        identifiers = object.__getattribute__(self, "_data")["identifiers"]
        return typ in identifiers

    # field-oriented interface. Intended to be the same as in LibraryDatabase

    def standard_field_keys(self):
        """
        return a list of all default keys, even if this book doesn't have them
        """
        return STANDARD_METADATA_FIELDS

    def custom_field_keys(self):
        """
        return a list of the custom fields in this book
        """
        return iterkeys(object.__getattribute__(self, "_data")["user_metadata"])

    def all_field_keys(self):
        """
        All field keys known by this instance, even if their value is None
        """
        _data = object.__getattribute__(self, "_data")
        return frozenset(ALL_METADATA_FIELDS.union(iterkeys(_data["user_metadata"])))

    def metadata_for_field(self, key):
        """
        return metadata describing a standard or custom field.
        :param key:
        :return:
        """
        if key not in self.custom_field_keys():
            return self.get_standard_metadata(key, make_copy=False)
        return self.get_user_metadata(key, make_copy=False)

    def all_non_none_fields(self):
        """
        Return a dictionary containing all non-None metadata fields, including the custom ones.
        """
        result = {}
        _data = object.__getattribute__(self, "_data")
        for attr in STANDARD_METADATA_FIELDS:
            v = _data.get(attr, None)
            if v is not None:
                result[attr] = v

        # separate these because they use self.get(), not _data.get()
        for attr in TOP_LEVEL_IDENTIFIERS:
            v = self.get(attr, None)
            if v is not None:
                result[attr] = v

        for attr in iterkeys(_data["user_metadata"]):
            v = self.get(attr, None)
            if v is not None:
                result[attr] = v
                if _data["user_metadata"][attr]["datatype"] == "series":
                    result[attr + "_index"] = _data["user_metadata"][attr]["#extra#"]
        return result

    # End of field-oriented interface

    # Extended interfaces. These permit one to get copies of metadata dictionaries, and to
    # get and set custom field metadata

    def get_standard_metadata(self, field, make_copy):
        """
        return field metadata from the field if it is there. Otherwise return
        None. field is the key name, not the label. Return a copy if requested,
        just in case the user wants to change values in the dict.
        :param field:
        :param make_copy:
        :return:
        """
        if field in field_metadata and field_metadata[field]["kind"] == "field":
            if make_copy:
                return copy.deepcopy(field_metadata[field])
            return field_metadata[field]
        return None

    @staticmethod
    def get_all_standard_metadata(make_copy):
        """
        return a dict containing all the standard field metadata associated with
        the book.
        :param make_copy:
        :return:
        """
        if not make_copy:
            return field_metadata
        res = {}
        for k in field_metadata:
            if field_metadata[k]["kind"] == "field":
                res[k] = copy.deepcopy(field_metadata[k])
        return res

    def get_all_user_metadata(self, make_copy):
        """
        return a dict containing all the custom field metadata associated with the book.
        :param make_copy:
        :return:
        """
        _data = object.__getattribute__(self, "_data")
        user_metadata = _data["user_metadata"]
        if not make_copy:
            return user_metadata
        res = {}
        for k in user_metadata:
            res[k] = copy.deepcopy(user_metadata[k])
        return res

    def get_user_metadata(self, field, make_copy):
        """
        return field metadata from the object if it is there. Otherwise return
        None. field is the key name, not the label. Return a copy if requested,
        just in case the user wants to change values in the dict.
        :param field:
        :param make_copy:
        :return:
        """
        _data = object.__getattribute__(self, "_data")
        _data = _data["user_metadata"]
        if field in _data:
            if make_copy:
                return copy.deepcopy(_data[field])
            return _data[field]
        return None

    def set_all_user_metadata(self, metadata):
        """
        store custom field metadata into the object. Field is the key name not the label
        :param metadata:
        :return:
        """
        if metadata is None:
            traceback.print_stack()
            return

        um = {}
        for key, meta in iteritems(metadata):
            m = meta.copy()
            if "#value#" not in m:
                if m["datatype"] == "text" and m["is_multiple"]:
                    m["#value#"] = []
                else:
                    m["#value#"] = None
            um[key] = m
        _data = object.__getattribute__(self, "_data")
        _data["user_metadata"] = um

    def set_user_metadata(self, field, metadata):
        """
        store custom field metadata for one column into the object. Field is the key name not the label
        :param field:
        :param metadata:
        :return:
        """
        if field is not None:
            if not field.startswith("#"):
                raise AttributeError("Custom field name %s must begin with '#'" % repr(field))
            if metadata is None:
                traceback.print_stack()
                return
            m = dict(metadata)

            # Copying the elements should not be necessary. The objects referenced
            # in the dict should not change. Of course, they can be replaced.
            # for k,v in metadata.iteritems():
            #     m[k] = copy.copy(v)
            if "#value#" not in m:
                if m["datatype"] == "text" and m["is_multiple"]:
                    m["#value#"] = []
                else:
                    m["#value#"] = None
            _data = object.__getattribute__(self, "_data")
            _data["user_metadata"][field] = m

    def template_to_attribute(self, other, ops):
        """
        Takes a list [(src,dest), (src,dest)], evaluates the template in the
        context of other, then copies the result to self[dest]. This is on a
        best-efforts basis. Some assignments can make no sense.
        :param other:
        :param ops:
        :return:
        """
        if not ops:
            return
        from LiuXin_alpha.metadata.book.formatter import SafeFormat

        formatter = SafeFormat()
        for op in ops:
            try:
                src = op[0]
                dest = op[1]
                val = formatter.safe_format(src, other, "PLUGBOARD TEMPLATE ERROR", other)
                if dest == "tags":
                    self.set(dest, [f.strip() for f in val.split(",") if f.strip()])
                elif dest == "authors":
                    self.set(dest, [f.strip() for f in val.split("&") if f.strip()])
                else:
                    self.set(dest, val)
            except:
                if DEBUG:
                    traceback.print_exc()

    # Old Metadata API {{{
    def print_all_attributes(self):
        for x in STANDARD_METADATA_FIELDS:
            prints("%s:" % x, getattr(self, x, "None"))
        for x in self.custom_field_keys():
            meta = self.get_user_metadata(x, make_copy=False)
            if meta is not None:
                prints(x, meta)
        prints("--------------")

    def smart_update(self, other, replace_metadata=False):
        """
        Merge the information in `other` into self. In case of conflicts, the information
        in `other` takes precedence, unless the information in `other` is NULL.
        :param other:
        :param replace_metadata:
        :return:
        """

        def copy_not_none(dest, src, attr):
            v = getattr(src, attr, None)
            if v not in (None, NULL_VALUES.get(attr, None)):
                setattr(dest, attr, copy.deepcopy(v))

        unknown = _("Unknown")
        if other.title and other.title != unknown:
            self.title = other.title
            if hasattr(other, "title_sort"):
                self.title_sort = other.title_sort

        if other.authors and (
            other.authors[0] != unknown
            or (
                not self.authors
                or (
                    len(self.authors) == 1
                    and self.authors[0] == unknown
                    and getattr(self, "author_sort", None) == unknown
                )
            )
        ):
            self.authors = list(other.authors)
            if hasattr(other, "author_sort_map"):
                self.author_sort_map = dict(other.author_sort_map)
            if hasattr(other, "author_sort"):
                self.author_sort = other.author_sort

        if replace_metadata:
            # SPECIAL_FIELDS = frozenset(['lpath', 'size', 'comments', 'thumbnail'])
            for attr in SC_COPYABLE_FIELDS:
                setattr(
                    self,
                    attr,
                    getattr(other, attr, 1.0 if attr == "series_index" else None),
                )
            self.tags = other.tags
            self.cover_data = getattr(other, "cover_data", NULL_VALUES["cover_data"])
            self.set_all_user_metadata(other.get_all_user_metadata(make_copy=True))
            for x in SC_FIELDS_COPY_NOT_NULL:
                copy_not_none(self, other, x)
            if callable(getattr(other, "get_identifiers", None)):
                self.set_identifiers(other.get_identifiers())
            # language is handled below
        else:
            for attr in SC_COPYABLE_FIELDS:
                copy_not_none(self, other, attr)
            for x in SC_FIELDS_COPY_NOT_NULL:
                copy_not_none(self, other, x)

            if other.tags:
                # Case-insensitive but case preserving merging
                lotags = [t.lower() for t in other.tags]
                lstags = [t.lower() for t in self.tags]
                ot, st = map(frozenset, (lotags, lstags))
                for t in st.intersection(ot):
                    sidx = lstags.index(t)
                    oidx = lotags.index(t)
                    self.tags[sidx] = other.tags[oidx]
                self.tags += [t for t in other.tags if t.lower() in ot - st]

            if getattr(other, "cover_data", False):
                other_cover = other.cover_data[-1]
                self_cover = self.cover_data[-1] if self.cover_data else ""
                if not self_cover:
                    self_cover = ""
                if not other_cover:
                    other_cover = ""
                if len(other_cover) > len(self_cover):
                    self.cover_data = other.cover_data

            if callable(getattr(other, "custom_field_keys", None)):
                for x in other.custom_field_keys():
                    meta = other.get_user_metadata(x, make_copy=True)
                    if meta is not None:
                        self_tags = self.get(x, [])
                        self.set_user_metadata(x, meta)  # get... did the deepcopy
                        other_tags = other.get(x, [])
                        if meta["datatype"] == "text" and meta["is_multiple"]:
                            # Case-insensitive but case preserving merging
                            lotags = [t.lower() for t in other_tags]
                            try:
                                lstags = [t.lower() for t in self_tags]
                            except TypeError:
                                # Happens if x is not a text, is_multiple field
                                # on self
                                lstags = []
                                self_tags = []
                            ot, st = map(frozenset, (lotags, lstags))
                            for t in st.intersection(ot):
                                sidx = lstags.index(t)
                                oidx = lotags.index(t)
                                self_tags[sidx] = other_tags[oidx]
                            self_tags += [t for t in other_tags if t.lower() in ot - st]
                            setattr(self, x, self_tags)

            my_comments = getattr(self, "comments", "")
            other_comments = getattr(other, "comments", "")
            if not my_comments:
                my_comments = ""
            if not other_comments:
                other_comments = ""
            if len(other_comments.strip()) > len(my_comments.strip()):
                self.comments = other_comments

            # Copy all the non-none identifiers
            if callable(getattr(other, "get_identifiers", None)):
                d = self.get_identifiers()
                s = other.get_identifiers()
                d.update([v for v in iteritems(s) if v[1] is not None])
                self.set_identifiers(d)
            else:
                # other structure not Metadata. Copy the top-level identifiers
                for attr in TOP_LEVEL_IDENTIFIERS:
                    copy_not_none(self, other, attr)

        other_lang = getattr(other, "languages", [])
        if other_lang and other_lang != ["und"]:
            self.languages = list(other_lang)
        if not getattr(self, "series", None):
            self.series_index = None

    def format_series_index(self, val=None):
        """
        Return the series index in a nicely formatted manner.
        :param val:
        :return:
        """
        from LiuXin_alpha.metadata import fmt_sidx

        v = self.series_index if val is None else val
        try:
            x = float(v)
        except (ValueError, TypeError):
            x = 1
        return fmt_sidx(x)

    def authors_from_string(self, raw):
        """
        Takes a string - tries to split it down into the individual authors mentioned in it.
        :param raw:
        :return:
        """
        from LiuXin_alpha.metadata.utils import string_to_authors

        self.authors = string_to_authors(raw)

    def format_authors(self):
        """
        Returns the authors as a string.
        :return:
        """
        from LiuXin_alpha.metadata.utils import authors_to_string

        return authors_to_string(self.authors)

    def format_tags(self):
        """
        Returns the tags as a csv list.
        :return:
        """
        return ", ".join([six_unicode(t) for t in sorted(self.tags, key=sort_key)])

    def format_rating(self, v=None, divide_by=1.0):
        """
        Returns the rating as a string.
        :param v:
        :param divide_by:
        :return:
        """
        if v is None:
            if self.rating is not None:
                return six_unicode(self.rating / divide_by)
            return "None"
        return six_unicode(v / divide_by)

    def format_field(self, key, series_with_index=True):
        """
        Returns the tuple (display_name, formatted_value)
        :param key:
        :param series_with_index:
        :return:
        """
        name, val, ign, ign = self.format_field_extended(key, series_with_index)
        return name, val

    def format_field_extended(self, key, series_with_index=True):
        """
        returns the tuple (display_name, formatted_value, original_value, field_metadata)
        :param key:
        :param series_with_index:
        :return:
        """
        from LiuXin_alpha.metadata.utils import authors_to_string
        from LiuXin_alpha.utils.date import format_date

        # Handle custom series index
        if key.startswith("#") and key.endswith("_index"):
            tkey = key[:-6]  # strip the _index
            cmeta = self.get_user_metadata(tkey, make_copy=False)
            if cmeta and cmeta["datatype"] == "series":
                if self.get(tkey):
                    res = self.get_extra(tkey)
                    return (
                        six_unicode(cmeta["name"] + "_index"),
                        self.format_series_index(res),
                        res,
                        cmeta,
                    )
                else:
                    return six_unicode(cmeta["name"] + "_index"), "", "", cmeta

        if key in self.custom_field_keys():
            res = self.get(key, None)  # get evaluates all necessary composites
            cmeta = self.get_user_metadata(key, make_copy=False)
            name = six_unicode(cmeta["name"])
            if res is None or res == "":  # can't check "not res" because of numeric fields
                return name, res, None, None

            orig_res = res
            datatype = cmeta["datatype"]

            if datatype == "text" and cmeta["is_multiple"]:
                res = cmeta["is_multiple"]["list_to_ui"].join(res)
            elif datatype == "series" and series_with_index:
                if self.get_extra(key) is not None:
                    res = res + " [%s]" % self.format_series_index(val=self.get_extra(key))
            elif datatype == "datetime":
                res = format_date(res, cmeta["display"].get("date_format", "dd MMM yyyy"))
            elif datatype == "bool":
                res = _("Yes") if res else _("No")
            elif datatype == "rating":
                res = "%.2g" % (res / 2.0)
            elif datatype in ["int", "float"]:
                try:
                    fmt = cmeta["display"].get("number_format", None)
                    res = fmt.format(res)
                except:
                    pass

            return name, six_unicode(res), orig_res, cmeta

        # convert top-level ids into their value
        if key in TOP_LEVEL_IDENTIFIERS:
            fmeta = field_metadata["identifiers"]
            name = key
            res = self.get(key, None)
            return name, res, res, fmeta

        # Translate aliases into the standard field name
        fmkey = field_metadata.search_term_to_field_key(key)
        if fmkey in field_metadata and field_metadata[fmkey]["kind"] == "field":
            res = self.get(key, None)
            fmeta = field_metadata[fmkey]
            name = six_unicode(fmeta["name"])

            if res is None or res == "":
                return name, res, None, None

            orig_res = res
            name = six_unicode(fmeta["name"])
            datatype = fmeta["datatype"]
            if key == "authors":
                res = authors_to_string(res)
            elif key == "series_index":
                res = self.format_series_index(res)
            elif datatype == "text" and fmeta["is_multiple"]:
                if isinstance(res, dict):
                    res = [k + ":" + v for k, v in res.items()]
                res = fmeta["is_multiple"]["list_to_ui"].join(sorted(filter(None, res), key=sort_key))
            elif datatype == "series" and series_with_index:
                res = res + " [%s]" % self.format_series_index()
            elif datatype == "datetime":
                res = format_date(res, fmeta["display"].get("date_format", "dd MMM yyyy"))
            elif datatype == "rating":
                res = "%.2g" % (res / 2.0)
            elif key == "size":
                res = human_readable(res)
            return name, six_unicode(res), orig_res, fmeta

        return None, None, None, None

    def __unicode__(self):
        """
        A string representation of this object, suitable for printing to console
        """
        from LiuXin_alpha.metadata.ebook_metadata_tools import authors_to_string
        from LiuXin_alpha.utils.date import isoformat

        ans = []

        def fmt(x, y):
            ans.append("%-20s: %s" % (six_unicode(x), six_unicode(y)))

        fmt("Title", self.title)
        if self.title_sort:
            fmt("Title sort", self.title_sort)
        if self.authors:
            fmt(
                "Author(s)",
                authors_to_string(self.authors)
                + ((" [" + self.author_sort + "]") if self.author_sort and self.author_sort != _("Unknown") else ""),
            )
        if self.publisher:
            fmt("Publisher", self.publisher)
        if getattr(self, "book_producer", False):
            fmt("Book Producer", self.book_producer)
        if self.tags:
            fmt("Tags", ", ".join([six_unicode(t) for t in self.tags]))
        if self.series:
            fmt("Series", self.series + " #%s" % self.format_series_index())
        if not self.is_null("languages"):
            fmt("Languages", ", ".join(self.languages))
        if self.rating is not None:
            fmt("Rating", ("%.2g" % (float(self.rating) / 2.0)) if self.rating else "")
        if self.timestamp is not None:
            fmt("Timestamp", isoformat(self.timestamp))
        if self.pubdate is not None:
            fmt("Published", isoformat(self.pubdate))
        if self.rights is not None:
            fmt("Rights", six_unicode(self.rights))
        if self.identifiers:
            fmt(
                "Identifiers",
                ", ".join(["%s:%s" % (k, v) for k, v in iteritems(self.identifiers)]),
            )
        if self.comments:
            fmt("Comments", self.comments)

        for key in self.custom_field_keys():
            val = self.get(key, None)
            if val:
                (name, val) = self.format_field(key)
                fmt(name, six_unicode(val))
        return "\n".join(ans)

    def to_html(self):
        """
        A HTML representation of this object.
        """
        from LiuXin_alpha.surfaces.renderers.calibre_metadata import (
            calibre_metadata_to_html,
        )

        return calibre_metadata_to_html(self)

    def __str__(self):
        return self.__unicode__()

    def __nonzero__(self):
        return bool(self.title or self.author or self.comments or self.tags)

    # }}}


def field_from_string(field, raw, field_metadata):
    """
    Parse the string raw to return an object that is suitable for calling
    set() on a Metadata object.
    :param field:
    :param raw:
    :param field_metadata:
    :return:
    """
    dt = field_metadata["datatype"]
    val = object
    if dt in {"int", "float"}:
        val = int(raw) if dt == "int" else float(raw)
    elif dt == "rating":
        val = float(raw) * 2
    elif dt == "datetime":
        from LiuXin_alpha.utils.date import parse_only_date

        val = parse_only_date(raw)
    elif dt == "bool":
        if raw.lower() in {"true", "yes", "y"}:
            val = True
        elif raw.lower() in {"false", "no", "n"}:
            val = False
        else:
            raise ValueError("Unknown value for %s: %s" % (field, raw))
    elif dt == "text":
        ism = field_metadata["is_multiple"]
        if ism:
            val = [x.strip() for x in raw.split(ism["ui_to_list"])]
            if field == "identifiers":
                val = {x.partition(":")[0]: x.partition(":")[-1] for x in val}
            elif field == "languages":
                from LiuXin_alpha.utils.localization import canonicalize_lang

                val = [canonicalize_lang(x) for x in val]
                val = [x for x in val if x]
    if val is object:
        val = raw
    return val

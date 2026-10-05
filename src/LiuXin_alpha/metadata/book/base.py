#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

"""
Store and format Calibre-compatible book metadata with custom fields and identifier aliases.

calibreMetadata separates standard data from arbitrary instance attributes, keeps
custom field descriptors alongside their values, and lazily evaluates composite
templates. OPF and WEMI persistence are delegated to the metadata adapters.

Example:
    Exercise the owning behavior with pytest::

        python -m pytest -q tests/metadata/book/test_book_metadata_base.py
"""
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

from LiuXin_alpha.catalog.field_metadata import FieldMetadata

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
    Format a byte count as a fixed-precision binary-megabyte value suffixed MB.

    Example:
        >>> human_readable(2 * 1024 * 1024)
        '2.00MB'


    :param size: Numeric byte count.
    :param precision: Decimal places inserted into the percent-format specifier.
    :return: Formatted string.
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
    Replace the module-wide field metadata registry with a fresh FieldMetadata instance.

    Existing external references to the old registry are not updated.

    Example:
        Exercise the owning behavior with pytest::

            python -m pytest -q tests/metadata/book/test_book_metadata_base.py


    :return: None.
    """
    global field_metadata
    field_metadata = FieldMetadata()


def ck(typ):
    """
    Normalize an identifier scheme by case folding, trimming, and removing colons and commas.

    Example:
        >>> ck(' DOI: ')
        'doi'


    :param typ: Identifier scheme string.
    :return: Normalized scheme string.
    """
    return icu_lower(typ).strip().replace(":", "").replace(",", "")


def cv(val):
    """
    Trim an identifier value and replace commas with vertical bars.

    Example:
        >>> cv(' 10.1/a,b ')
        '10.1/a|b'


    :param val: Identifier value string.
    :return: Cleaned identifier value.
    """
    return val.strip().replace(",", "|")


# Todo: Include all calibre fields, marked as such
class calibreMetadata(object):
    """
    Represent one book using Calibre-compatible standard fields and custom columns.

    Standard fields are attributes; custom lookup names such as #labels work through
    get/set and custom metadata descriptors. is_null recognizes empty/default values,
    including zero. Composite values are evaluated and cached on first access. Arbitrary
    attributes are allowed, but method names remain reserved. Use deepcopy_metadata for
    a clone of stored metadata alone.

    Example:
        >>> book = calibreMetadata('  Example  ', ['Writer'])
        >>> book.title, book.authors
        ('Example', ['Writer'])
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
        Initialize independent null defaults, then set title/authors or merge another metadata object.

        When other is supplied, title and authors arguments are ignored. A missing formatter
        creates SafeFormat; the template cache is retained as supplied.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/book/test_book_metadata_base.py


        :param title: Initial title; a false value leaves the localized Unknown default.
        :param authors: Initial author iterable; a false value leaves defaults.
        :param other: Source metadata object with the attributes required by this operation.
        :param template_cache: Optional cache passed to composite formatting.
        :param formatter: Formatter instance to retain, or None to create SafeFormat.
        :return: None.
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
        Test whether a field is absent, false, or equal to its configured null value.

        Zero counts as null. Any exception during the lookup also produces True.

        Example:
            >>> calibreMetadata('Example').is_null('tags')
            True


        :param field: Standard field, custom lookup name, or attribute name.
        :return: Whether the field is considered null.
        """
        try:
            null_val = NULL_VALUES.get(field, None)
            val = getattr(self, field, None)
            return not val or val == null_val
        except:
            return True

    def __getattribute__(self, field):

        """
        Read standard data, identifier aliases, language, custom columns, or ordinary attributes.

        Composite columns are evaluated once and cached, with a recursion sentinel installed
        before formatting. A custom series _index suffix resolves through get_extra. Unknown
        names raise AttributeError.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/book/test_book_metadata_base.py


        :param field: Standard field, custom lookup name, or attribute name.
        :return: Resolved value; mutable values can be the stored objects.
        """
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
        """
        Assign metadata using standard defaults, cleaned identifiers, and custom value/extra slots.

        Strings are stripped. language maps to the first-language list, and und clears it.
        Unknown names become ordinary instance attributes. Container inputs are generally
        retained rather than deep-copied.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/book/test_book_metadata_base.py


        :param field: Standard field, custom lookup name, or attribute name.
        :param val: Value to assign; strings are trimmed and standard None values use field
            defaults.
        :param extra: Optional custom-field auxiliary value, such as a series index.
        :return: None.
        """
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
        Assign a field and optional custom extra through the metadata attribute setter.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/book/test_book_metadata_base.py


        :param field: Standard field, custom lookup name, or attribute name.
        :param val: Value to assign; strings are trimmed and standard None values use field
            defaults.
        :param extra: Optional custom-field auxiliary value, such as a series index.
        :return: None.
        """
        self.__setattr__(field, val, extra)

    def __iter__(self):
        """
        Iterate keys currently present in the internal metadata dictionary.

        Custom column names are nested under user_metadata and are not expanded here.

        Example:
            >>> 'tags' in set(calibreMetadata('Example'))
            True


        :return: Iterator over stored top-level keys.
        """
        return iterkeys(object.__getattribute__(self, "_data"))

    def has_key(self, key):
        """
        Check direct membership in the internal metadata dictionary.

        Example:
            >>> calibreMetadata('Example').has_key('tags')
            True


        :param key: Field lookup key.
        :return: Whether the stored top-level key exists.
        """
        return key in object.__getattribute__(self, "_data")

    def deepcopy(self, class_generator=lambda: calibreMetadata(None)):
        """
        Clone the entire instance dictionary into an object created by a supplied factory.

        This includes arbitrary attributes and formatter state. Prefer deepcopy_metadata
        when only stored metadata should be copied.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/book/test_book_metadata_base.py


        :param class_generator: Zero-argument factory creating calibreMetadata or a
            subclass.
        :return: Clone, or None if the factory does not return a calibreMetadata instance.
        """
        m = class_generator()
        if not isinstance(m, calibreMetadata):
            return None
        object.__setattr__(m, "__dict__", copy.deepcopy(self.__dict__))
        return m

    def deepcopy_metadata(self):
        """
        Deep-copy stored metadata into a fresh base calibreMetadata instance.

        Arbitrary attributes, a custom formatter, and a custom template cache are not
        copied.

        Example:
            >>> book = calibreMetadata('Example')
            >>> clone = book.deepcopy_metadata()
            >>> clone.tags.append('new')
            >>> book.tags
            []


        :return: Independent base metadata object.
        """
        m = calibreMetadata(None)
        object.__setattr__(m, "_data", copy.deepcopy(object.__getattribute__(self, "_data")))
        return m

    def get_data(self):
        """
        Return an independent deep copy of the internal metadata dictionary for inspection.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/book/test_book_metadata_base.py


        :return: Copied stored data; ordinary instance attributes are excluded.
        """
        return copy.deepcopy(object.__getattribute__(self, "_data"))

    def get(self, field, default=None):
        """
        Read a metadata field, returning a fallback only when lookup raises AttributeError.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/book/test_book_metadata_base.py


        :param field: Standard field, custom lookup name, or attribute name.
        :param default: Fallback for an unknown field.
        :return: Resolved value or default; a stored None remains None.
        """
        try:
            return self.__getattribute__(field)
        except AttributeError:
            return default

    def get_extra(self, field, default=None):
        """
        Read the auxiliary value of a defined custom field.

        A missing or unreadable extra returns the fallback. An undefined custom field raises
        AttributeError.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/book/test_book_metadata_base.py


        :param field: Standard field, custom lookup name, or attribute name.
        :param default: Fallback if the defined field has no readable #extra# value.
        :return: Stored extra or default.
        """
        _data = object.__getattribute__(self, "_data")
        if field in iterkeys(_data["user_metadata"]):
            try:
                return _data["user_metadata"][field]["#extra#"]
            except:
                return default
        raise AttributeError("Metadata object has no attribute named: " + repr(field))

    def set(self, field, val, extra=None):
        """
        Assign a field and custom extra through the metadata attribute setter.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/book/test_book_metadata_base.py


        :param field: Standard field, custom lookup name, or attribute name.
        :param val: Value to assign; strings are trimmed and standard None values use field
            defaults.
        :param extra: Optional custom-field auxiliary value, such as a series index.
        :return: None.
        """
        self.__setattr__(field, val, extra)

    @classmethod
    def from_opf(cls, source):
        """
        Read OPF through the shared adapter and return this metadata class.

        The base class returns the adapter result directly; subclasses are constructed from
        its title, authors, and other metadata.

        Example:
            Exercise OPF conversion with pytest::

                python -m pytest -q tests/metadata/test_opf_tools.py


        :param source: OPF path, bytes, XML text, or readable stream accepted by the
            adapter; caller streams remain open.
        :return: Populated calibreMetadata or requested subclass.
        """
        from LiuXin_alpha.metadata.opf_tools import calibre_metadata_from_opf

        metadata = calibre_metadata_from_opf(source)
        if cls is calibreMetadata:
            return metadata
        return cls(metadata.title, metadata.authors, other=metadata)

    def to_opf_bytes(self, *, default_lang=None):
        """
        Serialize this book through the shared OPF adapter.

        Example:
            Exercise OPF conversion with pytest::

                python -m pytest -q tests/metadata/test_opf_tools.py


        :param default_lang: Optional default language forwarded to OPF serialization.
        :return: UTF-8 OPF bytes.
        """
        from LiuXin_alpha.metadata.opf_tools import metadata_to_opf_bytes

        return metadata_to_opf_bytes(self, default_lang=default_lang)

    def write_to_opf(self, path, *, default_lang=None):
        """
        Serialize this book and write the OPF file through the shared adapter.

        Example:
            Exercise OPF conversion with pytest::

                python -m pytest -q tests/metadata/test_opf_tools.py


        :param path: Destination path; the adapter creates parent directories and overwrites
            the file.
        :param default_lang: Optional default language forwarded to OPF serialization.
        :return: Path returned by the OPF file adapter.
        """
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
        Persist supported relation fields through the WEMI metadata writer.

        Supply item_id or target_row to select a LiuXin target, or store a LiuXin item id in
        db_id/application_id. Writer validation, skip/error reporting, and transaction
        behavior are delegated.

        Example:
            Exercise writer delegation with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param database: Caller-owned database used by the metadata writer for persistence;
            this method does not close it.
        :param fields: Optional iterable of supported relation fields to write; None uses
            writer defaults.
        :param target_level: WEMI level for relation writes, defaulting to work.
        :param item_id: Optional item identifier forwarded to the hydrator or OPF adapter.
        :param target_row: Optional explicit target row or row mapping.
        :param replace: Whether to replace existing relations for selected fields.
        :param mark_dirty: Whether to mark changed target metadata dirty.
        :return: MetadataWriteReport describing changes, skipped operations, and errors.
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
        Deep-copy the identifier dictionary so callers can edit the result independently.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/book/test_book_metadata_base.py


        :return: Independent scheme-to-value dictionary.
        """
        ans = object.__getattribute__(self, "_data")["identifiers"]
        if not ans:
            ans = {}
        return copy.deepcopy(ans)

    def _clean_identifier(self, typ, val):
        """
        Clean a truthy scheme and value using ck and cv; retain false inputs.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/book/test_book_metadata_base.py


        :param typ: Identifier scheme string.
        :param val: Identifier string or false value for deletion.
        :return: Pair of cleaned scheme and value.
        """
        if typ:
            typ = ck(typ)
        if val:
            val = cv(val)
        return typ, val

    def set_identifiers(self, identifiers):
        """
        Replace all identifiers with cleaned nonempty input pairs.

        This also replaces any ISBN previously set through the top-level alias.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/book/test_book_metadata_base.py


        :param identifiers: Mapping of scheme strings to value strings; false keys or values
            are omitted.
        :return: None.
        """
        cleaned = {ck(k): cv(v) for k, v in iteritems(identifiers) if k and v}
        object.__getattribute__(self, "_data")["identifiers"] = cleaned

    def set_identifier(self, typ, val):
        """
        Replace one cleaned identifier, or delete it when the value is false.

        An empty cleaned scheme is ignored.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/book/test_book_metadata_base.py


        :param typ: Identifier scheme string.
        :param val: Identifier value string, or a false value to delete the scheme.
        :return: None.
        """
        typ, val = self._clean_identifier(typ, val)
        if not typ:
            return
        identifiers = object.__getattribute__(self, "_data")["identifiers"]

        identifiers.pop(typ, None)
        if val:
            identifiers[typ] = val

    def has_identifier(self, typ):
        """
        Check whether an exact scheme key is stored without normalizing it.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/book/test_book_metadata_base.py


        :param typ: Identifier scheme string.
        :return: Whether the scheme is present.
        """
        identifiers = object.__getattribute__(self, "_data")["identifiers"]
        return typ in identifiers

    # field-oriented interface. Intended to be the same as in LibraryDatabase

    def standard_field_keys(self):
        """
        Return all standard field names, including fields unset on this instance.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/book/test_book_metadata_base.py


        :return: Shared immutable STANDARD_METADATA_FIELDS set.
        """
        return STANDARD_METADATA_FIELDS

    def custom_field_keys(self):
        """
        Iterate custom column lookup names currently defined on this book.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/book/test_book_metadata_base.py


        :return: Iterator over user_metadata keys.
        """
        return iterkeys(object.__getattribute__(self, "_data")["user_metadata"])

    def all_field_keys(self):
        """
        Collect all built-in metadata names and this instance's custom column names.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/book/test_book_metadata_base.py


        :return: New frozenset of known field names.
        """
        _data = object.__getattribute__(self, "_data")
        return frozenset(ALL_METADATA_FIELDS.union(iterkeys(_data["user_metadata"])))

    def metadata_for_field(self, key):
        """
        Look up a standard or custom field descriptor without copying it.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/book/test_book_metadata_base.py


        :param key: Field lookup key.
        :return: Stored descriptor mapping or None for an unknown field.
        """
        if key not in self.custom_field_keys():
            return self.get_standard_metadata(key, make_copy=False)
        return self.get_user_metadata(key, make_copy=False)

    def all_non_none_fields(self):
        """
        Collect non-None standard and custom values, including identifier aliases.

        Empty containers and other false values are retained. Custom composites may be
        evaluated, and non-None custom series also expose their _index key. Mutable values
        remain shared with this object.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/book/test_book_metadata_base.py


        :return: New field-to-value dictionary containing references to the selected values.
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
        Read a standard descriptor only when its registry kind is field.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/book/test_book_metadata_base.py


        :param field: Standard field, custom lookup name, or attribute name.
        :param make_copy: True for a deep copy; False for the stored object itself.
        :return: Descriptor, optional deep copy, or None.
        """
        if field in field_metadata and field_metadata[field]["kind"] == "field":
            if make_copy:
                return copy.deepcopy(field_metadata[field])
            return field_metadata[field]
        return None

    @staticmethod
    def get_all_standard_metadata(make_copy):
        """
        Expose the global registry or deep-copy its entries whose kind is field.

        The no-copy form returns the complete FieldMetadata registry, including non-field
        entries.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/book/test_book_metadata_base.py


        :param make_copy: True for a deep copy; False for the stored object itself.
        :return: Shared registry when make_copy is false, otherwise a plain dictionary of
            copied field descriptors.
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
        Expose or deep-copy all custom field descriptors and their stored values.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/book/test_book_metadata_base.py


        :param make_copy: True for a deep copy; False for the stored object itself.
        :return: User metadata dictionary, copied when requested.
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
        Read a custom field descriptor, optionally making a deep copy.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/book/test_book_metadata_base.py


        :param field: Standard field, custom lookup name, or attribute name.
        :param make_copy: True for a deep copy; False for the stored object itself.
        :return: Descriptor mapping or None if undefined.
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
        Replace custom metadata with shallow copies of the supplied descriptors.

        Missing #value# becomes [] for multiple text or None otherwise. Nested descriptor
        objects remain shared. None prints a diagnostic stack and leaves current metadata
        unchanged.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/book/test_book_metadata_base.py


        :param metadata: Mapping of lookup names to descriptor mappings, or None for the
            diagnostic no-op.
        :return: None.
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
        Store a shallow copy of one custom column descriptor and supply a missing default value.

        A non-None field must begin with # or AttributeError is raised. None as the field is
        ignored; None as metadata prints a diagnostic stack. Nested values remain shared.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/book/test_book_metadata_base.py


        :param field: Standard field, custom lookup name, or attribute name.
        :param metadata: Descriptor with datatype/is_multiple and optional #value#; None
            triggers diagnostics.
        :return: None.
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
        Evaluate plugboard templates against another book and assign their output here.

        Split tag output on commas and author output on ampersands, trimming empty entries.
        Other outputs are assigned as strings. Per-operation failures are suppressed, with
        tracebacks only when DEBUG is enabled.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/book/test_book_metadata_base.py


        :param other: Source metadata object with the attributes required by this operation.
        :param ops: Iterable of (template, destination-field) pairs; a false value does
            nothing.
        :return: None.
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
        """
        Print standard values, defined custom descriptors, and a separator for diagnostics.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/book/test_book_metadata_base.py


        :return: None.
        """
        for x in STANDARD_METADATA_FIELDS:
            prints("%s:" % x, getattr(self, x, "None"))
        for x in self.custom_field_keys():
            meta = self.get_user_metadata(x, make_copy=False)
            if meta is not None:
                prints(x, meta)
        prints("--------------")

    def smart_update(self, other, replace_metadata=False):
        """
        Merge another book into this one using field-specific replacement rules.

        A known source title and usable authors take precedence. Normal merging deep-copies
        non-null ordinary fields, combines tags and multiple custom text case-insensitively
        while adopting source spelling, prefers larger cover payloads and longer stripped
        comments, and overlays identifiers. Replacement copies ordinary fields even when
        null and replaces tags, cover data, custom metadata, and identifiers; selected
        optional fields still copy only non-null values. Some replacement containers are
        shared. Meaningful source languages replace the current list, and an absent series
        clears series_index.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/book/test_book_metadata_base.py


        :param other: Source metadata object with the attributes required by this operation.
        :param replace_metadata: Whether to use replacement behavior for the selected field
            groups.
        :return: None.
        """

        def copy_not_none(dest, src, attr):
            """
            Deep-copy one source attribute unless it is None or equals its field null sentinel.

            Example:
                Exercise the owning behavior with pytest::

                    python -m pytest -q tests/metadata/book/test_book_metadata_base.py


            :param dest: Metadata object receiving the copied attribute.
            :param src: Source object inspected with getattr.
            :param attr: Attribute name and NULL_VALUES lookup key.
            :return: None.
            """
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
                                if isinstance(self_tags, basestring):
                                    raise TypeError
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
        Format a supplied or stored series index through fmt_sidx, defaulting invalid values to one.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/book/test_book_metadata_base.py


        :param val: Explicit numeric value; None reads this book's series_index.
        :return: Formatted series index string.
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
        Split author text with the shared parser and replace the authors list.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/book/test_book_metadata_base.py


        :param raw: Author string accepted by string_to_authors.
        :return: None.
        """
        from LiuXin_alpha.metadata.utils import string_to_authors

        self.authors = string_to_authors(raw)

    def format_authors(self):
        """
        Join author names using the shared ampersand-escaping convention.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/book/test_book_metadata_base.py


        :return: Formatted author string.
        """
        from LiuXin_alpha.metadata.utils import authors_to_string

        return authors_to_string(self.authors)

    def format_tags(self):
        """
        Sort tags with the locale sort key and join their text forms with comma-space.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/book/test_book_metadata_base.py


        :return: Formatted tag string; the stored order is unchanged.
        """
        return ", ".join([six_unicode(t) for t in sorted(self.tags, key=sort_key)])

    def format_rating(self, v=None, divide_by=1.0):
        """
        Convert an explicit or stored rating to text after dividing by the requested scale.

        A missing stored rating renders as the literal None string. Division errors are not
        suppressed.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/book/test_book_metadata_base.py


        :param v: Explicit rating, or None to use this book's rating.
        :param divide_by: Numeric divisor, normally 1.0 or 2.0.
        :return: Rating string.
        """
        if v is None:
            if self.rating is not None:
                return six_unicode(self.rating / divide_by)
            return "None"
        return six_unicode(v / divide_by)

    def format_field(self, key, series_with_index=True):
        """
        Return the display name and formatted value from format_field_extended.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/book/test_book_metadata_base.py


        :param key: Field lookup key.
        :param series_with_index: Whether to append a formatted index to series names.
        :return: Pair of display name and value; unknown fields yield (None, None).
        """
        name, val, ign, ign = self.format_field_extended(key, series_with_index)
        return name, val

    def format_field_extended(self, key, series_with_index=True):
        """
        Format a field using its standard or custom descriptor.

        Handle identifiers, custom series indices, composites, multiple values, dates,
        booleans, ratings and numeric displays. Unknown fields yield four None values. Empty
        values retain their name but generally omit original value and metadata. Returned
        original values and descriptors are shared, and composite lookup may populate its
        cache.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/book/test_book_metadata_base.py


        :param key: Field lookup key.
        :param series_with_index: Whether series displays include their index.
        :return: Tuple (display_name, formatted_value, original_value, field_metadata).
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
        Render populated book metadata as labeled lines for console output.

        Include selected standard fields and truthy custom fields. Ratings are displayed on
        the five-point scale; comments are included as stored.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/book/test_book_metadata_base.py


        :return: Multiline Unicode string.
        """
        from LiuXin_alpha.metadata.ebook_metadata_tools import authors_to_string
        from LiuXin_alpha.utils.date import isoformat

        ans = []

        def fmt(x, y):
            """
            Append one aligned label/value line to the enclosing console output buffer.

            Example:
                Exercise the owning behavior with pytest::

                    python -m pytest -q tests/metadata/book/test_book_metadata_base.py


            :param x: Label converted to text and padded to at least 20 characters.
            :param y: Value converted to text.
            :return: None.
            """
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
        Delegate the HTML summary to the surfaces Calibre metadata renderer.

        Example:
            Exercise HTML rendering with pytest::

                python -m pytest -q tests/metadata/book/test_formatter_and_render.py


        :return: HTML string returned by calibre_metadata_to_html.
        """
        from LiuXin_alpha.surfaces.renderers.calibre_metadata import (
            calibre_metadata_to_html,
        )

        return calibre_metadata_to_html(self)

    def __str__(self):
        """
        Return the console metadata rendering supplied by __unicode__.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/book/test_book_metadata_base.py


        :return: Multiline string.
        """
        return self.__unicode__()

    def __nonzero__(self):
        """
        Evaluate the legacy truth hook from title, author, comments, and tags.

        This Python 2 hook is not a Python 3 __bool__ implementation.

        Example:
            >>> calibreMetadata('Example').__nonzero__()
            True


        :return: Whether any of those attributes is truthy.
        """
        return bool(self.title or self.author or self.comments or self.tags)

    # }}}


def field_from_string(field, raw, field_metadata):
    """
    Parse editable field text according to its metadata datatype.

    Convert numbers, double five-point ratings, parse dates, and recognize true/yes/y or
    false/no/n booleans. Unknown boolean text raises ValueError. Multiple text uses
    ui_to_list, with special identifier-pair and canonical language handling. Other
    datatypes retain the raw text; conversion errors propagate.

    Example:
        >>> field_from_string('rating', '3.5', {'datatype': 'rating'})
        7.0


    :param field: Standard field, custom lookup name, or attribute name.
    :param raw: Input string to parse.
    :param field_metadata: Descriptor containing datatype and, for text, is_multiple
        separators.
    :return: Parsed field value suitable for set().
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

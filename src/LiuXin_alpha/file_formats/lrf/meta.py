"""
Read, normalize and update LRF metadata and thumbnail records.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise meta through a consuming regression::

        python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py
"""
from __future__ import print_function
from __future__ import annotations

import typing as _typing

"""
This module presents an easy to use interface for getting and setting
meta information in LRF files.
Just create an L{LRFMetaFile} object and use its properties
to get and set meta information. For example:

>>> lrf = LRFMetaFile("mybook.lrf")
>>> print(lrf.title, lrf.author)
>>> lrf.category = "History"
"""

import os
import struct
import sys
import zlib
from functools import wraps
from shutil import copyfileobj

import xml.dom.minidom as dom

from LiuXin_alpha.metadata.utils import authors_to_string
from LiuXin_alpha.metadata.utils import calibreMetaInformation
from LiuXin_alpha.metadata.utils import string_to_authors
from LiuXin_alpha.metadata.containers.calibre_like_book_metadata import (
    CalibreLikeLiuXinBookMetaData as MetaData,
)

from LiuXin_alpha.utils.localization import trans as _

# Py2/Py3 compatibility
from LiuXin_alpha.utils.libraries.liuxin_six import six_cStringIO
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode

__license__ = "GPL v3"
__copyright__ = "2008, Kovid Goyal <kovid at kovidgoyal.net>"

BYTE = "<B"  #: Unsigned char little endian encoded in 1 byte
WORD = "<H"  #: Unsigned short little endian encoded in 2 bytes
DWORD = "<I"  #: Unsigned integer little endian encoded in 4 bytes
QWORD = "<Q"  #: Unsigned long long little endian encoded in 8 bytes


class field(object):
    """
    A U{Descriptor<http://www.cafepy.com/article/python_attributes_and_methods/python_attributes_and_methods.html>}, that implements access to protocol packets in a human readable way.

    Example:
        Exercise field through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py
    """

    def __init__(self: _typing.Self, start: int = 16, fmt: _typing.Any = DWORD) -> None:
        """
        See U{struct<http://docs.python.org/lib/module-struct.html>}.

        Example:
            Exercise field.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :param start: Value supplied for start under the utility contract.
        :param fmt: Date, number or template format specification.
        :return: None; validated state is stored on the receiving object.
        """
        self._fmt = fmt
        self._start = start

    def __get__(self: _typing.Self, obj: _typing.Any, typ: _typing.Any = None) -> _typing.Any:
        """
        Perform the get operation under explicit file-format and conversion rules.

        Example:
            Exercise field.  get   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :param obj: Value supplied for obj under the utility contract.
        :param typ: Value supplied for typ under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return obj.unpack(start=self._start, fmt=self._fmt)[0]

    def __set__(self: _typing.Self, obj: _typing.Any, val: _typing.Any) -> None:
        """
        Perform the set operation under explicit file-format and conversion rules.

        Example:
            Exercise field.  set   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :param obj: Value supplied for obj under the utility contract.
        :param val: Template or metadata value evaluated by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        obj.pack(val, start=self._start, fmt=self._fmt)

    def __repr__(self: _typing.Self) -> _typing.Any:
        """
        Perform the repr operation under explicit file-format and conversion rules.

        Example:
            Exercise field.  repr   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        typ = ""
        if self._fmt == DWORD:
            typ = "unsigned int"
        if self._fmt == QWORD:
            typ = "unsigned long long"
        return (
            "An "
            + typ
            + " stored in "
            + str(struct.calcsize(self._fmt))
            + " bytes starting at byte "
            + str(self._start)
        )


class versioned_field(field):
    """
    Provide the versioned field contract for validated ebook processing.

    Example:
        Exercise versioned field through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py
    """
    def __init__(self: _typing.Self, vfield: _typing.Any, version: _typing.Any, start: int = 0, fmt: _typing.Any = WORD) -> None:
        """
        Initialize and validate the versioned field state.

        Example:
            Exercise versioned field.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :param vfield: Value supplied for vfield under the utility contract.
        :param version: Value supplied for version under the utility contract.
        :param start: Value supplied for start under the utility contract.
        :param fmt: Date, number or template format specification.
        :return: None; validated state is stored on the receiving object.
        """
        field.__init__(self, start=start, fmt=fmt)
        self.vfield, self.version = vfield, version

    def enabled(self: _typing.Self, obj: _typing.Any) -> bool:
        """
        Perform the enabled operation under explicit file-format and conversion rules.

        Example:
            Exercise versioned field.enabled through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :param obj: Value supplied for obj under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if isinstance(self.vfield, field):
            if obj is None:
                return False
            vfield_value = self.vfield.__get__(obj, obj.__class__)
        else:
            vfield_value = self.vfield
        return vfield_value > self.version

    def __get__(self: _typing.Self, obj: _typing.Any, typ: _typing.Any = None) -> _typing.Any:
        """
        Perform the get operation under explicit file-format and conversion rules.

        Example:
            Exercise versioned field.  get   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :param obj: Value supplied for obj under the utility contract.
        :param typ: Value supplied for typ under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self.enabled(obj):
            return field.__get__(self, obj, typ=typ)
        else:
            return None

    def __set__(self: _typing.Self, obj: _typing.Any, val: _typing.Any) -> None:
        """
        Perform the set operation under explicit file-format and conversion rules.

        Example:
            Exercise versioned field.  set   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :param obj: Value supplied for obj under the utility contract.
        :param val: Template or metadata value evaluated by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if not self.enabled(obj):
            raise LRFException("Trying to set disabled field")
        else:
            field.__set__(self, obj, val)


class LRFException(Exception):
    """
    Report a lrfexception encountered while processing an ebook format.

    Example:
        Exercise LRFException through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py
    """
    pass


class fixed_stringfield(object):
    """
    A field storing a variable length string.

    Example:
        Exercise fixed stringfield through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py
    """

    def __init__(self: _typing.Self, length: int = 8, start: int = 0) -> None:
        """
        Initialize and validate the fixed stringfield state.

        Example:
            Exercise fixed stringfield.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :param length: Value supplied for length under the utility contract.
        :param start: Value supplied for start under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self._length = length
        self._start = start

    def __get__(self: _typing.Self, obj: _typing.Any, typ: _typing.Any = None) -> _typing.Any:
        """
        Perform the get operation under explicit file-format and conversion rules.

        Example:
            Exercise fixed stringfield.  get   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :param obj: Value supplied for obj under the utility contract.
        :param typ: Value supplied for typ under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        length = str(self._length)
        return obj.unpack(start=self._start, fmt="<" + length + "s")[0]

    def __set__(self: _typing.Self, obj: _typing.Any, val: _typing.Any) -> None:
        """
        Perform the set operation under explicit file-format and conversion rules.

        Example:
            Exercise fixed stringfield.  set   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :param obj: Value supplied for obj under the utility contract.
        :param val: Template or metadata value evaluated by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if val.__class__.__name__ != "str":
            val = str(val)
        if len(val) != self._length:
            raise LRFException("Trying to set fixed_stringfield with a string of  incorrect length")
        obj.pack(val, start=self._start, fmt="<" + str(len(val)) + "s")

    def __repr__(self: _typing.Self) -> _typing.Any:
        """
        Perform the repr operation under explicit file-format and conversion rules.

        Example:
            Exercise fixed stringfield.  repr   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return "A string of length " + str(self._length) + " starting at byte " + str(self._start)


class xml_attr_field(object):
    """
    descriptor for an xml_attr_field - gets and sets values for an xml attribute field

    Example:
        Exercise xml attr field through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py
    """

    def __init__(self: _typing.Self, tag_name: _typing.Any, attr: _typing.Any, parent: str = "BookInfo") -> None:
        """
        Initialize and validate the xml attr field state.

        Example:
            Exercise xml attr field.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :param tag_name: Value supplied for tag name under the utility contract.
        :param attr: Value supplied for attr under the utility contract.
        :param parent: Value supplied for parent under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.tag_name = tag_name
        self.parent = parent
        self.attr = attr

    def __get__(self: _typing.Self, obj: _typing.Any, typ: _typing.Any = None) -> _typing.Any:
        """
        Return the data in this field or '' if the field is empty

        Example:
            Exercise xml attr field.  get   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :param obj: Value supplied for obj under the utility contract.
        :param typ: Value supplied for typ under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        document = obj.info
        elems = document.getElementsByTagName(self.tag_name)
        if len(elems):
            elem = None
            for candidate in elems:
                if candidate.parentNode.nodeName == self.parent:
                    elem = candidate
            if elem and elem.hasAttribute(self.attr):
                return elem.getAttribute(self.attr)
        return ""

    def __set__(self: _typing.Self, obj: _typing.Any, val: _typing.Any) -> None:
        """
        Perform the set operation under explicit file-format and conversion rules.

        Example:
            Exercise xml attr field.  set   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :param obj: Value supplied for obj under the utility contract.
        :param val: Template or metadata value evaluated by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if val is None:
            val = ""
        document = obj.info
        elem = None
        elems = document.getElementsByTagName(self.tag_name)
        if len(elems):
            for candidate in elems:
                if candidate.parentNode.nodeName == self.parent:
                    elem = candidate
        if elem:
            elem.setAttribute(self.attr, val)
        obj.info = document

    def __repr__(self: _typing.Self) -> _typing.Any:
        """
        Perform the repr operation under explicit file-format and conversion rules.

        Example:
            Exercise xml attr field.  repr   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return "XML Attr Field: {} in {}".format(self.tag_name, self.parent)

    def __str__(self: _typing.Self) -> _typing.Any:
        """
        Perform the str operation under explicit file-format and conversion rules.

        Example:
            Exercise xml attr field.  str   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return "{}.{}".format(self.tag_name, self.attr)


class xml_field(object):
    """
    Descriptor that gets and sets XML based meta information from an LRF file. Works for simple XML fields of the form <tagname>data</tagname>

    Example:
        Exercise xml field through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py
    """

    def __init__(self: _typing.Self, tag_name: _typing.Any, parent: str = "BookInfo") -> None:
        """
        Initialize and validate the xml field state.

        Example:
            Exercise xml field.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :param tag_name: Value supplied for tag name under the utility contract.
        :param parent: Value supplied for parent under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.tag_name = tag_name
        self.parent = parent

    def __get__(self: _typing.Self, obj: _typing.Any, typ: _typing.Any = None) -> _typing.Any:
        """
        Return the data in this field or '' if the field is empty.

        Example:
            Exercise xml field.  get   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :param obj: Value supplied for obj under the utility contract.
        :param typ: Value supplied for typ under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        document = obj.info

        elems = document.getElementsByTagName(self.tag_name)
        if len(elems):
            elem = None

            for candidate in elems:
                if candidate.parentNode.nodeName == self.parent:
                    elem = candidate

            if elem:
                elem.normalize()
                if elem.hasChildNodes():
                    return elem.firstChild.data.strip()

        return ""

    def __set__(self: _typing.Self, obj: _typing.Any, val: _typing.Any) -> None:
        """
        Writes the element - into an existing element if a suitable one exists - creating one otherwise.

        Example:
            Exercise xml field.  set   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :param obj: Value supplied for obj under the utility contract.
        :param val: Template or metadata value evaluated by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        # Normalize to the empty string
        if not val:
            val = ""
        # Directly access the document
        document = obj.info

        def create_elem() -> _typing.Any:
            """
            Used to make an element if a suitable one doesn't already exist

            Example:
                Exercise xml field.  set  .create elem through a consuming regression::

                    python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            new_elem = document.createElement(self.tag_name)
            parent = document.getElementsByTagName(self.parent)[0]
            parent.appendChild(new_elem)
            return new_elem

        if not val:
            val = ""
        if not isinstance(val, str):
            if isinstance(val, (bytes, bytearray, memoryview)):
                val = bytes(val).decode("utf-8", "replace")
            else:
                val = str(val)

        elems = document.getElementsByTagName(self.tag_name)
        elem = None
        # See if there is an existing element that can be re-purposed or if a new element will have to be created
        if len(elems):

            for candidate in elems:
                if candidate.parentNode.nodeName == self.parent:
                    elem = candidate
            if not elem:
                elem = create_elem()
            else:
                elem.normalize()
                while elem.hasChildNodes():
                    elem.removeChild(elem.lastChild)

        else:
            elem = create_elem()
        elem.appendChild(document.createTextNode(val))

        obj.info = document

    def __str__(self: _typing.Self) -> _typing.Any:
        """
        Perform the str operation under explicit file-format and conversion rules.

        Example:
            Exercise xml field.  str   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.tag_name

    def __repr__(self: _typing.Self) -> _typing.Any:
        """
        Perform the repr operation under explicit file-format and conversion rules.

        Example:
            Exercise xml field.  repr   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return "XML Field: {} in {}".format(self.tag_name, self.parent)


# Todo: Come back and finish and test
class xml_multivalued_field(object):
    """
    Descriptor that gets and sets XML based meta information from an LRF file. Works for simple XMl fields of the form <tagname>data</tagname> - can cope with there being more than one of them.

    Example:
        Exercise xml multivalued field through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py
    """

    def __init__(self: _typing.Self, tag_name: _typing.Any, parent: str = "BookInfo") -> None:
        """
        Initialize and validate the xml multivalued field state.

        Example:
            Exercise xml multivalued field.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :param tag_name: Value supplied for tag name under the utility contract.
        :param parent: Value supplied for parent under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.tag_name = tag_name
        self.parent = parent

    def __get__(self: _typing.Self, obj: _typing.Any, typ: _typing.Any = None) -> _typing.Any:
        """
        Return the data for all the matching fields as a list. Returns an empty list if there are no matching elements.

        Example:
            Exercise xml multivalued field.  get   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :param obj: Value supplied for obj under the utility contract.
        :param typ: Value supplied for typ under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        document = obj.info

        vals = []
        elems = document.getElementsByTagName(self.tag_name)
        for elem in elems:
            if elem.parentNode.nodeName == self.parent:
                elem.normalize()
                if elem.hasChildNodes():
                    vals.append(elem.firstChild.data.strip())

        return vals

    def __set__(self: _typing.Self, instance: _typing.Any, value: _typing.Any) -> None:
        """
        Perform the set operation under explicit file-format and conversion rules.

        Example:
            Exercise xml multivalued field.  set   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :param instance: Value supplied for instance under the utility contract.
        :param value: Value normalized, stored, formatted or returned.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError


def insert_into_file(fileobj: _typing.Any, data: _typing.Any, start: _typing.Any, end: _typing.Any) -> _typing.Any:
    """
    Insert data into fileobj at position C{start}.

    Example:
        Exercise insert into file through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


    :param fileobj: Value supplied for fileobj under the utility contract.
    :param data: Value supplied for data under the utility contract.
    :param start: Value supplied for start under the utility contract.
    :param end: Value supplied for end under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    file_buffer = six_cStringIO()
    fileobj.seek(end)
    copyfileobj(fileobj, file_buffer, -1)
    file_buffer.flush()
    file_buffer.seek(0)
    fileobj.seek(start)
    fileobj.write(data)
    fileobj.flush()
    fileobj.truncate()
    delta = fileobj.tell() - end  # < 0 if len(data) < end-start
    copyfileobj(file_buffer, fileobj, -1)
    fileobj.flush()
    file_buffer.close()
    return delta


def get_metadata(stream: _typing.Any, calibre_md: bool = True) -> _typing.Any:
    """
    Return basic meta-data about the LRF file in C{stream} as a L{MetaInformation} object.

    Example:
        Exercise get metadata through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


    :param stream: Input or output stream wrapped by the terminal or compatibility
        layer.
    :param calibre_md: Value supplied for calibre md under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    lrf = stream if isinstance(stream, LRFMetaFile) else LRFMetaFile(stream)

    authors = string_to_authors(lrf.author)
    if calibre_md:
        mi = calibreMetaInformation(lrf.title, authors)
    else:
        mi = MetaData(lrf.title, authors)

    mi.comments = lrf.free_text
    mi.category = six_unicode(lrf.category).strip() + ", " + six_unicode(lrf.classification).strip()
    tags = [x.strip() for x in mi.category.split(",") if x.strip()]
    if tags:
        mi.tags = tags
    if mi.category.strip() == ",":
        mi.category = None
    mi.publisher = lrf.publisher
    mi.cover_data = lrf.get_cover()
    try:
        mi.title_sort = lrf.title_reading
        if not mi.title_sort:
            mi.title_sort = None
    except:
        pass
    try:
        mi.author_sort = lrf.author_reading
        if not mi.author_sort:
            mi.author_sort = None
    except:
        pass

    # No additional processing is needed - the LiuXin metadata object should take care of it all with a call to finalize
    if not calibre_md:
        mi.finalize()
        return mi

    # Todo: Move these standardizations into LiuXin.metadata.MetaData
    if not mi.title or "unknown" in mi.title.lower():
        mi.title = None
    if not mi.authors:
        mi.authors = None
    if not mi.author or "unknown" in six_unicode(mi.author).lower():
        mi.author = None
    if not mi.category or "unknown" in mi.category.lower():
        mi.category = None
    if (
        not mi.publisher
        or "unknown" in six_unicode(mi.publisher).lower()
        or "some publisher" in six_unicode(mi.publisher).lower()
    ):
        mi.publisher = None

    return mi


class LRFMetaFile(object):
    """
    Provides fields to read and write all metadata to an LRF file.

    Example:
        Exercise LRFMetaFile through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py
    """

    #: The first 6 bytes of all valid LRF files
    LRF_HEADER = "LRF".encode("utf-16le")

    lrf_header = fixed_stringfield(length=6, start=0x0)
    version = field(fmt=WORD, start=0x8)
    xor_key = field(fmt=WORD, start=0xA)
    root_object_id = field(fmt=DWORD, start=0xC)
    number_of_objects = field(fmt=QWORD, start=0x10)
    object_index_offset = field(fmt=QWORD, start=0x18)
    binding = field(fmt=BYTE, start=0x24)
    dpi = field(fmt=WORD, start=0x26)
    width = field(fmt=WORD, start=0x2A)
    height = field(fmt=WORD, start=0x2C)
    color_depth = field(fmt=BYTE, start=0x2E)
    toc_object_id = field(fmt=DWORD, start=0x44)
    toc_object_offset = field(fmt=DWORD, start=0x48)
    compressed_info_size = field(fmt=WORD, start=0x4C)
    thumbnail_type = versioned_field(version, 800, fmt=WORD, start=0x4E)
    thumbnail_size = versioned_field(version, 800, fmt=DWORD, start=0x50)
    uncompressed_info_size = versioned_field(compressed_info_size, 0, fmt=DWORD, start=0x54)

    title = xml_field("Title", parent="BookInfo")
    title_reading = xml_attr_field("Title", "reading", parent="BookInfo")
    author = xml_field("Author", parent="BookInfo")
    author_reading = xml_attr_field("Author", "reading", parent="BookInfo")

    # 16 characters. First two chars should be FB for personal use ebooks.
    book_id = xml_field("BookID", parent="BookInfo")
    publisher = xml_field("Publisher", parent="BookInfo")
    label = xml_field("Label", parent="BookInfo")
    category = xml_field("Category", parent="BookInfo")
    classification = xml_field("Classification", parent="BookInfo")
    free_text = xml_field("FreeText", parent="BookInfo")

    # Should use ISO 639 language codes
    language = xml_field("Language", parent="DocInfo")
    creator = xml_field("Creator", parent="DocInfo")

    # Format is %Y-%m-%d
    creation_date = xml_field("CreationDate", parent="DocInfo")
    producer = xml_field("Producer", parent="DocInfo")
    page = xml_field("SumPage", parent="DocInfo")

    def safe(func: _typing.Any) -> _typing.Any:
        """
        Decorator that ensures that function calls leave the pos in the underlying file unchanged

        Example:
            Exercise LRFMetaFile.safe through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        @wraps(func)
        def restore_pos(*args: _typing.Any, **kwargs: _typing.Any) -> _typing.Any:
            """
            Perform the restore pos operation under explicit file-format and conversion rules.

            Example:
                Exercise LRFMetaFile.safe.restore pos through a consuming regression::

                    python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


            :param args: Positional values forwarded to the compatibility implementation.
            :param kwargs: Keyword values forwarded to the compatibility implementation.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            obj = args[0]
            pos = obj._file.tell()
            res = func(*args, **kwargs)
            obj._file.seek(0, 2)
            if obj._file.tell() >= pos:
                obj._file.seek(pos)
            return res

        return restore_pos

    def safe_property(func: _typing.Any) -> _typing.Any:
        """
        Decorator that ensures that read or writing a property leaves the position in the underlying file unchanged

        Example:
            Exercise LRFMetaFile.safe property through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        def decorator(f: _typing.Any) -> _typing.Any:
            """
            Perform the decorator operation under explicit file-format and conversion rules.

            Example:
                Exercise LRFMetaFile.safe property.decorator through a consuming regression::

                    python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


            :param f: Value supplied for f under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            def restore_pos(*args: _typing.Any, **kwargs: _typing.Any) -> _typing.Any:
                """
                Perform the restore pos operation under explicit file-format and conversion rules.

                Example:
                    Exercise LRFMetaFile.safe property.decorator.restore pos through a consuming regression::

                        python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


                :param args: Positional values forwarded to the compatibility implementation.
                :param kwargs: Keyword values forwarded to the compatibility implementation.
                :return: The normalized value, metadata record, path, stream result or collection
                    described above.
                """
                obj = args[0]
                pos = obj._file.tell()
                res = f(*args, **kwargs)
                obj._file.seek(0, 2)
                if obj._file.tell() >= pos:
                    obj._file.seek(pos)
                return res

            return restore_pos

        locals_ = func()
        if "fget" in locals_:
            locals_["fget"] = decorator(locals_["fget"])
        if "fset" in locals_:
            locals_["fset"] = decorator(locals_["fset"])
        return property(**locals_)

    @safe_property
    def info() -> dict[_typing.Any, _typing.Any]:
        """
        Perform the info operation under explicit file-format and conversion rules.

        Example:
            Exercise LRFMetaFile.info through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        doc = """
        Document meta information as a minidom Document object.
        To set use a minidom document object.
        """

        def fget(self: _typing.Any) -> _typing.Any:
            """
            Perform the fget operation under explicit file-format and conversion rules.

            Example:
                Exercise LRFMetaFile.info.fget through a consuming regression::

                    python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


            :param self: Value supplied for self under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            if self.compressed_info_size == 0:
                raise LRFException("This document has no meta info")
            size = self.compressed_info_size - 4
            self._file.seek(self.info_start)
            try:
                src = zlib.decompress(self._file.read(size))
                if len(src) != self.uncompressed_info_size:
                    raise LRFException(
                        "Decompression of document meta info\
                                        yielded unexpected results"
                    )
                src_bytes = src if isinstance(src, (bytes, bytearray)) else str(src).encode("utf-8", "replace")
                for candidate in (src_bytes, src_bytes.replace(b"\x00", b"").strip()):
                    try:
                        return dom.parseString(candidate)
                    except Exception:
                        pass
                repaired = src_bytes.replace(b"\x00", b"").strip().decode("latin1", "replace")
                return dom.parseString(repaired.encode("utf-8"))
            except zlib.error:
                raise LRFException("Unable to decompress document meta information")

        def fset(self: _typing.Any, document: _typing.Any) -> None:
            """
            Perform the fset operation under explicit file-format and conversion rules.

            Example:
                Exercise LRFMetaFile.info.fset through a consuming regression::

                    python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


            :param self: Value supplied for self under the utility contract.
            :param document: Value supplied for document under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            info = document.toxml("utf-8")
            self.uncompressed_info_size = len(info)
            stream = zlib.compress(info)
            orig_size = self.compressed_info_size
            self.compressed_info_size = len(stream) + 4
            delta = insert_into_file(self._file, stream, self.info_start, self.info_start + orig_size - 4)

            if self.toc_object_offset > 0:
                self.toc_object_offset += delta
            self.object_index_offset += delta
            self.update_object_offsets(delta)

        return {"fget": fget, "fset": fset, "doc": doc}

    @safe_property
    def thumbnail_pos() -> dict[_typing.Any, _typing.Any]:
        """
        Perform the thumbnail pos operation under explicit file-format and conversion rules.

        Example:
            Exercise LRFMetaFile.thumbnail pos through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        doc = """ The position of the thumbnail in the LRF file """

        def fget(self: _typing.Any) -> _typing.Any:
            """
            Perform the fget operation under explicit file-format and conversion rules.

            Example:
                Exercise LRFMetaFile.thumbnail pos.fget through a consuming regression::

                    python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


            :param self: Value supplied for self under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self.info_start + self.compressed_info_size - 4

        return {"fget": fget, "doc": doc}

    @classmethod
    def _detect_thumbnail_type(cls: type[_typing.Self], slice: _typing.Any) -> _typing.Any:
        """
        @param slice: The first 16 bytes of the thumbnail

        Example:
            Exercise LRFMetaFile. detect thumbnail type through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :param slice: Value supplied for slice under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if isinstance(slice, str):
            slice = slice.encode("latin-1", "replace")
        ttype = 0x14  # GIF
        if b"PNG" in slice:
            ttype = 0x12
        if b"BM" in slice:
            ttype = 0x13
        if b"JFIF" in slice:
            ttype = 0x11
        return ttype

    @safe_property
    def thumbnail() -> dict[_typing.Any, _typing.Any]:
        """
        Perform the thumbnail operation under explicit file-format and conversion rules.

        Example:
            Exercise LRFMetaFile.thumbnail through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        doc = """
        The thumbnail.
        Represented as a string.
        The string you would get from the file read function.
        """

        def fget(self: _typing.Any) -> _typing.Any:
            """
            Perform the fget operation under explicit file-format and conversion rules.

            Example:
                Exercise LRFMetaFile.thumbnail.fget through a consuming regression::

                    python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


            :param self: Value supplied for self under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            size = self.thumbnail_size
            if size:
                self._file.seek(self.thumbnail_pos)
                return self._file.read(size)

        def fset(self: _typing.Any, data: _typing.Any) -> None:
            """
            Perform the fset operation under explicit file-format and conversion rules.

            Example:
                Exercise LRFMetaFile.thumbnail.fset through a consuming regression::

                    python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


            :param self: Value supplied for self under the utility contract.
            :param data: Value supplied for data under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            if self.version <= 800:
                raise LRFException("Cannot store thumbnails in LRF files of version <= 800")
            slice = data[0:16]
            orig_size = self.thumbnail_size
            self.thumbnail_size = len(data)
            delta = insert_into_file(self._file, data, self.thumbnail_pos, self.thumbnail_pos + orig_size)
            self.toc_object_offset += delta
            self.object_index_offset += delta
            self.thumbnail_type = self._detect_thumbnail_type(slice)
            self.update_object_offsets(delta)

        return {"fget": fget, "fset": fset, "doc": doc}

    def __init__(self: _typing.Self, file: _typing.Any) -> None:
        """
        @param file: A file object opened in the r+b mode

        Example:
            Exercise LRFMetaFile.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :param file: Value supplied for file under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        file.seek(0, 2)
        self.size = file.tell()
        self._file = file
        if self.lrf_header != LRFMetaFile.LRF_HEADER:
            raise LRFException(file.name + " has an invalid LRF header. Are you sure it is an LRF file?")
        # Byte at which the compressed meta information starts
        self.info_start = 0x58 if self.version > 800 else 0x53

    @safe
    def update_object_offsets(self: _typing.Self, delta: _typing.Any) -> None:
        """
        Run through the LRF Object index changing the offset by C{delta}.

        Example:
            Exercise LRFMetaFile.update object offsets through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :param delta: Value supplied for delta under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._file.seek(self.object_index_offset)
        count = self.number_of_objects
        while count > 0:
            raw = self._file.read(8)
            new_offset = struct.unpack(DWORD, raw[4:8])[0] + delta
            if new_offset >= (2**8) ** 4 or new_offset < 0x4C:
                raise LRFException(_("Invalid LRF file. Could not set metadata."))
            self._file.seek(-4, os.SEEK_CUR)
            self._file.write(struct.pack(DWORD, new_offset))
            self._file.seek(8, os.SEEK_CUR)
            count -= 1
        self._file.flush()

    @safe
    def unpack(self: _typing.Self, fmt: _typing.Any = DWORD, start: int = 0) -> _typing.Any:
        """
        Return decoded data from file.

        Example:
            Exercise LRFMetaFile.unpack through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :param fmt: Date, number or template format specification.
        :param start: Value supplied for start under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        end = start + struct.calcsize(fmt)
        self._file.seek(start)
        ret = struct.unpack(fmt, self._file.read(end - start))
        return ret

    @safe
    def pack(self: _typing.Self, *args: _typing.Any, **kwargs: _typing.Any) -> None:
        """
        Encode C{args} and write them to file. C{kwargs} must contain the keywords C{fmt} and C{start}

        Example:
            Exercise LRFMetaFile.pack through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        encoded = struct.pack(kwargs["fmt"], *args)
        self._file.seek(kwargs["start"])
        self._file.write(encoded)
        self._file.flush()

    def thumbail_extension(self: _typing.Self) -> _typing.Any:
        """
        Return the extension for the thumbnail image type as specified by L{self.thumbnail_type}. If the LRF file was created by buggy software, the extension maye be incorrect. See L{self.fix_thumbnail_type}.

        Example:
            Exercise LRFMetaFile.thumbail extension through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ext = "gif"
        ttype = self.thumbnail_type
        if ttype == 0x11:
            ext = "jpeg"
        elif ttype == 0x12:
            ext = "png"
        elif ttype == 0x13:
            ext = "bmp"
        return ext

    def fix_thumbnail_type(self: _typing.Self) -> None:
        """
        Attempt to guess the thumbnail image format and set L{self.thumbnail_type} accordingly.

        Example:
            Exercise LRFMetaFile.fix thumbnail type through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        slice = self.thumbnail[0:16]
        self.thumbnail_type = self._detect_thumbnail_type(slice)

    def seek(self: _typing.Self, *args: _typing.Any) -> _typing.Any:
        """
        See L{file.seek}

        Example:
            Exercise LRFMetaFile.seek through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :param args: Positional values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self._file.seek(*args)

    def tell(self: _typing.Self) -> _typing.Any:
        """
        See L{file.tell}

        Example:
            Exercise LRFMetaFile.tell through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self._file.tell()

    def read(self: _typing.Self) -> _typing.Any:
        """
        See L{file.read}

        Example:
            Exercise LRFMetaFile.read through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self._file.read()

    def write(self: _typing.Self, val: _typing.Any) -> None:
        """
        See L{file.write}

        Example:
            Exercise LRFMetaFile.write through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :param val: Template or metadata value evaluated by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._file.write(val)

    def _objects(self: _typing.Self) -> _typing.Iterator[_typing.Any]:
        """
        Perform the objects operation under explicit file-format and conversion rules.

        Example:
            Exercise LRFMetaFile. objects through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :return: An iterator yielding the normalized values described above.
        """
        self._file.seek(self.object_index_offset)
        c = self.number_of_objects
        while c > 0:
            c -= 1
            raw = self._file.read(16)
            pos = self._file.tell()
            yield struct.unpack("<IIII", raw)[:3]
            self._file.seek(pos)

    def get_objects_by_type(self: _typing.Self, type: _typing.Any) -> _typing.Any:
        """
        Return objects by type under the format's safety and compatibility rules.

        Example:
            Exercise LRFMetaFile.get objects by type through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :param type: Value supplied for type under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        from LiuXin_alpha.file_formats.lrf.tags import Tag

        objects = []
        for id, offset, size in self._objects():
            self._file.seek(offset)
            tag = Tag(self._file)
            if tag.id == 0xF500:
                obj_id, obj_type = struct.unpack("<IH", tag.contents)
                if obj_type == type:
                    objects.append((obj_id, offset, size))
        return objects

    def get_object_by_id(self: _typing.Self, tid: _typing.Any) -> tuple[_typing.Any, ...]:
        """
        Return object by id under the format's safety and compatibility rules.

        Example:
            Exercise LRFMetaFile.get object by id through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :param tid: Value supplied for tid under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        from LiuXin_alpha.file_formats.lrf.tags import Tag

        for id, offset, size in self._objects():
            self._file.seek(offset)
            tag = Tag(self._file)
            if tag.id == 0xF500:
                obj_id, obj_type = struct.unpack("<IH", tag.contents)
                if obj_id == tid:
                    return obj_id, offset, size, obj_type
        return False, False, False, False

    @safe
    def get_cover(self: _typing.Self) -> tuple[_typing.Any, ...] | None:
        """
        Return the preferred cover image and its normalized format metadata.

        Example:
            Exercise LRFMetaFile.get cover through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        from LiuXin_alpha.file_formats.lrf.objects import get_object

        for id, offset, size in self.get_objects_by_type(0x0C):
            image = get_object(None, self._file, id, offset, size, self.xor_key)
            id, offset, size = self.get_object_by_id(image.refstream)[:3]
            image_stream = get_object(None, self._file, id, offset, size, self.xor_key)
            return image_stream.file.rpartition(".")[-1], image_stream.stream
        return None


def option_parser() -> _typing.Any:
    """
    Perform the option parser operation under explicit file-format and conversion rules.

    Example:
        Exercise option parser through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    from LiuXin_alpha.utils.config import OptionParser
    from LiuXin_alpha.utils.calibre.constants import __appname__, __version__

    parser = OptionParser(
        usage=_(
            """%prog [options] mybook.lrf


Show/edit the metadata in an LRF file.\n\n"""
        ),
        version=__appname__ + " " + __version__,
        epilog="Created by Kovid Goyal",
    )
    parser.add_option(
        "-t",
        "--title",
        action="store",
        type="string",
        dest="title",
        help=_("Set the book title"),
    )
    parser.add_option(
        "--title-sort",
        action="store",
        type="string",
        default=None,
        dest="title_reading",
        help=_("Set sort key for the title"),
    )
    parser.add_option(
        "-a",
        "--author",
        action="store",
        type="string",
        dest="author",
        help=_("Set the author"),
    )
    parser.add_option(
        "--author-sort",
        action="store",
        type="string",
        default=None,
        dest="author_reading",
        help=_("Set sort key for the author"),
    )
    parser.add_option(
        "-c",
        "--category",
        action="store",
        type="string",
        dest="category",
        help=_("The category this book belongs to. E.g.: History"),
    )
    parser.add_option(
        "--thumbnail",
        action="store",
        type="string",
        dest="thumbnail",
        help=_("Path to a graphic that will be set as this files' thumbnail"),
    )
    parser.add_option(
        "--comment",
        action="store",
        type="string",
        dest="comment",
        help=_("Path to a txt file containing the comment to be stored in the lrf file."),
    )
    parser.add_option(
        "--get-thumbnail",
        action="store_true",
        dest="get_thumbnail",
        default=False,
        help=_("Extract thumbnail from LRF file"),
    )
    parser.add_option("--publisher", default=None, help=_("Set the publisher"))
    parser.add_option("--classification", default=None, help=_("Set the book classification"))
    parser.add_option("--creator", default=None, help=_("Set the book creator"))
    parser.add_option("--producer", default=None, help=_("Set the book producer"))
    parser.add_option(
        "--get-cover",
        action="store_true",
        default=False,
        help=_(
            "Extract cover from LRF file. Note that the LRF format has no defined cover, so we use "
            "some heuristics to guess the cover."
        ),
    )
    parser.add_option(
        "--bookid",
        action="store",
        type="string",
        default=None,
        dest="book_id",
        help=_("Set book ID"),
    )
    # The SumPage element specifies the number of "View"s (visible pages for the BookSetting element conditions) of the content.
    # Basically, the total pages per the page size, font size, etc. when the LRF is first created. Since this will change as the book is reflowed, it is probably not worth using.
    # parser.add_option("-p", "--page", action="store", type="string", \
    #                dest="page", help=_("Don't know what this is for"))

    return parser


def set_metadata(stream: _typing.Any, mi: _typing.Any) -> None:
    """
    Write the given metadata into a lrf stream. Supports writing title, authors, tags, comments, author_sort and publisher

    Example:
        Exercise set metadata through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


    :param stream: Input or output stream wrapped by the terminal or compatibility
        layer.
    :param mi: Metadata object exposed to the template function.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    lrf = LRFMetaFile(stream)

    if mi.title:
        lrf.title = mi.title

    if mi.authors:
        lrf.author = authors_to_string(authors=mi.authors, xml_safe=True)

    # Tags are stored in category and classification - nullify the classification and store them as a csv in category
    if mi.tags:
        lrf.classification = ""
        lrf.category = ", ".join(mi.tags)

    if getattr(mi, "category", False):
        lrf.category = mi.category

    if mi.comments:
        lrf.free_text = mi.comments

    if mi.author_sort:
        lrf.author_reading = mi.author_sort

    if mi.publisher:
        lrf.publisher = mi.publisher


def main(args: _typing.Any = sys.argv) -> int:
    """
    Perform the main operation under explicit file-format and conversion rules.

    Example:
        Exercise main through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


    :param args: Positional values forwarded to the compatibility implementation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    parser = option_parser()
    options, args = parser.parse_args(args)
    if len(args) != 2:
        parser.print_help()
        print()
        print("No lrf file specified")
        return 1
    lrf = LRFMetaFile(open(args[1], "r+b"))

    if options.title:
        lrf.title = options.title
    if options.title_reading is None:
        lrf.title_reading = options.title_reading
    if options.author_reading is None:
        lrf.author_reading = options.author_reading
    if options.author:
        lrf.author = options.author
    if options.publisher:
        lrf.publisher = options.publisher
    if options.classification:
        lrf.classification = options.classification
    if options.category:
        lrf.category = options.category
    if options.creator:
        lrf.creator = options.creator
    if options.producer:
        lrf.producer = options.producer
    if options.thumbnail:
        path = os.path.expanduser(os.path.expandvars(options.thumbnail))
        f = open(path, "rb")
        lrf.thumbnail = f.read()
        f.close()
    if options.book_id is not None:
        lrf.book_id = options.book_id
    if options.comment:
        path = os.path.expanduser(os.path.expandvars(options.comment))
        lrf.free_text = open(path).read()
    td = "None"
    if options.get_thumbnail:
        t = lrf.thumbnail
        td = "None"
        if t and len(t) > 0:
            td = os.path.basename(args[1]) + "_thumbnail." + lrf.thumbail_extension()
            f = open(td, "w")
            f.write(t)
            f.close()

    fields = LRFMetaFile.__dict__.items()
    fields.sort()
    for f in fields:
        if "XML" in str(f):
            print(str(f[1]) + ":", lrf.__getattribute__(f[0]).encode("utf-8"))
    if options.get_thumbnail:
        print("Thumbnail:", td)
    if options.get_cover:
        try:
            ext, data = lrf.get_cover()
        except:  # Fails on books created by LRFCreator 1.0
            ext, data = None, None
        if data:
            cover = os.path.splitext(os.path.basename(args[1]))[0] + "_cover." + ext
            open(cover, "wb").write(data)
            print("Cover:", cover)
        else:
            print("Could not find cover in the LRF file")


if __name__ == "__main__":
    sys.exit(main())

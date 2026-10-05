#!/usr/bin/env python
# vim:fileencoding=utf-8

"""
Inspect and describe MOBI container sections and records.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise containers through a consuming regression::

        python -m pytest -q tests/file_formats/mobi/test_mobi_modernized.py
"""
from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

import typing as _typing

from struct import unpack_from

from LiuXin_alpha.file_formats.mobi.debug.headers import EXTHHeader

__license__ = "GPL v3"
__copyright__ = "2014, Kovid Goyal <kovid at kovidgoyal.net>"


class ContainerHeader(object):
    """
    Provide the containerheader contract for validated ebook processing.

    Example:
        Exercise ContainerHeader through a consuming regression::

            python -m pytest -q tests/file_formats/mobi/test_mobi_modernized.py
    """
    def __init__(self: _typing.Self, data: _typing.Any) -> None:
        """
        Initialize and validate the containerheader state.

        Example:
            Exercise ContainerHeader.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/mobi/test_mobi_modernized.py


        :param data: Value supplied for data under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.ident = data[:4]
        self.record_size, self.type, self.count, self.encoding = unpack_from(b">IHHI", data, 4)
        self.encoding = {
            1252: "cp1252",
            65001: "utf-8",
        }.get(self.encoding, repr(self.encoding))
        rest = list(unpack_from(b">IIIIIIII", data, 16))
        self.num_of_resource_records = rest[2]
        self.num_of_non_dummy_resource_records = rest[3]
        self.offset_to_href_record = rest[4]
        self.unknowns1 = rest[:2]
        self.unknowns2 = rest[5]
        self.header_length = rest[6]
        self.title_length = rest[7]
        self.resources = []
        self.hrefs = []

        if data[48:52] == b"EXTH":
            self.exth = EXTHHeader(data[48:])
            self.title = data[48 + self.exth.length :][: self.title_length].decode(self.encoding)
            self.is_image_container = self.exth[539] == "application/image"
        else:
            self.exth = " No EXTH header present "
            self.title = ""
            self.is_image_container = False

        self.bytes_after_exth = data[self.header_length + self.title_length :]
        self.null_bytes_after_exth = len(self.bytes_after_exth) - len(self.bytes_after_exth.replace(b"\0", b""))

    def add_hrefs(self: _typing.Self, data: _typing.Any) -> None:
        # kindlegen inserts a trailing | after the last href
        """
        Perform the add hrefs operation under explicit file-format and conversion rules.

        Example:
            Exercise ContainerHeader.add hrefs through a consuming regression::

                python -m pytest -q tests/file_formats/mobi/test_mobi_modernized.py


        :param data: Value supplied for data under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.hrefs = filter(None, data.decode("utf-8").split("|"))

    def __str__(self: _typing.Self) -> _typing.Any:
        """
        Perform the str operation under explicit file-format and conversion rules.

        Example:
            Exercise ContainerHeader.  str   through a consuming regression::

                python -m pytest -q tests/file_formats/mobi/test_mobi_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ans = [("*" * 10) + " Container Header " + ("*" * 10)]
        a = ans.append
        a("Record size: %d" % self.record_size)
        a("Type: %d" % self.type)
        a("Total number of records in this container: %d" % self.count)
        a("Encoding: %s" % self.encoding)
        a("Unknowns1: %s" % self.unknowns1)
        a("Num of resource records: %d" % self.num_of_resource_records)
        a("Num of non-dummy resource records: %d" % self.num_of_non_dummy_resource_records)
        a("Offset to href record: %d" % self.offset_to_href_record)
        a("Unknowns2: %s" % self.unknowns2)
        a("Header length: %d" % self.header_length)
        a("Title Length: %s" % self.title_length)
        a("hrefs: %s" % self.hrefs)
        a("Null bytes after EXTH: %d" % self.null_bytes_after_exth)
        if len(self.bytes_after_exth) != self.null_bytes_after_exth:
            a("Non-null bytes present after EXTH header!!!!")
        return "\n".join(ans) + "\n\n" + str(self.exth) + "\n\n" + ("Title: %s" % self.title)

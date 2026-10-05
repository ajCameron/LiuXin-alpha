#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (C) 2006-2009 Søren Roug, European Environment Agency
#
# This is free software.  You may redistribute it under the terms
# of the Apache license and the GNU General Public License Version
# 2 or at your option any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public
# License along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
#
# Contributor(s): Michael Howitz, gocept gmbh & co. kg
#
# $Id: userfield.py 447 2008-07-10 20:01:30Z roug $

"""
Inspect and update ODF user-field declarations and values.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise userfield through a consuming regression::

        python -m pytest -q tests/file_formats/odf/test_odf_modernized.py
"""
from __future__ import annotations

import typing as _typing

import sys
import zipfile

from LiuXin_alpha.file_formats.odf.text import UserFieldDecl
from LiuXin_alpha.file_formats.odf.namespaces import OFFICENS
from LiuXin_alpha.file_formats.odf.opendocument import load

from LiuXin_alpha.utils.libraries.liuxin_six import six_string_types

OUTENCODING = "utf-8"


# OpenDocument v.1.0 section 6.7.1
VALUE_TYPES = {
    "float": (OFFICENS, "value"),
    "percentage": (OFFICENS, "value"),
    "currency": (OFFICENS, "value"),
    "date": (OFFICENS, "date-value"),
    "time": (OFFICENS, "time-value"),
    "boolean": (OFFICENS, "boolean-value"),
    "string": (OFFICENS, "string-value"),
}


class UserFields(object):
    """
    List, view and manipulate user fields.

    Example:
        Exercise UserFields through a consuming regression::

            python -m pytest -q tests/file_formats/odf/test_odf_modernized.py
    """

    # these attributes can be a filename or a file like object
    src_file = None
    dest_file = None

    def __init__(self: _typing.Self, src: _typing.Any = None, dest: _typing.Any = None) -> None:
        """
        Constructor

        Example:
            Exercise UserFields.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param src: Value supplied for src under the utility contract.
        :param dest: Value supplied for dest under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.src_file = src
        self.dest_file = dest
        self.document = None

    def loaddoc(self: _typing.Self) -> None:
        """
        Perform the loaddoc operation under explicit file-format and conversion rules.

        Example:
            Exercise UserFields.loaddoc through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if isinstance(self.src_file, six_string_types):
            # src_file is a filename, check if it is a zip-file
            if not zipfile.is_zipfile(self.src_file):
                raise TypeError("%s is no odt file." % self.src_file)
        elif self.src_file is None:
            # use stdin if no file given
            self.src_file = sys.stdin

        self.document = load(self.src_file)

    def savedoc(self: _typing.Self) -> None:
        # write output
        """
        Perform the savedoc operation under explicit file-format and conversion rules.

        Example:
            Exercise UserFields.savedoc through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.dest_file is None:
            # use stdout if no filename given
            self.document.save("-")
        else:
            self.document.save(self.dest_file)

    def list_fields(self: _typing.Self) -> _typing.Any:
        """
        List (extract) all known user-fields.

        Example:
            Exercise UserFields.list fields through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return [x[0] for x in self.list_fields_and_values()]

    def list_fields_and_values(self: _typing.Self, field_names: _typing.Any = None) -> _typing.Any:
        """
        List (extract) user-fields with type and value.

        Example:
            Exercise UserFields.list fields and values through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param field_names: Value supplied for field names under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        self.loaddoc()
        found_fields = []
        all_fields = self.document.getElementsByType(UserFieldDecl)
        for f in all_fields:
            value_type = f.getAttribute("valuetype")
            if value_type == "string":
                value = f.getAttribute("stringvalue")
            else:
                value = f.getAttribute("value")
            field_name = f.getAttribute("name")

            if field_names is None or field_name in field_names:
                found_fields.append(
                    (
                        field_name.encode(OUTENCODING),
                        value_type.encode(OUTENCODING),
                        value.encode(OUTENCODING),
                    )
                )
        return found_fields

    def list_values(self: _typing.Self, field_names: _typing.Any) -> _typing.Any:
        """
        Extract the contents of given field names from the file.

        Example:
            Exercise UserFields.list values through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param field_names: Value supplied for field names under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return [x[2] for x in self.list_fields_and_values(field_names)]

    def get(self: _typing.Self, field_name: _typing.Any) -> _typing.Any:
        """
        Extract the contents of this field from the file.

        Example:
            Exercise UserFields.get through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param field_name: Value supplied for field name under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        values = self.list_values([field_name])
        if not values:
            return None
        return values[0]

    def get_type_and_value(self: _typing.Self, field_name: _typing.Any) -> tuple[_typing.Any, ...] | None:
        """
        Extract the type and contents of this field from the file.

        Example:
            Exercise UserFields.get type and value through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param field_name: Value supplied for field name under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        fields = self.list_fields_and_values([field_name])
        if not fields:
            return None
        field_name, value_type, value = fields[0]
        return value_type, value

    def update(self: _typing.Self, data: _typing.Any) -> None:
        """
        Set the value of user fields. The field types will be the same.

        Example:
            Exercise UserFields.update through a consuming regression::

                python -m pytest -q tests/file_formats/odf/test_odf_modernized.py


        :param data: Value supplied for data under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.loaddoc()
        all_fields = self.document.getElementsByType(UserFieldDecl)
        for f in all_fields:
            field_name = f.getAttribute("name")
            if field_name in data:
                value_type = f.getAttribute("valuetype")
                value = data.get(field_name)
                if value_type == "string":
                    f.setAttribute("stringvalue", value)
                else:
                    f.setAttribute("value", value)
        self.savedoc()

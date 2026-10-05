# -*- coding: utf-8 -*-
"""
Parse and serialize Palm database container headers and records.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise header through a consuming regression::

        python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py
"""
from __future__ import annotations

import typing as _typing

import re
import struct
import time

from LiuXin_alpha.file_formats.pdb import PDBError

__license__ = "GPL v3"
__copyright__ = "2009, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"


PALMDB_HEADER_SIZE = 78
PALMDB_RECORD_TABLE_ENTRY_SIZE = 8
PALMDB_RECORD_TABLE_TRAILER_SIZE = 2


class PdbHeaderReader(object):
    """
    Parse pdbheaderreader data into normalized ebook structures.

    Example:
        Exercise PdbHeaderReader through a consuming regression::

            python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py
    """
    def __init__(self: _typing.Self, stream: _typing.Any) -> None:
        """
        Initialize and validate the pdbheaderreader state.

        Example:
            Exercise PdbHeaderReader.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py


        :param stream: Input or output stream wrapped by the terminal or compatibility
            layer.
        :return: None; validated state is stored on the receiving object.
        """
        self.stream = stream
        self.stream_length = self._stream_length()
        self.ident = self.identity()
        self.num_sections = self.section_count()
        self.title = self.name()
        self.section_headers = self._read_section_table()

    def _stream_length(self: _typing.Self) -> _typing.Any:
        """
        Perform the stream length operation under explicit file-format and conversion rules.

        Example:
            Exercise PdbHeaderReader. stream length through a consuming regression::

                python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            current = self.stream.tell()
        except Exception:
            current = None
        try:
            self.stream.seek(0, 2)
            return self.stream.tell()
        except Exception as err:
            raise PDBError("Unable to determine PDB stream length") from err
        finally:
            try:
                self.stream.seek(0 if current is None else current)
            except Exception:
                pass

    def _read_exact(self: _typing.Self, offset: _typing.Any, length: _typing.Any, context: _typing.Any) -> _typing.Any:
        """
        Read exact under the format's safety and compatibility rules.

        Example:
            Exercise PdbHeaderReader. read exact through a consuming regression::

                python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py


        :param offset: Value supplied for offset under the utility contract.
        :param length: Value supplied for length under the utility contract.
        :param context: Value supplied for context under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            self.stream.seek(offset)
            data = self.stream.read(length)
        except Exception as err:
            raise PDBError("Unable to read %s" % context) from err
        if isinstance(data, str):
            data = data.encode("latin-1", "replace")
        if len(data) != length:
            raise PDBError("Truncated %s" % context)
        return data

    def _validate_section_number(self: _typing.Self, number: _typing.Any) -> None:
        """
        Validate section number under the format's safety and compatibility rules.

        Example:
            Exercise PdbHeaderReader. validate section number through a consuming regression::

                python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py


        :param number: Value supplied for number under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if not (0 <= number < self.num_sections):
            raise PDBError("Not a valid section number %i" % number)

    def identity(self: _typing.Self) -> _typing.Any:
        """
        Perform the identity operation under explicit file-format and conversion rules.

        Example:
            Exercise PdbHeaderReader.identity through a consuming regression::

                python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self._read_exact(60, 8, "PDB identity").decode("utf-8", "replace")

    def section_count(self: _typing.Self) -> _typing.Any:
        """
        Perform the section count operation under explicit file-format and conversion rules.

        Example:
            Exercise PdbHeaderReader.section count through a consuming regression::

                python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        (count,) = struct.unpack(">H", self._read_exact(76, 2, "PDB record count"))
        if count < 1:
            raise PDBError("PDB record count must be at least one")
        return count

    def name(self: _typing.Self) -> _typing.Any:
        """
        Perform the name operation under explicit file-format and conversion rules.

        Example:
            Exercise PdbHeaderReader.name through a consuming regression::

                python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        raw_name = self._read_exact(0, 32, "PDB name")
        cleaned = re.sub(br"[^-A-Za-z0-9 ]+", b"_", raw_name.replace(b"\x00", b""))
        return cleaned.decode("ascii", "replace")

    def _read_section_table(self: _typing.Self) -> _typing.Any:
        """
        Read section table under the format's safety and compatibility rules.

        Example:
            Exercise PdbHeaderReader. read section table through a consuming regression::

                python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        table_start = PALMDB_HEADER_SIZE
        table_length = self.num_sections * PALMDB_RECORD_TABLE_ENTRY_SIZE
        table_end = table_start + table_length
        first_record_offset = table_end + PALMDB_RECORD_TABLE_TRAILER_SIZE
        if first_record_offset > self.stream_length:
            raise PDBError("PDB record table extends beyond the file")

        headers = []
        for number in range(self.num_sections):
            raw = self._read_exact(
                table_start + number * PALMDB_RECORD_TABLE_ENTRY_SIZE,
                PALMDB_RECORD_TABLE_ENTRY_SIZE,
                "PDB record table entry",
            )
            offset, a1, a2, a3, a4 = struct.unpack(">LBBBB", raw)
            flags, val = a1, a2 << 16 | a3 << 8 | a4
            headers.append((offset, flags, val))

        previous_offset = None
        for offset, _flags, _val in headers:
            if offset < first_record_offset:
                raise PDBError("PDB record offset points inside the header")
            if offset > self.stream_length:
                raise PDBError("PDB record offset points beyond the file")
            if previous_offset is not None and offset <= previous_offset:
                raise PDBError("PDB record offsets must be strictly increasing")
            previous_offset = offset

        return tuple(headers)

    def full_section_info(self: _typing.Self, number: _typing.Any) -> _typing.Any:
        """
        Perform the full section info operation under explicit file-format and conversion rules.

        Example:
            Exercise PdbHeaderReader.full section info through a consuming regression::

                python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py


        :param number: Value supplied for number under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        self._validate_section_number(number)
        return self.section_headers[number]

    def section_offset(self: _typing.Self, number: _typing.Any) -> _typing.Any:
        """
        Perform the section offset operation under explicit file-format and conversion rules.

        Example:
            Exercise PdbHeaderReader.section offset through a consuming regression::

                python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py


        :param number: Value supplied for number under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        self._validate_section_number(number)
        return self.section_headers[number][0]

    def section_data(self: _typing.Self, number: _typing.Any) -> _typing.Any:
        """
        Perform the section data operation under explicit file-format and conversion rules.

        Example:
            Exercise PdbHeaderReader.section data through a consuming regression::

                python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py


        :param number: Value supplied for number under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        self._validate_section_number(number)

        start = self.section_offset(number)
        if number == self.num_sections - 1:
            end = self.stream_length
        else:
            end = self.section_offset(number + 1)
        return self._read_exact(start, end - start, "PDB record data")


class PdbHeaderBuilder(object):
    """
    Provide the pdbheaderbuilder contract for validated ebook processing.

    Example:
        Exercise PdbHeaderBuilder through a consuming regression::

            python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py
    """
    def __init__(self: _typing.Self, identity: _typing.Any, title: _typing.Any) -> None:
        """
        Initialize and validate the pdbheaderbuilder state.

        Example:
            Exercise PdbHeaderBuilder.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py


        :param identity: Value supplied for identity under the utility contract.
        :param title: Value supplied for title under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.identity = identity.ljust(3, "\x00")[:8].encode("utf-8")
        if isinstance(title, str):
            title = title.encode("ascii", "replace")
        self.title = b"%s\x00" % re.sub(br"[^-A-Za-z0-9 ]+", b"_", title).ljust(31, b"\x00")[:31]

    def build_header(self: _typing.Self, section_lengths: _typing.Any, out_stream: _typing.Any) -> None:
        """
        Make a header for a pdb file.

        Example:
            Exercise PdbHeaderBuilder.build header through a consuming regression::

                python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py


        :param section_lengths: Value supplied for section lengths under the utility
            contract.
        :param out_stream: Value supplied for out stream under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        now = int(time.time())
        nrecords = len(section_lengths)

        out_stream.write(self.title + struct.pack(">HHIIIIII", 0, 0, now, now, 0, 0, 0, 0))
        out_stream.write(self.identity + struct.pack(">IIH", nrecords, 0, nrecords))

        offset = 78 + (8 * nrecords) + 2
        for record in section_lengths:
            out_stream.write(struct.pack(">LBBBB", int(offset), 0, 0, 0, 0))
            offset += record
        out_stream.write(b"\x00\x00")

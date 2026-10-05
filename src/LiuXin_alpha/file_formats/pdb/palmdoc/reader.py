# -*- coding: utf-8 -*-

"""
Read the package format into normalized text, metadata and resources.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise reader through a consuming regression::

        python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py
"""
from __future__ import annotations

import typing as _typing

import io
import struct

from LiuXin_alpha.file_formats.pdb import PDBError
from LiuXin_alpha.file_formats.pdb.formatreader import FormatReader

__license__ = "GPL v3"
__copyright__ = "2009, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"

PALMDOC_HEADER_RECORD_SIZE = 16
SUPPORTED_COMPRESSION = {1, 2, 258}


def _require_bytes(raw: _typing.Any, size: _typing.Any, context: _typing.Any) -> None:
    """
    Perform the require bytes operation under explicit file-format and conversion rules.

    Example:
        Exercise  require bytes through a consuming regression::

            python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py


    :param raw: Value supplied for raw under the utility contract.
    :param size: Value supplied for size under the utility contract.
    :param context: Value supplied for context under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    if len(raw) < size:
        raise PDBError("Truncated PalmDOC %s" % context)


class HeaderRecord(object):
    """
    The first record in the file is always the header record. It holds information related to the location of text, images, and so on in the file. This is used in conjunction with the sections defined in the file header.

    Example:
        Exercise HeaderRecord through a consuming regression::

            python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py
    """

    def __init__(self: _typing.Self, raw: _typing.Any) -> None:
        """
        Initialize and validate the headerrecord state.

        Example:
            Exercise HeaderRecord.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py


        :param raw: Value supplied for raw under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        _require_bytes(raw, PALMDOC_HEADER_RECORD_SIZE, "header record")
        (self.compression,) = struct.unpack(">H", raw[0:2])
        (self.num_records,) = struct.unpack(">H", raw[8:10])


class Reader(FormatReader):
    """
    Parse reader data into normalized ebook structures.

    Example:
        Exercise Reader through a consuming regression::

            python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py
    """
    def __init__(self: _typing.Self, header: _typing.Any, stream: _typing.Any, log: _typing.Any, options: _typing.Any) -> None:
        """
        Initialize and validate the reader state.

        Example:
            Exercise Reader.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py


        :param header: Value supplied for header under the utility contract.
        :param stream: Input or output stream wrapped by the terminal or compatibility
            layer.
        :param log: Value supplied for log under the utility contract.
        :param options: Value supplied for options under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.stream = stream
        self.log = log
        self.options = options

        self.sections = []
        for i in range(header.num_sections):
            self.sections.append(header.section_data(i))

        self.header_record = HeaderRecord(self.section_data(0))
        if self.header_record.compression not in SUPPORTED_COMPRESSION:
            raise PDBError("Unsupported PalmDOC compression type %i" % self.header_record.compression)
        if self.header_record.num_records > len(self.sections) - 1:
            raise PDBError("PalmDOC text record count exceeds available PDB sections")

    def section_data(self: _typing.Self, number: _typing.Any) -> _typing.Any:
        """
        Perform the section data operation under explicit file-format and conversion rules.

        Example:
            Exercise Reader.section data through a consuming regression::

                python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py


        :param number: Value supplied for number under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if number < 0 or number >= len(self.sections):
            raise PDBError("PalmDOC section %i is outside the PDB section table" % number)
        return self.sections[number]

    def decompress_text(self: _typing.Self, number: _typing.Any) -> _typing.Any:
        """
        Perform the decompress text operation under explicit file-format and conversion rules.

        Example:
            Exercise Reader.decompress text through a consuming regression::

                python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py


        :param number: Value supplied for number under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self.header_record.compression == 1:
            return self.section_data(number)
        if self.header_record.compression == 2 or self.header_record.compression == 258:
            from LiuXin_alpha.file_formats.compression.palmdoc import decompress_doc

            payload = self.section_data(number)
            try:
                return decompress_doc(payload)
            except Exception as err:
                raise PDBError("PalmDOC decompression failed for section %i: %s" % (number, err)) from err
        raise PDBError("Unsupported PalmDOC compression type %i" % self.header_record.compression)

    def extract_content(self: _typing.Self, output_dir: _typing.Any) -> _typing.Any:
        """
        Extract the contents of a palmdoc file.

        Example:
            Exercise Reader.extract content through a consuming regression::

                python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py


        :param output_dir: Value supplied for output dir under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        raw_txt = b""

        self.log.info("Decompressing text...")
        for i in range(1, self.header_record.num_records + 1):
            self.log.debug("\tDecompressing text section %i" % i)
            raw_txt += self.decompress_text(i)

        self.log.info("Converting text to OEB...")
        stream = io.BytesIO(raw_txt)

        from LiuXin_alpha.customize.ui import plugin_for_input_format

        txt_plugin = plugin_for_input_format("txt")
        for opt in txt_plugin.options:
            if not hasattr(self.options, opt.option.name):
                setattr(self.options, opt.option.name, opt.recommended_value)

        stream.seek(0)
        return txt_plugin.convert(stream, self.options, "txt", self.log, {})

# -*- coding: utf-8 -*-

"""
Serialize normalized content and metadata into the target format.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise writer through a consuming regression::

        python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py
"""
from __future__ import annotations

import typing as _typing

import struct

from LiuXin_alpha.file_formats.pdb.formatwriter import FormatWriter
from LiuXin_alpha.file_formats.pdb.header import PdbHeaderBuilder
from LiuXin_alpha.file_formats.txt.newlines import TxtNewlines, specified_newlines
from LiuXin_alpha.file_formats.txt.txtml import TXTMLizer

from LiuXin_alpha.utils.localization import trans as _

__license__ = "GPL v3"
__copyright__ = "2009, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"

MAX_RECORD_SIZE = 4096


class Writer(FormatWriter):
    """
    Provide the writer contract for validated ebook processing.

    Example:
        Exercise Writer through a consuming regression::

            python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py
    """
    def __init__(self: _typing.Self, opts: _typing.Any, log: _typing.Any) -> None:
        """
        Initialize and validate the writer state.

        Example:
            Exercise Writer.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py


        :param opts: Value supplied for opts under the utility contract.
        :param log: Value supplied for log under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.opts = opts
        self.log = log

    def write_content(self: _typing.Self, oeb_book: _typing.Any, out_stream: _typing.Any, metadata: _typing.Any = None) -> None:
        """
        Write PDB content out to file.

        Example:
            Exercise Writer.write content through a consuming regression::

                python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py


        :param oeb_book: Value supplied for oeb book under the utility contract.
        :param out_stream: Value supplied for out stream under the utility contract.
        :param metadata: Value supplied for metadata under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        from LiuXin_alpha.file_formats.compression.palmdoc import compress_doc

        title = (
            self.opts.title
            if hasattr(self.opts, "title") and self.opts.title
            else oeb_book.metadata.title[0].value
            if oeb_book.metadata.title != []
            else _("Unknown")
        )

        txt_records, txt_length = self._generate_text(oeb_book)
        header_record = self._header_record(txt_length, len(txt_records))

        section_lengths = [len(header_record)]
        self.log.info("Compessing data...")
        for i in range(0, len(txt_records)):
            self.log.debug("\tCompressing record %i" % i)
            txt_records[i] = compress_doc(txt_records[i])
            section_lengths.append(len(txt_records[i]))

        out_stream.seek(0)
        hb = PdbHeaderBuilder("TEXtREAd", title)
        hb.build_header(section_lengths, out_stream)

        for record in [header_record] + txt_records:
            out_stream.write(record)

    def _generate_text(self: _typing.Self, oeb_book: _typing.Any) -> tuple[_typing.Any, ...]:
        """
        Perform the generate text operation under explicit file-format and conversion rules.

        Example:
            Exercise Writer. generate text through a consuming regression::

                python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py


        :param oeb_book: Value supplied for oeb book under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        writer = TXTMLizer(self.log)
        txt = writer.extract_content(oeb_book, self.opts)

        self.log.debug("\tReplacing newlines with selected type...")
        txt = specified_newlines(TxtNewlines("windows").newline, txt).encode(self.opts.pdb_output_encoding, "replace")

        txt_length = len(txt)

        txt_records = []
        for i in range(0, (len(txt) // MAX_RECORD_SIZE) + 1):
            txt_records.append(txt[i * MAX_RECORD_SIZE : (i * MAX_RECORD_SIZE) + MAX_RECORD_SIZE])

        return txt_records, txt_length

    def _header_record(self: _typing.Self, txt_length: _typing.Any, record_count: _typing.Any) -> _typing.Any:
        """
        Perform the header record operation under explicit file-format and conversion rules.

        Example:
            Exercise Writer. header record through a consuming regression::

                python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py


        :param txt_length: Value supplied for txt length under the utility contract.
        :param record_count: Value supplied for record count under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        record = b""

        record += struct.pack(">H", 2)  # [0:2],   PalmDoc compression. (1 = No compression).
        record += struct.pack(">H", 0)  # [2:4],   Always 0.
        record += struct.pack(">L", txt_length)  # [4:8],   Uncompressed length of the entire text of the book.
        record += struct.pack(">H", record_count)  # [8:10],  Number of PDB records used for the text of the book.
        record += struct.pack(">H", MAX_RECORD_SIZE)  # [10-12], Maximum size of each record containing text, always
        #                                                          4096.
        record += struct.pack(">L", 0)  # [12-16], Current reading position, as an offset into the
        #                                                          uncompressed text.

        return record

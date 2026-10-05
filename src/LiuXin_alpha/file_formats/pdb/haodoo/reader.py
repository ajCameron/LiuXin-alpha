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

import os
import struct
import typing as _typing

from LiuXin_alpha.file_formats.pdb import PDBError
from LiuXin_alpha.file_formats.pdb.formatreader import FormatReader
from LiuXin_alpha.file_formats.txt.processor import HTML_TEMPLATE, opf_writer
from LiuXin_alpha.metadata.containers.calibre_like_book_metadata import (
    CalibreLikeLiuXinBookMetaData as MetaInformation,
)
from LiuXin_alpha.utils.calibre import prepare_string_for_xml

__license__ = "GPL v3"
__copyright__ = "2012, Kan-Ru Chen <kanru@kanru.info>"
__docformat__ = "restructuredtext en"

BPDB_IDENT = b"BOOKMTIT"
UPDB_IDENT = b"BOOKMTIU"

punct_table = {
    "︵": "（",
    "︶": "）",
    "︷": "｛",
    "︸": "｝",
    "︹": "〔",
    "︺": "〕",
    "︻": "【",
    "︼": "】",
    "︗": "〖",
    "︘": "〗",
    "﹇": "［］",
    "﹈": "［］",
    "︽": "《",
    "︾": "》",
    "︿": "〈",
    "﹀": "〉",
    "﹁": "「",
    "﹂": "」",
    "﹃": "『",
    "﹄": "』",
    "｜": "—",
    "︙": "…",
    "ⸯ": "～",
    "│": "…",
    "￤": "…",
    "　": "  ",
}


def fix_punct(line: _typing.Any) -> _typing.Any:
    """
    Perform the fix punct operation under explicit file-format and conversion rules.

    Example:
        Exercise fix punct through a consuming regression::

            python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py


    :param line: Value supplied for line under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    for (key, value) in punct_table.items():
        line = line.replace(key, value)
    return line


def _decode_text(raw: _typing.Any, encoding: _typing.Any, errors: str = "replace") -> _typing.Any:
    """
    Perform the decode text operation under explicit file-format and conversion rules.

    Example:
        Exercise  decode text through a consuming regression::

            python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py


    :param raw: Value supplied for raw under the utility contract.
    :param encoding: Value supplied for encoding under the utility contract.
    :param errors: Value supplied for errors under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return fix_punct(raw.decode(encoding, errors).rstrip("\x00"))


def _parse_record_count(raw: _typing.Any) -> _typing.Any:
    """
    Parse record count under the format's safety and compatibility rules.

    Example:
        Exercise  parse record count through a consuming regression::

            python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py


    :param raw: Value supplied for raw under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    normalized = raw.replace(b"\x00", b"").strip()
    try:
        count = int(normalized)
    except Exception as err:
        raise PDBError("Haodoo header has invalid record count") from err
    if count < 0:
        raise PDBError("Haodoo header has invalid record count")
    return count


def _validate_header_fields(fields: _typing.Any) -> None:
    """
    Validate header fields under the format's safety and compatibility rules.

    Example:
        Exercise  validate header fields through a consuming regression::

            python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py


    :param fields: Value supplied for fields under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    if len(fields) < 3:
        raise PDBError("Haodoo header is missing required fields")


def _validate_chapter_titles(num_records: _typing.Any, chapter_titles: _typing.Any) -> None:
    """
    Validate chapter titles under the format's safety and compatibility rules.

    Example:
        Exercise  validate chapter titles through a consuming regression::

            python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py


    :param num_records: Value supplied for num records under the utility contract.
    :param chapter_titles: Value supplied for chapter titles under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    if len(chapter_titles) != num_records:
        raise PDBError(
            "Haodoo chapter title count does not match record count: %d != %d"
            % (len(chapter_titles), num_records)
        )


class LegacyHeaderRecord(object):
    """
    Provide the legacyheaderrecord contract for validated ebook processing.

    Example:
        Exercise LegacyHeaderRecord through a consuming regression::

            python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py
    """
    def __init__(self: _typing.Self, raw: _typing.Any) -> None:
        """
        Initialize and validate the legacyheaderrecord state.

        Example:
            Exercise LegacyHeaderRecord.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py


        :param raw: Value supplied for raw under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        fields = raw.lstrip().replace(b"\x1b\x1b\x1b", b"\x1b").split(b"\x1b")
        _validate_header_fields(fields)
        self.title = _decode_text(fields[0], "cp950")
        self.num_records = _parse_record_count(fields[1])
        self.chapter_titles = [_decode_text(field, "cp950") for field in fields[2:]]
        _validate_chapter_titles(self.num_records, self.chapter_titles)


class UnicodeHeaderRecord(object):
    """
    Provide the unicodeheaderrecord contract for validated ebook processing.

    Example:
        Exercise UnicodeHeaderRecord through a consuming regression::

            python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py
    """
    def __init__(self: _typing.Self, raw: _typing.Any) -> None:
        """
        Initialize and validate the unicodeheaderrecord state.

        Example:
            Exercise UnicodeHeaderRecord.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py


        :param raw: Value supplied for raw under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        fields = (
            raw.lstrip()
            .replace(b"\x1b\x00\x1b\x00\x1b\x00", b"\x1b\x00")
            .split(b"\x1b\x00")
        )
        _validate_header_fields(fields)
        self.title = _decode_text(fields[0], "utf_16_le", "ignore")
        self.num_records = _parse_record_count(fields[1])
        chapter_blob = b"\x1b\x00".join(fields[2:])
        chapter_fields = (
            []
            if self.num_records == 0 and not chapter_blob
            else chapter_blob.split(b"\r\x00\n\x00")
        )
        self.chapter_titles = [
            _decode_text(field, "utf_16_le") for field in chapter_fields
        ]
        _validate_chapter_titles(self.num_records, self.chapter_titles)


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

        self.sections = []
        for i in range(header.num_sections):
            self.sections.append(header.section_data(i))

        ident = (
            header.ident.encode("ascii", "ignore")
            if isinstance(header.ident, str)
            else header.ident
        )
        if ident == BPDB_IDENT:
            self.header_record = LegacyHeaderRecord(self.section_data(0))
            self.encoding = "cp950"
        elif ident == UPDB_IDENT:
            self.header_record = UnicodeHeaderRecord(self.section_data(0))
            self.encoding = "utf_16_le"
        else:
            raise PDBError("Unsupported Haodoo identity: %s" % header.ident)

        available_chapter_records = max(len(self.sections) - 1, 0)
        if self.header_record.num_records > available_chapter_records:
            raise PDBError(
                "Haodoo declares %d chapter records but only %d are available"
                % (self.header_record.num_records, available_chapter_records)
            )

    def author(self: _typing.Self) -> _typing.Any:
        """
        Perform the author operation under explicit file-format and conversion rules.

        Example:
            Exercise Reader.author through a consuming regression::

                python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        self.stream.seek(35)
        version = struct.unpack(b">b", self.stream.read(1))[0]
        if version == 2:
            self.stream.seek(0)
            author = self.stream.read(35).rstrip(b"\x00").decode(self.encoding, "replace")
            return author
        else:
            return "Unknown"

    def get_metadata(self: _typing.Self) -> _typing.Any:
        """
        Return normalized metadata parsed from the supplied document.

        Example:
            Exercise Reader.get metadata through a consuming regression::

                python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        mi = MetaInformation(self.header_record.title, [self.author()])
        mi.language = "zh-tw"

        return mi

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
        if not (0 <= number < len(self.sections)):
            raise PDBError("Haodoo section number out of range: %s" % number)
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
        return self.section_data(number).decode(self.encoding, "replace").rstrip("\x00")

    def extract_content(self: _typing.Self, output_dir: _typing.Any) -> _typing.Any:
        """
        Extract content under the format's safety and compatibility rules.

        Example:
            Exercise Reader.extract content through a consuming regression::

                python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py


        :param output_dir: Value supplied for output dir under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        txt = ""

        self.log.info("Decompressing text...")
        for i in range(1, self.header_record.num_records + 1):
            self.log.debug("\tDecompressing text section %i" % i)
            title = self.header_record.chapter_titles[i - 1]
            lines = []
            title_added = False
            for line in self.decompress_text(i).splitlines():
                line = fix_punct(line)
                line = line.strip()
                if not title_added and title in line:
                    line = '<h1 class="chapter">' + line + "</h1>\n"
                    title_added = True
                else:
                    line = prepare_string_for_xml(line)
                lines.append("<p>%s</p>" % line)
            if not title_added:
                lines.insert(0, '<h1 class="chapter">' + title + "</h1>\n")
            txt += "\n".join(lines)

        self.log.info("Converting text to OEB...")
        html = HTML_TEMPLATE % (self.header_record.title, txt)
        with open(os.path.join(output_dir, "index.html"), "wb") as index:
            index.write(html.encode("utf-8"))

        mi = self.get_metadata()
        manifest = [("index.html", None)]
        spine = ["index.html"]
        opf_writer(output_dir, "metadata.opf", manifest, spine, mi)

        return os.path.join(output_dir, "metadata.opf")

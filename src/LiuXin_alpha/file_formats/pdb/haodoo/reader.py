# -*- coding: utf-8 -*-

"""
Read content from Haodoo.net pdb file.
"""

import struct
import os

from LiuXin_alpha.file_formats.pdb import PDBError
from LiuXin_alpha.file_formats.pdb.formatreader import FormatReader
from LiuXin_alpha.file_formats.txt.processor import opf_writer, HTML_TEMPLATE

from LiuXin_alpha.metadata.metadata import MetaData as MetaInformation

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


def fix_punct(line):
    for (key, value) in punct_table.items():
        line = line.replace(key, value)
    return line


def _decode_text(raw, encoding, errors="replace"):
    return fix_punct(raw.decode(encoding, errors).rstrip("\x00"))


def _parse_record_count(raw):
    normalized = raw.replace(b"\x00", b"").strip()
    try:
        count = int(normalized)
    except Exception as err:
        raise PDBError("Haodoo header has invalid record count") from err
    if count < 0:
        raise PDBError("Haodoo header has invalid record count")
    return count


def _validate_header_fields(fields):
    if len(fields) < 3:
        raise PDBError("Haodoo header is missing required fields")


def _validate_chapter_titles(num_records, chapter_titles):
    if len(chapter_titles) != num_records:
        raise PDBError(
            "Haodoo chapter title count does not match record count: %d != %d"
            % (len(chapter_titles), num_records)
        )


class LegacyHeaderRecord(object):
    def __init__(self, raw):
        fields = raw.lstrip().replace(b"\x1b\x1b\x1b", b"\x1b").split(b"\x1b")
        _validate_header_fields(fields)
        self.title = _decode_text(fields[0], "cp950")
        self.num_records = _parse_record_count(fields[1])
        self.chapter_titles = [_decode_text(field, "cp950") for field in fields[2:]]
        _validate_chapter_titles(self.num_records, self.chapter_titles)


class UnicodeHeaderRecord(object):
    def __init__(self, raw):
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
    def __init__(self, header, stream, log, options):
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

    def author(self):
        self.stream.seek(35)
        version = struct.unpack(b">b", self.stream.read(1))[0]
        if version == 2:
            self.stream.seek(0)
            author = self.stream.read(35).rstrip(b"\x00").decode(self.encoding, "replace")
            return author
        else:
            return "Unknown"

    def get_metadata(self):
        mi = MetaInformation(self.header_record.title, [self.author()])
        mi.language = "zh-tw"

        return mi

    def section_data(self, number):
        if not (0 <= number < len(self.sections)):
            raise PDBError("Haodoo section number out of range: %s" % number)
        return self.sections[number]

    def decompress_text(self, number):
        return self.section_data(number).decode(self.encoding, "replace").rstrip("\x00")

    def extract_content(self, output_dir):
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

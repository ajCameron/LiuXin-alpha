# -*- coding: utf-8 -*-

"""
Read content from ztxt pdb file.
"""
from __future__ import annotations

import typing as _typing

import io
import struct
import zlib

from LiuXin_alpha.file_formats.pdb.formatreader import FormatReader
from LiuXin_alpha.file_formats.pdb.ztxt import zTXTError

__license__ = "GPL v3"
__copyright__ = "2009, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"

SUPPORTED_VERSION = (1, 40)
ZTXT_HEADER_RECORD_SIZE = 32


def _require_bytes(raw: _typing.Any, size: _typing.Any, context: _typing.Any) -> None:
    if len(raw) < size:
        raise zTXTError("Truncated zTXT %s" % context)


class HeaderRecord(object):
    """
    The first record in the file is always the header record. It holds information related to the location of text,
    images, and so on in the file. This is used in conjunction with the sections defined in the file header.
    """

    def __init__(self: _typing.Self, raw: _typing.Any) -> None:
        _require_bytes(raw, ZTXT_HEADER_RECORD_SIZE, "header record")
        (self.version,) = struct.unpack(">H", raw[0:2])
        (self.num_records,) = struct.unpack(">H", raw[2:4])
        (self.size,) = struct.unpack(">L", raw[4:8])
        (self.record_size,) = struct.unpack(">H", raw[8:10])
        (self.flags,) = struct.unpack(">B", raw[18:19])


class Reader(FormatReader):
    def __init__(self: _typing.Self, header: _typing.Any, stream: _typing.Any, log: _typing.Any, options: _typing.Any) -> None:
        self.stream = stream
        self.log = log
        self.options = options

        self.sections = []
        for i in range(header.num_sections):
            self.sections.append(header.section_data(i))

        self.header_record = HeaderRecord(self.section_data(0))
        if self.header_record.num_records > len(self.sections) - 1:
            raise zTXTError("zTXT text record count exceeds available PDB sections")

        vmajor = (self.header_record.version & 0x0000FF00) >> 8
        vminor = self.header_record.version & 0x000000FF
        if vmajor < 1 or (vmajor == 1 and vminor < 40):
            raise zTXTError(
                "Unsupported ztxt version (%i.%i). Only versions newer than %i.%i are supported."
                % (vmajor, vminor, SUPPORTED_VERSION[0], SUPPORTED_VERSION[1])
            )

        if (self.header_record.flags & 0x01) == 0:
            raise zTXTError("Only compression method 1 (random access) is supported")

        self.log.debug("Foud ztxt version: %i.%i" % (vmajor, vminor))

        # Initalize the decompressor
        self.uncompressor = zlib.decompressobj()
        try:
            self.uncompressor.decompress(self.section_data(1))
        except zlib.error as err:
            raise zTXTError("zTXT decompression failed for section 1: %s" % err) from err

    def section_data(self: _typing.Self, number: _typing.Any) -> _typing.Any:
        if number < 0 or number >= len(self.sections):
            raise zTXTError("zTXT section %i is outside the PDB section table" % number)
        return self.sections[number]

    def decompress_text(self: _typing.Self, number: _typing.Any) -> _typing.Any:
        """
        Decompress the text from a particular section.
        :param number:
        :return:
        """
        if number == 1:
            self.uncompressor = zlib.decompressobj()
        try:
            return self.uncompressor.decompress(self.section_data(number))
        except zlib.error as err:
            raise zTXTError("zTXT decompression failed for section %i: %s" % (number, err)) from err

    def extract_content(self: _typing.Self, output_dir: _typing.Any) -> _typing.Any:
        """
        Extract the entire file.
        :param output_dir: All files ccontained in this archive will be dumped here.
        :return:
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

# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import struct
import zlib
from urllib.parse import unquote as urlunquote

from LiuXin_alpha.file_formats.opf.opf2 import OPFCreator
from LiuXin_alpha.file_formats.rb import HEADER, RocketBookError
from LiuXin_alpha.metadata.file_sources.rb import get_metadata
from LiuXin_alpha.utils.calibre import CurrentDir
from LiuXin_alpha.utils.libraries.liuxin_six import memory_range

__license__ = "GPL 3"
__copyright__ = "2009, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"


class RBToc(list):
    class Item(object):
        def __init__(self, name="", size=0, offset=0, flags=0):
            self.name = name
            self.size = size
            self.offset = offset
            self.flags = flags


class Reader(object):
    def __init__(self, stream, log, encoding=None):
        """
        Setup a reader to read from a file.

        :param stream: rb stream to read from
        :param log: log instance to write data to
        :param encoding: assume this encoding of the source text
        """
        self.stream = stream
        self.log = log
        self.encoding = encoding

        self.verify_file()
        self.mi = get_metadata(self.stream)
        self.toc = self.get_toc()

    def read_i32(self):
        return struct.unpack("<I", self.stream.read(4))[0]

    def verify_file(self):
        """
        Check that the size recorded in the file header matches the actual file size.
        """
        self.stream.seek(0)
        if self.stream.read(14) != HEADER:
            stream_name = getattr(self.stream, "name", "<stream>")
            raise RocketBookError(
                "Could not read file: %s. Does not contain a valid RocketBook Header." % stream_name
            )

        self.stream.seek(28)
        size = self.read_i32()
        self.stream.seek(0, os.SEEK_END)
        real_size = self.stream.tell()
        if size != real_size:
            raise RocketBookError(
                "File is corrupt. The file size recorded in the header does not match the actual file size."
            )

    def get_toc(self):
        """
        Read and return the file's table of contents.
        """
        self.stream.seek(24)
        toc_offset = self.read_i32()

        self.stream.seek(toc_offset)
        pages = self.read_i32()

        toc = RBToc()
        for _ in range(pages):
            name = self._read_toc_name()
            size, offset, flags = self.read_i32(), self.read_i32(), self.read_i32()
            toc.append(RBToc.Item(name=name, size=size, offset=offset, flags=flags))

        return toc

    def _read_toc_name(self) -> str:
        raw = self.stream.read(32).rstrip(b"\x00")
        try:
            decoded = raw.decode("utf-8")
        except Exception:
            decoded = raw.decode("cp1252", "replace")
        return urlunquote(decoded)

    def _item_output_path(self, output_dir: str, item_name: str) -> str:
        return os.path.join(output_dir, os.path.basename(item_name))

    def get_text(self, toc_item, output_dir):
        """
        Return the text content of a toc_item.
        """
        if toc_item.flags in (1, 2):
            return

        output = ""
        self.stream.seek(toc_item.offset)
        encoding = "cp1252" if self.encoding is None else self.encoding

        if toc_item.flags == 8:
            count = self.read_i32()
            self.read_i32()  # Uncompressed size of this section
            chunk_sizes = []
            for _ in memory_range(count):
                chunk_sizes.append(self.read_i32())

            for size in chunk_sizes:
                compressed_chunk = self.stream.read(size)
                output += zlib.decompress(compressed_chunk).decode(encoding, "replace")
        else:
            output += self.stream.read(toc_item.size).decode(encoding, "replace")

        with open(self._item_output_path(output_dir, toc_item.name), "wb") as html:
            html.write(output.replace("<TITLE>", "<TITLE> ").encode("utf-8"))

    def get_image(self, toc_item, output_dir):
        if toc_item.flags != 0:
            return

        self.stream.seek(toc_item.offset)
        data = self.stream.read(toc_item.size)

        with open(self._item_output_path(output_dir, toc_item.name), "wb") as img:
            img.write(data)

    def extract_content(self, output_dir):
        self.log.debug("Extracting content from file...")
        os.makedirs(output_dir, exist_ok=True)
        html = []
        images = []

        for item in self.toc:
            name = item.name.lower()
            if name.endswith("html"):
                self.log.debug("HTML item %s found..." % item.name)
                html.append(os.path.basename(item.name))
                self.get_text(item, output_dir)
            if name.endswith("png"):
                self.log.debug("PNG item %s found..." % item.name)
                images.append(os.path.basename(item.name))
                self.get_image(item, output_dir)

        return self.create_opf(output_dir, html, images)

    def create_opf(self, output_dir, pages, images):
        with CurrentDir(output_dir):
            opf = OPFCreator(output_dir, self.mi)
            manifest = []
            for page in pages + images:
                manifest.append((page, None))

            opf.create_manifest(manifest)
            opf.create_spine(pages)
            with open("metadata.opf", "wb") as opffile:
                opf.render(opffile)

        return os.path.join(output_dir, "metadata.opf")

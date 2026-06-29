# -*- coding: utf-8 -*-

"""
Read content from ereader pdb file with a 116 and 202 byte header created by Makebook.
"""

import os
import struct

from LiuXin_alpha.file_formats.opf.opf2 import OPFCreator
from LiuXin_alpha.file_formats.pdb.ereader import EreaderError, image_name
from LiuXin_alpha.file_formats.pdb.formatreader import FormatReader

from LiuXin_alpha.utils.calibre import CurrentDir

__license__ = "GPL v3"
__copyright__ = "2009, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"

HEADER_RECORD_SIZES = (116, 202)
IMAGE_RECORD_HEADER_SIZE = 62


class HeaderRecord(object):
    """
    The first record in the file is always the header record. It holds
    information related to the location of text, images, and so on
    in the file. This is used in conjunction with the sections
    defined in the file header.
    """

    def __init__(self, raw):
        if len(raw) not in HEADER_RECORD_SIZES:
            raise EreaderError("Size mismatch. eReader header record size %s KB is not supported." % len(raw))
        (self.version,) = struct.unpack(">H", raw[0:2])
        (self.non_text_offset,) = struct.unpack(">H", raw[8:10])

        self.num_text_pages = self.non_text_offset - 1


class Reader202(FormatReader):
    def __init__(self, header, stream, log, options):
        self.log = log
        self.encoding = options.input_encoding

        self.log.debug("202 byte header version found.")

        self.sections = []
        for i in range(header.num_sections):
            self.sections.append(header.section_data(i))

        self.header_record = HeaderRecord(self.section_data(0))

        if self.header_record.version not in (2, 4):
            raise EreaderError("Unknown book version %i." % self.header_record.version)
        self._validate_header_ranges()

        from LiuXin_alpha.metadata.file_sources.pdb import get_metadata

        self.mi = get_metadata(stream, False)

    def _validate_header_ranges(self):
        if self.header_record.non_text_offset < 1 or self.header_record.non_text_offset > len(self.sections):
            raise EreaderError("eReader text range exceeds available sections")

    def _text_encoding(self):
        return "cp1252" if self.encoding is None else self.encoding

    def section_data(self, number):
        if number < 0 or number >= len(self.sections):
            raise EreaderError("eReader section %i is outside the PDB section table" % number)
        return self.sections[number]

    def decompress_text(self, number):
        from LiuXin_alpha.file_formats.compression.palmdoc import decompress_doc

        payload = self.section_data(number)
        if isinstance(payload, str):
            payload = payload.encode("latin-1", "replace")
        try:
            raw = bytes(byte ^ 0xA5 for byte in payload)
            return decompress_doc(raw).decode(self._text_encoding(), "replace")
        except Exception as err:
            raise EreaderError("eReader text decompression failed for section %i: %s" % (number, err)) from err

    def get_image(self, number):
        name = None
        img = None

        data = self.section_data(number)
        if data.startswith(b"PNG"):
            if len(data) < IMAGE_RECORD_HEADER_SIZE:
                raise EreaderError("Truncated eReader image record")
            name = image_name(data[4 : 4 + 32]).strip("\x00")
            img = data[62:]

        return name, img

    def get_text_page(self, number):
        """
        Only palmdoc compression is supported. The text is xored with 0xA5 and
        assumed to be encoded as Windows-1252. The encoding is part of
        the eReader file spec and should always be this encoding.
        :param number:
        :return:
        """
        if number not in range(1, self.header_record.num_text_pages + 1):
            return ""

        return self.decompress_text(number)

    def extract_content(self, output_dir):
        from LiuXin_alpha.file_formats.pml.pmlconverter import pml_to_html

        output_dir = os.path.abspath(output_dir)

        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        pml = ""
        for i in range(1, self.header_record.num_text_pages + 1):
            self.log.debug("Extracting text page %i" % i)
            pml += self.get_text_page(i)

        title = self.mi.title
        if not isinstance(title, unicode):
            title = title.decode("utf-8", "replace")

        html = "<html><head><title>%s</title></head><body>%s</body></html>" % (
            title,
            pml_to_html(pml),
        )

        with CurrentDir(output_dir):
            with open("index.html", "wb") as index:
                self.log.debug("Writing text to index.html")
                index.write(html.encode("utf-8"))

        if not os.path.exists(os.path.join(output_dir, "images/")):
            os.makedirs(os.path.join(output_dir, "images/"))
        images = []
        with CurrentDir(os.path.join(output_dir, "images/")):
            for i in range(self.header_record.non_text_offset, len(self.sections)):
                name, img = self.get_image(i)
                if name:
                    images.append(name)
                    with open(name, "wb") as imgf:
                        self.log.debug("Writing image %s to images/" % name)
                        imgf.write(img)

        opf_path = self.create_opf(output_dir, images)

        return opf_path

    def create_opf(self, output_dir, images):
        with CurrentDir(output_dir):
            opf = OPFCreator(output_dir, self.mi)

            manifest = [("index.html", None)]

            for i in images:
                manifest.append((os.path.join("images/", i), None))

            opf.create_manifest(manifest)
            opf.create_spine(["index.html"])
            with open("metadata.opf", "wb") as opffile:
                opf.render(opffile)

        return os.path.join(output_dir, "metadata.opf")

    def dump_pml(self):
        """
        This is primarily used for debugging and 3rd party tools to
        get the plm markup that comprises the text in the file.
        """
        pml = ""

        for i in range(1, self.header_record.num_text_pages + 1):
            pml += self.get_text_page(i)

        return pml

    def dump_images(self, output_dir):
        """
        This is primarily used for debugging and 3rd party tools to
        get the images in the file.
        """
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        with CurrentDir(output_dir):
            for i in range(self.header_record.non_text_offset, len(self.sections)):
                name, img = self.get_image(i)
                if name:
                    with open(name, "wb") as imgf:
                        imgf.write(img)

# -*- coding: utf-8 -*-

from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

import typing as _typing

import os
import struct
import zlib
from collections import OrderedDict

from LiuXin_alpha.file_formats.compression.palmdoc import decompress_doc
from LiuXin_alpha.file_formats.pdb.plucker import PluckerError
from LiuXin_alpha.file_formats.pdb.formatreader import FormatReader

from LiuXin_alpha.utils.calibre import CurrentDir
from LiuXin_alpha.utils.libraries.liuxin_six import memory_range
from LiuXin_alpha.utils.libraries.liuxin_six import six_unichar
from LiuXin_alpha.utils.ptempfiles import TemporaryFile

try:
    from LiuXin_alpha.utils.wrappers.magick import Image, create_canvas
except Exception:
    try:
        from PIL import Image as _PILImage
    except Exception:
        _PILImage = None

    class Image(object):
        def __init__(self: _typing.Self) -> None:
            self._img = None
            self._quality = 75

        def read(self: _typing.Self, path: _typing.Any) -> None:
            if _PILImage is None:
                raise RuntimeError("No image backend is available.")
            self._img = _PILImage.open(path).convert("RGB")

        @property
        def size(self: _typing.Self) -> _typing.Any:
            if self._img is None:
                return 0, 0
            return self._img.size

        def set_compression_quality(self: _typing.Self, quality: _typing.Any) -> None:
            self._quality = int(quality)

        def save(self: _typing.Self, path: _typing.Any) -> None:
            if self._img is None:
                raise RuntimeError("No image loaded.")
            self._img.save(path, format="JPEG", quality=self._quality)

    class _Canvas(object):
        def __init__(self: _typing.Self, width: _typing.Any, height: _typing.Any) -> None:
            if _PILImage is None:
                raise RuntimeError("No image backend is available.")
            self._img = _PILImage.new("RGB", (int(width), int(height)), "white")
            self._quality = 75

        def compose(self: _typing.Self, image: _typing.Any, x_off: _typing.Any, y_off: _typing.Any) -> None:
            if getattr(image, "_img", None) is None:
                raise RuntimeError("No image loaded.")
            self._img.paste(image._img, (int(x_off), int(y_off)))

        def set_compression_quality(self: _typing.Self, quality: _typing.Any) -> None:
            self._quality = int(quality)

        def save(self: _typing.Self, path: _typing.Any) -> None:
            self._img.save(path, format="JPEG", quality=self._quality)

    def create_canvas(width: _typing.Any, height: _typing.Any) -> _typing.Any:
        return _Canvas(width, height)

__license__ = "GPL v3"
__copyright__ = "20011, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"


DATATYPE_PHTML = 0
DATATYPE_PHTML_COMPRESSED = 1
DATATYPE_TBMP = 2
DATATYPE_TBMP_COMPRESSED = 3
DATATYPE_MAILTO = 4
DATATYPE_LINK_INDEX = 5
DATATYPE_LINKS = 6
DATATYPE_LINKS_COMPRESSED = 7
DATATYPE_BOOKMARKS = 8
DATATYPE_CATEGORY = 9
DATATYPE_METADATA = 10
DATATYPE_STYLE_SHEET = 11
DATATYPE_FONT_PAGE = 12
DATATYPE_TABLE = 13
DATATYPE_TABLE_COMPRESSED = 14
DATATYPE_COMPOSITE_IMAGE = 15
DATATYPE_PAGELIST_METADATA = 16
DATATYPE_SORTED_URL_INDEX = 17
DATATYPE_SORTED_URL = 18
DATATYPE_SORTED_URL_COMPRESSED = 19
DATATYPE_EXT_ANCHOR_INDEX = 20
DATATYPE_EXT_ANCHOR = 21
DATATYPE_EXT_ANCHOR_COMPRESSED = 22

HEADER_RECORD_SIZE = 6
SECTION_HEADER_SIZE = 8
COMPOSITE_IMAGE_HEADER_SIZE = 4


def _as_bytes(raw: _typing.Any) -> _typing.Any:
    if isinstance(raw, bytes):
        return raw
    if isinstance(raw, bytearray):
        return bytes(raw)
    if isinstance(raw, str):
        return raw.encode("latin-1", "replace")
    return bytes(raw)


def _require_bytes(raw: _typing.Any, size: _typing.Any, context: _typing.Any) -> None:
    if len(raw) < size:
        raise PluckerError("Truncated Plucker %s" % context)


def _require_slice(raw: _typing.Any, offset: _typing.Any, size: _typing.Any, context: _typing.Any) -> None:
    if offset < 0 or size < 0 or offset + size > len(raw):
        raise PluckerError("Truncated Plucker %s" % context)


def _u16(raw: _typing.Any, offset: _typing.Any, context: _typing.Any) -> _typing.Any:
    _require_slice(raw, offset, 2, context)
    return struct.unpack(">H", raw[offset : offset + 2])[0]


def _u32(raw: _typing.Any, offset: _typing.Any, context: _typing.Any) -> _typing.Any:
    _require_slice(raw, offset, 4, context)
    return struct.unpack(">I", raw[offset : offset + 4])[0]


def _byte(raw: _typing.Any, offset: _typing.Any, context: _typing.Any) -> _typing.Any:
    _require_slice(raw, offset, 1, context)
    value = raw[offset]
    return value if isinstance(value, int) else ord(value)

# IETF IANA MIBenum value for the character set.
# See the http://www.iana.org/assignments/character-sets for valid values.
# Not all character sets are handled by Python. This is a small subset that
# the MIBenum maps to Python standard encodings
# from http://docs.python.org/library/codecs.html#standard-encodings
MIBNUM_TO_NAME = {
    3: "ascii",
    4: "latin_1",
    5: "iso8859_2",
    6: "iso8859_3",
    7: "iso8859_4",
    8: "iso8859_5",
    9: "iso8859_6",
    10: "iso8859_7",
    11: "iso8859_8",
    12: "iso8859_9",
    13: "iso8859_10",
    17: "shift_jis",
    18: "euc_jp",
    27: "utf_7",
    36: "euc_kr",
    37: "iso2022_kr",
    38: "euc_kr",
    39: "iso2022_jp",
    40: "iso2022_jp_2",
    106: "utf-8",
    109: "iso8859_13",
    110: "iso8859_14",
    111: "iso8859_15",
    112: "iso8859_16",
    1013: "utf_16_be",
    1014: "utf_16_le",
    1015: "utf_16",
    2009: "cp850",
    2010: "cp852",
    2011: "cp437",
    2013: "cp862",
    2025: "gb2312",
    2026: "big5",
    2028: "cp037",
    2043: "cp424",
    2044: "cp500",
    2046: "cp855",
    2047: "cp857",
    2048: "cp860",
    2049: "cp861",
    2050: "cp863",
    2051: "cp864",
    2052: "cp865",
    2054: "cp869",
    2063: "cp1026",
    2085: "hz",
    2086: "cp866",
    2087: "cp775",
    2089: "cp858",
    2091: "cp1140",
    2102: "big5hkscs",
    2250: "cp1250",
    2251: "cp1251",
    2252: "cp1252",
    2253: "cp1253",
    2254: "cp1254",
    2255: "cp1255",
    2256: "cp1256",
    2257: "cp1257",
    2258: "cp1258",
}


class HeaderRecord(object):
    """
    Plucker header. PDB record 0.
    """

    def __init__(self: _typing.Self, raw: _typing.Any) -> None:
        raw = _as_bytes(raw)
        _require_bytes(raw, HEADER_RECORD_SIZE, "record 0")

        self.uid = _u16(raw, 0, "record 0 uid")
        # This is labled version in the spec.
        # 2 is ZLIB compressed,
        # 1 is DOC compressed
        self.compression = _u16(raw, 2, "record 0 compression")
        self.records = _u16(raw, 4, "record 0 record count")
        _require_bytes(raw, HEADER_RECORD_SIZE + (4 * self.records), "record 0 reserved table")

        # uid of the first html file. This should link to other files which in turn may link to others.
        self.home_html = None

        self.reserved = {}
        for i in memory_range(self.records):
            adv = 4 * i
            name = _u16(raw, 6 + adv, "record 0 reserved entry name")
            local_id = _u16(raw, 8 + adv, "record 0 reserved entry id")
            self.reserved[local_id] = name
            if name == 0:
                self.home_html = local_id


class SectionHeader(object):
    """
    Every sections (record) has this header. It gives
    details about the section such as it's uid.
    """

    def __init__(self: _typing.Self, raw: _typing.Any) -> None:
        raw = _as_bytes(raw)
        _require_bytes(raw, SECTION_HEADER_SIZE, "section header")
        self.uid = _u16(raw, 0, "section uid")
        self.paragraphs = _u16(raw, 2, "section paragraph count")
        self.size = _u16(raw, 4, "section declared size")
        self.type = _byte(raw, 6, "section type")
        self.flags = _byte(raw, 7, "section flags")


class SectionHeaderText(object):
    """
    Sub header for text records.
    """

    def __init__(self: _typing.Self, section_header: _typing.Any, raw: _typing.Any) -> None:
        raw = _as_bytes(raw)
        # The uncompressed size of each paragraph.
        self.sizes = []
        # uncompressed offset of each paragraph starting
        # at the beginning of the PHTML.
        self.paragraph_offsets = []
        # Paragraph attributes.
        self.attributes = []

        table_size = section_header.paragraphs * 4
        _require_bytes(raw, table_size, "text paragraph table")

        for i in memory_range(section_header.paragraphs):
            adv = 4 * i
            self.sizes.append(_u16(raw, adv, "text paragraph size"))
            self.attributes.append(_u16(raw, 2 + adv, "text paragraph attributes"))

        running_offset = 0
        for size in self.sizes:
            running_offset += size
            self.paragraph_offsets.append(running_offset)

        if section_header.type == DATATYPE_PHTML and running_offset > len(raw[table_size:]):
            raise PluckerError("Plucker paragraph table exceeds PHTML payload")


class SectionMetadata(object):
    """
    Metadata.

    This does not store metadata such as title, or author.
    That metadata would be best retrieved with the PDB (plucker)
    metdata reader.

    This stores document specific information such as the
    text encoding.

    Note: There is a default encoding but each text section
    can be assigned a different encoding.
    """

    def __init__(self: _typing.Self, raw: _typing.Any) -> None:
        raw = _as_bytes(raw)
        self.default_encoding = "latin-1"
        self.exceptional_uid_encodings = {}
        self.owner_id = None

        _require_bytes(raw, 2, "metadata record count")
        record_count = _u16(raw, 0, "metadata record count")

        adv = 0
        for i in memory_range(record_count):
            record_start = 2 + adv
            _require_slice(raw, record_start, 4, "metadata record header")
            record_type = _u16(raw, record_start, "metadata record type")
            length = _u16(raw, record_start + 2, "metadata record length")
            record_size = 2 * length
            if record_size < 4:
                raise PluckerError("Invalid Plucker metadata record length")
            _require_slice(raw, record_start, record_size, "metadata record")
            payload_start = record_start + 4
            payload_length = record_size - 4

            # CharSet
            if record_type == 1:
                if payload_length < 2:
                    raise PluckerError("Truncated Plucker metadata charset record")
                val = _u16(raw, payload_start, "metadata charset record")
                self.default_encoding = MIBNUM_TO_NAME.get(val, "latin-1")
            # ExceptionalCharSets
            elif record_type == 2:
                if payload_length % 4:
                    raise PluckerError("Invalid Plucker exceptional charset record length")
                for ii_adv in memory_range(0, payload_length, 4):
                    uid = _u16(raw, payload_start + ii_adv, "metadata exceptional charset uid")
                    mib = _u16(raw, payload_start + ii_adv + 2, "metadata exceptional charset mib")
                    self.exceptional_uid_encodings[uid] = MIBNUM_TO_NAME.get(mib, "latin-1")
            # OwnerID
            elif record_type == 3:
                if payload_length < 4:
                    raise PluckerError("Truncated Plucker owner id metadata record")
                self.owner_id = _u32(raw, payload_start, "metadata owner id")
            # Author, Title, PubDate
            # Ignored here. The metadata reader plugin
            # will get this info because if it's missing
            # the metadata reader plugin will use fall
            # back data from elsewhere in the file.
            elif record_type in (4, 5, 6):
                pass
            # Linked Documents
            elif record_type == 7:
                pass

            adv += record_size


class SectionText(object):
    """
    Text data. Stores a text section header and the PHTML.
    """

    def __init__(self: _typing.Self, section_header: _typing.Any, raw: _typing.Any) -> None:
        raw = _as_bytes(raw)
        self.header = SectionHeaderText(section_header, raw)
        self.data = raw[section_header.paragraphs * 4 :]


class SectionCompositeImage(object):
    """
    A composite image consists of a a 2D array of rows and columns. The entries in the array are uid's.
    """

    def __init__(self: _typing.Self, raw: _typing.Any) -> None:
        raw = _as_bytes(raw)
        _require_bytes(raw, COMPOSITE_IMAGE_HEADER_SIZE, "composite image header")
        self.columns = _u16(raw, 0, "composite image columns")
        self.rows = _u16(raw, 2, "composite image rows")
        if self.columns < 1 or self.rows < 1:
            raise PluckerError("Invalid Plucker composite image dimensions")
        _require_bytes(raw, COMPOSITE_IMAGE_HEADER_SIZE + (self.columns * self.rows * 2), "composite image layout")

        # [
        #  [uid, uid, uid, ...],
        #  [uid, uid, uid, ...],
        #  ...
        # ]
        #
        # Each item in the layout is in it's
        # correct position in the final
        # composite.
        #
        # Each item in the layout is a uid
        # to an image record.
        self.layout = []
        offset = 4
        for i in memory_range(self.rows):
            col = []
            for j in memory_range(self.columns):
                col.append(_u16(raw, offset, "composite image reference"))
                offset += 2
            self.layout.append(col)


class Reader(FormatReader):
    """
    Convert a plucker archive into HTML.

    TODO:
          * UTF 16 and 32 characters.
          * Margins.
          * Alignment.
          * Font color.
          * DATATYPE_MAILTO
          * DATATYPE_TABLE(_COMPRESSED)
          * DATATYPE_EXT_ANCHOR_INDEX
          * DATATYPE_EXT_ANCHOR(_COMPRESSED)
    """

    def __init__(self: _typing.Self, header: _typing.Any, stream: _typing.Any, log: _typing.Any, options: _typing.Any) -> None:
        self.stream = stream
        self.log = log
        self.options = options

        # Mapping of section uid to our internal
        # list of sections.
        self.uid_section_number = OrderedDict()
        self.uid_text_secion_number = OrderedDict()
        self.uid_text_secion_encoding = {}
        self.uid_image_section_number = {}
        self.uid_composite_image_section_number = {}
        self.metadata_section_number = None
        self.default_encoding = "latin-1"
        self.owner_id = None
        self.sections = []

        # The Plucker record0 header
        self.header_record = HeaderRecord(header.section_data(0))
        if self.header_record.compression not in (1, 2):
            raise PluckerError("Unsupported Plucker compression type %i" % self.header_record.compression)

        for i in range(1, header.num_sections):
            section_number = len(self.sections)
            # The length of the section header.
            # Where the actual data in the section starts.
            start = 8
            section = None

            raw_data = header.section_data(i)
            # Every sections has a section header.
            section_header = SectionHeader(raw_data)
            section_payload = raw_data[start:]
            if section_header.size > len(section_payload):
                raise PluckerError("Plucker section declared size exceeds available record data")
            if section_header.size:
                section_payload = section_payload[: section_header.size]

            # Store sections we care able.
            if section_header.type in (DATATYPE_PHTML, DATATYPE_PHTML_COMPRESSED):
                self.uid_text_secion_number[section_header.uid] = section_number
                section = SectionText(section_header, section_payload)
            elif section_header.type in (DATATYPE_TBMP, DATATYPE_TBMP_COMPRESSED):
                self.uid_image_section_number[section_header.uid] = section_number
                section = section_payload
            elif section_header.type == DATATYPE_METADATA:
                self.metadata_section_number = section_number
                section = SectionMetadata(section_payload)
            elif section_header.type == DATATYPE_COMPOSITE_IMAGE:
                self.uid_composite_image_section_number[section_header.uid] = section_number
                section = SectionCompositeImage(section_payload)

            # Store the section.
            if section is not None:
                self.uid_section_number[section_header.uid] = section_number
                self.sections.append((section_header, section))

        self._validate_composite_image_references()

        # Store useful information from the metadata section locally
        # to make access easier.
        if self.metadata_section_number is not None:
            mdata_section = self.sections[self.metadata_section_number][1]
            for k, v in mdata_section.exceptional_uid_encodings.items():
                self.uid_text_secion_encoding[k] = v
            self.default_encoding = mdata_section.default_encoding
            self.owner_id = mdata_section.owner_id

        # Get the metadata (tile, author, ...) with the metadata reader.
        from LiuXin_alpha.metadata.file_sources.pdb import get_metadata

        self.mi = get_metadata(stream, False)

    def _validate_composite_image_references(self: _typing.Self) -> None:
        for composite_uid, num in self.uid_composite_image_section_number.items():
            _section_header, section_data = self.sections[num]
            for row in section_data.layout:
                for image_uid in row:
                    if image_uid not in self.uid_image_section_number:
                        raise PluckerError(
                            "Plucker composite image %s references missing image uid %s" % (composite_uid, image_uid)
                        )

    def extract_content(self: _typing.Self, output_dir: _typing.Any) -> _typing.Any:
        # Each text record is independent (unless the continuation
        # value is set in the previous record). Put each converted
        # text recored into a separate file. We will reference the
        # home.html file as the first file and let the HTML input
        # plugin assemble the order based on hyperlinks.
        with CurrentDir(output_dir):
            for uid, num in self.uid_text_secion_number.items():
                self.log.debug("Writing record with uid: %s as %s.html" % (uid, uid))
                with open("%s.html" % uid, "wb") as htmlf:
                    html = "<html><body>"
                    section_header, section_data = self.sections[num]
                    if section_header.type == DATATYPE_PHTML:
                        html += self.process_phtml(section_data.data, section_data.header.paragraph_offsets)
                    elif section_header.type == DATATYPE_PHTML_COMPRESSED:
                        d = self.decompress_phtml(section_data.data)
                        html += self.process_phtml(d, section_data.header.paragraph_offsets)
                    html += "</body></html>"
                    htmlf.write(html.encode("utf-8"))

        # Images.
        # Cache the image sizes in case they are used by a composite image.
        image_sizes = {}
        if not os.path.exists(os.path.join(output_dir, "images/")):
            os.makedirs(os.path.join(output_dir, "images/"))
        with CurrentDir(os.path.join(output_dir, "images/")):
            # Single images.
            for uid, num in self.uid_image_section_number.items():
                section_header, section_data = self.sections[num]
                if section_data:
                    idata = None
                    if section_header.type == DATATYPE_TBMP:
                        idata = section_data
                    elif section_header.type == DATATYPE_TBMP_COMPRESSED:
                        if self.header_record.compression == 1:
                            idata = decompress_doc(section_data)
                        elif self.header_record.compression == 2:
                            idata = zlib.decompress(section_data)
                    try:
                        with TemporaryFile(suffix=".palm") as itn:
                            with open(itn, "wb") as itf:
                                itf.write(idata)
                            im = Image()
                            im.read(itn)
                            image_sizes[uid] = im.size
                            im.set_compression_quality(70)
                            im.save("%s.jpg" % uid)
                            self.log.debug("Wrote image with uid %s to images/%s.jpg" % (uid, uid))
                    except Exception as e:
                        self.log.error("Failed to write image with uid %s: %s" % (uid, e))
                else:
                    self.log.error("Failed to write image with uid %s: No data." % uid)
            # Composite images.
            # We're going to use the already compressed .jpg images here.
            for uid, num in self.uid_composite_image_section_number.items():
                try:
                    section_header, section_data = self.sections[num]
                    # Get the final width and height.
                    width = 0
                    height = 0
                    for row in section_data.layout:
                        row_width = 0
                        col_height = 0
                        for col in row:
                            if col not in image_sizes:
                                raise Exception("Image with uid: %s missing." % col)
                            im = Image()
                            im.read("%s.jpg" % col)
                            w, h = im.size
                            row_width += w
                            if col_height < h:
                                col_height = h
                        if width < row_width:
                            width = row_width
                        height += col_height
                    # Create a new image the total size of all image
                    # parts. Put the parts into the new image.
                    canvas = create_canvas(width, height)
                    y_off = 0
                    for row in section_data.layout:
                        x_off = 0
                        largest_height = 0
                        for col in row:
                            im = Image()
                            im.read("%s.jpg" % col)
                            canvas.compose(im, x_off, y_off)
                            w, h = im.size
                            x_off += w
                            if largest_height < h:
                                largest_height = h
                        y_off += largest_height
                    canvas.set_compression_quality(70)
                    canvas.save("%s.jpg" % uid)
                    self.log.debug("Wrote composite image with uid %s to images/%s.jpg" % (uid, uid))
                except Exception as e:
                    self.log.error("Failed to write composite image with uid %s: %s" % (uid, e))

        # Run the HTML through the html processing plugin.
        from LiuXin_alpha.customize.ui import plugin_for_input_format

        html_input = plugin_for_input_format("html")
        for opt in html_input.options:
            setattr(self.options, opt.option.name, opt.recommended_value)
        self.options.input_encoding = "utf-8"
        odi = self.options.debug_pipeline
        self.options.debug_pipeline = None
        # Determine the home.html record uid. This should be set in the
        # reserved values in the metadata recored. home.html is the first
        # text record (should have hyper link references to other records)
        # in the document.
        try:
            home_html = self.header_record.home_html
            if not home_html:
                home_html = next(iter(self.uid_text_secion_number))
        except:
            raise Exception("Could not determine home.html")
        # Generate oeb from html conversion.
        oeb = html_input.convert(open("%s.html" % home_html, "rb"), self.options, "html", self.log, {})
        self.options.debug_pipeline = odi

        return oeb

    def decompress_phtml(self: _typing.Self, data: _typing.Any) -> _typing.Any:
        try:
            if self.header_record.compression == 2:
                if self.owner_id:
                    raise PluckerError("Encrypted Plucker PHTML is not supported")
                return zlib.decompress(data)
            elif self.header_record.compression == 1:
                return decompress_doc(data)
        except PluckerError:
            raise
        except Exception as err:
            raise PluckerError("Plucker PHTML decompression failed: %s" % err) from err
        raise PluckerError("Unsupported Plucker compression type %i" % self.header_record.compression)

    def _validate_phtml_image_uid(self: _typing.Self, uid: _typing.Any) -> None:
        if uid not in self.uid_image_section_number and uid not in self.uid_composite_image_section_number:
            raise PluckerError("Plucker PHTML references missing image uid %s" % uid)

    def process_phtml(self: _typing.Self, d: _typing.Any, paragraph_offsets: _typing.Any = None) -> _typing.Any:
        d = _as_bytes(d)

        if paragraph_offsets is None:
            paragraph_offsets = []

        html = '<p id="p0">'
        offset = 0
        paragraph_open = True
        link_open = False
        need_set_p_id = False
        p_num = 1
        font_specifier_close = ""

        while offset < len(d):
            if not paragraph_open:
                if need_set_p_id:
                    html += '<p id="p%s">' % p_num
                    p_num += 1
                    need_set_p_id = False
                else:
                    html += "<p>"
                paragraph_open = True

            c = _byte(d, offset, "PHTML byte")
            # PHTML "functions"
            if c == 0x0:
                offset += 1
                c = _byte(d, offset, "PHTML opcode")
                # Page link begins
                # 2 Bytes
                # record ID
                if c == 0x0A:
                    offset += 1
                    local_id = _u16(d, offset, "PHTML page link record id")
                    if local_id in self.uid_text_secion_number:
                        html += '<a href="%s.html">' % local_id
                        link_open = True
                    offset += 1
                # Targeted page link begins
                # 3 Bytes
                # record ID, target
                elif c == 0x0B:
                    _require_slice(d, offset + 1, 3, "PHTML targeted page link")
                    offset += 3
                # Paragraph link begins
                # 4 Bytes
                # record ID, paragraph number
                elif c == 0x0C:
                    offset += 1
                    local_id = _u16(d, offset, "PHTML paragraph link record id")
                    offset += 2
                    pid = _u16(d, offset, "PHTML paragraph link target")
                    if local_id in self.uid_text_secion_number:
                        html += '<a href="%s.html#p%s">' % (local_id, pid)
                        link_open = True
                    offset += 1
                # Targeted paragraph link begins
                # 5 Bytes
                # record ID, paragraph number, target
                elif c == 0x0D:
                    _require_slice(d, offset + 1, 5, "PHTML targeted paragraph link")
                    offset += 5
                # Link ends
                # 0 Bytes
                elif c == 0x08:
                    if link_open:
                        html += "</a>"
                        link_open = False
                # Set font
                # 1 Bytes
                # font specifier
                elif c == 0x11:
                    offset += 1
                    specifier = _byte(d, offset, "PHTML font specifier")
                    html += font_specifier_close
                    # Regular text
                    if specifier == 0:
                        font_specifier_close = ""
                    # h1
                    elif specifier == 1:
                        html += "<h1>"
                        font_specifier_close = "</h1>"
                    # h2
                    elif specifier == 2:
                        html += "<h2>"
                        font_specifier_close = "</h2>"
                    # h3
                    elif specifier == 3:
                        html += "<h13>"
                        font_specifier_close = "</h3>"
                    # h4
                    elif specifier == 4:
                        html += "<h4>"
                        font_specifier_close = "</h4>"
                    # h5
                    elif specifier == 5:
                        html += "<h5>"
                        font_specifier_close = "</h5>"
                    # h6
                    elif specifier == 6:
                        html += "<h6>"
                        font_specifier_close = "</h6>"
                    # Bold
                    elif specifier == 7:
                        html += "<b>"
                        font_specifier_close = "</b>"
                    # Fixed-width
                    elif specifier == 8:
                        html += "<tt>"
                        font_specifier_close = "</tt>"
                    # Small
                    elif specifier == 9:
                        html += "<small>"
                        font_specifier_close = "</small>"
                    # Subscript
                    elif specifier == 10:
                        html += "<sub>"
                        font_specifier_close = "</sub>"
                    # Superscript
                    elif specifier == 11:
                        html += "<sup>"
                        font_specifier_close = "</sup>"
                # Embedded image
                # 2 Bytes
                # image record ID
                elif c == 0x1A:
                    offset += 1
                    uid = _u16(d, offset, "PHTML embedded image id")
                    self._validate_phtml_image_uid(uid)
                    html += '<img src="images/%s.jpg" />' % uid
                    offset += 1
                # Set margin
                # 2 Bytes
                # left margin, right margin
                elif c == 0x22:
                    _require_slice(d, offset + 1, 2, "PHTML margin")
                    offset += 2
                # Alignment of text
                # 1 Bytes
                # alignment
                elif c == 0x29:
                    _require_slice(d, offset + 1, 1, "PHTML alignment")
                    offset += 1
                # Horizontal rule
                # 3 Bytes
                # 8-bit height, 8-bit width (pixels), 8-bit width (%, 1-100)
                elif c == 0x33:
                    _require_slice(d, offset + 1, 3, "PHTML horizontal rule")
                    offset += 3
                    if paragraph_open:
                        html += "</p>"
                        paragraph_open = False
                    html += "<hr />"
                # New line
                # 0 Bytes
                elif c == 0x38:
                    if paragraph_open:
                        html += "</p>\n"
                        paragraph_open = False
                # Italic text begins
                # 0 Bytes
                elif c == 0x40:
                    html += "<i>"
                # Italic text ends
                # 0 Bytes
                elif c == 0x48:
                    html += "</i>"
                # Set text color
                # 3 Bytes
                # 8-bit red, 8-bit green, 8-bit blue
                elif c == 0x53:
                    _require_slice(d, offset + 1, 3, "PHTML text color")
                    offset += 3
                # Multiple embedded image
                # 4 Bytes
                # alternate image record ID, image record ID
                elif c == 0x5C:
                    _require_slice(d, offset + 1, 4, "PHTML multiple embedded image")
                    offset += 3
                    uid = _u16(d, offset, "PHTML multiple embedded image id")
                    self._validate_phtml_image_uid(uid)
                    html += '<img src="images/%s.jpg" />' % uid
                    offset += 1
                # Underline text begins
                # 0 Bytes
                elif c == 0x60:
                    html += "<u>"
                # Underline text ends
                # 0 Bytes
                elif c == 0x68:
                    html += "</u>"
                # Strike-through text begins
                # 0 Bytes
                elif c == 0x70:
                    html += "<s>"
                # Strike-through text ends
                # 0 Bytes
                elif c == 0x78:
                    html += "</s>"
                # 16-bit Unicode character
                # 3 Bytes
                # alternate text length, 16-bit unicode character
                elif c == 0x83:
                    _require_slice(d, offset + 1, 3, "PHTML 16-bit unicode character")
                    offset += 3
                # 32-bit Unicode character
                # 5 Bytes
                # alternate text length, 32-bit unicode character
                elif c == 0x85:
                    _require_slice(d, offset + 1, 5, "PHTML 32-bit unicode character")
                    offset += 5
                # Begin custom font span
                # 6 Bytes
                # font page record ID, X page position, Y page position
                elif c == 0x8E:
                    _require_slice(d, offset + 1, 6, "PHTML custom font span")
                    offset += 6
                # Adjust custom font glyph position
                # 4 Bytes
                # X page position, Y page position
                elif c == 0x8C:
                    _require_slice(d, offset + 1, 4, "PHTML custom font glyph position")
                    offset += 4
                # Change font page
                # 2 Bytes
                # font record ID
                elif c == 0x8A:
                    _require_slice(d, offset + 1, 2, "PHTML font page change")
                    offset += 2
                # End custom font span
                # 0 Bytes
                elif c == 0x88:
                    pass
                # Begin new table row
                # 0 Bytes
                elif c == 0x90:
                    pass
                # Insert table (or table link)
                # 2 Bytes
                # table record ID
                elif c == 0x92:
                    _require_slice(d, offset + 1, 2, "PHTML table reference")
                    offset += 2
                # Table cell data
                # 7 Bytes
                # 8-bit alignment, 16-bit image record ID, 8-bit columns, 8-bit rows, 16-bit text length
                elif c == 0x97:
                    _require_slice(d, offset + 1, 7, "PHTML table cell")
                    offset += 7
                # Exact link modifier
                # 2 Bytes
                # Paragraph Offset (The Exact Link Modifier modifies a Paragraph Link or Targeted Paragraph
                # Link function to specify an exact byte offset within the paragraph.
                # This function must be followed immediately by the function it modifies).
                elif c == 0x9A:
                    _require_slice(d, offset + 1, 2, "PHTML exact link modifier")
                    offset += 2
            elif c == 0xA0:
                html += "&nbsp;"
            else:
                html += six_unichar(c)
            offset += 1
            if offset in paragraph_offsets:
                need_set_p_id = True
                if paragraph_open:
                    html += "</p>\n"
                    paragraph_open = False

        if paragraph_open:
            html += "</p>"

        return html

    def get_text_uid_encoding(self: _typing.Self, uid: _typing.Any) -> _typing.Any:
        # Return the user sepcified input encoding,
        # otherwise return the alternate encoding specified for the uid,
        # otherwise retur the default encoding for the document.
        return (
            self.options.input_encoding
            if self.options.input_encoding
            else self.uid_text_secion_encoding.get(uid, self.default_encoding)
        )

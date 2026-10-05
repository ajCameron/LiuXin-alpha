#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:fdm=marker:ai

"""
Serialize pages, resources and drawing operations into a PDF stream.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise serialize through a consuming regression::

        python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py
"""
from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

import typing as _typing

import hashlib

from LiuXin_alpha.utils.libraries.liuxin_six import long

try:
    from PyQt5.Qt import QBuffer, QByteArray, QImage, Qt, QColor, qRgba, QPainter
    _HAS_QT = True
except Exception:
    QBuffer = QByteArray = QImage = Qt = QColor = qRgba = QPainter = None
    _HAS_QT = False

from LiuXin_alpha.file_formats.pdf.render.common import (
    Reference,
    EOL,
    serialize,
    Stream,
    Dictionary,
    String,
    Name,
    Array,
    fmtnum,
)
from LiuXin_alpha.file_formats.pdf.render.fonts import FontManager
from LiuXin_alpha.file_formats.pdf.render.links import Links

from LiuXin_alpha.constants import __appname__, __version__
from LiuXin_alpha.utils.date import utcnow
from LiuXin_alpha.utils.libraries.liuxin_six import six_map
from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems

__license__ = "GPL v3"
__copyright__ = "2012, Kovid Goyal <kovid at kovidgoyal.net>"
__docformat__ = "restructuredtext en"

PDFVER = b"%PDF-1.4"  # 1.4 is needed for XMP metadata


def _require_qt() -> None:
    """
    Perform the require qt operation under explicit file-format and conversion rules.

    Example:
        Exercise  require qt through a consuming regression::

            python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    if not _HAS_QT:
        raise RuntimeError("PyQt5 is required for PDF serialization.")


class IndirectObjects(object):
    """
    Provide the indirectobjects contract for validated ebook processing.

    Example:
        Exercise IndirectObjects through a consuming regression::

            python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py
    """
    def __init__(self: _typing.Self) -> None:
        """
        Initialize and validate the indirectobjects state.

        Example:
            Exercise IndirectObjects.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :return: None; validated state is stored on the receiving object.
        """
        self._list = []
        self._map = {}
        self._offsets = []

    def __len__(self: _typing.Self) -> _typing.Any:
        """
        Perform the len operation under explicit file-format and conversion rules.

        Example:
            Exercise IndirectObjects.  len   through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return len(self._list)

    def add(self: _typing.Self, o: _typing.Any) -> _typing.Any:
        """
        Perform the add operation under explicit file-format and conversion rules.

        Example:
            Exercise IndirectObjects.add through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param o: Value supplied for o under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        self._list.append(o)
        ref = Reference(len(self._list), o)
        self._map[id(o)] = ref
        self._offsets.append(None)
        return ref

    def commit(self: _typing.Self, ref: _typing.Any, stream: _typing.Any) -> None:
        """
        Perform the commit operation under explicit file-format and conversion rules.

        Example:
            Exercise IndirectObjects.commit through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param ref: Value supplied for ref under the utility contract.
        :param stream: Input or output stream wrapped by the terminal or compatibility
            layer.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.write_obj(stream, ref.num, ref.obj)

    def write_obj(self: _typing.Self, stream: _typing.Any, num: _typing.Any, obj: _typing.Any) -> None:
        """
        Write obj under the format's safety and compatibility rules.

        Example:
            Exercise IndirectObjects.write obj through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param stream: Input or output stream wrapped by the terminal or compatibility
            layer.
        :param num: Value supplied for num under the utility contract.
        :param obj: Value supplied for obj under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        stream.write(EOL)
        self._offsets[num - 1] = stream.tell()
        stream.write("%d 0 obj" % num)
        stream.write(EOL)
        serialize(obj, stream)
        if stream.last_char != EOL:
            stream.write(EOL)
        stream.write("endobj")
        stream.write(EOL)

    def __getitem__(self: _typing.Self, o: _typing.Any) -> _typing.Any:
        """
        Perform the getitem operation under explicit file-format and conversion rules.

        Example:
            Exercise IndirectObjects.  getitem   through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param o: Value supplied for o under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            return self._map[id(self._list[o] if isinstance(o, int) else o)]
        except (KeyError, IndexError):
            raise KeyError("The object %r was not found" % o)

    def pdf_serialize(self: _typing.Self, stream: _typing.Any) -> None:
        """
        Perform the pdf serialize operation under explicit file-format and conversion rules.

        Example:
            Exercise IndirectObjects.pdf serialize through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param stream: Input or output stream wrapped by the terminal or compatibility
            layer.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for i, obj in enumerate(self._list):
            offset = self._offsets[i]
            if offset is None:
                self.write_obj(stream, i + 1, obj)

    def write_xref(self: _typing.Self, stream: _typing.Any) -> _typing.Any:
        """
        Write xref under the format's safety and compatibility rules.

        Example:
            Exercise IndirectObjects.write xref through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param stream: Input or output stream wrapped by the terminal or compatibility
            layer.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        self.xref_offset = stream.tell()
        stream.write(b"xref" + EOL)
        stream.write("0 %d" % (1 + len(self._offsets)))
        stream.write(EOL)
        stream.write("%010d 65535 f " % 0)
        stream.write(EOL)

        for offset in self._offsets:
            line = "%010d 00000 n " % offset
            stream.write(line.encode("ascii") + EOL)
        return self.xref_offset


class Page(Stream):
    """
    Provide the page contract for validated ebook processing.

    Example:
        Exercise Page through a consuming regression::

            python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py
    """
    def __init__(self: _typing.Self, parentref: _typing.Any, *args: _typing.Any, **kwargs: _typing.Any) -> None:
        """
        Initialize and validate the page state.

        Example:
            Exercise Page.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param parentref: Value supplied for parentref under the utility contract.
        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: None; validated state is stored on the receiving object.
        """
        super(Page, self).__init__(*args, **kwargs)
        self.page_dict = Dictionary(
            {
                "Type": Name("Page"),
                "Parent": parentref,
            }
        )
        self.opacities = {}
        self.fonts = {}
        self.xobjects = {}
        self.patterns = {}

    def set_opacity(self: _typing.Self, opref: _typing.Any) -> None:
        """
        Set opacity under the format's safety and compatibility rules.

        Example:
            Exercise Page.set opacity through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param opref: Value supplied for opref under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if opref not in self.opacities:
            self.opacities[opref] = "Opa%d" % len(self.opacities)
        name = self.opacities[opref]
        serialize(Name(name), self)
        self.write(b" gs ")

    def add_font(self: _typing.Self, fontref: _typing.Any) -> _typing.Any:
        """
        Perform the add font operation under explicit file-format and conversion rules.

        Example:
            Exercise Page.add font through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param fontref: Value supplied for fontref under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if fontref not in self.fonts:
            self.fonts[fontref] = "F%d" % len(self.fonts)
        return self.fonts[fontref]

    def add_image(self: _typing.Self, imgref: _typing.Any) -> _typing.Any:
        """
        Perform the add image operation under explicit file-format and conversion rules.

        Example:
            Exercise Page.add image through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param imgref: Value supplied for imgref under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if imgref not in self.xobjects:
            self.xobjects[imgref] = "Image%d" % len(self.xobjects)
        return self.xobjects[imgref]

    def add_pattern(self: _typing.Self, patternref: _typing.Any) -> _typing.Any:
        """
        Perform the add pattern operation under explicit file-format and conversion rules.

        Example:
            Exercise Page.add pattern through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param patternref: Value supplied for patternref under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if patternref not in self.patterns:
            self.patterns[patternref] = "Pat%d" % len(self.patterns)
        return self.patterns[patternref]

    def add_resources(self: _typing.Self) -> None:
        """
        Perform the add resources operation under explicit file-format and conversion rules.

        Example:
            Exercise Page.add resources through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        r = Dictionary()
        if self.opacities:
            extgs = Dictionary()
            for opref, name in iteritems(self.opacities):
                extgs[name] = opref
            r["ExtGState"] = extgs
        if self.fonts:
            fonts = Dictionary()
            for ref, name in iteritems(self.fonts):
                fonts[name] = ref
            r["Font"] = fonts
        if self.xobjects:
            xobjects = Dictionary()
            for ref, name in iteritems(self.xobjects):
                xobjects[name] = ref
            r["XObject"] = xobjects
        if self.patterns:
            r["ColorSpace"] = Dictionary({"PCSp": Array([Name("Pattern"), Name("DeviceRGB")])})
            patterns = Dictionary()
            for ref, name in iteritems(self.patterns):
                patterns[name] = ref
            r["Pattern"] = patterns
        if r:
            self.page_dict["Resources"] = r

    def end(self: _typing.Self, objects: _typing.Any, stream: _typing.Any) -> _typing.Any:
        """
        Perform the end operation under explicit file-format and conversion rules.

        Example:
            Exercise Page.end through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param objects: Value supplied for objects under the utility contract.
        :param stream: Input or output stream wrapped by the terminal or compatibility
            layer.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        contents = objects.add(self)
        objects.commit(contents, stream)
        self.page_dict["Contents"] = contents
        self.add_resources()
        ret = objects.add(self.page_dict)
        # objects.commit(ret, stream)
        return ret


class Path(object):
    """
    Provide the path contract for validated ebook processing.

    Example:
        Exercise Path through a consuming regression::

            python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py
    """
    def __init__(self: _typing.Self) -> None:
        """
        Initialize and validate the path state.

        Example:
            Exercise Path.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :return: None; validated state is stored on the receiving object.
        """
        self.ops = []

    def move_to(self: _typing.Self, x: _typing.Any, y: _typing.Any) -> None:
        """
        Perform the move to operation under explicit file-format and conversion rules.

        Example:
            Exercise Path.move to through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param x: Value supplied for x under the utility contract.
        :param y: Value supplied for y under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.ops.append((x, y, "m"))

    def line_to(self: _typing.Self, x: _typing.Any, y: _typing.Any) -> None:
        """
        Perform the line to operation under explicit file-format and conversion rules.

        Example:
            Exercise Path.line to through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param x: Value supplied for x under the utility contract.
        :param y: Value supplied for y under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.ops.append((x, y, "l"))

    def curve_to(self: _typing.Self, x1: _typing.Any, y1: _typing.Any, x2: _typing.Any, y2: _typing.Any, x: _typing.Any, y: _typing.Any) -> None:
        """
        Perform the curve to operation under explicit file-format and conversion rules.

        Example:
            Exercise Path.curve to through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param x1: Value supplied for x1 under the utility contract.
        :param y1: Value supplied for y1 under the utility contract.
        :param x2: Value supplied for x2 under the utility contract.
        :param y2: Value supplied for y2 under the utility contract.
        :param x: Value supplied for x under the utility contract.
        :param y: Value supplied for y under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.ops.append((x1, y1, x2, y2, x, y, "c"))

    def close(self: _typing.Self) -> None:
        """
        Perform the close operation under explicit file-format and conversion rules.

        Example:
            Exercise Path.close through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.ops.append(("h",))


class Catalog(Dictionary):
    """
    Provide the catalog contract for validated ebook processing.

    Example:
        Exercise Catalog through a consuming regression::

            python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py
    """
    def __init__(self: _typing.Self, pagetree: _typing.Any) -> None:
        """
        Initialize and validate the catalog state.

        Example:
            Exercise Catalog.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param pagetree: Value supplied for pagetree under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        super(Catalog, self).__init__({"Type": Name("Catalog"), "Pages": pagetree})


class PageTree(Dictionary):
    """
    Provide the pagetree contract for validated ebook processing.

    Example:
        Exercise PageTree through a consuming regression::

            python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py
    """
    def __init__(self: _typing.Self, page_size: _typing.Any) -> None:
        """
        Initialize and validate the pagetree state.

        Example:
            Exercise PageTree.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param page_size: Value supplied for page size under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        super(PageTree, self).__init__(
            {
                "Type": Name("Pages"),
                "MediaBox": Array([0, 0, page_size[0], page_size[1]]),
                "Kids": Array(),
                "Count": 0,
            }
        )

    def add_page(self: _typing.Self, pageref: _typing.Any) -> None:
        """
        Perform the add page operation under explicit file-format and conversion rules.

        Example:
            Exercise PageTree.add page through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param pageref: Value supplied for pageref under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self["Kids"].append(pageref)
        self["Count"] += 1

    def get_ref(self: _typing.Self, num: _typing.Any) -> _typing.Any:
        """
        Return ref under the format's safety and compatibility rules.

        Example:
            Exercise PageTree.get ref through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param num: Value supplied for num under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self["Kids"][num - 1]

    def get_num(self: _typing.Self, pageref: _typing.Any) -> _typing.Any:
        """
        Return num under the format's safety and compatibility rules.

        Example:
            Exercise PageTree.get num through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param pageref: Value supplied for pageref under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            return self["Kids"].index(pageref) + 1
        except ValueError:
            return -1


class HashingStream(object):
    """
    Provide the hashingstream contract for validated ebook processing.

    Example:
        Exercise HashingStream through a consuming regression::

            python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py
    """
    def __init__(self: _typing.Self, f: _typing.Any) -> None:
        """
        Initialize and validate the hashingstream state.

        Example:
            Exercise HashingStream.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param f: Value supplied for f under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.f = f
        self.tell = f.tell
        self.hashobj = hashlib.sha256()
        self.last_char = b""

    def write(self: _typing.Self, raw: _typing.Any) -> None:
        """
        Perform the write operation under explicit file-format and conversion rules.

        Example:
            Exercise HashingStream.write through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param raw: Value supplied for raw under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.write_raw(raw if isinstance(raw, bytes) else raw.encode("ascii"))

    def write_raw(self: _typing.Self, raw: _typing.Any) -> None:
        """
        Write raw under the format's safety and compatibility rules.

        Example:
            Exercise HashingStream.write raw through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param raw: Value supplied for raw under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.f.write(raw)
        self.hashobj.update(raw)
        if raw:
            self.last_char = raw[-1:]


class Image(Stream):
    """
    Provide the image contract for validated ebook processing.

    Example:
        Exercise Image through a consuming regression::

            python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py
    """
    def __init__(self: _typing.Self, data: _typing.Any, w: _typing.Any, h: _typing.Any, depth: _typing.Any, mask: _typing.Any, soft_mask: _typing.Any, dct: _typing.Any) -> None:
        """
        Initialize and validate the image state.

        Example:
            Exercise Image.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param data: Value supplied for data under the utility contract.
        :param w: Value supplied for w under the utility contract.
        :param h: Value supplied for h under the utility contract.
        :param depth: Value supplied for depth under the utility contract.
        :param mask: Value supplied for mask under the utility contract.
        :param soft_mask: Value supplied for soft mask under the utility contract.
        :param dct: Value supplied for dct under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        Stream.__init__(self)
        self.width, self.height, self.depth = w, h, depth
        self.mask, self.soft_mask = mask, soft_mask
        if dct:
            self.filters.append(Name("DCTDecode"))
        else:
            self.compress = True
        self.write(data)

    def add_extra_keys(self: _typing.Self, d: _typing.Any) -> None:
        """
        Add supported metadata keys to the PDF information dictionary.

        Example:
            Exercise Image.add extra keys through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param d: Value supplied for d under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        d["Type"] = Name("XObject")
        d["Subtype"] = Name("Image")
        d["Width"] = self.width
        d["Height"] = self.height
        if self.depth == 1:
            d["ImageMask"] = True
            d["Decode"] = Array([1, 0])
        else:
            d["BitsPerComponent"] = 8
            d["ColorSpace"] = Name("Device" + ("RGB" if self.depth == 32 else "Gray"))
        if self.mask is not None:
            d["Mask"] = self.mask
        if self.soft_mask is not None:
            d["SMask"] = self.soft_mask


class Metadata(Stream):
    """
    Provide the metadata contract for validated ebook processing.

    Example:
        Exercise Metadata through a consuming regression::

            python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py
    """
    def __init__(self: _typing.Self, mi: _typing.Any) -> None:
        """
        Initialize and validate the metadata state.

        Example:
            Exercise Metadata.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param mi: Metadata object exposed to the template function.
        :return: None; validated state is stored on the receiving object.
        """
        Stream.__init__(self)
        from LiuXin_alpha.metadata.xmp import metadata_to_xmp_packet

        self.write(metadata_to_xmp_packet(mi))

    def add_extra_keys(self: _typing.Self, d: _typing.Any) -> None:
        """
        Add supported metadata keys to the PDF information dictionary.

        Example:
            Exercise Metadata.add extra keys through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param d: Value supplied for d under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        d["Type"] = Name("Metadata")
        d["Subtype"] = Name("XML")


class PDFStream(object):

    """
    Provide the pdfstream contract for validated ebook processing.

    Example:
        Exercise PDFStream through a consuming regression::

            python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py
    """
    PATH_OPS = {
        # stroke fill   fill-rule
        (False, False, "winding"): "n",
        (False, False, "evenodd"): "n",
        (False, True, "winding"): "f",
        (False, True, "evenodd"): "f*",
        (True, False, "winding"): "S",
        (True, False, "evenodd"): "S",
        (True, True, "winding"): "B",
        (True, True, "evenodd"): "B*",
    }

    def __init__(self: _typing.Self, stream: _typing.Any, page_size: _typing.Any, compress: bool = False, mark_links: bool = False, debug: _typing.Any = print) -> None:
        """
        Initialize and validate the pdfstream state.

        Example:
            Exercise PDFStream.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param stream: Input or output stream wrapped by the terminal or compatibility
            layer.
        :param page_size: Value supplied for page size under the utility contract.
        :param compress: Value supplied for compress under the utility contract.
        :param mark_links: Value supplied for mark links under the utility contract.
        :param debug: Value supplied for debug under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.stream = HashingStream(stream)
        self.compress = compress
        self.write_line(PDFVER)
        # Todo: This is a hack - not sure the encoding is utf-8 always. Have to check.
        self.write_line('%íì¦"'.encode())
        creator = "%s %s [http://calibre-ebook.com]" % (__appname__, __version__)
        self.write_line(("%% Created by %s" % creator).encode("utf-8"))
        self.objects = IndirectObjects()
        self.objects.add(PageTree(page_size))
        self.objects.add(Catalog(self.page_tree))
        self.current_page = Page(self.page_tree, compress=self.compress)
        self.info = Dictionary(
            {
                "Creator": String(creator),
                "Producer": String(creator),
                "CreationDate": utcnow(),
            }
        )
        self.stroke_opacities, self.fill_opacities = {}, {}
        self.font_manager = FontManager(self.objects, self.compress)
        self.image_cache = {}
        self.pattern_cache, self.shader_cache = {}, {}
        self.debug = debug
        self.links = Links(self, mark_links, page_size)
        if _HAS_QT:
            i = QImage(1, 1, QImage.Format_ARGB32)
            i.fill(qRgba(0, 0, 0, 255))
            self.alpha_bit = i.constBits().asstring(4).find(b"\xff")
        else:
            # Used only by image embedding paths, which are guarded by _require_qt().
            self.alpha_bit = 3

    @property
    def page_tree(self: _typing.Self) -> _typing.Any:
        """
        Perform the page tree operation under explicit file-format and conversion rules.

        Example:
            Exercise PDFStream.page tree through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.objects[0]

    @property
    def catalog(self: _typing.Self) -> _typing.Any:
        """
        Perform the catalog operation under explicit file-format and conversion rules.

        Example:
            Exercise PDFStream.catalog through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.objects[1]

    def get_pageref(self: _typing.Self, pagenum: _typing.Any) -> _typing.Any:
        """
        Return pageref under the format's safety and compatibility rules.

        Example:
            Exercise PDFStream.get pageref through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param pagenum: Value supplied for pagenum under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.page_tree.obj.get_ref(pagenum)

    def set_metadata(self: _typing.Self, title: _typing.Any = None, author: _typing.Any = None, tags: _typing.Any = None, mi: _typing.Any = None) -> None:
        """
        Update document metadata while preserving unrelated package state.

        Example:
            Exercise PDFStream.set metadata through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param title: Value supplied for title under the utility contract.
        :param author: Value supplied for author under the utility contract.
        :param tags: Value supplied for tags under the utility contract.
        :param mi: Metadata object exposed to the template function.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if title:
            self.info["Title"] = String(title)
        if author:
            self.info["Author"] = String(author)
        if tags:
            self.info["Keywords"] = String(tags)
        if mi is not None:
            self.metadata = self.objects.add(Metadata(mi))
            self.catalog.obj["Metadata"] = self.metadata

    def write_line(self: _typing.Self, byts: bytes = b"") -> None:
        """
        Write line under the format's safety and compatibility rules.

        Example:
            Exercise PDFStream.write line through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param byts: Value supplied for byts under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        byts = byts if isinstance(byts, bytes) else byts.encode("ascii")
        self.stream.write(byts + EOL)

    def transform(self: _typing.Self, *args: _typing.Any) -> None:
        """
        Perform the transform operation under explicit file-format and conversion rules.

        Example:
            Exercise PDFStream.transform through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param args: Positional values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if len(args) == 1:
            m = args[0]
            vals = [m.m11(), m.m12(), m.m21(), m.m22(), m.dx(), m.dy()]
        else:
            vals = args
        cm = " ".join(six_map(fmtnum, vals))
        self.current_page.write_line(cm + " cm")

    def save_stack(self: _typing.Self) -> None:
        """
        Perform the save stack operation under explicit file-format and conversion rules.

        Example:
            Exercise PDFStream.save stack through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.current_page.write_line("q")

    def restore_stack(self: _typing.Self) -> None:
        """
        Perform the restore stack operation under explicit file-format and conversion rules.

        Example:
            Exercise PDFStream.restore stack through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.current_page.write_line("Q")

    def reset_stack(self: _typing.Self) -> None:
        """
        Perform the reset stack operation under explicit file-format and conversion rules.

        Example:
            Exercise PDFStream.reset stack through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.current_page.write_line("Q q")

    def draw_rect(self: _typing.Self, x: _typing.Any, y: _typing.Any, width: _typing.Any, height: _typing.Any, stroke: bool = True, fill: bool = False) -> None:
        """
        Perform the draw rect operation under explicit file-format and conversion rules.

        Example:
            Exercise PDFStream.draw rect through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param x: Value supplied for x under the utility contract.
        :param y: Value supplied for y under the utility contract.
        :param width: Value supplied for width under the utility contract.
        :param height: Value supplied for height under the utility contract.
        :param stroke: Value supplied for stroke under the utility contract.
        :param fill: Value supplied for fill under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.current_page.write("%s re " % " ".join(six_map(fmtnum, (x, y, width, height))))
        self.current_page.write_line(self.PATH_OPS[(stroke, fill, "winding")])

    def write_path(self: _typing.Self, path: _typing.Any) -> None:
        """
        Write path under the format's safety and compatibility rules.

        Example:
            Exercise PDFStream.write path through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param path: Filesystem path read, written, normalized or validated by the
            operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for i, op in enumerate(path.ops):
            if i != 0:
                self.current_page.write_line()
            for x in op:
                self.current_page.write((fmtnum(x) if isinstance(x, (int, long, float)) else x) + " ")

    def draw_path(self: _typing.Self, path: _typing.Any, stroke: bool = True, fill: bool = False, fill_rule: str = "winding") -> None:
        """
        Perform the draw path operation under explicit file-format and conversion rules.

        Example:
            Exercise PDFStream.draw path through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param path: Filesystem path read, written, normalized or validated by the
            operation.
        :param stroke: Value supplied for stroke under the utility contract.
        :param fill: Value supplied for fill under the utility contract.
        :param fill_rule: Value supplied for fill rule under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if not path.ops:
            return
        self.write_path(path)
        self.current_page.write_line(self.PATH_OPS[(stroke, fill, fill_rule)])

    def add_clip(self: _typing.Self, path: _typing.Any, fill_rule: str = "winding") -> None:
        """
        Perform the add clip operation under explicit file-format and conversion rules.

        Example:
            Exercise PDFStream.add clip through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param path: Filesystem path read, written, normalized or validated by the
            operation.
        :param fill_rule: Value supplied for fill rule under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if not path.ops:
            return
        self.write_path(path)
        op = "W" if fill_rule == "winding" else "W*"
        self.current_page.write_line(op + " " + "n")

    def serialize(self: _typing.Self, o: _typing.Any) -> None:
        """
        Perform the serialize operation under explicit file-format and conversion rules.

        Example:
            Exercise PDFStream.serialize through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param o: Value supplied for o under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        serialize(o, self.current_page)

    def set_stroke_opacity(self: _typing.Self, opacity: _typing.Any) -> None:
        """
        Set stroke opacity under the format's safety and compatibility rules.

        Example:
            Exercise PDFStream.set stroke opacity through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param opacity: Value supplied for opacity under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if opacity not in self.stroke_opacities:
            op = Dictionary({"Type": Name("ExtGState"), "CA": opacity})
            self.stroke_opacities[opacity] = self.objects.add(op)
        self.current_page.set_opacity(self.stroke_opacities[opacity])

    def set_fill_opacity(self: _typing.Self, opacity: _typing.Any) -> None:
        """
        Set fill opacity under the format's safety and compatibility rules.

        Example:
            Exercise PDFStream.set fill opacity through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param opacity: Value supplied for opacity under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        opacity = float(opacity)
        if opacity not in self.fill_opacities:
            op = Dictionary({"Type": Name("ExtGState"), "ca": opacity})
            self.fill_opacities[opacity] = self.objects.add(op)
        self.current_page.set_opacity(self.fill_opacities[opacity])

    def end_page(self: _typing.Self) -> None:
        """
        Finalize the current PDF page and attach its resources.

        Example:
            Exercise PDFStream.end page through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pageref = self.current_page.end(self.objects, self.stream)
        self.page_tree.obj.add_page(pageref)
        self.current_page = Page(self.page_tree, compress=self.compress)

    def draw_glyph_run(self: _typing.Self, transform: _typing.Any, size: _typing.Any, font_metrics: _typing.Any, glyphs: _typing.Any) -> None:
        """
        Serialize positioned glyphs using the selected embedded PDF font.

        Example:
            Exercise PDFStream.draw glyph run through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param transform: Value supplied for transform under the utility contract.
        :param size: Value supplied for size under the utility contract.
        :param font_metrics: Value supplied for font metrics under the utility contract.
        :param glyphs: Value supplied for glyphs under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        glyph_ids = {x[-1] for x in glyphs}
        fontref = self.font_manager.add_font(font_metrics, glyph_ids)
        name = self.current_page.add_font(fontref)
        self.current_page.write(b"BT ")
        serialize(Name(name), self.current_page)
        self.current_page.write(" %s Tf " % fmtnum(size))
        self.current_page.write("%s Tm " % " ".join(six_map(fmtnum, transform)))
        for x, y, glyph_id in glyphs:
            self.current_page.write_raw(("%s %s Td <%04X> Tj " % (fmtnum(x), fmtnum(y), glyph_id)).encode("ascii"))
        self.current_page.write_line(b" ET")

    def get_image(self: _typing.Self, cache_key: _typing.Any) -> _typing.Any:
        """
        Return image under the format's safety and compatibility rules.

        Example:
            Exercise PDFStream.get image through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param cache_key: Value supplied for cache key under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.image_cache.get(cache_key, None)

    def write_image(self: _typing.Self, data: _typing.Any, w: _typing.Any, h: _typing.Any, depth: _typing.Any, dct: bool = False, mask: _typing.Any = None, soft_mask: _typing.Any = None, cache_key: _typing.Any = None) -> _typing.Any:
        """
        Serialize an image resource and return its PDF object reference.

        Example:
            Exercise PDFStream.write image through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param data: Value supplied for data under the utility contract.
        :param w: Value supplied for w under the utility contract.
        :param h: Value supplied for h under the utility contract.
        :param depth: Value supplied for depth under the utility contract.
        :param dct: Value supplied for dct under the utility contract.
        :param mask: Value supplied for mask under the utility contract.
        :param soft_mask: Value supplied for soft mask under the utility contract.
        :param cache_key: Value supplied for cache key under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        imgobj = Image(data, w, h, depth, mask, soft_mask, dct)
        self.image_cache[cache_key] = r = self.objects.add(imgobj)
        self.objects.commit(r, self.stream)
        return r

    def add_image(self: _typing.Self, img: _typing.Any, cache_key: _typing.Any) -> _typing.Any:
        """
        Perform the add image operation under explicit file-format and conversion rules.

        Example:
            Exercise PDFStream.add image through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param img: Value supplied for img under the utility contract.
        :param cache_key: Value supplied for cache key under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        _require_qt()
        ref = self.get_image(cache_key)
        if ref is not None:
            return ref

        fmt = img.format()
        image = QImage(img)
        if (
            image.depth() == 1
            and img.colorTable().size() == 2
            and img.colorTable().at(0) == QColor(Qt.black).rgba()
            and img.colorTable().at(1) == QColor(Qt.white).rgba()
        ):
            if fmt == QImage.Format_MonoLSB:
                image = image.convertToFormat(QImage.Format_Mono)
            fmt = QImage.Format_Mono
        else:
            if fmt != QImage.Format_RGB32 and fmt != QImage.Format_ARGB32:
                image = image.convertToFormat(QImage.Format_ARGB32)
                fmt = QImage.Format_ARGB32

        w = image.width()
        h = image.height()
        d = image.depth()

        if fmt == QImage.Format_Mono:
            bytes_per_line = (w + 7) >> 3
            data = image.constBits().asstring(bytes_per_line * h)
            return self.write_image(data, w, h, d, cache_key=cache_key)

        has_alpha = False
        soft_mask = None

        tmask = None
        if fmt == QImage.Format_ARGB32:
            tmask = image.constBits().asstring(4 * w * h)[self.alpha_bit :: 4]
            sdata = bytearray(tmask)
            vals = set(sdata)
            vals.discard(255)  # discard opaque pixels
            has_alpha = bool(vals)
            if has_alpha:
                # Blend image onto a white background as otherwise Qt will render
                # transparent pixels as black
                background = QImage(image.size(), QImage.Format_ARGB32_Premultiplied)
                background.fill(Qt.white)
                painter = QPainter(background)
                painter.drawImage(0, 0, image)
                painter.end()
                image = background

        ba = QByteArray()
        buf = QBuffer(ba)
        image.save(buf, "jpeg", 94)
        data = bytes(ba.data())

        if has_alpha:
            soft_mask = self.write_image(tmask, w, h, 8)

        return self.write_image(data, w, h, 32, dct=True, soft_mask=soft_mask, cache_key=cache_key)

    def add_pattern(self: _typing.Self, pattern: _typing.Any) -> _typing.Any:
        """
        Perform the add pattern operation under explicit file-format and conversion rules.

        Example:
            Exercise PDFStream.add pattern through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param pattern: Value supplied for pattern under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if pattern.cache_key not in self.pattern_cache:
            self.pattern_cache[pattern.cache_key] = self.objects.add(pattern)
        return self.current_page.add_pattern(self.pattern_cache[pattern.cache_key])

    def add_shader(self: _typing.Self, shader: _typing.Any) -> _typing.Any:
        """
        Perform the add shader operation under explicit file-format and conversion rules.

        Example:
            Exercise PDFStream.add shader through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param shader: Value supplied for shader under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if shader.cache_key not in self.shader_cache:
            self.shader_cache[shader.cache_key] = self.objects.add(shader)
        return self.shader_cache[shader.cache_key]

    def draw_image(self: _typing.Self, x: _typing.Any, y: _typing.Any, width: _typing.Any, height: _typing.Any, imgref: _typing.Any) -> None:
        """
        Perform the draw image operation under explicit file-format and conversion rules.

        Example:
            Exercise PDFStream.draw image through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param x: Value supplied for x under the utility contract.
        :param y: Value supplied for y under the utility contract.
        :param width: Value supplied for width under the utility contract.
        :param height: Value supplied for height under the utility contract.
        :param imgref: Value supplied for imgref under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        name = self.current_page.add_image(imgref)
        self.current_page.write(
            "q %s 0 0 %s %s %s cm " % (fmtnum(width), fmtnum(-height), fmtnum(x), fmtnum(y + height))
        )
        serialize(Name(name), self.current_page)
        self.current_page.write_line(" Do Q")

    def apply_color_space(self: _typing.Self, color: _typing.Any, pattern: _typing.Any, stroke: bool = False) -> None:
        """
        Perform the apply color space operation under explicit file-format and conversion rules.

        Example:
            Exercise PDFStream.apply color space through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param color: Value supplied for color under the utility contract.
        :param pattern: Value supplied for pattern under the utility contract.
        :param stroke: Value supplied for stroke under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        wl = self.current_page.write_line
        if color is not None and pattern is None:
            wl(" ".join(six_map(fmtnum, color)) + (" RG" if stroke else " rg"))
        elif color is None and pattern is not None:
            wl("/Pattern %s /%s %s" % ("CS" if stroke else "cs", pattern, "SCN" if stroke else "scn"))
        elif color is not None and pattern is not None:
            col = " ".join(six_map(fmtnum, color))
            wl("/PCSp %s %s /%s %s" % ("CS" if stroke else "cs", col, pattern, "SCN" if stroke else "scn"))

    def apply_fill(self: _typing.Self, color: _typing.Any = None, pattern: _typing.Any = None, opacity: _typing.Any = None) -> None:
        """
        Perform the apply fill operation under explicit file-format and conversion rules.

        Example:
            Exercise PDFStream.apply fill through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param color: Value supplied for color under the utility contract.
        :param pattern: Value supplied for pattern under the utility contract.
        :param opacity: Value supplied for opacity under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if opacity is not None:
            self.set_fill_opacity(opacity)
        self.apply_color_space(color, pattern)

    def apply_stroke(self: _typing.Self, color: _typing.Any = None, pattern: _typing.Any = None, opacity: _typing.Any = None) -> None:
        """
        Perform the apply stroke operation under explicit file-format and conversion rules.

        Example:
            Exercise PDFStream.apply stroke through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param color: Value supplied for color under the utility contract.
        :param pattern: Value supplied for pattern under the utility contract.
        :param opacity: Value supplied for opacity under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if opacity is not None:
            self.set_stroke_opacity(opacity)
        self.apply_color_space(color, pattern, stroke=True)

    def end(self: _typing.Self) -> None:
        """
        Perform the end operation under explicit file-format and conversion rules.

        Example:
            Exercise PDFStream.end through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.current_page.getvalue():
            self.end_page()
        self.font_manager.embed_fonts(self.debug)
        inforef = self.objects.add(self.info)
        self.links.add_links()
        self.objects.pdf_serialize(self.stream)
        self.write_line()
        startxref = self.objects.write_xref(self.stream)
        file_id = String(self.stream.hashobj.hexdigest())
        self.write_line("trailer")
        trailer = Dictionary(
            {
                "Root": self.catalog,
                "Size": len(self.objects) + 1,
                "ID": Array([file_id, file_id]),
                "Info": inforef,
            }
        )
        serialize(trailer, self.stream)
        self.write_line("startxref")
        self.write_line("%d" % startxref)
        self.stream.write("%%EOF")

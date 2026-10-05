#!/usr/bin/env python2
# vim:fileencoding=utf-8
"""
Add normalized image resources, drawing markup and dimensions to DOCX output.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise images through a consuming regression::

        python -m pytest -q tests/file_formats/docx/test_docx_modernized.py
"""
from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

import typing as _typing

import os
import posixpath
from collections import namedtuple
from functools import partial

from lxml import etree

from LiuXin_alpha.file_formats.oeb.base import urlunquote
from LiuXin_alpha.file_formats.docx.images import pt_to_emu

from LiuXin_alpha.utils.storage.local.filenames import ascii_filename
from LiuXin_alpha.utils.image_tools.imghdr import identify
from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.resources import I

# Py2/Py3
from LiuXin_alpha.utils.libraries.liuxin_six import six_map
from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems
from LiuXin_alpha.utils.libraries.liuxin_six import dict_itervalues as itervalues

__license__ = "GPL v3"
__copyright__ = "2015, Kovid Goyal <kovid at kovidgoyal.net>"

Image = namedtuple("Image", "rid fname width height fmt item")


def as_num(x: _typing.Any) -> _typing.Any:
    """
    Perform the as num operation under explicit file-format and conversion rules.

    Example:
        Exercise as num through a consuming regression::

            python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


    :param x: Value supplied for x under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    try:
        return float(x)
    except Exception:
        pass
    return 0


def get_image_margins(style: _typing.Any) -> _typing.Any:
    """
    Return image margins under the format's safety and compatibility rules.

    Example:
        Exercise get image margins through a consuming regression::

            python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


    :param style: Value supplied for style under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    ans = {}
    for edge in "Left Right Top Bottom".split():
        val = as_num(getattr(style, "padding" + edge)) + as_num(getattr(style, "margin" + edge))
        ans["dist" + edge[0]] = str(pt_to_emu(val))
    return ans


class ImagesManager(object):
    """
    Provide the imagesmanager contract for validated ebook processing.

    Example:
        Exercise ImagesManager through a consuming regression::

            python -m pytest -q tests/file_formats/docx/test_docx_modernized.py
    """
    def __init__(self: _typing.Self, oeb: _typing.Any, document_relationships: _typing.Any) -> None:
        """
        Initialize and validate the imagesmanager state.

        Example:
            Exercise ImagesManager.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param oeb: Value supplied for oeb under the utility contract.
        :param document_relationships: Value supplied for document relationships under the
            utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.oeb, self.log = oeb, oeb.log
        self.images = {}
        self.seen_filenames = set()
        self.document_relationships = document_relationships
        self.count = 0

    def read_image(self: _typing.Self, href: _typing.Any) -> _typing.Any:
        """
        Read image under the format's safety and compatibility rules.

        Example:
            Exercise ImagesManager.read image through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param href: Value supplied for href under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if href not in self.images:
            item = self.oeb.manifest.hrefs.get(href)
            if item is None or not isinstance(item.data, bytes):
                return
            try:
                fmt, width, height = identify(item.data)
            except Exception:
                self.log.warning("Replacing corrupted image with blank: %s" % href)
                item.data = I("blank.png", data=True, allow_user_override=False)
                fmt, width, height = identify(item.data)
            image_fname = "media/" + self.create_filename(href, fmt)
            image_rid = self.document_relationships.add_image(image_fname)
            self.images[href] = Image(image_rid, image_fname, width, height, fmt, item)
            item.unload_data_from_memory()
        return self.images[href]

    def add_image(self: _typing.Self, img: _typing.Any, block: _typing.Any, stylizer: _typing.Any, bookmark: _typing.Any = None, as_block: bool = False) -> _typing.Any:
        """
        Perform the add image operation under explicit file-format and conversion rules.

        Example:
            Exercise ImagesManager.add image through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param img: Value supplied for img under the utility contract.
        :param block: Value supplied for block under the utility contract.
        :param stylizer: Value supplied for stylizer under the utility contract.
        :param bookmark: Value supplied for bookmark under the utility contract.
        :param as_block: Value supplied for as block under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        src = img.get("src")
        if not src:
            return
        href = self.abshref(src)
        try:
            rid = self.read_image(href).rid
        except AttributeError:
            return
        drawing = self.create_image_markup(img, stylizer, href, as_block=as_block)
        block.add_image(drawing, bookmark=bookmark)
        return rid

    def create_image_markup(self: _typing.Self, html_img: _typing.Any, stylizer: _typing.Any, href: _typing.Any, as_block: bool = False) -> _typing.Any:
        # TODO: img inside a link (clickable image)
        """
        Create image markup under the format's safety and compatibility rules.

        Example:
            Exercise ImagesManager.create image markup through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param html_img: Value supplied for html img under the utility contract.
        :param stylizer: Value supplied for stylizer under the utility contract.
        :param href: Value supplied for href under the utility contract.
        :param as_block: Value supplied for as block under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        style = stylizer.style(html_img)
        floating = style["float"]
        if floating not in {"left", "right"}:
            floating = None
        if as_block:
            ml, mr = style._get("margin-left"), style._get("margin-right")
            if ml == "auto":
                floating = "center" if mr == "auto" else "right"
            if mr == "auto":
                floating = "center" if ml == "auto" else "right"
        else:
            parent = html_img.getparent()
            if len(parent) == 1 and not (parent.text or "").strip() and not (html_img.tail or "").strip():
                # We have an inline image alone inside a block
                pstyle = stylizer.style(parent)
                if pstyle["text-align"] in ("center", "right") and "block" in pstyle["display"]:
                    floating = pstyle["text-align"]
        fake_margins = floating is None
        self.count += 1
        img = self.images[href]
        name = urlunquote(posixpath.basename(href))
        width, height = six_map(pt_to_emu, style.img_size(img.width, img.height))

        makeelement, namespaces = (
            self.document_relationships.namespace.makeelement,
            self.document_relationships.namespace.namespaces,
        )

        root = etree.Element("root", nsmap=namespaces)
        ans = makeelement(root, "w:drawing", append=False)
        if floating is None:
            parent = makeelement(ans, "wp:inline")
        else:
            parent = makeelement(ans, "wp:anchor", **get_image_margins(style))
            # The next three lines are boilerplate that Word requires, even
            # though the DOCX specs define defaults for all of them
            parent.set("simplePos", "0"), parent.set("relativeHeight", "1"), parent.set("behindDoc", "0"), parent.set(
                "locked", "0"
            )
            parent.set("layoutInCell", "1"), parent.set("allowOverlap", "1")
            makeelement(parent, "wp:simplePos", x="0", y="0")
            makeelement(makeelement(parent, "wp:positionH", relativeFrom="margin"), "wp:align").text = floating
            makeelement(makeelement(parent, "wp:positionV", relativeFrom="line"), "wp:align").text = "top"
        makeelement(parent, "wp:extent", cx=str(width), cy=str(height))
        if fake_margins:
            # DOCX does not support setting margins for inline images, so we
            # fake it by using effect extents to simulate margins
            makeelement(parent, "wp:effectExtent", **{k[-1].lower(): v for k, v in iteritems(get_image_margins(style))})
        else:
            makeelement(parent, "wp:effectExtent", l="0", r="0", t="0", b="0")
        if floating is not None:
            # The idiotic Word requires this to be after the extent settings
            if as_block:
                makeelement(parent, "wp:wrapTopAndBottom")
            else:
                makeelement(parent, "wp:wrapSquare", wrapText="bothSides")
        self.create_docx_image_markup(parent, name, html_img.get("alt") or name, img.rid, width, height)
        return ans

    def create_docx_image_markup(self: _typing.Self, parent: _typing.Any, name: _typing.Any, alt: _typing.Any, img_rid: _typing.Any, width: _typing.Any, height: _typing.Any) -> None:
        """
        Create docx image markup under the format's safety and compatibility rules.

        Example:
            Exercise ImagesManager.create docx image markup through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param parent: Value supplied for parent under the utility contract.
        :param name: Field, file, function or resource name addressed by the operation.
        :param alt: Value supplied for alt under the utility contract.
        :param img_rid: Value supplied for img rid under the utility contract.
        :param width: Value supplied for width under the utility contract.
        :param height: Value supplied for height under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        makeelement, namespaces = (
            self.document_relationships.namespace.makeelement,
            self.document_relationships.namespace.namespaces,
        )
        makeelement(parent, "wp:docPr", id=str(self.count), name=name, descr=alt)
        makeelement(
            makeelement(parent, "wp:cNvGraphicFramePr"),
            "a:graphicFrameLocks",
            noChangeAspect="1",
        )
        g = makeelement(parent, "a:graphic")
        gd = makeelement(g, "a:graphicData", uri=namespaces["pic"])
        pic = makeelement(gd, "pic:pic")
        nv_pic_pr = makeelement(pic, "pic:nvPicPr")
        makeelement(nv_pic_pr, "pic:cNvPr", id="0", name=name, descr=alt)
        makeelement(nv_pic_pr, "pic:cNvPicPr")
        bf = makeelement(pic, "pic:blipFill")
        makeelement(bf, "a:blip", r_embed=img_rid)
        makeelement(makeelement(bf, "a:stretch"), "a:fillRect")
        sp_pr = makeelement(pic, "pic:spPr")
        xfrm = makeelement(sp_pr, "a:xfrm")
        makeelement(xfrm, "a:off", x="0", y="0"), makeelement(xfrm, "a:ext", cx=str(width), cy=str(height))
        makeelement(makeelement(sp_pr, "a:prstGeom", prst="rect"), "a:avLst")

    def create_filename(self: _typing.Self, href: _typing.Any, fmt: _typing.Any) -> _typing.Any:
        """
        Create filename under the format's safety and compatibility rules.

        Example:
            Exercise ImagesManager.create filename through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param href: Value supplied for href under the utility contract.
        :param fmt: Date, number or template format specification.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        fname = ascii_filename(urlunquote(posixpath.basename(href)))
        fname = posixpath.splitext(fname)[0]
        fname = fname[:75].rstrip(".") or "image"
        num = 0
        base = fname
        while fname.lower() in self.seen_filenames:
            num += 1
            fname = base + str(num)
        self.seen_filenames.add(fname.lower())
        fname += os.extsep + fmt.lower()
        return fname

    def serialize(self: _typing.Self, images_map: _typing.Any) -> None:
        """
        Perform the serialize operation under explicit file-format and conversion rules.

        Example:
            Exercise ImagesManager.serialize through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param images_map: Value supplied for images map under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for img in itervalues(self.images):
            images_map["word/" + img.fname] = partial(self.get_data, img.item)

    def get_data(self: _typing.Self, item: _typing.Any) -> _typing.Any:
        """
        Return data under the format's safety and compatibility rules.

        Example:
            Exercise ImagesManager.get data through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param item: Value supplied for item under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            return item.data
        finally:
            item.unload_data_from_memory(False)

    def create_cover_markup(self: _typing.Self, img: _typing.Any, width: _typing.Any, height: _typing.Any) -> _typing.Any:
        """
        Create cover markup under the format's safety and compatibility rules.

        Example:
            Exercise ImagesManager.create cover markup through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param img: Value supplied for img under the utility contract.
        :param width: Value supplied for width under the utility contract.
        :param height: Value supplied for height under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        self.count += 1
        makeelement, namespaces = (
            self.document_relationships.namespace.makeelement,
            self.document_relationships.namespace.namespaces,
        )

        root = etree.Element("root", nsmap=namespaces)
        ans = makeelement(root, "w:drawing", append=False)
        parent = makeelement(ans, "wp:anchor", **{"dist" + edge: "0" for edge in "LRTB"})
        parent.set("simplePos", "0"), parent.set("relativeHeight", "1"), parent.set("behindDoc", "0"), parent.set(
            "locked", "0"
        )
        parent.set("layoutInCell", "1"), parent.set("allowOverlap", "1")
        makeelement(parent, "wp:simplePos", x="0", y="0")
        makeelement(makeelement(parent, "wp:positionH", relativeFrom="page"), "wp:align").text = "center"
        makeelement(makeelement(parent, "wp:positionV", relativeFrom="page"), "wp:align").text = "center"
        width, height = six_map(pt_to_emu, (width, height))
        makeelement(parent, "wp:extent", cx=str(width), cy=str(height))
        makeelement(parent, "wp:effectExtent", l="0", r="0", t="0", b="0")
        makeelement(parent, "wp:wrapTopAndBottom")
        self.create_docx_image_markup(parent, "cover.jpg", _("Cover"), img.rid, width, height)
        return ans

    def write_cover_block(self: _typing.Self, body: _typing.Any, cover_image: _typing.Any) -> None:
        """
        Write cover block under the format's safety and compatibility rules.

        Example:
            Exercise ImagesManager.write cover block through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param body: Value supplied for body under the utility contract.
        :param cover_image: Value supplied for cover image under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        makeelement, namespaces = (
            self.document_relationships.namespace.makeelement,
            self.document_relationships.namespace.namespaces,
        )
        pbb = body[0].xpath('//*[local-name()="pageBreakBefore"]')[0]
        pbb.set("{%s}val" % namespaces["w"], "on")
        p = makeelement(body, "w:p", append=False)
        body.insert(0, p)
        r = makeelement(p, "w:r")
        r.append(cover_image)

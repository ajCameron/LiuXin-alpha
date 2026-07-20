#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

from __future__ import with_statement
from __future__ import annotations

import typing as _typing

import os
import re

from LiuXin_alpha.utils.date import isoformat, now
from LiuXin_alpha.utils.mine_types import guess_type

# Py2/Py3 compatibility layer
from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode

__license__ = "GPL v3"
__copyright__ = "2009, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"


def meta_info_to_oeb_metadata(mi: _typing.Any, m: _typing.Any, log: _typing.Any, override_input_metadata: bool = False) -> None:
    """
    Prepare a section of oeb metadata for writing.
    :param mi:
    :type mi: calibreMetadata
    :param m:
    :type m: OEB Metadata object (LiuXin.file_formats.oeb.base Metadata object)
    :param log:
    :param override_input_metadata:
    :return:
    """
    if not hasattr(mi, "is_null"):
        raise TypeError("meta_info_to_oeb_metadata requires a metadata-like object with is_null()")

    def safe_attr(name: _typing.Any, default: _typing.Any = None) -> _typing.Any:
        try:
            return getattr(mi, name)
        except Exception:
            return default

    from LiuXin_alpha.file_formats.oeb.base import OPF

    if not mi.is_null("title"):
        m.clear("title")
        m.add("title", mi.title)
    title_sort = safe_attr("title_sort")
    if title_sort:
        if not m.title:
            m.add("title", title_sort)
        m.clear("title_sort")
        m.add("title_sort", title_sort)
    if not mi.is_null("authors"):
        m.filter("creator", lambda local_x: local_x.role.lower() in ["aut", ""])
        for a in mi.authors:
            attrib = {"role": "aut"}
            author_sort = safe_attr("author_sort")
            if author_sort:
                attrib[OPF("file-as")] = author_sort
            m.add("creator", a, attrib=attrib)
    if not mi.is_null("book_producer"):
        m.filter("contributor", lambda local_x: local_x.role.lower() == "bkp")
        m.add("contributor", safe_attr("book_producer"), role="bkp")
    elif override_input_metadata:
        m.filter("contributor", lambda local_x: local_x.role.lower() == "bkp")
    if not mi.is_null("comments"):
        m.clear("description")
        m.add("description", safe_attr("comments"))
    elif override_input_metadata:
        m.clear("description")
    if not mi.is_null("publisher"):
        m.clear("publisher")
        m.add("publisher", safe_attr("publisher"))
    elif override_input_metadata:
        m.clear("publisher")
    if not mi.is_null("series"):
        m.clear("series")
        m.add("series", safe_attr("series"))
    elif override_input_metadata:
        m.clear("series")
    identifiers = mi.get_identifiers()
    set_isbn = False
    for typ, val in iteritems(identifiers):
        if isinstance(val, dict):
            vals = list(val.keys())
        elif isinstance(val, (set, frozenset, list, tuple)):
            vals = list(val)
        elif val is None:
            vals = []
        else:
            vals = [val]
        vals = [six_unicode(v) for v in vals if v]
        if not vals:
            continue
        vals = sorted(vals)
        has = False
        if typ.lower() == "isbn":
            set_isbn = True
        primary_val = vals[0]
        for x in m.identifier:
            if x.scheme.lower() == typ.lower():
                x.content = primary_val
                has = True
        if not has:
            m.add("identifier", primary_val, scheme=typ.upper())
    if override_input_metadata and not set_isbn:
        m.filter("identifier", lambda local_x: local_x.scheme.lower() == "isbn")
    if not mi.is_null("languages"):
        m.clear("language")
        for lang in mi.languages:
            if lang and lang.lower() not in ("und", ""):
                m.add("language", lang)
    if not mi.is_null("series_index"):
        m.clear("series_index")
        try:
            series_index = mi.format_series_index()
        except Exception:
            series_index = safe_attr("series_index")
        m.add("series_index", series_index)
    elif override_input_metadata:
        m.clear("series_index")
    if not mi.is_null("rating"):
        m.clear("rating")
        m.add("rating", "%.2f" % safe_attr("rating"))
    elif override_input_metadata:
        m.clear("rating")
    if not mi.is_null("tags"):
        m.clear("subject")
        for t in safe_attr("tags", []) or []:
            m.add("subject", t)
    elif override_input_metadata:
        m.clear("subject")
    if not mi.is_null("pubdate"):
        m.clear("date")
        m.add("date", isoformat(safe_attr("pubdate")))
    if not mi.is_null("timestamp"):
        m.clear("timestamp")
        m.add("timestamp", isoformat(safe_attr("timestamp")))
    if not mi.is_null("rights"):
        m.clear("rights")
        m.add("rights", safe_attr("rights"))
    if not mi.is_null("publication_type"):
        m.clear("publication_type")
        m.add("publication_type", safe_attr("publication_type"))

    if not m.timestamp:
        m.add("timestamp", isoformat(now()))


class MergeMetadata(object):
    """
    Merge in user metadata, including cover
    """

    def __call__(self: _typing.Self, oeb: _typing.Any, mi: _typing.Any, opts: _typing.Any, override_input_metadata: bool = False) -> None:
        self.oeb, self.log = oeb, oeb.log
        m = self.oeb.metadata

        def safe_attr(name: _typing.Any, default: _typing.Any = None) -> _typing.Any:
            try:
                return getattr(mi, name)
            except Exception:
                return default

        def first_identifier_value(raw: _typing.Any) -> _typing.Any:
            if raw is None:
                return None
            if isinstance(raw, dict):
                vals = list(raw.keys())
            elif isinstance(raw, (set, frozenset, list, tuple)):
                vals = list(raw)
            else:
                vals = [raw]
            vals = [six_unicode(v) for v in vals if v]
            if not vals:
                return None
            return sorted(vals)[0]

        self.log("Merging user specified metadata...")
        meta_info_to_oeb_metadata(mi, m, oeb.log, override_input_metadata=override_input_metadata)
        cover_id = self.set_cover(mi, opts.prefer_metadata_cover)
        m.clear("cover")

        if cover_id is not None:
            m.add("cover", cover_id)

        uuid_val = first_identifier_value(safe_attr("uuid"))
        if uuid_val is not None:
            m.filter("identifier", lambda x: x.id == "uuid_id")
            self.oeb.metadata.add("identifier", uuid_val, id="uuid_id", scheme="uuid")
            self.oeb.uid = self.oeb.metadata.identifier[-1]

        app_id = first_identifier_value(safe_attr("application_id"))
        if app_id is not None:
            m.filter("identifier", lambda local_x: local_x.scheme == "calibre")
            self.oeb.metadata.add("identifier", app_id, scheme="calibre")

    def set_cover(self: _typing.Self, mi: _typing.Any, prefer_metadata_cover: _typing.Any) -> _typing.Any:
        cdata, ext = "", "jpg"
        cover = getattr(mi, "cover", None)
        cover_data = getattr(mi, "cover_data", None)
        if cover and os.access(cover, os.R_OK):
            cdata = open(cover, "rb").read()
            ext = cover.rpartition(".")[-1].lower().strip()
        elif cover_data:
            if isinstance(cover_data, dict):
                raw = next(iter(cover_data.keys()), None)
            else:
                raw = cover_data
            if isinstance(raw, (list, tuple)) and len(raw) >= 2 and raw[1]:
                ext = raw[0]
                cdata = raw[1]
        if ext not in ("png", "jpg", "jpeg"):
            ext = "jpg"
        cover_id = old_cover = None
        if "cover" in self.oeb.guide:
            old_cover = self.oeb.guide["cover"]
        if prefer_metadata_cover and old_cover is not None:
            cdata = ""
        if cdata:
            self.oeb.guide.remove("cover")
            self.oeb.guide.remove("titlepage")
        elif self.oeb.plumber_output_format in {"mobi", "azw3"} and old_cover is not None:
            # The amazon formats don't support html cover pages, so remove them even if no cover was specified.
            self.oeb.guide.remove("titlepage")
        if old_cover is not None:
            if old_cover.href in self.oeb.manifest.hrefs:
                item = self.oeb.manifest.hrefs[old_cover.href]
                if not cdata:
                    return item.id
                self.remove_old_cover(item)
            elif not cdata:
                cover_id = self.oeb.manifest.generate(id="cover")[0]
                self.oeb.manifest.add(cover_id, old_cover.href, "image/jpeg")
                return cover_id
        if cdata:
            cover_id, href = self.oeb.manifest.generate("cover", "cover." + ext)
            self.oeb.manifest.add(cover_id, href, guess_type("cover." + ext)[0], data=cdata)
            self.oeb.guide.add("cover", "Cover", href)
        return cover_id

    def remove_old_cover(self: _typing.Self, cover_item: _typing.Any) -> None:

        from lxml import etree
        from LiuXin_alpha.file_formats.oeb.base import XPath

        self.oeb.manifest.remove(cover_item)

        # Remove any references to the cover in the HTML
        affected_items = set()
        for item in self.oeb.spine:
            try:
                images = XPath("//h:img[@src]")(item.data)
            except:
                images = []
            removed = False
            for img in images:
                href = item.abshref(img.get("src"))
                if href == cover_item.href:
                    img.getparent().remove(img)
                    removed = True
            if removed:
                affected_items.add(item)

        # Check if the resulting HTML has no content, if so remove it
        for item in affected_items:
            body = XPath("//h:body")(item.data)
            if body:
                text = etree.tostring(body[0], method="text", encoding=six_unicode)
            else:
                text = ""
            text = re.sub(r"\s+", "", text)
            if not text and not XPath("//h:img|//svg:svg")(item.data):
                self.log("Removing %s as it is a wrapper around the cover image" % item.href)
                self.oeb.spine.remove(item)
                self.oeb.manifest.remove(item)

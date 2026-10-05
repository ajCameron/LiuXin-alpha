#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

"""
Generate metadata-jacket content for transformed books.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise jacket through a consuming regression::

        python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py
"""
from __future__ import annotations, with_statement

import os
import re
import sys
import typing as _typing
from string import Formatter
from xml.sax.saxutils import escape

from lxml import etree

from LiuXin_alpha.constants import iswindows
from LiuXin_alpha.file_formats.oeb.base import (
    XHTML,
    XHTML_NS,
    XPath,
    urldefrag,
    xml2text,
)
from LiuXin_alpha.library.comments import comments_to_html
from LiuXin_alpha.metadata import fmt_sidx
from LiuXin_alpha.utils.date import is_date_undefined, strftime
from LiuXin_alpha.utils.language_tools.icu import sort_key
from LiuXin_alpha.utils.libraries.BeautifulSoup import BeautifulSoup
from LiuXin_alpha.utils.libraries.calibre_chardet import strip_encoding_declarations
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode
from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.mine_types import guess_type
from LiuXin_alpha.utils.resources import P

unicode = str

__license__ = "GPL v3"
__copyright__ = "2009, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"

JACKET_XPATH = '//h:meta[@name="calibre-content" and @content="jacket"]'


class SafeFormatter(Formatter):
    """
    Provide the safeformatter contract for validated ebook processing.

    Example:
        Exercise SafeFormatter through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py
    """
    def get_value(self: _typing.Self, *args: _typing.Any, **kwargs: _typing.Any) -> _typing.Any:
        """
        Return value under the format's safety and compatibility rules.

        Example:
            Exercise SafeFormatter.get value through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            return Formatter.get_value(self, *args, **kwargs)
        except KeyError:
            return ""


class Jacket(object):
    """
    Book jacket manipulation. Remove first image and insert comments at start of book.

    Example:
        Exercise Jacket through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py
    """

    def remove_images(self: _typing.Self, item: _typing.Any, limit: int = 1) -> _typing.Any:
        """
        Perform the remove images operation under explicit file-format and conversion rules.

        Example:
            Exercise Jacket.remove images through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param item: Value supplied for item under the utility contract.
        :param limit: Value supplied for limit under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        path = XPath("//h:img[@src]")
        removed = 0
        for img in path(item.data):
            if removed >= limit:
                break
            href = item.abshref(img.get("src"))
            image = self.oeb.manifest.hrefs.get(href, None)
            if image is not None:
                self.oeb.manifest.remove(image)
                img.getparent().remove(img)
                removed += 1
        return removed

    def remove_first_image(self: _typing.Self) -> None:
        """
        Perform the remove first image operation under explicit file-format and conversion rules.

        Example:
            Exercise Jacket.remove first image through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        deleted_item = None
        for item in self.oeb.spine:
            removed = self.remove_images(item)
            if removed > 0:
                self.log("Removed first image")
                body = XPath("//h:body")(item.data)
                if body:
                    raw = xml2text(body[0]).strip()
                    imgs = XPath("//h:img|//svg:svg")(item.data)
                    if not raw and not imgs:
                        self.log("Removing %s as it has no content" % item.href)
                        self.oeb.manifest.remove(item)
                        deleted_item = item
                break
        if deleted_item is not None:
            for item in list(self.oeb.toc):
                href = urldefrag(item.href)[0]
                if href == deleted_item.href:
                    self.oeb.toc.remove(item)

    def insert_metadata(self: _typing.Self, mi: _typing.Any) -> None:
        """
        Perform the insert metadata operation under explicit file-format and conversion rules.

        Example:
            Exercise Jacket.insert metadata through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param mi: Metadata object exposed to the template function.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.log("Inserting metadata into book...")

        try:
            tags = map(six_unicode, self.oeb.metadata.subject)
        except:
            tags = []

        try:
            comments = six_unicode(self.oeb.metadata.description[0])
        except:
            comments = ""

        try:
            title = six_unicode(self.oeb.metadata.title[0])
        except:
            title = _("Unknown")

        root = render_jacket(
            mi,
            self.opts.output_profile,
            alt_title=title,
            alt_tags=tags,
            alt_comments=comments,
            rescale_fonts=True,
        )
        jacket_id, href = self.oeb.manifest.generate("calibre_jacket", "jacket.xhtml")

        jacket = self.oeb.manifest.add(jacket_id, href, guess_type(href)[0], data=root)
        self.oeb.spine.insert(0, jacket, True)
        self.oeb.inserted_metadata_jacket = jacket
        for img, path in referenced_images(root):
            self.oeb.log("Embedding referenced image %s into jacket" % path)
            ext = path.rpartition(".")[-1].lower()
            item_id, href = self.oeb.manifest.generate("jacket_image", "jacket_img." + ext)
            with open(path, "rb") as f:
                item = self.oeb.manifest.add(item_id, href, guess_type(href)[0], data=f.read())
            item.unload_data_from_memory()
            img.set("src", jacket.relhref(item.href))

    def remove_existing_jacket(self: _typing.Self) -> None:
        """
        Perform the remove existing jacket operation under explicit file-format and conversion rules.

        Example:
            Exercise Jacket.remove existing jacket through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for x in self.oeb.spine[:4]:
            if XPath(JACKET_XPATH)(x.data):
                self.remove_images(x, limit=sys.maxsize)
                self.oeb.manifest.remove(x)
                self.log("Removed existing jacket")
                break

    def __call__(self: _typing.Self, oeb: _typing.Any, opts: _typing.Any, metadata: _typing.Any) -> None:
        """
        Add metadata in jacket.xhtml if specified in opts. If not specified, remove previous jacket instance

        Example:
            Exercise Jacket.  call   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param oeb: Value supplied for oeb under the utility contract.
        :param opts: Value supplied for opts under the utility contract.
        :param metadata: Value supplied for metadata under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.oeb, self.opts, self.log = oeb, opts, oeb.log
        self.remove_existing_jacket()
        if opts.remove_first_image:
            self.remove_first_image()
        if opts.insert_metadata:
            self.insert_metadata(metadata)


# Render Jacket {{{


def get_rating(rating: _typing.Any, rchar: _typing.Any, e_rchar: _typing.Any) -> _typing.Any:
    """
    Return rating under the format's safety and compatibility rules.

    Example:
        Exercise get rating through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


    :param rating: Value supplied for rating under the utility contract.
    :param rchar: Value supplied for rchar under the utility contract.
    :param e_rchar: Value supplied for e rchar under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    ans = ""
    try:
        num = float(rating) / 2
    except:
        return ans
    num = max(0, num)
    num = min(num, 5)
    if num < 1:
        return ans

    ans = "%s%s" % (rchar * int(num), e_rchar * (5 - int(num)))
    return ans


class Series(unicode):
    """
    Provide the series contract for validated ebook processing.

    Example:
        Exercise Series through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py
    """
    def __new__(cls: type[_typing.Self], series: _typing.Any, series_index: _typing.Any) -> _typing.Any:
        """
        Perform the new operation under explicit file-format and conversion rules.

        Example:
            Exercise Series.  new   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param series: Value supplied for series under the utility contract.
        :param series_index: Value supplied for series index under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if series and series_index is not None:
            roman = _("Number {1} of <em>{0}</em>").format(
                escape(series), escape(fmt_sidx(series_index, use_roman=True))
            )
            series = escape(series + " [%s]" % fmt_sidx(series_index, use_roman=False))
        else:
            series = roman = escape(series or "")
        s = unicode.__new__(cls, series)
        s.roman = roman
        return s


class Tags(unicode):
    """
    Provide the tags contract for validated ebook processing.

    Example:
        Exercise Tags through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py
    """
    def __new__(cls: type[_typing.Self], tags: _typing.Any, output_profile: _typing.Any) -> _typing.Any:
        """
        Perform the new operation under explicit file-format and conversion rules.

        Example:
            Exercise Tags.  new   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param tags: Value supplied for tags under the utility contract.
        :param output_profile: Value supplied for output profile under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        tags = [escape(x) for x in tags or ()]
        t = unicode.__new__(cls, ", ".join(tags))
        t.alphabetical = ", ".join(sorted(tags, key=sort_key))
        t.tags_list = tags
        return t


def render_jacket(
    mi: _typing.Any,
    output_profile: _typing.Any,
    alt_title: _typing.Any = _("Unknown"),
    alt_tags: _typing.Any = None,
    alt_comments: str = "",
    alt_publisher: str = "",
    rescale_fonts: bool = False,
) -> _typing.Any:
    """
    Perform the render jacket operation under explicit file-format and conversion rules.

    Example:
        Exercise render jacket through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


    :param mi: Metadata object exposed to the template function.
    :param output_profile: Value supplied for output profile under the utility contract.
    :param alt_title: Value supplied for alt title under the utility contract.
    :param alt_tags: Value supplied for alt tags under the utility contract.
    :param alt_comments: Value supplied for alt comments under the utility contract.
    :param alt_publisher: Value supplied for alt publisher under the utility contract.
    :param rescale_fonts: Value supplied for rescale fonts under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if alt_tags is None:
        alt_tags = []

    css = P("jacket/stylesheet.css", data=True).decode("utf-8")
    template = P("jacket/template.xhtml", data=True).decode("utf-8")

    template = re.sub(r"<!--.*?-->", "", template, flags=re.DOTALL)
    css = re.sub(r"/\*.*?\*/", "", css, flags=re.DOTALL)

    try:
        title_str = mi.title if mi.title else alt_title
    except:
        title_str = _("Unknown")
    title_str = escape(title_str)
    title = '<span class="title">%s</span>' % title_str

    series = Series(mi.series, mi.series_index)
    try:
        publisher = mi.publisher if mi.publisher else alt_publisher
    except:
        publisher = ""
    publisher = escape(publisher)

    try:
        if is_date_undefined(mi.pubdate):
            pubdate = ""
        else:
            pubdate = strftime("%Y", mi.pubdate.timetuple())
    except:
        pubdate = ""

    rating = get_rating(mi.rating, output_profile.ratings_char, output_profile.empty_ratings_char)

    tags = Tags((mi.tags if mi.tags else alt_tags), output_profile)

    comments = mi.comments if mi.comments else alt_comments
    comments = comments.strip()
    orig_comments = comments
    if comments:
        comments = comments_to_html(comments)

    try:
        author = mi.format_authors()
    except:
        author = ""
    author = escape(author)

    def generate_html(local_comments: _typing.Any) -> _typing.Any:
        """
        Perform the generate html operation under explicit file-format and conversion rules.

        Example:
            Exercise render jacket.generate html through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param local_comments: Value supplied for local comments under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        args = dict(
            xmlns=XHTML_NS,
            title_str=title_str,
            css=css,
            title=title,
            author=author,
            publisher=publisher,
            pubdate_label=_("Published"),
            pubdate=pubdate,
            series_label=_("Series"),
            series=series,
            rating_label=_("Rating"),
            rating=rating,
            tags_label=_("Tags"),
            tags=tags,
            comments=local_comments,
            footer="",
            searchable_tags=" ".join(escape(t) + "ttt" for t in tags.tags_list),
        )
        for key in mi.custom_field_keys():
            try:
                display_name, val = mi.format_field_extended(key)[:2]
                key = key.replace("#", "_")
                args[key] = escape(val)
                args[key + "_label"] = escape(display_name)
            except:
                # if the val (custom column contents) is None, don't add to args
                pass

        if False:
            print("Custom column values available in jacket template:")
            for key in args.keys():
                if key.startswith("_") and not key.endswith("_label"):
                    print(" %s: %s" % ("#" + key[1:], args[key]))

        # Used in the comment describing use of custom columns in templates
        # Don't change this unless you also change it in template.xhtml
        args["_genre_label"] = args.get("_genre_label", "{_genre_label}")
        args["_genre"] = args.get("_genre", "{_genre}")

        formatter = SafeFormatter()
        generated_html = formatter.format(template, **args)

        # Post-process the generated html to strip out empty header items

        soup = BeautifulSoup(generated_html)
        if not series:
            series_tag = soup.find(attrs={"class": "cbj_series"})
            if series_tag is not None:
                series_tag.extract()
        if not rating:
            rating_tag = soup.find(attrs={"class": "cbj_rating"})
            if rating_tag is not None:
                rating_tag.extract()
        if not tags:
            tags_tag = soup.find(attrs={"class": "cbj_tags"})
            if tags_tag is not None:
                tags_tag.extract()
        if not pubdate:
            pubdate_tag = soup.find(attrs={"class": "cbj_pubdata"})
            if pubdate_tag is not None:
                pubdate_tag.extract()
        if output_profile.short_name != "kindle":
            hr_tag = soup.find("hr", attrs={"class": "cbj_kindle_banner_hr"})
            if hr_tag is not None:
                hr_tag.extract()

        return strip_encoding_declarations(soup.renderContents("utf-8").decode("utf-8"))

    from LiuXin_alpha.file_formats.oeb.base import RECOVER_PARSER

    try:
        root = etree.fromstring(generate_html(comments), parser=RECOVER_PARSER)
    except:
        try:
            root = etree.fromstring(generate_html(escape(orig_comments)), parser=RECOVER_PARSER)
        except:
            root = etree.fromstring(generate_html(""), parser=RECOVER_PARSER)

    if rescale_fonts:
        # We ensure that the conversion pipeline will set the font sizes for
        # text in the jacket to the same size as the font sizes for the rest of
        # the text in the book. That means that as long as the jacket uses
        # relative font sizes (em or %), the post conversion font size will be
        # the same as for text in the main book. So text with size x em will
        # be rescaled to the same value in both the jacket and the main content.
        #
        # We cannot use calibre_rescale_100 on the body tag as that will just
        # give the body tag a font size of 1em, which is useless.
        for body in root.xpath('//*[local-name()="body"]'):
            fw = body.makeelement(XHTML("div"))
            fw.set("class", "calibre_rescale_100")
            for child in body:
                fw.append(child)
            body.append(fw)
    from LiuXin_alpha.file_formats.oeb.polish.pretty import pretty_html_tree

    pretty_html_tree(None, root)
    return root


# }}}


def linearize_jacket(oeb: _typing.Any) -> None:
    """
    Perform the linearize jacket operation under explicit file-format and conversion rules.

    Example:
        Exercise linearize jacket through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


    :param oeb: Value supplied for oeb under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    for x in oeb.spine[:4]:
        if XPath(JACKET_XPATH)(x.data):
            for e in XPath("//h:table|//h:tr|//h:th")(x.data):
                e.tag = XHTML("div")
            for e in XPath("//h:td")(x.data):
                e.tag = XHTML("span")
            break


def referenced_images(root: _typing.Any) -> _typing.Iterator[_typing.Any]:
    """
    Perform the referenced images operation under explicit file-format and conversion rules.

    Example:
        Exercise referenced images through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


    :param root: Root directory that bounds path resolution or traversal.
    :return: An iterator yielding the normalized values described above.
    """
    for img in XPath("//h:img[@src]")(root):
        src = img.get("src")
        if src.startswith("file://"):
            path = src[7:]
            if iswindows and path.startswith("/"):
                path = path[1:]
            if os.path.exists(path):
                yield img, path

#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

"""
Read and normalize metadata from the package's ebook container.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise metadata through a consuming regression::

        python -m pytest -q tests/file_formats/chm/test_chm_modernized.py
"""
from __future__ import annotations

import typing as _typing

"""CHM metadata extraction support."""

import codecs
import io
import re

from LiuXin_alpha.metadata.utils import calibreMetaInformation, string_to_authors
from LiuXin_alpha.utils.calibre import force_unicode
from LiuXin_alpha.utils.libraries.calibre_chardet import xml_to_unicode
from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.utils.ptempfiles import TemporaryFile

__license__ = "GPL v3"
__copyright__ = "2010, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"


def _clean(s: _typing.Any) -> _typing.Any:
    """
    Perform the clean operation under explicit file-format and conversion rules.

    Example:
        Exercise  clean through a consuming regression::

            python -m pytest -q tests/file_formats/chm/test_chm_modernized.py


    :param s: Value supplied for s under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return s.replace("\u00a0", " ")


def _text_content(elem: _typing.Any) -> _typing.Any:
    """
    Perform the text content operation under explicit file-format and conversion rules.

    Example:
        Exercise  text content through a consuming regression::

            python -m pytest -q tests/file_formats/chm/test_chm_modernized.py


    :param elem: Value supplied for elem under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return "".join(elem.itertext()).strip() if elem is not None else ""


def _metadata_from_table(soup: _typing.Any, searchfor: _typing.Any) -> _typing.Any:
    """
    Perform the metadata from table operation under explicit file-format and conversion rules.

    Example:
        Exercise  metadata from table through a consuming regression::

            python -m pytest -q tests/file_formats/chm/test_chm_modernized.py


    :param soup: Value supplied for soup under the utility contract.
    :param searchfor: Value supplied for searchfor under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    for td in soup.xpath("//td"):
        td_text = _clean(_text_content(td))
        if not re.search(searchfor, td_text, flags=re.I):
            continue

        row = td.getparent()
        if row is not None:
            tds = row.xpath("./td")
            if len(tds) >= 2:
                label = _clean(_text_content(tds[0]))
                value = _clean(_text_content(tds[1]))
                if re.match(r"^\s*" + searchfor + r"\s*$", label, flags=re.I):
                    return re.sub(r"^:", "", value).strip()

        return re.sub(r"^[^:]+:", "", td_text).strip()
    return None


def _metadata_from_span(soup: _typing.Any, searchfor: _typing.Any) -> _typing.Any:
    """
    Perform the metadata from span operation under explicit file-format and conversion rules.

    Example:
        Exercise  metadata from span through a consuming regression::

            python -m pytest -q tests/file_formats/chm/test_chm_modernized.py


    :param soup: Value supplied for soup under the utility contract.
    :param searchfor: Value supplied for searchfor under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    for span in soup.xpath("//span[@class]"):
        klass = span.attrib.get("class", "")
        if re.search(searchfor, klass, flags=re.I):
            return _clean(_text_content(span))
    return None


def _get_authors(soup: _typing.Any) -> _typing.Any:
    """
    Perform the get authors operation under explicit file-format and conversion rules.

    Example:
        Exercise  get authors through a consuming regression::

            python -m pytest -q tests/file_formats/chm/test_chm_modernized.py


    :param soup: Value supplied for soup under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    aut = _metadata_from_span(soup, r"author") or _metadata_from_table(soup, r"^\s*by\s*:?\s+")
    ans = [_("Unknown")]
    if aut is not None:
        ans = string_to_authors(aut)
    return ans


def _get_publisher(soup: _typing.Any) -> bool:
    """
    Perform the get publisher operation under explicit file-format and conversion rules.

    Example:
        Exercise  get publisher through a consuming regression::

            python -m pytest -q tests/file_formats/chm/test_chm_modernized.py


    :param soup: Value supplied for soup under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return _metadata_from_span(soup, "imprint") or _metadata_from_table(soup, "publisher")


def _get_isbn(soup: _typing.Any) -> bool:
    """
    Perform the get isbn operation under explicit file-format and conversion rules.

    Example:
        Exercise  get isbn through a consuming regression::

            python -m pytest -q tests/file_formats/chm/test_chm_modernized.py


    :param soup: Value supplied for soup under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return _metadata_from_span(soup, "isbn") or _metadata_from_table(soup, "isbn")


def _get_comments(soup: _typing.Any) -> str | None:
    """
    Perform the get comments operation under explicit file-format and conversion rules.

    Example:
        Exercise  get comments through a consuming regression::

            python -m pytest -q tests/file_formats/chm/test_chm_modernized.py


    :param soup: Value supplied for soup under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    date = _metadata_from_span(soup, "cwdate") or _metadata_from_table(soup, "pub date")
    pages = _metadata_from_span(soup, "pages") or _metadata_from_table(soup, "pages")
    try:
        if date is None or pages is None:
            return None
        date = date.replace("\u00a9", "").strip()
        pages = re.search(r"\d+", pages).group(0)
        return f"Published {date}, {pages} pages."
    except Exception:
        pass
    return None


def _get_cover(soup: _typing.Any, rdr: _typing.Any) -> _typing.Any:
    """
    Perform the get cover operation under explicit file-format and conversion rules.

    Example:
        Exercise  get cover through a consuming regression::

            python -m pytest -q tests/file_formats/chm/test_chm_modernized.py


    :param soup: Value supplied for soup under the utility contract.
    :param rdr: Value supplied for rdr under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    ans = None
    try:
        for img in soup.xpath("//img[@alt][@src]"):
            if re.search(r"cover", img.attrib.get("alt", ""), flags=re.I):
                ans = img.attrib.get("src")
                break
    except Exception:
        ans = None
    if ans is None:
        ratios = {}
        for img in soup.xpath("//img[@src]"):
            try:
                ratio = abs(
                    float(re.search(r"[0-9.]+", img.attrib.get("height", "")).group())
                    / float(re.search(r"[0-9.]+", img.attrib.get("width", "")).group())
                    - 1.25
                )
                ratios[ratio] = img.attrib.get("src")
            except Exception:
                if img.attrib.get("src"):
                    ratios.setdefault(0, img.attrib.get("src"))
                continue
        if ratios:
            ans = ratios[sorted(ratios.keys())[0]]

    if ans is not None:
        try:
            ans = rdr.GetFile(ans)
        except Exception:
            ans = rdr.root + "/" + ans
            try:
                ans = rdr.GetFile(ans)
            except Exception:
                ans = None

        if ans is not None:
            try:
                from PIL import Image

                buf = io.BytesIO()
                Image.open(io.BytesIO(ans)).convert("RGB").save(buf, "JPEG")
                ans = buf.getvalue()
            except Exception:
                ans = None

    return ans


def get_metadata_from_reader(rdr: _typing.Any, calibre: bool = False) -> _typing.Any:
    """
    Get metadata from a CHM reader instance.

    Example:
        Exercise get metadata from reader through a consuming regression::

            python -m pytest -q tests/file_formats/chm/test_chm_modernized.py


    :param rdr: Value supplied for rdr under the utility contract.
    :param calibre: Value supplied for calibre under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    try:
        raw = rdr.get_home()
    except Exception:
        raw = rdr.GetFile(rdr.home)

    from lxml import html

    home_raw = xml_to_unicode(raw, strip_encoding_pats=True, resolve_entities=True)[0]
    home = html.fromstring(home_raw)

    title = getattr(rdr, "title", _("Unknown"))

    try:
        x = rdr.GetEncoding()
        if isinstance(x, bytes):
            x = x.decode("ascii")
        codecs.lookup(x)
        enc = x
    except Exception as e:
        default_log.log_exception(message="Attempt to read CHM encoding failed.", exception=e, level="INFO")
        enc = "cp1252"

    title = force_unicode(title, enc)
    authors = _get_authors(home)
    mi = calibreMetaInformation(title, authors)

    publisher = _get_publisher(home)
    if publisher:
        mi.publisher = publisher
    isbn = _get_isbn(home)
    if isbn:
        mi.isbn = isbn
    comments = _get_comments(home)
    if comments:
        mi.comments = comments

    cdata = _get_cover(home, rdr)
    if cdata is not None:
        mi.cover_data = ("jpg", cdata)

    return mi


def get_metadata(stream: _typing.Any) -> _typing.Any:
    """
    Return metadata under the format's safety and compatibility rules.

    Example:
        Exercise get metadata through a consuming regression::

            python -m pytest -q tests/file_formats/chm/test_chm_modernized.py


    :param stream: Input or output stream wrapped by the terminal or compatibility
        layer.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    with TemporaryFile("_chm_metadata.chm") as fname:
        with open(fname, "wb") as f:
            f.write(stream.read())

        from LiuXin_alpha.file_formats.chm.reader import CHMReader

        rdr = CHMReader(fname, default_log)
        return get_metadata_from_reader(rdr)

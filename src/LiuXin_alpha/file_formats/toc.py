#!/usr/bin/env  python

"""
Represent, traverse and serialize hierarchical ebook table-of-contents entries.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise toc through a consuming regression::

        python -m pytest -q tests/file_formats/test_toc_corner_cases.py
"""
# Todo: probably should be merged with ebook_toc in metadata

from __future__ import print_function
from __future__ import annotations

import typing as _typing

import os
import glob
import re
import logging
import functools
from urllib.parse import unquote, urlparse
from collections import Counter

from LiuXin_alpha.utils.libraries.liuxin_etree import etree, ElementMaker

from typing import Optional, Tuple

from LiuXin_alpha.utils.libraries.BeautifulSoup import BeautifulSoup

from LiuXin_alpha.utils.libraries.calibre_chardet import xml_to_unicode
from LiuXin_alpha.constants import __appname__, __version__
from LiuXin_alpha.utils.libraries.cleantext import clean_xml_chars

# Py2/Py3 compatability layer
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode

__license__ = "GPL v3"
__copyright__ = "2010, Kovid Goyal <kovid at kovidgoyal.net>"

logger = logging.getLogger(__name__)

NCX_NS = "http://www.daisy.org/z3986/2005/ncx/"
CALIBRE_NS = "http://calibre.kovidgoyal.net/2009/metadata"
NSMAP = {None: NCX_NS, "calibre": CALIBRE_NS}


E = ElementMaker(namespace=NCX_NS, nsmap=NSMAP)

C = ElementMaker(namespace=CALIBRE_NS, nsmap=NSMAP)


def _parse_href(raw_href: str) -> Optional[Tuple[str, Optional[str]]]:
    """
    Parse href under the format's safety and compatibility rules.

    Example:
        Exercise  parse href through a consuming regression::

            python -m pytest -q tests/file_formats/test_toc_corner_cases.py


    :param raw_href: Value supplied for raw href under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if raw_href is None:
        return None
    parsed = urlparse(unquote(raw_href))
    href = (parsed.path or "").strip()
    fragment = (parsed.fragment or "").strip() or None
    # Completely empty href+fragment entries are not meaningful in a TOC
    if not href and fragment is None:
        return None
    return href, fragment


class TOC(list):
    """
    Provide the toc contract for validated ebook processing.

    Example:
        Exercise TOC through a consuming regression::

            python -m pytest -q tests/file_formats/test_toc_corner_cases.py
    """
    def __init__(
        self: _typing.Self,
        href: Optional[str] = None,
        fragment: Optional[str] = None,
        text: Optional[str] = None,
        parent: Optional["TOC"] = None,
        play_order: int = 0,
        base_path: Optional[str] = None,
        type: str = "unknown",
        author: _typing.Any = None,
        description: _typing.Any = None,
        toc_thumbnail: _typing.Any = None,
    ) -> None:
        """
        Startup an empty table of contents.

        Example:
            Exercise TOC.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/test_toc_corner_cases.py


        :param href: Value supplied for href under the utility contract.
        :param fragment: Value supplied for fragment under the utility contract.
        :param text: Text parsed, normalized or rendered.
        :param parent: Value supplied for parent under the utility contract.
        :param play_order: Value supplied for play order under the utility contract.
        :param base_path: Value supplied for base path under the utility contract.
        :param type: Value supplied for type under the utility contract.
        :param author: Value supplied for author under the utility contract.
        :param description: Value supplied for description under the utility contract.
        :param toc_thumbnail: Value supplied for toc thumbnail under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        super().__init__()

        self.href = href
        self.fragment = fragment
        if not self.fragment:
            self.fragment = None
        self.text = text
        self.parent = parent
        self.base_path = os.getcwd() if base_path is None else base_path
        self.play_order = play_order
        self.type = type
        self.author = author
        self.description = description
        self.toc_thumbnail = toc_thumbnail

    def __str__(self: _typing.Self) -> _typing.Any:
        """
        Perform the str operation under explicit file-format and conversion rules.

        Example:
            Exercise TOC.  str   through a consuming regression::

                python -m pytest -q tests/file_formats/test_toc_corner_cases.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        lines = ["TOC: %s#%s %s" % (self.href, self.fragment, self.text)]
        for child in self:
            c = str(child).splitlines()
            for l in c:
                lines.append("\t" + l)
        return "\n".join(lines)

    def count(self: _typing.Self, type: _typing.Any) -> _typing.Any:
        """
        Perform the count operation under explicit file-format and conversion rules.

        Example:
            Exercise TOC.count through a consuming regression::

                python -m pytest -q tests/file_formats/test_toc_corner_cases.py


        :param type: Value supplied for type under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return len([i for i in self.flat() if i.type == type])

    def purge(self: _typing.Self, types: _typing.Any, max: int = 0) -> _typing.Any:
        """
        Perform the purge operation under explicit file-format and conversion rules.

        Example:
            Exercise TOC.purge through a consuming regression::

                python -m pytest -q tests/file_formats/test_toc_corner_cases.py


        :param types: Value supplied for types under the utility contract.
        :param max: Value supplied for max under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        remove = []
        for entry in self.flat():
            if entry.type in types:
                remove.append(entry)
        remove = remove[max:]
        for entry in remove:
            if entry.parent is None:
                continue
            entry.parent.remove(entry)
        return remove

    def remove(self: _typing.Self, entry: _typing.Any) -> None:
        """
        Perform the remove operation under explicit file-format and conversion rules.

        Example:
            Exercise TOC.remove through a consuming regression::

                python -m pytest -q tests/file_formats/test_toc_corner_cases.py


        :param entry: Value supplied for entry under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        list.remove(self, entry)
        entry.parent = None

    def add_item(
        self: _typing.Self,
        href: _typing.Any,
        fragment: _typing.Any,
        text: _typing.Any,
        play_order: _typing.Any = None,
        type: str = "unknown",
        author: _typing.Any = None,
        description: _typing.Any = None,
        toc_thumbnail: _typing.Any = None,
    ) -> _typing.Any:
        """
        Add an item to the toc

        Example:
            Exercise TOC.add item through a consuming regression::

                python -m pytest -q tests/file_formats/test_toc_corner_cases.py


        :param href: Value supplied for href under the utility contract.
        :param fragment: Value supplied for fragment under the utility contract.
        :param text: Text parsed, normalized or rendered.
        :param play_order: Value supplied for play order under the utility contract.
        :param type: Value supplied for type under the utility contract.
        :param author: Value supplied for author under the utility contract.
        :param description: Value supplied for description under the utility contract.
        :param toc_thumbnail: Value supplied for toc thumbnail under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if play_order is None:
            play_order = (self[-1].play_order if len(self) else self.play_order) + 1
        self.append(
            TOC(
                href=href,
                fragment=fragment,
                text=text,
                parent=self,
                base_path=self.base_path,
                play_order=play_order,
                type=type,
                author=author,
                description=description,
                toc_thumbnail=toc_thumbnail,
            )
        )
        return self[-1]

    def top_level_items(self: _typing.Self) -> _typing.Iterator[_typing.Any]:
        """
        Iterate through the top level files.

        Example:
            Exercise TOC.top level items through a consuming regression::

                python -m pytest -q tests/file_formats/test_toc_corner_cases.py


        :return: An iterator yielding the normalized values described above.
        """
        for item in self:
            if item.text is not None:
                yield item

    def depth(self: _typing.Self) -> _typing.Any:
        """
        Perform the depth operation under explicit file-format and conversion rules.

        Example:
            Exercise TOC.depth through a consuming regression::

                python -m pytest -q tests/file_formats/test_toc_corner_cases.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        depth = 1
        for obj in self:
            c = obj.depth()
            if c > depth - 1:
                depth = c + 1
        return depth

    def flat(self: _typing.Self) -> _typing.Iterator[_typing.Any]:
        """
        Depth first iteration over the tree rooted at self

        Example:
            Exercise TOC.flat through a consuming regression::

                python -m pytest -q tests/file_formats/test_toc_corner_cases.py


        :return: An iterator yielding the normalized values described above.
        """
        yield self
        for obj in self:
            for i in obj.flat():
                yield i

    @property
    def abspath(self: _typing.Self) -> _typing.Any:
        """
        Return the file this toc entry points to as a absolute path to a file on the system.

        Example:
            Exercise TOC.abspath through a consuming regression::

                python -m pytest -q tests/file_formats/test_toc_corner_cases.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self.href is None:
            return None
        path = self.href.replace("/", os.sep)
        if not os.path.isabs(path):
            path = os.path.join(self.base_path, path)
        return path

    def read_from_opf(self: _typing.Self, opfreader: _typing.Any) -> None:
        """
        Read from opf under the format's safety and compatibility rules.

        Example:
            Exercise TOC.read from opf through a consuming regression::

                python -m pytest -q tests/file_formats/test_toc_corner_cases.py


        :param opfreader: Value supplied for opfreader under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        toc = opfreader.soup.find("spine", toc=True)
        if toc is not None:
            toc = toc["toc"]
        if toc is None:
            try:
                toc = opfreader.soup.find("guide").find("reference", attrs={"type": "toc"})["href"]
            except:
                for item in opfreader.manifest:
                    if "toc" in item.href().lower():
                        toc = item.href()
                        break

        if toc is not None:
            if toc.lower() not in ("ncx", "ncxtoc"):
                toc = urlparse(unquote(toc))[2]
                toc = toc.replace("/", os.sep)
                if not os.path.isabs(toc):
                    toc = os.path.join(self.base_path, toc)

                try:
                    if not os.path.exists(toc):
                        bn = os.path.basename(toc)
                        bn = bn.replace("_top.htm", "_toc.htm")  # Bug in BAEN OPF files
                        toc = os.path.join(os.path.dirname(toc), bn)

                    self.read_html_toc(toc)
                except Exception as err:
                    logger.warning("Could not read HTML Table of Contents. Continuing anyway. Err: %s", err)

            else:
                path = opfreader.manifest.item(toc.lower())
                path = getattr(path, "path", path)
                if path and os.access(path, os.R_OK):
                    try:
                        self.read_ncx_toc(path)
                    except Exception as err:
                        logger.warning("Invalid NCX file: %s", err)
                    return
                cwd = os.path.abspath(self.base_path)
                m = glob.glob(os.path.join(cwd, "*.ncx"))
                if m:
                    toc = m[0]
                    self.read_ncx_toc(toc)

    def read_ncx_toc(self: _typing.Self, toc: _typing.Any, root: _typing.Any = None) -> None:
        """
        Read ncx toc under the format's safety and compatibility rules.

        Example:
            Exercise TOC.read ncx toc through a consuming regression::

                python -m pytest -q tests/file_formats/test_toc_corner_cases.py


        :param toc: Value supplied for toc under the utility contract.
        :param root: Root directory that bounds path resolution or traversal.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        self.base_path = os.path.dirname(toc)
        if root is None:
            with open(toc, "rb") as toc_file:
                raw = xml_to_unicode(toc_file.read(), assume_utf8=True, strip_encoding_pats=True)[0]
            root = etree.fromstring(raw, parser=etree.XMLParser(recover=True, no_network=True))
        xpn = {"re": "http://exslt.org/regular-expressions"}
        XPath = functools.partial(etree.XPath, namespaces=xpn)

        def get_attr(node: _typing.Any, default: _typing.Any = None, attr: str = "playorder") -> _typing.Any:
            """
            Return attr under the format's safety and compatibility rules.

            Example:
                Exercise TOC.read ncx toc.get attr through a consuming regression::

                    python -m pytest -q tests/file_formats/test_toc_corner_cases.py


            :param node: Value supplied for node under the utility contract.
            :param default: Value supplied for default under the utility contract.
            :param attr: Value supplied for attr under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            for name, val in node.attrib.items():
                if name and val and name.lower().endswith(attr):
                    return val
            return default

        nl_path = XPath('./*[re:match(local-name(), "navlabel$", "i")]')
        txt_path = XPath('./*[re:match(local-name(), "text$", "i")]')
        content_path = XPath('./*[re:match(local-name(), "content$", "i")]')
        np_path = XPath('./*[re:match(local-name(), "navpoint$", "i")]')

        def process_navpoint(np: _typing.Any, dest: _typing.Any) -> None:
            """
            Perform the process navpoint operation under explicit file-format and conversion rules.

            Example:
                Exercise TOC.read ncx toc.process navpoint through a consuming regression::

                    python -m pytest -q tests/file_formats/test_toc_corner_cases.py


            :param np: Value supplied for np under the utility contract.
            :param dest: Value supplied for dest under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            try:
                play_order = int(get_attr(np, 1))
            except:
                play_order = 1

            nd = dest
            nl = nl_path(np)
            if nl:
                nl = nl[0]
                text = ""
                for txt in txt_path(nl):
                    text += etree.tostring(txt, method="text", encoding=six_unicode, with_tail=False)
                text = re.sub(r"\s+", " ", text).strip()
                content = content_path(np)
                if content and text:
                    content = content[0]
                    src = _parse_href(content.get("src"))
                    if src is not None:
                        href, fragment = src
                        nd = dest.add_item(href, fragment, text)
                        nd.play_order = play_order

            for c in np_path(np):
                process_navpoint(c, nd)

        nm = XPath('//*[re:match(local-name(), "navmap$", "i")]')(root)
        if not nm:
            raise ValueError("NCX files must have a <navmap> element.")
        nm = nm[0]

        for child in np_path(nm):
            process_navpoint(child, self)

    def read_html_toc(self: _typing.Self, toc: _typing.Any) -> None:
        """
        Read html toc under the format's safety and compatibility rules.

        Example:
            Exercise TOC.read html toc through a consuming regression::

                python -m pytest -q tests/file_formats/test_toc_corner_cases.py


        :param toc: Value supplied for toc under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.base_path = os.path.dirname(toc)
        with open(toc, "rb") as f:
            raw = f.read()

        links = []
        try:
            from lxml import html as lxml_html

            root = lxml_html.fromstring(raw)
            for anchor in root.xpath("//a[@href]"):
                links.append((anchor.get("href"), " ".join(anchor.itertext())))
        except Exception:
            try:
                soup = BeautifulSoup(raw, convertEntities=BeautifulSoup.HTML_ENTITIES)
                for anchor in soup.findAll("a"):
                    if "href" in anchor:
                        links.append((anchor["href"], "".join(six_unicode(s) for s in anchor.findAll(text=True))))
            except Exception:
                logger.warning("Failed to parse HTML TOC: %s", toc, exc_info=True)
                return

        seen = {(item.href, item.fragment) for item in self.flat()}
        for href_text, anchor_text in links:
            src = _parse_href(href_text)
            if src is None:
                continue
            href, fragment = src
            txt = " ".join(anchor_text.split())
            key = (href, fragment)
            if key not in seen:
                self.add_item(href, fragment, txt)
                seen.add(key)

    def render(self: _typing.Self, stream: _typing.Any, uid: _typing.Any) -> None:
        """
        Perform the render operation under explicit file-format and conversion rules.

        Example:
            Exercise TOC.render through a consuming regression::

                python -m pytest -q tests/file_formats/test_toc_corner_cases.py


        :param stream: Input or output stream wrapped by the terminal or compatibility
            layer.
        :param uid: Value supplied for uid under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        root = E.ncx(
            E.head(
                E.meta(name="dtb:uid", content=str(uid)),
                E.meta(name="dtb:depth", content=str(self.depth())),
                E.meta(name="dtb:generator", content="%s (%s)" % (__appname__, __version__)),
                E.meta(name="dtb:totalPageCount", content="0"),
                E.meta(name="dtb:maxPageNumber", content="0"),
            ),
            E.docTitle(E.text("Table of Contents")),
        )
        navmap = E.navMap()
        root.append(navmap)
        root.set("{http://www.w3.org/XML/1998/namespace}lang", "en")
        c = Counter()

        def navpoint(parent: _typing.Any, np: _typing.Any) -> None:
            """
            Perform the navpoint operation under explicit file-format and conversion rules.

            Example:
                Exercise TOC.render.navpoint through a consuming regression::

                    python -m pytest -q tests/file_formats/test_toc_corner_cases.py


            :param parent: Value supplied for parent under the utility contract.
            :param np: Value supplied for np under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            text = np.text
            if not text:
                text = ""
            c[1] += 1
            item_id = "num_%d" % c[1]
            text = clean_xml_chars(text)
            href = six_unicode(np.href) if np.href is not None else ""
            if np.fragment:
                href += "#" + six_unicode(np.fragment)
            elem = E.navPoint(
                E.navLabel(E.text(re.sub(r"\s+", " ", text))),
                E.content(src=href),
                id=item_id,
                playOrder=str(np.play_order),
            )
            au = getattr(np, "author", None)
            if au:
                au = re.sub(r"\s+", " ", au)
                elem.append(C.meta(au, name="author"))
            desc = getattr(np, "description", None)
            if desc:
                desc = re.sub(r"\s+", " ", desc)
                elem.append(C.meta(desc, name="description"))
            idx = getattr(np, "toc_thumbnail", None)
            if idx:
                elem.append(C.meta(idx, name="toc_thumbnail"))
            parent.append(elem)
            for np2 in np:
                navpoint(elem, np2)

        for np in self:
            navpoint(navmap, np)
        raw = etree.tostring(root, encoding="utf-8", xml_declaration=True, pretty_print=True)
        stream.write(raw)

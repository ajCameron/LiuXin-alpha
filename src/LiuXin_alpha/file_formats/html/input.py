#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai
# seems to be working alright

"""
Read HTML sources and construct normalized conversion resources and metadata.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise input through a consuming regression::

        python -m pytest -q tests/file_formats/html/test_html_modernized.py
"""
from __future__ import with_statement, print_function
from __future__ import annotations

import typing as _typing

"""
Input plugin for HTML or OPF ebooks.
"""

import os
import re
import sys
import errno as gerrno

from LiuXin_alpha.constants import iswindows

from LiuXin_alpha.file_formats.oeb.base import urlunquote

from LiuXin_alpha.utils.text.xml_utils import replace_entities
from LiuXin_alpha.utils.libraries.calibre_chardet import detect_xml_encoding

# Py2/Py3 compatibility layer
from LiuXin_alpha.utils.libraries.liuxin_six import six_urlparse as urlparse
from LiuXin_alpha.utils.libraries.liuxin_six import six_urlunparse as urlunparse

unicode = str


def unicode_path(path_to_file: _typing.Any, abs: bool = False) -> _typing.Any:
    """
    Perform the unicode path operation under explicit file-format and conversion rules.

    Example:
        Exercise unicode path through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py


    :param path_to_file: Value supplied for path to file under the utility contract.
    :param abs: Value supplied for abs under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if isinstance(path_to_file, bytes):
        path = path_to_file.decode("utf-8", "replace")
    else:
        path = str(path_to_file)
    return os.path.abspath(path) if abs else path

__license__ = "GPL v3"
__copyright__ = "2009, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"


class Link(object):
    """
    Represents a link in a HTML file.

    Example:
        Exercise Link through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py
    """

    @classmethod
    def url_to_local_path(cls: type[_typing.Self], url: _typing.Any, base: _typing.Any) -> _typing.Any:
        """
        Perform the url to local path operation under explicit file-format and conversion rules.

        Example:
            Exercise Link.url to local path through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param url: Value supplied for url under the utility contract.
        :param base: Value supplied for base under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        path = url.path
        isabs = False
        if iswindows and path.startswith("/"):
            path = path[1:]
            isabs = True
        path = urlunparse(("", "", path, url.params, url.query, ""))
        path = urlunquote(path)
        if isabs or os.path.isabs(path):
            return path
        return os.path.abspath(os.path.join(base, path))

    def __init__(self: _typing.Self, url: _typing.Any, base: _typing.Any) -> None:
        """
        Initialize and validate the link state.

        Example:
            Exercise Link.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param url: Value supplied for url under the utility contract.
        :param base: Value supplied for base under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        assert isinstance(url, unicode) and isinstance(base, unicode)
        self.url = url
        self.parsed_url = urlparse(self.url)
        self.is_local = self.parsed_url.scheme in ("", "file")
        self.is_internal = self.is_local and not bool(self.parsed_url.path)
        self.path = None
        self.fragment = urlunquote(self.parsed_url.fragment)
        if self.is_local and not self.is_internal:
            self.path = self.url_to_local_path(self.parsed_url, base)

    def __hash__(self: _typing.Self) -> _typing.Any:
        """
        Perform the hash operation under explicit file-format and conversion rules.

        Example:
            Exercise Link.  hash   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self.path is None:
            return hash(self.url)
        return hash(self.path)

    def __eq__(self: _typing.Self, other: _typing.Any) -> bool:
        """
        Perform the eq operation under explicit file-format and conversion rules.

        Example:
            Exercise Link.  eq   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param other: Value supplied for other under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.path == getattr(other, "path", other)

    def __str__(self: _typing.Self) -> _typing.Any:
        """
        Perform the str operation under explicit file-format and conversion rules.

        Example:
            Exercise Link.  str   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return "Link: %s --> %s" % (self.url, self.path)


class IgnoreFile(Exception):
    """
    Provide the ignorefile contract for validated ebook processing.

    Example:
        Exercise IgnoreFile through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py
    """
    def __init__(self: _typing.Self, msg: _typing.Any, errno: _typing.Any) -> None:
        """
        Initialize and validate the ignorefile state.

        Example:
            Exercise IgnoreFile.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param msg: Value supplied for msg under the utility contract.
        :param errno: Value supplied for errno under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        Exception.__init__(self, msg)
        self.doesnt_exist = errno == gerrno.ENOENT
        self.errno = errno


class HTMLFile:

    """
    Contains basic information about an HTML file. This includes a list of links to other files as well as the encoding of each file. Also tries to detect if the file is not a HTML file in which case :member:`is_binary` is set to True.

    Example:
        Exercise HTMLFile through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py
    """

    HTML_PAT = re.compile(r"<\s*html", re.IGNORECASE)
    TITLE_PAT = re.compile("<title>([^<>]+)</title>", re.IGNORECASE)
    LINK_PAT = re.compile(
        r'<\s*a\s+.*?href\s*=\s*(?:(?:"(?P<url1>[^"]+)")|(?:\'(?P<url2>[^\']+)\')|' r"(?P<url3>[^\s>]+))",
        re.DOTALL | re.IGNORECASE,
    )

    def __init__(self: _typing.Self, path_to_html_file: _typing.Any, level: _typing.Any, encoding: _typing.Any, verbose: _typing.Any, referrer: _typing.Any = None) -> None:
        """
        Initialize and validate the htmlfile state.

        Example:
            Exercise HTMLFile.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param path_to_html_file: Value supplied for path to html file under the utility
            contract.
        :param level: Value supplied for level under the utility contract.
        :param encoding: Value supplied for encoding under the utility contract.
        :param verbose: Value supplied for verbose under the utility contract.
        :param referrer: Value supplied for referrer under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.path = unicode_path(path_to_html_file, abs=True)
        self.title = os.path.splitext(os.path.basename(self.path))[0]
        self.base = os.path.dirname(self.path)
        self.level = level
        self.referrer = referrer
        self.links = []

        try:
            with open(self.path, "rb") as f:
                src = header = f.read(4096)
                encoding = detect_xml_encoding(src)[1]
                if encoding:
                    try:
                        header = header.decode(encoding)
                    except (LookupError, ValueError):
                        pass
                self.is_binary = level > 0 and not bool(self.HTML_PAT.search(header))
                if not self.is_binary:
                    src += f.read()
        except IOError as err:
            msg = "Could not read from file: %s with error: %s" % (
                self.path,
                str(err),
            )
            if level == 0:
                raise IOError(msg)
            raise IgnoreFile(msg, err.errno)

        if not src:
            if level == 0:
                raise ValueError("The file %s is empty" % self.path)
            self.is_binary = True

        if not self.is_binary:
            if not encoding:
                encoding = detect_xml_encoding(src[:4096], verbose=verbose)[1]
            # Fall back to UTF-8 if no declaration/guess is available.
            self.encoding = encoding or "utf-8"

            src = src.decode(self.encoding, "replace")
            match = self.TITLE_PAT.search(src)
            self.title = match.group(1) if match is not None else self.title
            self.find_links(src)

    def __eq__(self: _typing.Self, other: _typing.Any) -> bool:
        """
        Perform the eq operation under explicit file-format and conversion rules.

        Example:
            Exercise HTMLFile.  eq   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param other: Value supplied for other under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.path == getattr(other, "path", other)

    def __hash__(self: _typing.Self) -> _typing.Any:
        """
        Perform the hash operation under explicit file-format and conversion rules.

        Example:
            Exercise HTMLFile.  hash   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return hash(self.path)

    def __str__(self: _typing.Self) -> _typing.Any:
        """
        Perform the str operation under explicit file-format and conversion rules.

        Example:
            Exercise HTMLFile.  str   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return "HTMLFile:%d:%s:%s" % (
            self.level,
            "b" if self.is_binary else "a",
            self.path,
        )

    def __repr__(self: _typing.Self) -> _typing.Any:
        """
        Perform the repr operation under explicit file-format and conversion rules.

        Example:
            Exercise HTMLFile.  repr   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return str(self)

    def find_links(self: _typing.Self, src: _typing.Any) -> None:
        """
        Find links under the format's safety and compatibility rules.

        Example:
            Exercise HTMLFile.find links through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param src: Value supplied for src under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for match in self.LINK_PAT.finditer(src):
            url = None
            for i in ("url1", "url2", "url3"):
                url = match.group(i)
                if url:
                    break
            url = replace_entities(url)
            try:
                link = self.resolve(url)
            except ValueError:
                # Unparseable URL, ignore
                continue
            if link not in self.links:
                self.links.append(link)

    def resolve(self: _typing.Self, url: _typing.Any) -> _typing.Any:
        """
        Perform the resolve operation under explicit file-format and conversion rules.

        Example:
            Exercise HTMLFile.resolve through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param url: Value supplied for url under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return Link(url, self.base)


def depth_first(root: _typing.Any, flat: _typing.Any, visited: _typing.Any = None) -> _typing.Iterator[_typing.Any]:
    """
    Perform the depth first operation under explicit file-format and conversion rules.

    Example:
        Exercise depth first through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py


    :param root: Root directory that bounds path resolution or traversal.
    :param flat: Value supplied for flat under the utility contract.
    :param visited: Value supplied for visited under the utility contract.
    :return: An iterator yielding the normalized values described above.
    """
    if visited is None:
        visited = set()
    yield root
    visited.add(root)
    for link in root.links:
        if link.path is not None and link not in visited:
            try:
                index = flat.index(link)
            except ValueError:  # Can happen if max_levels is used
                continue
            hf = flat[index]
            if hf not in visited:
                yield hf
                visited.add(hf)
                for hf in depth_first(hf, flat, visited):
                    if hf not in visited:
                        yield hf
                        visited.add(hf)


def traverse(path_to_html_file: str, max_levels: int = sys.maxsize, verbose: int = 0, encoding: str = None) -> tuple[_typing.Any, ...]:
    """
    Recursively traverse all links in the HTML file.

    Example:
        Exercise traverse through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py


    :param path_to_html_file: Value supplied for path to html file under the utility
        contract.
    :param max_levels: Value supplied for max levels under the utility contract.
    :param verbose: Value supplied for verbose under the utility contract.
    :param encoding: Value supplied for encoding under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    assert max_levels >= 0
    level = 0
    flat = [HTMLFile(path_to_html_file, level, encoding, verbose)]
    next_level = list(flat)
    while level < max_levels and len(next_level) > 0:
        level += 1
        nl = []
        for hf in next_level:
            rejects = []
            for link in hf.links:
                if link.path is None or link.path in flat:
                    continue
                try:
                    nf = HTMLFile(link.path, level, encoding, verbose, referrer=hf)
                    if nf.is_binary:
                        raise IgnoreFile("%s is a binary file" % nf.path, -1)
                    nl.append(nf)
                    flat.append(nf)
                except IgnoreFile as err:
                    rejects.append(link)
                    if not err.doesnt_exist or verbose > 1:
                        print(repr(err))
            for link in rejects:
                hf.links.remove(link)

        next_level = list(nl)
    orec = sys.getrecursionlimit()
    sys.setrecursionlimit(500000)
    try:
        return flat, list(depth_first(flat[0], flat))
    finally:
        sys.setrecursionlimit(orec)


def get_filelist(htmlfile: _typing.Any, dir: _typing.Any, opts: _typing.Any, log: _typing.Any) -> _typing.Any:
    """
    Build list of files referenced by html file or try to detect and use an OPF file instead.

    Example:
        Exercise get filelist through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py


    :param htmlfile: Value supplied for htmlfile under the utility contract.
    :param dir: Value supplied for dir under the utility contract.
    :param opts: Value supplied for opts under the utility contract.
    :param log: Value supplied for log under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    log.info("Building file list...")
    filelist = traverse(
        htmlfile,
        max_levels=int(opts.max_levels),
        verbose=opts.verbose,
        encoding=opts.input_encoding,
    )[0 if opts.breadth_first else 1]
    if opts.verbose:
        log.debug("\tFound files...")
        for f in filelist:
            log.debug("\t\t", f)
    return filelist

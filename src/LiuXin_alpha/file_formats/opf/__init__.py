#!/usr/bin/env  python

"""
Expose the supported opf compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py
"""

# Why is this here, and not in metadata?
# While OPF is almost exclusively used to store metadata it's a file format and this is the logic to read/write from it

# Todo: Fix that horrible mess - import errors can be fixed in other ways than duplicating a huge chunk of code

from __future__ import annotations, print_function

import copy
import functools
import glob
import json
import os
import re
import sys
import typing as _typing
import unittest
import uuid
from urllib.parse import unquote, urlparse

from LiuXin_alpha.constants import __appname__, __version__, filesystem_encoding
from LiuXin_alpha.file_formats.toc import TOC

# Todo: Probably gets merged into utils right?
from LiuXin_alpha.metadata.ebook_metadata_tools import check_isbn
from LiuXin_alpha.metadata.utils import calibreMetaInformation as MetaInformation
from LiuXin_alpha.metadata.utils import string_to_authors
from LiuXin_alpha.preferences import preferences as tweaks
from LiuXin_alpha.utils.calibre_compat.ebooks.metadata.book.base import (
    Metadata as Metadata,
)
from LiuXin_alpha.utils.date import isoformat, parse_date
from LiuXin_alpha.utils.language_tools.icu import lower as icu_lower
from LiuXin_alpha.utils.language_tools.icu import upper as icu_upper
from LiuXin_alpha.utils.libraries.calibre_chardet import xml_to_unicode
from LiuXin_alpha.utils.libraries.cleantext import clean_ascii_chars, clean_xml_chars
from LiuXin_alpha.utils.libraries.iso639.iso639_tools import canonicalize_lang
from LiuXin_alpha.utils.libraries.liuxin_etree import ElementMaker, etree
from LiuXin_alpha.utils.libraries.liuxin_six import six_cStringIO, six_unicode
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode as unicode
from LiuXin_alpha.utils.localization import get_lang
from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.logging import prints
from LiuXin_alpha.utils.mine_types import guess_type

__license__ = "GPL v3"
__copyright__ = "2008, Kovid Goyal kovid@kovidgoyal.net"
__docformat__ = "restructuredtext en"

pretty_print_opf = False


class PrettyPrint(object):
    """
    Provide the prettyprint contract for validated ebook processing.

    Example:
        Exercise PrettyPrint through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py
    """
    def __enter__(self: _typing.Self) -> None:
        """
        Implement the conversion resource's enter lifecycle operation.

        Example:
            Exercise PrettyPrint.  enter   through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        global pretty_print_opf
        pretty_print_opf = True

    def __exit__(self: _typing.Self, *args: _typing.Any) -> None:
        """
        Implement the conversion resource's exit lifecycle operation.

        Example:
            Exercise PrettyPrint.  exit   through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param args: Positional values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        global pretty_print_opf
        pretty_print_opf = False


pretty_print = PrettyPrint()


def _pretty_print(root: _typing.Any) -> None:
    """
    Perform the pretty print operation under explicit file-format and conversion rules.

    Example:
        Exercise  pretty print through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


    :param root: Root directory that bounds path resolution or traversal.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.oeb.polish.pretty import pretty_opf, pretty_xml_tree

    pretty_opf(root)
    pretty_xml_tree(root)


class Resource(object):  # {{{
    """
    Represents a resource (usually a file on the filesystem or a URL pointing to the web. Such resources are commonly referred to in OPF files.

    Example:
        Exercise Resource through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py
    """

    def __init__(self: _typing.Self, href_or_path: _typing.Any, basedir: _typing.Any = os.getcwd(), is_path: bool = True) -> None:
        """
        Initialize and validate the resource state.

        Example:
            Exercise Resource.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param href_or_path: Value supplied for href or path under the utility contract.
        :param basedir: Value supplied for basedir under the utility contract.
        :param is_path: Value supplied for is path under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.orig = href_or_path
        self._href = None
        self._basedir = basedir
        self.path = None
        self.fragment = ""
        try:
            self.mime_type = guess_type(href_or_path)[0]
        except:
            self.mime_type = None
        if self.mime_type is None:
            self.mime_type = "application/octet-stream"
        if is_path:
            path = href_or_path
            if not os.path.isabs(path):
                path = os.path.abspath(os.path.join(basedir, path))
            if isinstance(path, str):
                path = path
                # path = path.decode(sys.getfilesystemencoding())
            self.path = path
        else:
            href_or_path = href_or_path
            url = urlparse(href_or_path)
            if url[0] not in ("", "file"):
                self._href = href_or_path
            else:
                pc = url[2]
                if isinstance(pc, unicode):
                    pc = pc.encode("utf-8")
                pc = pc.decode("utf-8")
                self.path = os.path.abspath(os.path.join(basedir, pc.replace("/", os.sep)))
                self.fragment = url[-1]

    def href(self: _typing.Self, basedir: _typing.Any = None) -> _typing.Any:
        """
        Return a URL pointing to this resource. If it is a file on the filesystem the URL is relative to `basedir`.

        Example:
            Exercise Resource.href through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param basedir: Value supplied for basedir under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if basedir is None:
            if self._basedir:
                basedir = self._basedir
            else:
                basedir = os.getcwdu()
        if self.path is None:
            return self._href
        f = self.fragment.encode("utf-8") if isinstance(self.fragment, unicode) else self.fragment
        frag = "#" + f if self.fragment else ""
        if self.path == basedir:
            return "" + frag
        try:
            rpath = os.path.relpath(self.path, basedir)
        except ValueError:  # On windows path and basedir could be on different drives
            rpath = self.path
        if isinstance(rpath, unicode):
            rpath = rpath.encode("utf-8")
        return rpath.replace(os.sep, "/") + frag

    def set_basedir(self: _typing.Self, path: _typing.Any) -> None:
        """
        Set basedir under the format's safety and compatibility rules.

        Example:
            Exercise Resource.set basedir through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param path: Filesystem path read, written, normalized or validated by the
            operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._basedir = path

    def basedir(self: _typing.Self) -> _typing.Any:
        """
        Perform the basedir operation under explicit file-format and conversion rules.

        Example:
            Exercise Resource.basedir through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self._basedir

    def __repr__(self: _typing.Self) -> _typing.Any:
        """
        Perform the repr operation under explicit file-format and conversion rules.

        Example:
            Exercise Resource.  repr   through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return "Resource(%s, %s)" % (repr(self.path), repr(self.href()))


# }}}


class ResourceCollection(object):  # {{{
    """
    Provide the resourcecollection contract for validated ebook processing.

    Example:
        Exercise ResourceCollection through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py
    """
    def __init__(self: _typing.Self) -> None:
        """
        Initialize and validate the resourcecollection state.

        Example:
            Exercise ResourceCollection.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :return: None; validated state is stored on the receiving object.
        """
        self._resources = []

    def __iter__(self: _typing.Self) -> _typing.Iterator[_typing.Any]:
        """
        Perform the iter operation under explicit file-format and conversion rules.

        Example:
            Exercise ResourceCollection.  iter   through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :return: An iterator yielding the normalized values described above.
        """
        for r in self._resources:
            yield r

    def __len__(self: _typing.Self) -> _typing.Any:
        """
        Perform the len operation under explicit file-format and conversion rules.

        Example:
            Exercise ResourceCollection.  len   through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return len(self._resources)

    def __getitem__(self: _typing.Self, index: _typing.Any) -> _typing.Any:
        """
        Perform the getitem operation under explicit file-format and conversion rules.

        Example:
            Exercise ResourceCollection.  getitem   through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param index: Value supplied for index under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self._resources[index]

    def __bool__(self: _typing.Self) -> bool:
        """
        Perform the bool operation under explicit file-format and conversion rules.

        Example:
            Exercise ResourceCollection.  bool   through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :return: True when the documented condition holds; otherwise False.
        """
        return len(self._resources) > 0

    def __str__(self: _typing.Self) -> _typing.Any:
        """
        Perform the str operation under explicit file-format and conversion rules.

        Example:
            Exercise ResourceCollection.  str   through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        resources = map(repr, self)
        return "[%s]" % ", ".join(resources)

    def __repr__(self: _typing.Self) -> _typing.Any:
        """
        Perform the repr operation under explicit file-format and conversion rules.

        Example:
            Exercise ResourceCollection.  repr   through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return str(self)

    def append(self: _typing.Self, resource: _typing.Any) -> None:
        """
        Perform the append operation under explicit file-format and conversion rules.

        Example:
            Exercise ResourceCollection.append through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param resource: Value supplied for resource under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if not isinstance(resource, Resource):
            raise ValueError("Can only append objects of type Resource")
        self._resources.append(resource)

    def remove(self: _typing.Self, resource: _typing.Any) -> None:
        """
        Perform the remove operation under explicit file-format and conversion rules.

        Example:
            Exercise ResourceCollection.remove through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param resource: Value supplied for resource under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._resources.remove(resource)

    def replace(self: _typing.Self, start: _typing.Any, end: _typing.Any, items: _typing.Any) -> None:
        """
        Same as list[start:end] = items

        Example:
            Exercise ResourceCollection.replace through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param start: Value supplied for start under the utility contract.
        :param end: Value supplied for end under the utility contract.
        :param items: Value supplied for items under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._resources[start:end] = items

    def from_directory_contents(top: _typing.Any, topdown: bool = True) -> _typing.Any:
        """
        Perform the from directory contents operation under explicit file-format and conversion rules.

        Example:
            Exercise ResourceCollection.from directory contents through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param topdown: Value supplied for topdown under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        collection = ResourceCollection()
        for spec in os.walk(top, topdown=topdown):
            path = os.path.abspath(os.path.join(spec[0], spec[1]))
            res = Resource.from_path(path)
            res.set_basedir(top)
            collection.append(res)
        return collection

    def set_basedir(self: _typing.Self, path: _typing.Any) -> None:
        """
        Set basedir under the format's safety and compatibility rules.

        Example:
            Exercise ResourceCollection.set basedir through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param path: Filesystem path read, written, normalized or validated by the
            operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for res in self:
            res.set_basedir(path)


# }}}


class ManifestItem(Resource):  # {{{
    """
    Provide the manifestitem contract for validated ebook processing.

    Example:
        Exercise ManifestItem through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py
    """
    @staticmethod
    def from_opf_manifest_item(item: _typing.Any, basedir: _typing.Any) -> _typing.Any:
        """
        Perform the from opf manifest item operation under explicit file-format and conversion rules.

        Example:
            Exercise ManifestItem.from opf manifest item through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param item: Value supplied for item under the utility contract.
        :param basedir: Value supplied for basedir under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        href = item.get("href", None)
        if href:
            res = ManifestItem(href, basedir=basedir, is_path=True)
            mt = item.get("media-type", "").strip()
            if mt:
                res.mime_type = mt
            return res

    @property
    def media_type(self: _typing.Self) -> _typing.Any:
        """
        Perform the media type operation under explicit file-format and conversion rules.

        Example:
            Exercise ManifestItem.media type through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.mime_type

    @media_type.setter
    def media_type(self: _typing.Self, val: _typing.Any) -> None:
        """
        Perform the media type operation under explicit file-format and conversion rules.

        Example:
            Exercise ManifestItem.media type through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param val: Template or metadata value evaluated by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.mime_type = val

    def __unicode__(self: _typing.Self) -> _typing.Any:
        """
        Perform the unicode operation under explicit file-format and conversion rules.

        Example:
            Exercise ManifestItem.  unicode   through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return '<item id="%s" href="%s" media-type="%s" />' % (
            self.id,
            self.href(),
            self.media_type,
        )

    def __str__(self: _typing.Self) -> _typing.Any:
        """
        Perform the str operation under explicit file-format and conversion rules.

        Example:
            Exercise ManifestItem.  str   through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return six_unicode(self).encode("utf-8")

    def __repr__(self: _typing.Self) -> _typing.Any:
        """
        Perform the repr operation under explicit file-format and conversion rules.

        Example:
            Exercise ManifestItem.  repr   through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return six_unicode(self)

    def __getitem__(self: _typing.Self, index: _typing.Any) -> _typing.Any:
        """
        Perform the getitem operation under explicit file-format and conversion rules.

        Example:
            Exercise ManifestItem.  getitem   through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param index: Value supplied for index under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if index == 0:
            return self.href()
        if index == 1:
            return self.media_type
        raise IndexError("%d out of bounds." % index)


# }}}


# Todo: Prefer these when doing the merge - have actually been (lightly) touched
class Manifest(ResourceCollection):  # {{{
    """
    Provide the manifest contract for validated ebook processing.

    Example:
        Exercise Manifest through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py
    """
    @staticmethod
    def from_opf_manifest_element(items: _typing.Any, dir: _typing.Any) -> _typing.Any:
        """
        Perform the from opf manifest element operation under explicit file-format and conversion rules.

        Example:
            Exercise Manifest.from opf manifest element through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param items: Value supplied for items under the utility contract.
        :param dir: Value supplied for dir under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        m = Manifest()
        for item in items:
            try:
                m.append(ManifestItem.from_opf_manifest_item(item, dir))
                item_id = item.get("id", "")
                if not item_id:
                    item_id = "id%d" % m.next_id
                m[-1].id = item_id
                m.next_id += 1
            except ValueError:
                continue
        return m

    @staticmethod
    def from_paths(entries: _typing.Any) -> _typing.Any:
        """
        Build a Manifest from a given list of paths,

        Example:
            Exercise Manifest.from paths through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param entries: Value supplied for entries under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        m = Manifest()
        for path, mt in entries:
            mi = ManifestItem(path, is_path=True)
            if mt:
                mi.mime_type = mt
            mi.id = "id%d" % m.next_id
            m.next_id += 1
            m.append(mi)
        return m

    def add_item(self: _typing.Self, path: _typing.Any, mime_type: _typing.Any = None) -> _typing.Any:
        """
        Perform the add item operation under explicit file-format and conversion rules.

        Example:
            Exercise Manifest.add item through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param path: Filesystem path read, written, normalized or validated by the
            operation.
        :param mime_type: Value supplied for mime type under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        mi = ManifestItem(path, is_path=True)
        if mime_type:
            mi.mime_type = mime_type
        mi.id = "id%d" % self.next_id
        self.next_id += 1
        self.append(mi)
        return mi.id

    def __init__(self: _typing.Self) -> None:
        """
        Initialize and validate the manifest state.

        Example:
            Exercise Manifest.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :return: None; validated state is stored on the receiving object.
        """
        ResourceCollection.__init__(self)
        self.next_id = 1

    def item(self: _typing.Self, id: _typing.Any) -> _typing.Any:
        """
        Perform the item operation under explicit file-format and conversion rules.

        Example:
            Exercise Manifest.item through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param id: Value supplied for id under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        for i in self:
            if i.id == id:
                return i

    def id_for_path(self: _typing.Self, path: _typing.Any) -> _typing.Any:
        """
        Perform the id for path operation under explicit file-format and conversion rules.

        Example:
            Exercise Manifest.id for path through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param path: Filesystem path read, written, normalized or validated by the
            operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        path = os.path.normpath(os.path.abspath(path))
        for i in self:
            if i.path and os.path.normpath(i.path) == path:
                return i.id

    def path_for_id(self: _typing.Self, id: _typing.Any) -> _typing.Any:
        """
        Perform the path for id operation under explicit file-format and conversion rules.

        Example:
            Exercise Manifest.path for id through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param id: Value supplied for id under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        for i in self:
            if i.id == id:
                return i.path

    def type_for_id(self: _typing.Self, id: _typing.Any) -> _typing.Any:
        """
        Perform the type for id operation under explicit file-format and conversion rules.

        Example:
            Exercise Manifest.type for id through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param id: Value supplied for id under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        for i in self:
            if i.id == id:
                return i.mime_type


# }}}


class Spine(ResourceCollection):  # {{{
    """
    Forms the spine of an ebook - the thing that binds all the other elements together.

    Example:
        Exercise Spine through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py
    """

    class Item(Resource):
        """
        Provide the item contract for validated ebook processing.

        Example:
            Exercise Spine.Item through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py
        """
        def __init__(self: _typing.Self, idfunc: _typing.Any, *args: _typing.Any, **kwargs: _typing.Any) -> None:
            """
            Initialize and validate the item state.

            Example:
                Exercise Spine.Item.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


            :param idfunc: Value supplied for idfunc under the utility contract.
            :param args: Positional values forwarded to the compatibility implementation.
            :param kwargs: Keyword values forwarded to the compatibility implementation.
            :return: None; validated state is stored on the receiving object.
            """
            Resource.__init__(self, *args, **kwargs)
            self.is_linear = True
            self.id = idfunc(self.path)
            self.idref = None

        def __repr__(self: _typing.Self) -> _typing.Any:
            """
            Perform the repr operation under explicit file-format and conversion rules.

            Example:
                Exercise Spine.Item.  repr   through a consuming regression::

                    python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return "Spine.Item(path=%r, id=%s, is_linear=%s)" % (
                self.path,
                self.id,
                self.is_linear,
            )

    @staticmethod
    def from_opf_spine_element(itemrefs: _typing.Any, manifest: _typing.Any) -> _typing.Any:
        """
        Perform the from opf spine element operation under explicit file-format and conversion rules.

        Example:
            Exercise Spine.from opf spine element through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param itemrefs: Value supplied for itemrefs under the utility contract.
        :param manifest: Value supplied for manifest under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        s = Spine(manifest)
        seen = set()
        for itemref in itemrefs:
            idref = itemref.get("idref", None)
            if idref is not None:
                path = s.manifest.path_for_id(idref)
                if path and path not in seen:
                    r = Spine.Item(lambda x: idref, path, is_path=True)
                    r.is_linear = itemref.get("linear", "yes") == "yes"
                    r.idref = idref
                    s.append(r)
                    seen.add(path)
        return s

    # Todo: Check (same with the other static methods)
    @staticmethod
    def from_paths(paths: _typing.Any, manifest: _typing.Any) -> _typing.Any:
        """
        Perform the from paths operation under explicit file-format and conversion rules.

        Example:
            Exercise Spine.from paths through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param paths: Value supplied for paths under the utility contract.
        :param manifest: Value supplied for manifest under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        s = Spine(manifest)
        for path in paths:
            try:
                s.append(Spine.Item(s.manifest.id_for_path, path, is_path=True))
            except:
                continue
        return s

    def __init__(self: _typing.Self, manifest: _typing.Any) -> None:
        """
        Initialize and validate the spine state.

        Example:
            Exercise Spine.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param manifest: Value supplied for manifest under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        ResourceCollection.__init__(self)
        self.manifest = manifest

    def replace(self: _typing.Self, start: _typing.Any, end: _typing.Any, ids: _typing.Any) -> None:
        """
        Replace the items between start (inclusive) and end (not inclusive) with the items identified by ids. ids can be a list of any length.

        Example:
            Exercise Spine.replace through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param start: Value supplied for start under the utility contract.
        :param end: Value supplied for end under the utility contract.
        :param ids: Value supplied for ids under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        items = []
        for id in ids:
            path = self.manifest.path_for_id(id)
            if path is None:
                raise ValueError("id %s not in manifest")
            items.append(Spine.Item(lambda x: id, path, is_path=True))
        ResourceCollection.replace(start, end, items)

    def linear_items(self: _typing.Self) -> _typing.Iterator[_typing.Any]:
        """
        Perform the linear items operation under explicit file-format and conversion rules.

        Example:
            Exercise Spine.linear items through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :return: An iterator yielding the normalized values described above.
        """
        for r in self:
            if r.is_linear:
                yield r.path

    def nonlinear_items(self: _typing.Self) -> _typing.Iterator[_typing.Any]:
        """
        Perform the nonlinear items operation under explicit file-format and conversion rules.

        Example:
            Exercise Spine.nonlinear items through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :return: An iterator yielding the normalized values described above.
        """
        for r in self:
            if not r.is_linear:
                yield r.path

    def items(self: _typing.Self) -> _typing.Iterator[_typing.Any]:
        """
        Perform the items operation under explicit file-format and conversion rules.

        Example:
            Exercise Spine.items through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :return: An iterator yielding the normalized values described above.
        """
        for i in self:
            yield i.path


# }}}


class Guide(ResourceCollection):  # {{{
    """
    Provide the guide contract for validated ebook processing.

    Example:
        Exercise Guide through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py
    """
    class Reference(Resource):
        """
        Provide the reference contract for validated ebook processing.

        Example:
            Exercise Guide.Reference through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py
        """
        def from_opf_resource_item(ref: _typing.Any, basedir: _typing.Any) -> _typing.Any:
            """
            Perform the from opf resource item operation under explicit file-format and conversion rules.

            Example:
                Exercise Guide.Reference.from opf resource item through a consuming regression::

                    python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


            :param basedir: Value supplied for basedir under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            title, href, type = ref.get("title", ""), ref.get("href"), ref.get("type")
            res = Guide.Reference(href, basedir, is_path=True)
            res.title = title
            res.type = type
            return res

        def __repr__(self: _typing.Self) -> _typing.Any:
            """
            Perform the repr operation under explicit file-format and conversion rules.

            Example:
                Exercise Guide.Reference.  repr   through a consuming regression::

                    python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            ans = '<reference type="%s" href="%s" ' % (self.type, self.href())
            if self.title:
                ans += 'title="%s" ' % self.title
            return ans + "/>"

    @staticmethod
    def from_opf_guide(references: _typing.Any, base_dir: _typing.Any = os.getcwd()) -> _typing.Any:
        """
        Perform the from opf guide operation under explicit file-format and conversion rules.

        Example:
            Exercise Guide.from opf guide through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param references: Value supplied for references under the utility contract.
        :param base_dir: Value supplied for base dir under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        coll = Guide()
        for ref in references:
            try:
                ref = Guide.Reference.from_opf_resource_item(ref, base_dir)
                coll.append(ref)
            except:
                continue
        return coll

    def set_cover(self: _typing.Self, path: _typing.Any) -> None:
        """
        Replace the container's cover while preserving required package references.

        Example:
            Exercise Guide.set cover through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param path: Filesystem path read, written, normalized or validated by the
            operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        map(self.remove, [i for i in self if "cover" in i.type.lower()])
        for opf_type in ("cover", "other.ms-coverimage-standard", "other.ms-coverimage"):
            self.append(Guide.Reference(path, is_path=True))
            self[-1].type = opf_type
            self[-1].title = ""


# }}}


class MetadataField(object):
    """
    Provide the metadatafield contract for validated ebook processing.

    Example:
        Exercise MetadataField through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py
    """
    def __init__(
        self: _typing.Self,
        name: _typing.Any,
        is_dc: bool = True,
        formatter: _typing.Any = None,
        none_is: _typing.Any = None,
        renderer: _typing.Callable[..., _typing.Any] = lambda x: six_unicode(x),
    ) -> None:
        """
        Initialize and validate the metadatafield state.

        Example:
            Exercise MetadataField.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param is_dc: Value supplied for is dc under the utility contract.
        :param formatter: Template formatter supplying evaluation services and context.
        :param none_is: Value supplied for none is under the utility contract.
        :param renderer: Value supplied for renderer under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.name = name
        self.is_dc = is_dc
        self.formatter = formatter
        self.none_is = none_is
        self.renderer = renderer

    def __real_get__(self: _typing.Self, obj: _typing.Any, type: _typing.Any = None) -> _typing.Any:
        """
        Perform the real get operation under explicit file-format and conversion rules.

        Example:
            Exercise MetadataField.  real get   through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param obj: Value supplied for obj under the utility contract.
        :param type: Value supplied for type under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ans = obj.get_metadata_element(self.name)
        if ans is None:
            return None
        ans = obj.get_text(ans)
        if ans is None:
            return ans
        if self.formatter is not None:
            try:
                ans = self.formatter(ans)
            except:
                return None
        if hasattr(ans, "strip"):
            ans = ans.strip()
        return ans

    def __get__(self: _typing.Self, obj: _typing.Any, type: _typing.Any = None) -> _typing.Any:
        """
        Perform the get operation under explicit file-format and conversion rules.

        Example:
            Exercise MetadataField.  get   through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param obj: Value supplied for obj under the utility contract.
        :param type: Value supplied for type under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ans = self.__real_get__(obj, type)
        if ans is None:
            ans = self.none_is
        return ans

    def __set__(self: _typing.Self, obj: _typing.Any, val: _typing.Any) -> None:
        """
        Perform the set operation under explicit file-format and conversion rules.

        Example:
            Exercise MetadataField.  set   through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param obj: Value supplied for obj under the utility contract.
        :param val: Template or metadata value evaluated by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        elem = obj.get_metadata_element(self.name)
        if val is None:
            if elem is not None:
                elem.getparent().remove(elem)
            return
        if elem is None:
            elem = obj.create_metadata_element(self.name, is_dc=self.is_dc)
        obj.set_text(elem, self.renderer(val))


class TitleSortField(MetadataField):
    """
    Provide the titlesortfield contract for validated ebook processing.

    Example:
        Exercise TitleSortField through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py
    """
    def __get__(self: _typing.Self, obj: _typing.Any, type: _typing.Any = None) -> _typing.Any:
        """
        Perform the get operation under explicit file-format and conversion rules.

        Example:
            Exercise TitleSortField.  get   through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param obj: Value supplied for obj under the utility contract.
        :param type: Value supplied for type under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        c = self.__real_get__(obj, type)
        if c is None:
            matches = obj.title_path(obj.metadata)
            if matches:
                for match in matches:
                    ans = match.get("{%s}file-as" % obj.NAMESPACES["opf"], None)
                    if not ans:
                        ans = match.get("file-as", None)
                    if ans:
                        c = ans
        if not c:
            c = self.none_is
        else:
            c = c.strip()
        return c

    def __set__(self: _typing.Self, obj: _typing.Any, val: _typing.Any) -> None:
        """
        Perform the set operation under explicit file-format and conversion rules.

        Example:
            Exercise TitleSortField.  set   through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param obj: Value supplied for obj under the utility contract.
        :param val: Template or metadata value evaluated by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        MetadataField.__set__(self, obj, val)
        matches = obj.title_path(obj.metadata)
        if matches:
            for match in matches:
                for attr in list(match.attrib):
                    if attr.endswith("file-as"):
                        del match.attrib[attr]


def serialize_user_metadata(metadata_elem: _typing.Any, all_user_metadata: _typing.Any, tail: _typing.Any = "\n" + (" " * 8)) -> None:
    """
    Write user metadata.

    Example:
        Exercise serialize user metadata through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


    :param metadata_elem: Value supplied for metadata elem under the utility contract.
    :param all_user_metadata: Value supplied for all user metadata under the utility
        contract.
    :param tail: Value supplied for tail under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.metadata.book.json_codec import (
        encode_is_multiple,
        object_to_unicode,
    )
    from LiuXin_alpha.utils.config.config_tools import to_json

    for name, fm in all_user_metadata.items():
        try:
            fm = copy.copy(fm)
            encode_is_multiple(fm)
            fm = object_to_unicode(fm)
            fm = json.dumps(fm, default=to_json, ensure_ascii=False)
        except:
            prints("Failed to write user metadata:", name)
            import traceback

            traceback.print_exc()
            continue
        meta = metadata_elem.makeelement("meta")
        meta.set("name", "calibre:user_metadata:" + name)
        meta.set("content", fm)
        meta.tail = tail
        metadata_elem.append(meta)


def dump_dict(cats: _typing.Any) -> _typing.Any:
    """
    Perform the dump dict operation under explicit file-format and conversion rules.

    Example:
        Exercise dump dict through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


    :param cats: Value supplied for cats under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if not cats:
        cats = {}
    from LiuXin_alpha.metadata.book.json_codec import object_to_unicode

    return json.dumps(object_to_unicode(cats), ensure_ascii=False, skipkeys=True)


# Todo: Think this object is probably a duplciate - merge with the other one


class OPF(object):  # {{{

    """
    Provide the opf contract for validated ebook processing.

    Example:
        Exercise OPF through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py
    """
    MIMETYPE = "application/oebps-package+xml"
    PARSER = etree.XMLParser(recover=True)
    NAMESPACES = {
        None: "http://www.idpf.org/2007/opf",
        "dc": "http://purl.org/dc/elements/1.1/",
        "opf": "http://www.idpf.org/2007/opf",
    }
    META = "{%s}meta" % NAMESPACES["opf"]
    xpn = NAMESPACES.copy()
    xpn.pop(None)
    xpn["re"] = "http://exslt.org/regular-expressions"
    XPath = functools.partial(etree.XPath, namespaces=xpn)
    CONTENT = XPath('self::*[re:match(name(), "meta$", "i")]/@content')
    TEXT = XPath("string()")

    metadata_path = XPath('descendant::*[re:match(name(), "metadata", "i")]')
    metadata_elem_path = XPath(
        'descendant::*[re:match(name(), concat($name, "$"), "i") or (re:match(name(), "meta$", "i") '
        'and re:match(@name, concat("^calibre:", $name, "$"), "i"))]'
    )
    title_path = XPath('descendant::*[re:match(name(), "title", "i")]')
    authors_path = XPath(
        'descendant::*[re:match(name(), "creator", "i") and (@role="aut" or @opf:role="aut" or (not(@role) and not(@opf:role)))]'
    )
    bkp_path = XPath('descendant::*[re:match(name(), "contributor", "i") and (@role="bkp" or @opf:role="bkp")]')
    tags_path = XPath('descendant::*[re:match(name(), "subject", "i")]')
    isbn_path = XPath(
        'descendant::*[re:match(name(), "identifier", "i") and '
        + '(re:match(@scheme, "isbn", "i") or re:match(@opf:scheme, "isbn", "i"))]'
    )
    pubdate_path = XPath('descendant::*[re:match(name(), "date", "i")]')
    raster_cover_path = XPath(
        'descendant::*[re:match(name(), "meta", "i") and ' + 're:match(@name, "cover", "i") and @content]'
    )
    identifier_path = XPath('descendant::*[re:match(name(), "identifier", "i")]')
    application_id_path = XPath(
        'descendant::*[re:match(name(), "identifier", "i") and '
        + '(re:match(@opf:scheme, "calibre|libprs500", "i") or re:match(@scheme, "calibre|libprs500", "i"))]'
    )
    uuid_id_path = XPath(
        'descendant::*[re:match(name(), "identifier", "i") and '
        + '(re:match(@opf:scheme, "uuid", "i") or re:match(@scheme, "uuid", "i"))]'
    )
    languages_path = XPath('descendant::*[local-name()="language"]')

    manifest_path = XPath('descendant::*[re:match(name(), "manifest", "i")]/*[re:match(name(), "item", "i")]')
    manifest_ppath = XPath('descendant::*[re:match(name(), "manifest", "i")]')
    spine_path = XPath('descendant::*[re:match(name(), "spine", "i")]/*[re:match(name(), "itemref", "i")]')
    guide_path = XPath('descendant::*[re:match(name(), "guide", "i")]/*[re:match(name(), "reference", "i")]')

    publisher = MetadataField("publisher")
    comments = MetadataField("description")
    category = MetadataField("type")
    rights = MetadataField("rights")
    series = MetadataField("series", is_dc=False)
    if tweaks["use_series_auto_increment_tweak_when_importing"]:
        series_index = MetadataField("series_index", is_dc=False, formatter=float, none_is=None)
    else:
        series_index = MetadataField("series_index", is_dc=False, formatter=float, none_is=1)
    title_sort = TitleSortField("title_sort", is_dc=False)
    rating = MetadataField("rating", is_dc=False, formatter=float)
    publication_type = MetadataField("publication_type", is_dc=False)
    timestamp = MetadataField("timestamp", is_dc=False, formatter=parse_date, renderer=isoformat)
    user_categories = MetadataField("user_categories", is_dc=False, formatter=json.loads, renderer=dump_dict)
    author_link_map = MetadataField("author_link_map", is_dc=False, formatter=json.loads, renderer=dump_dict)

    def __init__(
        self: _typing.Self,
        stream: _typing.Any,
        basedir: _typing.Any = os.getcwd(),
        unquote_urls: bool = True,
        populate_spine: bool = True,
        try_to_guess_cover: bool = True,
    ) -> None:
        """
        Initialize and validate the opf state.

        Example:
            Exercise OPF.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param stream: Input or output stream wrapped by the terminal or compatibility
            layer.
        :param basedir: Value supplied for basedir under the utility contract.
        :param unquote_urls: Value supplied for unquote urls under the utility contract.
        :param populate_spine: Value supplied for populate spine under the utility contract.
        :param try_to_guess_cover: Value supplied for try to guess cover under the utility
            contract.
        :return: None; validated state is stored on the receiving object.
        """
        if not hasattr(stream, "read"):
            stream = open(stream, "rb")
        raw = stream.read()
        if not raw:
            raise ValueError("Empty file: " + getattr(stream, "name", "stream"))
        self.try_to_guess_cover = try_to_guess_cover
        self.basedir = self.base_dir = basedir
        self.path_to_html_toc = self.html_toc_fragment = None
        raw, self.encoding = xml_to_unicode(raw, strip_encoding_pats=True, resolve_entities=True, assume_utf8=True)
        raw = raw[raw.find("<") :]
        self.root = etree.fromstring(raw, self.PARSER)
        if self.root is None:
            raise ValueError("Not an OPF file")
        try:
            self.package_version = float(self.root.get("version", None))
        except (AttributeError, TypeError, ValueError):
            self.package_version = 0
        self.metadata = self.metadata_path(self.root)
        if not self.metadata:
            self.metadata = [self.root.makeelement("{http://www.idpf.org/2007/opf}metadata")]
            self.root.insert(0, self.metadata[0])
            self.metadata[0].tail = "\n"
        self.metadata = self.metadata[0]
        if unquote_urls:
            self.unquote_urls()
        self.manifest = Manifest()
        m = self.manifest_path(self.root)
        if m:
            self.manifest = Manifest.from_opf_manifest_element(m, basedir)
        self.spine = None
        s = self.spine_path(self.root)
        if populate_spine and s:
            self.spine = Spine.from_opf_spine_element(s, self.manifest)
        self.guide = None
        guide = self.guide_path(self.root)
        self.guide = Guide.from_opf_guide(guide, basedir) if guide else None
        self.cover_data = (None, None)
        self.find_toc()
        self.read_user_metadata()

    def read_user_metadata(self: _typing.Self) -> None:
        """
        Read user metadata under the format's safety and compatibility rules.

        Example:
            Exercise OPF.read user metadata through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._user_metadata_ = {}
        temp = Metadata("x", ["x"])
        from LiuXin_alpha.file_formats.metadata.book.json_codec import (
            decode_is_multiple,
        )
        from LiuXin_alpha.utils.config.config_tools import from_json

        elems = self.root.xpath('//*[name() = "meta" and starts-with(@name,' '"calibre:user_metadata:") and @content]')
        for elem in elems:
            name = elem.get("name")
            name = ":".join(name.split(":")[2:])
            if not name or not name.startswith("#"):
                continue
            fm = elem.get("content")
            try:
                fm = json.loads(fm, object_hook=from_json)
                decode_is_multiple(fm)
                temp.set_user_metadata(name, fm)
            except:
                prints("Failed to read user metadata:", name)
                import traceback

                traceback.print_exc()
                continue
        self._user_metadata_ = temp.get_all_user_metadata(True)

    def to_book_metadata(self: _typing.Self) -> _typing.Any:
        """
        Perform the to book metadata operation under explicit file-format and conversion rules.

        Example:
            Exercise OPF.to book metadata through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ans = MetaInformation(self)
        for n, v in self._user_metadata_.items():
            ans.set_user_metadata(n, v)

        ans.set_identifiers(self.get_identifiers())

        return ans

    def write_user_metadata(self: _typing.Self) -> None:
        """
        Write user metadata under the format's safety and compatibility rules.

        Example:
            Exercise OPF.write user metadata through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        elems = self.root.xpath('//*[name() = "meta" and starts-with(@name,' '"calibre:user_metadata:") and @content]')
        for elem in elems:
            elem.getparent().remove(elem)
        serialize_user_metadata(self.metadata, self._user_metadata_)

    def find_toc(self: _typing.Self) -> None:
        """
        Find toc under the format's safety and compatibility rules.

        Example:
            Exercise OPF.find toc through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.toc = None
        try:
            spine = self.XPath('descendant::*[re:match(name(), "spine", "i")]')(self.root)
            toc = None
            if spine:
                spine = spine[0]
                toc = spine.get("toc", None)
            if toc is None and self.guide:
                for item in self.guide:
                    if item.type and item.type.lower() == "toc":
                        toc = item.path
            if toc is None:
                for item in self.manifest:
                    if "toc" in item.href().lower():
                        toc = item.path

            if toc is None:
                return
            self.toc = TOC(base_path=self.base_dir)
            is_ncx = (
                getattr(self, "manifest", None) is not None
                and self.manifest.type_for_id(toc) is not None
                and "dtbncx" in self.manifest.type_for_id(toc)
            )
            if is_ncx or toc.lower() in ("ncx", "ncxtoc"):
                path = self.manifest.path_for_id(toc)
                if path:
                    self.toc.read_ncx_toc(path)
                else:
                    f = glob.glob(os.path.join(self.base_dir, "*.ncx"))
                    if f:
                        self.toc.read_ncx_toc(f[0])
            else:
                self.path_to_html_toc, self.html_toc_fragment = (
                    toc.partition("#")[0],
                    toc.partition("#")[-1],
                )
                if not os.access(self.path_to_html_toc, os.R_OK) or not os.path.isfile(self.path_to_html_toc):
                    self.path_to_html_toc = None
                self.toc.read_html_toc(toc)
        except:
            pass

    def get_text(self: _typing.Self, elem: _typing.Any) -> _typing.Any:
        """
        Return text under the format's safety and compatibility rules.

        Example:
            Exercise OPF.get text through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param elem: Value supplied for elem under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return "".join(self.CONTENT(elem) or self.TEXT(elem))

    def set_text(self: _typing.Self, elem: _typing.Any, content: _typing.Any) -> None:
        """
        Set text under the format's safety and compatibility rules.

        Example:
            Exercise OPF.set text through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param elem: Value supplied for elem under the utility contract.
        :param content: Value supplied for content under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if elem.tag == self.META:
            elem.attrib["content"] = content
        else:
            elem.text = content

    def itermanifest(self: _typing.Self) -> _typing.Any:
        """
        Perform the itermanifest operation under explicit file-format and conversion rules.

        Example:
            Exercise OPF.itermanifest through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.manifest_path(self.root)

    def create_manifest_item(self: _typing.Self, href: _typing.Any, media_type: _typing.Any) -> _typing.Any:
        """
        Create manifest item under the format's safety and compatibility rules.

        Example:
            Exercise OPF.create manifest item through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param href: Value supplied for href under the utility contract.
        :param media_type: Value supplied for media type under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ids = [i.get("id", None) for i in self.itermanifest()]
        id = None
        for c in xrange(1, sys.maxint):
            id = "id%d" % c
            if id not in ids:
                break
        if not media_type:
            media_type = "application/xhtml+xml"
        ans = etree.Element(
            "{%s}item" % self.NAMESPACES["opf"],
            attrib={"id": id, "href": href, "media-type": media_type},
        )
        ans.tail = "\n\t\t"
        return ans

    def replace_manifest_item(self: _typing.Self, item: _typing.Any, items: _typing.Any) -> _typing.Any:
        """
        Perform the replace manifest item operation under explicit file-format and conversion rules.

        Example:
            Exercise OPF.replace manifest item through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param item: Value supplied for item under the utility contract.
        :param items: Value supplied for items under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        items = [self.create_manifest_item(*i) for i in items]
        for i, item2 in enumerate(items):
            item2.set("id", item.get("id") + ".%d" % (i + 1))
        manifest = item.getparent()
        index = manifest.index(item)
        manifest[index : index + 1] = items
        return [i.get("id") for i in items]

    def add_path_to_manifest(self: _typing.Self, path: _typing.Any, media_type: _typing.Any) -> None:
        """
        Perform the add path to manifest operation under explicit file-format and conversion rules.

        Example:
            Exercise OPF.add path to manifest through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param path: Filesystem path read, written, normalized or validated by the
            operation.
        :param media_type: Value supplied for media type under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        has_path = False
        path = os.path.abspath(path)
        for i in self.itermanifest():
            xpath = os.path.join(self.base_dir, *(i.get("href", "").split("/")))
            if os.path.abspath(xpath) == path:
                has_path = True
                break
        if not has_path:
            href = os.path.relpath(path, self.base_dir).replace(os.sep, "/")
            item = self.create_manifest_item(href, media_type)
            manifest = self.manifest_ppath(self.root)[0]
            manifest.append(item)

    def iterspine(self: _typing.Self) -> _typing.Any:
        """
        Perform the iterspine operation under explicit file-format and conversion rules.

        Example:
            Exercise OPF.iterspine through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.spine_path(self.root)

    def spine_items(self: _typing.Self) -> _typing.Iterator[_typing.Any]:
        """
        Perform the spine items operation under explicit file-format and conversion rules.

        Example:
            Exercise OPF.spine items through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :return: An iterator yielding the normalized values described above.
        """
        for item in self.iterspine():
            idref = item.get("idref", "")
            for x in self.itermanifest():
                if x.get("id", None) == idref:
                    yield x.get("href", "")

    def first_spine_item(self: _typing.Self) -> _typing.Any:
        """
        Perform the first spine item operation under explicit file-format and conversion rules.

        Example:
            Exercise OPF.first spine item through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        items = self.iterspine()
        if not items:
            return None
        idref = items[0].get("idref", "")
        for x in self.itermanifest():
            if x.get("id", None) == idref:
                return x.get("href", None)

    def create_spine_item(self: _typing.Self, idref: _typing.Any) -> _typing.Any:
        """
        Create spine item under the format's safety and compatibility rules.

        Example:
            Exercise OPF.create spine item through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param idref: Value supplied for idref under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ans = etree.Element("{%s}itemref" % self.NAMESPACES["opf"], idref=idref)
        ans.tail = "\n\t\t"
        return ans

    def replace_spine_items_by_idref(self: _typing.Self, idref: _typing.Any, new_idrefs: _typing.Any) -> None:
        """
        Perform the replace spine items by idref operation under explicit file-format and conversion rules.

        Example:
            Exercise OPF.replace spine items by idref through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param idref: Value supplied for idref under the utility contract.
        :param new_idrefs: Value supplied for new idrefs under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        items = list(map(self.create_spine_item, new_idrefs))
        spine = self.XPath('/opf:package/*[re:match(name(), "spine", "i")]')(self.root)[0]
        old = [i for i in self.iterspine() if i.get("idref", None) == idref]
        for x in old:
            i = spine.index(x)
            spine[i : i + 1] = items

    def create_guide_element(self: _typing.Self) -> _typing.Any:
        """
        Create guide element under the format's safety and compatibility rules.

        Example:
            Exercise OPF.create guide element through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        e = etree.SubElement(self.root, "{%s}guide" % self.NAMESPACES["opf"])
        e.text = "\n        "
        e.tail = "\n"
        return e

    def remove_guide(self: _typing.Self) -> None:
        """
        Perform the remove guide operation under explicit file-format and conversion rules.

        Example:
            Exercise OPF.remove guide through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.guide = None
        for g in self.root.xpath(
            './*[re:match(name(), "guide", "i")]',
            namespaces={"re": "http://exslt.org/regular-expressions"},
        ):
            self.root.remove(g)

    def create_guide_item(self: _typing.Self, type: _typing.Any, title: _typing.Any, href: _typing.Any) -> _typing.Any:
        """
        Create guide item under the format's safety and compatibility rules.

        Example:
            Exercise OPF.create guide item through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param type: Value supplied for type under the utility contract.
        :param title: Value supplied for title under the utility contract.
        :param href: Value supplied for href under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        e = etree.Element("{%s}reference" % self.NAMESPACES["opf"], type=type, title=title, href=href)
        e.tail = "\n"
        return e

    def add_guide_item(self: _typing.Self, type: _typing.Any, title: _typing.Any, href: _typing.Any) -> None:
        """
        Perform the add guide item operation under explicit file-format and conversion rules.

        Example:
            Exercise OPF.add guide item through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param type: Value supplied for type under the utility contract.
        :param title: Value supplied for title under the utility contract.
        :param href: Value supplied for href under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        g = self.root.xpath(
            './*[re:match(name(), "guide", "i")]',
            namespaces={"re": "http://exslt.org/regular-expressions"},
        )[0]
        g.append(self.create_guide_item(type, title, href))

    def iterguide(self: _typing.Self) -> _typing.Any:
        """
        Perform the iterguide operation under explicit file-format and conversion rules.

        Example:
            Exercise OPF.iterguide through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.guide_path(self.root)

    def unquote_urls(self: _typing.Self) -> None:
        """
        Perform the unquote urls operation under explicit file-format and conversion rules.

        Example:
            Exercise OPF.unquote urls through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        def get_href(item: _typing.Any) -> _typing.Any:
            """
            Return href under the format's safety and compatibility rules.

            Example:
                Exercise OPF.unquote urls.get href through a consuming regression::

                    python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


            :param item: Value supplied for item under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            raw = unquote(item.get("href", ""))
            if not isinstance(raw, unicode):
                raw = raw.decode("utf-8")
            return raw

        for item in self.itermanifest():
            item.set("href", get_href(item))
        for item in self.iterguide():
            item.set("href", get_href(item))

    # TODO: Add support for EPUB 3 refinements
    @property
    def title(self: _typing.Self) -> _typing.Any:

        """
        Perform the title operation under explicit file-format and conversion rules.

        Example:
            Exercise OPF.title through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        for elem in self.title_path(self.metadata):
            title = self.get_text(elem)
            if title and title.strip():
                return re.sub(r"\s+", " ", title.strip())

    @title.setter
    def title(self: _typing.Self, val: _typing.Any) -> None:

        """
        Perform the title operation under explicit file-format and conversion rules.

        Example:
            Exercise OPF.title through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param val: Template or metadata value evaluated by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        val = (val or "").strip()
        titles = self.title_path(self.metadata)
        if self.package_version < 3:
            # EPUB 3 allows multiple title elements containing sub-titles,
            # series and other things. We all loooove EPUB 3.
            for title in titles:
                title.getparent().remove(title)
            titles = ()
        if val:
            title = titles[0] if titles else self.create_metadata_element("title")
            title.text = re.sub(r"\s+", " ", six_unicode(val))

    @property
    def authors(self: _typing.Self) -> _typing.Any:

        """
        Perform the authors operation under explicit file-format and conversion rules.

        Example:
            Exercise OPF.authors through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ans = []
        for elem in self.authors_path(self.metadata):
            ans.extend(string_to_authors(self.get_text(elem)))
        return ans

    @authors.setter
    def authors(self: _typing.Self, val: _typing.Any) -> None:

        """
        Perform the authors operation under explicit file-format and conversion rules.

        Example:
            Exercise OPF.authors through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param val: Template or metadata value evaluated by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        remove = list(self.authors_path(self.metadata))
        for elem in remove:
            elem.getparent().remove(elem)
        # Ensure new author element is at the top of the list
        # for broken implementations that always use the first
        # <dc:creator> element with no attention to the role
        for author in reversed(val):
            elem = self.metadata.makeelement("{%s}creator" % self.NAMESPACES["dc"], nsmap=self.NAMESPACES)
            elem.tail = "\n"
            self.metadata.insert(0, elem)
            elem.set("{%s}role" % self.NAMESPACES["opf"], "aut")
            self.set_text(elem, author.strip())

    @property
    def author_sort(self: _typing.Self) -> _typing.Any:

        """
        Perform the author sort operation under explicit file-format and conversion rules.

        Example:
            Exercise OPF.author sort through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        matches = self.authors_path(self.metadata)
        if matches:
            for match in matches:
                ans = match.get("{%s}file-as" % self.NAMESPACES["opf"], None)
                if not ans:
                    ans = match.get("file-as", None)
                if ans:
                    return ans

    @author_sort.setter
    def author_sort(self: _typing.Self, val: _typing.Any) -> None:

        """
        Perform the author sort operation under explicit file-format and conversion rules.

        Example:
            Exercise OPF.author sort through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param val: Template or metadata value evaluated by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        matches = self.authors_path(self.metadata)
        if matches:
            for key in matches[0].attrib:
                if key.endswith("file-as"):
                    matches[0].attrib.pop(key)
            matches[0].set("{%s}file-as" % self.NAMESPACES["opf"], six_unicode(val))

    @property
    def tags(self: _typing.Self) -> _typing.Any:

        """
        Perform the tags operation under explicit file-format and conversion rules.

        Example:
            Exercise OPF.tags through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ans = []
        for tag in self.tags_path(self.metadata):
            text = self.get_text(tag)
            if text and text.strip():
                ans.extend([x.strip() for x in text.split(",")])
        return ans

    @tags.setter
    def tags(self: _typing.Self, val: _typing.Any) -> None:

        """
        Perform the tags operation under explicit file-format and conversion rules.

        Example:
            Exercise OPF.tags through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param val: Template or metadata value evaluated by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for tag in list(self.tags_path(self.metadata)):
            tag.getparent().remove(tag)
        for tag in val:
            elem = self.create_metadata_element("subject")
            self.set_text(elem, six_unicode(tag))

    @property
    def pubdate(self: _typing.Self) -> _typing.Any:

        """
        Perform the pubdate operation under explicit file-format and conversion rules.

        Example:
            Exercise OPF.pubdate through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ans = None
        for match in self.pubdate_path(self.metadata):
            try:
                val = parse_date(etree.tostring(match, encoding=six_unicode, method="text", with_tail=False).strip())
            except:
                continue
            if ans is None or val < ans:
                ans = val
        return ans

    @pubdate.setter
    def pubdate(self: _typing.Self, val: _typing.Any) -> None:

        """
        Perform the pubdate operation under explicit file-format and conversion rules.

        Example:
            Exercise OPF.pubdate through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param val: Template or metadata value evaluated by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        least_val = least_elem = None
        for match in self.pubdate_path(self.metadata):
            try:
                cval = parse_date(etree.tostring(match, encoding=six_unicode, method="text", with_tail=False).strip())
            except:
                match.getparent().remove(match)
            else:
                if not val:
                    match.getparent().remove(match)
                if least_val is None or cval < least_val:
                    least_val, least_elem = cval, match

        if val:
            if least_val is None:
                least_elem = self.create_metadata_element("date")

            least_elem.attrib.clear()
            least_elem.text = isoformat(val)

    @property
    def isbn(self: _typing.Self) -> bool:

        """
        Perform the isbn operation under explicit file-format and conversion rules.

        Example:
            Exercise OPF.isbn through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        for match in self.isbn_path(self.metadata):
            return self.get_text(match) or None

    @isbn.setter
    def isbn(self: _typing.Self, val: _typing.Any) -> None:

        """
        Perform the isbn operation under explicit file-format and conversion rules.

        Example:
            Exercise OPF.isbn through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param val: Template or metadata value evaluated by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        matches = self.isbn_path(self.metadata)
        if not val:
            for x in matches:
                x.getparent().remove(x)
            return
        if not matches:
            attrib = {"{%s}scheme" % self.NAMESPACES["opf"]: "ISBN"}
            matches = [self.create_metadata_element("identifier", attrib=attrib)]

    def get_identifiers(self: _typing.Self) -> _typing.Any:
        """
        Return identifiers under the format's safety and compatibility rules.

        Example:
            Exercise OPF.get identifiers through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        identifiers = {}
        for x in self.XPath('descendant::*[local-name() = "identifier" and text()]')(self.metadata):
            found_scheme = False
            for attr, val in x.attrib.iteritems():
                if attr.endswith("scheme"):
                    typ = icu_lower(val)
                    val = etree.tostring(x, with_tail=False, encoding=six_unicode, method="text").strip()
                    if val and typ not in ("calibre", "uuid"):
                        if typ == "isbn" and val.lower().startswith("urn:isbn:"):
                            val = val[len("urn:isbn:") :]
                        identifiers[typ] = val
                    found_scheme = True
                    break
            if not found_scheme:
                val = etree.tostring(x, with_tail=False, encoding=six_unicode, method="text").strip()
                if val.lower().startswith("urn:isbn:"):
                    val = check_isbn(val.split(":")[-1])
                    if val is not None:
                        identifiers["isbn"] = val
        return identifiers

    def set_identifiers(self: _typing.Self, identifiers: _typing.Any) -> None:
        """
        Set identifiers under the format's safety and compatibility rules.

        Example:
            Exercise OPF.set identifiers through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param identifiers: Value supplied for identifiers under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        identifiers = identifiers.copy()
        uuid_id = None
        for attr in self.root.attrib:
            if attr.endswith("unique-identifier"):
                uuid_id = self.root.attrib[attr]
                break

        for x in self.XPath('descendant::*[local-name() = "identifier"]')(self.metadata):
            xid = x.get("id", None)
            is_package_identifier = uuid_id is not None and uuid_id == xid
            typ = {val for attr, val in x.attrib.iteritems() if attr.endswith("scheme")}
            if is_package_identifier:
                typ = tuple(typ)
                if typ and typ[0].lower() in identifiers:
                    self.set_text(x, identifiers.pop(typ[0].lower()))
                continue
            if typ and not (typ & {"calibre", "uuid"}):
                x.getparent().remove(x)

        for typ, val in identifiers.iteritems():
            attrib = {"{%s}scheme" % self.NAMESPACES["opf"]: typ.upper()}
            self.set_text(
                self.create_metadata_element("identifier", attrib=attrib),
                six_unicode(val),
            )

    @property
    def application_id(self: _typing.Self) -> bool:

        """
        Perform the application id operation under explicit file-format and conversion rules.

        Example:
            Exercise OPF.application id through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        for match in self.application_id_path(self.metadata):
            return self.get_text(match) or None

    @application_id.setter
    def application_id(self: _typing.Self, val: _typing.Any) -> None:

        """
        Perform the application id operation under explicit file-format and conversion rules.

        Example:
            Exercise OPF.application id through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param val: Template or metadata value evaluated by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        removed_ids = set()
        for x in tuple(self.application_id_path(self.metadata)):
            removed_ids.add(x.get("id", None))
            x.getparent().remove(x)

        uuid_id = None
        for attr in self.root.attrib:
            if attr.endswith("unique-identifier"):
                uuid_id = self.root.attrib[attr]
                break
        attrib = {"{%s}scheme" % self.NAMESPACES["opf"]: "calibre"}
        if uuid_id and uuid_id in removed_ids:
            attrib["id"] = uuid_id
        self.set_text(self.create_metadata_element("identifier", attrib=attrib), six_unicode(val))

    @property
    def uuid(self: _typing.Self) -> bool:

        """
        Perform the uuid operation under explicit file-format and conversion rules.

        Example:
            Exercise OPF.uuid through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        for match in self.uuid_id_path(self.metadata):
            return self.get_text(match) or None

    @uuid.setter
    def uuid(self: _typing.Self, val: _typing.Any) -> None:

        """
        Perform the uuid operation under explicit file-format and conversion rules.

        Example:
            Exercise OPF.uuid through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param val: Template or metadata value evaluated by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        matches = self.uuid_id_path(self.metadata)
        if not matches:
            attrib = {"{%s}scheme" % self.NAMESPACES["opf"]: "uuid"}
            matches = [self.create_metadata_element("identifier", attrib=attrib)]
        self.set_text(matches[0], six_unicode(val))

    @property
    def language(self: _typing.Self) -> _typing.Any:

        """
        Perform the language operation under explicit file-format and conversion rules.

        Example:
            Exercise OPF.language through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ans = self.languages
        if ans:
            return ans[0]

    @language.setter
    def language(self: _typing.Self, val: _typing.Any) -> None:
        """
        Perform the language operation under explicit file-format and conversion rules.

        Example:
            Exercise OPF.language through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param val: Template or metadata value evaluated by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.languages = [val]

    @property
    def languages(self: _typing.Self) -> _typing.Any:
        """
        Perform the languages operation under explicit file-format and conversion rules.

        Example:
            Exercise OPF.languages through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ans = []
        for match in self.languages_path(self.metadata):
            t = self.get_text(match)
            if t and t.strip():
                l = canonicalize_lang(t.strip())
                if l:
                    ans.append(l)
        return ans

    @languages.setter
    def languages(self: _typing.Self, val: _typing.Any) -> None:
        """
        Perform the languages operation under explicit file-format and conversion rules.

        Example:
            Exercise OPF.languages through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param val: Template or metadata value evaluated by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        matches = self.languages_path(self.metadata)
        for x in matches:
            x.getparent().remove(x)

        for lang in val:
            l = self.create_metadata_element("language")
            self.set_text(l, six_unicode(lang))

    @property
    def raw_languages(self: _typing.Self) -> _typing.Iterator[_typing.Any]:
        """
        Perform the raw languages operation under explicit file-format and conversion rules.

        Example:
            Exercise OPF.raw languages through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :return: An iterator yielding the normalized values described above.
        """
        for match in self.languages_path(self.metadata):
            t = self.get_text(match)
            if t and t.strip():
                yield t.strip()

    @property
    def book_producer(self: _typing.Self) -> bool:
        """
        Perform the book producer operation under explicit file-format and conversion rules.

        Example:
            Exercise OPF.book producer through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        for match in self.bkp_path(self.metadata):
            return self.get_text(match) or None

    @book_producer.setter
    def book_producer(self: _typing.Self, val: _typing.Any) -> None:
        """
        Perform the book producer operation under explicit file-format and conversion rules.

        Example:
            Exercise OPF.book producer through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param val: Template or metadata value evaluated by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        matches = self.bkp_path(self.metadata)
        if not matches:
            matches = [self.create_metadata_element("contributor")]
            matches[0].set("{%s}role" % self.NAMESPACES["opf"], "bkp")
        self.set_text(matches[0], six_unicode(val))

    def identifier_iter(self: _typing.Self) -> _typing.Iterator[_typing.Any]:
        """
        Perform the identifier iter operation under explicit file-format and conversion rules.

        Example:
            Exercise OPF.identifier iter through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :return: An iterator yielding the normalized values described above.
        """
        for item in self.identifier_path(self.metadata):
            yield item

    @property
    def raw_unique_identifier(self: _typing.Self) -> _typing.Any:
        """
        Perform the raw unique identifier operation under explicit file-format and conversion rules.

        Example:
            Exercise OPF.raw unique identifier through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        uuid_elem = None
        for attr in self.root.attrib:
            if attr.endswith("unique-identifier"):
                uuid_elem = self.root.attrib[attr]
                break
        if uuid_elem:
            matches = self.root.xpath("//*[@id=%r]" % uuid_elem)
            if matches:
                for m in matches:
                    raw = m.text
                    if raw:
                        return raw

    @property
    def unique_identifier(self: _typing.Self) -> _typing.Any:
        """
        Perform the unique identifier operation under explicit file-format and conversion rules.

        Example:
            Exercise OPF.unique identifier through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        raw = self.raw_unique_identifier
        if raw:
            return raw.rpartition(":")[-1]

    @property
    def page_progression_direction(self: _typing.Self) -> _typing.Any:
        """
        Perform the page progression direction operation under explicit file-format and conversion rules.

        Example:
            Exercise OPF.page progression direction through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        spine = self.XPath('descendant::*[re:match(name(), "spine", "i")][1]')(self.root)
        if spine:
            for k, v in spine[0].attrib.iteritems():
                if k == "page-progression-direction" or k.endswith("}page-progression-direction"):
                    return v

    def guess_cover(self: _typing.Self) -> _typing.Any:
        """
        Try to guess a cover. Needed for some old/badly formed OPF files.

        Example:
            Exercise OPF.guess cover through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self.base_dir and os.path.exists(self.base_dir):
            for item in self.identifier_path(self.metadata):
                scheme = None
                for key in item.attrib.keys():
                    if key.endswith("scheme"):
                        scheme = item.get(key)
                        break
                if scheme is None:
                    continue
                if item.text:
                    prefix = item.text.replace("-", "")
                    for suffix in [".jpg", ".jpeg", ".gif", ".png", ".bmp"]:
                        cpath = os.access(os.path.join(self.base_dir, prefix + suffix), os.R_OK)
                        if os.access(os.path.join(self.base_dir, prefix + suffix), os.R_OK):
                            return cpath

    @property
    def epub3_raster_cover(self: _typing.Self) -> _typing.Any:
        """
        Perform the epub3 raster cover operation under explicit file-format and conversion rules.

        Example:
            Exercise OPF.epub3 raster cover through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        for item in self.itermanifest():
            props = set((item.get("properties") or "").lower().split())
            if "cover-image" in props:
                mt = item.get("media-type", "")
                if mt and "xml" not in mt and "html" not in mt:
                    return item.get("href", None)

    @property
    def raster_cover(self: _typing.Self) -> _typing.Any:
        """
        Perform the raster cover operation under explicit file-format and conversion rules.

        Example:
            Exercise OPF.raster cover through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        covers = self.raster_cover_path(self.metadata)
        if covers:
            cover_id = covers[0].get("content")
            for item in self.itermanifest():
                if item.get("id", None) == cover_id:
                    mt = item.get("media-type", "")
                    if mt and "xml" not in mt and "html" not in mt:
                        return item.get("href", None)
            for item in self.itermanifest():
                if item.get("href", None) == cover_id:
                    mt = item.get("media-type", "")
                    if mt and mt.startswith("image/"):
                        return item.get("href", None)

        @property
        def guide_raster_cover(self: _typing.Any) -> _typing.Any:
            """
            Perform the guide raster cover operation under explicit file-format and conversion rules.

            Example:
                Exercise OPF.raster cover.guide raster cover through a consuming regression::

                    python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


            :param self: Value supplied for self under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            covers = self.guide_cover_path(self.root)
            if covers:
                mt_map = {i.get("href"): i for i in self.itermanifest()}
                for href in covers:
                    if href:
                        i = mt_map.get(href)
                        if i is not None:
                            iid, mt = i.get("id"), i.get("media-type")
                            if iid and mt and mt.lower() in {"image/png", "image/jpeg", "image/jpg", "image/gif"}:
                                return i

        @property
        def epub3_nav(self: _typing.Any) -> _typing.Any:
            """
            Perform the epub3 nav operation under explicit file-format and conversion rules.

            Example:
                Exercise OPF.raster cover.epub3 nav through a consuming regression::

                    python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


            :param self: Value supplied for self under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            if self.package_version >= 3.0:
                for item in self.itermanifest():
                    props = (item.get("properties") or "").lower().split()
                    if "nav" in props:
                        mt = item.get("media-type") or ""
                        if "html" in mt.lower():
                            mid = item.get("id")
                            if mid:
                                path = self.manifest.path_for_id(mid)
                                if path and os.path.exists(path):
                                    return path

    @property
    def cover(self: _typing.Self) -> _typing.Any:
        """
        Return the cover for the file.

        Example:
            Exercise OPF.cover through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self.guide is not None:
            for t in ("cover", "other.ms-coverimage-standard", "other.ms-coverimage"):
                for item in self.guide:
                    if item.type and item.type.lower() == t:
                        return item.path
        try:
            if self.try_to_guess_cover:
                return self.guess_cover()
        except:
            pass

    @cover.setter
    def cover(self: _typing.Self, path: _typing.Any) -> None:
        """
        Perform the cover operation under explicit file-format and conversion rules.

        Example:
            Exercise OPF.cover through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param path: Filesystem path read, written, normalized or validated by the
            operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.guide is not None:
            self.guide.set_cover(path)
            for item in list(self.iterguide()):
                if "cover" in item.get("type", ""):
                    item.getparent().remove(item)

        else:
            g = self.create_guide_element()
            self.guide = Guide()
            self.guide.set_cover(path)
            etree.SubElement(
                g,
                "opf:reference",
                nsmap=self.NAMESPACES,
                attrib={"type": "cover", "href": self.guide[-1].href()},
            )

        id = self.manifest.id_for_path(self.cover)
        if id is None:
            for t in ("cover", "other.ms-coverimage-standard", "other.ms-coverimage"):
                for item in self.guide:
                    if item.type.lower() == t:
                        self.create_manifest_item(item.href(), guess_type(path)[0])

    def get_metadata_element(self: _typing.Self, name: _typing.Any) -> _typing.Any:
        """
        Return metadata element under the format's safety and compatibility rules.

        Example:
            Exercise OPF.get metadata element through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param name: Field, file, function or resource name addressed by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        matches = self.metadata_elem_path(self.metadata, name=name)
        if matches:
            return matches[-1]

    def create_metadata_element(self: _typing.Self, name: _typing.Any, attrib: _typing.Any = None, is_dc: bool = True) -> _typing.Any:
        """
        Create metadata element under the format's safety and compatibility rules.

        Example:
            Exercise OPF.create metadata element through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param attrib: Value supplied for attrib under the utility contract.
        :param is_dc: Value supplied for is dc under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if is_dc:
            name = "{%s}%s" % (self.NAMESPACES["dc"], name)
        else:
            attrib = attrib or {}
            attrib["name"] = "calibre:" + name
            name = "{%s}%s" % (self.NAMESPACES["opf"], "meta")
        nsmap = dict(self.NAMESPACES)
        del nsmap["opf"]
        elem = etree.SubElement(self.metadata, name, attrib=attrib, nsmap=nsmap)
        elem.tail = "\n"
        return elem

    def render(self: _typing.Self, encoding: str = "utf-8") -> _typing.Any:
        """
        Perform the render operation under explicit file-format and conversion rules.

        Example:
            Exercise OPF.render through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param encoding: Value supplied for encoding under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        for meta in self.raster_cover_path(self.metadata):
            # Ensure that the name attribute occurs before the content
            # attribute. Needed for Nooks.
            a = meta.attrib
            c = a.get("content", None)
            if c is not None:
                del a["content"]
                a["content"] = c

        self.write_user_metadata()
        if pretty_print_opf:
            _pretty_print(self.root)
        raw = etree.tostring(self.root, encoding=encoding, pretty_print=True)
        if not raw.lstrip().startswith("<?xml "):
            raw = '<?xml version="1.0"  encoding="%s"?>\n' % encoding.upper() + raw
        return raw

    def smart_update(self: _typing.Self, mi: _typing.Any, replace_metadata: bool = False, apply_null: bool = False) -> None:
        """
        Merge metadata while retaining package fields absent from the update.

        Example:
            Exercise OPF.smart update through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param mi: Metadata object exposed to the template function.
        :param replace_metadata: Value supplied for replace metadata under the utility
            contract.
        :param apply_null: Value supplied for apply null under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for attr in (
            "title",
            "authors",
            "author_sort",
            "title_sort",
            "publisher",
            "series",
            "series_index",
            "rating",
            "isbn",
            "tags",
            "category",
            "comments",
            "book_producer",
            "pubdate",
            "user_categories",
            "author_link_map",
        ):
            val = getattr(mi, attr, None)
            is_null = val is None or val in ((), [], (None, None), {})
            if is_null:
                if apply_null and attr in {
                    "series",
                    "tags",
                    "isbn",
                    "comments",
                    "publisher",
                }:
                    setattr(self, attr, ([] if attr == "tags" else None))
            else:
                setattr(self, attr, val)
        langs = getattr(mi, "languages", [])
        if langs == ["und"]:
            langs = []
        if apply_null or langs:
            self.languages = langs or []
        temp = self.to_book_metadata()
        temp.smart_update(mi, replace_metadata=replace_metadata)
        if not replace_metadata and callable(getattr(temp, "custom_field_keys", None)):
            # We have to replace non-null fields regardless of the value of
            # replace_metadata to match the behavior of the builtin fields
            # above.
            for x in temp.custom_field_keys():
                meta = temp.get_user_metadata(x, make_copy=True)
                if meta is None:
                    continue
                if meta["datatype"] == "text" and meta["is_multiple"]:
                    val = mi.get(x, [])
                    if val or apply_null:
                        temp.set(x, val)
                elif meta["datatype"] in {"int", "float", "bool"}:
                    missing = object()
                    val = mi.get(x, missing)
                    if val is missing:
                        if apply_null:
                            temp.set(x, None)
                    elif apply_null or val is not None:
                        temp.set(x, val)
                elif apply_null and mi.is_null(x) and not temp.is_null(x):
                    temp.set(x, None)

        self._user_metadata_ = temp.get_all_user_metadata(True)


# }}}


class OPFCreator(Metadata):
    """
    Provide the opfcreator contract for validated ebook processing.

    Example:
        Exercise OPFCreator through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py
    """
    def __init__(self: _typing.Self, base_path: _typing.Any, other: _typing.Any) -> None:
        """
        Initialize. will eventually be. This is used by the L{create_manifest} method to convert paths to files into relative paths.

        Example:
            Exercise OPFCreator.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param base_path: Value supplied for base path under the utility contract.
        :param other: Value supplied for other under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        Metadata.__init__(self, title="", other=other)
        self.base_path = os.path.abspath(base_path)
        self.page_progression_direction = None
        if self.application_id is None:
            self.application_id = str(uuid.uuid4())
        if not isinstance(self.toc, TOC):
            self.toc = None
        if not self.authors:
            self.authors = [_("Unknown")]
        if self.guide is None:
            self.guide = Guide()
        if self.cover:
            self.guide.set_cover(self.cover)

    def create_manifest(self: _typing.Self, entries: _typing.Any) -> None:
        """
        Create <manifest>

        Example:
            Exercise OPFCreator.create manifest through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param entries: Value supplied for entries under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        entries = map(
            lambda x: x if os.path.isabs(x[0]) else (os.path.abspath(os.path.join(self.base_path, x[0])), x[1]),
            entries,
        )
        self.manifest = Manifest.from_paths(entries)
        self.manifest.set_basedir(self.base_path)

    def create_manifest_from_files_in(self: _typing.Self, files_and_dirs: _typing.Any, exclude: _typing.Callable[..., _typing.Any] = lambda x: False) -> None:
        """
        Create manifest from files in under the format's safety and compatibility rules.

        Example:
            Exercise OPFCreator.create manifest from files in through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param files_and_dirs: Value supplied for files and dirs under the utility contract.
        :param exclude: Value supplied for exclude under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        entries = []

        def dodir(dir: _typing.Any) -> None:
            """
            Perform the dodir operation under explicit file-format and conversion rules.

            Example:
                Exercise OPFCreator.create manifest from files in.dodir through a consuming regression::

                    python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


            :param dir: Value supplied for dir under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            for spec in os.walk(dir):
                root, files = spec[0], spec[-1]
                for name in files:
                    path = os.path.join(root, name)
                    if os.path.isfile(path) and not exclude(path):
                        entries.append((path, None))

        for i in files_and_dirs:
            if os.path.isdir(i):
                dodir(i)
            else:
                entries.append((i, None))

        self.create_manifest(entries)

    def create_spine(self: _typing.Self, entries: _typing.Any) -> None:
        """
        Create the <spine> element. Must first call :method:`create_manifest`.

        Example:
            Exercise OPFCreator.create spine through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param entries: Value supplied for entries under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        entries = map(
            lambda x: x if os.path.isabs(x) else os.path.abspath(os.path.join(self.base_path, x)),
            entries,
        )
        self.spine = Spine.from_paths(entries, self.manifest)

    def set_toc(self: _typing.Self, toc: _typing.Any) -> None:
        """
        Set the toc. You must call :method:`create_spine` before calling this method.

        Example:
            Exercise OPFCreator.set toc through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param toc: Value supplied for toc under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.toc = toc

    def create_guide(self: _typing.Self, guide_element: _typing.Any) -> None:
        """
        Create OPF guide references for recognized book landmarks.

        Example:
            Exercise OPFCreator.create guide through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param guide_element: Value supplied for guide element under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.guide = Guide.from_opf_guide(guide_element, self.base_path)
        self.guide.set_basedir(self.base_path)

    def render(
        self: _typing.Self,
        opf_stream: _typing.Any = sys.stdout,
        ncx_stream: _typing.Any = None,
        ncx_manifest_entry: _typing.Any = None,
        encoding: _typing.Any = None,
    ) -> None:
        """
        Perform the render operation under explicit file-format and conversion rules.

        Example:
            Exercise OPFCreator.render through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param opf_stream: Value supplied for opf stream under the utility contract.
        :param ncx_stream: Value supplied for ncx stream under the utility contract.
        :param ncx_manifest_entry: Value supplied for ncx manifest entry under the utility
            contract.
        :param encoding: Value supplied for encoding under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if encoding is None:
            encoding = "utf-8"
        toc = getattr(self, "toc", None)
        if self.manifest:
            self.manifest.set_basedir(self.base_path)
            if ncx_manifest_entry is not None and toc is not None:
                if not os.path.isabs(ncx_manifest_entry):
                    ncx_manifest_entry = os.path.join(self.base_path, ncx_manifest_entry)
                remove = [i for i in self.manifest if i.id == "ncx"]
                for item in remove:
                    self.manifest.remove(item)
                self.manifest.append(ManifestItem(ncx_manifest_entry, self.base_path))
                self.manifest[-1].id = "ncx"
                self.manifest[-1].mime_type = "application/x-dtbncx+xml"
        if self.guide is None:
            self.guide = Guide()
        if self.cover:
            cover = self.cover
            if not os.path.isabs(cover):
                cover = os.path.abspath(os.path.join(self.base_path, cover))
            self.guide.set_cover(cover)
        self.guide.set_basedir(self.base_path)

        # Actual rendering
        from LiuXin_alpha.file_formats.oeb.base import CALIBRE_NS, DC11_NS, OPF2_NS

        DNS = OPF2_NS + "___xx___"
        E = ElementMaker(namespace=DNS, nsmap={None: DNS})
        M = ElementMaker(namespace=DNS, nsmap={"dc": DC11_NS, "calibre": CALIBRE_NS, "opf": OPF2_NS})
        DC = ElementMaker(namespace=DC11_NS)

        def DC_ELEM(tag: _typing.Any, text: _typing.Any, dc_attrs: dict[_typing.Any, _typing.Any] = {}, opf_attrs: dict[_typing.Any, _typing.Any] = {}) -> _typing.Any:
            """
            Perform the DC ELEM operation under explicit file-format and conversion rules.

            Example:
                Exercise OPFCreator.render.DC ELEM through a consuming regression::

                    python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


            :param tag: Value supplied for tag under the utility contract.
            :param text: Text parsed, normalized or rendered.
            :param dc_attrs: Value supplied for dc attrs under the utility contract.
            :param opf_attrs: Value supplied for opf attrs under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            if text:
                elem = getattr(DC, tag)(clean_ascii_chars(text), **dc_attrs)
            else:
                elem = getattr(DC, tag)(**dc_attrs)
            for k, v in opf_attrs.items():
                elem.set("{%s}%s" % (OPF2_NS, k), v)
            return elem

        def CAL_ELEM(name: _typing.Any, content: _typing.Any) -> _typing.Any:
            """
            Perform the CAL ELEM operation under explicit file-format and conversion rules.

            Example:
                Exercise OPFCreator.render.CAL ELEM through a consuming regression::

                    python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


            :param name: Field, file, function or resource name addressed by the operation.
            :param content: Value supplied for content under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return M.meta(name=name, content=content)

        metadata = M.metadata()
        a = metadata.append
        role = {}
        a(DC_ELEM("title", self.title if self.title else _("Unknown"), opf_attrs=role))
        for i, author in enumerate(self.authors):
            fa = {"role": "aut"}
            if i == 0 and self.author_sort:
                fa["file-as"] = self.author_sort
            a(DC_ELEM("creator", author, opf_attrs=fa))
        a(
            DC_ELEM(
                "contributor",
                "%s (%s) [%s]" % (__appname__, __version__, "http://calibre-ebook.com"),
                opf_attrs={"role": "bkp", "file-as": __appname__},
            )
        )
        a(
            DC_ELEM(
                "identifier",
                str(self.application_id),
                opf_attrs={"scheme": __appname__},
                dc_attrs={"id": __appname__ + "_id"},
            )
        )
        if getattr(self, "pubdate", None) is not None:
            a(DC_ELEM("date", self.pubdate.isoformat()))
        langs = self.languages
        if not langs or langs == ["und"]:
            langs = [get_lang().replace("_", "-").partition("-")[0]]
        for lang in langs:
            a(DC_ELEM("language", lang))
        if self.comments:
            a(DC_ELEM("description", self.comments))
        if self.publisher:
            a(DC_ELEM("publisher", self.publisher))
        for key, val in self.get_identifiers().iteritems():
            a(DC_ELEM("identifier", val, opf_attrs={"scheme": icu_upper(key)}))
        if self.rights:
            a(DC_ELEM("rights", self.rights))
        if self.tags:
            for tag in self.tags:
                a(DC_ELEM("subject", tag))
        if self.series:
            a(CAL_ELEM("calibre:series", self.series))
            if self.series_index is not None:
                a(CAL_ELEM("calibre:series_index", self.format_series_index()))
        if self.title_sort:
            a(CAL_ELEM("calibre:title_sort", self.title_sort))
        if self.rating is not None:
            a(CAL_ELEM("calibre:rating", str(self.rating)))
        if self.timestamp is not None:
            a(CAL_ELEM("calibre:timestamp", self.timestamp.isoformat()))
        if self.publication_type is not None:
            a(CAL_ELEM("calibre:publication_type", self.publication_type))
        if self.user_categories:
            from LiuXin_alpha.file_formats.metadata.book.json_codec import (
                object_to_unicode,
            )

            a(
                CAL_ELEM(
                    "calibre:user_categories",
                    json.dumps(object_to_unicode(self.user_categories)),
                )
            )
        manifest = E.manifest()
        if self.manifest is not None:
            for ref in self.manifest:
                item = E.item(id=str(ref.id), href=ref.href())
                item.set("media-type", ref.mime_type)
                manifest.append(item)
        spine = E.spine()
        if self.toc is not None:
            spine.set("toc", "ncx")
        if self.page_progression_direction is not None:
            spine.set("page-progression-direction", self.page_progression_direction)
        if self.spine is not None:
            for ref in self.spine:
                if ref.id is not None:
                    spine.append(E.itemref(idref=ref.id))
        guide = E.guide()
        if self.guide is not None:
            for ref in self.guide:
                href = ref.href()
                if isinstance(href, bytes):
                    href = href.decode("utf-8")
                item = E.reference(type=ref.type, href=href)
                if ref.title:
                    item.set("title", ref.title)
                guide.append(item)

        serialize_user_metadata(metadata, self.get_all_user_metadata(False))

        root = E.package(metadata, manifest, spine, guide)
        root.set("unique-identifier", __appname__ + "_id")
        raw = etree.tostring(root, pretty_print=True, xml_declaration=True, encoding=encoding)
        raw = raw.replace(DNS, OPF2_NS)
        opf_stream.write(raw)
        opf_stream.flush()
        if toc is not None and ncx_stream is not None:
            toc.render(ncx_stream, self.application_id)
            ncx_stream.flush()


def metadata_to_opf(mi: _typing.Any, as_string: bool = True, default_lang: _typing.Any = None) -> _typing.Any:
    """
    Converts a given MetaData object to OPF for saving.

    Example:
        Exercise metadata to opf through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


    :param mi: Metadata object exposed to the template function.
    :param as_string: Value supplied for as string under the utility contract.
    :param default_lang: Value supplied for default lang under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    import textwrap

    from LiuXin_alpha.file_formats.oeb.base import DC, OPF

    if not mi.application_id:
        mi.application_id = str(uuid.uuid4())

    if not mi.uuid:
        mi.uuid = str(uuid.uuid4())

    if not mi.book_producer:
        mi.book_producer = __appname__ + " (%s) " % __version__ + "[http://calibre-ebook.com]"

    if not mi.languages:
        lang = get_lang().replace("_", "-").partition("-")[0] if default_lang is None else default_lang
        mi.languages = [lang]

    root = etree.fromstring(
        textwrap.dedent(
            """
    <package xmlns="http://www.idpf.org/2007/opf" unique-identifier="uuid_id" version="2.0">
        <metadata xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:opf="http://www.idpf.org/2007/opf">
            <dc:identifier opf:scheme="%(a)s" id="%(a)s_id">%(id)s</dc:identifier>
            <dc:identifier opf:scheme="uuid" id="uuid_id">%(uuid)s</dc:identifier>
            </metadata>
        <guide/>
    </package>
    """
            % dict(a=__appname__, id=mi.application_id, uuid=mi.uuid)
        )
    )
    metadata = root[0]
    guide = root[1]
    metadata[0].tail = "\n" + (" " * 8)

    def factory(tag: _typing.Any, text: _typing.Any = None, sort: _typing.Any = None, role: _typing.Any = None, scheme: _typing.Any = None, name: _typing.Any = None, content: _typing.Any = None) -> None:
        """
        Perform the factory operation under explicit file-format and conversion rules.

        Example:
            Exercise metadata to opf.factory through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param tag: Value supplied for tag under the utility contract.
        :param text: Text parsed, normalized or rendered.
        :param sort: Value supplied for sort under the utility contract.
        :param role: Value supplied for role under the utility contract.
        :param scheme: Value supplied for scheme under the utility contract.
        :param name: Field, file, function or resource name addressed by the operation.
        :param content: Value supplied for content under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        attrib = {}
        if sort:
            attrib[OPF("file-as")] = sort
        if role:
            attrib[OPF("role")] = role
        if scheme:
            attrib[OPF("scheme")] = scheme
        if name:
            attrib["name"] = name
        if content:
            attrib["content"] = content
        try:
            elem = metadata.makeelement(tag, attrib=attrib)
        except ValueError:
            elem = metadata.makeelement(tag, attrib={k: clean_xml_chars(v) for k, v in attrib.iteritems()})
        elem.tail = "\n" + (" " * 8)
        if text:
            try:
                elem.text = text.strip()
            except ValueError:
                elem.text = clean_ascii_chars(text.strip())
        metadata.append(elem)

    factory(DC("title"), mi.title)
    for au in mi.authors:
        factory(DC("creator"), au, mi.author_sort, "aut")
    factory(DC("contributor"), mi.book_producer, __appname__, "bkp")
    if hasattr(mi.pubdate, "isoformat"):
        factory(DC("date"), isoformat(mi.pubdate))
    if hasattr(mi, "category") and mi.category:
        factory(DC("type"), mi.category)
    if mi.comments:
        factory(DC("description"), clean_ascii_chars(mi.comments))
    if mi.publisher:
        factory(DC("publisher"), mi.publisher)
    for key, val in mi.get_identifiers().iteritems():
        factory(DC("identifier"), val, scheme=icu_upper(key))
    if mi.rights:
        factory(DC("rights"), mi.rights)
    for lang in mi.languages:
        if not lang or lang.lower() == "und":
            continue
        factory(DC("language"), lang)
    if mi.tags:
        for tag in mi.tags:
            factory(DC("subject"), tag)
    meta = lambda n, c: factory("meta", name="calibre:" + n, content=c)
    if getattr(mi, "author_link_map", None) is not None:
        meta("author_link_map", dump_dict(mi.author_link_map))
    if mi.series:
        meta("series", mi.series)
    if mi.series_index is not None:
        meta("series_index", mi.format_series_index())
    if mi.rating is not None:
        meta("rating", str(mi.rating))
    if hasattr(mi.timestamp, "isoformat"):
        meta("timestamp", isoformat(mi.timestamp))
    if mi.publication_type:
        meta("publication_type", mi.publication_type)
    if mi.title_sort:
        meta("title_sort", mi.title_sort)
    if mi.user_categories:
        meta("user_categories", dump_dict(mi.user_categories))

    serialize_user_metadata(metadata, mi.get_all_user_metadata(False))

    metadata[-1].tail = "\n" + (" " * 4)

    if mi.cover:
        if not isinstance(mi.cover, unicode):
            mi.cover = mi.cover.decode(filesystem_encoding)
        guide.text = "\n" + (" " * 8)
        r = guide.makeelement(
            OPF("reference"),
            attrib={"type": "cover", "title": _("Cover"), "href": mi.cover},
        )
        r.tail = "\n" + (" " * 4)
        guide.append(r)
    if pretty_print_opf:
        _pretty_print(root)

    return etree.tostring(root, pretty_print=True, encoding="utf-8", xml_declaration=True) if as_string else root


def test_m2o() -> None:

    """
    Perform the test m2o operation under explicit file-format and conversion rules.

    Example:
        Exercise test m2o through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.utils.date import now as nowf

    mi = MetaInformation("test & title", ['a"1', "a'2"])
    mi.title_sort = "a'\"b"
    mi.author_sort = "author sort"
    mi.pubdate = nowf()
    mi.language = "en"
    mi.comments = "what a fun book\n\n"
    mi.publisher = "publisher"
    mi.set_identifiers({"isbn": "booo", "dummy": "dummy"})
    mi.tags = ["a", "b"]
    mi.series = "s\"c'l&<>"
    mi.series_index = 3.34
    mi.rating = 3
    mi.timestamp = nowf()
    mi.publication_type = "ooooo"
    mi.rights = "yes"
    mi.cover = os.path.abspath("asd.jpg")
    opf = metadata_to_opf(mi)
    print(opf)
    newmi = MetaInformation(OPF(six_cStringIO(opf)))
    for attr in (
        "author_sort",
        "title_sort",
        "comments",
        "publisher",
        "series",
        "series_index",
        "rating",
        "isbn",
        "tags",
        "cover_data",
        "application_id",
        "language",
        "cover",
        "book_producer",
        "timestamp",
        "pubdate",
        "rights",
        "publication_type",
    ):
        o, n = getattr(mi, attr), getattr(newmi, attr)
        if o != n and o.strip() != n.strip():
            print("FAILED:", attr, getattr(mi, attr), "!=", getattr(newmi, attr))
    if mi.get_identifiers() != newmi.get_identifiers():
        print("FAILED:", "identifiers", mi.get_identifiers(), end=" ")
        print("!=", newmi.get_identifiers())


class OPFTest(unittest.TestCase):
    """
    Provide the opftest contract for validated ebook processing.

    Example:
        Exercise OPFTest through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py
    """
    def setUp(self: _typing.Self) -> None:
        """
        Perform the setUp operation under explicit file-format and conversion rules.

        Example:
            Exercise OPFTest.setUp through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.stream = six_cStringIO(
            """\
<?xml version="1.0"  encoding="UTF-8"?>
<package version="2.0" xmlns="http://www.idpf.org/2007/opf" >
<metadata xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:opf="http://www.idpf.org/2007/opf">
    <dc:title opf:file-as="Wow">A Cool &amp; &copy; &#223; Title</dc:title>
    <creator opf:role="aut" file-as="Monkey">Monkey Kitchen</creator>
    <creator opf:role="aut">Next</creator>
    <dc:subject>One</dc:subject><dc:subject>Two</dc:subject>
    <dc:identifier scheme="ISBN">123456789</dc:identifier>
    <dc:identifier scheme="dummy">dummy</dc:identifier>
    <meta name="calibre:series" content="A one book series" />
    <meta name="calibre:rating" content="4"/>
    <meta name="calibre:publication_type" content="test"/>
    <meta name="calibre:series_index" content="2.5" />
</metadata>
<manifest>
    <item id="1" href="a%20%7E%20b" media-type="text/txt" />
</manifest>
</package>
"""
        )
        self.opf = OPF(self.stream, os.getcwdu())

    def testReading(self: _typing.Self, opf: _typing.Any = None) -> None:
        """
        Perform the testReading operation under explicit file-format and conversion rules.

        Example:
            Exercise OPFTest.testReading through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :param opf: Value supplied for opf under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if opf is None:
            opf = self.opf
        self.assertEqual(opf.title, "A Cool & \xa9 \xdf Title")
        self.assertEqual(opf.authors, "Monkey Kitchen,Next".split(","))
        self.assertEqual(opf.author_sort, "Monkey")
        self.assertEqual(opf.title_sort, "Wow")
        self.assertEqual(opf.tags, ["One", "Two"])
        self.assertEqual(opf.isbn, "123456789")
        self.assertEqual(opf.series, "A one book series")
        self.assertEqual(opf.series_index, 2.5)
        self.assertEqual(opf.rating, 4)
        self.assertEqual(opf.publication_type, "test")
        self.assertEqual(list(opf.itermanifest())[0].get("href"), "a ~ b")
        self.assertEqual(opf.get_identifiers(), {"isbn": "123456789", "dummy": "dummy"})

    def testWriting(self: _typing.Self) -> None:
        """
        Perform the testWriting operation under explicit file-format and conversion rules.

        Example:
            Exercise OPFTest.testWriting through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for test in [
            ("title", "New & Title"),
            ("authors", ["One", "Two"]),
            ("author_sort", "Kitchen"),
            ("tags", ["Three"]),
            ("isbn", "a"),
            ("rating", 3),
            ("series_index", 1),
            ("title_sort", "ts"),
        ]:
            setattr(self.opf, *test)
            attr, val = test
            self.assertEqual(getattr(self.opf, attr), val)

        self.opf.render()

    def testCreator(self: _typing.Self) -> None:
        """
        Perform the testCreator operation under explicit file-format and conversion rules.

        Example:
            Exercise OPFTest.testCreator through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        opf = OPFCreator(os.getcwdu(), self.opf)
        buf = six_cStringIO()
        opf.render(buf)
        raw = buf.getvalue()
        self.testReading(opf=OPF(six_cStringIO(raw), os.getcwdu()))

    def testSmartUpdate(self: _typing.Self) -> None:
        """
        Perform the testSmartUpdate operation under explicit file-format and conversion rules.

        Example:
            Exercise OPFTest.testSmartUpdate through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.opf.smart_update(MetaInformation(self.opf))
        self.testReading()


def suite() -> _typing.Any:
    """
    Perform the suite operation under explicit file-format and conversion rules.

    Example:
        Exercise suite through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return unittest.TestLoader().loadTestsFromTestCase(OPFTest)


def test() -> None:
    """
    Perform the test operation under explicit file-format and conversion rules.

    Example:
        Exercise test through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    unittest.TextTestRunner(verbosity=2).run(suite())


def test_user_metadata() -> None:

    """
    Perform the test user metadata operation under explicit file-format and conversion rules.

    Example:
        Exercise test user metadata through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf2_object_smoke.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    mi = Metadata("Test title", ["test author1", "test author2"])
    um = {
        "#myseries": {
            "#value#": "test series\xe4",
            "datatype": "text",
            "is_multiple": None,
            "name": "My Series",
        },
        "#myseries_index": {"#value#": 2.45, "datatype": "float", "is_multiple": None},
        "#mytags": {
            "#value#": ["t1", "t2", "t3"],
            "datatype": "text",
            "is_multiple": "|",
            "name": "My Tags",
        },
    }
    mi.set_all_user_metadata(um)
    raw = metadata_to_opf(mi)
    opfc = OPFCreator(os.getcwdu(), other=mi)
    out = StringIO()
    opfc.render(out)
    raw2 = out.getvalue()
    f = StringIO(raw)
    opf = OPF(f)
    f2 = StringIO(raw2)
    opf2 = OPF(f2)
    assert um == opf._user_metadata_
    assert um == opf2._user_metadata_
    print(opf.render())


if __name__ == "__main__":
    # test_user_metadata()
    test_m2o()
    test()

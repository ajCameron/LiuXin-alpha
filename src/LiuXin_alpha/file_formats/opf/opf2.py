#!/usr/bin/env  python

"""
lxml based OPF parser.
"""

from __future__ import print_function

import copy
import functools
import glob
import json
import os
import re
import sys
import uuid
from collections import OrderedDict, defaultdict
from copy import deepcopy

from lxml import etree

from LiuXin_alpha.file_formats.utils import escape_xpath_attr

from LiuXin_alpha.utils.calibre_compat.ebooks.metadata.book.base import MetaInformation as calibreMetaInformation, Metadata as calibreMetadata
from LiuXin_alpha.metadata.utils import soft_float_to_int

from LiuXin_alpha.metadata.constants import CREATOR_CATEGORIES
from LiuXin_alpha.metadata.ebook_metadata_tools import (

    check_isbn,
    author_to_author_sort,
)
from LiuXin_alpha.metadata.utils import string_to_authors

# Todo: Eventually to be replaced with one metadata to rule them all
from LiuXin_alpha.metadata.containers.calibre_like_book_metadata import CalibreLikeLiuXinBookMetaData as MetaData
from LiuXin_alpha.metadata.constants import creator_to_marc
# Todo: Make sure everything has been refactored to a sensible place
from LiuXin_alpha.file_formats.toc import TOC
from LiuXin_alpha.metadata.utils import parse_opf, pretty_print_opf as _pretty_print

from LiuXin_alpha.preferences import preferences as prefs

from LiuXin_alpha.utils.mine_types import guess_type
from LiuXin_alpha.utils.logging import prints
from LiuXin_alpha.constants import __appname__, __version__, filesystem_encoding
from LiuXin_alpha.utils.libraries.cleantext import clean_ascii_chars, clean_xml_chars
from LiuXin_alpha.utils.date import parse_date, isoformat
from LiuXin_alpha.utils.language_tools.icu import lower as icu_lower
from LiuXin_alpha.utils.language_tools.icu import upper as icu_upper
from LiuXin_alpha.utils.localization import trans as _, get_lang, canonicalize_lang
from LiuXin_alpha.utils.logging import default_log

# Py2/Py3 compatibility layer
from LiuXin_alpha.utils.libraries.liuxin_six import (
    six_string_types, six_unicode, memory_range,
    dict_iteritems as iteritems, six_unquote as unquote, six_urlparse as urlparse)

unicode = str

__license__ = "GPL v3"
__copyright__ = "2008, Kovid Goyal kovid@kovidgoyal.net"
__docformat__ = "restructuredtext en"

# Dublin core metadata valid fields in the /elements/1.1/ namespace
# Taken from http://dublincore.org/documents/dcmi-terms/#elements-contributor
# contributor - An entity responsible for making contributions to the resource.
# coverage - The spatial or temporal topic of the resource, the spatial applicability of the resource, or the
#            jurisdiction under which the resource is relevant.
# creator - Examples of a Creator include a person, an organization, or a service. Typically, the name of a Creator
#           should be used to indicate the entity.
# date - A point or period of time associated with an event in the lifecycle of the resource.
# description - Description may include but is not limited to: an abstract, a table of contents, a graphical
#               representation, or a free-text account of the resource.
# format - The file format, physical medium, or dimensions of the resource.
# identifier - An unambiguous reference to the resource within a given context.
# language - Recommended best practice is to use a controlled vocabulary such as RFC 4646 [RFC4646].
# publisher - An entity responsible for making the resource available.
# relation - Recommended best practice is to identify the related resource by means of a string conforming to a formal
#            identification system.
# rights - Typically, rights information includes a statement about various property rights associated with the
#          resource, including intellectual property rights.
# source - A related resource from which the described resource is derived.
# subject - The topic of the resource.
# title - A name given to the resource.
# type - The nature of the resource (e.g. ebook)

pretty_print_opf = False


class PrettyPrint:
    """
    Turns the pretty print OPF option on and off.
    """

    def __enter__(self):
        global pretty_print_opf
        pretty_print_opf = True

    def __exit__(self, *args):
        global pretty_print_opf
        pretty_print_opf = False


pretty_print = PrettyPrint()


class Resource(object):  # {{{
    """
    Represents a resource (usually a file on the filesystem or a URL pointing to the web.

    Such resources are commonly referred to in OPF files.

    They have the interface:

    :member:`path`
    :member:`mime_type`
    :method:`href`
    """

    def __init__(self, href_or_path, basedir=os.getcwd(), is_path=True):
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
                path = path.decode(sys.getfilesystemencoding())
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

    def href(self, basedir=None):
        """
        Return a URL pointing to this resource. If it is a file on the filesystem the URL is relative to `basedir`.

        `basedir`: If None, the basedir of this resource is used (see :method:`set_basedir`).
        If this resource has no basedir, then the current working directory is used as the basedir.
        :param basedir:
        :return:
        """
        if basedir is None:
            if self._basedir:
                basedir = self._basedir
            else:
                basedir = os.getcwd()
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

    def set_basedir(self, path):
        self._basedir = path

    def basedir(self):
        return self._basedir

    def __repr__(self):
        return "Resource(%s, %s)" % (repr(self.path), repr(self.href()))


# }}}


class ResourceCollection(object):  # {{{
    def __init__(self):
        self._resources = []

    def __iter__(self):
        for r in self._resources:
            yield r

    def __len__(self):
        return len(self._resources)

    def __getitem__(self, index):
        return self._resources[index]

    def __bool__(self):
        return len(self._resources) > 0

    def __str__(self):
        resources = map(repr, self)
        return "[%s]" % ", ".join(resources)

    def __repr__(self):
        return str(self)

    def append(self, resource):
        if not isinstance(resource, Resource):
            raise ValueError("Can only append objects of type Resource")
        self._resources.append(resource)

    def remove(self, resource):
        self._resources.remove(resource)

    def replace(self, start, end, items):
        "Same as list[start:end] = items"
        self._resources[start:end] = items

    @staticmethod
    def from_directory_contents(top, topdown=True):
        """
        Makes and returns a ResourceCollection from the given starting position
        :param top: Starting position
        :param topdown: Passed throguh to os.walk
        :return:
        """
        collection = ResourceCollection()
        for spec in os.walk(top, topdown=topdown):
            path = os.path.abspath(os.path.join(spec[0], spec[1]))
            res = Resource.from_path(path)
            res.set_basedir(top)
            collection.append(res)
        return collection

    def set_basedir(self, path):
        for res in self:
            res.set_basedir(path)


# }}}


class ManifestItem(Resource):  # {{{
    @staticmethod
    def from_opf_manifest_item(item, basedir):
        href = item.get("href", None)
        if href:
            res = ManifestItem(href, basedir=basedir, is_path=True)
            mt = item.get("media-type", "").strip()
            if mt:
                res.mime_type = mt
            return res

    @property
    def media_type(self):
        return self.mime_type

    @media_type.setter
    def media_type(self, val):
        self.mime_type = val

    def __unicode__(self):
        return '<item id="%s" href="%s" media-type="%s" />' % (
            self.id,
            self.href(),
            self.media_type,
        )

    def __str__(self):
        return six_unicode(self).encode("utf-8")

    def __repr__(self):
        return six_unicode(self)

    def __getitem__(self, index):
        if index == 0:
            return self.href()
        if index == 1:
            return self.media_type
        raise IndexError("%d out of bounds." % index)


# }}}


class Manifest(ResourceCollection):  # {{{
    def append_from_opf_manifest_item(self, item, dir):
        self.append(ManifestItem.from_opf_manifest_item(item, dir))
        item_id = item.get("id", "")
        if not item_id:
            item_id = "id%d" % self.next_id
        self[-1].id = item_id
        self.next_id += 1

    @staticmethod
    def from_opf_manifest_element(items, dir):
        m = Manifest()
        for item in items:
            try:
                m.append_from_opf_manifest_item(item, dir)
            except ValueError:
                continue
        return m

    @staticmethod
    def from_paths(entries):
        """
        `entries`: List of (path, mime-type) If mime-type is None it is autodetected
        :param entries:
        :return:
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

    def add_item(self, path, mime_type=None):
        mi = ManifestItem(path, is_path=True)
        if mime_type:
            mi.mime_type = mime_type
        mi.id = "id%d" % self.next_id
        self.next_id += 1
        self.append(mi)
        return mi.id

    def __init__(self):
        ResourceCollection.__init__(self)
        self.next_id = 1

    def item(self, id):
        for i in self:
            if i.id == id:
                return i

    def id_for_path(self, path):
        path = os.path.normpath(os.path.abspath(path))
        for i in self:
            if i.path and os.path.normpath(i.path) == path:
                return i.id

    def path_for_id(self, id):
        for i in self:
            if i.id == id:
                return i.path

    def type_for_id(self, id):
        for i in self:
            if i.id == id:
                return i.mime_type


# }}}


class Spine(ResourceCollection):  # {{{
    class Item(Resource):
        def __init__(self, idfunc, *args, **kwargs):
            Resource.__init__(self, *args, **kwargs)
            self.is_linear = True
            self.id = idfunc(self.path)
            self.idref = None

        def __repr__(self):
            return "Spine.Item(path=%r, id=%s, is_linear=%s)" % (
                self.path,
                self.id,
                self.is_linear,
            )

    @staticmethod
    def from_opf_spine_element(itemrefs, manifest):
        s = Spine(manifest)
        seen = set()
        path_map = {i.id: i.path for i in s.manifest}
        for itemref in itemrefs:
            idref = itemref.get("idref", None)
            if idref is not None:
                path = path_map.get(idref)
                if path and path not in seen:
                    r = Spine.Item(lambda x: idref, path, is_path=True)
                    r.is_linear = itemref.get("linear", "yes") == "yes"
                    r.idref = idref
                    s.append(r)
                    seen.add(path)
        return s

    @staticmethod
    def from_paths(paths, manifest):
        s = Spine(manifest)
        for path in paths:
            try:
                s.append(Spine.Item(s.manifest.id_for_path, path, is_path=True))
            except:
                continue
        return s

    def __init__(self, manifest):
        ResourceCollection.__init__(self)
        self.manifest = manifest

    def replace(self, start, end, ids):
        """
        Replace the items between start (inclusive) and end (not inclusive) with the items identified by ids. ids can
        be a list of any length.
        :param start:
        :param end:
        :param ids:
        :return:
        """
        items = []
        for local_id in ids:
            path = self.manifest.path_for_id(local_id)
            if path is None:
                raise ValueError("id %s not in manifest")
            items.append(Spine.Item(lambda x: local_id, path, is_path=True))
        ResourceCollection.replace(start, end, items)

    def linear_items(self):
        for r in self:
            if r.is_linear:
                yield r.path

    def nonlinear_items(self):
        for r in self:
            if not r.is_linear:
                yield r.path

    def items(self):
        for i in self:
            yield i.path


# }}}


class Guide(ResourceCollection):  # {{{
    class Reference(Resource):
        @staticmethod
        def from_opf_resource_item(ref, basedir):
            title, href, type = ref.get("title", ""), ref.get("href"), ref.get("type")
            res = Guide.Reference(href, basedir, is_path=True)
            res.title = title
            res.type = type
            return res

        def __repr__(self):
            ans = '<reference type="%s" href="%s" ' % (self.type, self.href())
            if self.title:
                ans += 'title="%s" ' % self.title
            return ans + "/>"

    @staticmethod
    def from_opf_guide(references, base_dir=os.getcwd()):
        coll = Guide()
        for ref in references:
            try:
                ref = Guide.Reference.from_opf_resource_item(ref, base_dir)
                coll.append(ref)
            except:
                continue
        return coll

    def set_cover(self, path):
        map(self.remove, [i for i in self if "cover" in i.type.lower()])
        for local_type in (
            "cover",
            "other.ms-coverimage-standard",
            "other.ms-coverimage",
        ):
            self.append(Guide.Reference(path, is_path=True))
            self[-1].type = local_type
            self[-1].title = ""


# }}}


class MetadataField(object):
    """
    Descriptors for some common metadata fields with a predictable interface.
    Descriptor for a field which has a single value.
    """

    def __init__(
        self,
        name,
        is_dc=True,
        formatter=None,
        none_is=None,
        renderer=lambda x: six_unicode(x),
    ):
        """
        Provides various options for setting the behavior of the metadata field.
        :param name:
        :param is_dc: Is the metadata field part of the Dublin core metadata initiative
        :param formatter:
        :param none_is: Default value if the actual value of the element is None
        :param renderer:
        """
        self.name = name
        self.is_dc = is_dc
        self.formatter = formatter
        self.none_is = none_is
        self.renderer = renderer

    def __real_get__(self, obj, type=None):
        """
        Preforms the actual retrieval from the OPF, according to the pre-set behavioral defaults.
        :param obj:
        :param type:
        :return:
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

    def __get__(self, obj, type=None):
        ans = self.__real_get__(obj, type)
        if ans is None:
            ans = self.none_is
        return ans

    def __set__(self, obj, val):

        elem = obj.get_metadata_element(self.name)

        # If the value is None then remove the element, if it exists
        if val is None:
            if elem is not None:
                elem.getparent().remove(elem)
            return

        if elem is None:
            elem = obj.create_metadata_element(self.name, is_dc=self.is_dc)

        obj.set_text(elem, self.renderer(val))


class PluralMetadataField(object):
    """
    Represents a metadata field which has multiple entries.
    """

    def __init__(self, name, is_dc=True, none_is=None, renderer=lambda x: six_unicode(x)):
        """
        Provides various options for setting the behavior of the metadata field.
        :param name: The name of the metadata field - will
        :param is_dc: Is the metadata field part of the Dublin core metadata initiative
        :param none_is: Default value if there is no value to return
        :param renderer: Apply this to all the values before they're returned
        """
        self.name = name
        self.is_dc = is_dc
        self.none_is = none_is
        self.renderer = renderer

    def __real_get__(self, obj, type=None):
        """
        Preforms the actual retrieval from the OPF according to the pre-set behavioral defaults.
        :param obj:
        :param type:
        :return:
        """
        ans = obj.get_metadata_elements(self.name)
        if not ans:
            return None
        # Filter the None elements
        ans = [a for a in ans if a is not None]
        ans = [self.renderer(obj.get_text(a)) for a in ans]
        if not ans:
            return None
        return ans

    def __get__(self, obj, type):
        ans = self.__real_get__(obj, type)
        if ans is None:
            ans = self.none_is
        return ans

    def __set__(self, instance, value):
        raise NotImplementedError


class PluralSeriesIndexField(PluralMetadataField):
    """
    Represents the Series Index plural field - returns an OrderedDict keyed with the name of the series and valued with
    the index of that series.
    """

    def __get__(self, obj, type):
        ans = obj.get_metadata_elements(self.name)
        if not ans:
            return None
        new_ans = OrderedDict()
        for ans_elem in ans:
            ans_text = obj.TEXT(ans_elem)
            ans_series = re.sub(r"^series:", "", ans_text, re.I)
            ans_content = obj.CONTENT(ans_elem)
            new_ans[ans_series] = self.renderer(ans_content[0])
        return new_ans


class PluralRatingField(PluralMetadataField):
    """
    Represents the rating field - returns an OrderedDict keyed with the name of the rating and valued with that rating.
    """

    def __init__(self, name, is_dc=True, none_is=None, renderer=lambda x: six_unicode(x)):
        PluralMetadataField.__init__(self, name, is_dc, none_is, renderer)
        from LiuXin_alpha.file_formats.oeb.base import OPF as OPF_name

        self.opf_name = OPF_name

    def __get__(self, obj, type):
        ans = obj.get_metadata_elements(self.name)
        if not ans:
            return None
        new_ans = OrderedDict()
        for ans_elem in ans:
            ans_text = obj.CONTENT(ans_elem)
            try:
                ans_rating = self.renderer(ans_text[0])
            except ValueError:
                ans_rating = ans_text[0]
            ans_scheme = ans_elem.attrib[self.opf_name("scheme")]
            new_ans[ans_scheme] = ans_rating
        return new_ans


class TitleSortField(MetadataField):
    def __get__(self, obj, type=None):
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

    def __set__(self, obj, val):
        MetadataField.__set__(self, obj, val)
        matches = obj.title_path(obj.metadata)
        if matches:
            for match in matches:
                for attr in list(match.attrib):
                    if attr.endswith("file-as"):
                        del match.attrib[attr]


# ----------------------------------------------------------------------------------------------------------------------
#
# - HELPER FUNCTIONS


def serialize_user_metadata(metadata_elem, all_user_metadata, tail="\n" + (" " * 8)):
    from LiuXin.metadata.book.json_codec import object_to_unicode, encode_is_multiple
    from LiuXin.utils.config.config_tools import to_json

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


def dump_dict(cats):
    """
    Serialize a dictionary for storage.
    :param cats:
    :return:
    """
    if not cats:
        cats = {}
    from LiuXin.metadata.book.json_codec import object_to_unicode

    return json.dumps(object_to_unicode(cats), ensure_ascii=False, skipkeys=True)


#
# ----------------------------------------------------------------------------------------------------------------------


class OPF(object):  # {{{
    """
    Represents an OPF file object - used for parsing calibre .opf files, and extended to handle .lx files, as well as
    supporting more information extraction from regular .opf files.
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

    # XPath statements for the positions of the commonly used metadata objects
    # - Setup the tools needed to read/write the metadata - {{{

    CONTENT = XPath('self::*[re:match(name(), "meta$", "i")]/@content')

    TEXT = XPath("string()")

    metadata_path = XPath('descendant::*[re:match(name(), "metadata", "i")]')

    metadata_elem_path = XPath(
        'descendant::*[re:match(name(), concat($name, "$"), "i") or (re:match(name(), "meta$", "i") '
        'and re:match(@name, concat("^calibre:", $name, "$"), "i"))]'
    )

    title_path = XPath('descendant::*[re:match(name(), "title", "i")]')

    authors_path = XPath(
        'descendant::*[re:match(name(), "creator", "i") and '
        + '(@role="aut" or @opf:role="aut" or (not(@role) and not(@opf:role)))]'
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

    guide_cover_path = XPath(
        'descendant::*[local-name()="guide"]/*[local-name()="reference" and ' + 're:match(@type, "cover", "i")]/@href'
    )

    identifier_path = XPath('descendant::*[re:match(name(), "identifier", "i")]')

    application_id_path = XPath(
        'descendant::*[re:match(name(), "identifier", "i") and '
        + '(re:match(@opf:scheme, "calibre|libprs500", "i") '
        + 'or re:match(@scheme, "calibre|libprs500", "i"))]'
    )

    uuid_id_path = XPath(
        'descendant::*[re:match(name(), "identifier", "i") and '
        + '(re:match(@opf:scheme, "uuid", "i") or re:match(@scheme, "uuid", "i"))]'
    )

    languages_path = XPath('descendant::*[local-name()="language"]')

    all_languages_included = PluralMetadataField("inc_lang", is_dc=False)

    all_languages_available = PluralMetadataField("available_lang", is_dc=False)

    manifest_path = XPath('descendant::*[re:match(name(), "manifest", "i")]/*[re:match(name(), "item", "i")]')

    manifest_ppath = XPath('descendant::*[re:match(name(), "manifest", "i")]')

    spine_path = XPath('descendant::*[re:match(name(), "spine", "i")]/*[re:match(name(), "itemref", "i")]')

    guide_path = XPath('descendant::*[re:match(name(), "guide", "i")]/*[re:match(name(), "reference", "i")]')

    publisher = MetadataField("publisher")

    all_publishers = PluralMetadataField("publisher", is_dc=True)

    comments = MetadataField("description")

    category = MetadataField("type")

    rights = MetadataField("rights")

    # Returns the first series in the OPF object
    series = MetadataField("series", is_dc=False)

    # Return all the series in the object
    all_series = PluralMetadataField("series", is_dc=False)

    # - }}}

    if prefs["use_series_auto_increment_tweak_when_importing"]:
        series_index = MetadataField("series_index", is_dc=False, formatter=float, none_is=None)
    else:
        series_index = MetadataField("series_index", is_dc=False, formatter=float, none_is=1)

    all_series_index = PluralSeriesIndexField("series_index", is_dc=False, renderer=soft_float_to_int)

    subject = MetadataField("custom_subject", is_dc=False)

    all_subjects = PluralMetadataField("custom_subject", is_dc=False)

    synopsis = MetadataField("synopsis", is_dc=False)

    all_synopses = PluralMetadataField("synopsis", is_dc=False)

    imprint = MetadataField("imprint", is_dc=False)

    all_imprints = PluralMetadataField("imprint", is_dc=False)

    title_sort = TitleSortField("title_sort", is_dc=False)

    # Returns the first rating in the work
    # Todo: Make sure this rating is always the user rating - if present
    rating = MetadataField("rating", is_dc=False, formatter=float)

    publication_type = MetadataField("publication_type", is_dc=False)

    timestamp = MetadataField("timestamp", is_dc=False, formatter=parse_date, renderer=isoformat)

    user_categories = MetadataField("user_categories", is_dc=False, formatter=json.loads, renderer=dump_dict)

    author_link_map = MetadataField("author_link_map", is_dc=False, formatter=json.loads, renderer=dump_dict)

    wordcount = MetadataField("wordcount", is_dc=False, renderer=six_unicode)

    note = MetadataField("note", is_dc=False)

    all_notes = PluralMetadataField("note", is_dc=False)

    user_comment = MetadataField("user_comment", is_dc=False)

    all_user_comments = PluralMetadataField("user_comment", is_dc=False)

    all_ratings = PluralRatingField("rating", is_dc=True, renderer=float)

    doc_type = MetadataField("doc_type", is_dc=False)

    genre = MetadataField("genre", is_dc=False)

    all_genres = PluralMetadataField("genre", is_dc=False)

    original_filename = MetadataField("original_filename", is_dc=False)

    all_original_filenames = PluralMetadataField("original_filename", is_dc=False)

    original_filepath = MetadataField("original_filepath", is_dc=False)

    all_original_filepaths = PluralMetadataField("original_filepath", is_dc=False)

    last_modified = MetadataField("last_modified", is_dc=False, formatter=parse_date, renderer=isoformat)

    metadata_date = MetadataField("metadata_date", is_dc=False, formatter=parse_date, renderer=isoformat)

    metadata_language = MetadataField("metadata_language", is_dc=False)

    pubdate = MetadataField("pubdate", is_dc=True)

    @staticmethod
    def get_contributor_marc_code(contributor_type):
        """
        Takes a contributor type and returns a contributor marc code - or KeyErrors out.
        :param contributor_type:
        :return:
        """
        return creator_to_marc(contributor_type)

    @classmethod
    def get_contributor_path(cls, contributor_type):
        """
        A contributor of a type recognized by LiuXin - i.e. a value in LiuXin.constants.creator_marc_roles_dict.
        Allows support for contributors beyond the author type recognized by calibre.
        :param contributor_type:
        :return:
        """
        contributor_marc_code = cls.get_contributor_marc_code(contributor_type)

        contributor_path = cls.XPath(
            'descendant::*[re:match(name(), "creator", "i") and '
            '(@role="{0}" or @opf:role="{0}")]'.format(contributor_marc_code)
        )
        return contributor_path

    def __init__(
        self,
        stream,
        basedir=os.getcwd(),
        unquote_urls=True,
        populate_spine=True,
        try_to_guess_cover=True,
        preparsed_opf=None,
        read_toc=True,
    ):
        """
        Reads a stream and loads its data into the class.

        The stream must have some data in it.

        :param stream:
        :param basedir:
        :param unquote_urls:
        :param populate_spine:
        :param try_to_guess_cover:
        :param preparsed_opf:
        :param read_toc:
        """
        self.root = parse_opf(stream) if preparsed_opf is None else preparsed_opf
        try:
            self.package_version = float(self.root.get("version", None))
        except (AttributeError, TypeError, ValueError):
            self.package_version = 0
        self.metadata = self.metadata_path(self.root)

        self.try_to_guess_cover = try_to_guess_cover
        self.basedir = self.base_dir = basedir
        self.path_to_html_toc = self.html_toc_fragment = None

        # Set the metadata properties - make the metadata
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

        if read_toc:
            self.find_toc()
        else:
            self.toc = None

        self.read_user_metadata()

    def read_user_metadata(self):
        """
        Parse the file and read user information back out.
        :return:
        """
        from LiuXin.utils.config.config_tools import from_json
        from LiuXin.metadata.book.json_codec import decode_is_multiple

        self._user_metadata_ = {}
        temp = calibreMetadata("x", ["x"])

        elems = self.root.xpath('//*[name() = "meta" and starts-with(@name,"calibre:user_metadata:") and @content]')
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

    def to_book_metadata(self, calibre=True):
        """
        Return the metadata of the opf document as a calibreMetadata object.
        :return:
        """
        # If the opf tree is of a version greater than 3, use the parser for that version
        if self.package_version >= 3.0:
            from LiuXin.file_formats.opf.opf3 import read_metadata

            if calibre:
                return read_metadata(self.root, calibre_md_rtn=True)
            else:
                return read_metadata(self.root, calibre_md_rtn=False)

        if calibre:
            ans = calibreMetaInformation(self)
        else:
            ans = MetaData.from_calibre(self)

        for n, v in self._user_metadata_.items():
            ans.set_user_metadata(n, v)

        ans.set_identifiers(self.get_identifiers())

        return ans

    def write_user_metadata(self):
        elems = self.root.xpath('//*[name() = "meta" and starts-with(@name,"calibre:user_metadata:") and @content]')
        for elem in elems:
            elem.getparent().remove(elem)
        serialize_user_metadata(self.metadata, self._user_metadata_)

    def find_toc(self):
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

    def get_text(self, elem):
        """
        If there is text in the element returns the text of the element - otherwise returns the content.
        :param elem:
        :return:
        """
        return "".join(self.CONTENT(elem) or self.TEXT(elem))

    def set_text(self, elem, content):
        if elem.tag == self.META:
            elem.attrib["content"] = content
        else:
            elem.text = content

    def itermanifest(self):
        return self.manifest_path(self.root)

    def create_manifest_item(self, href, media_type):
        ids = [i.get("id", None) for i in self.itermanifest()]
        local_id = None
        for c in memory_range(1, sys.maxint):
            local_id = "id%d" % c
            if local_id not in ids:
                break
        if not media_type:
            media_type = "application/xhtml+xml"
        ans = etree.Element(
            "{%s}item" % self.NAMESPACES["opf"],
            attrib={"id": local_id, "href": href, "media-type": media_type},
        )
        ans.tail = "\n\t\t"
        return ans

    def replace_manifest_item(self, item, items):
        items = [self.create_manifest_item(*i) for i in items]
        for i, item2 in enumerate(items):
            item2.set("id", item.get("id") + ".%d" % (i + 1))
        manifest = item.getparent()
        index = manifest.index(item)
        manifest[index : index + 1] = items
        return [i.get("id") for i in items]

    def add_path_to_manifest(self, path, media_type):
        path = os.path.abspath(path)
        for i in self.itermanifest():
            xpath = os.path.join(self.base_dir, *(i.get("href", "").split("/")))
            if os.path.abspath(xpath) == path:
                return i.get("id")

        href = os.path.relpath(path, self.base_dir).replace(os.sep, "/")
        item = self.create_manifest_item(href, media_type)
        manifest = self.manifest_ppath(self.root)[0]
        manifest.append(item)
        self.manifest.append_from_opf_manifest_item(item, self.basedir)
        return item.get("id")

    def iterspine(self):
        return self.spine_path(self.root)

    def spine_items(self):
        for item in self.iterspine():
            idref = item.get("idref", "")
            for x in self.itermanifest():
                if x.get("id", None) == idref:
                    yield x.get("href", "")

    def first_spine_item(self):
        items = self.iterspine()
        if not items:
            return None
        idref = items[0].get("idref", "")
        for x in self.itermanifest():
            if x.get("id", None) == idref:
                return x.get("href", None)

    def create_spine_item(self, idref):
        ans = etree.Element("{%s}itemref" % self.NAMESPACES["opf"], idref=idref)
        ans.tail = "\n\t\t"
        return ans

    def replace_spine_items_by_idref(self, idref, new_idrefs):
        items = list(map(self.create_spine_item, new_idrefs))
        spine = self.XPath('/opf:package/*[re:match(name(), "spine", "i")]')(self.root)[0]
        old = [i for i in self.iterspine() if i.get("idref", None) == idref]
        for x in old:
            i = spine.index(x)
            spine[i : i + 1] = items

    def create_guide_element(self):
        e = etree.SubElement(self.root, "{%s}guide" % self.NAMESPACES["opf"])
        e.text = "\n        "
        e.tail = "\n"
        return e

    def remove_guide(self):
        self.guide = None
        for g in self.root.xpath(
            './*[re:match(name(), "guide", "i")]',
            namespaces={"re": "http://exslt.org/regular-expressions"},
        ):
            self.root.remove(g)

    def create_guide_item(self, type, title, href):
        e = etree.Element("{%s}reference" % self.NAMESPACES["opf"], type=type, title=title, href=href)
        e.tail = "\n"
        return e

    def add_guide_item(self, type, title, href):
        g = self.root.xpath(
            './*[re:match(name(), "guide", "i")]',
            namespaces={"re": "http://exslt.org/regular-expressions"},
        )[0]
        g.append(self.create_guide_item(type, title, href))

    def iterguide(self):
        return self.guide_path(self.root)

    def unquote_urls(self):
        def get_href(local_item):
            raw = unquote(local_item.get("href", ""))
            if not isinstance(raw, unicode):
                raw = raw.decode("utf-8")
            return raw

        for item in self.itermanifest():
            item.set("href", get_href(item))
        for item in self.iterguide():
            item.set("href", get_href(item))

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO REPRESENT THE OPF ATTRIBUTES AS PROPERTIES

    # TODO: Add support for EPUB 3 refinements
    # AN1: EPUB 3 allows multiple title elements containing sub-titles, series and other things. We all loooove EPUB 3.
    # AN2: It is pretty cool
    # {{{
    @property
    def title(self):
        for elem in self.title_path(self.metadata):
            title = self.get_text(elem)
            if title and title.strip():
                return re.sub(r"\s+", " ", title.strip())

    @title.setter
    def title(self, val):
        val = (val or "").strip()
        titles = self.title_path(self.metadata)
        if self.package_version < 3:
            # EPUB 3 allows multiple title elements containing sub-titles, series and other things.
            # We all loooove EPUB 3.
            for title in titles:
                title.getparent().remove(title)
            titles = ()
        if val:
            title = titles[0] if titles else self.create_metadata_element("title")
            title.text = re.sub(r"\s+", " ", six_unicode(val))

    # }}}

    # {{{
    @property
    def authors(self):
        """
        Returns a list of all creators with the role of author, in the order they appear in the document.
        :return:
        """
        ans = []

        for elem in self.authors_path(self.metadata):
            ans.extend(string_to_authors(self.get_text(elem)))
        return ans

    @authors.setter
    def authors(self, val):
        """
        Sets the values for all the authors in the row.
        :param val:
        :return:
        """
        # To ensure a clean add remove all the author fields first - will add them back in later
        remove = list(self.authors_path(self.metadata))
        for elem in remove:
            elem.getparent().remove(elem)

        # Ensure new author element is at the top of the list for broken implementations that always use the first
        # <dc:creator> element with no attention to the role
        for author in reversed(val):
            elem = self.metadata.makeelement("{%s}creator" % self.NAMESPACES["dc"], nsmap=self.NAMESPACES)
            elem.tail = "\n"
            self.metadata.insert(0, elem)
            elem.set("{%s}role" % self.NAMESPACES["opf"], "aut")
            self.set_text(elem, author.strip())

    # }}}

    # {{{
    def get_contributors(self, role):
        """
        Returns all the contributors with a particular role in the opf.
        :return:
        """
        contributor_path = self.get_contributor_path(contributor_type=role)
        ans = []
        for elem in contributor_path(self.metadata):
            ans.extend(string_to_authors(self.get_text(elem)))
        return ans

    def set_contributors(self, role, val):
        """
        Set the contributors with a particular role in the opf.
        :param role:
        :param val:
        :return:
        """
        contributor_marc_code = self.get_contributor_marc_code(contributor_type=role)
        # Authors need special attention - they need to be set first
        if contributor_marc_code == "aut":
            self.authors = val

        contributor_path = self.get_contributor_path(contributor_type=role)

        # Remove the existing authors
        remove = list(contributor_path(self.metadata))
        for elem in remove:
            elem.getparent().remove(elem)

        # Write the creators out - each new element will be appended to the opf - so want to reverse so that the highest
        # priority ones appear first
        for creator in val:
            elem = self.metadata.makeelement("{%s}creator" % self.NAMESPACES["dc"], nsmap=self.NAMESPACES)
            elem.tail = "\n"
            elem.set("{%s}role" % self.NAMESPACES["opf"], contributor_marc_code)
            self.set_text(elem, creator.strip())

    # }}}

    # {{{
    @property
    def author_sort(self):
        """
        The author sort field is just a string - reads the {OPF_NAMESPACE}file_as property and returns it
        :return:
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
    def author_sort(self, val):
        """
        Updates the file-as property with the new author sort
        :param val:
        :return:
        """
        matches = self.authors_path(self.metadata)
        if matches:
            for key in matches[0].attrib:
                if key.endswith("file-as"):
                    matches[0].attrib.pop(key)
            matches[0].set("{%s}file-as" % self.NAMESPACES["opf"], six_unicode(val))

    # }}}

    # {{{
    @property
    def tags(self):
        ans = []
        for tag in self.tags_path(self.metadata):
            text = self.get_text(tag)
            if text and text.strip():
                ans.extend([x.strip() for x in text.split(",")])
        return ans

    @tags.setter
    def tags(self, val):
        # Purge the old tags, if any
        for tag in list(self.tags_path(self.metadata)):
            tag.getparent().remove(tag)
        for tag in val:
            elem = self.create_metadata_element("subject")
            self.set_text(elem, six_unicode(tag))

    # }}}

    # {{{
    @property
    def pubdate(self):
        ans = None
        for match in self.pubdate_path(self.metadata):
            try:
                val = parse_date(etree.tostring(match, encoding=six_unicode, method="text", with_tail=False).strip())
            except Exception as e:
                info_str = "Unable to parse date"
                default_log.log_exception(info_str, e, "INFO")
            if ans is None or val < ans:
                ans = val
        return ans

    @pubdate.setter
    def pubdate(self, val):
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

    # }}}

    # {{{
    @property
    def isbn(self):
        for match in self.isbn_path(self.metadata):
            return self.get_text(match) or None

    @isbn.setter
    def isbn(self, val):
        matches = self.isbn_path(self.metadata)
        if not val:
            for x in matches:
                x.getparent().remove(x)
            return
        if not matches:
            attrib = {"{%s}scheme" % self.NAMESPACES["opf"]: "ISBN"}
            matches = [self.create_metadata_element("identifier", attrib=attrib)]
        self.set_text(matches[0], six_unicode(val))

    # }}}

    def get_all_identifiers(self):
        """
        Returns all the identifiers in the document in the form of a dictionary keyed with the type of the identifier
        and valued with a set containing all the instances of that identifier in the OPF object.
        :return:
        """
        identifiers = defaultdict(set)
        for x in self.XPath('descendant::*[local-name() = "identifier" and text()]')(self.metadata):
            found_scheme = False
            for attr, val in iteritems(x.attrib):
                if attr.endswith("scheme"):
                    typ = icu_lower(val)
                    val = etree.tostring(x, with_tail=False, encoding=six_unicode, method="text").strip()
                    if val and typ not in ("calibre", "uuid"):
                        if typ == "isbn" and val.lower().startswith("urn:isbn:"):
                            val = val[len("urn:isbn:") :]
                        identifiers[typ].add(val)
                    found_scheme = True
                    break
            if not found_scheme:
                val = etree.tostring(x, with_tail=False, encoding=six_unicode, method="text").strip()
                if val.lower().startswith("urn:isbn:"):
                    val = check_isbn(val.split(":")[-1])
                    if val is not None:
                        identifiers["isbn"].add(val)
        return identifiers

    def get_identifiers(self):
        """
        Miantained for calibre compatability reasons - returns a dictionary keyed with the name of the identifiers and
        valued with the first instance of that identifier type in the OPF object.
        If you want all the identifiers, call get_all_identifiers
        :return:
        """
        identifiers = defaultdict(set)
        for x in self.XPath('descendant::*[local-name() = "identifier" and text()]')(self.metadata):
            found_scheme = False
            for attr, val in iteritems(x.attrib):
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

    def set_identifiers(self, identifiers):
        identifiers = identifiers.copy()
        uuid_id = None
        for attr in self.root.attrib:
            if attr.endswith("unique-identifier"):
                uuid_id = self.root.attrib[attr]
                break

        for x in self.XPath('descendant::*[local-name() = "identifier"]')(self.metadata):
            xid = x.get("id", None)
            is_package_identifier = uuid_id is not None and uuid_id == xid
            typ = {val for attr, val in iteritems(x.attrib) if attr.endswith("scheme")}
            if is_package_identifier:
                typ = tuple(typ)
                if typ and typ[0].lower() in identifiers:
                    self.set_text(x, identifiers.pop(typ[0].lower()))
                continue
            if typ and not (typ & {"calibre", "uuid"}):
                x.getparent().remove(x)

        for typ, val in iteritems(identifiers):
            attrib = {"{%s}scheme" % self.NAMESPACES["opf"]: typ.upper()}
            self.set_text(
                self.create_metadata_element("identifier", attrib=attrib),
                six_unicode(val),
            )

    # {{{
    @property
    def application_id(self):

        for match in self.application_id_path(self.metadata):
            return self.get_text(match) or None

    @application_id.setter
    def application_id(self, val):
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

    # }}}

    # {{{
    @property
    def uuid(self):

        for match in self.uuid_id_path(self.metadata):
            return self.get_text(match) or None

    @uuid.setter
    def uuid(self, val):
        matches = self.uuid_id_path(self.metadata)
        if not matches:
            attrib = {"{%s}scheme" % self.NAMESPACES["opf"]: "uuid"}
            matches = [self.create_metadata_element("identifier", attrib=attrib)]
        self.set_text(matches[0], six_unicode(val))

    # }}}

    # {{{
    @property
    def language(self):

        ans = self.languages
        if ans:
            return ans[0]

    @language.setter
    def language(self, val):
        self.languages = [val]

    # }}}

    # {{{
    @property
    def languages(self):

        ans = []
        for match in self.languages_path(self.metadata):
            t = self.get_text(match)
            if t and t.strip():
                l = canonicalize_lang(t.strip())
                if l:
                    ans.append(l)

        return ans

    @languages.setter
    def languages(self, val):

        matches = self.languages_path(self.metadata)
        for x in matches:
            x.getparent().remove(x)

        for lang in val:
            l = self.create_metadata_element("language")
            self.set_text(l, six_unicode(lang))

    # }}}

    @property
    def raw_languages(self):
        for match in self.languages_path(self.metadata):
            t = self.get_text(match)
            if t and t.strip():
                yield t.strip()

    # {{{
    @property
    def book_producer(self):

        for match in self.bkp_path(self.metadata):
            return self.get_text(match) or None

    @book_producer.setter
    def book_producer(self, val):
        matches = self.bkp_path(self.metadata)
        if not matches:
            matches = [self.create_metadata_element("contributor")]
            matches[0].set("{%s}role" % self.NAMESPACES["opf"], "bkp")
        self.set_text(matches[0], six_unicode(val))

    # }}}

    def identifier_iter(self):
        for item in self.identifier_path(self.metadata):
            yield item

    @property
    def raw_unique_identifier(self):
        uuid_elem = None
        for attr in self.root.attrib:
            if attr.endswith("unique-identifier"):
                uuid_elem = self.root.attrib[attr]
                break
        if uuid_elem:
            matches = self.root.xpath("//*[@id=%s]" % escape_xpath_attr(uuid_elem))
            if matches:
                for m in matches:
                    raw = m.text
                    if raw:
                        return raw

    @property
    def unique_identifier(self):
        raw = self.raw_unique_identifier
        if raw:
            return raw.rpartition(":")[-1]

    @property
    def page_progression_direction(self):
        spine = self.XPath('descendant::*[re:match(name(), "spine", "i")][1]')(self.root)
        if spine:
            for k, v in iteritems(spine[0].attrib):
                if k == "page-progression-direction" or k.endswith("}page-progression-direction"):
                    return v

    def guess_cover(self):
        """
        Try to guess a cover. Needed for some old/badly formed OPF files.
        :return:
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
    def epub3_raster_cover(self):
        for item in self.itermanifest():
            props = set((item.get("properties") or "").lower().split())
            if "cover-image" in props:
                mt = item.get("media-type", "")
                if mt and "xml" not in mt and "html" not in mt:
                    return item.get("href", None)

    @property
    def raster_cover(self):
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
                    if mt and "xml" not in mt and "html" not in mt:
                        return item.get("href", None)
        elif self.package_version >= 3.0:
            return self.epub3_raster_cover

    @property
    def guide_raster_cover(self):
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
    def epub3_nav(self):
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

    # {{{
    @property
    def cover(self):

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
        finally:
            return

    @cover.setter
    def cover(self, path):

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
        element_id = self.manifest.id_for_path(self.cover)
        if element_id is None:
            for t in ("cover", "other.ms-coverimage-standard", "other.ms-coverimage"):
                for item in self.guide:
                    if item.type.lower() == t:
                        self.create_manifest_item(item.href(), guess_type(path)[0])

    # }}}

    #
    # ----------------------------------------------------------------------------------------------------------------------

    def get_metadata_elements(self, name):
        """
        Returns all the elements corresponding to a given metadata name.
        :param name:
        :return:
        """
        matches = self.metadata_elem_path(self.metadata, name=name)
        return [m for m in matches]

    def get_metadata_element(self, name):
        """
        Returns metadata matches corresponding to the given name - if there are multiple matches returns the first
        one in the document.
        :param name:
        :return:
        """
        matches = self.metadata_elem_path(self.metadata, name=name)
        if matches:
            return matches[0]

    def create_metadata_element(self, name, attrib=None, is_dc=True):
        """
        Make a metadata element.
        :param name:
        :param attrib:
        :param is_dc: Is the metadata Dublin core
        :return:
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

    def render(self, encoding="utf-8"):

        for meta in self.raster_cover_path(self.metadata):
            # Ensure that the name attribute occurs before the content
            # attribute. Needed for Nooks.
            a = meta.attrib
            c = a.get("content", None)
            if c is not None:
                del a["content"]
                a["content"] = c

        # The PocketBook requires calibre:series_index to come after
        # calibre:series or it fails to read series info
        # We swap attributes instead of elements, as that avoids namespace
        # re-declarations
        smap = {}
        for child in self.metadata.xpath('./*[@name="calibre:series" or @name="calibre:series_index"]'):
            smap[child.get("name")] = (child, self.metadata.index(child))
        if len(smap) == 2 and smap["calibre:series"][1] > smap["calibre:series_index"][1]:
            s, si = smap["calibre:series"][0], smap["calibre:series_index"][0]

            def swap(attr):
                t = s.get(attr, "")
                s.set(attr, si.get(attr, "")), si.set(attr, t)

            swap("name"), swap("content")

        self.write_user_metadata()
        if pretty_print_opf:
            _pretty_print(self.root)
        raw = etree.tostring(self.root, encoding=encoding, pretty_print=True)
        if not raw.lstrip().startswith("<?xml "):
            raw = '<?xml version="1.0"  encoding="%s"?>\n' % encoding.upper() + raw
        return raw

    def smart_update(self, mi, replace_metadata=False, apply_null=False):
        """
        Update the metadata in the opf file - optionally setting the metadata null.
        :param mi: The metadata to
        :param replace_metadata:
        :param apply_null:
        :return:
        """
        # Validate the function input
        assert isinstance(mi, calibreMetadata), "Method can only run on calibreMetadata object"

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
            is_null = val is None or val in ((), [], (None, None), {}) or (attr == "rating" and val < 0.1)
            if is_null:
                if apply_null and attr in {
                    "series",
                    "tags",
                    "isbn",
                    "comments",
                    "publisher",
                    "rating",
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


class OPFCreator(calibreMetadata):
    def __init__(self, base_path, other):
        """
        Initialize.
        :param base_path: An absolute path to the directory in which this OPF file
        will eventually be. This is used by the L{create_manifest} method
        to convert paths to files into relative paths.
        :param other:
        :return:
        """
        if not isinstance(other, calibreMetadata):
            other = other.to_calibre()

        calibreMetadata.__init__(self, title="", other=other)
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

    def create_manifest(self, entries):
        """
        Create <manifest>
        :param entries: List of (path, mime-type) If mime-type is None it is autodetected
        :return:
        """
        entries = map(
            lambda x: x if os.path.isabs(x[0]) else (os.path.abspath(os.path.join(self.base_path, x[0])), x[1]),
            entries,
        )
        self.manifest = Manifest.from_paths(entries)
        self.manifest.set_basedir(self.base_path)

    def create_manifest_from_files_in(self, files_and_dirs, exclude=lambda x: False):
        entries = []

        def dodir(local_dir):
            for spec in os.walk(local_dir):
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

    def create_spine(self, entries):
        """
        Create the <spine> element. Must first call :method:`create_manifest`.
        :param entries: List of paths
        :return:
        """
        entries = map(
            lambda x: x if os.path.isabs(x) else os.path.abspath(os.path.join(self.base_path, x)),
            entries,
        )
        self.spine = Spine.from_paths(entries, self.manifest)

    def set_toc(self, toc):
        """
        Set the toc. You must call :method:`create_spine` before calling this method.
        :param toc: A :class:`TOC` object
        """
        self.toc = toc

    def create_guide(self, guide_element):
        self.guide = Guide.from_opf_guide(guide_element, self.base_path)
        self.guide.set_basedir(self.base_path)

    # Todo: Seems to be code duplications here
    def render(
        self,
        opf_stream=sys.stdout,
        ncx_stream=None,
        ncx_manifest_entry=None,
        encoding=None,
        process_guide=None,
    ):
        """
        Render the OPF file to the given opf_stream.
        :param opf_stream:
        :param ncx_stream:
        :param ncx_manifest_entry:
        :param encoding:
        :param process_guide:
        :return:
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
        from lxml.builder import ElementMaker
        from LiuXin.file_formats.oeb.base import OPF2_NS, DC11_NS, CALIBRE_NS

        DNS = OPF2_NS + "___xx___"

        E = ElementMaker(namespace=DNS, nsmap={None: DNS})
        M = ElementMaker(namespace=DNS, nsmap={"dc": DC11_NS, "calibre": CALIBRE_NS, "opf": OPF2_NS})
        DC = ElementMaker(namespace=DC11_NS)

        def DC_ELEM(tag, text, dc_attrs={}, opf_attrs={}):
            if text:
                elem = getattr(DC, tag)(clean_ascii_chars(text), **dc_attrs)
            else:
                elem = getattr(DC, tag)(**dc_attrs)
            for k, v in opf_attrs.items():
                elem.set("{%s}%s" % (OPF2_NS, k), v)
            return elem

        def CAL_ELEM(name, content):
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
                "%s (%s) [%s]" % (__appname__, __version__, "https://calibre-ebook.com"),
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
            try:
                a(DC_ELEM("date", self.pubdate.isoformat()))
            except AttributeError as e:
                # Sometimes timestamps are not stored in the right datetime format
                if not e.message == "'str' object has no attribute 'isoformat'":
                    raise
                a(DC_ELEM("date", self.pubdate))

        langs = self.languages

        if not langs or langs == ["und"]:
            langs = [get_lang().replace("_", "-").partition("-")[0]]

        for lang in langs:
            a(DC_ELEM("language", lang))

        if self.comments:
            a(DC_ELEM("description", self.comments))

        if self.publisher:
            a(DC_ELEM("publisher", self.publisher))

        for key, val in iteritems(self.get_identifiers()):
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

        # Todo: This isn't right - check where the error is
        if self.timestamp is not None:
            try:
                a(CAL_ELEM("calibre:timestamp", self.timestamp.isoformat()))
            except AttributeError as e:
                # Sometimes timestamps are not stored in the right datetime format
                if not e.message == "'str' object has no attribute 'isoformat'":
                    raise
                a(CAL_ELEM("calibre:timestamp", self.timestamp))

        if self.publication_type is not None:
            a(CAL_ELEM("calibre:publication_type", self.publication_type))

        if self.user_categories:
            from LiuXin.metadata.book.json_codec import object_to_unicode

            a(
                CAL_ELEM(
                    "calibre:user_categories",
                    json.dumps(object_to_unicode(self.user_categories)),
                )
            )

        manifest = E.manifest()
        if self.manifest is not None:
            for ref in self.manifest:
                href = ref.href()
                if isinstance(href, bytes):
                    href = href.decode("utf-8")
                item = E.item(id=str(ref.id), href=href)
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
        if process_guide is not None:
            process_guide(E, guide)

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


def metadata_to_opf(mi, as_string=True, default_lang=None):
    """
    Take a calibreMetadata/Metadata object and serialize it to a string.
    :param mi:
    :type mi: LiuXin or calibre Metadata object
    :param as_string: If True then serializes the object to a string and returns it - if False then returns the tree.
    :type as_string: bool
    :param default_lang:
    :return:
    """
    from lxml import etree
    import textwrap
    from LiuXin.file_formats.oeb.base import OPF, DC

    # Use a direct copy of the calibreMetadata method to dump a calibre metadata object.
    if isinstance(mi, calibreMetadata):
        return calibre_metadata_to_opf(mi, as_string=as_string, default_lang=default_lang)

    # Do pre-processing before writing the metadata
    if not mi.application_id:
        mi.application_id = str(uuid.uuid4())

    if not mi.uuid:
        mi.uuid = str(uuid.uuid4())

    # If the book producer is not already set make it the name and version
    if not mi.book_producer:
        mi.book_producer = __appname__ + " (%s) " % __version__ + "[https://calibre-ebook.com]"

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

    def factory(tag, text=None, sort=None, role=None, scheme=None, name=None, content=None):
        """
        Adds an element to the OPF file.
        :param tag:
        :param text:
        :param sort:
        :param role:
        :param scheme:
        :param name:
        :param content:
        :return:
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
            elem = metadata.makeelement(tag, attrib={k: clean_xml_chars(v) for k, v in iteritems(attrib)})
        elem.tail = "\n" + (" " * 8)
        if text:
            try:
                elem.text = text.strip()
            except ValueError:
                elem.text = clean_ascii_chars(text.strip())
        metadata.append(elem)

    # Some fields aren't generally recognized - note that these are LiuXin metadata objects
    meta = lambda n, c: factory("meta", name="calibre:" + n, content=c)
    meta_text = lambda n, c, t: factory("meta", name="calibre:" + n, content=c, text=t)
    meta_scheme = lambda n, c, s: factory("meta", name="calibre:" + n, content=c, scheme=s)

    # Put the title and the creators are the top
    factory(DC("title"), mi.title)

    # CREATORS
    # Process authors first - so that they are place right after the title - then write out all other contributors
    # Some ebook readers just look for and use the first creator element - so make sure that the creators are first
    authors = mi.authors
    for au in authors:
        factory(tag=DC("creator"), text=au, sort=mi.creator_sort, role="aut")
    # Now write all the other creator roles out - remove the authors category so that it isn't written out twice
    creator_types = deepcopy(CREATOR_CATEGORIES)
    creator_types.remove("authors")
    for category in creator_types:
        for cr in mi.get(category):
            factory(
                tag=DC("creator"),
                text=cr,
                sort=author_to_author_sort(cr),
                role=creator_to_marc(category),
            )
    # --------

    # AUTHOR LINK MAP
    # Keyed with the author name and valued with the creator_id from the creators table - should uniquely specify the
    # id of that creator on the database (as creator names are unique)
    # Todo: Make sure that this is produced when reading metadata from the database
    if getattr(mi, "author_link_map", None) is not None:
        meta("author_link_map", dump_dict(mi.author_link_map))
    # ---------------

    # BOOK PRODUCER
    factory(DC("contributor"), mi.program_str, __appname__, "bkp")
    # -------------

    # CATEGORY
    # Mostly will be ebooks
    if hasattr(mi, "category") and mi.category:
        factory(DC("type"), mi.category)
    # --------

    # COMMENTS
    mi_comments = mi.comments
    for comment in mi_comments.keys():
        meta("user_comment", clean_ascii_chars(comment))
    # --------

    # DOC TYPE
    mi_doc_type = mi.doc_type
    meta("doc_type", mi_doc_type)
    # --------

    # FILENAME
    # Records the original names of the files that have gone into the book
    # Files names are recorded in the order that they where added in
    mi_filenames = mi.filename
    if mi_filenames:
        if isinstance(mi_filenames, list):
            for filename in mi_filenames:
                meta("original_filename", filename)
        elif isinstance(mi_filenames, six_string_types):
            meta("original_filename", mi_filenames)
        else:
            raise NotImplementedError("cannot parse filenames in this form")
    # --------

    # FILEPATH
    # Records the original paths to the files that have gone into the book
    mi_filepaths = mi.filepath
    if mi_filepaths:
        if isinstance(mi_filepaths, list):
            for filepath in mi_filepaths:
                meta("original_filepath", filepath)
        elif isinstance(mi_filepaths, six_string_types):
            meta("original_filepaths", mi_filepaths)
        else:
            raise NotImplementedError("cannot parse filepaths in this form")
    # --------

    # GENRES
    # DC type is reserved for a more general explanation of the type of the document - so using a custom field instead
    mi_genres = mi.genre
    for genre in mi_genres:
        meta("genre", genre)
    # ------

    # IDENTIFIERS
    for key, val in iteritems(mi.get_identifiers()):
        for id_val in val:
            factory(tag=DC("identifier"), text=id_val, scheme=icu_upper(key))
    # -----------

    # IMPRINT
    # add imprints as publishers - with the content note that they are an imprint
    mi_imprint = mi.imprint
    for imp in mi_imprint.keys():
        meta("imprint", imp)
    # -------

    # LANGUAGES
    # Be sure to put the primary language first - as many ebook readers will just look to the first language tag and
    # might accidentally match a custom language metadata type - may as well put it at the top just in case
    # Set the primary language of the work using the Dublin core language keyword
    if mi.language:
        factory(DC("language"), text=mi.language)

    # Set the languages in the work using the meta language field
    for lang in mi.languages:
        if not lang or lang.lower() == "und":
            continue
        meta("inc_lang", lang)

    # Note the languages available in the work - these are complete translations of the work available in the document
    mi_languages_available = mi.languages_available
    for lang in mi_languages_available.keys():
        if not lang or lang.lower() == "und":
            continue
        factory("available_lang", lang)
    # ---------

    # Todo: Really last modified should be updated every time one of the constituent peices of metadata has changes - work out how to do this on the database
    # LAST MODIFIED
    # When was the work that this OPF contains serialized metadata from last modified?
    mi_last_modified = mi.last_modified
    if hasattr(mi, "last_modified") and mi_last_modified:
        meta("last_modified", six_unicode(mi_last_modified))
    # -------------

    # METADATA DATE
    mi_metadata_date = mi.metadata_date
    if hasattr(mi, "metadata_date") and mi_metadata_date:
        meta("metadata_date", six_unicode(mi_metadata_date))
    # -------------

    # METADATA LANGUAGE
    mi_metadata_language = mi.metadata_language
    if hasattr(mi, "metadata_language") and mi_metadata_language and mi_metadata_language != "und":
        meta("metadata_language", six_unicode(mi_metadata_language))
    # -----------------

    # NOTES
    notes = mi.notes
    for note in notes.keys():
        meta("note", note)
    # -----

    # PUBDATE
    if hasattr(mi.pubdate, "isoformat"):
        factory(DC("date"), isoformat(mi.pubdate))
    # -------

    # PUBLISHERS
    # Write the publishers out - in the same order as they currently appear in the metadata object
    mi_publishers = mi.publisher
    for pub_str in mi_publishers.keys():
        factory(tag=DC("publisher"), text=pub_str, role="pbl")
    # ----------

    # PUBLICATION TYPE
    if hasattr(mi, "publication_type"):
        meta("publication_type", mi.publication_type)
    # ----------------

    # RATINGS
    mi_ratings = mi.ratings
    for rat_typ in mi_ratings.keys():
        meta_scheme("rating", six_unicode(mi_ratings[rat_typ]), rat_typ)
    # -------

    # RIGHTS
    if mi.rights:
        factory(DC("rights"), mi.rights)
    # ------

    # SERIES
    mi_series = mi.series
    # The first element in the opf object should be the highest priority series
    for series in mi_series.keys():
        meta("series", series)
    # ------

    # SERIES INDEX
    mi_series_index = mi.series_index
    for series_name in mi_series_index.keys():
        meta_text(
            "series_index",
            mi.format_series_index(mi_series_index[series_name]),
            "series:{}".format(series_name),
        )
    # ------------

    # SUBJECT
    # subjects are being used for the tag field - so the actual subject is stored as custom_subject
    mi_subjects = mi.subject
    for subject in mi_subjects:
        meta("custom_subject", subject)
    # -------

    # SYNOPSIS
    mi_synopses = mi.synopses
    for synopsis in mi_synopses:
        meta("synopsis", synopsis)
    # --------

    # TAGS
    # By default assume that all tags present are title tags
    if mi.tags:
        for tag in mi.tags:
            factory(DC("subject"), tag)
    # ----

    # TIMESTAMP
    if hasattr(mi.timestamp, "isoformat"):
        meta("timestamp", isoformat(mi.timestamp))
    # ---------

    # TITLE ID
    if hasattr(mi, "title_id") and mi.title_id:
        meta("title_id", mi.title_id)
    # --------

    # TITLE SORT
    if mi.title_sort:
        meta("title_sort", mi.title_sort)
    # ----------

    # USER CATEGORIES
    if hasattr(mi, "user_categories"):
        meta("user_categories", dump_dict(mi.user_categories))
    # ---------------

    # WORDCOUNT
    if hasattr(mi, "wordcount") and not mi.is_null("wordcount"):
        meta("wordcount", six_unicode(mi.wordcount))
    # ---------

    serialize_user_metadata(metadata, mi.get_all_user_metadata(False))

    metadata[-1].tail = "\n" + (" " * 4)

    # COVERS
    if hasattr(mi, "cover") and mi.cover:
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
    # ------

    if as_string:
        return etree.tostring(root, pretty_print=True, encoding="utf-8", xml_declaration=True) if as_string else root
    else:
        return root


def calibre_metadata_to_opf(mi, as_string=True, default_lang=None):
    """
    Write a calibre metadata instance out to an opf file.
    :param mi:
    :type mi: calibreMetadata object
    :param as_string:
    :type as_string: bool
    :param default_lang:
    :return:
    """
    from lxml import etree
    import textwrap
    from LiuXin.file_formats.oeb.base import OPF, DC

    if not mi.application_id:
        mi.application_id = str(uuid.uuid4())

    if not mi.uuid:
        mi.uuid = str(uuid.uuid4())

    if not mi.book_producer:
        mi.book_producer = __appname__ + " (%s) " % __version__ + "[https://calibre-ebook.com]"

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

    def factory(tag, text=None, sort=None, role=None, scheme=None, name=None, content=None):
        """
        Add an element to the opf.
        :param tag:
        :param text:
        :param sort:
        :param role:
        :param scheme:
        :param name:
        :param content:
        :return:
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

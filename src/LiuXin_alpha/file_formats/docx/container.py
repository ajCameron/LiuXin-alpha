#!/usr/bin/env python2
# vim:fileencoding=utf-8

from __future__ import unicode_literals, division, absolute_import, print_function

import os
import re
import shutil
import sys

from lxml import etree

from LiuXin_alpha.file_formats.docx import InvalidDOCX
from LiuXin_alpha.file_formats.archive_preflight import (
    normalized_zip_member_name,
    validate_zip_member_infos,
)
from LiuXin_alpha.file_formats.docx.names import DOCXNamespace
from LiuXin_alpha.file_formats.oeb.parse_utils import RECOVER_PARSER

from LiuXin_alpha.metadata.ebook_metadata_tools import string_to_authors
from LiuXin_alpha.metadata.containers.calibre_like_book_metadata import (
    CalibreLikeLiuXinBookMetaData as Metadata,
)
from LiuXin_alpha.metadata.ebook_metadata_tools import authors_to_sort_string

from LiuXin_alpha.utils.mine_types import guess_type
from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.localization import canonicalize_lang
from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.utils.ptempfiles import PersistentTemporaryDirectory
from LiuXin_alpha.utils.libraries.calibre_zipfile import ZipFile

from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode

__license__ = "GPL v3"
__copyright__ = "2013, Kovid Goyal <kovid at kovidgoyal.net>"


def walk(path):
    for base, _dirnames, filenames in os.walk(path):
        for filename in filenames:
            yield os.path.join(base, filename)


def fromstring(raw, parser=RECOVER_PARSER):
    return etree.fromstring(raw, parser=parser)


def _local_name(tag):
    return str(tag).rsplit("}", 1)[-1]


def _malformed_part(name, part_name):
    raise InvalidDOCX("The file %s docx file has malformed %s" % (name, part_name))


def _parse_required_xml_part(raw, name, part_name, root_name):
    try:
        root = fromstring(raw)
    except Exception as err:
        raise InvalidDOCX("The file %s docx file has malformed %s" % (name, part_name)) from err
    if root is None or _local_name(root.tag) != root_name:
        _malformed_part(name, part_name)
    return root


# Read metadata {{{
def read_doc_props(raw, mi, XPath):
    """
    Read the document metadata
    :param raw: The raw metadata string to parse
    :param mi: The metadata object to write the return out to
    :param XPath: The handler to parse the metadata
    :return:
    """
    root = fromstring(raw)

    titles = XPath("//dc:title")(root)
    if titles:
        title = titles[0].text
        if title and title.strip():
            mi.title = title.strip()

    # Include both the subject (if any) and the keywords in the metadata tags
    tags = []
    for subject in XPath("//dc:subject")(root):
        if subject.text and subject.text.strip():
            tags.append(subject.text.strip().replace(",", "_"))
    for keywords in XPath("//cp:keywords")(root):
        if keywords.text and keywords.text.strip():
            for x in keywords.text.split():
                # When writing tags out this encoding is used to replace spaces - otherwise the above will split the
                # tags on spaces
                x = re.sub(r"_-_", " ", x)
                tags.extend(y.strip() for y in x.split(",") if y.strip())
    if tags:
        mi.tags = tags

    authors = XPath("//dc:creator")(root)
    aut = []
    for author in authors:
        if author.text and author.text.strip():
            aut.extend(string_to_authors(author.text))
    if aut:
        mi.authors = aut
        mi.author_sort = authors_to_sort_string(aut)

    desc = XPath("//dc:description")(root)
    if desc:
        raw = etree.tostring(desc[0], method="text", encoding=six_unicode)
        raw = raw.replace("_x000d_", "")  # Word 2007 mangles newlines in the summary
        mi.comments = raw.strip()

    langs = []
    for lang in XPath("//dc:language")(root):
        if lang.text and lang.text.strip():
            l = canonicalize_lang(lang.text)
            if l:
                langs.append(l)
    if langs:
        mi.languages = langs


def read_app_props(raw, mi):
    root = fromstring(raw)
    company = root.xpath('//*[local-name()="Company"]')
    if company and company[0].text and company[0].text.strip():
        mi.publisher = company[0].text.strip()


def read_default_style_language(raw, mi, XPath):
    root = fromstring(raw)
    for lang in XPath("/w:styles/w:docDefaults/w:rPrDefault/w:rPr/w:lang/@w:val")(root):
        lang = canonicalize_lang(lang)
        if lang:
            mi.languages = [lang]
            break


# }}}


class DOCX(object):
    """
    Class representing a DocX file.
    """

    required_members = ("[Content_Types].xml", "_rels/.rels")
    max_archive_members = 4096
    max_member_uncompressed_size = 256 * 1024 * 1024
    max_total_uncompressed_size = 512 * 1024 * 1024
    max_compression_ratio = 1000
    min_compression_ratio_check_size = 1024 * 1024

    def __init__(self, path_or_stream, log=None, extract=True):
        self.docx_is_transitional = True
        stream = path_or_stream if hasattr(path_or_stream, "read") else open(path_or_stream, "rb")
        self.name = getattr(stream, "name", None) or "<stream>"
        self.log = log or default_log
        if extract:
            self.extract(stream)
        else:
            self.init_zipfile(stream)
        self.read_content_types()
        self.read_package_relationships()
        self.namespace = DOCXNamespace(self.docx_is_transitional)

    def init_zipfile(self, stream):
        self.validate_container_members(stream)
        self.zipf = ZipFile(stream)
        self.names = frozenset(self.zipf.namelist())

    def normalized_archive_member_name(self, name):
        return normalized_zip_member_name(
            name,
            member_label="DOCX archive",
            error_type=InvalidDOCX,
        )

    def validate_container_members(self, stream):
        stream.seek(0)
        try:
            zf = ZipFile(stream, "r")
        except Exception as err:
            stream.seek(0)
            raise InvalidDOCX("DOCX appears to be invalid ZIP file") from err

        try:
            names = set(
                validate_zip_member_infos(
                    zf.infolist(),
                    container_label="DOCX file",
                    member_label="DOCX archive",
                    error_type=InvalidDOCX,
                    max_archive_members=self.max_archive_members,
                    max_member_uncompressed_size=self.max_member_uncompressed_size,
                    max_total_uncompressed_size=self.max_total_uncompressed_size,
                    max_compression_ratio=self.max_compression_ratio,
                    min_compression_ratio_check_size=self.min_compression_ratio_check_size,
                )
            )

            missing = [name for name in self.required_members if name not in names]
            if missing:
                if missing == ["[Content_Types].xml"]:
                    raise InvalidDOCX("The file %s docx file has no [Content_Types].xml" % self.name)
                if missing == ["_rels/.rels"]:
                    raise InvalidDOCX("The file %s docx file has no _rels/.rels" % self.name)
                raise InvalidDOCX("DOCX file is missing required member(s): %s" % ", ".join(missing))
        finally:
            zf.close()
            stream.seek(0)

    def extract(self, stream):
        self.validate_container_members(stream)
        self.tdir = PersistentTemporaryDirectory("docx_container")
        try:
            zf = ZipFile(stream)
            zf.extractall(self.tdir)
        except:
            self.log.exception("DOCX appears to be invalid ZIP file, trying a more forgiving ZIP parser")
            from LiuXin_alpha.utils.decompression.localunzip import extractall

            stream.seek(0)
            extractall(stream, self.tdir)

        self.names = {}
        for f in walk(self.tdir):
            name = os.path.relpath(f, self.tdir).replace(os.sep, "/")
            self.names[name] = f

    def exists(self, name):
        return name in self.names

    def read(self, name):
        if hasattr(self, "zipf"):
            return self.zipf.open(name).read()
        path = self.names[name]
        with open(path, "rb") as f:
            return f.read()

    def read_content_types(self):
        try:
            raw = self.read("[Content_Types].xml")
        except KeyError:
            raise InvalidDOCX("The file %s docx file has no [Content_Types].xml" % self.name)
        root = _parse_required_xml_part(raw, self.name, "[Content_Types].xml", "Types")
        self.content_types = {}
        self.default_content_types = {}
        for item in root.xpath('//*[local-name()="Types"]/*[local-name()="Default" and @Extension and @ContentType]'):
            self.default_content_types[item.get("Extension").lower()] = item.get("ContentType")
        for item in root.xpath('//*[local-name()="Types"]/*[local-name()="Override" and @PartName and @ContentType]'):
            name = item.get("PartName").lstrip("/")
            self.content_types[name] = item.get("ContentType")

    def content_type(self, name):
        if name in self.content_types:
            return self.content_types[name]
        ext = name.rpartition(".")[-1].lower()
        if ext in self.default_content_types:
            return self.default_content_types[ext]
        return guess_type(name)[0]

    def read_package_relationships(self):
        try:
            raw = self.read("_rels/.rels")
        except KeyError:
            raise InvalidDOCX("The file %s docx file has no _rels/.rels" % self.name)
        root = _parse_required_xml_part(raw, self.name, "_rels/.rels", "Relationships")
        self.relationships = {}
        self.relationships_rmap = {}
        for item in root.xpath(
            '//*[local-name()="Relationships"]/*[local-name()="Relationship" and ' "@Type and @Target]"
        ):
            target = item.get("Target").lstrip("/")
            typ = item.get("Type")
            if target == "word/document.xml":
                self.docx_is_transitional = (
                    typ != "http://purl.oclc.org/ooxml/officeDocument/relationships/" "officeDocument"
                )
            self.relationships[typ] = target
            self.relationships_rmap[target] = typ

    @property
    def document_name(self):
        name = self.relationships.get(self.namespace.names["DOCUMENT"], None)
        if name is None:
            names = tuple(n for n in self.names if n == "document.xml" or n.endswith("/document.xml"))
            if not names:
                raise InvalidDOCX("The file %s docx file has no main document" % self.name)
            name = names[0]
        if name not in self.names:
            raise InvalidDOCX("The file %s docx file has no main document" % self.name)
        return name

    @property
    def document(self):
        name = self.document_name
        return _parse_required_xml_part(self.read(name), self.name, name, "document")

    @property
    def document_relationships(self):
        return self.get_relationships(self.document_name)

    def get_relationships(self, name):
        base = "/".join(name.split("/")[:-1])
        by_id, by_type = {}, {}
        parts = name.split("/")
        name = "/".join(parts[:-1] + ["_rels", parts[-1] + ".rels"])
        try:
            raw = self.read(name)
        except KeyError:
            pass
        else:
            root = _parse_required_xml_part(raw, self.name, name, "Relationships")
            for item in root.xpath(
                '//*[local-name()="Relationships"]/*[local-name()="Relationship" ' "and @Type and @Target]"
            ):
                target = item.get("Target")
                if item.get("TargetMode", None) != "External" and not target.startswith("#"):
                    target = "/".join((base, target.lstrip("/")))
                typ = item.get("Type")
                local_id = item.get("Id")
                by_id[local_id] = by_type[typ] = target

        return by_id, by_type

    def get_document_properties_names(self):
        name = self.relationships.get(self.namespace.names["DOCPROPS"], None)
        if name is None:
            # core.xml is where the metadata for the document is stored - is a (subset) of the Dublin Core metadata
            names = tuple(n for n in self.names if n.lower() == "docprops/core.xml")
            if names:
                name = names[0]
        yield name

        name = self.relationships.get(self.namespace.names["APPPROPS"], None)
        if name is None:
            names = tuple(n for n in self.names if n.lower() == "docprops/app.xml")
            if names:
                name = names[0]
        yield name

    @property
    def metadata(self):
        """
        Return the metadata for this file.
        :return:
        :rtype: LiuXin Metadata
        """
        mi = Metadata(_("Unknown"))
        dp_name, ap_name = self.get_document_properties_names()

        if dp_name:
            try:
                raw = self.read(dp_name)
            except KeyError:
                pass
            else:
                try:
                    read_doc_props(raw, mi, self.namespace.XPath)
                except Exception as err:
                    raise InvalidDOCX("The file %s docx file has malformed %s" % (self.name, dp_name)) from err

        if mi.is_null("language"):
            try:
                raw = self.read("word/styles.xml")
            except KeyError:
                pass
            else:
                try:
                    read_default_style_language(raw, mi, self.namespace.XPath)
                except Exception as err:
                    raise InvalidDOCX("The file %s docx file has malformed word/styles.xml" % self.name) from err

        ap_name = self.relationships.get(self.namespace.names["APPPROPS"], None)
        if ap_name:
            try:
                raw = self.read(ap_name)
            except KeyError:
                pass
            else:
                try:
                    read_app_props(raw, mi)
                except Exception as err:
                    raise InvalidDOCX("The file %s docx file has malformed %s" % (self.name, ap_name)) from err

        return mi

    def close(self):
        if hasattr(self, "zipf"):
            self.zipf.close()
        else:
            try:
                shutil.rmtree(self.tdir)
            except EnvironmentError:
                pass


if __name__ == "__main__":
    d = DOCX(sys.argv[-1], extract=False)
    print(d.metadata)

from __future__ import with_statement, print_function

import zipfile
from copy import deepcopy

import os
import re
import posixpath

from contextlib import closing

from LiuXin.constants import isosx

# Should be moved over into utils and merged
from LiuXin.file_formats.BeautifulSoup import BeautifulStoneSoup
from LiuXin.file_formats.opf.opf import (
    get_metadata as get_metadata_from_opf,
    set_metadata as set_metadata_opf,
)
from LiuXin.file_formats.opf.opf2 import OPF

from LiuXin.metadata.book.base import calibreMetadata
from LiuXin.metadata.metadata import MetaData

from LiuXin.utils.calibre import CurrentDir, walk
from LiuXin.utils.file_ops.file_ops import local_open as lopen
from LiuXin.utils.iso639.iso639_tools import lang_as_iso639_1
from LiuXin.utils.decompression.localunzip import LocalZipFile
from LiuXin.utils.ptempfiles import TemporaryDirectory, PersistentTemporaryFile
from LiuXin.utils.calibre_utils.calibre_zipfile import ZipFile, BadZipfile, safe_replace

# Py2/Py3 compatibility layer
from LiuXin.utils.lx_libraries.liuxin_six import six_map
from LiuXin.utils.lx_libraries.liuxin_six import six_cStringIO as StringIO

__license__ = "GPL v3"
__copyright__ = "2008, Kovid Goyal <kovid at kovidgoyal.net>"

"""Read meta information from epub files"""

# Controls whether or not this method should just return the file as a raw dictionary
# if set to True then it returns a dictionary keyed by the values. If False it returns a MetaData object
__dictionary_return__ = False
__author__ = "Cameron"


VALID_FOR = ["EPUB"]
PRIORITY_FOR = ["EPUB"]
RUN_COST = ["LOW"]

# ----------------------------------------------------------------------------------------------------------------------
#
# - EXCEPTIONS


class EpubParseError(Exception):
    pass


class EPubException(Exception):
    pass


class OCFException(EPubException):
    pass


class ContainerException(OCFException):
    pass


#
# ----------------------------------------------------------------------------------------------------------------------


def get_metadata_inplace(target_epub_path):
    """
    Extract metadata from the given on_disk file.
    Extracts the opf file and passes it over to the opf method for metadata extraction.
    :param target_epub_path:
    :return:
    """
    target_epub_path = deepcopy(target_epub_path)

    epub_in = zipfile.ZipFile(target_epub_path, "r")
    opf_pat = re.compile(r".*.opf", re.IGNORECASE)
    opf_files = [thing for thing in epub_in.namelist() if opf_pat.match(thing)]

    # Todo: Account for the possibility of multiple OPF files with the MetaData merge functionality. Coming soon.
    if len(opf_files) == 0:
        raise EpubParseError("OPF file not found")
    elif len(opf_files) == 1:
        opf_file = epub_in.read(opf_files[0])
        from LiuXin.metadata.file_sources.opf import get_metadata

        metadata_return = get_metadata(opf_file, text=True)
    else:
        raise EpubParseError("Too many OPF files found.")

    return metadata_return


def get_metadata(stream, extract_cover=True, calibre_metadata=True):
    """
    Return metadata as a :class:`Metadata` object
    :param stream:
    :param extract_cover:
    :param calibre_metadata: If True then returns the metadata as a calibreMetaData object.
    :return:
    """
    stream.seek(0)

    reader = get_zip_reader(stream)
    opfbytes = reader.read_bytes(reader.opf_path)
    mi, ver, raster_cover, first_spine_item = get_metadata_from_opf(opfbytes)
    assert isinstance(mi, calibreMetadata), "Return from get_metadata_from_opf was of unexpected type"

    if not calibre_metadata:
        # Use the LiuXin opf parser to read the metadata again
        from LiuXin.file_formats.opf.opf import DummyFile, parse_opf
        from LiuXin.metadata.file_sources.opf import get_metadata as opf_get_metadata

        # Parse the opf file and render it into a form suitable for use with the metadata reader
        opfbytes = DummyFile(opfbytes)
        root = parse_opf(opfbytes)

        mi = opf_get_metadata(target_file=root, file_is_raw_root=True, seek_md_node=False)
        assert isinstance(mi, MetaData), "Retrun from opf_get_metadata was not a LiuXin MetaData object"

    if extract_cover:
        base = posixpath.dirname(reader.opf_path)

        if raster_cover:
            raster_cover = posixpath.normpath(posixpath.join(base, raster_cover))
        if first_spine_item:
            first_spine_item = posixpath.normpath(posixpath.join(base, first_spine_item))

        try:
            cdata = get_cover(raster_cover, first_spine_item, reader)
            if cdata is not None:
                mi.cover_data = ("jpg", cdata)
        except RuntimeError:
            raise
        except Exception:
            import traceback

            traceback.print_exc()

    mi.timestamp = None

    return mi


def get_quick_metadata(stream):
    """
    Get metadata without extracting the cover.
    :param stream:
    :return:
    """
    return get_metadata(stream, False)


class Container(dict):
    """
    Contains the elements of an epub file.
    """

    def __init__(self, stream=None):
        if not stream:
            return

        soup = BeautifulStoneSoup(stream.read())
        container = soup.find(name=re.compile(r"container$", re.I))
        if not container:
            raise OCFException("<container> element missing")
        if container.get("version", None) != "1.0":
            raise EPubException("unsupported version of OCF")

        rootfiles = container.find(re.compile(r"rootfiles$", re.I))

        if not rootfiles:
            raise EPubException("<rootfiles/> element missing")

        for rootfile in rootfiles.findAll(re.compile(r"rootfile$", re.I)):
            try:
                self[rootfile["media-type"]] = rootfile["full-path"]
            except KeyError:
                raise EPubException("<rootfile/> element malformed")


class OCF(object):
    MIMETYPE = "application/epub+zip"
    CONTAINER_PATH = "META-INF/container.xml"
    ENCRYPTION_PATH = "META-INF/encryption.xml"

    def __init__(self):
        raise NotImplementedError("Abstract base class")


class Encryption(object):

    OBFUSCATION_ALGORITHMS = frozenset(["http://ns.adobe.com/pdf/enc#RC", "http://www.idpf.org/2008/embedding"])

    def __init__(self, raw):
        from lxml import etree

        self.root = etree.fromstring(raw) if raw else None
        self.entries = {}
        if self.root is not None:
            for em in self.root.xpath('descendant::*[contains(name(), "EncryptionMethod")]'):
                algorithm = em.get("Algorithm", "")
                cr = em.getparent().xpath('descendant::*[contains(name(), "CipherReference")]')
                if cr:
                    uri = cr[0].get("URI", "")
                    if uri and algorithm:
                        self.entries[uri] = algorithm

    def is_encrypted(self, uri):
        algo = self.entries.get(uri, None)
        return algo is not None and algo not in self.OBFUSCATION_ALGORITHMS


class OCFReader(OCF):
    def __init__(self):
        try:
            mimetype = self.open("mimetype").read().rstrip()
            if mimetype != OCF.MIMETYPE:
                print("WARNING: Invalid mimetype declaration", mimetype)
        except:
            print("WARNING: Epub doesn't contain a mimetype declaration")

        try:
            with closing(self.open(OCF.CONTAINER_PATH)) as f:
                self.container = Container(f)
        except KeyError:
            raise EPubException("missing OCF container.xml file")
        self.opf_path = self.container[OPF.MIMETYPE]
        if not self.opf_path:
            raise EPubException("missing OPF package file entry in container")
        self._opf_cached = self._encryption_meta_cached = None

    @property
    def opf(self):
        if self._opf_cached is None:
            try:
                with closing(self.open(self.opf_path)) as f:
                    self._opf_cached = OPF(f, self.root, populate_spine=False)
            except KeyError:
                raise EPubException("missing OPF package file")
        return self._opf_cached

    @property
    def encryption_meta(self):
        if self._encryption_meta_cached is None:
            try:
                with closing(self.open(self.ENCRYPTION_PATH)) as f:
                    self._encryption_meta_cached = Encryption(f.read())
            except:
                self._encryption_meta_cached = Encryption(None)
        return self._encryption_meta_cached

    def read_bytes(self, name):
        return self.open(name).read()


class OCFZipReader(OCFReader):
    def __init__(self, stream, mode="r", root=None):
        if isinstance(stream, (LocalZipFile, ZipFile)):
            self.archive = stream
        else:
            try:
                self.archive = ZipFile(stream, mode=mode)
            except BadZipfile:
                raise EPubException("not a ZIP .epub OCF container")
        self.root = root
        if self.root is None:
            name = getattr(stream, "name", False)
            if name:
                self.root = os.path.abspath(os.path.dirname(name))
            else:
                self.root = os.getcwdu()
        super(OCFZipReader, self).__init__()

    def open(self, name, mode="r"):
        if isinstance(self.archive, LocalZipFile):
            return self.archive.open(name)
        return StringIO(self.archive.read(name))

    def read_bytes(self, name):
        return self.archive.read(name)


def get_zip_reader(stream, root=None):
    """
    Try opening a zip file with the main reader - if this fails fall back on a more forgiving parser.
    :param stream:
    :param root:
    :return:
    """
    try:
        zf = ZipFile(stream, mode="r")
    except:
        stream.seek(0)
        # B&N ship broken EPUB files - handling them with a more forgiving parser
        zf = LocalZipFile(stream)
    return OCFZipReader(zf, root=root)


class OCFDirReader(OCFReader):
    def __init__(self, path):
        self.root = path
        super(OCFDirReader, self).__init__()

    def open(self, path, *args, **kwargs):
        return open(os.path.join(self.root, path), *args, **kwargs)


def render_cover(opf, opf_path, zf, reader=None):
    """
    Render the cover from the opf file.
    :param opf:
    :param opf_path:
    :param zf:
    :param reader:
    :return:
    """
    from LiuXin.file_formats import render_html_svg_workaround

    from LiuXin.utils.logger import default_log

    cpage = opf.first_spine_item()
    if not cpage:
        return
    if reader is not None and reader.encryption_meta.is_encrypted(cpage):
        return

    with TemporaryDirectory("_epub_meta") as tdir:
        with CurrentDir(tdir):
            zf.extractall()
            opf_path = opf_path.replace("/", os.sep)
            cpage = os.path.join(tdir, os.path.dirname(opf_path), cpage)
            if not os.path.exists(cpage):
                return

            # Original calibre
            # zf.extractall()
            # cpage = os.path.join(tdir, cpage)
            # if not os.path.exists(cpage):
            #     return

            if isosx:
                # On OS X trying to render a HTML cover which uses embedded fonts more than once in the same process
                # causes a crash in Qt so be safe and remove the fonts as well as any @font-face rules
                for f in walk("."):
                    if os.path.splitext(f)[1].lower() in (".ttf", ".otf"):
                        os.remove(f)
                ffpat = re.compile(rb"@font-face.*?{.*?}", re.DOTALL | re.IGNORECASE)
                with lopen(cpage, "r+b") as f:
                    raw = f.read()
                    f.truncate(0)
                    f.seek(0)
                    raw = ffpat.sub(b"", raw)
                    f.write(raw)

                from LiuXin.utils.calibre_chardet import xml_to_unicode

                raw = xml_to_unicode(raw, strip_encoding_pats=True, resolve_entities=True)[0]
                from lxml import html

                for link in html.fromstring(raw).xpath("//link"):
                    href = link.get("href", "")
                    if href:
                        path = os.path.join(os.path.dirname(cpage), href)
                        if os.path.exists(path):
                            with lopen(path, "r+b") as f:
                                raw = f.read()
                                f.truncate(0)
                                f.seek(0)
                                raw = ffpat.sub(b"", raw)
                                f.write(raw)

            return render_html_svg_workaround(cpage, default_log)


def get_cover_from_disk(opf, opf_path, stream, reader=None):
    raster_cover = opf.raster_cover
    stream.seek(0)
    try:
        zf = ZipFile(stream)
    except:
        stream.seek(0)
        zf = LocalZipFile(stream)

    if raster_cover:
        base = posixpath.dirname(opf_path)
        cpath = posixpath.normpath(posixpath.join(base, raster_cover))
        if reader is not None and reader.encryption_meta.is_encrypted(cpath):
            return

        try:
            member = zf.getinfo(cpath)
        except:
            pass
        else:
            f = zf.open(member)
            data = f.read()
            f.close()
            zf.close()
            return data

    return render_cover(opf, opf_path, zf, reader=reader)


def get_cover(raster_cover, first_spine_item, reader):
    zf = reader.archive

    if raster_cover:
        if reader.encryption_meta.is_encrypted(raster_cover):
            return
        try:
            member = zf.getinfo(raster_cover)
        except Exception:
            pass
        else:
            f = zf.open(member)
            data = f.read()
            f.close()
            zf.close()
            return data

    return render_cover(first_spine_item, zf, reader=reader)


def serialize_cover_data(new_cdata, cpath):
    from LiuXin.utils.img import save_cover_data_to

    return save_cover_data_to(new_cdata, data_fmt=os.path.splitext(cpath)[1][1:])


def _write_new_cover(new_cdata, cpath):
    """
    Write the replacement cover into the epub file
    :param new_cdata:
    :param cpath:
    :return:
    """
    from LiuXin.utils.magick.draw import save_cover_data_to

    new_cover = PersistentTemporaryFile(suffix=os.path.splitext(cpath)[1])
    new_cover.close()
    save_cover_data_to(new_cdata, new_cover.name)
    return new_cover


def normalize_languages(opf_languages, mi_languages):
    """
    Preserve original country codes and use 2-letter lang codes where possible.
    :param opf_languages:
    :param mi_languages:
    :return:
    """
    from LiuXin.utils.spell import parse_lang_code

    def parse(x):
        try:
            return parse_lang_code(x)
        except ValueError:
            return None

    opf_languages = filter(None, six_map(parse, opf_languages))
    cc_map = {c.langcode: c.countrycode for c in opf_languages}
    mi_languages = filter(None, six_map(parse, mi_languages))

    def norm(x):
        lc = x.langcode
        cc = x.countrycode or cc_map.get(lc, None)
        lc = lang_as_iso639_1(lc) or lc
        if cc:
            lc += "-" + cc
        return lc

    return list(six_map(norm, mi_languages))


def update_metadata(opf, mi, apply_null=False, update_timestamp=False, force_identifiers=False):
    """
    Update the metadata in the file.
    :param opf:
    :param mi:
    :param apply_null:
    :param update_timestamp:
    :param force_identifiers:
    :return:
    """
    for x in ("guide", "toc", "manifest", "spine"):
        setattr(mi, x, None)

    if mi.languages:
        mi.languages = normalize_languages(list(opf.raw_languages) or [], mi.languages)

    opf.smart_update(mi, apply_null=apply_null)

    if getattr(mi, "uuid", None):
        opf.application_id = mi.uuid

    if apply_null or force_identifiers:
        opf.set_identifiers(mi.get_identifiers())
    else:
        orig = opf.get_identifiers()
        orig.update(mi.get_identifiers())
        opf.set_identifiers({k: v for k, v in orig.iteritems() if k and v})

    if update_timestamp and mi.timestamp is not None:
        opf.timestamp = mi.timestamp


def set_metadata(
    stream,
    mi,
    apply_null=False,
    update_timestamp=False,
    force_identifiers=False,
    add_missing_cover=True,
):
    """
    Write metadata out to the given stream.
    :param stream:
    :param mi:
    :param apply_null: Controls if null values are written over the old data from the new.
    :param update_timestamp:
    :param force_identifiers:
    :param add_missing_cover:
    :return:
    """
    assert isinstance(mi, calibreMetadata), "Method can only run on calibreMetadata object"

    stream.seek(0)
    reader = get_zip_reader(stream, root=os.getcwdu())
    new_cdata = None
    try:
        new_cdata = mi.cover_data[1]
        if not new_cdata:
            raise Exception("no cover")
    except Exception:
        try:
            with lopen(mi.cover, "rb") as f:
                new_cdata = f.read()
        except Exception:
            pass

    # Add the option to update the metadata instead
    opfbytes, ver, raster_cover = set_metadata_opf(
        reader.read_bytes(reader.opf_path),
        mi,
        cover_prefix=posixpath.dirname(reader.opf_path),
        cover_data=new_cdata,
        apply_null=apply_null,
        update_timestamp=update_timestamp,
        force_identifiers=force_identifiers,
        add_missing_cover=add_missing_cover,
    )

    cpath = None
    replacements = {}
    if new_cdata and raster_cover:
        try:
            cpath = posixpath.join(posixpath.dirname(reader.opf_path), raster_cover)
            cover_replacable = not reader.encryption_meta.is_encrypted(cpath) and os.path.splitext(cpath)[
                1
            ].lower() in (".png", ".jpg", ".jpeg")
            if cover_replacable:
                replacements[cpath] = serialize_cover_data(new_cdata, cpath)
        except Exception:
            import traceback

            traceback.print_exc()

    if isinstance(reader.archive, LocalZipFile):
        reader.archive.safe_replace(
            reader.container[OPF.MIMETYPE],
            opfbytes,
            extra_replacements=replacements,
            add_missing=True,
        )
    else:
        safe_replace(
            stream,
            reader.container[OPF.MIMETYPE],
            opfbytes,
            extra_replacements=replacements,
            add_missing=True,
        )
    try:
        if cpath is not None:
            replacements[cpath].close()
            os.remove(replacements[cpath].name)
    except:
        pass

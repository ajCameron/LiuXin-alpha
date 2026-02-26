#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

from __future__ import unicode_literals, division, absolute_import, print_function

import os
import re
import tempfile
from functools import partial
try:
    from past.builtins import unicode
except ModuleNotFoundError:
    from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode as unicode

from LiuXin_alpha.utils.which_os import islinux, isbsd

from LiuXin_alpha.customize.conversion import InputFormatPlugin, OptionRecommendation

from LiuXin_alpha.metadata.book.base import calibreMetadata as MetaData

from LiuXin_alpha.utils.storage.local.filenames import ascii_filename
from imghdr import what
from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.localization import get_lang
from LiuXin_alpha.utils.libraries.liuxin_six import six_zip as izip


__license__ = "GPL v3"
__copyright__ = "2012, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"


class HTMLInput(InputFormatPlugin):

    name = "HTML Input"
    author = "Kovid Goyal"
    description = "Convert HTML and OPF files to an OEB"
    file_types = {"opf", "html", "htm", "xhtml", "xhtm", "shtm", "shtml"}

    options = {
        OptionRecommendation(
            name="breadth_first",
            recommended_value=False,
            level=OptionRecommendation.LOW,
            option_help=_("Traverse links in HTML files breadth first. Normally, " "they are traversed depth first."),
        ),
        OptionRecommendation(
            name="max_levels",
            recommended_value=5,
            level=OptionRecommendation.LOW,
            option_help=_(
                "Maximum levels of recursion when following links in "
                "HTML files. Must be non-negative. 0 implies that no "
                "links in the root HTML file are followed. Default is "
                "%default."
            ),
        ),
        OptionRecommendation(
            name="dont_package",
            recommended_value=False,
            level=OptionRecommendation.LOW,
            option_help=_(
                "Normally this input plugin re-arranges all the input "
                "files into a standard folder hierarchy. Only use this option "
                "if you know what you are doing as it can result in various "
                "nasty side effects in the rest of the conversion pipeline."
            ),
        ),
    }

    def convert(self, stream, options, file_ext, log, accelerators):
        """
        Convert a html file stream into an OEB file.
        :param stream: The html stream (open as rb).
        :param options:
        :param file_ext:
        :param log:
        :param accelerators:
        :return:
        """
        self._is_case_sensitive = None
        basedir = os.getcwd()
        self.opts = options

        # If the file is a physical file on disk and can be found from the stream the convert it directly
        fname = None
        if hasattr(stream, "name"):
            basedir = os.path.dirname(stream.name)
            fname = os.path.basename(stream.name)

        if file_ext != "opf":
            if options.dont_package:
                raise ValueError("The --dont-package option is not supported for an HTML input file")
            try:
                from LiuXin_alpha.metadata.file_sources.html import get_metadata

                mi = get_metadata(stream)
            except ModuleNotFoundError:
                mi = MetaData(None, [])
            # We need calibre metadata
            if not isinstance(mi, MetaData) and hasattr(mi, "to_calibre"):
                try:
                    mi = mi.to_calibre()
                except Exception:
                    pass
            if not isinstance(mi, MetaData):
                mi = MetaData(getattr(mi, "title", None), getattr(mi, "authors", None) or [])

            if fname:
                # Todo: Merge with from_string LX metadata extractor
                try:
                    from LiuXin_alpha.metadata.meta import metadata_from_filename

                    fmi = metadata_from_filename(fname)
                except ModuleNotFoundError:
                    fmi = None
                if fmi is not None:
                    if not isinstance(fmi, MetaData) and hasattr(fmi, "to_calibre"):
                        try:
                            fmi = fmi.to_calibre()
                        except Exception:
                            pass
                    if not isinstance(fmi, MetaData):
                        fmi = MetaData(getattr(fmi, "title", None), getattr(fmi, "authors", None) or [])
                    fmi.smart_update(mi)
                    mi = fmi
            if hasattr(mi, "is_null") and mi.is_null("title"):
                mi.title = os.path.splitext(fname or os.path.basename(stream.name))[0]
            oeb = self.create_oebbook(stream.name, basedir, options, log, mi)
            return oeb

        # If the file cannot be found then run the full conversion method - which can deal with just a stream
        from LiuXin_alpha.file_formats.conversion.plumber import create_oebbook

        return create_oebbook(log, stream.name, options, encoding=options.input_encoding)

    def is_case_sensitive(self, path):

        if getattr(self, "_is_case_sensitive", None) is not None:
            return self._is_case_sensitive
        if not path or not os.path.exists(path):
            return islinux or isbsd
        self._is_case_sensitive = not (os.path.exists(path.lower()) and os.path.exists(path.upper()))
        return self._is_case_sensitive

    def create_oebbook(self, htmlpath, basedir, opts, log, mi):
        """
        Create an oeb book from an HTML document.
        :param htmlpath: The path to the HTML
        :param basedir:
        :param opts: Options for the conversion
        :param log: A log instance
        :param mi: Metadata to be written into the file
        :return:
        """
        import uuid
        try:
            import cssutils
        except ModuleNotFoundError:
            cssutils = None
        import logging
        from LiuXin_alpha.file_formats.conversion.plumber import create_oebbook
        from LiuXin_alpha.file_formats.html.input import get_filelist
        from LiuXin_alpha.file_formats.oeb.base import (
            DirContainer,
            rewrite_links,
            urlnormalize,
            urldefrag,
            BINARY_MIME,
            OEB_STYLES,
            xpath,
        )
        from LiuXin_alpha.file_formats.oeb.transforms.metadata import (
            meta_info_to_oeb_metadata,
        )

        from LiuXin_alpha.metadata.utils import string_to_authors

        from LiuXin_alpha.utils.mine_types import guess_type
        from LiuXin_alpha.utils.localization import canonicalize_lang

        if cssutils is not None:
            cssutils.log.setLevel(logging.WARN)
        self.OEB_STYLES = OEB_STYLES
        oeb = create_oebbook(log, None, opts, self, encoding=opts.input_encoding, populate=False)
        self.oeb = oeb

        metadata = oeb.metadata
        meta_info_to_oeb_metadata(mi, metadata, log)
        if not metadata.language:
            l = canonicalize_lang(getattr(opts, "language", None))
            if not l:
                oeb.logger.warn("Language not specified")
                l = get_lang().replace("_", "-")
            metadata.add("language", l)
        if not metadata.creator:
            a = getattr(opts, "authors", None)
            if a:
                a = string_to_authors(a)
            if not a:
                oeb.logger.warn("Creator not specified")
                a = [self.oeb.translate(_("Unknown"))]
            for aut in a:
                metadata.add("creator", aut)

        if not metadata.title:
            oeb.logger.warn("Title not specified")
            metadata.add("title", self.oeb.translate(_("Unknown")))
        bookid = str(uuid.uuid4())
        metadata.add("identifier", bookid, id="uuid_id", scheme="uuid")
        for ident in metadata.identifier:
            if "id" in ident.attrib:
                self.oeb.uid = metadata.identifier[0]
                break

        filelist = get_filelist(htmlpath, basedir, opts, log)
        filelist = [f for f in filelist if not f.is_binary]
        htmlfile_map = {}
        for f in filelist:
            path = f.path
            oeb.container = DirContainer(os.path.dirname(path), log, ignore_opf=True)
            bname = os.path.basename(path)
            file_id, href = oeb.manifest.generate(id="html", href=ascii_filename(bname))
            htmlfile_map[path] = href
            item = oeb.manifest.add(file_id, href, "text/html")
            item.html_input_href = bname
            oeb.spine.add(item, True)

        self.added_resources = {}
        self.log = log
        self.log("Normalizing filename cases")
        for path, href in htmlfile_map.items():
            if not self.is_case_sensitive(path):
                path = path.lower()
            self.added_resources[path] = href
        self.urlnormalize, self.DirContainer = urlnormalize, DirContainer
        self.urldefrag = urldefrag
        self.guess_type, self.BINARY_MIME = guess_type, BINARY_MIME

        self.log("Rewriting HTML links")
        for f in filelist:
            path = f.path
            dpath = os.path.dirname(path)
            oeb.container = DirContainer(dpath, log, ignore_opf=True)
            item = oeb.manifest.hrefs[htmlfile_map[path]]
            rewrite_links(item.data, partial(self.resource_adder, base=dpath))

        for item in oeb.manifest.values():
            if item.media_type in self.OEB_STYLES:
                dpath = None
                for path, href in self.added_resources.items():
                    if href == item.href:
                        dpath = os.path.dirname(path)
                        break
                if cssutils is not None:
                    cssutils.replaceUrls(item.data, partial(self.resource_adder, base=dpath))

        toc = self.oeb.toc
        self.oeb.auto_generated_toc = True
        titles = []
        headers = []
        for item in self.oeb.spine:
            if not item.linear:
                continue
            html = item.data
            title = "".join(xpath(html, "/h:html/h:head/h:title/text()"))
            title = re.sub(r"\s+", " ", title.strip())
            if title:
                titles.append(title)
            headers.append("(unlabled)")
            for tag in ("h1", "h2", "h3", "h4", "h5", "strong"):
                expr = "/h:html/h:body//h:%s[position()=1]/text()"
                header = "".join(xpath(html, expr % tag))
                header = re.sub(r"\s+", " ", header.strip())
                if header:
                    headers[-1] = header
                    break
        use = titles
        if len(titles) > len(set(titles)):
            use = headers
        for title, item in izip(use, self.oeb.spine):
            if not item.linear:
                continue
            toc.add(title, item.href)

        oeb.container = DirContainer(os.getcwd(), oeb.log, ignore_opf=True)
        return oeb

    def link_to_local_path(self, link_, base=None):

        from LiuXin_alpha.file_formats.html.input import Link

        if not isinstance(link_, unicode):
            try:
                link_ = link_.decode("utf-8", "error")
            except Exception as e:
                self.log.warn("Failed to decode link %r. Ignoring" % link_ + " - exception message: {}".format(e))
                return None, None
        try:
            l = Link(link_, base if base else os.getcwd())
        except Exception as e:
            self.log.exception("Failed to process link: %r" % link_ + " - exception message: {}".format(e))
            return None, None

        if l.path is None:
            # Not a local resource
            return None, None

        link = l.path.replace("/", os.sep).strip()
        frag = l.fragment
        if not link:
            return None, None
        return link, frag

    def resource_adder(self, link_, base=None):
        from urllib.parse import quote

        link, frag = self.link_to_local_path(link_, base=base)
        if link is None:
            return link_
        try:
            if base and not os.path.isabs(link):
                link = os.path.join(base, link)
            link = os.path.abspath(link)
        except:
            return link_
        if not os.access(link, os.R_OK):
            return link_
        if os.path.isdir(link):
            self.log.warn("%s is a link to a directory. Ignoring.", link_)
            return link_
        if not self.is_case_sensitive(tempfile.gettempdir()):
            link = link.lower()
        if link not in self.added_resources:
            bhref = os.path.basename(link)
            local_id, href = self.oeb.manifest.generate(id="added", href=bhref)
            guessed = self.guess_type(href)[0]
            media_type = guessed or self.BINARY_MIME
            if media_type == "text/plain":
                self.log.warn("Ignoring link to text file %r" % link_)
                return None
            if media_type == self.BINARY_MIME:
                # Check for the common case, images
                try:
                    img = what(link)
                except EnvironmentError:
                    pass
                else:
                    if img:
                        media_type = self.guess_type("dummy." + img)[0] or self.BINARY_MIME

            self.oeb.log.debug("Added", link)
            self.oeb.container = self.DirContainer(os.path.dirname(link), self.oeb.log, ignore_opf=True)
            # Load into memory
            item = self.oeb.manifest.add(local_id, href, media_type)
            # bhref refers to an already existing file. The read() method of
            # DirContainer will call unquote on it before trying to read the
            # file, therefore we quote it here.
            if isinstance(bhref, bytes):
                bhref = bhref.decode("utf-8", "replace")
            item.html_input_href = quote(bhref)
            if guessed in self.OEB_STYLES:
                item.override_css_fetch = partial(self.css_import_handler, os.path.dirname(link))
            item.data
            self.added_resources[link] = href

        nlink = self.added_resources[link]
        if frag:
            nlink = "#".join((nlink, frag))
        return nlink

    def css_import_handler(self, base, href):
        link, frag = self.link_to_local_path(href, base=base)
        if link is None or not os.access(link, os.R_OK) or os.path.isdir(link):
            return None, None
        try:
            raw = open(link, "rb").read().decode("utf-8", "replace")
            raw = self.oeb.css_preprocessor(raw, add_namespace=True)
        except Exception as e:
            self.log.exception("Failed to read CSS file: %r - exception message: %s", link, str(e))
            return None, None
        return None, raw

#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:fdm=marker:ai

from __future__ import unicode_literals, division, absolute_import, print_function

import codecs
import shutil
import os
import posixpath
from urlparse import urlparse
from collections import Counter, defaultdict

from LiuXin_alpha.file_formats.chardet import strip_encoding_declarations

from LiuXin_alpha.utils.calibre import sanitize_file_name_unicode

from LiuXin_alpha.utils.localization import trans as _

# Py2/Py3 compatability layer
from LiuXin_alpha.utils.lx_libraries.liuxin_six import dict_iteritems as iteritems
from LiuXin_alpha.utils.lx_libraries.liuxin_six import dict_itervalues as itervalues

__license__ = "GPL v3"
__copyright__ = "2013, Kovid Goyal <kovid at kovidgoyal.net>"
__docformat__ = "restructuredtext en"


class LinkReplacer(object):
    def __init__(self, base, container, link_map, frag_map):
        self.base = base
        self.frag_map = frag_map
        self.link_map = link_map
        self.container = container
        self.replaced = False

    def __call__(self, url):
        if url and url.startswith("#"):
            repl = self.frag_map(self.base, url[1:])
            if not repl or repl == url[1:]:
                return url
            self.replaced = True
            return "#" + repl
        name = self.container.href_to_name(url, self.base)
        if not name:
            return url
        nname = self.link_map.get(name, None)
        if not nname:
            return url
        purl = urlparse(url)
        href = self.container.name_to_href(nname, self.base)
        if purl.fragment:
            nfrag = self.frag_map(name, purl.fragment)
            if nfrag:
                href += "#%s" % nfrag
        if href != url:
            self.replaced = True
        return href


class LinkRebaser(object):
    def __init__(self, container, old_name, new_name):
        self.old_name, self.new_name = old_name, new_name
        self.container = container
        self.replaced = False

    def __call__(self, url):
        if url and url.startswith("#"):
            return url
        purl = urlparse(url)
        frag = purl.fragment
        name = self.container.href_to_name(url, self.old_name)
        if not name:
            return url
        if name == self.old_name:
            name = self.new_name
        href = self.container.name_to_href(name, self.new_name)
        if frag:
            href += "#" + frag
        if href != url:
            self.replaced = True
        return href


def replace_links(container, link_map, frag_map=lambda name, frag: frag, replace_in_opf=False):
    """
    Replace links to files in the container. Will iterate over all files in the container and change the specified
    links in them.

    :param container:
    :param link_map: A mapping of old canonical name to new canonical name.
            For example: :code:`{'images/old.png': 'images/new.png'}`
    :param frag_map: A callable that takes two arguments ``(name, anchor)`` and
        returns a new anchor. This is useful if you need to change the anchors in
        HTML files. By default, it does nothing.
    :param replace_in_opf: If False, links are not replaced in the OPF file.
    :return:
    """
    for name, media_type in iteritems(container.mime_map):
        if name == container.opf_name and not replace_in_opf:
            continue
        repl = LinkReplacer(name, container, link_map, frag_map)
        container.replace_links(name, repl)


def smarten_punctuation(container, report):
    from LiuXin_alpha.file_formats.conversion.preprocess import smarten_punctuation

    smartened = False
    for path in container.spine_items:
        name = container.abspath_to_name(path)
        changed = False
        with container.open(name, "r+b") as f:
            html = container.decode(f.read())
            newhtml = smarten_punctuation(html, container.log)
            if newhtml != html:
                changed = True
                report(_("Smartened punctuation in: %s") % name)
                newhtml = strip_encoding_declarations(newhtml)
                f.seek(0)
                f.truncate()
                f.write(codecs.BOM_UTF8 + newhtml.encode("utf-8"))
        if changed:
            # Add an encoding declaration (it will be added automatically when
            # serialized)
            root = container.parsed(name)
            for m in root.xpath('descendant::*[local-name()="meta" and @http-equiv]'):
                m.getparent().remove(m)
            container.dirty(name)
            smartened = True
    if not smartened:
        report(_("No punctuation that could be smartened found"))
    return smartened


def rename_files(container, file_map):
    """
    Rename files in the container, automatically updating all links to them.

    :param container:
    :param file_map: A mapping of old canonical name to new canonical name, for
        example: :code:`{'text/chapter1.html': 'chapter1.html'}`.
    :return:
    """
    overlap = set(file_map).intersection(set(itervalues(file_map)))
    if overlap:
        raise ValueError(
            "Circular rename detected. The files %s are both rename targets and destinations" % ", ".join(overlap)
        )
    for name, dest in iteritems(file_map):
        if container.exists(dest):
            if name != dest and name.lower() == dest.lower():
                # A case change on an OS with a case insensitive file-system.
                continue
            raise ValueError("Cannot rename {0} to {1} as {1} already exists".format(name, dest))
    if len(tuple(itervalues(file_map))) != len(set(itervalues(file_map))):
        raise ValueError("Cannot rename, the set of destination files contains duplicates")
    link_map = {}
    for current_name, new_name in iteritems(file_map):
        container.rename(current_name, new_name)
        if new_name != container.opf_name:  # OPF is handled by the container
            link_map[current_name] = new_name
    replace_links(container, link_map, replace_in_opf=True)


def replace_file(container, name, path, basename, force_mt=None):
    dirname, base = name.rpartition("/")[0::2]
    nname = sanitize_file_name_unicode(basename)
    if dirname:
        nname = dirname + "/" + nname
    with open(path, "rb") as src:
        if name != nname:
            count = 0
            b, e = nname.rpartition(".")[0::2]
            while container.exists(nname):
                count += 1
                nname = b + ("_%d.%s" % (count, e))
            rename_files(container, {name: nname})
            mt = force_mt or container.guess_type(nname)
            for itemid, q in iteritems(container.manifest_id_map):
                if q == nname:
                    for item in container.opf_xpath('//opf:manifest/opf:item[@href and @id="%s"]' % itemid):
                        item.set("media-type", mt)
        container.dirty(container.opf_name)
        with container.open(nname, "wb") as dest:
            shutil.copyfileobj(src, dest)


def mt_to_category(container, mt):

    from LiuXin_alpha.file_formats.oeb.polish.utils import guess_type
    from LiuXin_alpha.file_formats.oeb.polish.container import OEB_FONTS
    from LiuXin_alpha.file_formats.oeb.base import OEB_DOCS, OEB_STYLES

    if mt in OEB_DOCS:
        category = "text"
    elif mt in OEB_STYLES:
        category = "style"
    elif mt in OEB_FONTS:
        category = "font"
    elif mt == guess_type("a.opf"):
        category = "opf"
    elif mt == guess_type("a.ncx"):
        category = "toc"
    else:
        category = mt.partition("/")[0]
    return category


def get_recommended_folders(container, names):
    """
    Return the folders that are recommended for the given filenames. The
    recommendation is based on where the majority of files of the same type are
    located in the container. If no files of a particular type are present, the
    recommended folder is assumed to be the folder containing the OPF file.
    :param container:
    :param names:
    :return:
    """

    from LiuXin_alpha.file_formats.oeb.polish.utils import guess_type

    counts = defaultdict(Counter)
    for name, mt in iteritems(container.mime_map):
        folder = name.rpartition("/")[0] if "/" in name else ""
        counts[mt_to_category(container, mt)][folder] += 1

    try:
        opf_folder = counts["opf"].most_common(1)[0][0]
    except KeyError:
        opf_folder = ""

    recommendations = {category: counter.most_common(1)[0][0] for category, counter in iteritems(counts)}
    return {
        n: recommendations.get(mt_to_category(container, guess_type(os.path.basename(n))), opf_folder) for n in names
    }


def rationalize_folders(container, folder_type_map):
    all_names = set(container.mime_map)
    new_names = set()
    name_map = {}
    for name in all_names:
        if name.startswith("META-INF/"):
            continue
        category = mt_to_category(container, container.mime_map[name])
        folder = folder_type_map.get(category, None)
        if folder is not None:
            bn = posixpath.basename(name)
            new_name = posixpath.join(folder, bn)
            if new_name != name:
                c = 0
                while new_name in all_names or new_name in new_names:
                    c += 1
                    n, ext = bn.rpartition(".")[0::2]
                    new_name = posixpath.join(folder, "%s_%d.%s" % (n, c, ext))
                name_map[name] = new_name
                new_names.add(new_name)
    return name_map

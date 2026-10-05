#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:fdm=marker:ai

"""
Replace text and resources throughout an EPUB/OEB container.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise replace through a consuming regression::

        python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
"""
from __future__ import (
    absolute_import,
    annotations,
    division,
    print_function,
    unicode_literals,
)

import codecs
import os
import posixpath
import shutil
import typing as _typing
from collections import Counter, defaultdict
from urllib.parse import urlparse

from LiuXin_alpha.utils.libraries.calibre_chardet import strip_encoding_declarations

# Py2/Py3 compatability layer
from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems
from LiuXin_alpha.utils.libraries.liuxin_six import dict_itervalues as itervalues
from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.storage.local.filenames import sanitize_file_name_unicode

__license__ = "GPL v3"
__copyright__ = "2013, Kovid Goyal <kovid at kovidgoyal.net>"
__docformat__ = "restructuredtext en"


class LinkReplacer(object):
    """
    Provide the linkreplacer contract for validated ebook processing.

    Example:
        Exercise LinkReplacer through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
    """
    def __init__(self: _typing.Self, base: _typing.Any, container: _typing.Any, link_map: _typing.Any, frag_map: _typing.Any) -> None:
        """
        Initialize and validate the linkreplacer state.

        Example:
            Exercise LinkReplacer.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param base: Value supplied for base under the utility contract.
        :param container: Value supplied for container under the utility contract.
        :param link_map: Value supplied for link map under the utility contract.
        :param frag_map: Value supplied for frag map under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.base = base
        self.frag_map = frag_map
        self.link_map = link_map
        self.container = container
        self.replaced = False

    def __call__(self: _typing.Self, url: _typing.Any) -> _typing.Any:
        """
        Perform the call operation under explicit file-format and conversion rules.

        Example:
            Exercise LinkReplacer.  call   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param url: Value supplied for url under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if url and url.startswith("#"):
            repl = self.frag_map(self.base, url[1:])
            if not repl or repl == url[1:]:
                return url
            self.replaced = True
            return "#" + repl
        try:
            name = self.container.href_to_name(url, self.base)
        except ValueError:
            # Malformed absolute filesystem links can appear in legacy books.
            return url
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
    """
    Provide the linkrebaser contract for validated ebook processing.

    Example:
        Exercise LinkRebaser through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
    """
    def __init__(self: _typing.Self, container: _typing.Any, old_name: _typing.Any, new_name: _typing.Any) -> None:
        """
        Initialize and validate the linkrebaser state.

        Example:
            Exercise LinkRebaser.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param container: Value supplied for container under the utility contract.
        :param old_name: Value supplied for old name under the utility contract.
        :param new_name: Value supplied for new name under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.old_name, self.new_name = old_name, new_name
        self.container = container
        self.replaced = False

    def __call__(self: _typing.Self, url: _typing.Any) -> _typing.Any:
        """
        Perform the call operation under explicit file-format and conversion rules.

        Example:
            Exercise LinkRebaser.  call   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param url: Value supplied for url under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if url and url.startswith("#"):
            return url
        purl = urlparse(url)
        frag = purl.fragment
        try:
            name = self.container.href_to_name(url, self.old_name)
        except ValueError:
            # Malformed absolute filesystem links can appear in legacy books.
            return url
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


def replace_links(container: _typing.Any, link_map: _typing.Any, frag_map: _typing.Callable[..., _typing.Any] = lambda name, frag: frag, replace_in_opf: bool = False) -> None:
    """
    Replace links to files in the container. Will iterate over all files in the container and change the specified links in them.

    Example:
        Exercise replace links through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param container: Value supplied for container under the utility contract.
    :param link_map: Value supplied for link map under the utility contract.
    :param frag_map: Value supplied for frag map under the utility contract.
    :param replace_in_opf: Value supplied for replace in opf under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    for name, media_type in iteritems(container.mime_map):
        if name == container.opf_name and not replace_in_opf:
            continue
        repl = LinkReplacer(name, container, link_map, frag_map)
        container.replace_links(name, repl)


def smarten_punctuation(container: _typing.Any, report: _typing.Any) -> _typing.Any:
    """
    Perform the smarten punctuation operation under explicit file-format and conversion rules.

    Example:
        Exercise smarten punctuation through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param container: Value supplied for container under the utility contract.
    :param report: Value supplied for report under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
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


def rename_files(container: _typing.Any, file_map: _typing.Any) -> None:
    """
    Rename files in the container, automatically updating all links to them.

    Example:
        Exercise rename files through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param container: Value supplied for container under the utility contract.
    :param file_map: Value supplied for file map under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
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


def replace_file(container: _typing.Any, name: _typing.Any, path: _typing.Any, basename: _typing.Any, force_mt: _typing.Any = None) -> None:
    """
    Replace a container resource while preserving package references.

    Example:
        Exercise replace file through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param container: Value supplied for container under the utility contract.
    :param name: Field, file, function or resource name addressed by the operation.
    :param path: Filesystem path read, written, normalized or validated by the
        operation.
    :param basename: Value supplied for basename under the utility contract.
    :param force_mt: Value supplied for force mt under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
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


def mt_to_category(container: _typing.Any, mt: _typing.Any) -> _typing.Any:

    """
    Perform the mt to category operation under explicit file-format and conversion rules.

    Example:
        Exercise mt to category through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param container: Value supplied for container under the utility contract.
    :param mt: Value supplied for mt under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    from LiuXin_alpha.file_formats.oeb.base import OEB_DOCS, OEB_STYLES
    from LiuXin_alpha.file_formats.oeb.polish.container import OEB_FONTS
    from LiuXin_alpha.file_formats.oeb.polish.utils import guess_type

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


def get_recommended_folders(container: _typing.Any, names: _typing.Any) -> _typing.Any:
    """
    Return the folders that are recommended for the given filenames. The recommendation is based on where the majority of files of the same type are located in the container. If no files of a particular type are present, the recommended folder is assumed to be the folder containing the OPF file.

    Example:
        Exercise get recommended folders through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param container: Value supplied for container under the utility contract.
    :param names: Value supplied for names under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
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


def rationalize_folders(container: _typing.Any, folder_type_map: _typing.Any) -> _typing.Any:
    """
    Move resources into canonical folders and rewrite their references.

    Example:
        Exercise rationalize folders through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param container: Value supplied for container under the utility contract.
    :param folder_type_map: Value supplied for folder type map under the utility
        contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
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

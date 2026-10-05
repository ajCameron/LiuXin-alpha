"""
Remove unused resources from the OEB manifest.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise trimmanifest through a consuming regression::

        python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py
"""
from __future__ import with_statement
from __future__ import annotations

import typing as _typing

"""
OPF manifest trimming transform.
"""

from LiuXin_alpha.file_formats.oeb.base import CSS_MIME, OEB_DOCS
from LiuXin_alpha.file_formats.oeb.base import urlnormalize, iterlinks

from LiuXin_alpha.utils.libraries.liuxin_six import six_urldefrag as urldefrag

__license__ = "GPL v3"
__copyright__ = "2008, Marshall T. Vandegrift <llasram@gmail.com>"


class ManifestTrimmer(object):
    """
    Remove unused files from the manifest.

    Example:
        Exercise ManifestTrimmer through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py
    """

    @classmethod
    def config(cls: type[_typing.Self], cfg: _typing.Any) -> _typing.Any:
        """
        Perform the config operation under explicit file-format and conversion rules.

        Example:
            Exercise ManifestTrimmer.config through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param cfg: Value supplied for cfg under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return cfg

    @classmethod
    def generate(cls: type[_typing.Self], opts: _typing.Any) -> _typing.Any:
        """
        Perform the generate operation under explicit file-format and conversion rules.

        Example:
            Exercise ManifestTrimmer.generate through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param opts: Value supplied for opts under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return cls()

    def __call__(self: _typing.Self, oeb: _typing.Any, context: _typing.Any) -> None:
        """
        Check that every file mentioned in the manifest is being used somewhere. If it isn't then remove it.

        Example:
            Exercise ManifestTrimmer.  call   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param oeb: Value supplied for oeb under the utility contract.
        :param context: Value supplied for context under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        try:
            import cssutils
        except ImportError:
            cssutils = None

        oeb.logger.info("Trimming unused files from manifest...")
        self.opts = context
        used = set()

        for term in oeb.metadata:
            for item in oeb.metadata[term]:
                if item.value in oeb.manifest.hrefs:
                    used.add(oeb.manifest.hrefs[item.value])
                elif item.value in oeb.manifest.ids:
                    used.add(oeb.manifest.ids[item.value])

        for ref in oeb.guide.values():
            path, _ = urldefrag(ref.href)
            if path in oeb.manifest.hrefs:
                used.add(oeb.manifest.hrefs[path])

        # TOC items are required to be in the spine
        for item in oeb.spine:
            used.add(item)
        unchecked = used
        while unchecked:
            new = set()
            for item in unchecked:
                if (item.media_type in OEB_DOCS or item.media_type[-4:] in ("/xml", "+xml")) and item.data is not None:
                    hrefs = [r[2] for r in iterlinks(item.data)]
                    for href in hrefs:
                        if isinstance(href, bytes):
                            href = href.decode("utf-8")
                        try:
                            href = item.abshref(urlnormalize(href))
                        except:
                            continue
                        if href in oeb.manifest.hrefs:
                            found = oeb.manifest.hrefs[href]
                            if found not in used:
                                new.add(found)
                elif item.media_type == CSS_MIME and cssutils is not None:
                    for href in cssutils.getUrls(item.data):
                        href = item.abshref(urlnormalize(href))
                        if href in oeb.manifest.hrefs:
                            found = oeb.manifest.hrefs[href]
                            if found not in used:
                                new.add(found)
            used.update(new)
            unchecked = new
        for item in oeb.manifest.values():
            if item not in used:
                oeb.logger.info("Trimming %r from manifest" % item.href)
                oeb.manifest.remove(item)

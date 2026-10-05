"""
Serialize normalized OEB books and resources to disk.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise writer through a consuming regression::

        python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py
"""
from __future__ import with_statement
from __future__ import annotations

import typing as _typing

"""
Directory output OEBBook writer.
"""

import os

from LiuXin_alpha.file_formats.oeb.base import DirContainer, OEBError
from LiuXin_alpha.file_formats.oeb.base import OPF_MIME, xml2str

from LiuXin_alpha.utils.localization import trans as _

__license__ = "GPL v3"
__copyright__ = "2008, Marshall T. Vandegrift <llasram@gmail.com>"

__all__ = ["OEBWriter"]


class OEBWriter(object):
    """
    Class which stores and writes out an OEB.

    Example:
        Exercise OEBWriter through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py
    """

    # Default renderer profile for content written with this Writer.
    DEFAULT_PROFILE = "PRS505"

    # List of transforms to apply to content written with this Writer.
    TRANSFORMS = []

    def __init__(self: _typing.Self, version: str = "2.0", page_map: bool = False, pretty_print: bool = False) -> None:
        """
        Initialize and validate the oebwriter state.

        Example:
            Exercise OEBWriter.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param version: Value supplied for version under the utility contract.
        :param page_map: Value supplied for page map under the utility contract.
        :param pretty_print: Value supplied for pretty print under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.version = version
        self.page_map = page_map
        self.pretty_print = pretty_print

    @classmethod
    def config(cls: type[_typing.Self], cfg: _typing.Any) -> _typing.Any:
        """
        Add any book-writing options to the :class:`Config` object

        Example:
            Exercise OEBWriter.config through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param cfg: Value supplied for cfg under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        oeb = cfg.add_group("oeb", _("OPF/NCX/etc. generation options."))
        versions = ["1.2", "2.0"]
        oeb(
            "opf_version",
            ["--opf-version"],
            default="2.0",
            choices=versions,
            help=_("OPF version to generate. Default is %default."),
        )
        oeb(
            "adobe_page_map",
            ["--adobe-page-map"],
            default=False,
            help=_('Generate an Adobe "page-map" file if pagination ' "information is available."),
        )
        return cfg

    @classmethod
    def generate(cls: type[_typing.Self], opts: _typing.Any) -> _typing.Any:
        """
        Generate a Writer instance from command-line options.

        Example:
            Exercise OEBWriter.generate through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param opts: Value supplied for opts under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        version = opts.opf_version
        page_map = opts.adobe_page_map
        pretty_print = opts.pretty_print
        return cls(version=version, page_map=page_map, pretty_print=pretty_print)

    def __call__(self: _typing.Self, oeb: _typing.Any, path: _typing.Any) -> None:
        """
        Write the book in the :class:`OEBBook` object :param:`oeb` to a folder at :param:`path`.

        Example:
            Exercise OEBWriter.  call   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param oeb: Value supplied for oeb under the utility contract.
        :param path: Filesystem path read, written, normalized or validated by the
            operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        version = int(self.version[0])
        opfname = None
        if os.path.splitext(path)[1].lower() == ".opf":
            opfname = os.path.basename(path)
            path = os.path.dirname(path)
        if not os.path.isdir(path):
            os.mkdir(path)
        output = DirContainer(path, oeb.log)
        for item in oeb.manifest.values():
            payload = str(item)
            if isinstance(payload, str):
                payload = payload.encode("utf-8")
            output.write(item.href, payload)

        if version == 1:
            metadata = oeb.to_opf1()
        elif version == 2:
            metadata = oeb.to_opf2(page_map=self.page_map)
        else:
            raise OEBError("Unrecognized OPF version %r" % self.version)
        pretty_print = self.pretty_print
        for mime, (href, data) in metadata.items():
            if opfname and mime == OPF_MIME:
                href = opfname
            output.write(href, xml2str(data, pretty_print=pretty_print))
        return

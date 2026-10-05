#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

"""
Open MOBI containers for controlled inspection and reconstruction.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise tweak through a consuming regression::

        python -m pytest -q tests/file_formats/mobi/test_mobi_modernized.py
"""
from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

import typing as _typing

import glob
import os

from LiuXin_alpha.file_formats import DRMError

from LiuXin_alpha.file_formats.mobi import MobiError
from LiuXin_alpha.file_formats.mobi.reader.headers import MetadataHeader

from LiuXin_alpha.utils.storage.local import CurrentDir
from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.logging import default_log

try:
    from LiuXin_alpha.utils.ipc.simple_worker import fork_job
except ModuleNotFoundError:
    # IPC worker module is not ported yet; keep import-time compatibility.
    def fork_job(*args: _typing.Any, **kwargs: _typing.Any) -> None:
        """
        Perform the fork job operation under explicit file-format and conversion rules.

        Example:
            Exercise fork job through a consuming regression::

                python -m pytest -q tests/file_formats/mobi/test_mobi_modernized.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise RuntimeError("LiuXin_alpha.utils.ipc.simple_worker is not available in this port.")

__license__ = "GPL v3"
__copyright__ = "2012, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"


class BadFormat(ValueError):
    """
    Provide the badformat contract for validated ebook processing.

    Example:
        Exercise BadFormat through a consuming regression::

            python -m pytest -q tests/file_formats/mobi/test_mobi_modernized.py
    """
    pass


def do_explode(path: _typing.Any, dest: _typing.Any) -> _typing.Any:
    """
    Perform the do explode operation under explicit file-format and conversion rules.

    Example:
        Exercise do explode through a consuming regression::

            python -m pytest -q tests/file_formats/mobi/test_mobi_modernized.py


    :param path: Filesystem path read, written, normalized or validated by the
        operation.
    :param dest: Value supplied for dest under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    from LiuXin_alpha.file_formats.mobi.reader.mobi6 import MobiReader
    from LiuXin_alpha.file_formats.mobi.reader.mobi8 import Mobi8Reader

    with open(path, "rb") as stream:
        mr = MobiReader(stream, default_log, None, None)

        with CurrentDir(dest):
            mr = Mobi8Reader(mr, default_log)
            opf = os.path.abspath(mr())
            try:
                os.remove("debug-raw.html")
            except:
                pass

    return opf


def explode(path: _typing.Any, dest: _typing.Any, question: _typing.Callable[..., _typing.Any] = lambda x: True) -> _typing.Any:
    """
    Decompress and prepare a book for tweaking.

    Example:
        Exercise explode through a consuming regression::

            python -m pytest -q tests/file_formats/mobi/test_mobi_modernized.py


    :param path: Filesystem path read, written, normalized or validated by the
        operation.
    :param dest: Value supplied for dest under the utility contract.
    :param question: Value supplied for question under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    with open(path, "rb") as stream:
        raw = stream.read(3)
        stream.seek(0)
        if raw == b"TPZ":
            raise BadFormat(_("This is not a MOBI file. It is a Topaz file."))

        try:
            header = MetadataHeader(stream, default_log)
        except MobiError:
            raise BadFormat(_("This is not a MOBI file."))

        if header.encryption_type != 0:
            raise DRMError(_("This file is locked with DRM. It cannot be tweaked."))

        kf8_type = header.kf8_type

        if kf8_type is None:
            raise BadFormat(
                _(
                    "This MOBI file does not contain a KF8 format "
                    "book. KF8 is the new format from Amazon. calibre can "
                    "only tweak MOBI files that contain KF8 books. Older "
                    "MOBI files without KF8 are not tweakable."
                )
            )

        if kf8_type == "joint":
            if not question(
                _(
                    "This MOBI file contains both KF8 and "
                    "older Mobi6 data. Tweaking it will remove the Mobi6 data, which "
                    "means the file will not be usable on older Kindles. Are you "
                    "sure?"
                )
            ):
                return None

    return fork_job("LiuXin_alpha.file_formats.mobi.tweak", "do_explode", args=(path, dest), no_output=True)["result"]


def set_cover(oeb: _typing.Any) -> None:
    """
    Change the cover for the exploded book in OEB form.

    Example:
        Exercise set cover through a consuming regression::

            python -m pytest -q tests/file_formats/mobi/test_mobi_modernized.py


    :param oeb: Value supplied for oeb under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    if "cover" not in oeb.guide or oeb.metadata["cover"]:
        return
    cover = oeb.guide["cover"]
    if cover.href in oeb.manifest.hrefs:
        item = oeb.manifest.hrefs[cover.href]
        oeb.metadata.clear("cover")
        oeb.metadata.add("cover", item.id)


def do_rebuild(opf: _typing.Any, dest_path: _typing.Any) -> None:
    """
    Perform the do rebuild operation under explicit file-format and conversion rules.

    Example:
        Exercise do rebuild through a consuming regression::

            python -m pytest -q tests/file_formats/mobi/test_mobi_modernized.py


    :param opf: Value supplied for opf under the utility contract.
    :param dest_path: Value supplied for dest path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.customize.ui import plugin_for_input_format, plugin_for_output_format
    from LiuXin_alpha.file_formats.conversion.plumber import Plumber, create_oebbook

    plumber = Plumber(opf, dest_path, default_log)
    plumber.setup_options()
    inp = plugin_for_input_format("azw3")
    outp = plugin_for_output_format("azw3")

    plumber.opts.mobi_passthrough = True
    oeb = create_oebbook(default_log, opf, plumber.opts)
    set_cover(oeb)
    outp.convert(oeb, dest_path, inp, plumber.opts, default_log)


def rebuild(src_dir: _typing.Any, dest_path: _typing.Any) -> None:
    """
    Take the exploded, tweaked, Open EBook and build it back into a mobi file.

    Example:
        Exercise rebuild through a consuming regression::

            python -m pytest -q tests/file_formats/mobi/test_mobi_modernized.py


    :param src_dir: Value supplied for src dir under the utility contract.
    :param dest_path: Value supplied for dest path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    opf = glob.glob(os.path.join(src_dir, "*.opf"))
    if not opf:
        raise ValueError("No OPF file found in %s" % src_dir)
    opf = opf[0]

    # For debugging, uncomment the following two lines
    # def fork_job(a, b, args=None, no_output=True):
    #     do_rebuild(*args)
    fork_job("LiuXin_alpha.file_formats.mobi.tweak", "do_rebuild", args=(opf, dest_path), no_output=True)

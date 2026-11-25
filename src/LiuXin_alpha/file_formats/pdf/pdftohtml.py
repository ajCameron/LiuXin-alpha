# -*- coding: utf-8 -*-

"""
Convert PDF extraction output into normalized HTML content.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise pdftohtml through a consuming regression::

        python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py
"""
from __future__ import print_function
from __future__ import annotations

import typing as _typing

import os
import re
import shutil
import subprocess
import sys

from LiuXin_alpha.constants import iswindows
from LiuXin_alpha.utils.bounded_process import ToolLimits, run_tool
from LiuXin_alpha.utils.image_tools.executable_paths import bundled_executable_directory
from LiuXin_alpha.utils.which_os import isbsd

from LiuXin_alpha.file_formats import ConversionError, DRMError

from LiuXin_alpha.utils.localization import trans as _

__license__ = "GPL 3"
__copyright__ = "2008, Kovid Goyal <kovid at kovidgoyal.net>, " "2009, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"


# Todo: Sort/modify - install pdftohtml - make finding it more robust
PDFTOHTML = os.path.join(
    bundled_executable_directory(),
    "pdftohtml.exe" if iswindows and hasattr(sys, "frozen") else "pdftohtml",
)
#: Windows process-creation flags: CREATE_NO_WINDOW, so that no console pops up.
CREATION_FLAGS = 0x08 if iswindows and hasattr(sys, "frozen") else 0

#: The bounds on one run. pdftohtml parses whatever file it is given; a crafted one
#: can keep it busy, fill memory or print without end. A whole book's HTML and images
#: are written, so the limits are wider than for poppler's metadata tools.
PDFTOHTML_LIMITS = ToolLimits(
    timeout=600.0,
    cpu_seconds=600,
    memory_bytes=2 << 30,
    output_bytes=512 << 20,
)


def pdftohtml(output_dir: _typing.Any, pdf_path: _typing.Any, no_images: _typing.Any, as_xml: bool = False) -> None:
    """
    Convert the pdf into html using the pdftohtml app. This will write the html as index.html into output_dir. It will also write all extracted images to the output_dir

    Example:
        Exercise pdftohtml through a consuming regression::

            python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


    :param output_dir: Value supplied for output dir under the utility contract.
    :param pdf_path: Value supplied for pdf path under the utility contract.
    :param no_images: Value supplied for no images under the utility contract.
    :param as_xml: Value supplied for as xml under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """

    pdfsrc = os.path.join(output_dir, "src.pdf")
    index = os.path.join(output_dir, "index." + ("xml" if as_xml else "html"))

    with open(pdf_path, "rb") as src, open(pdfsrc, "wb") as dest:
        shutil.copyfileobj(src, dest)

    # pdftohtml does not always (Linux) respect absolute paths, so it runs in the
    # output folder and is given names; this process's folder does not change.
    cmd = [
        PDFTOHTML,
        "-enc",
        "UTF-8",
        "-noframes",
        "-p",
        "-nomerge",
        "-nodrm",
        "-q",
        os.path.basename(pdfsrc),
        os.path.basename(index),
    ]
    if isbsd:
        cmd.remove("-nodrm")
    if no_images:
        cmd.append("-i")
    if as_xml:
        cmd.append("-xml")

    try:
        out = run_tool(cmd, cwd=output_dir, limits=PDFTOHTML_LIMITS, creationflags=CREATION_FLAGS).strip()
    except subprocess.CalledProcessError as err:
        raise _conversion_error(err) from err
    finally:
        try:
            os.remove(pdfsrc)
        except OSError:
            pass
    if out:
        print("pdftohtml log:")
        print(out.decode("utf-8", "replace"))
    if not os.path.exists(index) or os.stat(index).st_size < 100:
        raise DRMError()

    if not as_xml:
        with open(index, "r+b") as i:
            raw = i.read()
            raw = flip_images(raw)
            raw = b"<!-- created by calibre's pdftohtml -->\n" + raw
            i.seek(0)
            i.truncate()
            # versions of pdftohtml >= 0.20 output self closing <br> tags, this breaks the pdf heuristics regexps,
            # so replace them
            i.write(raw.replace(b"<br/>", b"<br>"))


def _conversion_error(err: subprocess.CalledProcessError) -> ConversionError:
    """
    Explain why pdftohtml failed: missing, stopped at its limits, or its own error.

    Example:
        >>> str(_conversion_error(subprocess.CalledProcessError(127, ["pdftohtml"])))
        'Could not find pdftohtml, check it is in your PATH'
        >>> str(_conversion_error(subprocess.CalledProcessError(-9, ["pdftohtml"]))).startswith("pdftohtml was stopped")
        True


    :param err: What :func:`~LiuXin_alpha.utils.bounded_process.run_tool` raised.
    :return: The error to raise.
    """
    if err.returncode == 127:
        return ConversionError(_("Could not find pdftohtml, check it is in your PATH"))
    detail = (err.stderr or b"").decode("utf-8", "replace").strip()
    if err.returncode < 0:
        return ConversionError(
            f"pdftohtml was stopped (signal {-err.returncode}): the PDF took it past its time, "
            f"memory or output limits\n{detail}"
        )
    return ConversionError("return code: %d\n%s" % (err.returncode, detail))


def flip_image(img: _typing.Any, flip: _typing.Any) -> None:
    """
    Perform the flip image operation under explicit file-format and conversion rules.

    Example:
        Exercise flip image through a consuming regression::

            python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


    :param img: Value supplied for img under the utility contract.
    :param flip: Value supplied for flip under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    try:
        from PIL import Image as PILImage
    except Exception:
        return

    with PILImage.open(img) as im:
        if b"x" in flip:
            im = im.transpose(PILImage.FLIP_LEFT_RIGHT)
        if b"y" in flip:
            im = im.transpose(PILImage.FLIP_TOP_BOTTOM)
        im.save(img)


def flip_images(raw: _typing.Any) -> _typing.Any:
    """
    Perform the flip images operation under explicit file-format and conversion rules.

    Example:
        Exercise flip images through a consuming regression::

            python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


    :param raw: Value supplied for raw under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    for match in re.finditer(b"<IMG[^>]+/?>", raw, flags=re.I):
        img = match.group()
        m = re.search(rb'class="(x|y|xy)flip"', img)
        if m is None:
            continue
        flip = m.group(1)
        src = re.search(rb'src="([^"]+)"', img)
        if src is None:
            continue
        img = src.group(1)
        if not os.path.exists(img):
            continue
        flip_image(img, flip)
    raw = re.sub(rb"<STYLE.+?</STYLE>\s*", b"", raw, flags=re.I | re.DOTALL)
    return raw

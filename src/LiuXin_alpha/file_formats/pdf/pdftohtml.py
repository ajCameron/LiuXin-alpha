# -*- coding: utf-8 -*-

from __future__ import print_function
from __future__ import annotations

import typing as _typing

import errno
import os
import re
import shutil
import subprocess
import sys
from functools import partial

from LiuXin_alpha.constants import isosx, iswindows
from LiuXin_alpha.utils.which_os import islinux, isbsd

from LiuXin_alpha.file_formats import ConversionError, DRMError

from LiuXin_alpha.utils.calibre import CurrentDir
from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.ptempfiles import PersistentTemporaryFile

__license__ = "GPL 3"
__copyright__ = "2008, Kovid Goyal <kovid at kovidgoyal.net>, " "2009, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"


# Todo: Sort/modify - install pdftohtml - make finding it more robust
PDFTOHTML = "pdftohtml"
popen = subprocess.Popen
if isosx and hasattr(sys, "frameworks_dir"):
    PDFTOHTML = os.path.join(getattr(sys, "frameworks_dir"), PDFTOHTML)

if iswindows and hasattr(sys, "frozen"):
    PDFTOHTML = os.path.join(os.path.dirname(sys.executable), "pdftohtml.exe")
    popen = partial(subprocess.Popen, creationflags=0x08)  # CREATE_NO_WINDOW=0x08 so that no ugly console is popped up

if (islinux or isbsd) and getattr(sys, "frozen", False):
    PDFTOHTML = os.path.join(sys.executables_location, "bin", "pdftohtml")


def pdftohtml(output_dir: _typing.Any, pdf_path: _typing.Any, no_images: _typing.Any, as_xml: bool = False) -> None:
    """
    Convert the pdf into html using the pdftohtml app.
    This will write the html as index.html into output_dir.
    It will also write all extracted images to the output_dir
    :param output_dir:
    :param pdf_path:
    :param no_images:
    :param as_xml:
    :return:
    """

    pdfsrc = os.path.join(output_dir, "src.pdf")
    index = os.path.join(output_dir, "index." + ("xml" if as_xml else "html"))

    with open(pdf_path, "rb") as src, open(pdfsrc, "wb") as dest:
        shutil.copyfileobj(src, dest)

    with CurrentDir(output_dir):
        # This is necessary as pdftohtml doesn't always (linux) respect absolute paths.
        def a(x: _typing.Any) -> _typing.Any:
            return os.path.basename(x)

        cmd = [
            PDFTOHTML,
            "-enc",
            "UTF-8",
            "-noframes",
            "-p",
            "-nomerge",
            "-nodrm",
            "-q",
            a(pdfsrc),
            a(index),
        ]

        if isbsd:
            cmd.remove("-nodrm")
        if no_images:
            cmd.append("-i")
        if as_xml:
            cmd.append("-xml")

        logf = PersistentTemporaryFile("pdftohtml_log")
        try:
            p = popen(cmd, stderr=logf._fd, stdout=logf._fd, stdin=subprocess.PIPE)
        except OSError as err:
            if err.errno == errno.ENOENT:
                raise ConversionError(_("Could not find pdftohtml, check it is in your PATH"))
            else:
                raise

        ret = "Internal Python error"
        while True:
            try:
                ret = p.wait()
                break
            except OSError as e:
                if e.errno == errno.EINTR:
                    continue
                else:
                    raise
        logf.flush()
        logf.close()
        with open(logf.name, "rb") as log_temp_file:
            out = log_temp_file.read().strip()
        try:
            os.remove(pdfsrc)
        except:
            pass
        if ret != 0:
            raise ConversionError("return code: %d\n%s" % (ret, out.decode("utf-8", "replace")))
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


def flip_image(img: _typing.Any, flip: _typing.Any) -> None:
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

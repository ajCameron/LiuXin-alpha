#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

import typing as _typing

import sys
import os
import shlex
import subprocess
import shutil
import unicodedata

from LiuXin_alpha.constants import iswindows, __appname__

from LiuXin_alpha.utils.calibre import as_unicode, walk
from LiuXin_alpha import prints
try:
    from LiuXin_alpha.utils.ipc.simple_worker import WorkerError
except ModuleNotFoundError:
    class WorkerError(RuntimeError):
        def __init__(self: _typing.Self, message: _typing.Any, orig_tb: _typing.Any = None) -> None:
            super().__init__(message)
            self.orig_tb = orig_tb
from LiuXin_alpha.utils.decompression.libunzip import extract as zipextract
from LiuXin_alpha.utils.ptempfiles import TemporaryDirectory, TemporaryFile
from LiuXin_alpha.utils.libraries.calibre_zipfile import ZipFile, ZIP_DEFLATED, ZIP_STORED

__license__ = "GPL v3"
__copyright__ = "2012, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"


class Error(ValueError):
    pass


def ask_cli_question(msg: _typing.Any) -> bool:
    prints(msg, end=" [y/N]: ")
    sys.stdout.flush()

    if iswindows:
        import msvcrt

        ans = msvcrt.getch()
    else:
        import tty
        import termios

        old_settings = termios.tcgetattr(sys.stdin.fileno())
        try:
            tty.setraw(sys.stdin.fileno())
            try:
                ans = sys.stdin.read(1)
            except KeyboardInterrupt:
                ans = ""
        finally:
            termios.tcsetattr(sys.stdin.fileno(), termios.TCSADRAIN, old_settings)
    print()
    if isinstance(ans, bytes):
        ans = ans.decode("ascii", "ignore")
    return (ans or "").lower() == "y"


def mobi_exploder(path: _typing.Any, tdir: _typing.Any, question: _typing.Callable[..., _typing.Any] = lambda x: True) -> _typing.Any:
    from LiuXin_alpha.utils.calibre.ebooks.mobi.tweak import explode, BadFormat

    try:
        return explode(path, tdir, question=question)
    except BadFormat as e:
        raise Error(as_unicode(e))


def zip_exploder(path: _typing.Any, tdir: _typing.Any, question: _typing.Callable[..., _typing.Any] = lambda x: True) -> _typing.Any:
    try:
        zipextract(path, tdir)
    except Exception as err:
        raise Error("Failed to unpack {}: {}".format(path, as_unicode(err)))
    for f in walk(tdir):
        if f.lower().endswith(".opf"):
            return f
    raise Error("Invalid book: Could not find .opf")


def zip_rebuilder(tdir: _typing.Any, path: _typing.Any) -> None:
    output_abspath = os.path.abspath(path)
    with ZipFile(path, "w", compression=ZIP_DEFLATED) as zf:
        # Write mimetype
        mt = os.path.join(tdir, "mimetype")
        if os.path.exists(mt):
            zf.write(mt, "mimetype", compress_type=ZIP_STORED)
        # Write everything else
        exclude_files = {".DS_Store", "mimetype", "iTunesMetadata.plist"}
        for root, dirs, files in os.walk(tdir):
            dirs.sort()
            files.sort()
            for fn in files:
                if fn in exclude_files:
                    continue
                absfn = os.path.join(root, fn)
                if os.path.abspath(absfn) == output_abspath:
                    continue
                zfn = unicodedata.normalize("NFC", os.path.relpath(absfn, tdir).replace(os.sep, "/"))
                zf.write(absfn, zfn)


def get_tools(fmt: _typing.Any) -> _typing.Any:
    fmt = (fmt or "").lower()

    if fmt in {"mobi", "azw", "azw3"}:
        from LiuXin_alpha.utils.calibre.ebooks.mobi.tweak import rebuild

        ans = mobi_exploder, rebuild
    elif fmt in {"epub", "htmlz"}:
        ans = zip_exploder, zip_rebuilder
    else:
        ans = None, None

    return ans


def tweak(ebook_file: _typing.Any) -> None:
    """
    Command line interface to the Tweak Book tool
    :param ebook_file:
    :return:
    """
    fmt = ebook_file.rpartition(".")[-1].lower()
    exploder, rebuilder = get_tools(fmt)
    if exploder is None:
        prints(
            "Cannot tweak %s files. Supported formats are: EPUB, HTMLZ, AZW3, MOBI",
            file=sys.stderr,
        )
        raise SystemExit(1)

    with TemporaryDirectory("_tweak_" + os.path.basename(ebook_file).rpartition(".")[0]) as tdir:
        try:
            opf = exploder(ebook_file, tdir, question=ask_cli_question)
        except WorkerError as e:
            prints("Failed to unpack", ebook_file)
            if getattr(e, "orig_tb", None):
                prints(e.orig_tb)
            raise SystemExit(1)
        except Error as e:
            prints(as_unicode(e), file=sys.stderr)
            raise SystemExit(1)
        except Exception as e:
            prints("Failed to unpack", ebook_file, file=sys.stderr)
            prints(as_unicode(e), file=sys.stderr)
            raise SystemExit(1)

        if opf is None:
            # The question was answered with No
            return

        ed = os.environ.get("EDITOR", "dummy")
        ed = (ed or "").strip() or "dummy"
        cmd = shlex.split(ed)
        if not cmd:
            cmd = ["dummy"]
        editor_name = os.path.basename(cmd[0]).lower()
        isvim = editor_name in {"vi", "vim"} or editor_name.endswith("vim")

        prints("Book extracted to", tdir)

        if not isvim:
            prints(
                "Make your tweaks and once you are done,",
                __appname__,
                "will rebuild",
                ebook_file,
                "from",
                tdir,
            )
            print()
            proceed = ask_cli_question("Rebuild " + ebook_file + "?")
        else:
            base = os.path.basename(ebook_file)
            with TemporaryFile(base + ".zip") as zipf:
                with ZipFile(zipf, "w") as zf:
                    zf.add_dir(tdir)
                try:
                    subprocess.check_call(cmd + [zipf])
                except:
                    prints(ed, "failed, aborting...")
                    raise SystemExit(1)
                with ZipFile(zipf, "r") as zf:
                    shutil.rmtree(tdir)
                    os.mkdir(tdir)
                    zf.extractall(path=tdir)
            proceed = True

        if proceed:
            prints("Rebuilding", ebook_file, "please wait ...")
            try:
                rebuilder(tdir, ebook_file)
            except WorkerError as e:
                prints("Failed to rebuild", ebook_file)
                if getattr(e, "orig_tb", None):
                    prints(e.orig_tb)
                raise SystemExit(1)
            except Exception as e:
                prints("Failed to rebuild", ebook_file, file=sys.stderr)
                prints(as_unicode(e), file=sys.stderr)
                raise SystemExit(1)
            prints(ebook_file, "successfully tweaked")

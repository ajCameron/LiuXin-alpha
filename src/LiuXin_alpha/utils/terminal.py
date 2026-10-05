#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:fdm=marker:ai

"""
Detect terminal capabilities and provide portable ANSI or Windows colour streams.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise terminal through a consuming regression::

        python -m pytest -q tests/utils/test_terminal.py
"""
from __future__ import unicode_literals, division, absolute_import, print_function

import os
import sys
import re
from builtins import zip as izip

from LiuXin_alpha.utils.libraries.liuxin_clint import puts
from LiuXin_alpha.utils.libraries.liuxin_clint import colored as clint_colored

from typing import Optional, Iterable, Tuple

from LiuXin_alpha.utils.which_os import iswindows
from LiuXin_alpha.utils.libraries.liuxin_six import iteritems

__license__ = "GPL v3"
__copyright__ = "2012, Kovid Goyal <kovid at kovidgoyal.net>"
__docformat__ = "restructuredtext en"


def fmt(code):
    """
    Perform the fmt utility operation under explicit compatibility rules.

    Example:
        Exercise fmt through a consuming regression::

            python -m pytest -q tests/utils/test_terminal.py


    :param code: Value supplied for code under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return ("\033[%dm" % code).encode("ascii")


RATTRIBUTES = dict(
    izip(
        range(1, 9),
        ("bold", "dark", "", "underline", "blink", "", "reverse", "concealed"),
    )
)
ATTRIBUTES = {v: fmt(k) for k, v in iteritems(RATTRIBUTES)}
del ATTRIBUTES[""]

RBACKGROUNDS = dict(
    izip(
        range(41, 48),
        ("red", "green", "yellow", "blue", "magenta", "cyan", "white"),
    )
)
BACKGROUNDS = {v: fmt(k) for k, v in iteritems(RBACKGROUNDS)}

RCOLORS = dict(
    izip(
        range(31, 38),
        (
            "red",
            "green",
            "yellow",
            "blue",
            "magenta",
            "cyan",
            "white",
        ),
    )
)
COLORS = {v: fmt(k) for k, v in iteritems(RCOLORS)}

RESET = fmt(0)

if iswindows:
    # From wincon.h
    WCOLORS = {c: i for i, c in enumerate(("black", "blue", "green", "cyan", "red", "magenta", "yellow", "white"))}

    def to_flag(fg, bg, bold):
        """
        Perform the to flag utility operation under explicit compatibility rules.

        Example:
            Exercise to flag through a consuming regression::

                python -m pytest -q tests/utils/test_terminal.py


        :param fg: Value supplied for fg under the utility contract.
        :param bg: Value supplied for bg under the utility contract.
        :param bold: Value supplied for bold under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        val = 0
        if bold:
            val |= 0x08
        if fg in WCOLORS:
            val |= WCOLORS[fg]
        if bg in WCOLORS:
            val |= WCOLORS[bg] << 4
        return val


def colored(text, fg=None, bg=None, bold=False):
    """
    Perform the colored utility operation under explicit compatibility rules.

    Example:
        Exercise colored through a consuming regression::

            python -m pytest -q tests/utils/test_terminal.py


    :param text: Text parsed, normalized or rendered.
    :param fg: Value supplied for fg under the utility contract.
    :param bg: Value supplied for bg under the utility contract.
    :param bold: Value supplied for bold under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    prefix = []
    if fg is not None:
        prefix.append(COLORS[fg])
    if bg is not None:
        prefix.append(BACKGROUNDS[bg])
    if bold:
        prefix.append(ATTRIBUTES["bold"])
    prefix = b"".join(prefix)
    suffix = RESET
    if isinstance(text, type("")):
        prefix = prefix.decode("ascii")
        suffix = suffix.decode("ascii")
    return prefix + text + suffix


class Detect(object):
    """
    Provide the Detect utility contract with explicit state and cleanup behavior.

    Example:
        Exercise Detect through a consuming regression::

            python -m pytest -q tests/utils/test_terminal.py
    """
    def __init__(self, stream):
        """
        Initialize and validate the Detect state.

        Example:
            Exercise Detect.  init   through a consuming regression::

                python -m pytest -q tests/utils/test_terminal.py


        :param stream: Input or output stream wrapped by the terminal or compatibility
            layer.
        :return: None; validated state is stored on the receiving object.
        """
        self.stream = stream or sys.stdout
        self.isatty = getattr(self.stream, "isatty", lambda: False)()
        force_ansi = "CALIBRE_FORCE_ANSI" in os.environ
        if not self.isatty and force_ansi:
            self.isatty = True
        self.isansi = force_ansi or not iswindows
        self.set_console = self.write_console = None
        self.is_console = False
        if not self.isansi:
            try:
                import msvcrt

                self.msvcrt = msvcrt
                self.file_handle = msvcrt.get_osfhandle(self.stream.fileno())
                from ctypes import (
                    windll,
                    wintypes,
                    byref,
                    c_wchar_p,
                    c_size_t,
                    POINTER,
                    WinDLL,
                )

                mode = wintypes.DWORD(0)
                f = windll.kernel32.GetConsoleMode
                f.argtypes, f.restype = [
                    wintypes.HANDLE,
                    POINTER(wintypes.DWORD),
                ], wintypes.BOOL
                if f(self.file_handle, byref(mode)):
                    # Stream is a console
                    self.set_console = windll.kernel32.SetConsoleTextAttribute
                    self.wcslen = crt().wcslen
                    self.wcslen.argtypes, self.wcslen.restype = [c_wchar_p], c_size_t
                    self.write_console = WinDLL("kernel32", use_last_error=True).WriteConsoleW
                    self.write_console.argtypes = [
                        wintypes.HANDLE,
                        wintypes.c_wchar_p,
                        wintypes.DWORD,
                        POINTER(wintypes.DWORD),
                        wintypes.LPVOID,
                    ]
                    self.write_console.restype = wintypes.BOOL
                    self.is_console = True
            except:
                pass

    def write_unicode_text(self, text, ignore_errors=False):
        """
        Windows only method that writes unicode strings correctly to the windows console using the Win32 API

        Example:
            Exercise Detect.write unicode text through a consuming regression::

                python -m pytest -q tests/utils/test_terminal.py


        :param text: Text parsed, normalized or rendered.
        :param ignore_errors: Value supplied for ignore errors under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.is_console:
            from ctypes import wintypes, byref, c_wchar_p

            written = wintypes.DWORD(0)
            chunk = len(text)
            while text:
                t, text = text[:chunk], text[chunk:]
                wt = c_wchar_p(t)
                if not self.write_console(self.file_handle, wt, self.wcslen(wt), byref(written), None):
                    # Older versions of windows can fail to write large strings
                    # to console with WriteConsoleW (seen it happen on Win XP)
                    import ctypes, winerror

                    err = ctypes.get_last_error()
                    if err == winerror.ERROR_NOT_ENOUGH_MEMORY and chunk >= 128:
                        # Retry with a smaller chunk size (give up if chunk < 128)
                        chunk = chunk // 2
                        text = t + text
                        continue
                    if err == winerror.ERROR_GEN_FAILURE:
                        # On newer windows, this happens when trying to write
                        # non-ascii chars to the console and the console is set
                        # to use raster fonts (the default). In this case
                        # rather than failing, write an informative error
                        # message and the asciized version of the text.
                        print(
                            "Non-ASCII text detected. You must set your Console's font to"
                            " Lucida Console or Consolas or some other TrueType font to see this text",
                            file=self.stream,
                            end=" -- ",
                        )
                        from LiuXin_alpha.utils.calibre.utils.filenames import ascii_text

                        print(ascii_text(t + text), file=self.stream, end="")
                        continue
                    if not ignore_errors:
                        raise ctypes.WinError(err)


_crt = None


def crt():
    # We use the C runtime bundled with the calibre windows build
    """
    Perform the crt utility operation under explicit compatibility rules.

    Example:
        Exercise crt through a consuming regression::

            python -m pytest -q tests/utils/test_terminal.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    global _crt
    if _crt is None:
        import glob, ctypes

        d = os.path.join(os.path.dirname(sys.executable), "*.CRT", "msvcr*.dll")
        _crt = ctypes.CDLL(glob.glob(d)[0])
    return _crt


class ColoredStream(Detect):
    """
    Provide the ColoredStream utility contract with explicit state and cleanup behavior.

    Example:
        Exercise ColoredStream through a consuming regression::

            python -m pytest -q tests/utils/test_terminal.py
    """
    def __init__(self, stream=None, fg=None, bg=None, bold=False):
        """
        Initialize and validate the ColoredStream state.

        Example:
            Exercise ColoredStream.  init   through a consuming regression::

                python -m pytest -q tests/utils/test_terminal.py


        :param stream: Input or output stream wrapped by the terminal or compatibility
            layer.
        :param fg: Value supplied for fg under the utility contract.
        :param bg: Value supplied for bg under the utility contract.
        :param bold: Value supplied for bold under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        super(ColoredStream, self).__init__(stream)
        self.fg, self.bg, self.bold = fg, bg, bold
        if self.set_console is not None:
            self.wval = to_flag(self.fg, self.bg, bold)

    def __enter__(self):
        """
        Implement the resource's enter lifecycle operation.

        Example:
            Exercise ColoredStream.  enter   through a consuming regression::

                python -m pytest -q tests/utils/test_terminal.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if not self.isatty:
            return self
        if self.isansi:
            if self.bold:
                self.stream.write(ATTRIBUTES["bold"])
            if self.bg is not None:
                self.stream.write(BACKGROUNDS[self.bg])
            if self.fg is not None:
                self.stream.write(COLORS[self.fg])
        elif self.set_console is not None:
            if self.wval != 0:
                self.set_console(self.file_handle, self.wval)
        return self

    def __exit__(self, *args, **kwargs):
        """
        Implement the resource's exit lifecycle operation.

        Example:
            Exercise ColoredStream.  exit   through a consuming regression::

                python -m pytest -q tests/utils/test_terminal.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if not self.isatty:
            return
        if not self.fg and not self.bg and not self.bold:
            return
        if self.isansi:
            self.stream.write(RESET)
        elif self.set_console is not None:
            self.set_console(self.file_handle, WCOLORS["white"])


class ANSIStream(Detect):

    """
    Provide the ANSIStream utility contract with explicit state and cleanup behavior.

    Example:
        Exercise ANSIStream through a consuming regression::

            python -m pytest -q tests/utils/test_terminal.py
    """
    ANSI_RE = re.compile(r"\033\[((?:\d|;)*)([a-zA-Z])")
    ANSI_RE_BYTES = re.compile(rb"\033\[((?:\d|;)*)([a-zA-Z])")

    def __init__(self, stream=None):
        """
        Initialize and validate the ANSIStream state.

        Example:
            Exercise ANSIStream.  init   through a consuming regression::

                python -m pytest -q tests/utils/test_terminal.py


        :param stream: Input or output stream wrapped by the terminal or compatibility
            layer.
        :return: None; validated state is stored on the receiving object.
        """
        super(ANSIStream, self).__init__(stream)
        self.encoding = getattr(self.stream, "encoding", "utf-8") or "utf-8"

    def write(self, text):
        """
        Forward the write operation while preserving adapter ownership rules.

        Example:
            Exercise ANSIStream.write through a consuming regression::

                python -m pytest -q tests/utils/test_terminal.py


        :param text: Text parsed, normalized or rendered.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if not self.isatty:
            return self.strip_and_write(text)

        if self.isansi:
            return self._write_stream(text)

        if not self.isansi and self.set_console is None:
            return self.strip_and_write(text)

        self.write_and_convert(text)

    def strip_and_write(self, text):
        """
        Perform the strip and write utility operation under explicit compatibility rules.

        Example:
            Exercise ANSIStream.strip and write through a consuming regression::

                python -m pytest -q tests/utils/test_terminal.py


        :param text: Text parsed, normalized or rendered.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ansi_re = self.ANSI_RE_BYTES if isinstance(text, (bytes, bytearray)) else self.ANSI_RE
        empty = b"" if isinstance(text, (bytes, bytearray)) else ""
        return self._write_stream(ansi_re.sub(empty, text))

    def _write_stream(self, text):
        """
        Perform the write stream utility operation under explicit compatibility rules.

        Example:
            Exercise ANSIStream. write stream through a consuming regression::

                python -m pytest -q tests/utils/test_terminal.py


        :param text: Text parsed, normalized or rendered.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            return self.stream.write(text)
        except TypeError:
            if isinstance(text, (bytes, bytearray)):
                return self.stream.write(bytes(text).decode(self.encoding, "replace"))
            return self.stream.write(str(text).encode(self.encoding, "replace"))

    def write_and_convert(self, text):
        """
        Write the given text to our wrapped stream, stripping any ANSI sequences from the text, and optionally converting them into win32 calls.

        Example:
            Exercise ANSIStream.write and convert through a consuming regression::

                python -m pytest -q tests/utils/test_terminal.py


        :param text: Text parsed, normalized or rendered.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.last_state = (None, None, False)
        cursor = 0
        ansi_re = self.ANSI_RE_BYTES if isinstance(text, (bytes, bytearray)) else self.ANSI_RE
        for match in ansi_re.finditer(text):
            start, end = match.span()
            self.write_plain_text(text, cursor, start)
            self.convert_ansi(*match.groups())
            cursor = end
        self.write_plain_text(text, cursor, len(text))
        self.stream.flush()

    def write_plain_text(self, text, start, end):
        """
        Perform the write plain text utility operation under explicit compatibility rules.

        Example:
            Exercise ANSIStream.write plain text through a consuming regression::

                python -m pytest -q tests/utils/test_terminal.py


        :param text: Text parsed, normalized or rendered.
        :param start: Value supplied for start under the utility contract.
        :param end: Value supplied for end under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if start < end:
            text = text[start:end]
            if self.is_console and isinstance(text, bytes):
                try:
                    utext = text.decode(self.encoding)
                except ValueError:
                    pass
                else:
                    return self.write_unicode_text(utext)
            self._write_stream(text)

    def convert_ansi(self, paramstring, command):
        """
        Perform the convert ansi utility operation under explicit compatibility rules.

        Example:
            Exercise ANSIStream.convert ansi through a consuming regression::

                python -m pytest -q tests/utils/test_terminal.py


        :param paramstring: Value supplied for paramstring under the utility contract.
        :param command: Value supplied for command under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        params = self.extract_params(paramstring)
        self.call_win32(command, params)

    def extract_params(self, paramstring):
        """
        Perform the extract params utility operation under explicit compatibility rules.

        Example:
            Exercise ANSIStream.extract params through a consuming regression::

                python -m pytest -q tests/utils/test_terminal.py


        :param paramstring: Value supplied for paramstring under the utility contract.
        :return: An iterator yielding the normalized values described above.
        """
        def split(paramstring):
            """
            Perform the split utility operation under explicit compatibility rules.

            Example:
                Exercise ANSIStream.extract params.split through a consuming regression::

                    python -m pytest -q tests/utils/test_terminal.py


            :param paramstring: Value supplied for paramstring under the utility contract.
            :return: An iterator yielding the normalized values described above.
            """
            separator = b";" if isinstance(paramstring, (bytes, bytearray)) else ";"
            for p in paramstring.split(separator):
                if p:
                    yield int(p)

        return tuple(split(paramstring))

    def call_win32(self, command, params):
        """
        Perform the call win32 utility operation under explicit compatibility rules.

        Example:
            Exercise ANSIStream.call win32 through a consuming regression::

                python -m pytest -q tests/utils/test_terminal.py


        :param command: Value supplied for command under the utility contract.
        :param params: Value supplied for params under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if command not in (b"m", "m"):
            return
        fg, bg, bold = self.last_state

        for param in params:
            if param in RCOLORS:
                fg = RCOLORS[param]
            elif param in RBACKGROUNDS:
                bg = RBACKGROUNDS[param]
            elif param == 1:
                bold = True
            elif param == 0:
                fg = "white"
                bg, bold = None, False

        self.last_state = (fg, bg, bold)
        if fg or bg or bold:
            self.set_console(self.file_handle, to_flag(fg, bg, bold))
        else:
            self.set_console(self.file_handle, WCOLORS["white"])


def windows_terminfo():
    """
    Perform the windows terminfo utility operation under explicit compatibility rules.

    Example:
        Exercise windows terminfo through a consuming regression::

            python -m pytest -q tests/utils/test_terminal.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    from ctypes import Structure, byref
    from ctypes.wintypes import SHORT, WORD

    class COORD(Structure):

        """
        struct in wincon.h

        Example:
            Exercise windows terminfo.COORD through a consuming regression::

                python -m pytest -q tests/utils/test_terminal.py
        """

        _fields_ = [
            ("X", SHORT),
            ("Y", SHORT),
        ]

    class SMALL_RECT(Structure):

        """
        struct in wincon.h.

        Example:
            Exercise windows terminfo.SMALL RECT through a consuming regression::

                python -m pytest -q tests/utils/test_terminal.py
        """

        _fields_ = [
            ("Left", SHORT),
            ("Top", SHORT),
            ("Right", SHORT),
            ("Bottom", SHORT),
        ]

    class CONSOLE_SCREEN_BUFFER_INFO(Structure):

        """
        struct in wincon.h.

        Example:
            Exercise windows terminfo.CONSOLE SCREEN BUFFER INFO through a consuming regression::

                python -m pytest -q tests/utils/test_terminal.py
        """

        _fields_ = [
            ("dwSize", COORD),
            ("dwCursorPosition", COORD),
            ("wAttributes", WORD),
            ("srWindow", SMALL_RECT),
            ("dwMaximumWindowSize", COORD),
        ]

    csbi = CONSOLE_SCREEN_BUFFER_INFO()
    import msvcrt

    file_handle = msvcrt.get_osfhandle(sys.stdout.fileno())
    from ctypes import windll

    success = windll.kernel32.GetConsoleScreenBufferInfo(file_handle, byref(csbi))
    if not success:
        raise Exception("stdout is not a console?")
    return csbi


def geometry():
    """
    Perform the geometry utility operation under explicit compatibility rules.

    Example:
        Exercise geometry through a consuming regression::

            python -m pytest -q tests/utils/test_terminal.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if iswindows:
        try:

            ti = windows_terminfo()
            return (ti.dwSize.X or 80, ti.dwSize.Y or 80)
        except:
            return 80, 80
    try:
        import curses

        curses.setupterm()
    except:
        return 80, 80
    else:
        width = curses.tigetnum("cols") or 80
        height = curses.tigetnum("lines") or 80
        return width, height


def test():
    """
    Perform the test utility operation under explicit compatibility rules.

    Example:
        Exercise test through a consuming regression::

            python -m pytest -q tests/utils/test_terminal.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    s = ANSIStream()

    text = [
        colored(t, fg=t) + ". " + colored(t, fg=t, bold=True) + "."
        for t in (
            "red",
            "yellow",
            "green",
            "white",
            "cyan",
            "magenta",
            "blue",
        )
    ]
    s.write("\n".join(text))
    u = "\u041c\u0438\u0445\u0430\u0438\u043b fällen"
    print()
    s.write_unicode_text(u)
    print()


# Todo: Implement this as part of actually making the progress grid work - THIS IS A REALLY GOOD IDEA
# Even if it wasn't actually needed to make the progress grid
class RestorePosition(object):
    """
    Context manager to restore the cursor to it's original position after preforming an operation.

    Example:
        Exercise RestorePosition through a consuming regression::

            python -m pytest -q tests/utils/test_terminal.py
    """

    # https://stackoverflow.com/questions/35526014/how-can-i-get-the-cursors-position-in-an-ansi-terminal
    def __enter__(self):
        """
        Implement the resource's enter lifecycle operation.

        Example:
            Exercise RestorePosition.  enter   through a consuming regression::

                python -m pytest -q tests/utils/test_terminal.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Implement the resource's exit lifecycle operation.

        Example:
            Exercise RestorePosition.  exit   through a consuming regression::

                python -m pytest -q tests/utils/test_terminal.py


        :param exc_type: Value supplied for exc type under the utility contract.
        :param exc_val: Value supplied for exc val under the utility contract.
        :param exc_tb: Value supplied for exc tb under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pass


def getTerminalSize():
    """
    Returns a tuple of the height and width of the terminal window.

    Example:
        Exercise getTerminalSize through a consuming regression::

            python -m pytest -q tests/utils/test_terminal.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    import os

    env = os.environ

    def ioctl_GWINSZ(fd):
        """
        Perform the ioctl GWINSZ utility operation under explicit compatibility rules.

        Example:
            Exercise getTerminalSize.ioctl GWINSZ through a consuming regression::

                python -m pytest -q tests/utils/test_terminal.py


        :param fd: Value supplied for fd under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            import fcntl, termios, struct, os

            cr = struct.unpack("hh", fcntl.ioctl(fd, termios.TIOCGWINSZ, "1234"))
        except:
            return
        return cr

    cr = ioctl_GWINSZ(0) or ioctl_GWINSZ(1) or ioctl_GWINSZ(2)
    if not cr:
        try:
            fd = os.open(os.ctermid(), os.O_RDONLY)
            cr = ioctl_GWINSZ(fd)
            os.close(fd)
        except:
            pass
    if not cr:
        cr = (env.get("LINES", 25), env.get("COLUMNS", 80))

        ### Use get(key[, default]) instead of a try/catch
        # try:
        #    cr = (env['LINES'], env['COLUMNS'])
        # except:
        #    cr = (25, 80)
    return int(cr[1]), int(cr[0])


# Todo: Make this _actually_ safe
def safe_terminal_info_print(info_strs):
    """
    Reliably display text in the terminal - displays in green when possible.

    Example:
        Exercise safe terminal info print through a consuming regression::

            python -m pytest -q tests/utils/test_terminal.py


    :param info_strs: Value supplied for info strs under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    term_width, term_height = getTerminalSize()
    info_strs = (
        [
            "#" * term_width,
        ]
        + info_strs
        + [
            "#" * term_width,
        ]
    )
    puts(clint_colored.green("\n".join(info_strs)))


# -----------------------------------------------------------------------------
# -- Methods to get input from the user start here
# -----------------------------------------------------------------------------


def print_or_prompt(output: str, user_input: bool = False) -> Optional[str]:
    """
    Has the same signature as the Python3 print function - will be useful when directing the print output around.

    Example:
        Exercise print or prompt through a consuming regression::

            python -m pytest -q tests/utils/test_terminal.py


    :param output: Value supplied for output under the utility contract.
    :param user_input: Value supplied for user input under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if not user_input:
        print(output)
        return None
    else:
        new_user_input = input(output)
        new_user_input = str(new_user_input)
        return new_user_input



def y_n_input(message: str) -> bool:
    """
    Takes a message. Prints it. Attempts to parse what the user types back to obtain a y/n input.

    Example:
        Exercise y n input through a consuming regression::

            python -m pytest -q tests/utils/test_terminal.py


    :param message: Value supplied for message under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """

    print_or_prompt(message)

    user_response = print_or_prompt("Please enter y/n\n", True)
    user_response = user_response[:1].lower()

    while user_response not in ["y", "n"]:

        if len(user_response) == 0:
            new_message = "Please enter something. y/n ideally."
            user_response = print_or_prompt(new_message, True)
        else:
            user_response = user_response[1:].lower()
            if user_response not in ["y", "n"]:
                new_message = "Please enter y or n"
                user_response = print_or_prompt(new_message, True)

    if user_response == "y":
        return True

    elif user_response == "n":
        return False

    else:
        err_str = "Error - y_n_input has reached a point it shouldn't"
        raise NotImplementedError(err_str)


def select_from_options(options: Iterable[str]) -> str:
    """
    Takes a list of options - asks the user to choose.

    Example:
        Exercise select from options through a consuming regression::

            python -m pytest -q tests/utils/test_terminal.py


    :param options: Value supplied for options under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    options = list(options)

    header = "Please choose one of the following options.\n"
    print_or_prompt(header)
    valid_input = False
    valid_input_range = range(1, len(options) + 1)

    user_input = ""
    for i in range(len(options)):
        user_input += str(i + 1) + " : " + str(options[i]) + "\n"
    print_or_prompt(user_input)
    usr_rtn = input("Choice?\n")

    while not valid_input:

        try:
            usr_rtn_int = int(usr_rtn)
            if usr_rtn_int in valid_input_range:
                return options[usr_rtn_int - 1]
            else:
                output_str = "Please enter an integer in the range.\n"
                usr_rtn = input(output_str)

        except ValueError:
            output_str = "Unable to parse return as an integer.\n"
            output_str += "Please enter an integer in the range.\n"
            usr_rtn = input(output_str)

    raise NotImplementedError("Please enter an integer in the range.")

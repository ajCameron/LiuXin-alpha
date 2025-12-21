
"""
This is looking to be a common pattern, so a specific event log would be helpful.

This is intended to be embedded in a lot of classes - and provide a common interface
(probably out to the databases, but the advantage of common interface is it doesn't need to be decided now).
"""

import logging
import sys
import os

from LiuXin_alpha.utils.which_os import iswindows

from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode as unicode


default_log = logging.getLogger("LiuXin_alpha-default-log")


def multi_string_print(*args: str) -> None:
    """
    Print when multiple arguments are given.

    :param args:
    :return:
    """
    args = [str(arg) for arg in args]
    print("\n".join(args))



LiuXin_print = multi_string_print
LiuXin_warning_print = multi_string_print


def prints(*args, **kwargs):
    """
    Print Unicode arguments safely by encoding them to preferred_encoding.

    Has the same signature as the print function from Python 3.
    Except for the additional keyword argument safe_encode.
    Which if set to True will cause the function to use repr when encoding fails.

    :param args:
    :param kwargs:
    :return:
    """
    file = kwargs.get("file", sys.stdout)
    sep = kwargs.get("sep", " ")
    end = kwargs.get("end", "\n")
    try:
        enc = preferred_encoding
    except:
        enc = sys.getdefaultencoding()

    safe_encode = kwargs.get("safe_encode", False)

    if "CALIBRE_WORKER" in os.environ:
        enc = "utf-8"

    for i, arg in enumerate(args):

        if isinstance(arg, str):

            if iswindows:
                from LiuXin_alpha.utils.terminal import Detect

                # Todo: This is absolutely not working in any way at all - even a bit
                # Todo: In fact, it is on fire. Right now. Actual flames.
                cs = Detect(file)
                if cs.is_console:
                    cs.write_unicode_text(arg)
                    if i != len(args) - 1:
                        file.write(sep)
                    continue

            try:
                arg = arg.encode(enc)
            except UnicodeEncodeError:
                try:
                    arg = arg.encode("utf-8")
                except:
                    if not safe_encode:
                        raise
                    arg = repr(arg)

            # arg is now in bytes - try turning it back into a utf-8 string
            try:
                arg = arg.decode("utf-8")
            except UnicodeEncodeError:
                if not safe_encode:
                    raise
                arg = repr(arg)

        if isinstance(arg, bytes):
            arg = arg.decode("utf-8")

        if not isinstance(arg, str):
            try:
                arg = str(arg)
            except ValueError:
                arg = unicode(arg)
            if isinstance(arg, unicode):
                try:
                    arg = arg.encode(enc)
                except UnicodeEncodeError:
                    try:
                        arg = arg.encode("utf-8")
                    except:
                        if not safe_encode:
                            raise
                        arg = repr(arg)

        try:
            file.write(arg)
        except:
            import reprlib

            file.write(reprlib.repr(arg))
        if i != len(args) - 1:
            file.write(bytes(sep, "utf-8").decode("utf-8"))

    file.write(bytes(end, "utf-8").decode("utf-8"))


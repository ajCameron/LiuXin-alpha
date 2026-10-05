

"""
Format dates with Calibre-compatible handling for historical years.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise calibre strftime through a consuming regression::

        python -m pytest -q tests/scripts/test_docstring_migration.py
"""
import time

from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode as unicode

from LiuXin_alpha.utils.which_os import iswindows


def strftime(fmt, t=None):
    """
    A version of strftime that returns unicode strings and tries to handle dates before 1900.

    Example:
        Exercise strftime through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_migration.py


    :param fmt: Date, number or template format specification.
    :param t: Value supplied for t under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if not fmt:
        return ""
    if t is None:
        t = time.localtime()
    if hasattr(t, "timetuple"):
        t = t.timetuple()
    early_year = t[0] < 1900
    if early_year:
        replacement = 1900 if t[0] % 4 == 0 else 1901
        fmt = fmt.replace("%Y", "_early year hack##")
        t = list(t)
        orig_year = t[0]
        t[0] = replacement
    ans = None

    if iswindows:
        if isinstance(fmt, unicode):
            fmt = fmt.encode("mbcs")
        fmt = fmt.replace(b"%e", b"%#d")
        ans = plugins["winutil"][0].strftime(fmt, t)
    else:
        ans = time.strftime(fmt, t).decode(preferred_encoding, "replace")

    if early_year:
        ans = ans.replace("_early year hack##", str(orig_year))

    return ans

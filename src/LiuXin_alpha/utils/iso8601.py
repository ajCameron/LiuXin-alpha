#!/usr/bin/env python2
# vim:fileencoding=utf-8
# License: GPLv3 Copyright: 2016, Kovid Goyal <kovid at kovidgoyal.net>

"""
Parse ISO-8601 date/time values with safe local-time handling.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise iso8601 through a consuming regression::

        python -m pytest -q tests/scripts/test_docstring_ownership.py
"""
from __future__ import unicode_literals, division, absolute_import, print_function
from datetime import datetime

from LiuXin_alpha.utils.libraries.liuxin_dateutil.tz import tzlocal, tzutc, tzoffset

from LiuXin_alpha.utils.plugins import plugins

speedup, err = plugins["speedup"]
if not speedup:
    raise RuntimeError(err)


class SafeLocalTimeZone(tzlocal):
    """
    Provide the SafeLocalTimeZone utility contract with explicit state and cleanup behavior.

    Example:
        Exercise SafeLocalTimeZone through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_ownership.py
    """
    def _isdst(self, dt):
        # This method in tzlocal raises ValueError if dt is out of range.
        # In such cases, just assume that dt is not DST.
        """
        Perform the isdst utility operation under explicit compatibility rules.

        Example:
            Exercise SafeLocalTimeZone. isdst through a consuming regression::

                python -m pytest -q tests/scripts/test_docstring_ownership.py


        :param dt: Date or datetime value parsed, normalized or rendered.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            return super(SafeLocalTimeZone, self)._isdst(dt)
        except Exception:
            pass
        return False


utc_tz = tzutc()
local_tz = SafeLocalTimeZone()
del tzutc, tzlocal
UNDEFINED_DATE = datetime(101, 1, 1, tzinfo=utc_tz)


def parse_iso8601(date_string, assume_utc=False, as_utc=True):
    """
    Parse iso8601 under the documented compatibility and safety rules.

    Example:
        Exercise parse iso8601 through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_ownership.py


    :param date_string: Value supplied for date string under the utility contract.
    :param assume_utc: Whether naive input is interpreted as UTC.
    :param as_utc: Whether the result is normalized to UTC.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if not date_string:
        return UNDEFINED_DATE
    dt, aware, tzseconds = speedup.parse_iso8601(date_string)
    tz = utc_tz if assume_utc else local_tz
    if aware:  # timezone was specified
        if tzseconds == 0:
            tz = utc_tz
        else:
            sign = "-" if tzseconds < 0 else "+"
            description = "%s%02d:%02d" % (
                sign,
                abs(tzseconds) // 3600,
                (abs(tzseconds) % 3600) // 60,
            )
            tz = tzoffset(description, tzseconds)
    dt = dt.replace(tzinfo=tz)
    if as_utc and tz is utc_tz:
        return dt
    return dt.astimezone(utc_tz if as_utc else local_tz)


if __name__ == "__main__":
    import sys

    print(parse_iso8601(sys.argv[-1]))

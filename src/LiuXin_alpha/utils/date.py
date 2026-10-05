#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

"""
Parse, normalize, compare and render dates under explicit UTC and local-time policies.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise date through a consuming regression::

        python -m pytest -q tests/utils/test_date_stubbed.py
"""
from __future__ import with_statement

import re
import time
from datetime import datetime, time as dtime, timedelta, MINYEAR, MAXYEAR
from functools import partial

from LiuXin_alpha.utils.libraries.liuxin_dateutil.tz import tzlocal, tzutc, EPOCHORDINAL

from LiuXin_alpha.utils.which_os import iswindows, isosx

from LiuXin_alpha.utils.plugins import plugins

from LiuXin_alpha.utils.localization import lcdata

from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode

__license__ = "GPL v3"
__copyright__ = "2010, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"


def strftime(fmt, t=None):
    """
    A version of strftime that returns unicode strings and tries to handle dates before 1900.

    Example:
        Exercise strftime through a consuming regression::

            python -m pytest -q tests/utils/test_date_stubbed.py


    :param fmt: Date, number or template format specification.
    :param t: Value supplied for t under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    orig_year = 2025

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
        ans = time.strftime(fmt, t)

        # if isinstance(fmt, str):
        #     fmt = fmt.encode("mbcs")
        # fmt = fmt.replace(b"%e", b"%#d")
        # ans = plugins["winutil"][0].strftime(fmt, t)
    else:
        ans = time.strftime(fmt, t)

    if early_year:
        ans = ans.replace("_early year hack##", str(orig_year))

    return ans


class SafeLocalTimeZone(tzlocal):
    """
    Provide the SafeLocalTimeZone utility contract with explicit state and cleanup behavior.

    Example:
        Exercise SafeLocalTimeZone through a consuming regression::

            python -m pytest -q tests/utils/test_date_stubbed.py
    """
    def _isdst(self, dt):
        # We can't use mktime here. It is unstable when deciding if
        # the hour near to a change is DST or not.
        #
        # timestamp = time.mktime((dt.year, dt.month, dt.day, dt.hour,
        #                         dt.minute, dt.second, dt.weekday(), 0, -1))
        # return time.localtime(timestamp).tm_isdst
        #
        # The code above yields the following result:
        #
        # >>> import tz, datetime
        # >>> t = tz.tzlocal()
        # >>> datetime.datetime(2003,2,15,23,tzinfo=t).tzname()
        # 'BRDT'
        # >>> datetime.datetime(2003,2,16,0,tzinfo=t).tzname()
        # 'BRST'
        # >>> datetime.datetime(2003,2,15,23,tzinfo=t).tzname()
        # 'BRST'
        # >>> datetime.datetime(2003,2,15,22,tzinfo=t).tzname()
        # 'BRDT'
        # >>> datetime.datetime(2003,2,15,23,tzinfo=t).tzname()
        # 'BRDT'
        #
        # Here is a more stable implementation:
        #
        """
        Perform the isdst utility operation under explicit compatibility rules.

        Example:
            Exercise SafeLocalTimeZone. isdst through a consuming regression::

                python -m pytest -q tests/utils/test_date_stubbed.py


        :param dt: Date or datetime value parsed, normalized or rendered.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            timestamp = (dt.toordinal() - EPOCHORDINAL) * 86400 + dt.hour * 3600 + dt.minute * 60 + dt.second
            return time.localtime(timestamp + time.timezone).tm_isdst
        except ValueError:
            pass
        return False


utc_tz = _utc_tz = tzutc()
local_tz = _local_tz = SafeLocalTimeZone()

# When parsing ambiguous dates that could be either dd-MM Or MM-dd use the
# user's locale preferences
if iswindows:
    import ctypes

    LOCALE_SSHORTDATE, LOCALE_USER_DEFAULT = 0x1F, 0
    buf = ctypes.create_string_buffer(b"\0", 255)
    try:
        ctypes.windll.kernel32.GetLocaleInfoA(LOCALE_USER_DEFAULT, LOCALE_SSHORTDATE, buf, 255)
        parse_date_day_first = buf.value.index(b"d") < buf.value.index(b"M")
    except:
        parse_date_day_first = False
    del ctypes, LOCALE_SSHORTDATE, buf, LOCALE_USER_DEFAULT
elif isosx:
    try:
        date_fmt = plugins["usbobserver"][0].date_format()
        parse_date_day_first = date_fmt.index("d") < date_fmt.index("M")
    except:
        parse_date_day_first = False
else:
    try:

        def first_index(raw, queries):
            """
            Perform the first index utility operation under explicit compatibility rules.

            Example:
                Exercise first index through a consuming regression::

                    python -m pytest -q tests/utils/test_date_stubbed.py


            :param raw: Value supplied for raw under the utility contract.
            :param queries: Value supplied for queries under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            for q in queries:
                try:
                    return raw.index(q)
                except ValueError:
                    pass
            return -1

        import locale

        raw = locale.nl_langinfo(locale.D_FMT)
        parse_date_day_first = first_index(raw, ("%d", "%a", "%A")) < first_index(raw, ("%m", "%b", "%B"))
        del raw, first_index
    except:
        parse_date_day_first = False

UNDEFINED_DATE = datetime(101, 1, 1, tzinfo=utc_tz)
DEFAULT_DATE = datetime(2000, 1, 1, tzinfo=utc_tz)
EPOCH = datetime(1970, 1, 1, tzinfo=_utc_tz)


def is_date_undefined(qt_or_dt):
    """
    Return or update whether is date undefined holds for the compatibility value.

    Example:
        Exercise is date undefined through a consuming regression::

            python -m pytest -q tests/utils/test_date_stubbed.py


    :param qt_or_dt: Value supplied for qt or dt under the utility contract.
    :return: True when the documented condition holds; otherwise False.
    """
    d = qt_or_dt
    if d is None:
        return True
    if hasattr(d, "toString"):
        if hasattr(d, "date"):
            d = d.date()
        try:
            d = datetime(d.year(), d.month(), d.day(), tzinfo=utc_tz)
        except ValueError:
            return True  # Undefined QDate
    return d.year < UNDEFINED_DATE.year or (
        d.year == UNDEFINED_DATE.year and d.month == UNDEFINED_DATE.month and d.day == UNDEFINED_DATE.day
    )


def parse_date(date_string, assume_utc=False, as_utc=True, default=None):
    """
    Parse a date/time string into a timezone aware datetime object. The timezone is always either UTC or the local timezone.

    Example:
        Exercise parse date through a consuming regression::

            python -m pytest -q tests/utils/test_date_stubbed.py


    :param date_string: Value supplied for date string under the utility contract.
    :param assume_utc: Whether naive input is interpreted as UTC.
    :param as_utc: Whether the result is normalized to UTC.
    :param default: Value supplied for default under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    from LiuXin_alpha.utils.libraries.liuxin_dateutil.parser import parse

    if not date_string:
        return UNDEFINED_DATE
    if default is None:
        func = datetime.utcnow if assume_utc else datetime.now
        default = func().replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
            tzinfo=_utc_tz if assume_utc else _local_tz,
        )
    dt = parse(date_string, default=default, dayfirst=parse_date_day_first)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_utc_tz if assume_utc else _local_tz)
    return dt.astimezone(_utc_tz if as_utc else _local_tz)


def fix_only_date(val):
    """
    Perform the fix only date utility operation under explicit compatibility rules.

    Example:
        Exercise fix only date through a consuming regression::

            python -m pytest -q tests/utils/test_date_stubbed.py


    :param val: Template or metadata value evaluated by the operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if val in ("", None):
        return val
    if isinstance(val, str):
        raw = val.strip()
        if not raw:
            return raw
        if re.match(r"^\d{4}$", raw):
            return raw + "-01-01"
        if re.match(r"^\d{4}-\d{2}$", raw):
            return raw + "-01"
        return raw
    n = val + timedelta(days=1)
    if n.month > val.month:
        val = val.replace(day=val.day - 1)
    if val.day == 1:
        val = val.replace(day=2)
    return val


def parse_only_date(raw, assume_utc=True, as_utc=True):
    """
    Parse a date string that contains no time information in a manner that guarantees that the month and year are always correct in all timezones, and the day is at most one day wrong.

    Example:
        Exercise parse only date through a consuming regression::

            python -m pytest -q tests/utils/test_date_stubbed.py


    :param raw: Value supplied for raw under the utility contract.
    :param assume_utc: Whether naive input is interpreted as UTC.
    :param as_utc: Whether the result is normalized to UTC.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    f = utcnow if assume_utc else now
    default = f().replace(hour=0, minute=0, second=0, microsecond=0, day=15)
    ans = parse_date(raw, default=default, assume_utc=assume_utc, as_utc=as_utc)
    n = ans + timedelta(days=1)
    if n.month > ans.month:
        ans = ans.replace(day=ans.day - 1)
    if ans.day == 1:
        ans = ans.replace(day=2)
    return ans


def strptime(val, fmt, assume_utc=False, as_utc=True):
    """
    Perform the strptime utility operation under explicit compatibility rules.

    Example:
        Exercise strptime through a consuming regression::

            python -m pytest -q tests/utils/test_date_stubbed.py


    :param val: Template or metadata value evaluated by the operation.
    :param fmt: Date, number or template format specification.
    :param assume_utc: Whether naive input is interpreted as UTC.
    :param as_utc: Whether the result is normalized to UTC.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    dt = datetime.strptime(val, fmt)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_utc_tz if assume_utc else _local_tz)
    return dt.astimezone(_utc_tz if as_utc else _local_tz)


def dt_factory(time_t, assume_utc=False, as_utc=True):
    """
    Perform the dt factory utility operation under explicit compatibility rules.

    Example:
        Exercise dt factory through a consuming regression::

            python -m pytest -q tests/utils/test_date_stubbed.py


    :param time_t: Value supplied for time t under the utility contract.
    :param assume_utc: Whether naive input is interpreted as UTC.
    :param as_utc: Whether the result is normalized to UTC.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    dt = datetime(*(time_t[0:6]))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_utc_tz if assume_utc else _local_tz)
    return dt.astimezone(_utc_tz if as_utc else _local_tz)


safeyear = lambda x: min(max(x, MINYEAR), MAXYEAR)


def qt_to_dt(qdate_or_qdatetime, as_utc=True):
    """
    Perform the qt to dt utility operation under explicit compatibility rules.

    Example:
        Exercise qt to dt through a consuming regression::

            python -m pytest -q tests/utils/test_date_stubbed.py


    :param qdate_or_qdatetime: Value supplied for qdate or qdatetime under the utility
        contract.
    :param as_utc: Whether the result is normalized to UTC.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    o = qdate_or_qdatetime
    if hasattr(o, "toUTC"):
        # QDateTime
        o = o.toUTC()
        d, t = o.date(), o.time()
        try:
            ans = datetime(
                safeyear(d.year()),
                d.month(),
                d.day(),
                t.hour(),
                t.minute(),
                t.second(),
                t.msec() * 1000,
                utc_tz,
            )
        except ValueError:
            ans = datetime(
                safeyear(d.year()),
                d.month(),
                1,
                t.hour(),
                t.minute(),
                t.second(),
                t.msec() * 1000,
                utc_tz,
            )
        if not as_utc:
            ans = ans.astimezone(local_tz)
        return ans

    try:
        dt = datetime(safeyear(o.year()), o.month(), o.day()).replace(tzinfo=_local_tz)
    except ValueError:
        dt = datetime(safeyear(o.year()), o.month(), 1).replace(tzinfo=_local_tz)
    return dt.astimezone(_utc_tz if as_utc else _local_tz)


def fromtimestamp(ctime, as_utc=True):
    """
    Perform the fromtimestamp utility operation under explicit compatibility rules.

    Example:
        Exercise fromtimestamp through a consuming regression::

            python -m pytest -q tests/utils/test_date_stubbed.py


    :param ctime: Value supplied for ctime under the utility contract.
    :param as_utc: Whether the result is normalized to UTC.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    dt = datetime.utcfromtimestamp(ctime).replace(tzinfo=_utc_tz)
    if not as_utc:
        dt = dt.astimezone(_local_tz)
    return dt


def fromordinal(day, as_utc=True):
    """
    Perform the fromordinal utility operation under explicit compatibility rules.

    Example:
        Exercise fromordinal through a consuming regression::

            python -m pytest -q tests/utils/test_date_stubbed.py


    :param day: Value supplied for day under the utility contract.
    :param as_utc: Whether the result is normalized to UTC.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return datetime.fromordinal(day).replace(tzinfo=_utc_tz if as_utc else _local_tz)


def isoformat(date_time, assume_utc=False, as_utc=True, sep="T"):
    """
    Perform the isoformat utility operation under explicit compatibility rules.

    Example:
        Exercise isoformat through a consuming regression::

            python -m pytest -q tests/utils/test_date_stubbed.py


    :param date_time: Value supplied for date time under the utility contract.
    :param assume_utc: Whether naive input is interpreted as UTC.
    :param as_utc: Whether the result is normalized to UTC.
    :param sep: Delimiter used to split or join list values.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if not hasattr(date_time, "tzinfo"):
        return six_unicode(date_time.isoformat())
    if date_time.tzinfo is None:
        date_time = date_time.replace(tzinfo=_utc_tz if assume_utc else _local_tz)
    date_time = date_time.astimezone(_utc_tz if as_utc else _local_tz)
    # str(sep) because isoformat barfs with unicode sep on python 2.x
    return six_unicode(date_time.isoformat(str(sep)))


def as_local_time(date_time, assume_utc=True):
    """
    Perform the as local time utility operation under explicit compatibility rules.

    Example:
        Exercise as local time through a consuming regression::

            python -m pytest -q tests/utils/test_date_stubbed.py


    :param date_time: Value supplied for date time under the utility contract.
    :param assume_utc: Whether naive input is interpreted as UTC.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if not hasattr(date_time, "tzinfo"):
        return date_time
    if date_time.tzinfo is None:
        date_time = date_time.replace(tzinfo=_utc_tz if assume_utc else _local_tz)
    return date_time.astimezone(_local_tz)


def dt_as_local(dt):
    """
    Perform the dt as local utility operation under explicit compatibility rules.

    Example:
        Exercise dt as local through a consuming regression::

            python -m pytest -q tests/utils/test_date_stubbed.py


    :param dt: Date or datetime value parsed, normalized or rendered.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if dt.tzinfo is local_tz:
        return dt
    return dt.astimezone(local_tz)


def as_utc(date_time, assume_utc=True):
    """
    Perform the as utc utility operation under explicit compatibility rules.

    Example:
        Exercise as utc through a consuming regression::

            python -m pytest -q tests/utils/test_date_stubbed.py


    :param date_time: Value supplied for date time under the utility contract.
    :param assume_utc: Whether naive input is interpreted as UTC.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if not hasattr(date_time, "tzinfo"):
        return date_time
    if date_time.tzinfo is None:
        date_time = date_time.replace(tzinfo=_utc_tz if assume_utc else _local_tz)
    return date_time.astimezone(_utc_tz)


def now():
    """
    Perform the now utility operation under explicit compatibility rules.

    Example:
        Exercise now through a consuming regression::

            python -m pytest -q tests/utils/test_date_stubbed.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return datetime.now().replace(tzinfo=_local_tz)


def utcnow():
    """
    Perform the utcnow utility operation under explicit compatibility rules.

    Example:
        Exercise utcnow through a consuming regression::

            python -m pytest -q tests/utils/test_date_stubbed.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return datetime.now(_utc_tz)


def utcfromtimestamp(stamp):
    """
    Perform the utcfromtimestamp utility operation under explicit compatibility rules.

    Example:
        Exercise utcfromtimestamp through a consuming regression::

            python -m pytest -q tests/utils/test_date_stubbed.py


    :param stamp: Value supplied for stamp under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    try:
        return datetime.utcfromtimestamp(stamp).replace(tzinfo=_utc_tz)
    except ValueError:
        # Raised if stamp is out of range for the platforms gmtime function
        # For example, this happens with negative values on windows
        try:
            return EPOCH + timedelta(seconds=stamp)
        except (ValueError, OverflowError):
            # datetime can only represent years between 1 and 9999
            import traceback

            traceback.print_exc()
    return utcnow()


def timestampfromdt(dt, assume_utc=True):
    """
    Perform the timestampfromdt utility operation under explicit compatibility rules.

    Example:
        Exercise timestampfromdt through a consuming regression::

            python -m pytest -q tests/utils/test_date_stubbed.py


    :param dt: Date or datetime value parsed, normalized or rendered.
    :param assume_utc: Whether naive input is interpreted as UTC.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return (as_utc(dt, assume_utc=assume_utc) - EPOCH).total_seconds()


# Format date functions


def fd_format_hour(dt, ampm, hr):
    """
    Perform the fd format hour utility operation under explicit compatibility rules.

    Example:
        Exercise fd format hour through a consuming regression::

            python -m pytest -q tests/utils/test_date_stubbed.py


    :param dt: Date or datetime value parsed, normalized or rendered.
    :param ampm: Value supplied for ampm under the utility contract.
    :param hr: Value supplied for hr under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    l = len(hr)
    h = dt.hour
    if ampm:
        h = h % 12
    if l == 1:
        return "%d" % h
    return "%02d" % h


def fd_format_minute(dt, ampm, min):
    """
    Perform the fd format minute utility operation under explicit compatibility rules.

    Example:
        Exercise fd format minute through a consuming regression::

            python -m pytest -q tests/utils/test_date_stubbed.py


    :param dt: Date or datetime value parsed, normalized or rendered.
    :param ampm: Value supplied for ampm under the utility contract.
    :param min: Value supplied for min under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    l = len(min)
    if l == 1:
        return "%d" % dt.minute
    return "%02d" % dt.minute


def fd_format_second(dt, ampm, sec):
    """
    Perform the fd format second utility operation under explicit compatibility rules.

    Example:
        Exercise fd format second through a consuming regression::

            python -m pytest -q tests/utils/test_date_stubbed.py


    :param dt: Date or datetime value parsed, normalized or rendered.
    :param ampm: Value supplied for ampm under the utility contract.
    :param sec: Value supplied for sec under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    l = len(sec)
    if l == 1:
        return "%d" % dt.second
    return "%02d" % dt.second


def fd_format_ampm(dt, ampm, ap):
    """
    Perform the fd format ampm utility operation under explicit compatibility rules.

    Example:
        Exercise fd format ampm through a consuming regression::

            python -m pytest -q tests/utils/test_date_stubbed.py


    :param dt: Date or datetime value parsed, normalized or rendered.
    :param ampm: Value supplied for ampm under the utility contract.
    :param ap: Value supplied for ap under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    res = strftime("%p", t=dt.timetuple())
    if ap == "AP":
        return res
    return res.lower()


def fd_format_day(dt, ampm, dy):
    """
    Perform the fd format day utility operation under explicit compatibility rules.

    Example:
        Exercise fd format day through a consuming regression::

            python -m pytest -q tests/utils/test_date_stubbed.py


    :param dt: Date or datetime value parsed, normalized or rendered.
    :param ampm: Value supplied for ampm under the utility contract.
    :param dy: Value supplied for dy under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    l = len(dy)
    if l == 1:
        return "%d" % dt.day
    if l == 2:
        return "%02d" % dt.day
    return lcdata["abday" if l == 3 else "day"][(dt.weekday() + 1) % 7]


def fd_format_month(dt, ampm, mo):
    """
    Perform the fd format month utility operation under explicit compatibility rules.

    Example:
        Exercise fd format month through a consuming regression::

            python -m pytest -q tests/utils/test_date_stubbed.py


    :param dt: Date or datetime value parsed, normalized or rendered.
    :param ampm: Value supplied for ampm under the utility contract.
    :param mo: Value supplied for mo under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    l = len(mo)
    if l == 1:
        return "%d" % dt.month
    if l == 2:
        return "%02d" % dt.month
    return lcdata["abmon" if l == 3 else "mon"][dt.month - 1]


def fd_format_year(dt, ampm, yr):
    """
    Perform the fd format year utility operation under explicit compatibility rules.

    Example:
        Exercise fd format year through a consuming regression::

            python -m pytest -q tests/utils/test_date_stubbed.py


    :param dt: Date or datetime value parsed, normalized or rendered.
    :param ampm: Value supplied for ampm under the utility contract.
    :param yr: Value supplied for yr under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if len(yr) == 2:
        return "%02d" % (dt.year % 100)
    return "%04d" % dt.year


fd_function_index = {
    "d": fd_format_day,
    "M": fd_format_month,
    "y": fd_format_year,
    "h": fd_format_hour,
    "m": fd_format_minute,
    "s": fd_format_second,
    "a": fd_format_ampm,
    "A": fd_format_ampm,
}


def fd_repl_func(dt, ampm, mo):
    """
    Perform the fd repl func utility operation under explicit compatibility rules.

    Example:
        Exercise fd repl func through a consuming regression::

            python -m pytest -q tests/utils/test_date_stubbed.py


    :param dt: Date or datetime value parsed, normalized or rendered.
    :param ampm: Value supplied for ampm under the utility contract.
    :param mo: Value supplied for mo under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    s = mo.group(0)
    if not s:
        return ""
    return fd_function_index[s[0]](dt, ampm, s)


def format_date(dt, format, assume_utc=False, as_utc=False):
    """
    Return a date formatted as a string using a subset of Qt's formatting codes

    Example:
        Exercise format date through a consuming regression::

            python -m pytest -q tests/utils/test_date_stubbed.py


    :param dt: Date or datetime value parsed, normalized or rendered.
    :param format: Value supplied for format under the utility contract.
    :param assume_utc: Whether naive input is interpreted as UTC.
    :param as_utc: Whether the result is normalized to UTC.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if not format:
        format = "dd MMM yyyy"

    if not isinstance(dt, datetime):
        dt = datetime.combine(dt, dtime())

    if hasattr(dt, "tzinfo"):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=_utc_tz if assume_utc else _local_tz)
        dt = dt.astimezone(_utc_tz if as_utc else _local_tz)

    if format == "iso":
        return isoformat(dt, assume_utc=assume_utc, as_utc=as_utc)

    if dt == UNDEFINED_DATE:
        return ""

    repl_func = partial(fd_repl_func, dt, "ap" in format.lower())
    return re.sub(
        "(s{1,2})|(m{1,2})|(h{1,2})|(ap)|(AP)|(d{1,4}|M{1,4}|(?:yyyy|yy))",
        repl_func,
        format,
    )


# Clean date functions


def cd_has_hour(tt, dt):
    """
    Perform the cd has hour utility operation under explicit compatibility rules.

    Example:
        Exercise cd has hour through a consuming regression::

            python -m pytest -q tests/utils/test_date_stubbed.py


    :param tt: Value supplied for tt under the utility contract.
    :param dt: Date or datetime value parsed, normalized or rendered.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    tt["hour"] = dt.hour
    return ""


def cd_has_minute(tt, dt):
    """
    Perform the cd has minute utility operation under explicit compatibility rules.

    Example:
        Exercise cd has minute through a consuming regression::

            python -m pytest -q tests/utils/test_date_stubbed.py


    :param tt: Value supplied for tt under the utility contract.
    :param dt: Date or datetime value parsed, normalized or rendered.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    tt["min"] = dt.minute
    return ""


def cd_has_second(tt, dt):
    """
    Perform the cd has second utility operation under explicit compatibility rules.

    Example:
        Exercise cd has second through a consuming regression::

            python -m pytest -q tests/utils/test_date_stubbed.py


    :param tt: Value supplied for tt under the utility contract.
    :param dt: Date or datetime value parsed, normalized or rendered.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    tt["sec"] = dt.second
    return ""


def cd_has_day(tt, dt):
    """
    Perform the cd has day utility operation under explicit compatibility rules.

    Example:
        Exercise cd has day through a consuming regression::

            python -m pytest -q tests/utils/test_date_stubbed.py


    :param tt: Value supplied for tt under the utility contract.
    :param dt: Date or datetime value parsed, normalized or rendered.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    tt["day"] = dt.day
    return ""


def cd_has_month(tt, dt):
    """
    Perform the cd has month utility operation under explicit compatibility rules.

    Example:
        Exercise cd has month through a consuming regression::

            python -m pytest -q tests/utils/test_date_stubbed.py


    :param tt: Value supplied for tt under the utility contract.
    :param dt: Date or datetime value parsed, normalized or rendered.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    tt["mon"] = dt.month
    return ""


def cd_has_year(tt, dt):
    """
    Perform the cd has year utility operation under explicit compatibility rules.

    Example:
        Exercise cd has year through a consuming regression::

            python -m pytest -q tests/utils/test_date_stubbed.py


    :param tt: Value supplied for tt under the utility contract.
    :param dt: Date or datetime value parsed, normalized or rendered.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    tt["year"] = dt.year
    return ""


cd_function_index = {
    "d": cd_has_day,
    "M": cd_has_month,
    "y": cd_has_year,
    "h": cd_has_hour,
    "m": cd_has_minute,
    "s": cd_has_second,
}


def cd_repl_func(tt, dt, match_object):
    """
    Perform the cd repl func utility operation under explicit compatibility rules.

    Example:
        Exercise cd repl func through a consuming regression::

            python -m pytest -q tests/utils/test_date_stubbed.py


    :param tt: Value supplied for tt under the utility contract.
    :param dt: Date or datetime value parsed, normalized or rendered.
    :param match_object: Value supplied for match object under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    s = match_object.group(0)
    if not s:
        return ""
    return cd_function_index[s[0]](tt, dt)


def clean_date_for_sort(dt, fmt=None):
    """
    Return dt with fields not in shown in format set to a default

    Example:
        Exercise clean date for sort through a consuming regression::

            python -m pytest -q tests/utils/test_date_stubbed.py


    :param dt: Date or datetime value parsed, normalized or rendered.
    :param fmt: Date, number or template format specification.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if not fmt:
        fmt = "yyMd"

    if not isinstance(dt, datetime):
        dt = datetime.combine(dt, dtime())

    if hasattr(dt, "tzinfo"):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=_local_tz)
        dt = as_local_time(dt)

    if fmt == "iso":
        fmt = "yyMdhms"

    tt = {
        "year": UNDEFINED_DATE.year,
        "mon": UNDEFINED_DATE.month,
        "day": UNDEFINED_DATE.day,
        "hour": UNDEFINED_DATE.hour,
        "min": UNDEFINED_DATE.minute,
        "sec": UNDEFINED_DATE.second,
    }

    repl_func = partial(cd_repl_func, tt, dt)
    re.sub("(s{1,2})|(m{1,2})|(h{1,2})|(d{1,4}|M{1,4}|(?:yyyy|yy))", repl_func, fmt)
    return dt.replace(
        year=tt["year"],
        month=tt["mon"],
        day=tt["day"],
        hour=tt["hour"],
        minute=tt["min"],
        second=tt["sec"],
        microsecond=0,
    )


def replace_months(datestr, clang):
    # Replace months by english equivalent for parse_date
    """
    Perform the replace months utility operation under explicit compatibility rules.

    Example:
        Exercise replace months through a consuming regression::

            python -m pytest -q tests/utils/test_date_stubbed.py


    :param datestr: Value supplied for datestr under the utility contract.
    :param clang: Value supplied for clang under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    frtoen = {
        "[jJ]anvier": "jan",
        "[fF].vrier": "feb",
        "[mM]ars": "mar",
        "[aA]vril": "apr",
        "[mM]ai": "may",
        "[jJ]uin": "jun",
        "[jJ]uillet": "jul",
        "[aA]o.t": "aug",
        "[sS]eptembre": "sep",
        "[Oo]ctobre": "oct",
        "[nN]ovembre": "nov",
        "[dD].cembre": "dec",
    }
    detoen = {
        "[jJ]anuar": "jan",
        "[fF]ebruar": "feb",
        "[mM].rz": "mar",
        "[aA]pril": "apr",
        "[mM]ai": "may",
        "[jJ]uni": "jun",
        "[jJ]uli": "jul",
        "[aA]ugust": "aug",
        "[sS]eptember": "sep",
        "[Oo]ktober": "oct",
        "[nN]ovember": "nov",
        "[dD]ezember": "dec",
    }

    if clang == "fr":
        dictoen = frtoen
    elif clang == "de":
        dictoen = detoen
    else:
        return datestr

    for k in dictoen.iterkeys():
        tmp = re.sub(k, dictoen[k], datestr)
        if tmp != datestr:
            break
    return tmp


def isoformat_timestamp(stamp=None, assume_utc=True, as_utc=True):
    """
    Returns the current datetime, or a supplied timestamp, as an isoformat timestamp.

    Example:
        Exercise isoformat timestamp through a consuming regression::

            python -m pytest -q tests/utils/test_date_stubbed.py


    :param stamp: Value supplied for stamp under the utility contract.
    :param assume_utc: Whether naive input is interpreted as UTC.
    :param as_utc: Whether the result is normalized to UTC.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    import datetime

    if stamp is None:
        return six_unicode(datetime.datetime.now().isoformat())
    if hasattr(stamp, "tzinfo"):
        return isoformat(stamp, assume_utc=assume_utc, as_utc=as_utc)
    return isoformat(utcfromtimestamp(stamp), assume_utc=True, as_utc=as_utc)


def file_date():
    """
    A date of the form ddmmyyyy suitable for writing into file names.

    Example:
        Exercise file date through a consuming regression::

            python -m pytest -q tests/utils/test_date_stubbed.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return six_unicode(time.strftime("%d%m%Y"))

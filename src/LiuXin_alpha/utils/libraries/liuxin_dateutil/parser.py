# -*- coding:iso-8859-1 -*-
"""
Parse free-form date and time strings into normalized datetime values.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise parser through a consuming regression::

        python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py
"""
__author__ = "Gustavo Niemeyer <gustavo@niemeyer.net>"
__license__ = "PSF License"

import datetime
import string
import time
import sys
import os

try:
    from io import StringIO
except ImportError:
    from io import StringIO

from . import relativedelta
from . import tz


__all__ = ["parse", "parserinfo"]


# Some pointers:
#
# http://www.cl.cam.ac.uk/~mgk25/iso-time.html
# http://www.iso.ch/iso/en/prods-services/popstds/datesandtime.html
# http://www.w3.org/TR/NOTE-datetime
# http://ringmaster.arc.nasa.gov/tools/time_formats.html
# http://search.cpan.org/author/MUIR/Time-modules-2003.0211/lib/Time/ParseDate.pm
# http://stein.cshl.org/jade/distrib/docs/java.text.SimpleDateFormat.html

from past.builtins import str


class _timelex(object):
    """
    Provide the timelex utility contract with explicit state and cleanup behavior.

    Example:
        Exercise  timelex through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py
    """
    def __init__(self, instream):
        """
        Initialize and validate the timelex state.

        Example:
            Exercise  timelex.  init   through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param instream: Value supplied for instream under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        if isinstance(instream, str):
            instream = StringIO(instream)
        self.instream = instream
        self.wordchars = (
            "abcdfeghijklmnopqrstuvwxyz"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ_"
            "ßàáâãäåæçèéêëìíîïðñòóôõöøùúûüýþÿ"
            "ÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖØÙÚÛÜÝÞ"
        )
        self.numchars = "0123456789"
        self.whitespace = " \t\r\n"
        self.charstack = []
        self.tokenstack = []
        self.eof = False

    def get_token(self):
        """
        Return token under the documented compatibility and safety rules.

        Example:
            Exercise  timelex.get token through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self.tokenstack:
            return self.tokenstack.pop(0)
        seenletters = False
        token = None
        state = None
        wordchars = self.wordchars
        numchars = self.numchars
        whitespace = self.whitespace
        while not self.eof:
            if self.charstack:
                nextchar = self.charstack.pop(0)
            else:
                nextchar = self.instream.read(1)
                while nextchar == "\x00":
                    nextchar = self.instream.read(1)
            if not nextchar:
                self.eof = True
                break
            elif not state:
                token = nextchar
                if nextchar in wordchars:
                    state = "a"
                elif nextchar in numchars:
                    state = "0"
                elif nextchar in whitespace:
                    token = " "
                    break  # emit token
                else:
                    break  # emit token
            elif state == "a":
                seenletters = True
                if nextchar in wordchars:
                    token += nextchar
                elif nextchar == ".":
                    token += nextchar
                    state = "a."
                else:
                    self.charstack.append(nextchar)
                    break  # emit token
            elif state == "0":
                if nextchar in numchars:
                    token += nextchar
                elif nextchar == ".":
                    token += nextchar
                    state = "0."
                else:
                    self.charstack.append(nextchar)
                    break  # emit token
            elif state == "a.":
                seenletters = True
                if nextchar == "." or nextchar in wordchars:
                    token += nextchar
                elif nextchar in numchars and token[-1] == ".":
                    token += nextchar
                    state = "0."
                else:
                    self.charstack.append(nextchar)
                    break  # emit token
            elif state == "0.":
                if nextchar == "." or nextchar in numchars:
                    token += nextchar
                elif nextchar in wordchars and token[-1] == ".":
                    token += nextchar
                    state = "a."
                else:
                    self.charstack.append(nextchar)
                    break  # emit token
        if state in ("a.", "0.") and (seenletters or token.count(".") > 1 or token[-1] == "."):
            l = token.split(".")
            token = l[0]
            for tok in l[1:]:
                self.tokenstack.append(".")
                if tok:
                    self.tokenstack.append(tok)
        return token

    def __iter__(self):
        """
        Expose iter behavior for the compatibility container.

        Example:
            Exercise  timelex.  iter   through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self

    def __next__(self):
        """
        Perform the next utility operation under explicit compatibility rules.

        Example:
            Exercise  timelex.  next   through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        token = self.get_token()
        if token is None:
            raise StopIteration
        return token

    def split(cls, s):
        """
        Perform the split utility operation under explicit compatibility rules.

        Example:
            Exercise  timelex.split through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param s: Value supplied for s under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return list(cls(s))

    split = classmethod(split)


class _resultbase(object):
    """
    Provide the resultbase utility contract with explicit state and cleanup behavior.

    Example:
        Exercise  resultbase through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py
    """
    def __init__(self):
        """
        Initialize and validate the resultbase state.

        Example:
            Exercise  resultbase.  init   through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; validated state is stored on the receiving object.
        """
        for attr in self.__slots__:
            setattr(self, attr, None)

    def _repr(self, classname):
        """
        Perform the repr utility operation under explicit compatibility rules.

        Example:
            Exercise  resultbase. repr through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param classname: Value supplied for classname under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        l = []
        for attr in self.__slots__:
            value = getattr(self, attr)
            if value is not None:
                l.append("%s=%s" % (attr, repr(value)))
        return "%s(%s)" % (classname, ", ".join(l))

    def __repr__(self):
        """
        Perform the repr utility operation under explicit compatibility rules.

        Example:
            Exercise  resultbase.  repr   through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self._repr(self.__class__.__name__)


class parserinfo(object):

    # m from a.m/p.m, t from ISO T separator
    """
    Provide the parserinfo utility contract with explicit state and cleanup behavior.

    Example:
        Exercise parserinfo through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py
    """
    JUMP = [
        " ",
        ".",
        ",",
        ";",
        "-",
        "/",
        "'",
        "at",
        "on",
        "and",
        "ad",
        "m",
        "t",
        "of",
        "st",
        "nd",
        "rd",
        "th",
    ]

    WEEKDAYS = [
        ("Mon", "Monday"),
        ("Tue", "Tuesday"),
        ("Wed", "Wednesday"),
        ("Thu", "Thursday"),
        ("Fri", "Friday"),
        ("Sat", "Saturday"),
        ("Sun", "Sunday"),
    ]
    MONTHS = [
        ("Jan", "January"),
        ("Feb", "February"),
        ("Mar", "March"),
        ("Apr", "April"),
        ("May", "May"),
        ("Jun", "June"),
        ("Jul", "July"),
        ("Aug", "August"),
        ("Sep", "September"),
        ("Oct", "October"),
        ("Nov", "November"),
        ("Dec", "December"),
    ]
    HMS = [
        ("h", "hour", "hours"),
        ("m", "minute", "minutes"),
        ("s", "second", "seconds"),
    ]
    AMPM = [("am", "a"), ("pm", "p")]
    UTCZONE = ["UTC", "GMT", "Z"]
    PERTAIN = ["of"]
    TZOFFSET = {}

    def __init__(self, dayfirst=False, yearfirst=False):
        """
        Initialize and validate the parserinfo state.

        Example:
            Exercise parserinfo.  init   through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param dayfirst: Value supplied for dayfirst under the utility contract.
        :param yearfirst: Value supplied for yearfirst under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self._jump = self._convert(self.JUMP)
        self._weekdays = self._convert(self.WEEKDAYS)
        self._months = self._convert(self.MONTHS)
        self._hms = self._convert(self.HMS)
        self._ampm = self._convert(self.AMPM)
        self._utczone = self._convert(self.UTCZONE)
        self._pertain = self._convert(self.PERTAIN)

        self.dayfirst = dayfirst
        self.yearfirst = yearfirst

        self._year = time.localtime().tm_year
        self._century = self._year // 100 * 100

    def _convert(self, lst):
        """
        Perform the convert utility operation under explicit compatibility rules.

        Example:
            Exercise parserinfo. convert through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param lst: Value supplied for lst under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        dct = {}
        for i in range(len(lst)):
            v = lst[i]
            if isinstance(v, tuple):
                for v in v:
                    dct[v.lower()] = i
            else:
                dct[v.lower()] = i
        return dct

    def jump(self, name):
        """
        Perform the jump utility operation under explicit compatibility rules.

        Example:
            Exercise parserinfo.jump through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param name: Field, file, function or resource name addressed by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return name.lower() in self._jump

    def weekday(self, name):
        """
        Perform the weekday utility operation under explicit compatibility rules.

        Example:
            Exercise parserinfo.weekday through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param name: Field, file, function or resource name addressed by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if len(name) >= 3:
            try:
                return self._weekdays[name.lower()]
            except KeyError:
                pass
        return None

    def month(self, name):
        """
        Perform the month utility operation under explicit compatibility rules.

        Example:
            Exercise parserinfo.month through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param name: Field, file, function or resource name addressed by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if len(name) >= 3:
            try:
                return self._months[name.lower()] + 1
            except KeyError:
                pass
        return None

    def hms(self, name):
        """
        Perform the hms utility operation under explicit compatibility rules.

        Example:
            Exercise parserinfo.hms through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param name: Field, file, function or resource name addressed by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            return self._hms[name.lower()]
        except KeyError:
            return None

    def ampm(self, name):
        """
        Perform the ampm utility operation under explicit compatibility rules.

        Example:
            Exercise parserinfo.ampm through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param name: Field, file, function or resource name addressed by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            return self._ampm[name.lower()]
        except KeyError:
            return None

    def pertain(self, name):
        """
        Perform the pertain utility operation under explicit compatibility rules.

        Example:
            Exercise parserinfo.pertain through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param name: Field, file, function or resource name addressed by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return name.lower() in self._pertain

    def utczone(self, name):
        """
        Perform the utczone utility operation under explicit compatibility rules.

        Example:
            Exercise parserinfo.utczone through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param name: Field, file, function or resource name addressed by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return name.lower() in self._utczone

    def tzoffset(self, name):
        """
        Perform the tzoffset utility operation under explicit compatibility rules.

        Example:
            Exercise parserinfo.tzoffset through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param name: Field, file, function or resource name addressed by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if name in self._utczone:
            return 0
        return self.TZOFFSET.get(name)

    def convertyear(self, year):
        """
        Perform the convertyear utility operation under explicit compatibility rules.

        Example:
            Exercise parserinfo.convertyear through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param year: Value supplied for year under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if year < 100:
            year += self._century
            if abs(year - self._year) >= 50:
                if year < self._year:
                    year += 100
                else:
                    year -= 100
        return year

    def validate(self, res):
        # move to info
        """
        Perform the validate utility operation under explicit compatibility rules.

        Example:
            Exercise parserinfo.validate through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param res: Value supplied for res under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if res.year is not None:
            res.year = self.convertyear(res.year)
        if res.tzoffset == 0 and not res.tzname or res.tzname == "Z":
            res.tzname = "UTC"
            res.tzoffset = 0
        elif res.tzoffset != 0 and res.tzname and self.utczone(res.tzname):
            res.tzoffset = 0
        return True


class parser(object):
    """
    Provide the parser utility contract with explicit state and cleanup behavior.

    Example:
        Exercise parser through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py
    """
    def __init__(self, info=None):
        """
        Initialize and validate the parser state.

        Example:
            Exercise parser.  init   through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param info: Value supplied for info under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.info = info or parserinfo()

    def parse(self, timestr, default=None, ignoretz=False, tzinfos=None, **kwargs):
        """
        Parse the supplied date text and return its normalized datetime value.

        Example:
            Exercise parser.parse through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param timestr: Value supplied for timestr under the utility contract.
        :param default: Value supplied for default under the utility contract.
        :param ignoretz: Value supplied for ignoretz under the utility contract.
        :param tzinfos: Value supplied for tzinfos under the utility contract.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if not default:
            default = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        res = self._parse(timestr, **kwargs)
        if res is None:
            raise ValueError("unknown string format")
        repl = {}
        for attr in ["year", "month", "day", "hour", "minute", "second", "microsecond"]:
            value = getattr(res, attr)
            if value is not None:
                repl[attr] = value
        ret = default.replace(**repl)
        if res.weekday is not None and not res.day:
            ret = ret + relativedelta.relativedelta(weekday=res.weekday)
        if not ignoretz:
            if callable(tzinfos) or tzinfos and res.tzname in tzinfos:
                if callable(tzinfos):
                    tzdata = tzinfos(res.tzname, res.tzoffset)
                else:
                    tzdata = tzinfos.get(res.tzname)
                if isinstance(tzdata, datetime.tzinfo):
                    tzinfo = tzdata
                elif isinstance(tzdata, str):
                    tzinfo = tz.tzstr(tzdata)
                elif isinstance(tzdata, int):
                    tzinfo = tz.tzoffset(res.tzname, tzdata)
                else:
                    raise ValueError("offset must be tzinfo subclass, " "tz string, or int offset")
                ret = ret.replace(tzinfo=tzinfo)
            elif res.tzname and res.tzname in time.tzname:
                ret = ret.replace(tzinfo=tz.tzlocal())
            elif res.tzoffset == 0:
                ret = ret.replace(tzinfo=tz.tzutc())
            elif res.tzoffset:
                ret = ret.replace(tzinfo=tz.tzoffset(res.tzname, res.tzoffset))
        return ret

    class _result(_resultbase):
        """
        Provide the result utility contract with explicit state and cleanup behavior.

        Example:
            Exercise parser. result through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py
        """
        __slots__ = [
            "year",
            "month",
            "day",
            "weekday",
            "hour",
            "minute",
            "second",
            "microsecond",
            "tzname",
            "tzoffset",
        ]

    def _parse(self, timestr, dayfirst=None, yearfirst=None, fuzzy=False):
        """
        Perform the parse utility operation under explicit compatibility rules.

        Example:
            Exercise parser. parse through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param timestr: Value supplied for timestr under the utility contract.
        :param dayfirst: Value supplied for dayfirst under the utility contract.
        :param yearfirst: Value supplied for yearfirst under the utility contract.
        :param fuzzy: Value supplied for fuzzy under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        info = self.info
        if dayfirst is None:
            dayfirst = info.dayfirst
        if yearfirst is None:
            yearfirst = info.yearfirst
        res = self._result()
        l = _timelex.split(timestr)
        try:

            # year/month/day list
            ymd = []

            # Index of the month string in ymd
            mstridx = -1

            len_l = len(l)
            i = 0
            while i < len_l:

                # Check if it's a number
                try:
                    value_repr = l[i]
                    value = float(value_repr)
                except ValueError:
                    value = None

                if value is not None:
                    # Token is a number
                    len_li = len(l[i])
                    i += 1
                    if len(ymd) == 3 and len_li in (2, 4) and (i >= len_l or (l[i] != ":" and info.hms(l[i]) is None)):
                        # 19990101T23[59]
                        s = l[i - 1]
                        res.hour = int(s[:2])
                        if len_li == 4:
                            res.minute = int(s[2:])
                    elif len_li == 6 or (len_li > 6 and l[i - 1].find(".") == 6):
                        # YYMMDD or HHMMSS[.ss]
                        s = l[i - 1]
                        if not ymd and l[i - 1].find(".") == -1:
                            ymd.append(info.convertyear(int(s[:2])))
                            ymd.append(int(s[2:4]))
                            ymd.append(int(s[4:]))
                        else:
                            # 19990101T235959[.59]
                            res.hour = int(s[:2])
                            res.minute = int(s[2:4])
                            res.second, res.microsecond = _parsems(s[4:])
                    elif len_li == 8:
                        # YYYYMMDD
                        s = l[i - 1]
                        ymd.append(int(s[:4]))
                        ymd.append(int(s[4:6]))
                        ymd.append(int(s[6:]))
                    elif len_li in (12, 14):
                        # YYYYMMDDhhmm[ss]
                        s = l[i - 1]
                        ymd.append(int(s[:4]))
                        ymd.append(int(s[4:6]))
                        ymd.append(int(s[6:8]))
                        res.hour = int(s[8:10])
                        res.minute = int(s[10:12])
                        if len_li == 14:
                            res.second = int(s[12:])
                    elif (i < len_l and info.hms(l[i]) is not None) or (
                        i + 1 < len_l and l[i] == " " and info.hms(l[i + 1]) is not None
                    ):
                        # HH[ ]h or MM[ ]m or SS[.ss][ ]s
                        if l[i] == " ":
                            i += 1
                        idx = info.hms(l[i])
                        while True:
                            if idx == 0:
                                res.hour = int(value)
                                if value % 1:
                                    res.minute = int(60 * (value % 1))
                            elif idx == 1:
                                res.minute = int(value)
                                if value % 1:
                                    res.second = int(60 * (value % 1))
                            elif idx == 2:
                                res.second, res.microsecond = _parsems(value_repr)
                            i += 1
                            if i >= len_l or idx == 2:
                                break
                            # 12h00
                            try:
                                value_repr = l[i]
                                value = float(value_repr)
                            except ValueError:
                                break
                            else:
                                i += 1
                                idx += 1
                                if i < len_l:
                                    newidx = info.hms(l[i])
                                    if newidx is not None:
                                        idx = newidx
                    elif i + 1 < len_l and l[i] == ":":
                        # HH:MM[:SS[.ss]]
                        res.hour = int(value)
                        i += 1
                        value = float(l[i])
                        res.minute = int(value)
                        if value % 1:
                            res.second = int(60 * (value % 1))
                        i += 1
                        if i < len_l and l[i] == ":":
                            res.second, res.microsecond = _parsems(l[i + 1])
                            i += 2
                    elif i < len_l and l[i] in ("-", "/", "."):
                        sep = l[i]
                        ymd.append(int(value))
                        i += 1
                        if i < len_l and not info.jump(l[i]):
                            try:
                                # 01-01[-01]
                                ymd.append(int(l[i]))
                            except ValueError:
                                # 01-Jan[-01]
                                value = info.month(l[i])
                                if value is not None:
                                    ymd.append(value)
                                    assert mstridx == -1
                                    mstridx = len(ymd) - 1
                                else:
                                    return None
                            i += 1
                            if i < len_l and l[i] == sep:
                                # We have three members
                                i += 1
                                value = info.month(l[i])
                                if value is not None:
                                    ymd.append(value)
                                    mstridx = len(ymd) - 1
                                    assert mstridx == -1
                                else:
                                    ymd.append(int(l[i]))
                                i += 1
                    elif i >= len_l or info.jump(l[i]):
                        if i + 1 < len_l and info.ampm(l[i + 1]) is not None:
                            # 12 am
                            res.hour = int(value)
                            if res.hour < 12 and info.ampm(l[i + 1]) == 1:
                                res.hour += 12
                            elif res.hour == 12 and info.ampm(l[i + 1]) == 0:
                                res.hour = 0
                            i += 1
                        else:
                            # Year, month or day
                            ymd.append(int(value))
                        i += 1
                    elif info.ampm(l[i]) is not None:
                        # 12am
                        res.hour = int(value)
                        if res.hour < 12 and info.ampm(l[i]) == 1:
                            res.hour += 12
                        elif res.hour == 12 and info.ampm(l[i]) == 0:
                            res.hour = 0
                        i += 1
                    elif not fuzzy:
                        return None
                    else:
                        i += 1
                    continue

                # Check weekday
                value = info.weekday(l[i])
                if value is not None:
                    res.weekday = value
                    i += 1
                    continue

                # Check month name
                value = info.month(l[i])
                if value is not None:
                    ymd.append(value)
                    assert mstridx == -1
                    mstridx = len(ymd) - 1
                    i += 1
                    if i < len_l:
                        if l[i] in ("-", "/"):
                            # Jan-01[-99]
                            sep = l[i]
                            i += 1
                            ymd.append(int(l[i]))
                            i += 1
                            if i < len_l and l[i] == sep:
                                # Jan-01-99
                                i += 1
                                ymd.append(int(l[i]))
                                i += 1
                        elif i + 3 < len_l and l[i] == l[i + 2] == " " and info.pertain(l[i + 1]):
                            # Jan of 01
                            # In this case, 01 is clearly year
                            try:
                                value = int(l[i + 3])
                            except ValueError:
                                # Wrong guess
                                pass
                            else:
                                # Convert it here to become unambiguous
                                ymd.append(info.convertyear(value))
                            i += 4
                    continue

                # Check am/pm
                value = info.ampm(l[i])
                if value is not None:
                    if value == 1 and res.hour < 12:
                        res.hour += 12
                    elif value == 0 and res.hour == 12:
                        res.hour = 0
                    i += 1
                    continue

                # Check for a timezone name
                if (
                    res.hour is not None
                    and len(l[i]) <= 5
                    and res.tzname is None
                    and res.tzoffset is None
                    and not [x for x in l[i] if x not in string.ascii_uppercase]
                ):
                    res.tzname = l[i]
                    res.tzoffset = info.tzoffset(res.tzname)
                    i += 1

                    # Check for something like GMT+3, or BRST+3. Notice
                    # that it doesn't mean "I am 3 hours after GMT", but
                    # "my time +3 is GMT". If found, we reverse the
                    # logic so that timezone parsing code will get it
                    # right.
                    if i < len_l and l[i] in ("+", "-"):
                        l[i] = ("+", "-")[l[i] == "+"]
                        res.tzoffset = None
                        if info.utczone(res.tzname):
                            # With something like GMT+3, the timezone
                            # is *not* GMT.
                            res.tzname = None

                    continue

                # Check for a numbered timezone
                if res.hour is not None and l[i] in ("+", "-"):
                    signal = (-1, 1)[l[i] == "+"]
                    i += 1
                    len_li = len(l[i])
                    if len_li == 4:
                        # -0300
                        res.tzoffset = int(l[i][:2]) * 3600 + int(l[i][2:]) * 60
                    elif i + 1 < len_l and l[i + 1] == ":":
                        # -03:00
                        res.tzoffset = int(l[i]) * 3600 + int(l[i + 2]) * 60
                        i += 2
                    elif len_li <= 2:
                        # -[0]3
                        res.tzoffset = int(l[i][:2]) * 3600
                    else:
                        return None
                    i += 1
                    res.tzoffset *= signal

                    # Look for a timezone name between parenthesis
                    if (
                        i + 3 < len_l
                        and info.jump(l[i])
                        and l[i + 1] == "("
                        and l[i + 3] == ")"
                        and 3 <= len(l[i + 2]) <= 5
                        and not [x for x in l[i + 2] if x not in string.ascii_uppercase]
                    ):
                        # -0300 (BRST)
                        res.tzname = l[i + 2]
                        i += 4
                    continue

                # Check jumps
                if not (info.jump(l[i]) or fuzzy):
                    return None

                i += 1

            # Process year/month/day
            len_ymd = len(ymd)
            if len_ymd > 3:
                # More than three members!?
                return None
            elif len_ymd == 1 or (mstridx != -1 and len_ymd == 2):
                # One member, or two members with a month string
                if mstridx != -1:
                    res.month = ymd[mstridx]
                    del ymd[mstridx]
                if len_ymd > 1 or mstridx == -1:
                    if ymd[0] > 31:
                        res.year = ymd[0]
                    else:
                        res.day = ymd[0]
            elif len_ymd == 2:
                # Two members with numbers
                if ymd[0] > 31:
                    # 99-01
                    res.year, res.month = ymd
                elif ymd[1] > 31:
                    # 01-99
                    res.month, res.year = ymd
                elif dayfirst and ymd[1] <= 12:
                    # 13-01
                    res.day, res.month = ymd
                else:
                    # 01-13
                    res.month, res.day = ymd
            if len_ymd == 3:
                # Three members
                if mstridx == 0:
                    res.month, res.day, res.year = ymd
                elif mstridx == 1:
                    if ymd[0] > 31 or (yearfirst and ymd[2] <= 31):
                        # 99-Jan-01
                        res.year, res.month, res.day = ymd
                    else:
                        # 01-Jan-01
                        # Give precendence to day-first, since
                        # two-digit years is usually hand-written.
                        res.day, res.month, res.year = ymd
                elif mstridx == 2:
                    # WTF!?
                    if ymd[1] > 31:
                        # 01-99-Jan
                        res.day, res.year, res.month = ymd
                    else:
                        # 99-01-Jan
                        res.year, res.day, res.month = ymd
                else:
                    if ymd[0] > 31 or (yearfirst and ymd[1] <= 12 and ymd[2] <= 31):
                        # 99-01-01
                        res.year, res.month, res.day = ymd
                    elif ymd[0] > 12 or (dayfirst and ymd[1] <= 12):
                        # 13-01-01
                        res.day, res.month, res.year = ymd
                    else:
                        # 01-13-01
                        res.month, res.day, res.year = ymd

        except (IndexError, ValueError, AssertionError):
            return None

        if not info.validate(res):
            return None
        return res


DEFAULTPARSER = parser()


def parse(timestr, parserinfo=None, **kwargs):
    """
    Parse the supplied date text and return its normalized datetime value.

    Example:
        Exercise parse through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


    :param timestr: Value supplied for timestr under the utility contract.
    :param parserinfo: Value supplied for parserinfo under the utility contract.
    :param kwargs: Keyword values forwarded to the compatibility implementation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if parserinfo:
        return parser(parserinfo).parse(timestr, **kwargs)
    else:
        return DEFAULTPARSER.parse(timestr, **kwargs)


class _tzparser(object):
    """
    Provide the tzparser utility contract with explicit state and cleanup behavior.

    Example:
        Exercise  tzparser through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py
    """
    class _result(_resultbase):

        """
        Provide the result utility contract with explicit state and cleanup behavior.

        Example:
            Exercise  tzparser. result through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py
        """
        __slots__ = ["stdabbr", "stdoffset", "dstabbr", "dstoffset", "start", "end"]

        class _attr(_resultbase):
            """
            Provide the attr utility contract with explicit state and cleanup behavior.

            Example:
                Exercise  tzparser. result. attr through a consuming regression::

                    python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py
            """
            __slots__ = ["month", "week", "weekday", "yday", "jyday", "day", "time"]

        def __repr__(self):
            """
            Perform the repr utility operation under explicit compatibility rules.

            Example:
                Exercise  tzparser. result.  repr   through a consuming regression::

                    python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return self._repr("")

        def __init__(self):
            """
            Initialize and validate the result state.

            Example:
                Exercise  tzparser. result.  init   through a consuming regression::

                    python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


            :return: None; validated state is stored on the receiving object.
            """
            _resultbase.__init__(self)
            self.start = self._attr()
            self.end = self._attr()

    def parse(self, tzstr):
        """
        Parse the supplied date text and return its normalized datetime value.

        Example:
            Exercise  tzparser.parse through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param tzstr: Value supplied for tzstr under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        res = self._result()
        l = _timelex.split(tzstr)
        try:

            len_l = len(l)

            i = 0
            while i < len_l:
                # BRST+3[BRDT[+2]]
                j = i
                while j < len_l and not [x for x in l[j] if x in "0123456789:,-+"]:
                    j += 1
                if j != i:
                    if not res.stdabbr:
                        offattr = "stdoffset"
                        res.stdabbr = "".join(l[i:j])
                    else:
                        offattr = "dstoffset"
                        res.dstabbr = "".join(l[i:j])
                    i = j
                    if i < len_l and (l[i] in ("+", "-") or l[i][0] in "0123456789"):
                        if l[i] in ("+", "-"):
                            # Yes, that's right.  See the TZ variable
                            # documentation.
                            signal = (1, -1)[l[i] == "+"]
                            i += 1
                        else:
                            signal = -1
                        len_li = len(l[i])
                        if len_li == 4:
                            # -0300
                            setattr(
                                res,
                                offattr,
                                (int(l[i][:2]) * 3600 + int(l[i][2:]) * 60) * signal,
                            )
                        elif i + 1 < len_l and l[i + 1] == ":":
                            # -03:00
                            setattr(
                                res,
                                offattr,
                                (int(l[i]) * 3600 + int(l[i + 2]) * 60) * signal,
                            )
                            i += 2
                        elif len_li <= 2:
                            # -[0]3
                            setattr(res, offattr, int(l[i][:2]) * 3600 * signal)
                        else:
                            return None
                        i += 1
                    if res.dstabbr:
                        break
                else:
                    break

            if i < len_l:
                for j in range(i, len_l):
                    if l[j] == ";":
                        l[j] = ","

                assert l[i] == ","

                i += 1

            if i >= len_l:
                pass
            elif 8 <= l.count(",") <= 9 and not [y for x in l[i:] if x != "," for y in x if y not in "0123456789"]:
                # GMT0BST,3,0,30,3600,10,0,26,7200[,3600]
                for x in (res.start, res.end):
                    x.month = int(l[i])
                    i += 2
                    if l[i] == "-":
                        value = int(l[i + 1]) * -1
                        i += 1
                    else:
                        value = int(l[i])
                    i += 2
                    if value:
                        x.week = value
                        x.weekday = (int(l[i]) - 1) % 7
                    else:
                        x.day = int(l[i])
                    i += 2
                    x.time = int(l[i])
                    i += 2
                if i < len_l:
                    if l[i] in ("-", "+"):
                        signal = (-1, 1)[l[i] == "+"]
                        i += 1
                    else:
                        signal = 1
                    res.dstoffset = (res.stdoffset + int(l[i])) * signal
            elif (
                l.count(",") == 2
                and l[i:].count("/") <= 2
                and not [
                    y for x in l[i:] if x not in (",", "/", "J", "M", ".", "-", ":") for y in x if y not in "0123456789"
                ]
            ):
                for x in (res.start, res.end):
                    if l[i] == "J":
                        # non-leap year day (1 based)
                        i += 1
                        x.jyday = int(l[i])
                    elif l[i] == "M":
                        # month[-.]week[-.]weekday
                        i += 1
                        x.month = int(l[i])
                        i += 1
                        assert l[i] in ("-", ".")
                        i += 1
                        x.week = int(l[i])
                        if x.week == 5:
                            x.week = -1
                        i += 1
                        assert l[i] in ("-", ".")
                        i += 1
                        x.weekday = (int(l[i]) - 1) % 7
                    else:
                        # year day (zero based)
                        x.yday = int(l[i]) + 1

                    i += 1

                    if i < len_l and l[i] == "/":
                        i += 1
                        # start time
                        len_li = len(l[i])
                        if len_li == 4:
                            # -0300
                            x.time = int(l[i][:2]) * 3600 + int(l[i][2:]) * 60
                        elif i + 1 < len_l and l[i + 1] == ":":
                            # -03:00
                            x.time = int(l[i]) * 3600 + int(l[i + 2]) * 60
                            i += 2
                            if i + 1 < len_l and l[i + 1] == ":":
                                i += 2
                                x.time += int(l[i])
                        elif len_li <= 2:
                            # -[0]3
                            x.time = int(l[i][:2]) * 3600
                        else:
                            return None
                        i += 1

                    assert i == len_l or l[i] == ","

                    i += 1

                assert i >= len_l

        except (IndexError, ValueError, AssertionError):
            return None

        return res


DEFAULTTZPARSER = _tzparser()


def _parsetz(tzstr):
    """
    Perform the parsetz utility operation under explicit compatibility rules.

    Example:
        Exercise  parsetz through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


    :param tzstr: Value supplied for tzstr under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return DEFAULTTZPARSER.parse(tzstr)


def _parsems(value):
    """
    Parse a I[.F] seconds value into (seconds, microseconds).

    Example:
        Exercise  parsems through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


    :param value: Value normalized, stored, formatted or returned.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if "." not in value:
        return int(value), 0
    else:
        i, f = value.split(".")
        return int(i), int(f.ljust(6, "0")[:6])


# vim:ts=4:sw=4:et

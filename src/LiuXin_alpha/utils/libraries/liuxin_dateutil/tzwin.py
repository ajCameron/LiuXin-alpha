# This code was originally contributed by Jeffrey Harris.
"""
Read Windows timezone definitions through the retained dateutil interface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise tzwin through a consuming regression::

        python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py
"""
import datetime
import struct
import _winreg

__author__ = "Jeffrey Harris & Gustavo Niemeyer <gustavo@niemeyer.net>"

__all__ = ["tzwin", "tzwinlocal"]

ONEWEEK = datetime.timedelta(7)

TZKEYNAMENT = r"SOFTWARE\Microsoft\Windows NT\CurrentVersion\Time Zones"
TZKEYNAME9X = r"SOFTWARE\Microsoft\Windows\CurrentVersion\Time Zones"
TZLOCALKEYNAME = r"SYSTEM\CurrentControlSet\Control\TimeZoneInformation"


def _settzkeyname():
    """
    Perform the settzkeyname utility operation under explicit compatibility rules.

    Example:
        Exercise  settzkeyname through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    global TZKEYNAME
    handle = _winreg.ConnectRegistry(None, _winreg.HKEY_LOCAL_MACHINE)
    try:
        _winreg.OpenKey(handle, TZKEYNAMENT).Close()
        TZKEYNAME = TZKEYNAMENT
    except WindowsError:
        TZKEYNAME = TZKEYNAME9X
    handle.Close()


_settzkeyname()


class tzwinbase(datetime.tzinfo):
    """
    tzinfo class based on win32's timezones available in the registry.

    Example:
        Exercise tzwinbase through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py
    """

    def utcoffset(self, dt):
        """
        Perform the utcoffset utility operation under explicit compatibility rules.

        Example:
            Exercise tzwinbase.utcoffset through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param dt: Date or datetime value parsed, normalized or rendered.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self._isdst(dt):
            return datetime.timedelta(minutes=self._dstoffset)
        else:
            return datetime.timedelta(minutes=self._stdoffset)

    def dst(self, dt):
        """
        Perform the dst utility operation under explicit compatibility rules.

        Example:
            Exercise tzwinbase.dst through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param dt: Date or datetime value parsed, normalized or rendered.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self._isdst(dt):
            minutes = self._dstoffset - self._stdoffset
            return datetime.timedelta(minutes=minutes)
        else:
            return datetime.timedelta(0)

    def tzname(self, dt):
        """
        Perform the tzname utility operation under explicit compatibility rules.

        Example:
            Exercise tzwinbase.tzname through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param dt: Date or datetime value parsed, normalized or rendered.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self._isdst(dt):
            return self._dstname
        else:
            return self._stdname

    def list():
        """
        Return a list of all time zones known to the system.

        Example:
            Exercise tzwinbase.list through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        handle = _winreg.ConnectRegistry(None, _winreg.HKEY_LOCAL_MACHINE)
        tzkey = _winreg.OpenKey(handle, TZKEYNAME)
        result = [_winreg.EnumKey(tzkey, i) for i in range(_winreg.QueryInfoKey(tzkey)[0])]
        tzkey.Close()
        handle.Close()
        return result

    list = staticmethod(list)

    def display(self):
        """
        Perform the display utility operation under explicit compatibility rules.

        Example:
            Exercise tzwinbase.display through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self._display

    def _isdst(self, dt):
        """
        Perform the isdst utility operation under explicit compatibility rules.

        Example:
            Exercise tzwinbase. isdst through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param dt: Date or datetime value parsed, normalized or rendered.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        dston = picknthweekday(
            dt.year,
            self._dstmonth,
            self._dstdayofweek,
            self._dsthour,
            self._dstminute,
            self._dstweeknumber,
        )
        dstoff = picknthweekday(
            dt.year,
            self._stdmonth,
            self._stddayofweek,
            self._stdhour,
            self._stdminute,
            self._stdweeknumber,
        )
        if dston < dstoff:
            return dston <= dt.replace(tzinfo=None) < dstoff
        else:
            return not dstoff <= dt.replace(tzinfo=None) < dston


class tzwin(tzwinbase):
    """
    Provide the tzwin utility contract with explicit state and cleanup behavior.

    Example:
        Exercise tzwin through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py
    """
    def __init__(self, name):
        """
        Initialize and validate the tzwin state.

        Example:
            Exercise tzwin.  init   through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param name: Field, file, function or resource name addressed by the operation.
        :return: None; validated state is stored on the receiving object.
        """
        self._name = name

        handle = _winreg.ConnectRegistry(None, _winreg.HKEY_LOCAL_MACHINE)
        tzkey = _winreg.OpenKey(handle, r"%s\%s" % (TZKEYNAME, name))
        keydict = valuestodict(tzkey)
        tzkey.Close()
        handle.Close()

        self._stdname = keydict["Std"].encode("iso-8859-1")
        self._dstname = keydict["Dlt"].encode("iso-8859-1")

        self._display = keydict["Display"]

        # See http://ww_winreg.jsiinc.com/SUBA/tip0300/rh0398.htm
        tup = struct.unpack("=3l16h", keydict["TZI"])
        self._stdoffset = -tup[0] - tup[1]  # Bias + StandardBias * -1
        self._dstoffset = self._stdoffset - tup[2]  # + DaylightBias * -1

        (
            self._stdmonth,
            self._stddayofweek,  # Sunday = 0
            self._stdweeknumber,  # Last = 5
            self._stdhour,
            self._stdminute,
        ) = tup[4:9]

        (
            self._dstmonth,
            self._dstdayofweek,  # Sunday = 0
            self._dstweeknumber,  # Last = 5
            self._dsthour,
            self._dstminute,
        ) = tup[12:17]

    def __repr__(self):
        """
        Perform the repr utility operation under explicit compatibility rules.

        Example:
            Exercise tzwin.  repr   through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return "tzwin(%s)" % repr(self._name)

    def __reduce__(self):
        """
        Perform the reduce utility operation under explicit compatibility rules.

        Example:
            Exercise tzwin.  reduce   through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return (self.__class__, (self._name,))


class tzwinlocal(tzwinbase):
    """
    Provide the tzwinlocal utility contract with explicit state and cleanup behavior.

    Example:
        Exercise tzwinlocal through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py
    """
    def __init__(self):

        """
        Initialize and validate the tzwinlocal state.

        Example:
            Exercise tzwinlocal.  init   through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; validated state is stored on the receiving object.
        """
        handle = _winreg.ConnectRegistry(None, _winreg.HKEY_LOCAL_MACHINE)

        tzlocalkey = _winreg.OpenKey(handle, TZLOCALKEYNAME)
        keydict = valuestodict(tzlocalkey)
        tzlocalkey.Close()

        self._stdname = keydict["StandardName"].encode("iso-8859-1")
        self._dstname = keydict["DaylightName"].encode("iso-8859-1")

        try:
            tzkey = _winreg.OpenKey(handle, r"%s\%s" % (TZKEYNAME, self._stdname))
            _keydict = valuestodict(tzkey)
            self._display = _keydict["Display"]
            tzkey.Close()
        except OSError:
            self._display = None

        handle.Close()

        self._stdoffset = -keydict["Bias"] - keydict["StandardBias"]
        self._dstoffset = self._stdoffset - keydict["DaylightBias"]

        # See http://ww_winreg.jsiinc.com/SUBA/tip0300/rh0398.htm
        tup = struct.unpack("=8h", keydict["StandardStart"])

        (
            self._stdmonth,
            self._stddayofweek,  # Sunday = 0
            self._stdweeknumber,  # Last = 5
            self._stdhour,
            self._stdminute,
        ) = tup[1:6]

        tup = struct.unpack("=8h", keydict["DaylightStart"])

        (
            self._dstmonth,
            self._dstdayofweek,  # Sunday = 0
            self._dstweeknumber,  # Last = 5
            self._dsthour,
            self._dstminute,
        ) = tup[1:6]

    def __reduce__(self):
        """
        Perform the reduce utility operation under explicit compatibility rules.

        Example:
            Exercise tzwinlocal.  reduce   through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return (self.__class__, ())


def picknthweekday(year, month, dayofweek, hour, minute, whichweek):
    """
    dayofweek == 0 means Sunday, whichweek 5 means last instance

    Example:
        Exercise picknthweekday through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


    :param year: Value supplied for year under the utility contract.
    :param month: Value supplied for month under the utility contract.
    :param dayofweek: Value supplied for dayofweek under the utility contract.
    :param hour: Value supplied for hour under the utility contract.
    :param minute: Value supplied for minute under the utility contract.
    :param whichweek: Value supplied for whichweek under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    first = datetime.datetime(year, month, 1, hour, minute)
    weekdayone = first.replace(day=((dayofweek - first.isoweekday()) % 7 + 1))
    for n in xrange(whichweek):
        dt = weekdayone + (whichweek - n) * ONEWEEK
        if dt.month == month:
            return dt


def valuestodict(key):
    """
    Convert a registry key's values to a dictionary.

    Example:
        Exercise valuestodict through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


    :param key: Metadata, identifier or local-variable key.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    dict = {}
    size = _winreg.QueryInfoKey(key)[1]
    for i in range(size):
        data = _winreg.EnumValue(key, i)
        dict[data[0]] = data[1]
    return dict

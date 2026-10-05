"""
Generate recurring datetime sequences from RFC-style recurrence rules.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise rrule through a consuming regression::

        python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py
"""
__author__ = "Gustavo Niemeyer <gustavo@niemeyer.net>"
__license__ = "PSF License"

import itertools
import datetime
import calendar
import _thread
import sys

from past.builtins import cmp

__all__ = [
    "rrule",
    "rruleset",
    "rrulestr",
    "YEARLY",
    "MONTHLY",
    "WEEKLY",
    "DAILY",
    "HOURLY",
    "MINUTELY",
    "SECONDLY",
    "MO",
    "TU",
    "WE",
    "TH",
    "FR",
    "SA",
    "SU",
]

# Every mask is 7 days longer to handle cross-year weekly periods.
M366MASK = tuple(
    [1] * 31
    + [2] * 29
    + [3] * 31
    + [4] * 30
    + [5] * 31
    + [6] * 30
    + [7] * 31
    + [8] * 31
    + [9] * 30
    + [10] * 31
    + [11] * 30
    + [12] * 31
    + [1] * 7
)
M365MASK = list(M366MASK)
M29, M30, M31 = list(range(1, 30)), list(range(1, 31)), list(range(1, 32))
MDAY366MASK = tuple(M31 + M29 + M31 + M30 + M31 + M30 + M31 + M31 + M30 + M31 + M30 + M31 + M31[:7])
MDAY365MASK = list(MDAY366MASK)
M29, M30, M31 = list(range(-29, 0)), list(range(-30, 0)), list(range(-31, 0))
NMDAY366MASK = tuple(M31 + M29 + M31 + M30 + M31 + M30 + M31 + M31 + M30 + M31 + M30 + M31 + M31[:7])
NMDAY365MASK = list(NMDAY366MASK)
M366RANGE = (0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366)
M365RANGE = (0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365)
WDAYMASK = [0, 1, 2, 3, 4, 5, 6] * 55
del M29, M30, M31, M365MASK[59], MDAY365MASK[59], NMDAY365MASK[31]
MDAY365MASK = tuple(MDAY365MASK)
M365MASK = tuple(M365MASK)

(YEARLY, MONTHLY, WEEKLY, DAILY, HOURLY, MINUTELY, SECONDLY) = list(range(7))

# Imported on demand.
easter = None
parser = None


class weekday(object):
    """
    Provide the weekday utility contract with explicit state and cleanup behavior.

    Example:
        Exercise weekday through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py
    """
    __slots__ = ["weekday", "n"]

    def __init__(self, weekday, n=None):
        """
        Initialize and validate the weekday state.

        Example:
            Exercise weekday.  init   through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param weekday: Value supplied for weekday under the utility contract.
        :param n: Value supplied for n under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        if n == 0:
            raise ValueError("Can't create weekday with n == 0")
        self.weekday = weekday
        self.n = n

    def __call__(self, n):
        """
        Perform the call utility operation under explicit compatibility rules.

        Example:
            Exercise weekday.  call   through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param n: Value supplied for n under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if n == self.n:
            return self
        else:
            return self.__class__(self.weekday, n)

    def __eq__(self, other):
        """
        Perform the eq utility operation under explicit compatibility rules.

        Example:
            Exercise weekday.  eq   through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param other: Value supplied for other under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            if self.weekday != other.weekday or self.n != other.n:
                return False
        except AttributeError:
            return False
        return True

    def __repr__(self):
        """
        Perform the repr utility operation under explicit compatibility rules.

        Example:
            Exercise weekday.  repr   through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        s = ("MO", "TU", "WE", "TH", "FR", "SA", "SU")[self.weekday]
        if not self.n:
            return s
        else:
            return "%s(%+d)" % (s, self.n)


MO, TU, WE, TH, FR, SA, SU = weekdays = tuple([weekday(x) for x in range(7)])


class rrulebase:
    """
    Provide the rrulebase utility contract with explicit state and cleanup behavior.

    Example:
        Exercise rrulebase through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py
    """
    def __init__(self, cache=False):
        """
        Initialize and validate the rrulebase state.

        Example:
            Exercise rrulebase.  init   through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param cache: Value supplied for cache under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        if cache:
            self._cache = []
            self._cache_lock = _thread.allocate_lock()
            self._cache_gen = self._iter()
            self._cache_complete = False
        else:
            self._cache = None
            self._cache_complete = False
        self._len = None

    def __iter__(self):
        """
        Expose iter behavior for the compatibility container.

        Example:
            Exercise rrulebase.  iter   through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self._cache_complete:
            return iter(self._cache)
        elif self._cache is None:
            return self._iter()
        else:
            return self._iter_cached()

    def _iter_cached(self):
        """
        Iterate cached under the documented compatibility and safety rules.

        Example:
            Exercise rrulebase. iter cached through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: An iterator yielding the normalized values described above.
        """
        i = 0
        gen = self._cache_gen
        cache = self._cache
        acquire = self._cache_lock.acquire
        release = self._cache_lock.release
        while gen:
            if i == len(cache):
                acquire()
                if self._cache_complete:
                    break
                try:
                    for j in range(10):
                        cache.append(next(gen))
                except StopIteration:
                    self._cache_gen = gen = None
                    self._cache_complete = True
                    break
                release()
            yield cache[i]
            i += 1
        while i < self._len:
            yield cache[i]
            i += 1

    def __getitem__(self, item):
        """
        Expose getitem behavior for the compatibility container.

        Example:
            Exercise rrulebase.  getitem   through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param item: Value supplied for item under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self._cache_complete:
            return self._cache[item]
        elif isinstance(item, slice):
            if item.step and item.step < 0:
                return list(iter(self))[item]
            else:
                return list(itertools.islice(self, item.start or 0, item.stop or sys.maxsize, item.step or 1))
        elif item >= 0:
            gen = iter(self)
            try:
                for i in range(item + 1):
                    res = next(gen)
            except StopIteration:
                raise IndexError
            return res
        else:
            return list(iter(self))[item]

    def __contains__(self, item):
        """
        Perform the contains utility operation under explicit compatibility rules.

        Example:
            Exercise rrulebase.  contains   through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param item: Value supplied for item under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self._cache_complete:
            return item in self._cache
        else:
            for i in self:
                if i == item:
                    return True
                elif i > item:
                    return False
        return False

    # __len__() introduces a large performance penality.
    def count(self):
        """
        Perform the count utility operation under explicit compatibility rules.

        Example:
            Exercise rrulebase.count through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self._len is None:
            for x in self:
                pass
        return self._len

    def before(self, dt, inc=False):
        """
        Perform the before utility operation under explicit compatibility rules.

        Example:
            Exercise rrulebase.before through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param dt: Date or datetime value parsed, normalized or rendered.
        :param inc: Value supplied for inc under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self._cache_complete:
            gen = self._cache
        else:
            gen = self
        last = None
        if inc:
            for i in gen:
                if i > dt:
                    break
                last = i
        else:
            for i in gen:
                if i >= dt:
                    break
                last = i
        return last

    def after(self, dt, inc=False):
        """
        Perform the after utility operation under explicit compatibility rules.

        Example:
            Exercise rrulebase.after through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param dt: Date or datetime value parsed, normalized or rendered.
        :param inc: Value supplied for inc under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self._cache_complete:
            gen = self._cache
        else:
            gen = self
        if inc:
            for i in gen:
                if i >= dt:
                    return i
        else:
            for i in gen:
                if i > dt:
                    return i
        return None

    def between(self, after, before, inc=False):
        """
        Perform the between utility operation under explicit compatibility rules.

        Example:
            Exercise rrulebase.between through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param after: Value supplied for after under the utility contract.
        :param before: Value supplied for before under the utility contract.
        :param inc: Value supplied for inc under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self._cache_complete:
            gen = self._cache
        else:
            gen = self
        started = False
        l = []
        if inc:
            for i in gen:
                if i > before:
                    break
                elif not started:
                    if i >= after:
                        started = True
                        l.append(i)
                else:
                    l.append(i)
        else:
            for i in gen:
                if i >= before:
                    break
                elif not started:
                    if i > after:
                        started = True
                        l.append(i)
                else:
                    l.append(i)
        return l


class rrule(rrulebase):
    """
    Provide the rrule utility contract with explicit state and cleanup behavior.

    Example:
        Exercise rrule through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py
    """
    def __init__(
        self,
        freq,
        dtstart=None,
        interval=1,
        wkst=None,
        count=None,
        until=None,
        bysetpos=None,
        bymonth=None,
        bymonthday=None,
        byyearday=None,
        byeaster=None,
        byweekno=None,
        byweekday=None,
        byhour=None,
        byminute=None,
        bysecond=None,
        cache=False,
    ):
        """
        Initialize and validate the rrule state.

        Example:
            Exercise rrule.  init   through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param freq: Value supplied for freq under the utility contract.
        :param dtstart: Value supplied for dtstart under the utility contract.
        :param interval: Value supplied for interval under the utility contract.
        :param wkst: Value supplied for wkst under the utility contract.
        :param count: Value supplied for count under the utility contract.
        :param until: Value supplied for until under the utility contract.
        :param bysetpos: Value supplied for bysetpos under the utility contract.
        :param bymonth: Value supplied for bymonth under the utility contract.
        :param bymonthday: Value supplied for bymonthday under the utility contract.
        :param byyearday: Value supplied for byyearday under the utility contract.
        :param byeaster: Value supplied for byeaster under the utility contract.
        :param byweekno: Value supplied for byweekno under the utility contract.
        :param byweekday: Value supplied for byweekday under the utility contract.
        :param byhour: Value supplied for byhour under the utility contract.
        :param byminute: Value supplied for byminute under the utility contract.
        :param bysecond: Value supplied for bysecond under the utility contract.
        :param cache: Value supplied for cache under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        rrulebase.__init__(self, cache)
        global easter
        if not dtstart:
            dtstart = datetime.datetime.now().replace(microsecond=0)
        elif not isinstance(dtstart, datetime.datetime):
            dtstart = datetime.datetime.fromordinal(dtstart.toordinal())
        else:
            dtstart = dtstart.replace(microsecond=0)
        self._dtstart = dtstart
        self._tzinfo = dtstart.tzinfo
        self._freq = freq
        self._interval = interval
        self._count = count
        if until and not isinstance(until, datetime.datetime):
            until = datetime.datetime.fromordinal(until.toordinal())
        self._until = until
        if wkst is None:
            self._wkst = calendar.firstweekday()
        elif type(wkst) is int:
            self._wkst = wkst
        else:
            self._wkst = wkst.weekday
        if bysetpos is None:
            self._bysetpos = None
        elif type(bysetpos) is int:
            if bysetpos == 0 or not (-366 <= bysetpos <= 366):
                raise ValueError("bysetpos must be between 1 and 366, " "or between -366 and -1")
            self._bysetpos = (bysetpos,)
        else:
            self._bysetpos = tuple(bysetpos)
            for pos in self._bysetpos:
                if pos == 0 or not (-366 <= pos <= 366):
                    raise ValueError("bysetpos must be between 1 and 366, " "or between -366 and -1")
        if not (byweekno or byyearday or bymonthday or byweekday is not None or byeaster is not None):
            if freq == YEARLY:
                if not bymonth:
                    bymonth = dtstart.month
                bymonthday = dtstart.day
            elif freq == MONTHLY:
                bymonthday = dtstart.day
            elif freq == WEEKLY:
                byweekday = dtstart.weekday()
        # bymonth
        if not bymonth:
            self._bymonth = None
        elif type(bymonth) is int:
            self._bymonth = (bymonth,)
        else:
            self._bymonth = tuple(bymonth)
        # byyearday
        if not byyearday:
            self._byyearday = None
        elif type(byyearday) is int:
            self._byyearday = (byyearday,)
        else:
            self._byyearday = tuple(byyearday)
        # byeaster
        if byeaster is not None:
            if not easter:
                from dateutil import easter
            if type(byeaster) is int:
                self._byeaster = (byeaster,)
            else:
                self._byeaster = tuple(byeaster)
        else:
            self._byeaster = None
        # bymonthay
        if not bymonthday:
            self._bymonthday = ()
            self._bynmonthday = ()
        elif type(bymonthday) is int:
            if bymonthday < 0:
                self._bynmonthday = (bymonthday,)
                self._bymonthday = ()
            else:
                self._bymonthday = (bymonthday,)
                self._bynmonthday = ()
        else:
            self._bymonthday = tuple([x for x in bymonthday if x > 0])
            self._bynmonthday = tuple([x for x in bymonthday if x < 0])
        # byweekno
        if byweekno is None:
            self._byweekno = None
        elif type(byweekno) is int:
            self._byweekno = (byweekno,)
        else:
            self._byweekno = tuple(byweekno)
        # byweekday / bynweekday
        if byweekday is None:
            self._byweekday = None
            self._bynweekday = None
        elif type(byweekday) is int:
            self._byweekday = (byweekday,)
            self._bynweekday = None
        elif hasattr(byweekday, "n"):
            if not byweekday.n or freq > MONTHLY:
                self._byweekday = (byweekday.weekday,)
                self._bynweekday = None
            else:
                self._bynweekday = ((byweekday.weekday, byweekday.n),)
                self._byweekday = None
        else:
            self._byweekday = []
            self._bynweekday = []
            for wday in byweekday:
                if type(wday) is int:
                    self._byweekday.append(wday)
                elif not wday.n or freq > MONTHLY:
                    self._byweekday.append(wday.weekday)
                else:
                    self._bynweekday.append((wday.weekday, wday.n))
            self._byweekday = tuple(self._byweekday)
            self._bynweekday = tuple(self._bynweekday)
            if not self._byweekday:
                self._byweekday = None
            elif not self._bynweekday:
                self._bynweekday = None
        # byhour
        if byhour is None:
            if freq < HOURLY:
                self._byhour = (dtstart.hour,)
            else:
                self._byhour = None
        elif type(byhour) is int:
            self._byhour = (byhour,)
        else:
            self._byhour = tuple(byhour)
        # byminute
        if byminute is None:
            if freq < MINUTELY:
                self._byminute = (dtstart.minute,)
            else:
                self._byminute = None
        elif type(byminute) is int:
            self._byminute = (byminute,)
        else:
            self._byminute = tuple(byminute)
        # bysecond
        if bysecond is None:
            if freq < SECONDLY:
                self._bysecond = (dtstart.second,)
            else:
                self._bysecond = None
        elif type(bysecond) is int:
            self._bysecond = (bysecond,)
        else:
            self._bysecond = tuple(bysecond)

        if self._freq >= HOURLY:
            self._timeset = None
        else:
            self._timeset = []
            for hour in self._byhour:
                for minute in self._byminute:
                    for second in self._bysecond:
                        self._timeset.append(datetime.time(hour, minute, second, tzinfo=self._tzinfo))
            self._timeset.sort()
            self._timeset = tuple(self._timeset)

    def _iter(self):
        """
        Perform the iter utility operation under explicit compatibility rules.

        Example:
            Exercise rrule. iter through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: An iterator yielding the normalized values described above.
        """
        (
            year,
            month,
            day,
            hour,
            minute,
            second,
            weekday,
            yearday,
            _,
        ) = self._dtstart.timetuple()

        # Some local variables to speed things up a bit
        freq = self._freq
        interval = self._interval
        wkst = self._wkst
        until = self._until
        bymonth = self._bymonth
        byweekno = self._byweekno
        byyearday = self._byyearday
        byweekday = self._byweekday
        byeaster = self._byeaster
        bymonthday = self._bymonthday
        bynmonthday = self._bynmonthday
        bysetpos = self._bysetpos
        byhour = self._byhour
        byminute = self._byminute
        bysecond = self._bysecond

        ii = _iterinfo(self)
        ii.rebuild(year, month)

        getdayset = {
            YEARLY: ii.ydayset,
            MONTHLY: ii.mdayset,
            WEEKLY: ii.wdayset,
            DAILY: ii.ddayset,
            HOURLY: ii.ddayset,
            MINUTELY: ii.ddayset,
            SECONDLY: ii.ddayset,
        }[freq]

        if freq < HOURLY:
            timeset = self._timeset
        else:
            gettimeset = {
                HOURLY: ii.htimeset,
                MINUTELY: ii.mtimeset,
                SECONDLY: ii.stimeset,
            }[freq]
            if (
                (freq >= HOURLY and self._byhour and hour not in self._byhour)
                or (freq >= MINUTELY and self._byminute and minute not in self._byminute)
                or (freq >= SECONDLY and self._bysecond and second not in self._bysecond)
            ):
                timeset = ()
            else:
                timeset = gettimeset(hour, minute, second)

        total = 0
        count = self._count
        while True:
            # Get dayset with the right frequency
            dayset, start, end = getdayset(year, month, day)

            # Do the "hard" work ;-)
            filtered = False
            for i in dayset[start:end]:
                if (
                    (bymonth and ii.mmask[i] not in bymonth)
                    or (byweekno and not ii.wnomask[i])
                    or (byweekday and ii.wdaymask[i] not in byweekday)
                    or (ii.nwdaymask and not ii.nwdaymask[i])
                    or (byeaster and not ii.eastermask[i])
                    or (
                        (bymonthday or bynmonthday)
                        and ii.mdaymask[i] not in bymonthday
                        and ii.nmdaymask[i] not in bynmonthday
                    )
                    or (
                        byyearday
                        and (
                            (i < ii.yearlen and i + 1 not in byyearday and -ii.yearlen + i not in byyearday)
                            or (
                                i >= ii.yearlen
                                and i + 1 - ii.yearlen not in byyearday
                                and -ii.nextyearlen + i - ii.yearlen not in byyearday
                            )
                        )
                    )
                ):
                    dayset[i] = None
                    filtered = True

            # Output results
            if bysetpos and timeset:
                poslist = []
                for pos in bysetpos:
                    if pos < 0:
                        daypos, timepos = divmod(pos, len(timeset))
                    else:
                        daypos, timepos = divmod(pos - 1, len(timeset))
                    try:
                        i = [x for x in dayset[start:end] if x is not None][daypos]
                        time = timeset[timepos]
                    except IndexError:
                        pass
                    else:
                        date = datetime.date.fromordinal(ii.yearordinal + i)
                        res = datetime.datetime.combine(date, time)
                        if res not in poslist:
                            poslist.append(res)
                poslist.sort()
                for res in poslist:
                    if until and res > until:
                        self._len = total
                        return
                    elif res >= self._dtstart:
                        total += 1
                        yield res
                        if count:
                            count -= 1
                            if not count:
                                self._len = total
                                return
            else:
                for i in dayset[start:end]:
                    if i is not None:
                        date = datetime.date.fromordinal(ii.yearordinal + i)
                        for time in timeset:
                            res = datetime.datetime.combine(date, time)
                            if until and res > until:
                                self._len = total
                                return
                            elif res >= self._dtstart:
                                total += 1
                                yield res
                                if count:
                                    count -= 1
                                    if not count:
                                        self._len = total
                                        return

            # Handle frequency and interval
            fixday = False
            if freq == YEARLY:
                year += interval
                if year > datetime.MAXYEAR:
                    self._len = total
                    return
                ii.rebuild(year, month)
            elif freq == MONTHLY:
                month += interval
                if month > 12:
                    div, mod = divmod(month, 12)
                    month = mod
                    year += div
                    if month == 0:
                        month = 12
                        year -= 1
                    if year > datetime.MAXYEAR:
                        self._len = total
                        return
                ii.rebuild(year, month)
            elif freq == WEEKLY:
                if wkst > weekday:
                    day += -(weekday + 1 + (6 - wkst)) + self._interval * 7
                else:
                    day += -(weekday - wkst) + self._interval * 7
                weekday = wkst
                fixday = True
            elif freq == DAILY:
                day += interval
                fixday = True
            elif freq == HOURLY:
                if filtered:
                    # Jump to one iteration before next day
                    hour += ((23 - hour) // interval) * interval
                while True:
                    hour += interval
                    div, mod = divmod(hour, 24)
                    if div:
                        hour = mod
                        day += div
                        fixday = True
                    if not byhour or hour in byhour:
                        break
                timeset = gettimeset(hour, minute, second)
            elif freq == MINUTELY:
                if filtered:
                    # Jump to one iteration before next day
                    minute += ((1439 - (hour * 60 + minute)) // interval) * interval
                while True:
                    minute += interval
                    div, mod = divmod(minute, 60)
                    if div:
                        minute = mod
                        hour += div
                        div, mod = divmod(hour, 24)
                        if div:
                            hour = mod
                            day += div
                            fixday = True
                            filtered = False
                    if (not byhour or hour in byhour) and (not byminute or minute in byminute):
                        break
                timeset = gettimeset(hour, minute, second)
            elif freq == SECONDLY:
                if filtered:
                    # Jump to one iteration before next day
                    second += ((86399 - (hour * 3600 + minute * 60 + second)) // interval) * interval
                while True:
                    second += self._interval
                    div, mod = divmod(second, 60)
                    if div:
                        second = mod
                        minute += div
                        div, mod = divmod(minute, 60)
                        if div:
                            minute = mod
                            hour += div
                            div, mod = divmod(hour, 24)
                            if div:
                                hour = mod
                                day += div
                                fixday = True
                    if (
                        (not byhour or hour in byhour)
                        and (not byminute or minute in byminute)
                        and (not bysecond or second in bysecond)
                    ):
                        break
                timeset = gettimeset(hour, minute, second)

            if fixday and day > 28:
                daysinmonth = calendar.monthrange(year, month)[1]
                if day > daysinmonth:
                    while day > daysinmonth:
                        day -= daysinmonth
                        month += 1
                        if month == 13:
                            month = 1
                            year += 1
                            if year > datetime.MAXYEAR:
                                self._len = total
                                return
                        daysinmonth = calendar.monthrange(year, month)[1]
                    ii.rebuild(year, month)


class _iterinfo(object):
    """
    Provide the iterinfo utility contract with explicit state and cleanup behavior.

    Example:
        Exercise  iterinfo through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py
    """
    __slots__ = [
        "rrule",
        "lastyear",
        "lastmonth",
        "yearlen",
        "nextyearlen",
        "yearordinal",
        "yearweekday",
        "mmask",
        "mrange",
        "mdaymask",
        "nmdaymask",
        "wdaymask",
        "wnomask",
        "nwdaymask",
        "eastermask",
    ]

    def __init__(self, rrule):
        """
        Initialize and validate the iterinfo state.

        Example:
            Exercise  iterinfo.  init   through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param rrule: Value supplied for rrule under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        for attr in self.__slots__:
            setattr(self, attr, None)
        self.rrule = rrule

    def rebuild(self, year, month):
        # Every mask is 7 days longer to handle cross-year weekly periods.
        """
        Perform the rebuild utility operation under explicit compatibility rules.

        Example:
            Exercise  iterinfo.rebuild through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param year: Value supplied for year under the utility contract.
        :param month: Value supplied for month under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        rr = self.rrule
        if year != self.lastyear:
            self.yearlen = 365 + calendar.isleap(year)
            self.nextyearlen = 365 + calendar.isleap(year + 1)
            firstyday = datetime.date(year, 1, 1)
            self.yearordinal = firstyday.toordinal()
            self.yearweekday = firstyday.weekday()

            wday = datetime.date(year, 1, 1).weekday()
            if self.yearlen == 365:
                self.mmask = M365MASK
                self.mdaymask = MDAY365MASK
                self.nmdaymask = NMDAY365MASK
                self.wdaymask = WDAYMASK[wday:]
                self.mrange = M365RANGE
            else:
                self.mmask = M366MASK
                self.mdaymask = MDAY366MASK
                self.nmdaymask = NMDAY366MASK
                self.wdaymask = WDAYMASK[wday:]
                self.mrange = M366RANGE

            if not rr._byweekno:
                self.wnomask = None
            else:
                self.wnomask = [0] * (self.yearlen + 7)
                # no1wkst = firstwkst = self.wdaymask.index(rr._wkst)
                no1wkst = firstwkst = (7 - self.yearweekday + rr._wkst) % 7
                if no1wkst >= 4:
                    no1wkst = 0
                    # Number of days in the year, plus the days we got
                    # from last year.
                    wyearlen = self.yearlen + (self.yearweekday - rr._wkst) % 7
                else:
                    # Number of days in the year, minus the days we
                    # left in last year.
                    wyearlen = self.yearlen - no1wkst
                div, mod = divmod(wyearlen, 7)
                numweeks = div + mod // 4
                for n in rr._byweekno:
                    if n < 0:
                        n += numweeks + 1
                    if not (0 < n <= numweeks):
                        continue
                    if n > 1:
                        i = no1wkst + (n - 1) * 7
                        if no1wkst != firstwkst:
                            i -= 7 - firstwkst
                    else:
                        i = no1wkst
                    for j in range(7):
                        self.wnomask[i] = 1
                        i += 1
                        if self.wdaymask[i] == rr._wkst:
                            break
                if 1 in rr._byweekno:
                    # Check week number 1 of next year as well
                    # TODO: Check -numweeks for next year.
                    i = no1wkst + numweeks * 7
                    if no1wkst != firstwkst:
                        i -= 7 - firstwkst
                    if i < self.yearlen:
                        # If week starts in next year, we
                        # don't care about it.
                        for j in range(7):
                            self.wnomask[i] = 1
                            i += 1
                            if self.wdaymask[i] == rr._wkst:
                                break
                if no1wkst:
                    # Check last week number of last year as
                    # well. If no1wkst is 0, either the year
                    # started on week start, or week number 1
                    # got days from last year, so there are no
                    # days from last year's last week number in
                    # this year.
                    if -1 not in rr._byweekno:
                        lyearweekday = datetime.date(year - 1, 1, 1).weekday()
                        lno1wkst = (7 - lyearweekday + rr._wkst) % 7
                        lyearlen = 365 + calendar.isleap(year - 1)
                        if lno1wkst >= 4:
                            lno1wkst = 0
                            lnumweeks = 52 + (lyearlen + (lyearweekday - rr._wkst) % 7) % 7 // 4
                        else:
                            lnumweeks = 52 + (self.yearlen - no1wkst) % 7 // 4
                    else:
                        lnumweeks = -1
                    if lnumweeks in rr._byweekno:
                        for i in range(no1wkst):
                            self.wnomask[i] = 1

        if rr._bynweekday and (month != self.lastmonth or year != self.lastyear):
            ranges = []
            if rr._freq == YEARLY:
                if rr._bymonth:
                    for month in rr._bymonth:
                        ranges.append(self.mrange[month - 1 : month + 1])
                else:
                    ranges = [(0, self.yearlen)]
            elif rr._freq == MONTHLY:
                ranges = [self.mrange[month - 1 : month + 1]]
            if ranges:
                # Weekly frequency won't get here, so we may not
                # care about cross-year weekly periods.
                self.nwdaymask = [0] * self.yearlen
                for first, last in ranges:
                    last -= 1
                    for wday, n in rr._bynweekday:
                        if n < 0:
                            i = last + (n + 1) * 7
                            i -= (self.wdaymask[i] - wday) % 7
                        else:
                            i = first + (n - 1) * 7
                            i += (7 - self.wdaymask[i] + wday) % 7
                        if first <= i <= last:
                            self.nwdaymask[i] = 1

        if rr._byeaster:
            self.eastermask = [0] * (self.yearlen + 7)
            eyday = easter.easter(year).toordinal() - self.yearordinal
            for offset in rr._byeaster:
                self.eastermask[eyday + offset] = 1

        self.lastyear = year
        self.lastmonth = month

    def ydayset(self, year, month, day):
        """
        Perform the ydayset utility operation under explicit compatibility rules.

        Example:
            Exercise  iterinfo.ydayset through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param year: Value supplied for year under the utility contract.
        :param month: Value supplied for month under the utility contract.
        :param day: Value supplied for day under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return list(range(self.yearlen)), 0, self.yearlen

    def mdayset(self, year, month, day):
        """
        Perform the mdayset utility operation under explicit compatibility rules.

        Example:
            Exercise  iterinfo.mdayset through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param year: Value supplied for year under the utility contract.
        :param month: Value supplied for month under the utility contract.
        :param day: Value supplied for day under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        set = [None] * self.yearlen
        start, end = self.mrange[month - 1 : month + 1]
        for i in range(start, end):
            set[i] = i
        return set, start, end

    def wdayset(self, year, month, day):
        # We need to handle cross-year weeks here.
        """
        Perform the wdayset utility operation under explicit compatibility rules.

        Example:
            Exercise  iterinfo.wdayset through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param year: Value supplied for year under the utility contract.
        :param month: Value supplied for month under the utility contract.
        :param day: Value supplied for day under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        set = [None] * (self.yearlen + 7)
        i = datetime.date(year, month, day).toordinal() - self.yearordinal
        start = i
        for j in range(7):
            set[i] = i
            i += 1
            # if (not (0 <= i < self.yearlen) or
            #    self.wdaymask[i] == self.rrule._wkst):
            # This will cross the year boundary, if necessary.
            if self.wdaymask[i] == self.rrule._wkst:
                break
        return set, start, i

    def ddayset(self, year, month, day):
        """
        Perform the ddayset utility operation under explicit compatibility rules.

        Example:
            Exercise  iterinfo.ddayset through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param year: Value supplied for year under the utility contract.
        :param month: Value supplied for month under the utility contract.
        :param day: Value supplied for day under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        set = [None] * self.yearlen
        i = datetime.date(year, month, day).toordinal() - self.yearordinal
        set[i] = i
        return set, i, i + 1

    def htimeset(self, hour, minute, second):
        """
        Perform the htimeset utility operation under explicit compatibility rules.

        Example:
            Exercise  iterinfo.htimeset through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param hour: Value supplied for hour under the utility contract.
        :param minute: Value supplied for minute under the utility contract.
        :param second: Value supplied for second under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        set = []
        rr = self.rrule
        for minute in rr._byminute:
            for second in rr._bysecond:
                set.append(datetime.time(hour, minute, second, tzinfo=rr._tzinfo))
        set.sort()
        return set

    def mtimeset(self, hour, minute, second):
        """
        Perform the mtimeset utility operation under explicit compatibility rules.

        Example:
            Exercise  iterinfo.mtimeset through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param hour: Value supplied for hour under the utility contract.
        :param minute: Value supplied for minute under the utility contract.
        :param second: Value supplied for second under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        set = []
        rr = self.rrule
        for second in rr._bysecond:
            set.append(datetime.time(hour, minute, second, tzinfo=rr._tzinfo))
        set.sort()
        return set

    def stimeset(self, hour, minute, second):
        """
        Perform the stimeset utility operation under explicit compatibility rules.

        Example:
            Exercise  iterinfo.stimeset through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param hour: Value supplied for hour under the utility contract.
        :param minute: Value supplied for minute under the utility contract.
        :param second: Value supplied for second under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return (datetime.time(hour, minute, second, tzinfo=self.rrule._tzinfo),)


class rruleset(rrulebase):
    """
    Provide the rruleset utility contract with explicit state and cleanup behavior.

    Example:
        Exercise rruleset through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py
    """
    class _genitem:
        """
        Provide the genitem utility contract with explicit state and cleanup behavior.

        Example:
            Exercise rruleset. genitem through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py
        """
        def __init__(self, genlist, gen):
            """
            Initialize and validate the genitem state.

            Example:
                Exercise rruleset. genitem.  init   through a consuming regression::

                    python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


            :param genlist: Value supplied for genlist under the utility contract.
            :param gen: Value supplied for gen under the utility contract.
            :return: None; validated state is stored on the receiving object.
            """
            try:
                self.dt = gen()
                genlist.append(self)
            except StopIteration:
                pass
            self.genlist = genlist
            self.gen = gen

        def __next__(self):
            """
            Perform the next utility operation under explicit compatibility rules.

            Example:
                Exercise rruleset. genitem.  next   through a consuming regression::

                    python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            try:
                self.dt = self.gen()
            except StopIteration:
                self.genlist.remove(self)

        def __cmp__(self, other):
            """
            Perform the cmp utility operation under explicit compatibility rules.

            Example:
                Exercise rruleset. genitem.  cmp   through a consuming regression::

                    python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


            :param other: Value supplied for other under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return cmp(self.dt, other.dt)

    def __init__(self, cache=False):
        """
        Initialize and validate the rruleset state.

        Example:
            Exercise rruleset.  init   through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param cache: Value supplied for cache under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        rrulebase.__init__(self, cache)
        self._rrule = []
        self._rdate = []
        self._exrule = []
        self._exdate = []

    def rrule(self, rrule):
        """
        Perform the rrule utility operation under explicit compatibility rules.

        Example:
            Exercise rruleset.rrule through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param rrule: Value supplied for rrule under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._rrule.append(rrule)

    def rdate(self, rdate):
        """
        Perform the rdate utility operation under explicit compatibility rules.

        Example:
            Exercise rruleset.rdate through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param rdate: Value supplied for rdate under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._rdate.append(rdate)

    def exrule(self, exrule):
        """
        Perform the exrule utility operation under explicit compatibility rules.

        Example:
            Exercise rruleset.exrule through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param exrule: Value supplied for exrule under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._exrule.append(exrule)

    def exdate(self, exdate):
        """
        Perform the exdate utility operation under explicit compatibility rules.

        Example:
            Exercise rruleset.exdate through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param exdate: Value supplied for exdate under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._exdate.append(exdate)

    def _iter(self):
        """
        Perform the iter utility operation under explicit compatibility rules.

        Example:
            Exercise rruleset. iter through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: An iterator yielding the normalized values described above.
        """
        rlist = []
        self._rdate.sort()
        self._genitem(rlist, iter(self._rdate).__next__)
        for gen in [iter(x).__next__ for x in self._rrule]:
            self._genitem(rlist, gen)
        rlist.sort()
        exlist = []
        self._exdate.sort()
        self._genitem(exlist, iter(self._exdate).__next__)
        for gen in [iter(x).__next__ for x in self._exrule]:
            self._genitem(exlist, gen)
        exlist.sort()
        lastdt = None
        total = 0
        while rlist:
            ritem = rlist[0]
            if not lastdt or lastdt != ritem.dt:
                while exlist and exlist[0] < ritem:
                    next(exlist[0])
                    exlist.sort()
                if not exlist or ritem != exlist[0]:
                    total += 1
                    yield ritem.dt
                lastdt = ritem.dt
            next(ritem)
            rlist.sort()
        self._len = total


class _rrulestr:

    """
    Provide the rrulestr utility contract with explicit state and cleanup behavior.

    Example:
        Exercise  rrulestr through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py
    """
    _freq_map = {
        "YEARLY": YEARLY,
        "MONTHLY": MONTHLY,
        "WEEKLY": WEEKLY,
        "DAILY": DAILY,
        "HOURLY": HOURLY,
        "MINUTELY": MINUTELY,
        "SECONDLY": SECONDLY,
    }

    _weekday_map = {"MO": 0, "TU": 1, "WE": 2, "TH": 3, "FR": 4, "SA": 5, "SU": 6}

    def _handle_int(self, rrkwargs, name, value, **kwargs):
        """
        Perform the handle int utility operation under explicit compatibility rules.

        Example:
            Exercise  rrulestr. handle int through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param rrkwargs: Value supplied for rrkwargs under the utility contract.
        :param name: Field, file, function or resource name addressed by the operation.
        :param value: Value normalized, stored, formatted or returned.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        rrkwargs[name.lower()] = int(value)

    def _handle_int_list(self, rrkwargs, name, value, **kwargs):
        """
        Perform the handle int list utility operation under explicit compatibility rules.

        Example:
            Exercise  rrulestr. handle int list through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param rrkwargs: Value supplied for rrkwargs under the utility contract.
        :param name: Field, file, function or resource name addressed by the operation.
        :param value: Value normalized, stored, formatted or returned.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        rrkwargs[name.lower()] = [int(x) for x in value.split(",")]

    _handle_INTERVAL = _handle_int
    _handle_COUNT = _handle_int
    _handle_BYSETPOS = _handle_int_list
    _handle_BYMONTH = _handle_int_list
    _handle_BYMONTHDAY = _handle_int_list
    _handle_BYYEARDAY = _handle_int_list
    _handle_BYEASTER = _handle_int_list
    _handle_BYWEEKNO = _handle_int_list
    _handle_BYHOUR = _handle_int_list
    _handle_BYMINUTE = _handle_int_list
    _handle_BYSECOND = _handle_int_list

    def _handle_FREQ(self, rrkwargs, name, value, **kwargs):
        """
        Perform the handle FREQ utility operation under explicit compatibility rules.

        Example:
            Exercise  rrulestr. handle FREQ through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param rrkwargs: Value supplied for rrkwargs under the utility contract.
        :param name: Field, file, function or resource name addressed by the operation.
        :param value: Value normalized, stored, formatted or returned.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        rrkwargs["freq"] = self._freq_map[value]

    def _handle_UNTIL(self, rrkwargs, name, value, **kwargs):
        """
        Perform the handle UNTIL utility operation under explicit compatibility rules.

        Example:
            Exercise  rrulestr. handle UNTIL through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param rrkwargs: Value supplied for rrkwargs under the utility contract.
        :param name: Field, file, function or resource name addressed by the operation.
        :param value: Value normalized, stored, formatted or returned.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        global parser
        if not parser:
            from dateutil import parser
        try:
            rrkwargs["until"] = parser.parse(value, ignoretz=kwargs.get("ignoretz"), tzinfos=kwargs.get("tzinfos"))
        except ValueError:
            raise ValueError("invalid until date")

    def _handle_WKST(self, rrkwargs, name, value, **kwargs):
        """
        Perform the handle WKST utility operation under explicit compatibility rules.

        Example:
            Exercise  rrulestr. handle WKST through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param rrkwargs: Value supplied for rrkwargs under the utility contract.
        :param name: Field, file, function or resource name addressed by the operation.
        :param value: Value normalized, stored, formatted or returned.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        rrkwargs["wkst"] = self._weekday_map[value]

    def _handle_BYWEEKDAY(self, rrkwargs, name, value, **kwarsg):
        """
        Perform the handle BYWEEKDAY utility operation under explicit compatibility rules.

        Example:
            Exercise  rrulestr. handle BYWEEKDAY through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param rrkwargs: Value supplied for rrkwargs under the utility contract.
        :param name: Field, file, function or resource name addressed by the operation.
        :param value: Value normalized, stored, formatted or returned.
        :param kwarsg: Value supplied for kwarsg under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        l = []
        for wday in value.split(","):
            for i in range(len(wday)):
                if wday[i] not in "+-0123456789":
                    break
            n = wday[:i] or None
            w = wday[i:]
            if n:
                n = int(n)
            l.append(weekdays[self._weekday_map[w]](n))
        rrkwargs["byweekday"] = l

    _handle_BYDAY = _handle_BYWEEKDAY

    def _parse_rfc_rrule(self, line, dtstart=None, cache=False, ignoretz=False, tzinfos=None):
        """
        Parse rfc rrule under the documented compatibility and safety rules.

        Example:
            Exercise  rrulestr. parse rfc rrule through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param line: Value supplied for line under the utility contract.
        :param dtstart: Value supplied for dtstart under the utility contract.
        :param cache: Value supplied for cache under the utility contract.
        :param ignoretz: Value supplied for ignoretz under the utility contract.
        :param tzinfos: Value supplied for tzinfos under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if line.find(":") != -1:
            name, value = line.split(":")
            if name != "RRULE":
                raise ValueError("unknown parameter name")
        else:
            value = line
        rrkwargs = {}
        for pair in value.split(";"):
            name, value = pair.split("=")
            name = name.upper()
            value = value.upper()
            try:
                getattr(self, "_handle_" + name)(rrkwargs, name, value, ignoretz=ignoretz, tzinfos=tzinfos)
            except AttributeError:
                raise ValueError("unknown parameter '%s'" % name)
            except (KeyError, ValueError):
                raise ValueError("invalid '%s': %s" % (name, value))
        return rrule(dtstart=dtstart, cache=cache, **rrkwargs)

    def _parse_rfc(
        self,
        s,
        dtstart=None,
        cache=False,
        unfold=False,
        forceset=False,
        compatible=False,
        ignoretz=False,
        tzinfos=None,
    ):
        """
        Parse rfc under the documented compatibility and safety rules.

        Example:
            Exercise  rrulestr. parse rfc through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param s: Value supplied for s under the utility contract.
        :param dtstart: Value supplied for dtstart under the utility contract.
        :param cache: Value supplied for cache under the utility contract.
        :param unfold: Value supplied for unfold under the utility contract.
        :param forceset: Value supplied for forceset under the utility contract.
        :param compatible: Value supplied for compatible under the utility contract.
        :param ignoretz: Value supplied for ignoretz under the utility contract.
        :param tzinfos: Value supplied for tzinfos under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        global parser
        if compatible:
            forceset = True
            unfold = True
        s = s.upper()
        if not s.strip():
            raise ValueError("empty string")
        if unfold:
            lines = s.splitlines()
            i = 0
            while i < len(lines):
                line = lines[i].rstrip()
                if not line:
                    del lines[i]
                elif i > 0 and line[0] == " ":
                    lines[i - 1] += line[1:]
                    del lines[i]
                else:
                    i += 1
        else:
            lines = s.split()
        if not forceset and len(lines) == 1 and (s.find(":") == -1 or s.startswith("RRULE:")):
            return self._parse_rfc_rrule(
                lines[0],
                cache=cache,
                dtstart=dtstart,
                ignoretz=ignoretz,
                tzinfos=tzinfos,
            )
        else:
            rrulevals = []
            rdatevals = []
            exrulevals = []
            exdatevals = []
            for line in lines:
                if not line:
                    continue
                if line.find(":") == -1:
                    name = "RRULE"
                    value = line
                else:
                    name, value = line.split(":", 1)
                parms = name.split(";")
                if not parms:
                    raise ValueError("empty property name")
                name = parms[0]
                parms = parms[1:]
                if name == "RRULE":
                    for parm in parms:
                        raise ValueError("unsupported RRULE parm: " + parm)
                    rrulevals.append(value)
                elif name == "RDATE":
                    for parm in parms:
                        if parm != "VALUE=DATE-TIME":
                            raise ValueError("unsupported RDATE parm: " + parm)
                    rdatevals.append(value)
                elif name == "EXRULE":
                    for parm in parms:
                        raise ValueError("unsupported EXRULE parm: " + parm)
                    exrulevals.append(value)
                elif name == "EXDATE":
                    for parm in parms:
                        if parm != "VALUE=DATE-TIME":
                            raise ValueError("unsupported RDATE parm: " + parm)
                    exdatevals.append(value)
                elif name == "DTSTART":
                    for parm in parms:
                        raise ValueError("unsupported DTSTART parm: " + parm)
                    if not parser:
                        from dateutil import parser
                    dtstart = parser.parse(value, ignoretz=ignoretz, tzinfos=tzinfos)
                else:
                    raise ValueError("unsupported property: " + name)
            if forceset or len(rrulevals) > 1 or rdatevals or exrulevals or exdatevals:
                if not parser and (rdatevals or exdatevals):
                    from dateutil import parser
                set = rruleset(cache=cache)
                for value in rrulevals:
                    set.rrule(self._parse_rfc_rrule(value, dtstart=dtstart, ignoretz=ignoretz, tzinfos=tzinfos))
                for value in rdatevals:
                    for datestr in value.split(","):
                        set.rdate(parser.parse(datestr, ignoretz=ignoretz, tzinfos=tzinfos))
                for value in exrulevals:
                    set.exrule(self._parse_rfc_rrule(value, dtstart=dtstart, ignoretz=ignoretz, tzinfos=tzinfos))
                for value in exdatevals:
                    for datestr in value.split(","):
                        set.exdate(parser.parse(datestr, ignoretz=ignoretz, tzinfos=tzinfos))
                if compatible and dtstart:
                    set.rdate(dtstart)
                return set
            else:
                return self._parse_rfc_rrule(
                    rrulevals[0],
                    dtstart=dtstart,
                    cache=cache,
                    ignoretz=ignoretz,
                    tzinfos=tzinfos,
                )

    def __call__(self, s, **kwargs):
        """
        Perform the call utility operation under explicit compatibility rules.

        Example:
            Exercise  rrulestr.  call   through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param s: Value supplied for s under the utility contract.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self._parse_rfc(s, **kwargs)


rrulestr = _rrulestr()

# vim:ts=4:sw=4:et

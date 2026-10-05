"""
Represent and apply calendar-relative date and time deltas.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise relativedelta through a consuming regression::

        python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py
"""
__author__ = "Gustavo Niemeyer <gustavo@niemeyer.net>"
__license__ = "PSF License"

import datetime
import calendar

__all__ = ["relativedelta", "MO", "TU", "WE", "TH", "FR", "SA", "SU"]


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


class relativedelta:
    """
    The relativedelta type is based on the specification of the excelent work done by M.-A. Lemburg in his mx.DateTime extension. However, notice that this type does *NOT* implement the same algorithm as his work. Do *NOT* expect it to behave like mx.DateTime's counterpart.

    Example:
        Exercise relativedelta through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py
    """

    def __init__(
        self,
        dt1=None,
        dt2=None,
        years=0,
        months=0,
        days=0,
        leapdays=0,
        weeks=0,
        hours=0,
        minutes=0,
        seconds=0,
        microseconds=0,
        year=None,
        month=None,
        day=None,
        weekday=None,
        yearday=None,
        nlyearday=None,
        hour=None,
        minute=None,
        second=None,
        microsecond=None,
    ):
        """
        Initialize and validate the relativedelta state.

        Example:
            Exercise relativedelta.  init   through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param dt1: Value supplied for dt1 under the utility contract.
        :param dt2: Value supplied for dt2 under the utility contract.
        :param years: Value supplied for years under the utility contract.
        :param months: Value supplied for months under the utility contract.
        :param days: Value supplied for days under the utility contract.
        :param leapdays: Value supplied for leapdays under the utility contract.
        :param weeks: Value supplied for weeks under the utility contract.
        :param hours: Value supplied for hours under the utility contract.
        :param minutes: Value supplied for minutes under the utility contract.
        :param seconds: Value supplied for seconds under the utility contract.
        :param microseconds: Value supplied for microseconds under the utility contract.
        :param year: Value supplied for year under the utility contract.
        :param month: Value supplied for month under the utility contract.
        :param day: Value supplied for day under the utility contract.
        :param weekday: Value supplied for weekday under the utility contract.
        :param yearday: Value supplied for yearday under the utility contract.
        :param nlyearday: Value supplied for nlyearday under the utility contract.
        :param hour: Value supplied for hour under the utility contract.
        :param minute: Value supplied for minute under the utility contract.
        :param second: Value supplied for second under the utility contract.
        :param microsecond: Value supplied for microsecond under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        if dt1 and dt2:
            if not isinstance(dt1, datetime.date) or not isinstance(dt2, datetime.date):
                raise TypeError("relativedelta only diffs datetime/date")
            if type(dt1) is not type(dt2):
                if not isinstance(dt1, datetime.datetime):
                    dt1 = datetime.datetime.fromordinal(dt1.toordinal())
                elif not isinstance(dt2, datetime.datetime):
                    dt2 = datetime.datetime.fromordinal(dt2.toordinal())
            self.years = 0
            self.months = 0
            self.days = 0
            self.leapdays = 0
            self.hours = 0
            self.minutes = 0
            self.seconds = 0
            self.microseconds = 0
            self.year = None
            self.month = None
            self.day = None
            self.weekday = None
            self.hour = None
            self.minute = None
            self.second = None
            self.microsecond = None
            self._has_time = 0

            months = (dt1.year * 12 + dt1.month) - (dt2.year * 12 + dt2.month)
            self._set_months(months)
            dtm = self.__radd__(dt2)
            if dt1 < dt2:
                while dt1 > dtm:
                    months += 1
                    self._set_months(months)
                    dtm = self.__radd__(dt2)
            else:
                while dt1 < dtm:
                    months -= 1
                    self._set_months(months)
                    dtm = self.__radd__(dt2)
            delta = dt1 - dtm
            self.seconds = delta.seconds + delta.days * 86400
            self.microseconds = delta.microseconds
        else:
            self.years = years
            self.months = months
            self.days = days + weeks * 7
            self.leapdays = leapdays
            self.hours = hours
            self.minutes = minutes
            self.seconds = seconds
            self.microseconds = microseconds
            self.year = year
            self.month = month
            self.day = day
            self.hour = hour
            self.minute = minute
            self.second = second
            self.microsecond = microsecond

            if type(weekday) is int:
                self.weekday = weekdays[weekday]
            else:
                self.weekday = weekday

            yday = 0
            if nlyearday:
                yday = nlyearday
            elif yearday:
                yday = yearday
                if yearday > 59:
                    self.leapdays = -1
            if yday:
                ydayidx = [31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 366]
                for idx, ydays in enumerate(ydayidx):
                    if yday <= ydays:
                        self.month = idx + 1
                        if idx == 0:
                            self.day = yday
                        else:
                            self.day = yday - ydayidx[idx - 1]
                        break
                else:
                    raise ValueError("invalid year day (%d)" % yday)

        self._fix()

    def _fix(self):
        """
        Perform the fix utility operation under explicit compatibility rules.

        Example:
            Exercise relativedelta. fix through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if abs(self.microseconds) > 999999:
            s = self.microseconds // abs(self.microseconds)
            div, mod = divmod(self.microseconds * s, 1000000)
            self.microseconds = mod * s
            self.seconds += div * s
        if abs(self.seconds) > 59:
            s = self.seconds // abs(self.seconds)
            div, mod = divmod(self.seconds * s, 60)
            self.seconds = mod * s
            self.minutes += div * s
        if abs(self.minutes) > 59:
            s = self.minutes // abs(self.minutes)
            div, mod = divmod(self.minutes * s, 60)
            self.minutes = mod * s
            self.hours += div * s
        if abs(self.hours) > 23:
            s = self.hours // abs(self.hours)
            div, mod = divmod(self.hours * s, 24)
            self.hours = mod * s
            self.days += div * s
        if abs(self.months) > 11:
            s = self.months // abs(self.months)
            div, mod = divmod(self.months * s, 12)
            self.months = mod * s
            self.years += div * s
        if (
            self.hours
            or self.minutes
            or self.seconds
            or self.microseconds
            or self.hour is not None
            or self.minute is not None
            or self.second is not None
            or self.microsecond is not None
        ):
            self._has_time = 1
        else:
            self._has_time = 0

    def _set_months(self, months):
        """
        Perform the set months utility operation under explicit compatibility rules.

        Example:
            Exercise relativedelta. set months through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param months: Value supplied for months under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.months = months
        if abs(self.months) > 11:
            s = self.months // abs(self.months)
            div, mod = divmod(self.months * s, 12)
            self.months = mod * s
            self.years = div * s
        else:
            self.years = 0

    def __radd__(self, other):
        """
        Perform the radd utility operation under explicit compatibility rules.

        Example:
            Exercise relativedelta.  radd   through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param other: Value supplied for other under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if not isinstance(other, datetime.date):
            raise TypeError("unsupported type for add operation")
        elif self._has_time and not isinstance(other, datetime.datetime):
            other = datetime.datetime.fromordinal(other.toordinal())
        year = (self.year or other.year) + self.years
        month = self.month or other.month
        if self.months:
            assert 1 <= abs(self.months) <= 12
            month += self.months
            if month > 12:
                year += 1
                month -= 12
            elif month < 1:
                year -= 1
                month += 12
        day = min(calendar.monthrange(year, month)[1], self.day or other.day)
        repl = {"year": year, "month": month, "day": day}
        for attr in ["hour", "minute", "second", "microsecond"]:
            value = getattr(self, attr)
            if value is not None:
                repl[attr] = value
        days = self.days
        if self.leapdays and month > 2 and calendar.isleap(year):
            days += self.leapdays
        ret = other.replace(**repl) + datetime.timedelta(
            days=days, hours=self.hours, minutes=self.minutes, seconds=self.seconds, microseconds=self.microseconds
        )
        if self.weekday:
            weekday, nth = self.weekday.weekday, self.weekday.n or 1
            jumpdays = (abs(nth) - 1) * 7
            if nth > 0:
                jumpdays += (7 - ret.weekday() + weekday) % 7
            else:
                jumpdays += (ret.weekday() - weekday) % 7
                jumpdays *= -1
            ret += datetime.timedelta(days=jumpdays)
        return ret

    def __rsub__(self, other):
        """
        Perform the rsub utility operation under explicit compatibility rules.

        Example:
            Exercise relativedelta.  rsub   through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param other: Value supplied for other under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.__neg__().__radd__(other)

    def __add__(self, other):
        """
        Perform the add utility operation under explicit compatibility rules.

        Example:
            Exercise relativedelta.  add   through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param other: Value supplied for other under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if not isinstance(other, relativedelta):
            raise TypeError("unsupported type for add operation")
        return relativedelta(
            years=other.years + self.years,
            months=other.months + self.months,
            days=other.days + self.days,
            hours=other.hours + self.hours,
            minutes=other.minutes + self.minutes,
            seconds=other.seconds + self.seconds,
            microseconds=other.microseconds + self.microseconds,
            leapdays=other.leapdays or self.leapdays,
            year=other.year or self.year,
            month=other.month or self.month,
            day=other.day or self.day,
            weekday=other.weekday or self.weekday,
            hour=other.hour or self.hour,
            minute=other.minute or self.minute,
            second=other.second or self.second,
            microsecond=other.second or self.microsecond,
        )

    def __sub__(self, other):
        """
        Perform the sub utility operation under explicit compatibility rules.

        Example:
            Exercise relativedelta.  sub   through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param other: Value supplied for other under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if not isinstance(other, relativedelta):
            raise TypeError("unsupported type for sub operation")
        return relativedelta(
            years=other.years - self.years,
            months=other.months - self.months,
            days=other.days - self.days,
            hours=other.hours - self.hours,
            minutes=other.minutes - self.minutes,
            seconds=other.seconds - self.seconds,
            microseconds=other.microseconds - self.microseconds,
            leapdays=other.leapdays or self.leapdays,
            year=other.year or self.year,
            month=other.month or self.month,
            day=other.day or self.day,
            weekday=other.weekday or self.weekday,
            hour=other.hour or self.hour,
            minute=other.minute or self.minute,
            second=other.second or self.second,
            microsecond=other.second or self.microsecond,
        )

    def __neg__(self):
        """
        Perform the neg utility operation under explicit compatibility rules.

        Example:
            Exercise relativedelta.  neg   through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return relativedelta(
            years=-self.years,
            months=-self.months,
            days=-self.days,
            hours=-self.hours,
            minutes=-self.minutes,
            seconds=-self.seconds,
            microseconds=-self.microseconds,
            leapdays=self.leapdays,
            year=self.year,
            month=self.month,
            day=self.day,
            weekday=self.weekday,
            hour=self.hour,
            minute=self.minute,
            second=self.second,
            microsecond=self.microsecond,
        )

    def __bool__(self):
        """
        Expose bool behavior for the compatibility container.

        Example:
            Exercise relativedelta.  bool   through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: True when the documented condition holds; otherwise False.
        """
        return not (
            not self.years
            and not self.months
            and not self.days
            and not self.hours
            and not self.minutes
            and not self.seconds
            and not self.microseconds
            and not self.leapdays
            and self.year is None
            and self.month is None
            and self.day is None
            and self.weekday is None
            and self.hour is None
            and self.minute is None
            and self.second is None
            and self.microsecond is None
        )

    def __mul__(self, other):
        """
        Perform the mul utility operation under explicit compatibility rules.

        Example:
            Exercise relativedelta.  mul   through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param other: Value supplied for other under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        f = float(other)
        return relativedelta(
            years=self.years * f,
            months=self.months * f,
            days=self.days * f,
            hours=self.hours * f,
            minutes=self.minutes * f,
            seconds=self.seconds * f,
            microseconds=self.microseconds * f,
            leapdays=self.leapdays,
            year=self.year,
            month=self.month,
            day=self.day,
            weekday=self.weekday,
            hour=self.hour,
            minute=self.minute,
            second=self.second,
            microsecond=self.microsecond,
        )

    def __eq__(self, other):
        """
        Perform the eq utility operation under explicit compatibility rules.

        Example:
            Exercise relativedelta.  eq   through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param other: Value supplied for other under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if not isinstance(other, relativedelta):
            return False
        if self.weekday or other.weekday:
            if not self.weekday or not other.weekday:
                return False
            if self.weekday.weekday != other.weekday.weekday:
                return False
            n1, n2 = self.weekday.n, other.weekday.n
            if n1 != n2 and not ((not n1 or n1 == 1) and (not n2 or n2 == 1)):
                return False
        return (
            self.years == other.years
            and self.months == other.months
            and self.days == other.days
            and self.hours == other.hours
            and self.minutes == other.minutes
            and self.seconds == other.seconds
            and self.leapdays == other.leapdays
            and self.year == other.year
            and self.month == other.month
            and self.day == other.day
            and self.hour == other.hour
            and self.minute == other.minute
            and self.second == other.second
            and self.microsecond == other.microsecond
        )

    def __ne__(self, other):
        """
        Perform the ne utility operation under explicit compatibility rules.

        Example:
            Exercise relativedelta.  ne   through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param other: Value supplied for other under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return not self.__eq__(other)

    def __div__(self, other):
        """
        Perform the div utility operation under explicit compatibility rules.

        Example:
            Exercise relativedelta.  div   through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :param other: Value supplied for other under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.__mul__(1 / float(other))

    def __repr__(self):
        """
        Perform the repr utility operation under explicit compatibility rules.

        Example:
            Exercise relativedelta.  repr   through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        l = []
        for attr in ["years", "months", "days", "leapdays", "hours", "minutes", "seconds", "microseconds"]:
            value = getattr(self, attr)
            if value:
                l.append("%s=%+d" % (attr, value))
        for attr in ["year", "month", "day", "weekday", "hour", "minute", "second", "microsecond"]:
            value = getattr(self, attr)
            if value is not None:
                l.append("%s=%s" % (attr, repr(value)))
        return "%s(%s)" % (self.__class__.__name__, ", ".join(l))


# vim:ts=4:sw=4:et

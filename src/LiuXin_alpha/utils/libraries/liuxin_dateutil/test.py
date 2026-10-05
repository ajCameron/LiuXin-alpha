#!/usr/bin/python
# -*- encoding: utf-8 -*-
"""
Exercise the bundled dateutil parser, delta, recurrence and timezone compatibility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test through a consuming regression::

        python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py
"""
from cStringIO import StringIO
import unittest
import calendar
import time
import base64
import os

# Add build directory to search path
if os.path.exists("build"):
    from distutils.util import get_platform
    import sys

    s = "build/lib.%s-%.3s" % (get_platform(), sys.version)
    s = os.path.join(os.getcwd(), s)
    sys.path.insert(0, s)

from dateutil.relativedelta import *
from dateutil.parser import *
from dateutil.easter import *
from dateutil.rrule import *
from dateutil.tz import *
from dateutil import zoneinfo

from datetime import *


class RelativeDeltaTest(unittest.TestCase):
    """
    Provide the RelativeDeltaTest utility contract with explicit state and cleanup behavior.

    Example:
        Exercise RelativeDeltaTest through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py
    """
    now = datetime(2003, 9, 17, 20, 54, 47, 282310)
    today = date(2003, 9, 17)

    def testNextMonth(self):
        """
        Perform the testNextMonth utility operation under explicit compatibility rules.

        Example:
            Exercise RelativeDeltaTest.testNextMonth through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            self.now + relativedelta(months=+1),
            datetime(2003, 10, 17, 20, 54, 47, 282310),
        )

    def testNextMonthPlusOneWeek(self):
        """
        Perform the testNextMonthPlusOneWeek utility operation under explicit compatibility rules.

        Example:
            Exercise RelativeDeltaTest.testNextMonthPlusOneWeek through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            self.now + relativedelta(months=+1, weeks=+1),
            datetime(2003, 10, 24, 20, 54, 47, 282310),
        )

    def testNextMonthPlusOneWeek10am(self):
        """
        Perform the testNextMonthPlusOneWeek10am utility operation under explicit compatibility rules.

        Example:
            Exercise RelativeDeltaTest.testNextMonthPlusOneWeek10am through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            self.today + relativedelta(months=+1, weeks=+1, hour=10),
            datetime(2003, 10, 24, 10, 0),
        )

    def testNextMonthPlusOneWeek10amDiff(self):
        """
        Perform the testNextMonthPlusOneWeek10amDiff utility operation under explicit compatibility rules.

        Example:
            Exercise RelativeDeltaTest.testNextMonthPlusOneWeek10amDiff through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            relativedelta(datetime(2003, 10, 24, 10, 0), self.today),
            relativedelta(months=+1, days=+7, hours=+10),
        )

    def testOneMonthBeforeOneYear(self):
        """
        Perform the testOneMonthBeforeOneYear utility operation under explicit compatibility rules.

        Example:
            Exercise RelativeDeltaTest.testOneMonthBeforeOneYear through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            self.now + relativedelta(years=+1, months=-1),
            datetime(2004, 8, 17, 20, 54, 47, 282310),
        )

    def testMonthsOfDiffNumOfDays(self):
        """
        Perform the testMonthsOfDiffNumOfDays utility operation under explicit compatibility rules.

        Example:
            Exercise RelativeDeltaTest.testMonthsOfDiffNumOfDays through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(date(2003, 1, 27) + relativedelta(months=+1), date(2003, 2, 27))
        self.assertEqual(date(2003, 1, 31) + relativedelta(months=+1), date(2003, 2, 28))
        self.assertEqual(date(2003, 1, 31) + relativedelta(months=+2), date(2003, 3, 31))

    def testMonthsOfDiffNumOfDaysWithYears(self):
        """
        Perform the testMonthsOfDiffNumOfDaysWithYears utility operation under explicit compatibility rules.

        Example:
            Exercise RelativeDeltaTest.testMonthsOfDiffNumOfDaysWithYears through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(date(2000, 2, 28) + relativedelta(years=+1), date(2001, 2, 28))
        self.assertEqual(date(2000, 2, 29) + relativedelta(years=+1), date(2001, 2, 28))

        self.assertEqual(date(1999, 2, 28) + relativedelta(years=+1), date(2000, 2, 28))
        self.assertEqual(date(1999, 3, 1) + relativedelta(years=+1), date(2000, 3, 1))
        self.assertEqual(date(1999, 3, 1) + relativedelta(years=+1), date(2000, 3, 1))

        self.assertEqual(date(2001, 2, 28) + relativedelta(years=-1), date(2000, 2, 28))
        self.assertEqual(date(2001, 3, 1) + relativedelta(years=-1), date(2000, 3, 1))

    def testNextFriday(self):
        """
        Perform the testNextFriday utility operation under explicit compatibility rules.

        Example:
            Exercise RelativeDeltaTest.testNextFriday through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(self.today + relativedelta(weekday=FR), date(2003, 9, 19))

    def testNextFridayInt(self):
        """
        Perform the testNextFridayInt utility operation under explicit compatibility rules.

        Example:
            Exercise RelativeDeltaTest.testNextFridayInt through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(self.today + relativedelta(weekday=calendar.FRIDAY), date(2003, 9, 19))

    def testLastFridayInThisMonth(self):
        """
        Perform the testLastFridayInThisMonth utility operation under explicit compatibility rules.

        Example:
            Exercise RelativeDeltaTest.testLastFridayInThisMonth through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(self.today + relativedelta(day=31, weekday=FR(-1)), date(2003, 9, 26))

    def testNextWednesdayIsToday(self):
        """
        Perform the testNextWednesdayIsToday utility operation under explicit compatibility rules.

        Example:
            Exercise RelativeDeltaTest.testNextWednesdayIsToday through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(self.today + relativedelta(weekday=WE), date(2003, 9, 17))

    def testNextWenesdayNotToday(self):
        """
        Perform the testNextWenesdayNotToday utility operation under explicit compatibility rules.

        Example:
            Exercise RelativeDeltaTest.testNextWenesdayNotToday through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(self.today + relativedelta(days=+1, weekday=WE), date(2003, 9, 24))

    def test15thISOYearWeek(self):
        """
        Perform the test15thISOYearWeek utility operation under explicit compatibility rules.

        Example:
            Exercise RelativeDeltaTest.test15thISOYearWeek through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            date(2003, 1, 1) + relativedelta(day=4, weeks=+14, weekday=MO(-1)),
            date(2003, 4, 7),
        )

    def testMillenniumAge(self):
        """
        Perform the testMillenniumAge utility operation under explicit compatibility rules.

        Example:
            Exercise RelativeDeltaTest.testMillenniumAge through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            relativedelta(self.now, date(2001, 1, 1)),
            relativedelta(
                years=+2,
                months=+8,
                days=+16,
                hours=+20,
                minutes=+54,
                seconds=+47,
                microseconds=+282310,
            ),
        )

    def testJohnAge(self):
        """
        Perform the testJohnAge utility operation under explicit compatibility rules.

        Example:
            Exercise RelativeDeltaTest.testJohnAge through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            relativedelta(self.now, datetime(1978, 4, 5, 12, 0)),
            relativedelta(
                years=+25,
                months=+5,
                days=+12,
                hours=+8,
                minutes=+54,
                seconds=+47,
                microseconds=+282310,
            ),
        )

    def testJohnAgeWithDate(self):
        """
        Perform the testJohnAgeWithDate utility operation under explicit compatibility rules.

        Example:
            Exercise RelativeDeltaTest.testJohnAgeWithDate through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            relativedelta(self.today, datetime(1978, 4, 5, 12, 0)),
            relativedelta(years=+25, months=+5, days=+11, hours=+12),
        )

    def testYearDay(self):
        """
        Perform the testYearDay utility operation under explicit compatibility rules.

        Example:
            Exercise RelativeDeltaTest.testYearDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(date(2003, 1, 1) + relativedelta(yearday=260), date(2003, 9, 17))
        self.assertEqual(date(2002, 1, 1) + relativedelta(yearday=260), date(2002, 9, 17))
        self.assertEqual(date(2000, 1, 1) + relativedelta(yearday=260), date(2000, 9, 16))
        self.assertEqual(self.today + relativedelta(yearday=261), date(2003, 9, 18))

    def testYearDayBug(self):
        # Tests a problem reported by Adam Ryan.
        """
        Perform the testYearDayBug utility operation under explicit compatibility rules.

        Example:
            Exercise RelativeDeltaTest.testYearDayBug through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(date(2010, 1, 1) + relativedelta(yearday=15), date(2010, 1, 15))

    def testNonLeapYearDay(self):
        """
        Perform the testNonLeapYearDay utility operation under explicit compatibility rules.

        Example:
            Exercise RelativeDeltaTest.testNonLeapYearDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(date(2003, 1, 1) + relativedelta(nlyearday=260), date(2003, 9, 17))
        self.assertEqual(date(2002, 1, 1) + relativedelta(nlyearday=260), date(2002, 9, 17))
        self.assertEqual(date(2000, 1, 1) + relativedelta(nlyearday=260), date(2000, 9, 17))
        self.assertEqual(self.today + relativedelta(yearday=261), date(2003, 9, 18))


class RRuleTest(unittest.TestCase):
    """
    Provide the RRuleTest utility contract with explicit state and cleanup behavior.

    Example:
        Exercise RRuleTest through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py
    """
    def testYearly(self):
        """
        Perform the testYearly utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testYearly through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(YEARLY, count=3, dtstart=parse("19970902T090000"))),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1998, 9, 2, 9, 0),
                datetime(1999, 9, 2, 9, 0),
            ],
        )

    def testYearlyInterval(self):
        """
        Perform the testYearlyInterval utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testYearlyInterval through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(YEARLY, count=3, interval=2, dtstart=parse("19970902T090000"))),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1999, 9, 2, 9, 0),
                datetime(2001, 9, 2, 9, 0),
            ],
        )

    def testYearlyIntervalLarge(self):
        """
        Perform the testYearlyIntervalLarge utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testYearlyIntervalLarge through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(YEARLY, count=3, interval=100, dtstart=parse("19970902T090000"))),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(2097, 9, 2, 9, 0),
                datetime(2197, 9, 2, 9, 0),
            ],
        )

    def testYearlyByMonth(self):
        """
        Perform the testYearlyByMonth utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testYearlyByMonth through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(YEARLY, count=3, bymonth=(1, 3), dtstart=parse("19970902T090000"))),
            [
                datetime(1998, 1, 2, 9, 0),
                datetime(1998, 3, 2, 9, 0),
                datetime(1999, 1, 2, 9, 0),
            ],
        )

    def testYearlyByMonthDay(self):
        """
        Perform the testYearlyByMonthDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testYearlyByMonthDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(YEARLY, count=3, bymonthday=(1, 3), dtstart=parse("19970902T090000"))),
            [
                datetime(1997, 9, 3, 9, 0),
                datetime(1997, 10, 1, 9, 0),
                datetime(1997, 10, 3, 9, 0),
            ],
        )

    def testYearlyByMonthAndMonthDay(self):
        """
        Perform the testYearlyByMonthAndMonthDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testYearlyByMonthAndMonthDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    YEARLY,
                    count=3,
                    bymonth=(1, 3),
                    bymonthday=(5, 7),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 1, 5, 9, 0),
                datetime(1998, 1, 7, 9, 0),
                datetime(1998, 3, 5, 9, 0),
            ],
        )

    def testYearlyByWeekDay(self):
        """
        Perform the testYearlyByWeekDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testYearlyByWeekDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    YEARLY,
                    count=3,
                    byweekday=(TU, TH),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1997, 9, 4, 9, 0),
                datetime(1997, 9, 9, 9, 0),
            ],
        )

    def testYearlyByNWeekDay(self):
        """
        Perform the testYearlyByNWeekDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testYearlyByNWeekDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    YEARLY,
                    count=3,
                    byweekday=(TU(1), TH(-1)),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 12, 25, 9, 0),
                datetime(1998, 1, 6, 9, 0),
                datetime(1998, 12, 31, 9, 0),
            ],
        )

    def testYearlyByNWeekDayLarge(self):
        """
        Perform the testYearlyByNWeekDayLarge utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testYearlyByNWeekDayLarge through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    YEARLY,
                    count=3,
                    byweekday=(TU(3), TH(-3)),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 12, 11, 9, 0),
                datetime(1998, 1, 20, 9, 0),
                datetime(1998, 12, 17, 9, 0),
            ],
        )

    def testYearlyByMonthAndWeekDay(self):
        """
        Perform the testYearlyByMonthAndWeekDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testYearlyByMonthAndWeekDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    YEARLY,
                    count=3,
                    bymonth=(1, 3),
                    byweekday=(TU, TH),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 1, 1, 9, 0),
                datetime(1998, 1, 6, 9, 0),
                datetime(1998, 1, 8, 9, 0),
            ],
        )

    def testYearlyByMonthAndNWeekDay(self):
        """
        Perform the testYearlyByMonthAndNWeekDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testYearlyByMonthAndNWeekDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    YEARLY,
                    count=3,
                    bymonth=(1, 3),
                    byweekday=(TU(1), TH(-1)),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 1, 6, 9, 0),
                datetime(1998, 1, 29, 9, 0),
                datetime(1998, 3, 3, 9, 0),
            ],
        )

    def testYearlyByMonthAndNWeekDayLarge(self):
        # This is interesting because the TH(-3) ends up before
        # the TU(3).
        """
        Perform the testYearlyByMonthAndNWeekDayLarge utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testYearlyByMonthAndNWeekDayLarge through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    YEARLY,
                    count=3,
                    bymonth=(1, 3),
                    byweekday=(TU(3), TH(-3)),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 1, 15, 9, 0),
                datetime(1998, 1, 20, 9, 0),
                datetime(1998, 3, 12, 9, 0),
            ],
        )

    def testYearlyByMonthDayAndWeekDay(self):
        """
        Perform the testYearlyByMonthDayAndWeekDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testYearlyByMonthDayAndWeekDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    YEARLY,
                    count=3,
                    bymonthday=(1, 3),
                    byweekday=(TU, TH),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 1, 1, 9, 0),
                datetime(1998, 2, 3, 9, 0),
                datetime(1998, 3, 3, 9, 0),
            ],
        )

    def testYearlyByMonthAndMonthDayAndWeekDay(self):
        """
        Perform the testYearlyByMonthAndMonthDayAndWeekDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testYearlyByMonthAndMonthDayAndWeekDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    YEARLY,
                    count=3,
                    bymonth=(1, 3),
                    bymonthday=(1, 3),
                    byweekday=(TU, TH),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 1, 1, 9, 0),
                datetime(1998, 3, 3, 9, 0),
                datetime(2001, 3, 1, 9, 0),
            ],
        )

    def testYearlyByYearDay(self):
        """
        Perform the testYearlyByYearDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testYearlyByYearDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    YEARLY,
                    count=4,
                    byyearday=(1, 100, 200, 365),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 12, 31, 9, 0),
                datetime(1998, 1, 1, 9, 0),
                datetime(1998, 4, 10, 9, 0),
                datetime(1998, 7, 19, 9, 0),
            ],
        )

    def testYearlyByYearDayNeg(self):
        """
        Perform the testYearlyByYearDayNeg utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testYearlyByYearDayNeg through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    YEARLY,
                    count=4,
                    byyearday=(-365, -266, -166, -1),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 12, 31, 9, 0),
                datetime(1998, 1, 1, 9, 0),
                datetime(1998, 4, 10, 9, 0),
                datetime(1998, 7, 19, 9, 0),
            ],
        )

    def testYearlyByMonthAndYearDay(self):
        """
        Perform the testYearlyByMonthAndYearDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testYearlyByMonthAndYearDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    YEARLY,
                    count=4,
                    bymonth=(4, 7),
                    byyearday=(1, 100, 200, 365),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 4, 10, 9, 0),
                datetime(1998, 7, 19, 9, 0),
                datetime(1999, 4, 10, 9, 0),
                datetime(1999, 7, 19, 9, 0),
            ],
        )

    def testYearlyByMonthAndYearDayNeg(self):
        """
        Perform the testYearlyByMonthAndYearDayNeg utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testYearlyByMonthAndYearDayNeg through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    YEARLY,
                    count=4,
                    bymonth=(4, 7),
                    byyearday=(-365, -266, -166, -1),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 4, 10, 9, 0),
                datetime(1998, 7, 19, 9, 0),
                datetime(1999, 4, 10, 9, 0),
                datetime(1999, 7, 19, 9, 0),
            ],
        )

    def testYearlyByWeekNo(self):
        """
        Perform the testYearlyByWeekNo utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testYearlyByWeekNo through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(YEARLY, count=3, byweekno=20, dtstart=parse("19970902T090000"))),
            [
                datetime(1998, 5, 11, 9, 0),
                datetime(1998, 5, 12, 9, 0),
                datetime(1998, 5, 13, 9, 0),
            ],
        )

    def testYearlyByWeekNoAndWeekDay(self):
        # That's a nice one. The first days of week number one
        # may be in the last year.
        """
        Perform the testYearlyByWeekNoAndWeekDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testYearlyByWeekNoAndWeekDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    YEARLY,
                    count=3,
                    byweekno=1,
                    byweekday=MO,
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 12, 29, 9, 0),
                datetime(1999, 1, 4, 9, 0),
                datetime(2000, 1, 3, 9, 0),
            ],
        )

    def testYearlyByWeekNoAndWeekDayLarge(self):
        # Another nice test. The last days of week number 52/53
        # may be in the next year.
        """
        Perform the testYearlyByWeekNoAndWeekDayLarge utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testYearlyByWeekNoAndWeekDayLarge through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    YEARLY,
                    count=3,
                    byweekno=52,
                    byweekday=SU,
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 12, 28, 9, 0),
                datetime(1998, 12, 27, 9, 0),
                datetime(2000, 1, 2, 9, 0),
            ],
        )

    def testYearlyByWeekNoAndWeekDayLast(self):
        """
        Perform the testYearlyByWeekNoAndWeekDayLast utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testYearlyByWeekNoAndWeekDayLast through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    YEARLY,
                    count=3,
                    byweekno=-1,
                    byweekday=SU,
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 12, 28, 9, 0),
                datetime(1999, 1, 3, 9, 0),
                datetime(2000, 1, 2, 9, 0),
            ],
        )

    def testYearlyByEaster(self):
        """
        Perform the testYearlyByEaster utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testYearlyByEaster through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(YEARLY, count=3, byeaster=0, dtstart=parse("19970902T090000"))),
            [
                datetime(1998, 4, 12, 9, 0),
                datetime(1999, 4, 4, 9, 0),
                datetime(2000, 4, 23, 9, 0),
            ],
        )

    def testYearlyByEasterPos(self):
        """
        Perform the testYearlyByEasterPos utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testYearlyByEasterPos through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(YEARLY, count=3, byeaster=1, dtstart=parse("19970902T090000"))),
            [
                datetime(1998, 4, 13, 9, 0),
                datetime(1999, 4, 5, 9, 0),
                datetime(2000, 4, 24, 9, 0),
            ],
        )

    def testYearlyByEasterNeg(self):
        """
        Perform the testYearlyByEasterNeg utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testYearlyByEasterNeg through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(YEARLY, count=3, byeaster=-1, dtstart=parse("19970902T090000"))),
            [
                datetime(1998, 4, 11, 9, 0),
                datetime(1999, 4, 3, 9, 0),
                datetime(2000, 4, 22, 9, 0),
            ],
        )

    def testYearlyByWeekNoAndWeekDay53(self):
        """
        Perform the testYearlyByWeekNoAndWeekDay53 utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testYearlyByWeekNoAndWeekDay53 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    YEARLY,
                    count=3,
                    byweekno=53,
                    byweekday=MO,
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 12, 28, 9, 0),
                datetime(2004, 12, 27, 9, 0),
                datetime(2009, 12, 28, 9, 0),
            ],
        )

    def testYearlyByWeekNoAndWeekDay53(self):
        """
        Perform the testYearlyByWeekNoAndWeekDay53 utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testYearlyByWeekNoAndWeekDay53 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    YEARLY,
                    count=3,
                    byweekno=53,
                    byweekday=MO,
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 12, 28, 9, 0),
                datetime(2004, 12, 27, 9, 0),
                datetime(2009, 12, 28, 9, 0),
            ],
        )

    def testYearlyByHour(self):
        """
        Perform the testYearlyByHour utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testYearlyByHour through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(YEARLY, count=3, byhour=(6, 18), dtstart=parse("19970902T090000"))),
            [
                datetime(1997, 9, 2, 18, 0),
                datetime(1998, 9, 2, 6, 0),
                datetime(1998, 9, 2, 18, 0),
            ],
        )

    def testYearlyByMinute(self):
        """
        Perform the testYearlyByMinute utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testYearlyByMinute through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(YEARLY, count=3, byminute=(6, 18), dtstart=parse("19970902T090000"))),
            [
                datetime(1997, 9, 2, 9, 6),
                datetime(1997, 9, 2, 9, 18),
                datetime(1998, 9, 2, 9, 6),
            ],
        )

    def testYearlyBySecond(self):
        """
        Perform the testYearlyBySecond utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testYearlyBySecond through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(YEARLY, count=3, bysecond=(6, 18), dtstart=parse("19970902T090000"))),
            [
                datetime(1997, 9, 2, 9, 0, 6),
                datetime(1997, 9, 2, 9, 0, 18),
                datetime(1998, 9, 2, 9, 0, 6),
            ],
        )

    def testYearlyByHourAndMinute(self):
        """
        Perform the testYearlyByHourAndMinute utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testYearlyByHourAndMinute through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    YEARLY,
                    count=3,
                    byhour=(6, 18),
                    byminute=(6, 18),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 9, 2, 18, 6),
                datetime(1997, 9, 2, 18, 18),
                datetime(1998, 9, 2, 6, 6),
            ],
        )

    def testYearlyByHourAndSecond(self):
        """
        Perform the testYearlyByHourAndSecond utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testYearlyByHourAndSecond through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    YEARLY,
                    count=3,
                    byhour=(6, 18),
                    bysecond=(6, 18),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 9, 2, 18, 0, 6),
                datetime(1997, 9, 2, 18, 0, 18),
                datetime(1998, 9, 2, 6, 0, 6),
            ],
        )

    def testYearlyByMinuteAndSecond(self):
        """
        Perform the testYearlyByMinuteAndSecond utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testYearlyByMinuteAndSecond through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    YEARLY,
                    count=3,
                    byminute=(6, 18),
                    bysecond=(6, 18),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 9, 2, 9, 6, 6),
                datetime(1997, 9, 2, 9, 6, 18),
                datetime(1997, 9, 2, 9, 18, 6),
            ],
        )

    def testYearlyByHourAndMinuteAndSecond(self):
        """
        Perform the testYearlyByHourAndMinuteAndSecond utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testYearlyByHourAndMinuteAndSecond through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    YEARLY,
                    count=3,
                    byhour=(6, 18),
                    byminute=(6, 18),
                    bysecond=(6, 18),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 9, 2, 18, 6, 6),
                datetime(1997, 9, 2, 18, 6, 18),
                datetime(1997, 9, 2, 18, 18, 6),
            ],
        )

    def testYearlyBySetPos(self):
        """
        Perform the testYearlyBySetPos utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testYearlyBySetPos through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    YEARLY,
                    count=3,
                    bymonthday=15,
                    byhour=(6, 18),
                    bysetpos=(3, -3),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 11, 15, 18, 0),
                datetime(1998, 2, 15, 6, 0),
                datetime(1998, 11, 15, 18, 0),
            ],
        )

    def testMonthly(self):
        """
        Perform the testMonthly utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMonthly through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(MONTHLY, count=3, dtstart=parse("19970902T090000"))),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1997, 10, 2, 9, 0),
                datetime(1997, 11, 2, 9, 0),
            ],
        )

    def testMonthlyInterval(self):
        """
        Perform the testMonthlyInterval utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMonthlyInterval through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(MONTHLY, count=3, interval=2, dtstart=parse("19970902T090000"))),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1997, 11, 2, 9, 0),
                datetime(1998, 1, 2, 9, 0),
            ],
        )

    def testMonthlyIntervalLarge(self):
        """
        Perform the testMonthlyIntervalLarge utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMonthlyIntervalLarge through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(MONTHLY, count=3, interval=18, dtstart=parse("19970902T090000"))),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1999, 3, 2, 9, 0),
                datetime(2000, 9, 2, 9, 0),
            ],
        )

    def testMonthlyByMonth(self):
        """
        Perform the testMonthlyByMonth utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMonthlyByMonth through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(MONTHLY, count=3, bymonth=(1, 3), dtstart=parse("19970902T090000"))),
            [
                datetime(1998, 1, 2, 9, 0),
                datetime(1998, 3, 2, 9, 0),
                datetime(1999, 1, 2, 9, 0),
            ],
        )

    def testMonthlyByMonthDay(self):
        """
        Perform the testMonthlyByMonthDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMonthlyByMonthDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    MONTHLY,
                    count=3,
                    bymonthday=(1, 3),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 9, 3, 9, 0),
                datetime(1997, 10, 1, 9, 0),
                datetime(1997, 10, 3, 9, 0),
            ],
        )

    def testMonthlyByMonthAndMonthDay(self):
        """
        Perform the testMonthlyByMonthAndMonthDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMonthlyByMonthAndMonthDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    MONTHLY,
                    count=3,
                    bymonth=(1, 3),
                    bymonthday=(5, 7),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 1, 5, 9, 0),
                datetime(1998, 1, 7, 9, 0),
                datetime(1998, 3, 5, 9, 0),
            ],
        )

    def testMonthlyByWeekDay(self):
        """
        Perform the testMonthlyByWeekDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMonthlyByWeekDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    MONTHLY,
                    count=3,
                    byweekday=(TU, TH),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1997, 9, 4, 9, 0),
                datetime(1997, 9, 9, 9, 0),
            ],
        )

    def testMonthlyByNWeekDay(self):
        """
        Perform the testMonthlyByNWeekDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMonthlyByNWeekDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    MONTHLY,
                    count=3,
                    byweekday=(TU(1), TH(-1)),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1997, 9, 25, 9, 0),
                datetime(1997, 10, 7, 9, 0),
            ],
        )

    def testMonthlyByNWeekDayLarge(self):
        """
        Perform the testMonthlyByNWeekDayLarge utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMonthlyByNWeekDayLarge through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    MONTHLY,
                    count=3,
                    byweekday=(TU(3), TH(-3)),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 9, 11, 9, 0),
                datetime(1997, 9, 16, 9, 0),
                datetime(1997, 10, 16, 9, 0),
            ],
        )

    def testMonthlyByMonthAndWeekDay(self):
        """
        Perform the testMonthlyByMonthAndWeekDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMonthlyByMonthAndWeekDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    MONTHLY,
                    count=3,
                    bymonth=(1, 3),
                    byweekday=(TU, TH),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 1, 1, 9, 0),
                datetime(1998, 1, 6, 9, 0),
                datetime(1998, 1, 8, 9, 0),
            ],
        )

    def testMonthlyByMonthAndNWeekDay(self):
        """
        Perform the testMonthlyByMonthAndNWeekDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMonthlyByMonthAndNWeekDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    MONTHLY,
                    count=3,
                    bymonth=(1, 3),
                    byweekday=(TU(1), TH(-1)),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 1, 6, 9, 0),
                datetime(1998, 1, 29, 9, 0),
                datetime(1998, 3, 3, 9, 0),
            ],
        )

    def testMonthlyByMonthAndNWeekDayLarge(self):
        """
        Perform the testMonthlyByMonthAndNWeekDayLarge utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMonthlyByMonthAndNWeekDayLarge through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    MONTHLY,
                    count=3,
                    bymonth=(1, 3),
                    byweekday=(TU(3), TH(-3)),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 1, 15, 9, 0),
                datetime(1998, 1, 20, 9, 0),
                datetime(1998, 3, 12, 9, 0),
            ],
        )

    def testMonthlyByMonthDayAndWeekDay(self):
        """
        Perform the testMonthlyByMonthDayAndWeekDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMonthlyByMonthDayAndWeekDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    MONTHLY,
                    count=3,
                    bymonthday=(1, 3),
                    byweekday=(TU, TH),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 1, 1, 9, 0),
                datetime(1998, 2, 3, 9, 0),
                datetime(1998, 3, 3, 9, 0),
            ],
        )

    def testMonthlyByMonthAndMonthDayAndWeekDay(self):
        """
        Perform the testMonthlyByMonthAndMonthDayAndWeekDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMonthlyByMonthAndMonthDayAndWeekDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    MONTHLY,
                    count=3,
                    bymonth=(1, 3),
                    bymonthday=(1, 3),
                    byweekday=(TU, TH),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 1, 1, 9, 0),
                datetime(1998, 3, 3, 9, 0),
                datetime(2001, 3, 1, 9, 0),
            ],
        )

    def testMonthlyByYearDay(self):
        """
        Perform the testMonthlyByYearDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMonthlyByYearDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    MONTHLY,
                    count=4,
                    byyearday=(1, 100, 200, 365),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 12, 31, 9, 0),
                datetime(1998, 1, 1, 9, 0),
                datetime(1998, 4, 10, 9, 0),
                datetime(1998, 7, 19, 9, 0),
            ],
        )

    def testMonthlyByYearDayNeg(self):
        """
        Perform the testMonthlyByYearDayNeg utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMonthlyByYearDayNeg through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    MONTHLY,
                    count=4,
                    byyearday=(-365, -266, -166, -1),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 12, 31, 9, 0),
                datetime(1998, 1, 1, 9, 0),
                datetime(1998, 4, 10, 9, 0),
                datetime(1998, 7, 19, 9, 0),
            ],
        )

    def testMonthlyByMonthAndYearDay(self):
        """
        Perform the testMonthlyByMonthAndYearDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMonthlyByMonthAndYearDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    MONTHLY,
                    count=4,
                    bymonth=(4, 7),
                    byyearday=(1, 100, 200, 365),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 4, 10, 9, 0),
                datetime(1998, 7, 19, 9, 0),
                datetime(1999, 4, 10, 9, 0),
                datetime(1999, 7, 19, 9, 0),
            ],
        )

    def testMonthlyByMonthAndYearDayNeg(self):
        """
        Perform the testMonthlyByMonthAndYearDayNeg utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMonthlyByMonthAndYearDayNeg through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    MONTHLY,
                    count=4,
                    bymonth=(4, 7),
                    byyearday=(-365, -266, -166, -1),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 4, 10, 9, 0),
                datetime(1998, 7, 19, 9, 0),
                datetime(1999, 4, 10, 9, 0),
                datetime(1999, 7, 19, 9, 0),
            ],
        )

    def testMonthlyByWeekNo(self):
        """
        Perform the testMonthlyByWeekNo utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMonthlyByWeekNo through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(MONTHLY, count=3, byweekno=20, dtstart=parse("19970902T090000"))),
            [
                datetime(1998, 5, 11, 9, 0),
                datetime(1998, 5, 12, 9, 0),
                datetime(1998, 5, 13, 9, 0),
            ],
        )

    def testMonthlyByWeekNoAndWeekDay(self):
        # That's a nice one. The first days of week number one
        # may be in the last year.
        """
        Perform the testMonthlyByWeekNoAndWeekDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMonthlyByWeekNoAndWeekDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    MONTHLY,
                    count=3,
                    byweekno=1,
                    byweekday=MO,
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 12, 29, 9, 0),
                datetime(1999, 1, 4, 9, 0),
                datetime(2000, 1, 3, 9, 0),
            ],
        )

    def testMonthlyByWeekNoAndWeekDayLarge(self):
        # Another nice test. The last days of week number 52/53
        # may be in the next year.
        """
        Perform the testMonthlyByWeekNoAndWeekDayLarge utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMonthlyByWeekNoAndWeekDayLarge through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    MONTHLY,
                    count=3,
                    byweekno=52,
                    byweekday=SU,
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 12, 28, 9, 0),
                datetime(1998, 12, 27, 9, 0),
                datetime(2000, 1, 2, 9, 0),
            ],
        )

    def testMonthlyByWeekNoAndWeekDayLast(self):
        """
        Perform the testMonthlyByWeekNoAndWeekDayLast utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMonthlyByWeekNoAndWeekDayLast through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    MONTHLY,
                    count=3,
                    byweekno=-1,
                    byweekday=SU,
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 12, 28, 9, 0),
                datetime(1999, 1, 3, 9, 0),
                datetime(2000, 1, 2, 9, 0),
            ],
        )

    def testMonthlyByWeekNoAndWeekDay53(self):
        """
        Perform the testMonthlyByWeekNoAndWeekDay53 utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMonthlyByWeekNoAndWeekDay53 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    MONTHLY,
                    count=3,
                    byweekno=53,
                    byweekday=MO,
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 12, 28, 9, 0),
                datetime(2004, 12, 27, 9, 0),
                datetime(2009, 12, 28, 9, 0),
            ],
        )

    def testMonthlyByEaster(self):
        """
        Perform the testMonthlyByEaster utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMonthlyByEaster through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(MONTHLY, count=3, byeaster=0, dtstart=parse("19970902T090000"))),
            [
                datetime(1998, 4, 12, 9, 0),
                datetime(1999, 4, 4, 9, 0),
                datetime(2000, 4, 23, 9, 0),
            ],
        )

    def testMonthlyByEasterPos(self):
        """
        Perform the testMonthlyByEasterPos utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMonthlyByEasterPos through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(MONTHLY, count=3, byeaster=1, dtstart=parse("19970902T090000"))),
            [
                datetime(1998, 4, 13, 9, 0),
                datetime(1999, 4, 5, 9, 0),
                datetime(2000, 4, 24, 9, 0),
            ],
        )

    def testMonthlyByEasterNeg(self):
        """
        Perform the testMonthlyByEasterNeg utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMonthlyByEasterNeg through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(MONTHLY, count=3, byeaster=-1, dtstart=parse("19970902T090000"))),
            [
                datetime(1998, 4, 11, 9, 0),
                datetime(1999, 4, 3, 9, 0),
                datetime(2000, 4, 22, 9, 0),
            ],
        )

    def testMonthlyByHour(self):
        """
        Perform the testMonthlyByHour utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMonthlyByHour through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(MONTHLY, count=3, byhour=(6, 18), dtstart=parse("19970902T090000"))),
            [
                datetime(1997, 9, 2, 18, 0),
                datetime(1997, 10, 2, 6, 0),
                datetime(1997, 10, 2, 18, 0),
            ],
        )

    def testMonthlyByMinute(self):
        """
        Perform the testMonthlyByMinute utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMonthlyByMinute through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(MONTHLY, count=3, byminute=(6, 18), dtstart=parse("19970902T090000"))),
            [
                datetime(1997, 9, 2, 9, 6),
                datetime(1997, 9, 2, 9, 18),
                datetime(1997, 10, 2, 9, 6),
            ],
        )

    def testMonthlyBySecond(self):
        """
        Perform the testMonthlyBySecond utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMonthlyBySecond through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(MONTHLY, count=3, bysecond=(6, 18), dtstart=parse("19970902T090000"))),
            [
                datetime(1997, 9, 2, 9, 0, 6),
                datetime(1997, 9, 2, 9, 0, 18),
                datetime(1997, 10, 2, 9, 0, 6),
            ],
        )

    def testMonthlyByHourAndMinute(self):
        """
        Perform the testMonthlyByHourAndMinute utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMonthlyByHourAndMinute through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    MONTHLY,
                    count=3,
                    byhour=(6, 18),
                    byminute=(6, 18),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 9, 2, 18, 6),
                datetime(1997, 9, 2, 18, 18),
                datetime(1997, 10, 2, 6, 6),
            ],
        )

    def testMonthlyByHourAndSecond(self):
        """
        Perform the testMonthlyByHourAndSecond utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMonthlyByHourAndSecond through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    MONTHLY,
                    count=3,
                    byhour=(6, 18),
                    bysecond=(6, 18),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 9, 2, 18, 0, 6),
                datetime(1997, 9, 2, 18, 0, 18),
                datetime(1997, 10, 2, 6, 0, 6),
            ],
        )

    def testMonthlyByMinuteAndSecond(self):
        """
        Perform the testMonthlyByMinuteAndSecond utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMonthlyByMinuteAndSecond through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    MONTHLY,
                    count=3,
                    byminute=(6, 18),
                    bysecond=(6, 18),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 9, 2, 9, 6, 6),
                datetime(1997, 9, 2, 9, 6, 18),
                datetime(1997, 9, 2, 9, 18, 6),
            ],
        )

    def testMonthlyByHourAndMinuteAndSecond(self):
        """
        Perform the testMonthlyByHourAndMinuteAndSecond utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMonthlyByHourAndMinuteAndSecond through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    MONTHLY,
                    count=3,
                    byhour=(6, 18),
                    byminute=(6, 18),
                    bysecond=(6, 18),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 9, 2, 18, 6, 6),
                datetime(1997, 9, 2, 18, 6, 18),
                datetime(1997, 9, 2, 18, 18, 6),
            ],
        )

    def testMonthlyBySetPos(self):
        """
        Perform the testMonthlyBySetPos utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMonthlyBySetPos through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    MONTHLY,
                    count=3,
                    bymonthday=(13, 17),
                    byhour=(6, 18),
                    bysetpos=(3, -3),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 9, 13, 18, 0),
                datetime(1997, 9, 17, 6, 0),
                datetime(1997, 10, 13, 18, 0),
            ],
        )

    def testWeekly(self):
        """
        Perform the testWeekly utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testWeekly through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(WEEKLY, count=3, dtstart=parse("19970902T090000"))),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1997, 9, 9, 9, 0),
                datetime(1997, 9, 16, 9, 0),
            ],
        )

    def testWeeklyInterval(self):
        """
        Perform the testWeeklyInterval utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testWeeklyInterval through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(WEEKLY, count=3, interval=2, dtstart=parse("19970902T090000"))),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1997, 9, 16, 9, 0),
                datetime(1997, 9, 30, 9, 0),
            ],
        )

    def testWeeklyIntervalLarge(self):
        """
        Perform the testWeeklyIntervalLarge utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testWeeklyIntervalLarge through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(WEEKLY, count=3, interval=20, dtstart=parse("19970902T090000"))),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1998, 1, 20, 9, 0),
                datetime(1998, 6, 9, 9, 0),
            ],
        )

    def testWeeklyByMonth(self):
        """
        Perform the testWeeklyByMonth utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testWeeklyByMonth through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(WEEKLY, count=3, bymonth=(1, 3), dtstart=parse("19970902T090000"))),
            [
                datetime(1998, 1, 6, 9, 0),
                datetime(1998, 1, 13, 9, 0),
                datetime(1998, 1, 20, 9, 0),
            ],
        )

    def testWeeklyByMonthDay(self):
        """
        Perform the testWeeklyByMonthDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testWeeklyByMonthDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(WEEKLY, count=3, bymonthday=(1, 3), dtstart=parse("19970902T090000"))),
            [
                datetime(1997, 9, 3, 9, 0),
                datetime(1997, 10, 1, 9, 0),
                datetime(1997, 10, 3, 9, 0),
            ],
        )

    def testWeeklyByMonthAndMonthDay(self):
        """
        Perform the testWeeklyByMonthAndMonthDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testWeeklyByMonthAndMonthDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    WEEKLY,
                    count=3,
                    bymonth=(1, 3),
                    bymonthday=(5, 7),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 1, 5, 9, 0),
                datetime(1998, 1, 7, 9, 0),
                datetime(1998, 3, 5, 9, 0),
            ],
        )

    def testWeeklyByWeekDay(self):
        """
        Perform the testWeeklyByWeekDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testWeeklyByWeekDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    WEEKLY,
                    count=3,
                    byweekday=(TU, TH),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1997, 9, 4, 9, 0),
                datetime(1997, 9, 9, 9, 0),
            ],
        )

    def testWeeklyByNWeekDay(self):
        """
        Perform the testWeeklyByNWeekDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testWeeklyByNWeekDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    WEEKLY,
                    count=3,
                    byweekday=(TU(1), TH(-1)),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1997, 9, 4, 9, 0),
                datetime(1997, 9, 9, 9, 0),
            ],
        )

    def testWeeklyByMonthAndWeekDay(self):
        # This test is interesting, because it crosses the year
        # boundary in a weekly period to find day '1' as a
        # valid recurrence.
        """
        Perform the testWeeklyByMonthAndWeekDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testWeeklyByMonthAndWeekDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    WEEKLY,
                    count=3,
                    bymonth=(1, 3),
                    byweekday=(TU, TH),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 1, 1, 9, 0),
                datetime(1998, 1, 6, 9, 0),
                datetime(1998, 1, 8, 9, 0),
            ],
        )

    def testWeeklyByMonthAndNWeekDay(self):
        """
        Perform the testWeeklyByMonthAndNWeekDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testWeeklyByMonthAndNWeekDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    WEEKLY,
                    count=3,
                    bymonth=(1, 3),
                    byweekday=(TU(1), TH(-1)),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 1, 1, 9, 0),
                datetime(1998, 1, 6, 9, 0),
                datetime(1998, 1, 8, 9, 0),
            ],
        )

    def testWeeklyByMonthDayAndWeekDay(self):
        """
        Perform the testWeeklyByMonthDayAndWeekDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testWeeklyByMonthDayAndWeekDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    WEEKLY,
                    count=3,
                    bymonthday=(1, 3),
                    byweekday=(TU, TH),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 1, 1, 9, 0),
                datetime(1998, 2, 3, 9, 0),
                datetime(1998, 3, 3, 9, 0),
            ],
        )

    def testWeeklyByMonthAndMonthDayAndWeekDay(self):
        """
        Perform the testWeeklyByMonthAndMonthDayAndWeekDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testWeeklyByMonthAndMonthDayAndWeekDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    WEEKLY,
                    count=3,
                    bymonth=(1, 3),
                    bymonthday=(1, 3),
                    byweekday=(TU, TH),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 1, 1, 9, 0),
                datetime(1998, 3, 3, 9, 0),
                datetime(2001, 3, 1, 9, 0),
            ],
        )

    def testWeeklyByYearDay(self):
        """
        Perform the testWeeklyByYearDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testWeeklyByYearDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    WEEKLY,
                    count=4,
                    byyearday=(1, 100, 200, 365),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 12, 31, 9, 0),
                datetime(1998, 1, 1, 9, 0),
                datetime(1998, 4, 10, 9, 0),
                datetime(1998, 7, 19, 9, 0),
            ],
        )

    def testWeeklyByYearDayNeg(self):
        """
        Perform the testWeeklyByYearDayNeg utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testWeeklyByYearDayNeg through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    WEEKLY,
                    count=4,
                    byyearday=(-365, -266, -166, -1),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 12, 31, 9, 0),
                datetime(1998, 1, 1, 9, 0),
                datetime(1998, 4, 10, 9, 0),
                datetime(1998, 7, 19, 9, 0),
            ],
        )

    def testWeeklyByMonthAndYearDay(self):
        """
        Perform the testWeeklyByMonthAndYearDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testWeeklyByMonthAndYearDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    WEEKLY,
                    count=4,
                    bymonth=(1, 7),
                    byyearday=(1, 100, 200, 365),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 1, 1, 9, 0),
                datetime(1998, 7, 19, 9, 0),
                datetime(1999, 1, 1, 9, 0),
                datetime(1999, 7, 19, 9, 0),
            ],
        )

    def testWeeklyByMonthAndYearDayNeg(self):
        """
        Perform the testWeeklyByMonthAndYearDayNeg utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testWeeklyByMonthAndYearDayNeg through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    WEEKLY,
                    count=4,
                    bymonth=(1, 7),
                    byyearday=(-365, -266, -166, -1),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 1, 1, 9, 0),
                datetime(1998, 7, 19, 9, 0),
                datetime(1999, 1, 1, 9, 0),
                datetime(1999, 7, 19, 9, 0),
            ],
        )

    def testWeeklyByWeekNo(self):
        """
        Perform the testWeeklyByWeekNo utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testWeeklyByWeekNo through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(WEEKLY, count=3, byweekno=20, dtstart=parse("19970902T090000"))),
            [
                datetime(1998, 5, 11, 9, 0),
                datetime(1998, 5, 12, 9, 0),
                datetime(1998, 5, 13, 9, 0),
            ],
        )

    def testWeeklyByWeekNoAndWeekDay(self):
        # That's a nice one. The first days of week number one
        # may be in the last year.
        """
        Perform the testWeeklyByWeekNoAndWeekDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testWeeklyByWeekNoAndWeekDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    WEEKLY,
                    count=3,
                    byweekno=1,
                    byweekday=MO,
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 12, 29, 9, 0),
                datetime(1999, 1, 4, 9, 0),
                datetime(2000, 1, 3, 9, 0),
            ],
        )

    def testWeeklyByWeekNoAndWeekDayLarge(self):
        # Another nice test. The last days of week number 52/53
        # may be in the next year.
        """
        Perform the testWeeklyByWeekNoAndWeekDayLarge utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testWeeklyByWeekNoAndWeekDayLarge through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    WEEKLY,
                    count=3,
                    byweekno=52,
                    byweekday=SU,
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 12, 28, 9, 0),
                datetime(1998, 12, 27, 9, 0),
                datetime(2000, 1, 2, 9, 0),
            ],
        )

    def testWeeklyByWeekNoAndWeekDayLast(self):
        """
        Perform the testWeeklyByWeekNoAndWeekDayLast utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testWeeklyByWeekNoAndWeekDayLast through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    WEEKLY,
                    count=3,
                    byweekno=-1,
                    byweekday=SU,
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 12, 28, 9, 0),
                datetime(1999, 1, 3, 9, 0),
                datetime(2000, 1, 2, 9, 0),
            ],
        )

    def testWeeklyByWeekNoAndWeekDay53(self):
        """
        Perform the testWeeklyByWeekNoAndWeekDay53 utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testWeeklyByWeekNoAndWeekDay53 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    WEEKLY,
                    count=3,
                    byweekno=53,
                    byweekday=MO,
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 12, 28, 9, 0),
                datetime(2004, 12, 27, 9, 0),
                datetime(2009, 12, 28, 9, 0),
            ],
        )

    def testWeeklyByEaster(self):
        """
        Perform the testWeeklyByEaster utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testWeeklyByEaster through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(WEEKLY, count=3, byeaster=0, dtstart=parse("19970902T090000"))),
            [
                datetime(1998, 4, 12, 9, 0),
                datetime(1999, 4, 4, 9, 0),
                datetime(2000, 4, 23, 9, 0),
            ],
        )

    def testWeeklyByEasterPos(self):
        """
        Perform the testWeeklyByEasterPos utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testWeeklyByEasterPos through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(WEEKLY, count=3, byeaster=1, dtstart=parse("19970902T090000"))),
            [
                datetime(1998, 4, 13, 9, 0),
                datetime(1999, 4, 5, 9, 0),
                datetime(2000, 4, 24, 9, 0),
            ],
        )

    def testWeeklyByEasterNeg(self):
        """
        Perform the testWeeklyByEasterNeg utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testWeeklyByEasterNeg through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(WEEKLY, count=3, byeaster=-1, dtstart=parse("19970902T090000"))),
            [
                datetime(1998, 4, 11, 9, 0),
                datetime(1999, 4, 3, 9, 0),
                datetime(2000, 4, 22, 9, 0),
            ],
        )

    def testWeeklyByHour(self):
        """
        Perform the testWeeklyByHour utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testWeeklyByHour through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(WEEKLY, count=3, byhour=(6, 18), dtstart=parse("19970902T090000"))),
            [
                datetime(1997, 9, 2, 18, 0),
                datetime(1997, 9, 9, 6, 0),
                datetime(1997, 9, 9, 18, 0),
            ],
        )

    def testWeeklyByMinute(self):
        """
        Perform the testWeeklyByMinute utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testWeeklyByMinute through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(WEEKLY, count=3, byminute=(6, 18), dtstart=parse("19970902T090000"))),
            [
                datetime(1997, 9, 2, 9, 6),
                datetime(1997, 9, 2, 9, 18),
                datetime(1997, 9, 9, 9, 6),
            ],
        )

    def testWeeklyBySecond(self):
        """
        Perform the testWeeklyBySecond utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testWeeklyBySecond through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(WEEKLY, count=3, bysecond=(6, 18), dtstart=parse("19970902T090000"))),
            [
                datetime(1997, 9, 2, 9, 0, 6),
                datetime(1997, 9, 2, 9, 0, 18),
                datetime(1997, 9, 9, 9, 0, 6),
            ],
        )

    def testWeeklyByHourAndMinute(self):
        """
        Perform the testWeeklyByHourAndMinute utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testWeeklyByHourAndMinute through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    WEEKLY,
                    count=3,
                    byhour=(6, 18),
                    byminute=(6, 18),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 9, 2, 18, 6),
                datetime(1997, 9, 2, 18, 18),
                datetime(1997, 9, 9, 6, 6),
            ],
        )

    def testWeeklyByHourAndSecond(self):
        """
        Perform the testWeeklyByHourAndSecond utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testWeeklyByHourAndSecond through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    WEEKLY,
                    count=3,
                    byhour=(6, 18),
                    bysecond=(6, 18),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 9, 2, 18, 0, 6),
                datetime(1997, 9, 2, 18, 0, 18),
                datetime(1997, 9, 9, 6, 0, 6),
            ],
        )

    def testWeeklyByMinuteAndSecond(self):
        """
        Perform the testWeeklyByMinuteAndSecond utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testWeeklyByMinuteAndSecond through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    WEEKLY,
                    count=3,
                    byminute=(6, 18),
                    bysecond=(6, 18),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 9, 2, 9, 6, 6),
                datetime(1997, 9, 2, 9, 6, 18),
                datetime(1997, 9, 2, 9, 18, 6),
            ],
        )

    def testWeeklyByHourAndMinuteAndSecond(self):
        """
        Perform the testWeeklyByHourAndMinuteAndSecond utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testWeeklyByHourAndMinuteAndSecond through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    WEEKLY,
                    count=3,
                    byhour=(6, 18),
                    byminute=(6, 18),
                    bysecond=(6, 18),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 9, 2, 18, 6, 6),
                datetime(1997, 9, 2, 18, 6, 18),
                datetime(1997, 9, 2, 18, 18, 6),
            ],
        )

    def testWeeklyBySetPos(self):
        """
        Perform the testWeeklyBySetPos utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testWeeklyBySetPos through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    WEEKLY,
                    count=3,
                    byweekday=(TU, TH),
                    byhour=(6, 18),
                    bysetpos=(3, -3),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 9, 2, 18, 0),
                datetime(1997, 9, 4, 6, 0),
                datetime(1997, 9, 9, 18, 0),
            ],
        )

    def testDaily(self):
        """
        Perform the testDaily utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testDaily through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(DAILY, count=3, dtstart=parse("19970902T090000"))),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1997, 9, 3, 9, 0),
                datetime(1997, 9, 4, 9, 0),
            ],
        )

    def testDailyInterval(self):
        """
        Perform the testDailyInterval utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testDailyInterval through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(DAILY, count=3, interval=2, dtstart=parse("19970902T090000"))),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1997, 9, 4, 9, 0),
                datetime(1997, 9, 6, 9, 0),
            ],
        )

    def testDailyIntervalLarge(self):
        """
        Perform the testDailyIntervalLarge utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testDailyIntervalLarge through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(DAILY, count=3, interval=92, dtstart=parse("19970902T090000"))),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1997, 12, 3, 9, 0),
                datetime(1998, 3, 5, 9, 0),
            ],
        )

    def testDailyByMonth(self):
        """
        Perform the testDailyByMonth utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testDailyByMonth through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(DAILY, count=3, bymonth=(1, 3), dtstart=parse("19970902T090000"))),
            [
                datetime(1998, 1, 1, 9, 0),
                datetime(1998, 1, 2, 9, 0),
                datetime(1998, 1, 3, 9, 0),
            ],
        )

    def testDailyByMonthDay(self):
        """
        Perform the testDailyByMonthDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testDailyByMonthDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(DAILY, count=3, bymonthday=(1, 3), dtstart=parse("19970902T090000"))),
            [
                datetime(1997, 9, 3, 9, 0),
                datetime(1997, 10, 1, 9, 0),
                datetime(1997, 10, 3, 9, 0),
            ],
        )

    def testDailyByMonthAndMonthDay(self):
        """
        Perform the testDailyByMonthAndMonthDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testDailyByMonthAndMonthDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    DAILY,
                    count=3,
                    bymonth=(1, 3),
                    bymonthday=(5, 7),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 1, 5, 9, 0),
                datetime(1998, 1, 7, 9, 0),
                datetime(1998, 3, 5, 9, 0),
            ],
        )

    def testDailyByWeekDay(self):
        """
        Perform the testDailyByWeekDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testDailyByWeekDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(DAILY, count=3, byweekday=(TU, TH), dtstart=parse("19970902T090000"))),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1997, 9, 4, 9, 0),
                datetime(1997, 9, 9, 9, 0),
            ],
        )

    def testDailyByNWeekDay(self):
        """
        Perform the testDailyByNWeekDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testDailyByNWeekDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    DAILY,
                    count=3,
                    byweekday=(TU(1), TH(-1)),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1997, 9, 4, 9, 0),
                datetime(1997, 9, 9, 9, 0),
            ],
        )

    def testDailyByMonthAndWeekDay(self):
        """
        Perform the testDailyByMonthAndWeekDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testDailyByMonthAndWeekDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    DAILY,
                    count=3,
                    bymonth=(1, 3),
                    byweekday=(TU, TH),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 1, 1, 9, 0),
                datetime(1998, 1, 6, 9, 0),
                datetime(1998, 1, 8, 9, 0),
            ],
        )

    def testDailyByMonthAndNWeekDay(self):
        """
        Perform the testDailyByMonthAndNWeekDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testDailyByMonthAndNWeekDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    DAILY,
                    count=3,
                    bymonth=(1, 3),
                    byweekday=(TU(1), TH(-1)),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 1, 1, 9, 0),
                datetime(1998, 1, 6, 9, 0),
                datetime(1998, 1, 8, 9, 0),
            ],
        )

    def testDailyByMonthDayAndWeekDay(self):
        """
        Perform the testDailyByMonthDayAndWeekDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testDailyByMonthDayAndWeekDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    DAILY,
                    count=3,
                    bymonthday=(1, 3),
                    byweekday=(TU, TH),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 1, 1, 9, 0),
                datetime(1998, 2, 3, 9, 0),
                datetime(1998, 3, 3, 9, 0),
            ],
        )

    def testDailyByMonthAndMonthDayAndWeekDay(self):
        """
        Perform the testDailyByMonthAndMonthDayAndWeekDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testDailyByMonthAndMonthDayAndWeekDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    DAILY,
                    count=3,
                    bymonth=(1, 3),
                    bymonthday=(1, 3),
                    byweekday=(TU, TH),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 1, 1, 9, 0),
                datetime(1998, 3, 3, 9, 0),
                datetime(2001, 3, 1, 9, 0),
            ],
        )

    def testDailyByYearDay(self):
        """
        Perform the testDailyByYearDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testDailyByYearDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    DAILY,
                    count=4,
                    byyearday=(1, 100, 200, 365),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 12, 31, 9, 0),
                datetime(1998, 1, 1, 9, 0),
                datetime(1998, 4, 10, 9, 0),
                datetime(1998, 7, 19, 9, 0),
            ],
        )

    def testDailyByYearDayNeg(self):
        """
        Perform the testDailyByYearDayNeg utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testDailyByYearDayNeg through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    DAILY,
                    count=4,
                    byyearday=(-365, -266, -166, -1),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 12, 31, 9, 0),
                datetime(1998, 1, 1, 9, 0),
                datetime(1998, 4, 10, 9, 0),
                datetime(1998, 7, 19, 9, 0),
            ],
        )

    def testDailyByMonthAndYearDay(self):
        """
        Perform the testDailyByMonthAndYearDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testDailyByMonthAndYearDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    DAILY,
                    count=4,
                    bymonth=(1, 7),
                    byyearday=(1, 100, 200, 365),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 1, 1, 9, 0),
                datetime(1998, 7, 19, 9, 0),
                datetime(1999, 1, 1, 9, 0),
                datetime(1999, 7, 19, 9, 0),
            ],
        )

    def testDailyByMonthAndYearDayNeg(self):
        """
        Perform the testDailyByMonthAndYearDayNeg utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testDailyByMonthAndYearDayNeg through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    DAILY,
                    count=4,
                    bymonth=(1, 7),
                    byyearday=(-365, -266, -166, -1),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 1, 1, 9, 0),
                datetime(1998, 7, 19, 9, 0),
                datetime(1999, 1, 1, 9, 0),
                datetime(1999, 7, 19, 9, 0),
            ],
        )

    def testDailyByWeekNo(self):
        """
        Perform the testDailyByWeekNo utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testDailyByWeekNo through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(DAILY, count=3, byweekno=20, dtstart=parse("19970902T090000"))),
            [
                datetime(1998, 5, 11, 9, 0),
                datetime(1998, 5, 12, 9, 0),
                datetime(1998, 5, 13, 9, 0),
            ],
        )

    def testDailyByWeekNoAndWeekDay(self):
        # That's a nice one. The first days of week number one
        # may be in the last year.
        """
        Perform the testDailyByWeekNoAndWeekDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testDailyByWeekNoAndWeekDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    DAILY,
                    count=3,
                    byweekno=1,
                    byweekday=MO,
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 12, 29, 9, 0),
                datetime(1999, 1, 4, 9, 0),
                datetime(2000, 1, 3, 9, 0),
            ],
        )

    def testDailyByWeekNoAndWeekDayLarge(self):
        # Another nice test. The last days of week number 52/53
        # may be in the next year.
        """
        Perform the testDailyByWeekNoAndWeekDayLarge utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testDailyByWeekNoAndWeekDayLarge through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    DAILY,
                    count=3,
                    byweekno=52,
                    byweekday=SU,
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 12, 28, 9, 0),
                datetime(1998, 12, 27, 9, 0),
                datetime(2000, 1, 2, 9, 0),
            ],
        )

    def testDailyByWeekNoAndWeekDayLast(self):
        """
        Perform the testDailyByWeekNoAndWeekDayLast utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testDailyByWeekNoAndWeekDayLast through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    DAILY,
                    count=3,
                    byweekno=-1,
                    byweekday=SU,
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 12, 28, 9, 0),
                datetime(1999, 1, 3, 9, 0),
                datetime(2000, 1, 2, 9, 0),
            ],
        )

    def testDailyByWeekNoAndWeekDay53(self):
        """
        Perform the testDailyByWeekNoAndWeekDay53 utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testDailyByWeekNoAndWeekDay53 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    DAILY,
                    count=3,
                    byweekno=53,
                    byweekday=MO,
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 12, 28, 9, 0),
                datetime(2004, 12, 27, 9, 0),
                datetime(2009, 12, 28, 9, 0),
            ],
        )

    def testDailyByEaster(self):
        """
        Perform the testDailyByEaster utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testDailyByEaster through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(DAILY, count=3, byeaster=0, dtstart=parse("19970902T090000"))),
            [
                datetime(1998, 4, 12, 9, 0),
                datetime(1999, 4, 4, 9, 0),
                datetime(2000, 4, 23, 9, 0),
            ],
        )

    def testDailyByEasterPos(self):
        """
        Perform the testDailyByEasterPos utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testDailyByEasterPos through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(DAILY, count=3, byeaster=1, dtstart=parse("19970902T090000"))),
            [
                datetime(1998, 4, 13, 9, 0),
                datetime(1999, 4, 5, 9, 0),
                datetime(2000, 4, 24, 9, 0),
            ],
        )

    def testDailyByEasterNeg(self):
        """
        Perform the testDailyByEasterNeg utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testDailyByEasterNeg through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(DAILY, count=3, byeaster=-1, dtstart=parse("19970902T090000"))),
            [
                datetime(1998, 4, 11, 9, 0),
                datetime(1999, 4, 3, 9, 0),
                datetime(2000, 4, 22, 9, 0),
            ],
        )

    def testDailyByHour(self):
        """
        Perform the testDailyByHour utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testDailyByHour through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(DAILY, count=3, byhour=(6, 18), dtstart=parse("19970902T090000"))),
            [
                datetime(1997, 9, 2, 18, 0),
                datetime(1997, 9, 3, 6, 0),
                datetime(1997, 9, 3, 18, 0),
            ],
        )

    def testDailyByMinute(self):
        """
        Perform the testDailyByMinute utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testDailyByMinute through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(DAILY, count=3, byminute=(6, 18), dtstart=parse("19970902T090000"))),
            [
                datetime(1997, 9, 2, 9, 6),
                datetime(1997, 9, 2, 9, 18),
                datetime(1997, 9, 3, 9, 6),
            ],
        )

    def testDailyBySecond(self):
        """
        Perform the testDailyBySecond utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testDailyBySecond through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(DAILY, count=3, bysecond=(6, 18), dtstart=parse("19970902T090000"))),
            [
                datetime(1997, 9, 2, 9, 0, 6),
                datetime(1997, 9, 2, 9, 0, 18),
                datetime(1997, 9, 3, 9, 0, 6),
            ],
        )

    def testDailyByHourAndMinute(self):
        """
        Perform the testDailyByHourAndMinute utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testDailyByHourAndMinute through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    DAILY,
                    count=3,
                    byhour=(6, 18),
                    byminute=(6, 18),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 9, 2, 18, 6),
                datetime(1997, 9, 2, 18, 18),
                datetime(1997, 9, 3, 6, 6),
            ],
        )

    def testDailyByHourAndSecond(self):
        """
        Perform the testDailyByHourAndSecond utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testDailyByHourAndSecond through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    DAILY,
                    count=3,
                    byhour=(6, 18),
                    bysecond=(6, 18),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 9, 2, 18, 0, 6),
                datetime(1997, 9, 2, 18, 0, 18),
                datetime(1997, 9, 3, 6, 0, 6),
            ],
        )

    def testDailyByMinuteAndSecond(self):
        """
        Perform the testDailyByMinuteAndSecond utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testDailyByMinuteAndSecond through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    DAILY,
                    count=3,
                    byminute=(6, 18),
                    bysecond=(6, 18),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 9, 2, 9, 6, 6),
                datetime(1997, 9, 2, 9, 6, 18),
                datetime(1997, 9, 2, 9, 18, 6),
            ],
        )

    def testDailyByHourAndMinuteAndSecond(self):
        """
        Perform the testDailyByHourAndMinuteAndSecond utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testDailyByHourAndMinuteAndSecond through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    DAILY,
                    count=3,
                    byhour=(6, 18),
                    byminute=(6, 18),
                    bysecond=(6, 18),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 9, 2, 18, 6, 6),
                datetime(1997, 9, 2, 18, 6, 18),
                datetime(1997, 9, 2, 18, 18, 6),
            ],
        )

    def testDailyBySetPos(self):
        """
        Perform the testDailyBySetPos utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testDailyBySetPos through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    DAILY,
                    count=3,
                    byhour=(6, 18),
                    byminute=(15, 45),
                    bysetpos=(3, -3),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 9, 2, 18, 15),
                datetime(1997, 9, 3, 6, 45),
                datetime(1997, 9, 3, 18, 15),
            ],
        )

    def testHourly(self):
        """
        Perform the testHourly utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testHourly through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(HOURLY, count=3, dtstart=parse("19970902T090000"))),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1997, 9, 2, 10, 0),
                datetime(1997, 9, 2, 11, 0),
            ],
        )

    def testHourlyInterval(self):
        """
        Perform the testHourlyInterval utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testHourlyInterval through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(HOURLY, count=3, interval=2, dtstart=parse("19970902T090000"))),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1997, 9, 2, 11, 0),
                datetime(1997, 9, 2, 13, 0),
            ],
        )

    def testHourlyIntervalLarge(self):
        """
        Perform the testHourlyIntervalLarge utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testHourlyIntervalLarge through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(HOURLY, count=3, interval=769, dtstart=parse("19970902T090000"))),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1997, 10, 4, 10, 0),
                datetime(1997, 11, 5, 11, 0),
            ],
        )

    def testHourlyByMonth(self):
        """
        Perform the testHourlyByMonth utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testHourlyByMonth through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(HOURLY, count=3, bymonth=(1, 3), dtstart=parse("19970902T090000"))),
            [
                datetime(1998, 1, 1, 0, 0),
                datetime(1998, 1, 1, 1, 0),
                datetime(1998, 1, 1, 2, 0),
            ],
        )

    def testHourlyByMonthDay(self):
        """
        Perform the testHourlyByMonthDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testHourlyByMonthDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(HOURLY, count=3, bymonthday=(1, 3), dtstart=parse("19970902T090000"))),
            [
                datetime(1997, 9, 3, 0, 0),
                datetime(1997, 9, 3, 1, 0),
                datetime(1997, 9, 3, 2, 0),
            ],
        )

    def testHourlyByMonthAndMonthDay(self):
        """
        Perform the testHourlyByMonthAndMonthDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testHourlyByMonthAndMonthDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    HOURLY,
                    count=3,
                    bymonth=(1, 3),
                    bymonthday=(5, 7),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 1, 5, 0, 0),
                datetime(1998, 1, 5, 1, 0),
                datetime(1998, 1, 5, 2, 0),
            ],
        )

    def testHourlyByWeekDay(self):
        """
        Perform the testHourlyByWeekDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testHourlyByWeekDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    HOURLY,
                    count=3,
                    byweekday=(TU, TH),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1997, 9, 2, 10, 0),
                datetime(1997, 9, 2, 11, 0),
            ],
        )

    def testHourlyByNWeekDay(self):
        """
        Perform the testHourlyByNWeekDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testHourlyByNWeekDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    HOURLY,
                    count=3,
                    byweekday=(TU(1), TH(-1)),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1997, 9, 2, 10, 0),
                datetime(1997, 9, 2, 11, 0),
            ],
        )

    def testHourlyByMonthAndWeekDay(self):
        """
        Perform the testHourlyByMonthAndWeekDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testHourlyByMonthAndWeekDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    HOURLY,
                    count=3,
                    bymonth=(1, 3),
                    byweekday=(TU, TH),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 1, 1, 0, 0),
                datetime(1998, 1, 1, 1, 0),
                datetime(1998, 1, 1, 2, 0),
            ],
        )

    def testHourlyByMonthAndNWeekDay(self):
        """
        Perform the testHourlyByMonthAndNWeekDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testHourlyByMonthAndNWeekDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    HOURLY,
                    count=3,
                    bymonth=(1, 3),
                    byweekday=(TU(1), TH(-1)),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 1, 1, 0, 0),
                datetime(1998, 1, 1, 1, 0),
                datetime(1998, 1, 1, 2, 0),
            ],
        )

    def testHourlyByMonthDayAndWeekDay(self):
        """
        Perform the testHourlyByMonthDayAndWeekDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testHourlyByMonthDayAndWeekDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    HOURLY,
                    count=3,
                    bymonthday=(1, 3),
                    byweekday=(TU, TH),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 1, 1, 0, 0),
                datetime(1998, 1, 1, 1, 0),
                datetime(1998, 1, 1, 2, 0),
            ],
        )

    def testHourlyByMonthAndMonthDayAndWeekDay(self):
        """
        Perform the testHourlyByMonthAndMonthDayAndWeekDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testHourlyByMonthAndMonthDayAndWeekDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    HOURLY,
                    count=3,
                    bymonth=(1, 3),
                    bymonthday=(1, 3),
                    byweekday=(TU, TH),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 1, 1, 0, 0),
                datetime(1998, 1, 1, 1, 0),
                datetime(1998, 1, 1, 2, 0),
            ],
        )

    def testHourlyByYearDay(self):
        """
        Perform the testHourlyByYearDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testHourlyByYearDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    HOURLY,
                    count=4,
                    byyearday=(1, 100, 200, 365),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 12, 31, 0, 0),
                datetime(1997, 12, 31, 1, 0),
                datetime(1997, 12, 31, 2, 0),
                datetime(1997, 12, 31, 3, 0),
            ],
        )

    def testHourlyByYearDayNeg(self):
        """
        Perform the testHourlyByYearDayNeg utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testHourlyByYearDayNeg through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    HOURLY,
                    count=4,
                    byyearday=(-365, -266, -166, -1),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 12, 31, 0, 0),
                datetime(1997, 12, 31, 1, 0),
                datetime(1997, 12, 31, 2, 0),
                datetime(1997, 12, 31, 3, 0),
            ],
        )

    def testHourlyByMonthAndYearDay(self):
        """
        Perform the testHourlyByMonthAndYearDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testHourlyByMonthAndYearDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    HOURLY,
                    count=4,
                    bymonth=(4, 7),
                    byyearday=(1, 100, 200, 365),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 4, 10, 0, 0),
                datetime(1998, 4, 10, 1, 0),
                datetime(1998, 4, 10, 2, 0),
                datetime(1998, 4, 10, 3, 0),
            ],
        )

    def testHourlyByMonthAndYearDayNeg(self):
        """
        Perform the testHourlyByMonthAndYearDayNeg utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testHourlyByMonthAndYearDayNeg through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    HOURLY,
                    count=4,
                    bymonth=(4, 7),
                    byyearday=(-365, -266, -166, -1),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 4, 10, 0, 0),
                datetime(1998, 4, 10, 1, 0),
                datetime(1998, 4, 10, 2, 0),
                datetime(1998, 4, 10, 3, 0),
            ],
        )

    def testHourlyByWeekNo(self):
        """
        Perform the testHourlyByWeekNo utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testHourlyByWeekNo through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(HOURLY, count=3, byweekno=20, dtstart=parse("19970902T090000"))),
            [
                datetime(1998, 5, 11, 0, 0),
                datetime(1998, 5, 11, 1, 0),
                datetime(1998, 5, 11, 2, 0),
            ],
        )

    def testHourlyByWeekNoAndWeekDay(self):
        """
        Perform the testHourlyByWeekNoAndWeekDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testHourlyByWeekNoAndWeekDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    HOURLY,
                    count=3,
                    byweekno=1,
                    byweekday=MO,
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 12, 29, 0, 0),
                datetime(1997, 12, 29, 1, 0),
                datetime(1997, 12, 29, 2, 0),
            ],
        )

    def testHourlyByWeekNoAndWeekDayLarge(self):
        """
        Perform the testHourlyByWeekNoAndWeekDayLarge utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testHourlyByWeekNoAndWeekDayLarge through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    HOURLY,
                    count=3,
                    byweekno=52,
                    byweekday=SU,
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 12, 28, 0, 0),
                datetime(1997, 12, 28, 1, 0),
                datetime(1997, 12, 28, 2, 0),
            ],
        )

    def testHourlyByWeekNoAndWeekDayLast(self):
        """
        Perform the testHourlyByWeekNoAndWeekDayLast utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testHourlyByWeekNoAndWeekDayLast through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    HOURLY,
                    count=3,
                    byweekno=-1,
                    byweekday=SU,
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 12, 28, 0, 0),
                datetime(1997, 12, 28, 1, 0),
                datetime(1997, 12, 28, 2, 0),
            ],
        )

    def testHourlyByWeekNoAndWeekDay53(self):
        """
        Perform the testHourlyByWeekNoAndWeekDay53 utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testHourlyByWeekNoAndWeekDay53 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    HOURLY,
                    count=3,
                    byweekno=53,
                    byweekday=MO,
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 12, 28, 0, 0),
                datetime(1998, 12, 28, 1, 0),
                datetime(1998, 12, 28, 2, 0),
            ],
        )

    def testHourlyByEaster(self):
        """
        Perform the testHourlyByEaster utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testHourlyByEaster through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(HOURLY, count=3, byeaster=0, dtstart=parse("19970902T090000"))),
            [
                datetime(1998, 4, 12, 0, 0),
                datetime(1998, 4, 12, 1, 0),
                datetime(1998, 4, 12, 2, 0),
            ],
        )

    def testHourlyByEasterPos(self):
        """
        Perform the testHourlyByEasterPos utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testHourlyByEasterPos through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(HOURLY, count=3, byeaster=1, dtstart=parse("19970902T090000"))),
            [
                datetime(1998, 4, 13, 0, 0),
                datetime(1998, 4, 13, 1, 0),
                datetime(1998, 4, 13, 2, 0),
            ],
        )

    def testHourlyByEasterNeg(self):
        """
        Perform the testHourlyByEasterNeg utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testHourlyByEasterNeg through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(HOURLY, count=3, byeaster=-1, dtstart=parse("19970902T090000"))),
            [
                datetime(1998, 4, 11, 0, 0),
                datetime(1998, 4, 11, 1, 0),
                datetime(1998, 4, 11, 2, 0),
            ],
        )

    def testHourlyByHour(self):
        """
        Perform the testHourlyByHour utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testHourlyByHour through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(HOURLY, count=3, byhour=(6, 18), dtstart=parse("19970902T090000"))),
            [
                datetime(1997, 9, 2, 18, 0),
                datetime(1997, 9, 3, 6, 0),
                datetime(1997, 9, 3, 18, 0),
            ],
        )

    def testHourlyByMinute(self):
        """
        Perform the testHourlyByMinute utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testHourlyByMinute through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(HOURLY, count=3, byminute=(6, 18), dtstart=parse("19970902T090000"))),
            [
                datetime(1997, 9, 2, 9, 6),
                datetime(1997, 9, 2, 9, 18),
                datetime(1997, 9, 2, 10, 6),
            ],
        )

    def testHourlyBySecond(self):
        """
        Perform the testHourlyBySecond utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testHourlyBySecond through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(HOURLY, count=3, bysecond=(6, 18), dtstart=parse("19970902T090000"))),
            [
                datetime(1997, 9, 2, 9, 0, 6),
                datetime(1997, 9, 2, 9, 0, 18),
                datetime(1997, 9, 2, 10, 0, 6),
            ],
        )

    def testHourlyByHourAndMinute(self):
        """
        Perform the testHourlyByHourAndMinute utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testHourlyByHourAndMinute through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    HOURLY,
                    count=3,
                    byhour=(6, 18),
                    byminute=(6, 18),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 9, 2, 18, 6),
                datetime(1997, 9, 2, 18, 18),
                datetime(1997, 9, 3, 6, 6),
            ],
        )

    def testHourlyByHourAndSecond(self):
        """
        Perform the testHourlyByHourAndSecond utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testHourlyByHourAndSecond through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    HOURLY,
                    count=3,
                    byhour=(6, 18),
                    bysecond=(6, 18),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 9, 2, 18, 0, 6),
                datetime(1997, 9, 2, 18, 0, 18),
                datetime(1997, 9, 3, 6, 0, 6),
            ],
        )

    def testHourlyByMinuteAndSecond(self):
        """
        Perform the testHourlyByMinuteAndSecond utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testHourlyByMinuteAndSecond through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    HOURLY,
                    count=3,
                    byminute=(6, 18),
                    bysecond=(6, 18),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 9, 2, 9, 6, 6),
                datetime(1997, 9, 2, 9, 6, 18),
                datetime(1997, 9, 2, 9, 18, 6),
            ],
        )

    def testHourlyByHourAndMinuteAndSecond(self):
        """
        Perform the testHourlyByHourAndMinuteAndSecond utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testHourlyByHourAndMinuteAndSecond through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    HOURLY,
                    count=3,
                    byhour=(6, 18),
                    byminute=(6, 18),
                    bysecond=(6, 18),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 9, 2, 18, 6, 6),
                datetime(1997, 9, 2, 18, 6, 18),
                datetime(1997, 9, 2, 18, 18, 6),
            ],
        )

    def testHourlyBySetPos(self):
        """
        Perform the testHourlyBySetPos utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testHourlyBySetPos through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    HOURLY,
                    count=3,
                    byminute=(15, 45),
                    bysecond=(15, 45),
                    bysetpos=(3, -3),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 9, 2, 9, 15, 45),
                datetime(1997, 9, 2, 9, 45, 15),
                datetime(1997, 9, 2, 10, 15, 45),
            ],
        )

    def testMinutely(self):
        """
        Perform the testMinutely utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMinutely through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(MINUTELY, count=3, dtstart=parse("19970902T090000"))),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1997, 9, 2, 9, 1),
                datetime(1997, 9, 2, 9, 2),
            ],
        )

    def testMinutelyInterval(self):
        """
        Perform the testMinutelyInterval utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMinutelyInterval through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(MINUTELY, count=3, interval=2, dtstart=parse("19970902T090000"))),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1997, 9, 2, 9, 2),
                datetime(1997, 9, 2, 9, 4),
            ],
        )

    def testMinutelyIntervalLarge(self):
        """
        Perform the testMinutelyIntervalLarge utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMinutelyIntervalLarge through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(MINUTELY, count=3, interval=1501, dtstart=parse("19970902T090000"))),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1997, 9, 3, 10, 1),
                datetime(1997, 9, 4, 11, 2),
            ],
        )

    def testMinutelyByMonth(self):
        """
        Perform the testMinutelyByMonth utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMinutelyByMonth through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(MINUTELY, count=3, bymonth=(1, 3), dtstart=parse("19970902T090000"))),
            [
                datetime(1998, 1, 1, 0, 0),
                datetime(1998, 1, 1, 0, 1),
                datetime(1998, 1, 1, 0, 2),
            ],
        )

    def testMinutelyByMonthDay(self):
        """
        Perform the testMinutelyByMonthDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMinutelyByMonthDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    MINUTELY,
                    count=3,
                    bymonthday=(1, 3),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 9, 3, 0, 0),
                datetime(1997, 9, 3, 0, 1),
                datetime(1997, 9, 3, 0, 2),
            ],
        )

    def testMinutelyByMonthAndMonthDay(self):
        """
        Perform the testMinutelyByMonthAndMonthDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMinutelyByMonthAndMonthDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    MINUTELY,
                    count=3,
                    bymonth=(1, 3),
                    bymonthday=(5, 7),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 1, 5, 0, 0),
                datetime(1998, 1, 5, 0, 1),
                datetime(1998, 1, 5, 0, 2),
            ],
        )

    def testMinutelyByWeekDay(self):
        """
        Perform the testMinutelyByWeekDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMinutelyByWeekDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    MINUTELY,
                    count=3,
                    byweekday=(TU, TH),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1997, 9, 2, 9, 1),
                datetime(1997, 9, 2, 9, 2),
            ],
        )

    def testMinutelyByNWeekDay(self):
        """
        Perform the testMinutelyByNWeekDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMinutelyByNWeekDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    MINUTELY,
                    count=3,
                    byweekday=(TU(1), TH(-1)),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1997, 9, 2, 9, 1),
                datetime(1997, 9, 2, 9, 2),
            ],
        )

    def testMinutelyByMonthAndWeekDay(self):
        """
        Perform the testMinutelyByMonthAndWeekDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMinutelyByMonthAndWeekDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    MINUTELY,
                    count=3,
                    bymonth=(1, 3),
                    byweekday=(TU, TH),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 1, 1, 0, 0),
                datetime(1998, 1, 1, 0, 1),
                datetime(1998, 1, 1, 0, 2),
            ],
        )

    def testMinutelyByMonthAndNWeekDay(self):
        """
        Perform the testMinutelyByMonthAndNWeekDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMinutelyByMonthAndNWeekDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    MINUTELY,
                    count=3,
                    bymonth=(1, 3),
                    byweekday=(TU(1), TH(-1)),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 1, 1, 0, 0),
                datetime(1998, 1, 1, 0, 1),
                datetime(1998, 1, 1, 0, 2),
            ],
        )

    def testMinutelyByMonthDayAndWeekDay(self):
        """
        Perform the testMinutelyByMonthDayAndWeekDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMinutelyByMonthDayAndWeekDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    MINUTELY,
                    count=3,
                    bymonthday=(1, 3),
                    byweekday=(TU, TH),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 1, 1, 0, 0),
                datetime(1998, 1, 1, 0, 1),
                datetime(1998, 1, 1, 0, 2),
            ],
        )

    def testMinutelyByMonthAndMonthDayAndWeekDay(self):
        """
        Perform the testMinutelyByMonthAndMonthDayAndWeekDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMinutelyByMonthAndMonthDayAndWeekDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    MINUTELY,
                    count=3,
                    bymonth=(1, 3),
                    bymonthday=(1, 3),
                    byweekday=(TU, TH),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 1, 1, 0, 0),
                datetime(1998, 1, 1, 0, 1),
                datetime(1998, 1, 1, 0, 2),
            ],
        )

    def testMinutelyByYearDay(self):
        """
        Perform the testMinutelyByYearDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMinutelyByYearDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    MINUTELY,
                    count=4,
                    byyearday=(1, 100, 200, 365),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 12, 31, 0, 0),
                datetime(1997, 12, 31, 0, 1),
                datetime(1997, 12, 31, 0, 2),
                datetime(1997, 12, 31, 0, 3),
            ],
        )

    def testMinutelyByYearDayNeg(self):
        """
        Perform the testMinutelyByYearDayNeg utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMinutelyByYearDayNeg through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    MINUTELY,
                    count=4,
                    byyearday=(-365, -266, -166, -1),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 12, 31, 0, 0),
                datetime(1997, 12, 31, 0, 1),
                datetime(1997, 12, 31, 0, 2),
                datetime(1997, 12, 31, 0, 3),
            ],
        )

    def testMinutelyByMonthAndYearDay(self):
        """
        Perform the testMinutelyByMonthAndYearDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMinutelyByMonthAndYearDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    MINUTELY,
                    count=4,
                    bymonth=(4, 7),
                    byyearday=(1, 100, 200, 365),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 4, 10, 0, 0),
                datetime(1998, 4, 10, 0, 1),
                datetime(1998, 4, 10, 0, 2),
                datetime(1998, 4, 10, 0, 3),
            ],
        )

    def testMinutelyByMonthAndYearDayNeg(self):
        """
        Perform the testMinutelyByMonthAndYearDayNeg utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMinutelyByMonthAndYearDayNeg through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    MINUTELY,
                    count=4,
                    bymonth=(4, 7),
                    byyearday=(-365, -266, -166, -1),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 4, 10, 0, 0),
                datetime(1998, 4, 10, 0, 1),
                datetime(1998, 4, 10, 0, 2),
                datetime(1998, 4, 10, 0, 3),
            ],
        )

    def testMinutelyByWeekNo(self):
        """
        Perform the testMinutelyByWeekNo utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMinutelyByWeekNo through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(MINUTELY, count=3, byweekno=20, dtstart=parse("19970902T090000"))),
            [
                datetime(1998, 5, 11, 0, 0),
                datetime(1998, 5, 11, 0, 1),
                datetime(1998, 5, 11, 0, 2),
            ],
        )

    def testMinutelyByWeekNoAndWeekDay(self):
        """
        Perform the testMinutelyByWeekNoAndWeekDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMinutelyByWeekNoAndWeekDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    MINUTELY,
                    count=3,
                    byweekno=1,
                    byweekday=MO,
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 12, 29, 0, 0),
                datetime(1997, 12, 29, 0, 1),
                datetime(1997, 12, 29, 0, 2),
            ],
        )

    def testMinutelyByWeekNoAndWeekDayLarge(self):
        """
        Perform the testMinutelyByWeekNoAndWeekDayLarge utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMinutelyByWeekNoAndWeekDayLarge through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    MINUTELY,
                    count=3,
                    byweekno=52,
                    byweekday=SU,
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 12, 28, 0, 0),
                datetime(1997, 12, 28, 0, 1),
                datetime(1997, 12, 28, 0, 2),
            ],
        )

    def testMinutelyByWeekNoAndWeekDayLast(self):
        """
        Perform the testMinutelyByWeekNoAndWeekDayLast utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMinutelyByWeekNoAndWeekDayLast through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    MINUTELY,
                    count=3,
                    byweekno=-1,
                    byweekday=SU,
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 12, 28, 0, 0),
                datetime(1997, 12, 28, 0, 1),
                datetime(1997, 12, 28, 0, 2),
            ],
        )

    def testMinutelyByWeekNoAndWeekDay53(self):
        """
        Perform the testMinutelyByWeekNoAndWeekDay53 utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMinutelyByWeekNoAndWeekDay53 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    MINUTELY,
                    count=3,
                    byweekno=53,
                    byweekday=MO,
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 12, 28, 0, 0),
                datetime(1998, 12, 28, 0, 1),
                datetime(1998, 12, 28, 0, 2),
            ],
        )

    def testMinutelyByEaster(self):
        """
        Perform the testMinutelyByEaster utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMinutelyByEaster through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(MINUTELY, count=3, byeaster=0, dtstart=parse("19970902T090000"))),
            [
                datetime(1998, 4, 12, 0, 0),
                datetime(1998, 4, 12, 0, 1),
                datetime(1998, 4, 12, 0, 2),
            ],
        )

    def testMinutelyByEasterPos(self):
        """
        Perform the testMinutelyByEasterPos utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMinutelyByEasterPos through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(MINUTELY, count=3, byeaster=1, dtstart=parse("19970902T090000"))),
            [
                datetime(1998, 4, 13, 0, 0),
                datetime(1998, 4, 13, 0, 1),
                datetime(1998, 4, 13, 0, 2),
            ],
        )

    def testMinutelyByEasterNeg(self):
        """
        Perform the testMinutelyByEasterNeg utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMinutelyByEasterNeg through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(MINUTELY, count=3, byeaster=-1, dtstart=parse("19970902T090000"))),
            [
                datetime(1998, 4, 11, 0, 0),
                datetime(1998, 4, 11, 0, 1),
                datetime(1998, 4, 11, 0, 2),
            ],
        )

    def testMinutelyByHour(self):
        """
        Perform the testMinutelyByHour utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMinutelyByHour through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(MINUTELY, count=3, byhour=(6, 18), dtstart=parse("19970902T090000"))),
            [
                datetime(1997, 9, 2, 18, 0),
                datetime(1997, 9, 2, 18, 1),
                datetime(1997, 9, 2, 18, 2),
            ],
        )

    def testMinutelyByMinute(self):
        """
        Perform the testMinutelyByMinute utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMinutelyByMinute through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    MINUTELY,
                    count=3,
                    byminute=(6, 18),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 9, 2, 9, 6),
                datetime(1997, 9, 2, 9, 18),
                datetime(1997, 9, 2, 10, 6),
            ],
        )

    def testMinutelyBySecond(self):
        """
        Perform the testMinutelyBySecond utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMinutelyBySecond through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    MINUTELY,
                    count=3,
                    bysecond=(6, 18),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 9, 2, 9, 0, 6),
                datetime(1997, 9, 2, 9, 0, 18),
                datetime(1997, 9, 2, 9, 1, 6),
            ],
        )

    def testMinutelyByHourAndMinute(self):
        """
        Perform the testMinutelyByHourAndMinute utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMinutelyByHourAndMinute through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    MINUTELY,
                    count=3,
                    byhour=(6, 18),
                    byminute=(6, 18),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 9, 2, 18, 6),
                datetime(1997, 9, 2, 18, 18),
                datetime(1997, 9, 3, 6, 6),
            ],
        )

    def testMinutelyByHourAndSecond(self):
        """
        Perform the testMinutelyByHourAndSecond utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMinutelyByHourAndSecond through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    MINUTELY,
                    count=3,
                    byhour=(6, 18),
                    bysecond=(6, 18),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 9, 2, 18, 0, 6),
                datetime(1997, 9, 2, 18, 0, 18),
                datetime(1997, 9, 2, 18, 1, 6),
            ],
        )

    def testMinutelyByMinuteAndSecond(self):
        """
        Perform the testMinutelyByMinuteAndSecond utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMinutelyByMinuteAndSecond through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    MINUTELY,
                    count=3,
                    byminute=(6, 18),
                    bysecond=(6, 18),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 9, 2, 9, 6, 6),
                datetime(1997, 9, 2, 9, 6, 18),
                datetime(1997, 9, 2, 9, 18, 6),
            ],
        )

    def testMinutelyByHourAndMinuteAndSecond(self):
        """
        Perform the testMinutelyByHourAndMinuteAndSecond utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMinutelyByHourAndMinuteAndSecond through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    MINUTELY,
                    count=3,
                    byhour=(6, 18),
                    byminute=(6, 18),
                    bysecond=(6, 18),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 9, 2, 18, 6, 6),
                datetime(1997, 9, 2, 18, 6, 18),
                datetime(1997, 9, 2, 18, 18, 6),
            ],
        )

    def testMinutelyBySetPos(self):
        """
        Perform the testMinutelyBySetPos utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMinutelyBySetPos through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    MINUTELY,
                    count=3,
                    bysecond=(15, 30, 45),
                    bysetpos=(3, -3),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 9, 2, 9, 0, 15),
                datetime(1997, 9, 2, 9, 0, 45),
                datetime(1997, 9, 2, 9, 1, 15),
            ],
        )

    def testSecondly(self):
        """
        Perform the testSecondly utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testSecondly through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(SECONDLY, count=3, dtstart=parse("19970902T090000"))),
            [
                datetime(1997, 9, 2, 9, 0, 0),
                datetime(1997, 9, 2, 9, 0, 1),
                datetime(1997, 9, 2, 9, 0, 2),
            ],
        )

    def testSecondlyInterval(self):
        """
        Perform the testSecondlyInterval utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testSecondlyInterval through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(SECONDLY, count=3, interval=2, dtstart=parse("19970902T090000"))),
            [
                datetime(1997, 9, 2, 9, 0, 0),
                datetime(1997, 9, 2, 9, 0, 2),
                datetime(1997, 9, 2, 9, 0, 4),
            ],
        )

    def testSecondlyIntervalLarge(self):
        """
        Perform the testSecondlyIntervalLarge utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testSecondlyIntervalLarge through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(SECONDLY, count=3, interval=90061, dtstart=parse("19970902T090000"))),
            [
                datetime(1997, 9, 2, 9, 0, 0),
                datetime(1997, 9, 3, 10, 1, 1),
                datetime(1997, 9, 4, 11, 2, 2),
            ],
        )

    def testSecondlyByMonth(self):
        """
        Perform the testSecondlyByMonth utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testSecondlyByMonth through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(SECONDLY, count=3, bymonth=(1, 3), dtstart=parse("19970902T090000"))),
            [
                datetime(1998, 1, 1, 0, 0, 0),
                datetime(1998, 1, 1, 0, 0, 1),
                datetime(1998, 1, 1, 0, 0, 2),
            ],
        )

    def testSecondlyByMonthDay(self):
        """
        Perform the testSecondlyByMonthDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testSecondlyByMonthDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    SECONDLY,
                    count=3,
                    bymonthday=(1, 3),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 9, 3, 0, 0, 0),
                datetime(1997, 9, 3, 0, 0, 1),
                datetime(1997, 9, 3, 0, 0, 2),
            ],
        )

    def testSecondlyByMonthAndMonthDay(self):
        """
        Perform the testSecondlyByMonthAndMonthDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testSecondlyByMonthAndMonthDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    SECONDLY,
                    count=3,
                    bymonth=(1, 3),
                    bymonthday=(5, 7),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 1, 5, 0, 0, 0),
                datetime(1998, 1, 5, 0, 0, 1),
                datetime(1998, 1, 5, 0, 0, 2),
            ],
        )

    def testSecondlyByWeekDay(self):
        """
        Perform the testSecondlyByWeekDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testSecondlyByWeekDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    SECONDLY,
                    count=3,
                    byweekday=(TU, TH),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 9, 2, 9, 0, 0),
                datetime(1997, 9, 2, 9, 0, 1),
                datetime(1997, 9, 2, 9, 0, 2),
            ],
        )

    def testSecondlyByNWeekDay(self):
        """
        Perform the testSecondlyByNWeekDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testSecondlyByNWeekDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    SECONDLY,
                    count=3,
                    byweekday=(TU(1), TH(-1)),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 9, 2, 9, 0, 0),
                datetime(1997, 9, 2, 9, 0, 1),
                datetime(1997, 9, 2, 9, 0, 2),
            ],
        )

    def testSecondlyByMonthAndWeekDay(self):
        """
        Perform the testSecondlyByMonthAndWeekDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testSecondlyByMonthAndWeekDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    SECONDLY,
                    count=3,
                    bymonth=(1, 3),
                    byweekday=(TU, TH),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 1, 1, 0, 0, 0),
                datetime(1998, 1, 1, 0, 0, 1),
                datetime(1998, 1, 1, 0, 0, 2),
            ],
        )

    def testSecondlyByMonthAndNWeekDay(self):
        """
        Perform the testSecondlyByMonthAndNWeekDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testSecondlyByMonthAndNWeekDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    SECONDLY,
                    count=3,
                    bymonth=(1, 3),
                    byweekday=(TU(1), TH(-1)),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 1, 1, 0, 0, 0),
                datetime(1998, 1, 1, 0, 0, 1),
                datetime(1998, 1, 1, 0, 0, 2),
            ],
        )

    def testSecondlyByMonthDayAndWeekDay(self):
        """
        Perform the testSecondlyByMonthDayAndWeekDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testSecondlyByMonthDayAndWeekDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    SECONDLY,
                    count=3,
                    bymonthday=(1, 3),
                    byweekday=(TU, TH),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 1, 1, 0, 0, 0),
                datetime(1998, 1, 1, 0, 0, 1),
                datetime(1998, 1, 1, 0, 0, 2),
            ],
        )

    def testSecondlyByMonthAndMonthDayAndWeekDay(self):
        """
        Perform the testSecondlyByMonthAndMonthDayAndWeekDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testSecondlyByMonthAndMonthDayAndWeekDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    SECONDLY,
                    count=3,
                    bymonth=(1, 3),
                    bymonthday=(1, 3),
                    byweekday=(TU, TH),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 1, 1, 0, 0, 0),
                datetime(1998, 1, 1, 0, 0, 1),
                datetime(1998, 1, 1, 0, 0, 2),
            ],
        )

    def testSecondlyByYearDay(self):
        """
        Perform the testSecondlyByYearDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testSecondlyByYearDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    SECONDLY,
                    count=4,
                    byyearday=(1, 100, 200, 365),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 12, 31, 0, 0, 0),
                datetime(1997, 12, 31, 0, 0, 1),
                datetime(1997, 12, 31, 0, 0, 2),
                datetime(1997, 12, 31, 0, 0, 3),
            ],
        )

    def testSecondlyByYearDayNeg(self):
        """
        Perform the testSecondlyByYearDayNeg utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testSecondlyByYearDayNeg through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    SECONDLY,
                    count=4,
                    byyearday=(-365, -266, -166, -1),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 12, 31, 0, 0, 0),
                datetime(1997, 12, 31, 0, 0, 1),
                datetime(1997, 12, 31, 0, 0, 2),
                datetime(1997, 12, 31, 0, 0, 3),
            ],
        )

    def testSecondlyByMonthAndYearDay(self):
        """
        Perform the testSecondlyByMonthAndYearDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testSecondlyByMonthAndYearDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    SECONDLY,
                    count=4,
                    bymonth=(4, 7),
                    byyearday=(1, 100, 200, 365),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 4, 10, 0, 0, 0),
                datetime(1998, 4, 10, 0, 0, 1),
                datetime(1998, 4, 10, 0, 0, 2),
                datetime(1998, 4, 10, 0, 0, 3),
            ],
        )

    def testSecondlyByMonthAndYearDayNeg(self):
        """
        Perform the testSecondlyByMonthAndYearDayNeg utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testSecondlyByMonthAndYearDayNeg through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    SECONDLY,
                    count=4,
                    bymonth=(4, 7),
                    byyearday=(-365, -266, -166, -1),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 4, 10, 0, 0, 0),
                datetime(1998, 4, 10, 0, 0, 1),
                datetime(1998, 4, 10, 0, 0, 2),
                datetime(1998, 4, 10, 0, 0, 3),
            ],
        )

    def testSecondlyByWeekNo(self):
        """
        Perform the testSecondlyByWeekNo utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testSecondlyByWeekNo through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(SECONDLY, count=3, byweekno=20, dtstart=parse("19970902T090000"))),
            [
                datetime(1998, 5, 11, 0, 0, 0),
                datetime(1998, 5, 11, 0, 0, 1),
                datetime(1998, 5, 11, 0, 0, 2),
            ],
        )

    def testSecondlyByWeekNoAndWeekDay(self):
        """
        Perform the testSecondlyByWeekNoAndWeekDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testSecondlyByWeekNoAndWeekDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    SECONDLY,
                    count=3,
                    byweekno=1,
                    byweekday=MO,
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 12, 29, 0, 0, 0),
                datetime(1997, 12, 29, 0, 0, 1),
                datetime(1997, 12, 29, 0, 0, 2),
            ],
        )

    def testSecondlyByWeekNoAndWeekDayLarge(self):
        """
        Perform the testSecondlyByWeekNoAndWeekDayLarge utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testSecondlyByWeekNoAndWeekDayLarge through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    SECONDLY,
                    count=3,
                    byweekno=52,
                    byweekday=SU,
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 12, 28, 0, 0, 0),
                datetime(1997, 12, 28, 0, 0, 1),
                datetime(1997, 12, 28, 0, 0, 2),
            ],
        )

    def testSecondlyByWeekNoAndWeekDayLast(self):
        """
        Perform the testSecondlyByWeekNoAndWeekDayLast utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testSecondlyByWeekNoAndWeekDayLast through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    SECONDLY,
                    count=3,
                    byweekno=-1,
                    byweekday=SU,
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 12, 28, 0, 0, 0),
                datetime(1997, 12, 28, 0, 0, 1),
                datetime(1997, 12, 28, 0, 0, 2),
            ],
        )

    def testSecondlyByWeekNoAndWeekDay53(self):
        """
        Perform the testSecondlyByWeekNoAndWeekDay53 utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testSecondlyByWeekNoAndWeekDay53 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    SECONDLY,
                    count=3,
                    byweekno=53,
                    byweekday=MO,
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1998, 12, 28, 0, 0, 0),
                datetime(1998, 12, 28, 0, 0, 1),
                datetime(1998, 12, 28, 0, 0, 2),
            ],
        )

    def testSecondlyByEaster(self):
        """
        Perform the testSecondlyByEaster utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testSecondlyByEaster through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(SECONDLY, count=3, byeaster=0, dtstart=parse("19970902T090000"))),
            [
                datetime(1998, 4, 12, 0, 0, 0),
                datetime(1998, 4, 12, 0, 0, 1),
                datetime(1998, 4, 12, 0, 0, 2),
            ],
        )

    def testSecondlyByEasterPos(self):
        """
        Perform the testSecondlyByEasterPos utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testSecondlyByEasterPos through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(SECONDLY, count=3, byeaster=1, dtstart=parse("19970902T090000"))),
            [
                datetime(1998, 4, 13, 0, 0, 0),
                datetime(1998, 4, 13, 0, 0, 1),
                datetime(1998, 4, 13, 0, 0, 2),
            ],
        )

    def testSecondlyByEasterNeg(self):
        """
        Perform the testSecondlyByEasterNeg utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testSecondlyByEasterNeg through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(SECONDLY, count=3, byeaster=-1, dtstart=parse("19970902T090000"))),
            [
                datetime(1998, 4, 11, 0, 0, 0),
                datetime(1998, 4, 11, 0, 0, 1),
                datetime(1998, 4, 11, 0, 0, 2),
            ],
        )

    def testSecondlyByHour(self):
        """
        Perform the testSecondlyByHour utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testSecondlyByHour through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(SECONDLY, count=3, byhour=(6, 18), dtstart=parse("19970902T090000"))),
            [
                datetime(1997, 9, 2, 18, 0, 0),
                datetime(1997, 9, 2, 18, 0, 1),
                datetime(1997, 9, 2, 18, 0, 2),
            ],
        )

    def testSecondlyByMinute(self):
        """
        Perform the testSecondlyByMinute utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testSecondlyByMinute through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    SECONDLY,
                    count=3,
                    byminute=(6, 18),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 9, 2, 9, 6, 0),
                datetime(1997, 9, 2, 9, 6, 1),
                datetime(1997, 9, 2, 9, 6, 2),
            ],
        )

    def testSecondlyBySecond(self):
        """
        Perform the testSecondlyBySecond utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testSecondlyBySecond through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    SECONDLY,
                    count=3,
                    bysecond=(6, 18),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 9, 2, 9, 0, 6),
                datetime(1997, 9, 2, 9, 0, 18),
                datetime(1997, 9, 2, 9, 1, 6),
            ],
        )

    def testSecondlyByHourAndMinute(self):
        """
        Perform the testSecondlyByHourAndMinute utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testSecondlyByHourAndMinute through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    SECONDLY,
                    count=3,
                    byhour=(6, 18),
                    byminute=(6, 18),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 9, 2, 18, 6, 0),
                datetime(1997, 9, 2, 18, 6, 1),
                datetime(1997, 9, 2, 18, 6, 2),
            ],
        )

    def testSecondlyByHourAndSecond(self):
        """
        Perform the testSecondlyByHourAndSecond utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testSecondlyByHourAndSecond through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    SECONDLY,
                    count=3,
                    byhour=(6, 18),
                    bysecond=(6, 18),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 9, 2, 18, 0, 6),
                datetime(1997, 9, 2, 18, 0, 18),
                datetime(1997, 9, 2, 18, 1, 6),
            ],
        )

    def testSecondlyByMinuteAndSecond(self):
        """
        Perform the testSecondlyByMinuteAndSecond utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testSecondlyByMinuteAndSecond through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    SECONDLY,
                    count=3,
                    byminute=(6, 18),
                    bysecond=(6, 18),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 9, 2, 9, 6, 6),
                datetime(1997, 9, 2, 9, 6, 18),
                datetime(1997, 9, 2, 9, 18, 6),
            ],
        )

    def testSecondlyByHourAndMinuteAndSecond(self):
        """
        Perform the testSecondlyByHourAndMinuteAndSecond utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testSecondlyByHourAndMinuteAndSecond through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    SECONDLY,
                    count=3,
                    byhour=(6, 18),
                    byminute=(6, 18),
                    bysecond=(6, 18),
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 9, 2, 18, 6, 6),
                datetime(1997, 9, 2, 18, 6, 18),
                datetime(1997, 9, 2, 18, 18, 6),
            ],
        )

    def testSecondlyByHourAndMinuteAndSecondBug(self):
        # This explores a bug found by Mathieu Bridon.
        """
        Perform the testSecondlyByHourAndMinuteAndSecondBug utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testSecondlyByHourAndMinuteAndSecondBug through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    SECONDLY,
                    count=3,
                    bysecond=(0,),
                    byminute=(1,),
                    dtstart=parse("20100322120100"),
                )
            ),
            [
                datetime(2010, 3, 22, 12, 1),
                datetime(2010, 3, 22, 13, 1),
                datetime(2010, 3, 22, 14, 1),
            ],
        )

    def testUntilNotMatching(self):
        """
        Perform the testUntilNotMatching utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testUntilNotMatching through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    DAILY,
                    count=3,
                    dtstart=parse("19970902T090000"),
                    until=parse("19970905T080000"),
                )
            ),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1997, 9, 3, 9, 0),
                datetime(1997, 9, 4, 9, 0),
            ],
        )

    def testUntilMatching(self):
        """
        Perform the testUntilMatching utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testUntilMatching through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    DAILY,
                    count=3,
                    dtstart=parse("19970902T090000"),
                    until=parse("19970904T090000"),
                )
            ),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1997, 9, 3, 9, 0),
                datetime(1997, 9, 4, 9, 0),
            ],
        )

    def testUntilSingle(self):
        """
        Perform the testUntilSingle utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testUntilSingle through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    DAILY,
                    count=3,
                    dtstart=parse("19970902T090000"),
                    until=parse("19970902T090000"),
                )
            ),
            [datetime(1997, 9, 2, 9, 0)],
        )

    def testUntilEmpty(self):
        """
        Perform the testUntilEmpty utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testUntilEmpty through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    DAILY,
                    count=3,
                    dtstart=parse("19970902T090000"),
                    until=parse("19970901T090000"),
                )
            ),
            [],
        )

    def testUntilWithDate(self):
        """
        Perform the testUntilWithDate utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testUntilWithDate through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    DAILY,
                    count=3,
                    dtstart=parse("19970902T090000"),
                    until=date(1997, 9, 5),
                )
            ),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1997, 9, 3, 9, 0),
                datetime(1997, 9, 4, 9, 0),
            ],
        )

    def testWkStIntervalMO(self):
        """
        Perform the testWkStIntervalMO utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testWkStIntervalMO through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    WEEKLY,
                    count=3,
                    interval=2,
                    byweekday=(TU, SU),
                    wkst=MO,
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1997, 9, 7, 9, 0),
                datetime(1997, 9, 16, 9, 0),
            ],
        )

    def testWkStIntervalSU(self):
        """
        Perform the testWkStIntervalSU utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testWkStIntervalSU through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    WEEKLY,
                    count=3,
                    interval=2,
                    byweekday=(TU, SU),
                    wkst=SU,
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1997, 9, 14, 9, 0),
                datetime(1997, 9, 16, 9, 0),
            ],
        )

    def testDTStartIsDate(self):
        """
        Perform the testDTStartIsDate utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testDTStartIsDate through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(DAILY, count=3, dtstart=date(1997, 9, 2))),
            [
                datetime(1997, 9, 2, 0, 0),
                datetime(1997, 9, 3, 0, 0),
                datetime(1997, 9, 4, 0, 0),
            ],
        )

    def testDTStartWithMicroseconds(self):
        """
        Perform the testDTStartWithMicroseconds utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testDTStartWithMicroseconds through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrule(DAILY, count=3, dtstart=parse("19970902T090000.5"))),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1997, 9, 3, 9, 0),
                datetime(1997, 9, 4, 9, 0),
            ],
        )

    def testMaxYear(self):
        """
        Perform the testMaxYear utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testMaxYear through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrule(
                    YEARLY,
                    count=3,
                    bymonth=2,
                    bymonthday=31,
                    dtstart=parse("99970902T090000"),
                )
            ),
            [],
        )

    def testGetItem(self):
        """
        Perform the testGetItem utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testGetItem through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            rrule(DAILY, count=3, dtstart=parse("19970902T090000"))[0],
            datetime(1997, 9, 2, 9, 0),
        )

    def testGetItemNeg(self):
        """
        Perform the testGetItemNeg utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testGetItemNeg through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            rrule(DAILY, count=3, dtstart=parse("19970902T090000"))[-1],
            datetime(1997, 9, 4, 9, 0),
        )

    def testGetItemSlice(self):
        """
        Perform the testGetItemSlice utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testGetItemSlice through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            rrule(
                DAILY,
                # count=3,
                dtstart=parse("19970902T090000"),
            )[1:2],
            [datetime(1997, 9, 3, 9, 0)],
        )

    def testGetItemSliceEmpty(self):
        """
        Perform the testGetItemSliceEmpty utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testGetItemSliceEmpty through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            rrule(DAILY, count=3, dtstart=parse("19970902T090000"))[:],
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1997, 9, 3, 9, 0),
                datetime(1997, 9, 4, 9, 0),
            ],
        )

    def testGetItemSliceStep(self):
        """
        Perform the testGetItemSliceStep utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testGetItemSliceStep through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            rrule(DAILY, count=3, dtstart=parse("19970902T090000"))[::-2],
            [datetime(1997, 9, 4, 9, 0), datetime(1997, 9, 2, 9, 0)],
        )

    def testCount(self):
        """
        Perform the testCount utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testCount through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(rrule(DAILY, count=3, dtstart=parse("19970902T090000")).count(), 3)

    def testContains(self):
        """
        Perform the testContains utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testContains through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        rr = rrule(DAILY, count=3, dtstart=parse("19970902T090000"))
        self.assertEqual(datetime(1997, 9, 3, 9, 0) in rr, True)

    def testContainsNot(self):
        """
        Perform the testContainsNot utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testContainsNot through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        rr = rrule(DAILY, count=3, dtstart=parse("19970902T090000"))
        self.assertEqual(datetime(1997, 9, 3, 9, 0) not in rr, False)

    def testBefore(self):
        """
        Perform the testBefore utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testBefore through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            rrule(
                DAILY,
                # count=5,
                dtstart=parse("19970902T090000"),
            ).before(parse("19970905T090000")),
            datetime(1997, 9, 4, 9, 0),
        )

    def testBeforeInc(self):
        """
        Perform the testBeforeInc utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testBeforeInc through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            rrule(
                DAILY,
                # count=5,
                dtstart=parse("19970902T090000"),
            ).before(parse("19970905T090000"), inc=True),
            datetime(1997, 9, 5, 9, 0),
        )

    def testAfter(self):
        """
        Perform the testAfter utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testAfter through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            rrule(
                DAILY,
                # count=5,
                dtstart=parse("19970902T090000"),
            ).after(parse("19970904T090000")),
            datetime(1997, 9, 5, 9, 0),
        )

    def testAfterInc(self):
        """
        Perform the testAfterInc utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testAfterInc through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            rrule(
                DAILY,
                # count=5,
                dtstart=parse("19970902T090000"),
            ).after(parse("19970904T090000"), inc=True),
            datetime(1997, 9, 4, 9, 0),
        )

    def testBetween(self):
        """
        Perform the testBetween utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testBetween through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            rrule(
                DAILY,
                # count=5,
                dtstart=parse("19970902T090000"),
            ).between(parse("19970902T090000"), parse("19970906T090000")),
            [
                datetime(1997, 9, 3, 9, 0),
                datetime(1997, 9, 4, 9, 0),
                datetime(1997, 9, 5, 9, 0),
            ],
        )

    def testBetweenInc(self):
        """
        Perform the testBetweenInc utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testBetweenInc through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            rrule(
                DAILY,
                # count=5,
                dtstart=parse("19970902T090000"),
            ).between(parse("19970902T090000"), parse("19970906T090000"), inc=True),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1997, 9, 3, 9, 0),
                datetime(1997, 9, 4, 9, 0),
                datetime(1997, 9, 5, 9, 0),
                datetime(1997, 9, 6, 9, 0),
            ],
        )

    def testCachePre(self):
        """
        Perform the testCachePre utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testCachePre through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        rr = rrule(DAILY, count=15, cache=True, dtstart=parse("19970902T090000"))
        self.assertEqual(
            list(rr),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1997, 9, 3, 9, 0),
                datetime(1997, 9, 4, 9, 0),
                datetime(1997, 9, 5, 9, 0),
                datetime(1997, 9, 6, 9, 0),
                datetime(1997, 9, 7, 9, 0),
                datetime(1997, 9, 8, 9, 0),
                datetime(1997, 9, 9, 9, 0),
                datetime(1997, 9, 10, 9, 0),
                datetime(1997, 9, 11, 9, 0),
                datetime(1997, 9, 12, 9, 0),
                datetime(1997, 9, 13, 9, 0),
                datetime(1997, 9, 14, 9, 0),
                datetime(1997, 9, 15, 9, 0),
                datetime(1997, 9, 16, 9, 0),
            ],
        )

    def testCachePost(self):
        """
        Perform the testCachePost utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testCachePost through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        rr = rrule(DAILY, count=15, cache=True, dtstart=parse("19970902T090000"))
        for x in rr:
            pass
        self.assertEqual(
            list(rr),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1997, 9, 3, 9, 0),
                datetime(1997, 9, 4, 9, 0),
                datetime(1997, 9, 5, 9, 0),
                datetime(1997, 9, 6, 9, 0),
                datetime(1997, 9, 7, 9, 0),
                datetime(1997, 9, 8, 9, 0),
                datetime(1997, 9, 9, 9, 0),
                datetime(1997, 9, 10, 9, 0),
                datetime(1997, 9, 11, 9, 0),
                datetime(1997, 9, 12, 9, 0),
                datetime(1997, 9, 13, 9, 0),
                datetime(1997, 9, 14, 9, 0),
                datetime(1997, 9, 15, 9, 0),
                datetime(1997, 9, 16, 9, 0),
            ],
        )

    def testCachePostInternal(self):
        """
        Perform the testCachePostInternal utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testCachePostInternal through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        rr = rrule(DAILY, count=15, cache=True, dtstart=parse("19970902T090000"))
        for x in rr:
            pass
        self.assertEqual(
            rr._cache,
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1997, 9, 3, 9, 0),
                datetime(1997, 9, 4, 9, 0),
                datetime(1997, 9, 5, 9, 0),
                datetime(1997, 9, 6, 9, 0),
                datetime(1997, 9, 7, 9, 0),
                datetime(1997, 9, 8, 9, 0),
                datetime(1997, 9, 9, 9, 0),
                datetime(1997, 9, 10, 9, 0),
                datetime(1997, 9, 11, 9, 0),
                datetime(1997, 9, 12, 9, 0),
                datetime(1997, 9, 13, 9, 0),
                datetime(1997, 9, 14, 9, 0),
                datetime(1997, 9, 15, 9, 0),
                datetime(1997, 9, 16, 9, 0),
            ],
        )

    def testCachePreContains(self):
        """
        Perform the testCachePreContains utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testCachePreContains through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        rr = rrule(DAILY, count=3, cache=True, dtstart=parse("19970902T090000"))
        self.assertEqual(datetime(1997, 9, 3, 9, 0) in rr, True)

    def testCachePostContains(self):
        """
        Perform the testCachePostContains utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testCachePostContains through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        rr = rrule(DAILY, count=3, cache=True, dtstart=parse("19970902T090000"))
        for x in rr:
            pass
        self.assertEqual(datetime(1997, 9, 3, 9, 0) in rr, True)

    def testSet(self):
        """
        Perform the testSet utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testSet through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        set = rruleset()
        set.rrule(rrule(YEARLY, count=2, byweekday=TU, dtstart=parse("19970902T090000")))
        set.rrule(rrule(YEARLY, count=1, byweekday=TH, dtstart=parse("19970902T090000")))
        self.assertEqual(
            list(set),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1997, 9, 4, 9, 0),
                datetime(1997, 9, 9, 9, 0),
            ],
        )

    def testSetDate(self):
        """
        Perform the testSetDate utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testSetDate through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        set = rruleset()
        set.rrule(rrule(YEARLY, count=1, byweekday=TU, dtstart=parse("19970902T090000")))
        set.rdate(datetime(1997, 9, 4, 9))
        set.rdate(datetime(1997, 9, 9, 9))
        self.assertEqual(
            list(set),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1997, 9, 4, 9, 0),
                datetime(1997, 9, 9, 9, 0),
            ],
        )

    def testSetExRule(self):
        """
        Perform the testSetExRule utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testSetExRule through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        set = rruleset()
        set.rrule(rrule(YEARLY, count=6, byweekday=(TU, TH), dtstart=parse("19970902T090000")))
        set.exrule(rrule(YEARLY, count=3, byweekday=TH, dtstart=parse("19970902T090000")))
        self.assertEqual(
            list(set),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1997, 9, 9, 9, 0),
                datetime(1997, 9, 16, 9, 0),
            ],
        )

    def testSetExDate(self):
        """
        Perform the testSetExDate utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testSetExDate through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        set = rruleset()
        set.rrule(rrule(YEARLY, count=6, byweekday=(TU, TH), dtstart=parse("19970902T090000")))
        set.exdate(datetime(1997, 9, 4, 9))
        set.exdate(datetime(1997, 9, 11, 9))
        set.exdate(datetime(1997, 9, 18, 9))
        self.assertEqual(
            list(set),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1997, 9, 9, 9, 0),
                datetime(1997, 9, 16, 9, 0),
            ],
        )

    def testSetExDateRevOrder(self):
        """
        Perform the testSetExDateRevOrder utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testSetExDateRevOrder through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        set = rruleset()
        set.rrule(rrule(MONTHLY, count=5, bymonthday=10, dtstart=parse("20040101T090000")))
        set.exdate(datetime(2004, 4, 10, 9, 0))
        set.exdate(datetime(2004, 2, 10, 9, 0))
        self.assertEqual(
            list(set),
            [
                datetime(2004, 1, 10, 9, 0),
                datetime(2004, 3, 10, 9, 0),
                datetime(2004, 5, 10, 9, 0),
            ],
        )

    def testSetDateAndExDate(self):
        """
        Perform the testSetDateAndExDate utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testSetDateAndExDate through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        set = rruleset()
        set.rdate(datetime(1997, 9, 2, 9))
        set.rdate(datetime(1997, 9, 4, 9))
        set.rdate(datetime(1997, 9, 9, 9))
        set.rdate(datetime(1997, 9, 11, 9))
        set.rdate(datetime(1997, 9, 16, 9))
        set.rdate(datetime(1997, 9, 18, 9))
        set.exdate(datetime(1997, 9, 4, 9))
        set.exdate(datetime(1997, 9, 11, 9))
        set.exdate(datetime(1997, 9, 18, 9))
        self.assertEqual(
            list(set),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1997, 9, 9, 9, 0),
                datetime(1997, 9, 16, 9, 0),
            ],
        )

    def testSetDateAndExRule(self):
        """
        Perform the testSetDateAndExRule utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testSetDateAndExRule through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        set = rruleset()
        set.rdate(datetime(1997, 9, 2, 9))
        set.rdate(datetime(1997, 9, 4, 9))
        set.rdate(datetime(1997, 9, 9, 9))
        set.rdate(datetime(1997, 9, 11, 9))
        set.rdate(datetime(1997, 9, 16, 9))
        set.rdate(datetime(1997, 9, 18, 9))
        set.exrule(rrule(YEARLY, count=3, byweekday=TH, dtstart=parse("19970902T090000")))
        self.assertEqual(
            list(set),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1997, 9, 9, 9, 0),
                datetime(1997, 9, 16, 9, 0),
            ],
        )

    def testSetCount(self):
        """
        Perform the testSetCount utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testSetCount through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        set = rruleset()
        set.rrule(rrule(YEARLY, count=6, byweekday=(TU, TH), dtstart=parse("19970902T090000")))
        set.exrule(rrule(YEARLY, count=3, byweekday=TH, dtstart=parse("19970902T090000")))
        self.assertEqual(set.count(), 3)

    def testSetCachePre(self):
        """
        Perform the testSetCachePre utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testSetCachePre through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        set = rruleset()
        set.rrule(rrule(YEARLY, count=2, byweekday=TU, dtstart=parse("19970902T090000")))
        set.rrule(rrule(YEARLY, count=1, byweekday=TH, dtstart=parse("19970902T090000")))
        self.assertEqual(
            list(set),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1997, 9, 4, 9, 0),
                datetime(1997, 9, 9, 9, 0),
            ],
        )

    def testSetCachePost(self):
        """
        Perform the testSetCachePost utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testSetCachePost through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        set = rruleset(cache=True)
        set.rrule(rrule(YEARLY, count=2, byweekday=TU, dtstart=parse("19970902T090000")))
        set.rrule(rrule(YEARLY, count=1, byweekday=TH, dtstart=parse("19970902T090000")))
        for x in set:
            pass
        self.assertEqual(
            list(set),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1997, 9, 4, 9, 0),
                datetime(1997, 9, 9, 9, 0),
            ],
        )

    def testSetCachePostInternal(self):
        """
        Perform the testSetCachePostInternal utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testSetCachePostInternal through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        set = rruleset(cache=True)
        set.rrule(rrule(YEARLY, count=2, byweekday=TU, dtstart=parse("19970902T090000")))
        set.rrule(rrule(YEARLY, count=1, byweekday=TH, dtstart=parse("19970902T090000")))
        for x in set:
            pass
        self.assertEqual(
            list(set._cache),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1997, 9, 4, 9, 0),
                datetime(1997, 9, 9, 9, 0),
            ],
        )

    def testStr(self):
        """
        Perform the testStr utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testStr through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrulestr("DTSTART:19970902T090000\n" "RRULE:FREQ=YEARLY;COUNT=3\n")),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1998, 9, 2, 9, 0),
                datetime(1999, 9, 2, 9, 0),
            ],
        )

    def testStrType(self):
        """
        Perform the testStrType utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testStrType through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            isinstance(
                rrulestr("DTSTART:19970902T090000\n" "RRULE:FREQ=YEARLY;COUNT=3\n"),
                rrule,
            ),
            True,
        )

    def testStrForceSetType(self):
        """
        Perform the testStrForceSetType utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testStrForceSetType through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            isinstance(
                rrulestr(
                    "DTSTART:19970902T090000\n" "RRULE:FREQ=YEARLY;COUNT=3\n",
                    forceset=True,
                ),
                rruleset,
            ),
            True,
        )

    def testStrSetType(self):
        """
        Perform the testStrSetType utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testStrSetType through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            isinstance(
                rrulestr(
                    "DTSTART:19970902T090000\n"
                    "RRULE:FREQ=YEARLY;COUNT=2;BYDAY=TU\n"
                    "RRULE:FREQ=YEARLY;COUNT=1;BYDAY=TH\n"
                ),
                rruleset,
            ),
            True,
        )

    def testStrCase(self):
        """
        Perform the testStrCase utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testStrCase through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrulestr("dtstart:19970902T090000\n" "rrule:freq=yearly;count=3\n")),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1998, 9, 2, 9, 0),
                datetime(1999, 9, 2, 9, 0),
            ],
        )

    def testStrSpaces(self):
        """
        Perform the testStrSpaces utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testStrSpaces through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrulestr(" DTSTART:19970902T090000 " " RRULE:FREQ=YEARLY;COUNT=3 ")),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1998, 9, 2, 9, 0),
                datetime(1999, 9, 2, 9, 0),
            ],
        )

    def testStrSpacesAndLines(self):
        """
        Perform the testStrSpacesAndLines utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testStrSpacesAndLines through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrulestr(" DTSTART:19970902T090000 \n" " \n" " RRULE:FREQ=YEARLY;COUNT=3 \n")),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1998, 9, 2, 9, 0),
                datetime(1999, 9, 2, 9, 0),
            ],
        )

    def testStrNoDTStart(self):
        """
        Perform the testStrNoDTStart utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testStrNoDTStart through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrulestr("RRULE:FREQ=YEARLY;COUNT=3\n", dtstart=parse("19970902T090000"))),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1998, 9, 2, 9, 0),
                datetime(1999, 9, 2, 9, 0),
            ],
        )

    def testStrValueOnly(self):
        """
        Perform the testStrValueOnly utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testStrValueOnly through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrulestr("FREQ=YEARLY;COUNT=3\n", dtstart=parse("19970902T090000"))),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1998, 9, 2, 9, 0),
                datetime(1999, 9, 2, 9, 0),
            ],
        )

    def testStrUnfold(self):
        """
        Perform the testStrUnfold utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testStrUnfold through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrulestr(
                    "FREQ=YEA\n RLY;COUNT=3\n",
                    unfold=True,
                    dtstart=parse("19970902T090000"),
                )
            ),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1998, 9, 2, 9, 0),
                datetime(1999, 9, 2, 9, 0),
            ],
        )

    def testStrSet(self):
        """
        Perform the testStrSet utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testStrSet through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrulestr(
                    "DTSTART:19970902T090000\n"
                    "RRULE:FREQ=YEARLY;COUNT=2;BYDAY=TU\n"
                    "RRULE:FREQ=YEARLY;COUNT=1;BYDAY=TH\n"
                )
            ),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1997, 9, 4, 9, 0),
                datetime(1997, 9, 9, 9, 0),
            ],
        )

    def testStrSetDate(self):
        """
        Perform the testStrSetDate utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testStrSetDate through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrulestr(
                    "DTSTART:19970902T090000\n"
                    "RRULE:FREQ=YEARLY;COUNT=1;BYDAY=TU\n"
                    "RDATE:19970904T090000\n"
                    "RDATE:19970909T090000\n"
                )
            ),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1997, 9, 4, 9, 0),
                datetime(1997, 9, 9, 9, 0),
            ],
        )

    def testStrSetExRule(self):
        """
        Perform the testStrSetExRule utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testStrSetExRule through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrulestr(
                    "DTSTART:19970902T090000\n"
                    "RRULE:FREQ=YEARLY;COUNT=6;BYDAY=TU,TH\n"
                    "EXRULE:FREQ=YEARLY;COUNT=3;BYDAY=TH\n"
                )
            ),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1997, 9, 9, 9, 0),
                datetime(1997, 9, 16, 9, 0),
            ],
        )

    def testStrSetExDate(self):
        """
        Perform the testStrSetExDate utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testStrSetExDate through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrulestr(
                    "DTSTART:19970902T090000\n"
                    "RRULE:FREQ=YEARLY;COUNT=6;BYDAY=TU,TH\n"
                    "EXDATE:19970904T090000\n"
                    "EXDATE:19970911T090000\n"
                    "EXDATE:19970918T090000\n"
                )
            ),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1997, 9, 9, 9, 0),
                datetime(1997, 9, 16, 9, 0),
            ],
        )

    def testStrSetDateAndExDate(self):
        """
        Perform the testStrSetDateAndExDate utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testStrSetDateAndExDate through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrulestr(
                    "DTSTART:19970902T090000\n"
                    "RDATE:19970902T090000\n"
                    "RDATE:19970904T090000\n"
                    "RDATE:19970909T090000\n"
                    "RDATE:19970911T090000\n"
                    "RDATE:19970916T090000\n"
                    "RDATE:19970918T090000\n"
                    "EXDATE:19970904T090000\n"
                    "EXDATE:19970911T090000\n"
                    "EXDATE:19970918T090000\n"
                )
            ),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1997, 9, 9, 9, 0),
                datetime(1997, 9, 16, 9, 0),
            ],
        )

    def testStrSetDateAndExRule(self):
        """
        Perform the testStrSetDateAndExRule utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testStrSetDateAndExRule through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrulestr(
                    "DTSTART:19970902T090000\n"
                    "RDATE:19970902T090000\n"
                    "RDATE:19970904T090000\n"
                    "RDATE:19970909T090000\n"
                    "RDATE:19970911T090000\n"
                    "RDATE:19970916T090000\n"
                    "RDATE:19970918T090000\n"
                    "EXRULE:FREQ=YEARLY;COUNT=3;BYDAY=TH\n"
                )
            ),
            [
                datetime(1997, 9, 2, 9, 0),
                datetime(1997, 9, 9, 9, 0),
                datetime(1997, 9, 16, 9, 0),
            ],
        )

    def testStrKeywords(self):
        """
        Perform the testStrKeywords utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testStrKeywords through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(
                rrulestr(
                    "DTSTART:19970902T090000\n"
                    "RRULE:FREQ=YEARLY;COUNT=3;INTERVAL=3;"
                    "BYMONTH=3;BYWEEKDAY=TH;BYMONTHDAY=3;"
                    "BYHOUR=3;BYMINUTE=3;BYSECOND=3\n"
                )
            ),
            [
                datetime(2033, 3, 3, 3, 3, 3),
                datetime(2039, 3, 3, 3, 3, 3),
                datetime(2072, 3, 3, 3, 3, 3),
            ],
        )

    def testStrNWeekDay(self):
        """
        Perform the testStrNWeekDay utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testStrNWeekDay through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            list(rrulestr("DTSTART:19970902T090000\n" "RRULE:FREQ=YEARLY;COUNT=3;BYDAY=1TU,-1TH\n")),
            [
                datetime(1997, 12, 25, 9, 0),
                datetime(1998, 1, 6, 9, 0),
                datetime(1998, 12, 31, 9, 0),
            ],
        )

    def testBadBySetPos(self):
        """
        Perform the testBadBySetPos utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testBadBySetPos through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertRaises(
            ValueError,
            rrule,
            MONTHLY,
            count=1,
            bysetpos=0,
            dtstart=parse("19970902T090000"),
        )

    def testBadBySetPosMany(self):
        """
        Perform the testBadBySetPosMany utility operation under explicit compatibility rules.

        Example:
            Exercise RRuleTest.testBadBySetPosMany through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertRaises(
            ValueError,
            rrule,
            MONTHLY,
            count=1,
            bysetpos=(-1, 0, 1),
            dtstart=parse("19970902T090000"),
        )


class ParserTest(unittest.TestCase):
    """
    Provide the ParserTest utility contract with explicit state and cleanup behavior.

    Example:
        Exercise ParserTest through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py
    """
    def setUp(self):
        """
        Perform the setUp utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.setUp through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.tzinfos = {"BRST": -10800}
        self.brsttz = tzoffset("BRST", -10800)
        self.default = datetime(2003, 9, 25)

    def testDateCommandFormat(self):
        """
        Perform the testDateCommandFormat utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateCommandFormat through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            parse("Thu Sep 25 10:36:28 BRST 2003", tzinfos=self.tzinfos),
            datetime(2003, 9, 25, 10, 36, 28, tzinfo=self.brsttz),
        )

    def testDateCommandFormatUnicode(self):
        """
        Perform the testDateCommandFormatUnicode utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateCommandFormatUnicode through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            parse("Thu Sep 25 10:36:28 BRST 2003", tzinfos=self.tzinfos),
            datetime(2003, 9, 25, 10, 36, 28, tzinfo=self.brsttz),
        )

    def testDateCommandFormatReversed(self):
        """
        Perform the testDateCommandFormatReversed utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateCommandFormatReversed through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            parse("2003 10:36:28 BRST 25 Sep Thu", tzinfos=self.tzinfos),
            datetime(2003, 9, 25, 10, 36, 28, tzinfo=self.brsttz),
        )

    def testDateCommandFormatIgnoreTz(self):
        """
        Perform the testDateCommandFormatIgnoreTz utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateCommandFormatIgnoreTz through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            parse("Thu Sep 25 10:36:28 BRST 2003", ignoretz=True),
            datetime(2003, 9, 25, 10, 36, 28),
        )

    def testDateCommandFormatStrip1(self):
        """
        Perform the testDateCommandFormatStrip1 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateCommandFormatStrip1 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("Thu Sep 25 10:36:28 2003"), datetime(2003, 9, 25, 10, 36, 28))

    def testDateCommandFormatStrip2(self):
        """
        Perform the testDateCommandFormatStrip2 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateCommandFormatStrip2 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            parse("Thu Sep 25 10:36:28", default=self.default),
            datetime(2003, 9, 25, 10, 36, 28),
        )

    def testDateCommandFormatStrip3(self):
        """
        Perform the testDateCommandFormatStrip3 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateCommandFormatStrip3 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            parse("Thu Sep 10:36:28", default=self.default),
            datetime(2003, 9, 25, 10, 36, 28),
        )

    def testDateCommandFormatStrip4(self):
        """
        Perform the testDateCommandFormatStrip4 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateCommandFormatStrip4 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            parse("Thu 10:36:28", default=self.default),
            datetime(2003, 9, 25, 10, 36, 28),
        )

    def testDateCommandFormatStrip5(self):
        """
        Perform the testDateCommandFormatStrip5 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateCommandFormatStrip5 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            parse("Sep 10:36:28", default=self.default),
            datetime(2003, 9, 25, 10, 36, 28),
        )

    def testDateCommandFormatStrip6(self):
        """
        Perform the testDateCommandFormatStrip6 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateCommandFormatStrip6 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("10:36:28", default=self.default), datetime(2003, 9, 25, 10, 36, 28))

    def testDateCommandFormatStrip7(self):
        """
        Perform the testDateCommandFormatStrip7 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateCommandFormatStrip7 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("10:36", default=self.default), datetime(2003, 9, 25, 10, 36))

    def testDateCommandFormatStrip8(self):
        """
        Perform the testDateCommandFormatStrip8 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateCommandFormatStrip8 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("Thu Sep 25 2003"), datetime(2003, 9, 25))

    def testDateCommandFormatStrip9(self):
        """
        Perform the testDateCommandFormatStrip9 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateCommandFormatStrip9 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("Sep 25 2003"), datetime(2003, 9, 25))

    def testDateCommandFormatStrip9(self):
        """
        Perform the testDateCommandFormatStrip9 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateCommandFormatStrip9 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("Sep 2003", default=self.default), datetime(2003, 9, 25))

    def testDateCommandFormatStrip10(self):
        """
        Perform the testDateCommandFormatStrip10 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateCommandFormatStrip10 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("Sep", default=self.default), datetime(2003, 9, 25))

    def testDateCommandFormatStrip11(self):
        """
        Perform the testDateCommandFormatStrip11 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateCommandFormatStrip11 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("2003", default=self.default), datetime(2003, 9, 25))

    def testDateRCommandFormat(self):
        """
        Perform the testDateRCommandFormat utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateRCommandFormat through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            parse("Thu, 25 Sep 2003 10:49:41 -0300"),
            datetime(2003, 9, 25, 10, 49, 41, tzinfo=self.brsttz),
        )

    def testISOFormat(self):
        """
        Perform the testISOFormat utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testISOFormat through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            parse("2003-09-25T10:49:41.5-03:00"),
            datetime(2003, 9, 25, 10, 49, 41, 500000, tzinfo=self.brsttz),
        )

    def testISOFormatStrip1(self):
        """
        Perform the testISOFormatStrip1 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testISOFormatStrip1 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            parse("2003-09-25T10:49:41-03:00"),
            datetime(2003, 9, 25, 10, 49, 41, tzinfo=self.brsttz),
        )

    def testISOFormatStrip2(self):
        """
        Perform the testISOFormatStrip2 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testISOFormatStrip2 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("2003-09-25T10:49:41"), datetime(2003, 9, 25, 10, 49, 41))

    def testISOFormatStrip3(self):
        """
        Perform the testISOFormatStrip3 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testISOFormatStrip3 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("2003-09-25T10:49"), datetime(2003, 9, 25, 10, 49))

    def testISOFormatStrip4(self):
        """
        Perform the testISOFormatStrip4 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testISOFormatStrip4 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("2003-09-25T10"), datetime(2003, 9, 25, 10))

    def testISOFormatStrip5(self):
        """
        Perform the testISOFormatStrip5 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testISOFormatStrip5 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("2003-09-25"), datetime(2003, 9, 25))

    def testISOStrippedFormat(self):
        """
        Perform the testISOStrippedFormat utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testISOStrippedFormat through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            parse("20030925T104941.5-0300"),
            datetime(2003, 9, 25, 10, 49, 41, 500000, tzinfo=self.brsttz),
        )

    def testISOStrippedFormatStrip1(self):
        """
        Perform the testISOStrippedFormatStrip1 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testISOStrippedFormatStrip1 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            parse("20030925T104941-0300"),
            datetime(2003, 9, 25, 10, 49, 41, tzinfo=self.brsttz),
        )

    def testISOStrippedFormatStrip2(self):
        """
        Perform the testISOStrippedFormatStrip2 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testISOStrippedFormatStrip2 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("20030925T104941"), datetime(2003, 9, 25, 10, 49, 41))

    def testISOStrippedFormatStrip3(self):
        """
        Perform the testISOStrippedFormatStrip3 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testISOStrippedFormatStrip3 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("20030925T1049"), datetime(2003, 9, 25, 10, 49, 0))

    def testISOStrippedFormatStrip4(self):
        """
        Perform the testISOStrippedFormatStrip4 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testISOStrippedFormatStrip4 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("20030925T10"), datetime(2003, 9, 25, 10))

    def testISOStrippedFormatStrip5(self):
        """
        Perform the testISOStrippedFormatStrip5 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testISOStrippedFormatStrip5 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("20030925"), datetime(2003, 9, 25))

    def testNoSeparator1(self):
        """
        Perform the testNoSeparator1 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testNoSeparator1 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("199709020908"), datetime(1997, 9, 2, 9, 8))

    def testNoSeparator2(self):
        """
        Perform the testNoSeparator2 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testNoSeparator2 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("19970902090807"), datetime(1997, 9, 2, 9, 8, 7))

    def testDateWithDash1(self):
        """
        Perform the testDateWithDash1 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateWithDash1 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("2003-09-25"), datetime(2003, 9, 25))

    def testDateWithDash2(self):
        """
        Perform the testDateWithDash2 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateWithDash2 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("2003-Sep-25"), datetime(2003, 9, 25))

    def testDateWithDash3(self):
        """
        Perform the testDateWithDash3 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateWithDash3 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("25-Sep-2003"), datetime(2003, 9, 25))

    def testDateWithDash4(self):
        """
        Perform the testDateWithDash4 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateWithDash4 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("25-Sep-2003"), datetime(2003, 9, 25))

    def testDateWithDash5(self):
        """
        Perform the testDateWithDash5 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateWithDash5 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("Sep-25-2003"), datetime(2003, 9, 25))

    def testDateWithDash6(self):
        """
        Perform the testDateWithDash6 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateWithDash6 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("09-25-2003"), datetime(2003, 9, 25))

    def testDateWithDash7(self):
        """
        Perform the testDateWithDash7 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateWithDash7 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("25-09-2003"), datetime(2003, 9, 25))

    def testDateWithDash8(self):
        """
        Perform the testDateWithDash8 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateWithDash8 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("10-09-2003", dayfirst=True), datetime(2003, 9, 10))

    def testDateWithDash9(self):
        """
        Perform the testDateWithDash9 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateWithDash9 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("10-09-2003"), datetime(2003, 10, 9))

    def testDateWithDash10(self):
        """
        Perform the testDateWithDash10 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateWithDash10 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("10-09-03"), datetime(2003, 10, 9))

    def testDateWithDash11(self):
        """
        Perform the testDateWithDash11 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateWithDash11 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("10-09-03", yearfirst=True), datetime(2010, 9, 3))

    def testDateWithDot1(self):
        """
        Perform the testDateWithDot1 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateWithDot1 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("2003.09.25"), datetime(2003, 9, 25))

    def testDateWithDot2(self):
        """
        Perform the testDateWithDot2 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateWithDot2 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("2003.Sep.25"), datetime(2003, 9, 25))

    def testDateWithDot3(self):
        """
        Perform the testDateWithDot3 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateWithDot3 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("25.Sep.2003"), datetime(2003, 9, 25))

    def testDateWithDot4(self):
        """
        Perform the testDateWithDot4 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateWithDot4 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("25.Sep.2003"), datetime(2003, 9, 25))

    def testDateWithDot5(self):
        """
        Perform the testDateWithDot5 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateWithDot5 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("Sep.25.2003"), datetime(2003, 9, 25))

    def testDateWithDot6(self):
        """
        Perform the testDateWithDot6 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateWithDot6 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("09.25.2003"), datetime(2003, 9, 25))

    def testDateWithDot7(self):
        """
        Perform the testDateWithDot7 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateWithDot7 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("25.09.2003"), datetime(2003, 9, 25))

    def testDateWithDot8(self):
        """
        Perform the testDateWithDot8 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateWithDot8 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("10.09.2003", dayfirst=True), datetime(2003, 9, 10))

    def testDateWithDot9(self):
        """
        Perform the testDateWithDot9 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateWithDot9 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("10.09.2003"), datetime(2003, 10, 9))

    def testDateWithDot10(self):
        """
        Perform the testDateWithDot10 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateWithDot10 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("10.09.03"), datetime(2003, 10, 9))

    def testDateWithDot11(self):
        """
        Perform the testDateWithDot11 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateWithDot11 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("10.09.03", yearfirst=True), datetime(2010, 9, 3))

    def testDateWithSlash1(self):
        """
        Perform the testDateWithSlash1 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateWithSlash1 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("2003/09/25"), datetime(2003, 9, 25))

    def testDateWithSlash2(self):
        """
        Perform the testDateWithSlash2 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateWithSlash2 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("2003/Sep/25"), datetime(2003, 9, 25))

    def testDateWithSlash3(self):
        """
        Perform the testDateWithSlash3 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateWithSlash3 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("25/Sep/2003"), datetime(2003, 9, 25))

    def testDateWithSlash4(self):
        """
        Perform the testDateWithSlash4 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateWithSlash4 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("25/Sep/2003"), datetime(2003, 9, 25))

    def testDateWithSlash5(self):
        """
        Perform the testDateWithSlash5 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateWithSlash5 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("Sep/25/2003"), datetime(2003, 9, 25))

    def testDateWithSlash6(self):
        """
        Perform the testDateWithSlash6 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateWithSlash6 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("09/25/2003"), datetime(2003, 9, 25))

    def testDateWithSlash7(self):
        """
        Perform the testDateWithSlash7 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateWithSlash7 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("25/09/2003"), datetime(2003, 9, 25))

    def testDateWithSlash8(self):
        """
        Perform the testDateWithSlash8 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateWithSlash8 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("10/09/2003", dayfirst=True), datetime(2003, 9, 10))

    def testDateWithSlash9(self):
        """
        Perform the testDateWithSlash9 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateWithSlash9 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("10/09/2003"), datetime(2003, 10, 9))

    def testDateWithSlash10(self):
        """
        Perform the testDateWithSlash10 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateWithSlash10 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("10/09/03"), datetime(2003, 10, 9))

    def testDateWithSlash11(self):
        """
        Perform the testDateWithSlash11 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateWithSlash11 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("10/09/03", yearfirst=True), datetime(2010, 9, 3))

    def testDateWithSpace12(self):
        """
        Perform the testDateWithSpace12 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateWithSpace12 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("25 09 03"), datetime(2003, 9, 25))

    def testDateWithSpace13(self):
        """
        Perform the testDateWithSpace13 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateWithSpace13 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("25 09 03"), datetime(2003, 9, 25))

    def testDateWithSpace1(self):
        """
        Perform the testDateWithSpace1 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateWithSpace1 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("2003 09 25"), datetime(2003, 9, 25))

    def testDateWithSpace2(self):
        """
        Perform the testDateWithSpace2 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateWithSpace2 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("2003 Sep 25"), datetime(2003, 9, 25))

    def testDateWithSpace3(self):
        """
        Perform the testDateWithSpace3 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateWithSpace3 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("25 Sep 2003"), datetime(2003, 9, 25))

    def testDateWithSpace4(self):
        """
        Perform the testDateWithSpace4 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateWithSpace4 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("25 Sep 2003"), datetime(2003, 9, 25))

    def testDateWithSpace5(self):
        """
        Perform the testDateWithSpace5 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateWithSpace5 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("Sep 25 2003"), datetime(2003, 9, 25))

    def testDateWithSpace6(self):
        """
        Perform the testDateWithSpace6 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateWithSpace6 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("09 25 2003"), datetime(2003, 9, 25))

    def testDateWithSpace7(self):
        """
        Perform the testDateWithSpace7 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateWithSpace7 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("25 09 2003"), datetime(2003, 9, 25))

    def testDateWithSpace8(self):
        """
        Perform the testDateWithSpace8 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateWithSpace8 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("10 09 2003", dayfirst=True), datetime(2003, 9, 10))

    def testDateWithSpace9(self):
        """
        Perform the testDateWithSpace9 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateWithSpace9 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("10 09 2003"), datetime(2003, 10, 9))

    def testDateWithSpace10(self):
        """
        Perform the testDateWithSpace10 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateWithSpace10 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("10 09 03"), datetime(2003, 10, 9))

    def testDateWithSpace11(self):
        """
        Perform the testDateWithSpace11 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateWithSpace11 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("10 09 03", yearfirst=True), datetime(2010, 9, 3))

    def testDateWithSpace12(self):
        """
        Perform the testDateWithSpace12 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateWithSpace12 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("25 09 03"), datetime(2003, 9, 25))

    def testDateWithSpace13(self):
        """
        Perform the testDateWithSpace13 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testDateWithSpace13 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("25 09 03"), datetime(2003, 9, 25))

    def testStrangelyOrderedDate1(self):
        """
        Perform the testStrangelyOrderedDate1 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testStrangelyOrderedDate1 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("03 25 Sep"), datetime(2003, 9, 25))

    def testStrangelyOrderedDate2(self):
        """
        Perform the testStrangelyOrderedDate2 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testStrangelyOrderedDate2 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("2003 25 Sep"), datetime(2003, 9, 25))

    def testStrangelyOrderedDate3(self):
        """
        Perform the testStrangelyOrderedDate3 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testStrangelyOrderedDate3 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("25 03 Sep"), datetime(2025, 9, 3))

    def testHourWithLetters(self):
        """
        Perform the testHourWithLetters utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testHourWithLetters through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            parse("10h36m28.5s", default=self.default),
            datetime(2003, 9, 25, 10, 36, 28, 500000),
        )

    def testHourWithLettersStrip1(self):
        """
        Perform the testHourWithLettersStrip1 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testHourWithLettersStrip1 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("10h36m28s", default=self.default), datetime(2003, 9, 25, 10, 36, 28))

    def testHourWithLettersStrip1(self):
        """
        Perform the testHourWithLettersStrip1 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testHourWithLettersStrip1 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("10h36m", default=self.default), datetime(2003, 9, 25, 10, 36))

    def testHourWithLettersStrip2(self):
        """
        Perform the testHourWithLettersStrip2 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testHourWithLettersStrip2 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("10h", default=self.default), datetime(2003, 9, 25, 10))

    def testHourAmPm1(self):
        """
        Perform the testHourAmPm1 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testHourAmPm1 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("10h am", default=self.default), datetime(2003, 9, 25, 10))

    def testHourAmPm2(self):
        """
        Perform the testHourAmPm2 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testHourAmPm2 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("10h pm", default=self.default), datetime(2003, 9, 25, 22))

    def testHourAmPm3(self):
        """
        Perform the testHourAmPm3 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testHourAmPm3 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("10am", default=self.default), datetime(2003, 9, 25, 10))

    def testHourAmPm4(self):
        """
        Perform the testHourAmPm4 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testHourAmPm4 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("10pm", default=self.default), datetime(2003, 9, 25, 22))

    def testHourAmPm5(self):
        """
        Perform the testHourAmPm5 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testHourAmPm5 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("10:00 am", default=self.default), datetime(2003, 9, 25, 10))

    def testHourAmPm6(self):
        """
        Perform the testHourAmPm6 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testHourAmPm6 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("10:00 pm", default=self.default), datetime(2003, 9, 25, 22))

    def testHourAmPm7(self):
        """
        Perform the testHourAmPm7 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testHourAmPm7 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("10:00am", default=self.default), datetime(2003, 9, 25, 10))

    def testHourAmPm8(self):
        """
        Perform the testHourAmPm8 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testHourAmPm8 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("10:00pm", default=self.default), datetime(2003, 9, 25, 22))

    def testHourAmPm9(self):
        """
        Perform the testHourAmPm9 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testHourAmPm9 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("10:00a.m", default=self.default), datetime(2003, 9, 25, 10))

    def testHourAmPm10(self):
        """
        Perform the testHourAmPm10 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testHourAmPm10 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("10:00p.m", default=self.default), datetime(2003, 9, 25, 22))

    def testHourAmPm11(self):
        """
        Perform the testHourAmPm11 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testHourAmPm11 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("10:00a.m.", default=self.default), datetime(2003, 9, 25, 10))

    def testHourAmPm12(self):
        """
        Perform the testHourAmPm12 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testHourAmPm12 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("10:00p.m.", default=self.default), datetime(2003, 9, 25, 22))

    def testPertain(self):
        """
        Perform the testPertain utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testPertain through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("Sep 03", default=self.default), datetime(2003, 9, 3))
        self.assertEqual(parse("Sep of 03", default=self.default), datetime(2003, 9, 25))

    def testWeekdayAlone(self):
        """
        Perform the testWeekdayAlone utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testWeekdayAlone through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("Wed", default=self.default), datetime(2003, 10, 1))

    def testLongWeekday(self):
        """
        Perform the testLongWeekday utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testLongWeekday through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("Wednesday", default=self.default), datetime(2003, 10, 1))

    def testLongMonth(self):
        """
        Perform the testLongMonth utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testLongMonth through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("October", default=self.default), datetime(2003, 10, 25))

    def testZeroYear(self):
        """
        Perform the testZeroYear utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testZeroYear through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("31-Dec-00", default=self.default), datetime(2000, 12, 31))

    def testFuzzy(self):
        """
        Perform the testFuzzy utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testFuzzy through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        s = "Today is 25 of September of 2003, exactly " "at 10:49:41 with timezone -03:00."
        self.assertEqual(parse(s, fuzzy=True), datetime(2003, 9, 25, 10, 49, 41, tzinfo=self.brsttz))

    def testExtraSpace(self):
        """
        Perform the testExtraSpace utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testExtraSpace through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("  July   4 ,  1976   12:01:02   am  "), datetime(1976, 7, 4, 0, 1, 2))

    def testRandomFormat1(self):
        """
        Perform the testRandomFormat1 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testRandomFormat1 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("Wed, July 10, '96"), datetime(1996, 7, 10, 0, 0))

    def testRandomFormat2(self):
        """
        Perform the testRandomFormat2 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testRandomFormat2 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            parse("1996.07.10 AD at 15:08:56 PDT", ignoretz=True),
            datetime(1996, 7, 10, 15, 8, 56),
        )

    def testRandomFormat3(self):
        """
        Perform the testRandomFormat3 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testRandomFormat3 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("1996.July.10 AD 12:08 PM"), datetime(1996, 7, 10, 12, 8))

    def testRandomFormat4(self):
        """
        Perform the testRandomFormat4 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testRandomFormat4 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            parse("Tuesday, April 12, 1952 AD 3:30:42pm PST", ignoretz=True),
            datetime(1952, 4, 12, 15, 30, 42),
        )

    def testRandomFormat5(self):
        """
        Perform the testRandomFormat5 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testRandomFormat5 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            parse("November 5, 1994, 8:15:30 am EST", ignoretz=True),
            datetime(1994, 11, 5, 8, 15, 30),
        )

    def testRandomFormat6(self):
        """
        Perform the testRandomFormat6 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testRandomFormat6 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            parse("1994-11-05T08:15:30-05:00", ignoretz=True),
            datetime(1994, 11, 5, 8, 15, 30),
        )

    def testRandomFormat7(self):
        """
        Perform the testRandomFormat7 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testRandomFormat7 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            parse("1994-11-05T08:15:30Z", ignoretz=True),
            datetime(1994, 11, 5, 8, 15, 30),
        )

    def testRandomFormat8(self):
        """
        Perform the testRandomFormat8 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testRandomFormat8 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("July 4, 1976"), datetime(1976, 7, 4))

    def testRandomFormat9(self):
        """
        Perform the testRandomFormat9 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testRandomFormat9 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("7 4 1976"), datetime(1976, 7, 4))

    def testRandomFormat10(self):
        """
        Perform the testRandomFormat10 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testRandomFormat10 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("4 jul 1976"), datetime(1976, 7, 4))

    def testRandomFormat11(self):
        """
        Perform the testRandomFormat11 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testRandomFormat11 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("7-4-76"), datetime(1976, 7, 4))

    def testRandomFormat12(self):
        """
        Perform the testRandomFormat12 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testRandomFormat12 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("19760704"), datetime(1976, 7, 4))

    def testRandomFormat13(self):
        """
        Perform the testRandomFormat13 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testRandomFormat13 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("0:01:02", default=self.default), datetime(2003, 9, 25, 0, 1, 2))

    def testRandomFormat14(self):
        """
        Perform the testRandomFormat14 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testRandomFormat14 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("12h 01m02s am", default=self.default), datetime(2003, 9, 25, 0, 1, 2))

    def testRandomFormat15(self):
        """
        Perform the testRandomFormat15 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testRandomFormat15 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("0:01:02 on July 4, 1976"), datetime(1976, 7, 4, 0, 1, 2))

    def testRandomFormat16(self):
        """
        Perform the testRandomFormat16 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testRandomFormat16 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("0:01:02 on July 4, 1976"), datetime(1976, 7, 4, 0, 1, 2))

    def testRandomFormat17(self):
        """
        Perform the testRandomFormat17 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testRandomFormat17 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("1976-07-04T00:01:02Z", ignoretz=True), datetime(1976, 7, 4, 0, 1, 2))

    def testRandomFormat18(self):
        """
        Perform the testRandomFormat18 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testRandomFormat18 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("July 4, 1976 12:01:02 am"), datetime(1976, 7, 4, 0, 1, 2))

    def testRandomFormat19(self):
        """
        Perform the testRandomFormat19 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testRandomFormat19 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("Mon Jan  2 04:24:27 1995"), datetime(1995, 1, 2, 4, 24, 27))

    def testRandomFormat20(self):
        """
        Perform the testRandomFormat20 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testRandomFormat20 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            parse("Tue Apr 4 00:22:12 PDT 1995", ignoretz=True),
            datetime(1995, 4, 4, 0, 22, 12),
        )

    def testRandomFormat21(self):
        """
        Perform the testRandomFormat21 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testRandomFormat21 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("04.04.95 00:22"), datetime(1995, 4, 4, 0, 22))

    def testRandomFormat22(self):
        """
        Perform the testRandomFormat22 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testRandomFormat22 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("Jan 1 1999 11:23:34.578"), datetime(1999, 1, 1, 11, 23, 34, 578000))

    def testRandomFormat23(self):
        """
        Perform the testRandomFormat23 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testRandomFormat23 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("950404 122212"), datetime(1995, 4, 4, 12, 22, 12))

    def testRandomFormat24(self):
        """
        Perform the testRandomFormat24 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testRandomFormat24 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            parse("0:00 PM, PST", default=self.default, ignoretz=True),
            datetime(2003, 9, 25, 12, 0),
        )

    def testRandomFormat25(self):
        """
        Perform the testRandomFormat25 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testRandomFormat25 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("12:08 PM", default=self.default), datetime(2003, 9, 25, 12, 8))

    def testRandomFormat26(self):
        """
        Perform the testRandomFormat26 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testRandomFormat26 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("5:50 A.M. on June 13, 1990"), datetime(1990, 6, 13, 5, 50))

    def testRandomFormat27(self):
        """
        Perform the testRandomFormat27 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testRandomFormat27 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("3rd of May 2001"), datetime(2001, 5, 3))

    def testRandomFormat28(self):
        """
        Perform the testRandomFormat28 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testRandomFormat28 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("5th of March 2001"), datetime(2001, 3, 5))

    def testRandomFormat29(self):
        """
        Perform the testRandomFormat29 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testRandomFormat29 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("1st of May 2003"), datetime(2003, 5, 1))

    def testRandomFormat30(self):
        """
        Perform the testRandomFormat30 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testRandomFormat30 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("01h02m03", default=self.default), datetime(2003, 9, 25, 1, 2, 3))

    def testRandomFormat31(self):
        """
        Perform the testRandomFormat31 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testRandomFormat31 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("01h02", default=self.default), datetime(2003, 9, 25, 1, 2))

    def testRandomFormat32(self):
        """
        Perform the testRandomFormat32 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testRandomFormat32 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("01h02s", default=self.default), datetime(2003, 9, 25, 1, 0, 2))

    def testRandomFormat33(self):
        """
        Perform the testRandomFormat33 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testRandomFormat33 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("01m02", default=self.default), datetime(2003, 9, 25, 0, 1, 2))

    def testRandomFormat34(self):
        """
        Perform the testRandomFormat34 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testRandomFormat34 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(parse("01m02h", default=self.default), datetime(2003, 9, 25, 2, 1))

    def testRandomFormat35(self):
        """
        Perform the testRandomFormat35 utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testRandomFormat35 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            parse("2004 10 Apr 11h30m", default=self.default),
            datetime(2004, 4, 10, 11, 30),
        )

    def testIncreasingCTime(self):
        # This test will check 200 different years, every month, every day,
        # every hour, every minute, every second, and every weekday, using
        # a delta of more or less 1 year, 1 month, 1 day, 1 minute and
        # 1 second.
        """
        Perform the testIncreasingCTime utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testIncreasingCTime through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        delta = timedelta(days=365 + 31 + 1, seconds=1 + 60 + 60 * 60)
        dt = datetime(1900, 1, 1, 0, 0, 0, 0)
        for i in range(200):
            self.assertEqual(parse(dt.ctime()), dt)
            dt += delta

    def testIncreasingISOFormat(self):
        """
        Perform the testIncreasingISOFormat utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testIncreasingISOFormat through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        delta = timedelta(days=365 + 31 + 1, seconds=1 + 60 + 60 * 60)
        dt = datetime(1900, 1, 1, 0, 0, 0, 0)
        for i in range(200):
            self.assertEqual(parse(dt.isoformat()), dt)
            dt += delta

    def testMicrosecondsPrecisionError(self):
        # Skip found out that sad precision problem. :-(
        """
        Perform the testMicrosecondsPrecisionError utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testMicrosecondsPrecisionError through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        dt1 = parse("00:11:25.01")
        dt2 = parse("00:12:10.01")
        self.assertEquals(dt1.microsecond, 10000)
        self.assertEquals(dt2.microsecond, 10000)

    def testMicrosecondPrecisionErrorReturns(self):
        # One more precision issue, discovered by Eric Brown.  This should
        # be the last one, as we're no longer using floating points.
        """
        Perform the testMicrosecondPrecisionErrorReturns utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testMicrosecondPrecisionErrorReturns through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for ms in [
            100001,
            100000,
            99999,
            99998,
            10001,
            10000,
            9999,
            9998,
            1001,
            1000,
            999,
            998,
            101,
            100,
            99,
            98,
        ]:
            dt = datetime(2008, 2, 27, 21, 26, 1, ms)
            self.assertEquals(parse(dt.isoformat()), dt)

    def testHighPrecisionSeconds(self):
        """
        Perform the testHighPrecisionSeconds utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testHighPrecisionSeconds through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEquals(
            parse("20080227T21:26:01.123456789"),
            datetime(2008, 2, 27, 21, 26, 1, 123456),
        )

    def testCustomParserInfo(self):
        # Custom parser info wasn't working, as Michael Elsdörfer discovered.
        """
        Perform the testCustomParserInfo utility operation under explicit compatibility rules.

        Example:
            Exercise ParserTest.testCustomParserInfo through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        from dateutil.parser import parserinfo, parser

        class myparserinfo(parserinfo):
            """
            Provide the myparserinfo utility contract with explicit state and cleanup behavior.

            Example:
                Exercise ParserTest.testCustomParserInfo.myparserinfo through a consuming regression::

                    python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py
            """
            MONTHS = parserinfo.MONTHS[:]
            MONTHS[0] = ("Foo", "Foo")

        myparser = parser(myparserinfo())
        dt = myparser.parse("01/Foo/2007")
        self.assertEquals(dt, datetime(2007, 1, 1))


class EasterTest(unittest.TestCase):
    """
    Provide the EasterTest utility contract with explicit state and cleanup behavior.

    Example:
        Exercise EasterTest through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py
    """
    easterlist = [
        # WESTERN            ORTHODOX
        (date(1990, 4, 15), date(1990, 4, 15)),
        (date(1991, 3, 31), date(1991, 4, 7)),
        (date(1992, 4, 19), date(1992, 4, 26)),
        (date(1993, 4, 11), date(1993, 4, 18)),
        (date(1994, 4, 3), date(1994, 5, 1)),
        (date(1995, 4, 16), date(1995, 4, 23)),
        (date(1996, 4, 7), date(1996, 4, 14)),
        (date(1997, 3, 30), date(1997, 4, 27)),
        (date(1998, 4, 12), date(1998, 4, 19)),
        (date(1999, 4, 4), date(1999, 4, 11)),
        (date(2000, 4, 23), date(2000, 4, 30)),
        (date(2001, 4, 15), date(2001, 4, 15)),
        (date(2002, 3, 31), date(2002, 5, 5)),
        (date(2003, 4, 20), date(2003, 4, 27)),
        (date(2004, 4, 11), date(2004, 4, 11)),
        (date(2005, 3, 27), date(2005, 5, 1)),
        (date(2006, 4, 16), date(2006, 4, 23)),
        (date(2007, 4, 8), date(2007, 4, 8)),
        (date(2008, 3, 23), date(2008, 4, 27)),
        (date(2009, 4, 12), date(2009, 4, 19)),
        (date(2010, 4, 4), date(2010, 4, 4)),
        (date(2011, 4, 24), date(2011, 4, 24)),
        (date(2012, 4, 8), date(2012, 4, 15)),
        (date(2013, 3, 31), date(2013, 5, 5)),
        (date(2014, 4, 20), date(2014, 4, 20)),
        (date(2015, 4, 5), date(2015, 4, 12)),
        (date(2016, 3, 27), date(2016, 5, 1)),
        (date(2017, 4, 16), date(2017, 4, 16)),
        (date(2018, 4, 1), date(2018, 4, 8)),
        (date(2019, 4, 21), date(2019, 4, 28)),
        (date(2020, 4, 12), date(2020, 4, 19)),
        (date(2021, 4, 4), date(2021, 5, 2)),
        (date(2022, 4, 17), date(2022, 4, 24)),
        (date(2023, 4, 9), date(2023, 4, 16)),
        (date(2024, 3, 31), date(2024, 5, 5)),
        (date(2025, 4, 20), date(2025, 4, 20)),
        (date(2026, 4, 5), date(2026, 4, 12)),
        (date(2027, 3, 28), date(2027, 5, 2)),
        (date(2028, 4, 16), date(2028, 4, 16)),
        (date(2029, 4, 1), date(2029, 4, 8)),
        (date(2030, 4, 21), date(2030, 4, 28)),
        (date(2031, 4, 13), date(2031, 4, 13)),
        (date(2032, 3, 28), date(2032, 5, 2)),
        (date(2033, 4, 17), date(2033, 4, 24)),
        (date(2034, 4, 9), date(2034, 4, 9)),
        (date(2035, 3, 25), date(2035, 4, 29)),
        (date(2036, 4, 13), date(2036, 4, 20)),
        (date(2037, 4, 5), date(2037, 4, 5)),
        (date(2038, 4, 25), date(2038, 4, 25)),
        (date(2039, 4, 10), date(2039, 4, 17)),
        (date(2040, 4, 1), date(2040, 5, 6)),
        (date(2041, 4, 21), date(2041, 4, 21)),
        (date(2042, 4, 6), date(2042, 4, 13)),
        (date(2043, 3, 29), date(2043, 5, 3)),
        (date(2044, 4, 17), date(2044, 4, 24)),
        (date(2045, 4, 9), date(2045, 4, 9)),
        (date(2046, 3, 25), date(2046, 4, 29)),
        (date(2047, 4, 14), date(2047, 4, 21)),
        (date(2048, 4, 5), date(2048, 4, 5)),
        (date(2049, 4, 18), date(2049, 4, 25)),
        (date(2050, 4, 10), date(2050, 4, 17)),
    ]

    def testEaster(self):
        """
        Perform the testEaster utility operation under explicit compatibility rules.

        Example:
            Exercise EasterTest.testEaster through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for western, orthodox in self.easterlist:
            self.assertEqual(western, easter(western.year, EASTER_WESTERN))
            self.assertEqual(orthodox, easter(orthodox.year, EASTER_ORTHODOX))


class TZTest(unittest.TestCase):

    """
    Provide the TZTest utility contract with explicit state and cleanup behavior.

    Example:
        Exercise TZTest through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py
    """
    TZFILE_EST5EDT = """
VFppZgAAAAAAAAAAAAAAAAAAAAAAAAAEAAAABAAAAAAAAADrAAAABAAAABCeph5wn7rrYKCGAHCh
ms1gomXicKOD6eCkaq5wpTWnYKZTyvCnFYlgqDOs8Kj+peCqE47wqt6H4KvzcPCsvmngrdNS8K6e
S+CvszTwsH4t4LGcUXCyZ0pgs3wzcLRHLGC1XBVwticOYLc793C4BvBguRvZcLnm0mC7BPXwu8a0
YLzk1/C9r9DgvsS58L+PsuDApJvwwW+U4MKEffDDT3bgxGRf8MUvWODGTXxwxw864MgtXnDI+Fdg
yg1AcMrYOWDLiPBw0iP0cNJg++DTdeTw1EDd4NVVxvDWIL/g1zWo8NgAoeDZFYrw2eCD4Nr+p3Db
wGXg3N6JcN2pgmDevmtw34lkYOCeTXDhaUZg4n4vcONJKGDkXhFw5Vcu4OZHLfDnNxDg6CcP8OkW
8uDqBvHw6vbU4Ovm0/Ds1rbg7ca18O6/02Dvr9Jw8J+1YPGPtHDyf5dg82+WcPRfeWD1T3hw9j9b
YPcvWnD4KHfg+Q88cPoIWeD6+Fjw++g74PzYOvD9yB3g/rgc8P+n/+AAl/7wAYfh4AJ34PADcP5g
BGD9cAVQ4GAGQN9wBzDCYAeNGXAJEKRgCa2U8ArwhmAL4IVwDNmi4A3AZ3AOuYTgD6mD8BCZZuAR
iWXwEnlI4BNpR/AUWSrgFUkp8BY5DOAXKQvwGCIpYBkI7fAaAgtgGvIKcBvh7WAc0exwHcHPYB6x
znAfobFgIHYA8CGBk2AiVeLwI2qv4CQ1xPAlSpHgJhWm8Ccqc+An/sNwKQpV4CnepXAq6jfgK76H
cCzTVGAtnmlwLrM2YC9+S3AwkxhgMWdn8DJy+mAzR0nwNFLcYDUnK/A2Mr5gNwcN8Dgb2uA45u/w
Ofu84DrG0fA7257gPK/ucD27gOA+j9BwP5ti4EBvsnBBhH9gQk+UcENkYWBEL3ZwRURDYEYPWHBH
JCVgR/h08EkEB2BJ2FbwSuPpYEu4OPBMzQXgTZga8E6s5+BPd/zwUIzJ4FFhGXBSbKvgU0D7cFRM
jeBVIN1wVixv4FcAv3BYFYxgWOChcFn1bmBawINwW9VQYFypn/BdtTJgXomB8F+VFGBgaWPwYX4w
4GJJRfBjXhLgZCkn8GU99OBmEkRwZx3W4GfyJnBo/bjgadIIcGrdmuBrsepwbMa3YG2RzHBupplg
b3GucHCGe2BxWsrwcmZdYHM6rPB0Rj9gdRqO8HYvW+B2+nDweA894HjaUvB57x/gero08HvPAeB8
o1Fwfa7j4H6DM3B/jsXgAAEAAQABAAEAAQABAAEAAQABAAEAAQABAAEAAQABAAEAAQABAAEAAQAB
AAEAAQABAgMBAAEAAQABAAEAAQABAAEAAQABAAEAAQABAAEAAQABAAEAAQABAAEAAQABAAEAAQAB
AAEAAQABAAEAAQABAAEAAQABAAEAAQABAAEAAQABAAEAAQABAAEAAQABAAEAAQABAAEAAQABAAEA
AQABAAEAAQABAAEAAQABAAEAAQABAAEAAQABAAEAAQABAAEAAQABAAEAAQABAAEAAQABAAEAAQAB
AAEAAQABAAEAAQABAAEAAQABAAEAAf//x8ABAP//ubAABP//x8ABCP//x8ABDEVEVABFU1QARVdU
AEVQVAAAAAABAAAAAQ==
    """

    EUROPE_HELSINKI = """
VFppZgAAAAAAAAAAAAAAAAAAAAAAAAAFAAAABQAAAAAAAAB1AAAABQAAAA2kc28Yy85RYMy/hdAV
I+uQFhPckBcDzZAX876QGOOvkBnToJAaw5GQG7y9EBysrhAdnJ8QHoyQEB98gRAgbHIQIVxjECJM
VBAjPEUQJCw2ECUcJxAmDBgQJwVDkCf1NJAo5SWQKdUWkCrFB5ArtPiQLKTpkC2U2pAuhMuQL3S8
kDBkrZAxXdkQMnK0EDM9uxA0UpYQNR2dEDYyeBA2/X8QOBuUkDjdYRA5+3aQOr1DEDvbWJA8pl+Q
Pbs6kD6GQZA/mxyQQGYjkEGEORBCRgWQQ2QbEEQl55BFQ/0QRgXJkEcj3xBH7uYQSQPBEEnOyBBK
46MQS66qEEzMv5BNjowQTqyhkE9ubhBQjIOQUVeKkFJsZZBTN2yQVExHkFUXTpBWLCmQVvcwkFgV
RhBY1xKQWfUoEFq29JBb1QoQXKAREF207BBef/MQX5TOEGBf1RBhfeqQYj+3EGNdzJBkH5kQZT2u
kGYItZBnHZCQZ+iXkGj9cpBpyHmQat1UkGuoW5BsxnEQbYg9kG6mUxBvaB+QcIY1EHFRPBByZhcQ
czEeEHRF+RB1EQAQdi8VkHbw4hB4DveQeNDEEHnu2ZB6sKYQe867kHyZwpB9rp2QfnmkkH+Of5AC
AQIDBAMEAwQDBAMEAwQDBAMEAwQDBAMEAwQDBAMEAwQDBAMEAwQDBAMEAwQDBAMEAwQDBAMEAwQD
BAMEAwQDBAMEAwQDBAMEAwQDBAMEAwQDBAMEAwQDBAMEAwQDBAMEAwQDBAMEAwQDBAMEAwQDBAME
AwQAABdoAAAAACowAQQAABwgAAkAACowAQQAABwgAAlITVQARUVTVABFRVQAAAAAAQEAAAABAQ==
    """

    NEW_YORK = """
VFppZgAAAAAAAAAAAAAAAAAAAAAAAAAEAAAABAAAABcAAADrAAAABAAAABCeph5wn7rrYKCGAHCh
ms1gomXicKOD6eCkaq5wpTWnYKZTyvCnFYlgqDOs8Kj+peCqE47wqt6H4KvzcPCsvmngrdNS8K6e
S+CvszTwsH4t4LGcUXCyZ0pgs3wzcLRHLGC1XBVwticOYLc793C4BvBguRvZcLnm0mC7BPXwu8a0
YLzk1/C9r9DgvsS58L+PsuDApJvwwW+U4MKEffDDT3bgxGRf8MUvWODGTXxwxw864MgtXnDI+Fdg
yg1AcMrYOWDLiPBw0iP0cNJg++DTdeTw1EDd4NVVxvDWIL/g1zWo8NgAoeDZFYrw2eCD4Nr+p3Db
wGXg3N6JcN2pgmDevmtw34lkYOCeTXDhaUZg4n4vcONJKGDkXhFw5Vcu4OZHLfDnNxDg6CcP8OkW
8uDqBvHw6vbU4Ovm0/Ds1rbg7ca18O6/02Dvr9Jw8J+1YPGPtHDyf5dg82+WcPRfeWD1T3hw9j9b
YPcvWnD4KHfg+Q88cPoIWeD6+Fjw++g74PzYOvD9yB3g/rgc8P+n/+AAl/7wAYfh4AJ34PADcP5g
BGD9cAVQ4GEGQN9yBzDCYgeNGXMJEKRjCa2U9ArwhmQL4IV1DNmi5Q3AZ3YOuYTmD6mD9xCZZucR
iWX4EnlI6BNpR/kUWSrpFUkp+RY5DOoXKQv6GCIpaxkI7fsaAgtsGvIKfBvh7Wwc0ex8HcHPbR6x
zn0fobFtIHYA/SGBk20iVeL+I2qv7iQ1xP4lSpHuJhWm/ycqc+8n/sOAKQpV8CnepYAq6jfxK76H
gSzTVHItnmmCLrM2cy9+S4MwkxhzMWdoBDJy+nQzR0oENFLcdTUnLAU2Mr51NwcOBjgb2vY45vAG
Ofu89jrG0gY72572PK/uhj27gPY+j9CGP5ti9kBvsoZBhH92Qk+UhkNkYXZEL3aHRURDd0XzqQdH
LV/3R9OLB0kNQfdJs20HSu0j90uciYdM1kB3TXxrh062IndPXE2HUJYEd1E8L4dSdeZ3UxwRh1RV
yHdU+/OHVjWqd1blEAdYHsb3WMTyB1n+qPdapNQHW96K91yEtgddvmz3XmSYB1+eTvdgTbSHYYdr
d2ItlodjZ013ZA14h2VHL3dl7VqHZycRd2fNPIdpBvN3aa0eh2rm1XdrljsHbM/x9212HQdur9P3
b1X/B3CPtfdxNeEHcm+X93MVwwd0T3n3dP7fh3Y4lnd23sGHeBh4d3i+o4d5+Fp3ep6Fh3vYPHd8
fmeHfbged35eSYd/mAB3AAEAAQABAAEAAQABAAEAAQABAAEAAQABAAEAAQABAAEAAQABAAEAAQAB
AAEAAQABAgMBAAEAAQABAAEAAQABAAEAAQABAAEAAQABAAEAAQABAAEAAQABAAEAAQABAAEAAQAB
AAEAAQABAAEAAQABAAEAAQABAAEAAQABAAEAAQABAAEAAQABAAEAAQABAAEAAQABAAEAAQABAAEA
AQABAAEAAQABAAEAAQABAAEAAQABAAEAAQABAAEAAQABAAEAAQABAAEAAQABAAEAAQABAAEAAQAB
AAEAAQABAAEAAQABAAEAAQABAAEAAf//x8ABAP//ubAABP//x8ABCP//x8ABDEVEVABFU1QARVdU
AEVQVAAEslgAAAAAAQWk7AEAAAACB4YfggAAAAMJZ1MDAAAABAtIhoQAAAAFDSsLhQAAAAYPDD8G
AAAABxDtcocAAAAIEs6mCAAAAAkVn8qJAAAACheA/goAAAALGWIxiwAAAAwdJeoMAAAADSHa5Q0A
AAAOJZ6djgAAAA8nf9EPAAAAECpQ9ZAAAAARLDIpEQAAABIuE1ySAAAAEzDnJBMAAAAUM7hIlAAA
ABU2jBAVAAAAFkO3G5YAAAAXAAAAAQAAAAE=
    """

    TZICAL_EST5EDT = """
BEGIN:VTIMEZONE
TZID:US-Eastern
LAST-MODIFIED:19870101T000000Z
TZURL:http://zones.stds_r_us.net/tz/US-Eastern
BEGIN:STANDARD
DTSTART:19671029T020000
RRULE:FREQ=YEARLY;BYDAY=-1SU;BYMONTH=10
TZOFFSETFROM:-0400
TZOFFSETTO:-0500
TZNAME:EST
END:STANDARD
BEGIN:DAYLIGHT
DTSTART:19870405T020000
RRULE:FREQ=YEARLY;BYDAY=1SU;BYMONTH=4
TZOFFSETFROM:-0500
TZOFFSETTO:-0400
TZNAME:EDT
END:DAYLIGHT
END:VTIMEZONE
    """

    def testStrStart1(self):
        """
        Perform the testStrStart1 utility operation under explicit compatibility rules.

        Example:
            Exercise TZTest.testStrStart1 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(datetime(2003, 4, 6, 1, 59, tzinfo=tzstr("EST5EDT")).tzname(), "EST")
        self.assertEqual(datetime(2003, 4, 6, 2, 00, tzinfo=tzstr("EST5EDT")).tzname(), "EDT")

    def testStrEnd1(self):
        """
        Perform the testStrEnd1 utility operation under explicit compatibility rules.

        Example:
            Exercise TZTest.testStrEnd1 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(datetime(2003, 10, 26, 0, 59, tzinfo=tzstr("EST5EDT")).tzname(), "EDT")
        self.assertEqual(datetime(2003, 10, 26, 1, 00, tzinfo=tzstr("EST5EDT")).tzname(), "EST")

    def testStrStart2(self):
        """
        Perform the testStrStart2 utility operation under explicit compatibility rules.

        Example:
            Exercise TZTest.testStrStart2 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        s = "EST5EDT,4,0,6,7200,10,0,26,7200,3600"
        self.assertEqual(datetime(2003, 4, 6, 1, 59, tzinfo=tzstr(s)).tzname(), "EST")
        self.assertEqual(datetime(2003, 4, 6, 2, 00, tzinfo=tzstr(s)).tzname(), "EDT")

    def testStrEnd2(self):
        """
        Perform the testStrEnd2 utility operation under explicit compatibility rules.

        Example:
            Exercise TZTest.testStrEnd2 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        s = "EST5EDT,4,0,6,7200,10,0,26,7200,3600"
        self.assertEqual(datetime(2003, 10, 26, 0, 59, tzinfo=tzstr(s)).tzname(), "EDT")
        self.assertEqual(datetime(2003, 10, 26, 1, 00, tzinfo=tzstr(s)).tzname(), "EST")

    def testStrStart3(self):
        """
        Perform the testStrStart3 utility operation under explicit compatibility rules.

        Example:
            Exercise TZTest.testStrStart3 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        s = "EST5EDT,4,1,0,7200,10,-1,0,7200,3600"
        self.assertEqual(datetime(2003, 4, 6, 1, 59, tzinfo=tzstr(s)).tzname(), "EST")
        self.assertEqual(datetime(2003, 4, 6, 2, 00, tzinfo=tzstr(s)).tzname(), "EDT")

    def testStrEnd3(self):
        """
        Perform the testStrEnd3 utility operation under explicit compatibility rules.

        Example:
            Exercise TZTest.testStrEnd3 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        s = "EST5EDT,4,1,0,7200,10,-1,0,7200,3600"
        self.assertEqual(datetime(2003, 10, 26, 0, 59, tzinfo=tzstr(s)).tzname(), "EDT")
        self.assertEqual(datetime(2003, 10, 26, 1, 00, tzinfo=tzstr(s)).tzname(), "EST")

    def testStrStart4(self):
        """
        Perform the testStrStart4 utility operation under explicit compatibility rules.

        Example:
            Exercise TZTest.testStrStart4 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        s = "EST5EDT4,M4.1.0/02:00:00,M10-5-0/02:00"
        self.assertEqual(datetime(2003, 4, 6, 1, 59, tzinfo=tzstr(s)).tzname(), "EST")
        self.assertEqual(datetime(2003, 4, 6, 2, 00, tzinfo=tzstr(s)).tzname(), "EDT")

    def testStrEnd4(self):
        """
        Perform the testStrEnd4 utility operation under explicit compatibility rules.

        Example:
            Exercise TZTest.testStrEnd4 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        s = "EST5EDT4,M4.1.0/02:00:00,M10-5-0/02:00"
        self.assertEqual(datetime(2003, 10, 26, 0, 59, tzinfo=tzstr(s)).tzname(), "EDT")
        self.assertEqual(datetime(2003, 10, 26, 1, 00, tzinfo=tzstr(s)).tzname(), "EST")

    def testStrStart5(self):
        """
        Perform the testStrStart5 utility operation under explicit compatibility rules.

        Example:
            Exercise TZTest.testStrStart5 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        s = "EST5EDT4,95/02:00:00,298/02:00"
        self.assertEqual(datetime(2003, 4, 6, 1, 59, tzinfo=tzstr(s)).tzname(), "EST")
        self.assertEqual(datetime(2003, 4, 6, 2, 00, tzinfo=tzstr(s)).tzname(), "EDT")

    def testStrEnd5(self):
        """
        Perform the testStrEnd5 utility operation under explicit compatibility rules.

        Example:
            Exercise TZTest.testStrEnd5 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        s = "EST5EDT4,95/02:00:00,298/02"
        self.assertEqual(datetime(2003, 10, 26, 0, 59, tzinfo=tzstr(s)).tzname(), "EDT")
        self.assertEqual(datetime(2003, 10, 26, 1, 00, tzinfo=tzstr(s)).tzname(), "EST")

    def testStrStart6(self):
        """
        Perform the testStrStart6 utility operation under explicit compatibility rules.

        Example:
            Exercise TZTest.testStrStart6 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        s = "EST5EDT4,J96/02:00:00,J299/02:00"
        self.assertEqual(datetime(2003, 4, 6, 1, 59, tzinfo=tzstr(s)).tzname(), "EST")
        self.assertEqual(datetime(2003, 4, 6, 2, 00, tzinfo=tzstr(s)).tzname(), "EDT")

    def testStrEnd6(self):
        """
        Perform the testStrEnd6 utility operation under explicit compatibility rules.

        Example:
            Exercise TZTest.testStrEnd6 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        s = "EST5EDT4,J96/02:00:00,J299/02"
        self.assertEqual(datetime(2003, 10, 26, 0, 59, tzinfo=tzstr(s)).tzname(), "EDT")
        self.assertEqual(datetime(2003, 10, 26, 1, 00, tzinfo=tzstr(s)).tzname(), "EST")

    def testStrCmp1(self):
        """
        Perform the testStrCmp1 utility operation under explicit compatibility rules.

        Example:
            Exercise TZTest.testStrCmp1 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(tzstr("EST5EDT"), tzstr("EST5EDT4,M4.1.0/02:00:00,M10-5-0/02:00"))

    def testStrCmp2(self):
        """
        Perform the testStrCmp2 utility operation under explicit compatibility rules.

        Example:
            Exercise TZTest.testStrCmp2 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(tzstr("EST5EDT"), tzstr("EST5EDT,4,1,0,7200,10,-1,0,7200,3600"))

    def testRangeCmp1(self):
        """
        Perform the testRangeCmp1 utility operation under explicit compatibility rules.

        Example:
            Exercise TZTest.testRangeCmp1 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(
            tzstr("EST5EDT"),
            tzrange(
                "EST",
                -18000,
                "EDT",
                -14400,
                relativedelta(hours=+2, month=4, day=1, weekday=SU(+1)),
                relativedelta(hours=+1, month=10, day=31, weekday=SU(-1)),
            ),
        )

    def testRangeCmp2(self):
        """
        Perform the testRangeCmp2 utility operation under explicit compatibility rules.

        Example:
            Exercise TZTest.testRangeCmp2 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.assertEqual(tzstr("EST5EDT"), tzrange("EST", -18000, "EDT"))

    def testFileStart1(self):
        """
        Perform the testFileStart1 utility operation under explicit compatibility rules.

        Example:
            Exercise TZTest.testFileStart1 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        tz = tzfile(StringIO(base64.decodestring(self.TZFILE_EST5EDT)))
        self.assertEqual(datetime(2003, 4, 6, 1, 59, tzinfo=tz).tzname(), "EST")
        self.assertEqual(datetime(2003, 4, 6, 2, 00, tzinfo=tz).tzname(), "EDT")

    def testFileEnd1(self):
        """
        Perform the testFileEnd1 utility operation under explicit compatibility rules.

        Example:
            Exercise TZTest.testFileEnd1 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        tz = tzfile(StringIO(base64.decodestring(self.TZFILE_EST5EDT)))
        self.assertEqual(datetime(2003, 10, 26, 0, 59, tzinfo=tz).tzname(), "EDT")
        self.assertEqual(datetime(2003, 10, 26, 1, 00, tzinfo=tz).tzname(), "EST")

    def testZoneInfoFileStart1(self):
        """
        Perform the testZoneInfoFileStart1 utility operation under explicit compatibility rules.

        Example:
            Exercise TZTest.testZoneInfoFileStart1 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        tz = zoneinfo.gettz("EST5EDT")
        self.assertEqual(datetime(2003, 4, 6, 1, 59, tzinfo=tz).tzname(), "EST")
        self.assertEqual(datetime(2003, 4, 6, 2, 00, tzinfo=tz).tzname(), "EDT")

    def testZoneInfoFileEnd1(self):
        """
        Perform the testZoneInfoFileEnd1 utility operation under explicit compatibility rules.

        Example:
            Exercise TZTest.testZoneInfoFileEnd1 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        tz = zoneinfo.gettz("EST5EDT")
        self.assertEqual(datetime(2003, 10, 26, 0, 59, tzinfo=tz).tzname(), "EDT")
        self.assertEqual(datetime(2003, 10, 26, 1, 00, tzinfo=tz).tzname(), "EST")

    def testZoneInfoOffsetSignal(self):
        """
        Perform the testZoneInfoOffsetSignal utility operation under explicit compatibility rules.

        Example:
            Exercise TZTest.testZoneInfoOffsetSignal through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        utc = gettz("UTC")
        nyc = zoneinfo.gettz("America/New_York")
        t0 = datetime(2007, 11, 4, 0, 30, tzinfo=nyc)
        t1 = t0.astimezone(utc)
        t2 = t1.astimezone(nyc)
        self.assertEquals(t0, t2)
        self.assertEquals(nyc.dst(t0), timedelta(hours=1))

    def testICalStart1(self):
        """
        Perform the testICalStart1 utility operation under explicit compatibility rules.

        Example:
            Exercise TZTest.testICalStart1 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        tz = tzical(StringIO(self.TZICAL_EST5EDT)).get()
        self.assertEqual(datetime(2003, 4, 6, 1, 59, tzinfo=tz).tzname(), "EST")
        self.assertEqual(datetime(2003, 4, 6, 2, 00, tzinfo=tz).tzname(), "EDT")

    def testICalEnd1(self):
        """
        Perform the testICalEnd1 utility operation under explicit compatibility rules.

        Example:
            Exercise TZTest.testICalEnd1 through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        tz = tzical(StringIO(self.TZICAL_EST5EDT)).get()
        self.assertEqual(datetime(2003, 10, 26, 0, 59, tzinfo=tz).tzname(), "EDT")
        self.assertEqual(datetime(2003, 10, 26, 1, 00, tzinfo=tz).tzname(), "EST")

    def testRoundNonFullMinutes(self):
        # This timezone has an offset of 5992 seconds in 1900-01-01.
        """
        Perform the testRoundNonFullMinutes utility operation under explicit compatibility rules.

        Example:
            Exercise TZTest.testRoundNonFullMinutes through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        tz = tzfile(StringIO(base64.decodestring(self.EUROPE_HELSINKI)))
        self.assertEquals(str(datetime(1900, 1, 1, 0, 0, tzinfo=tz)), "1900-01-01 00:00:00+01:40")

    def testLeapCountDecodesProperly(self):
        # This timezone has leapcnt, and failed to decode until
        # Eugene Oden notified about the issue.
        """
        Perform the testLeapCountDecodesProperly utility operation under explicit compatibility rules.

        Example:
            Exercise TZTest.testLeapCountDecodesProperly through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        tz = tzfile(StringIO(base64.decodestring(self.NEW_YORK)))
        self.assertEquals(datetime(2007, 3, 31, 20, 12).tzname(), None)

    def testBrokenIsDstHandling(self):
        # tzrange._isdst() was using a date() rather than a datetime().
        # Issue reported by Lennart Regebro.
        """
        Perform the testBrokenIsDstHandling utility operation under explicit compatibility rules.

        Example:
            Exercise TZTest.testBrokenIsDstHandling through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        dt = datetime(2007, 8, 6, 4, 10, tzinfo=tzutc())
        self.assertEquals(
            dt.astimezone(tz=gettz("GMT+2")),
            datetime(2007, 8, 6, 6, 10, tzinfo=tzstr("GMT+2")),
        )

    def testGMTHasNoDaylight(self):
        # tzstr("GMT+2") improperly considered daylight saving time.
        # Issue reported by Lennart Regebro.
        """
        Perform the testGMTHasNoDaylight utility operation under explicit compatibility rules.

        Example:
            Exercise TZTest.testGMTHasNoDaylight through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        dt = datetime(2007, 8, 6, 4, 10)
        self.assertEquals(gettz("GMT+2").dst(dt), timedelta(0))

    def testGMTOffset(self):
        # GMT and UTC offsets have inverted signal when compared to the
        # usual TZ variable handling.
        """
        Perform the testGMTOffset utility operation under explicit compatibility rules.

        Example:
            Exercise TZTest.testGMTOffset through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        dt = datetime(2007, 8, 6, 4, 10, tzinfo=tzutc())
        self.assertEquals(
            dt.astimezone(tz=tzstr("GMT+2")),
            datetime(2007, 8, 6, 6, 10, tzinfo=tzstr("GMT+2")),
        )
        self.assertEquals(
            dt.astimezone(tz=gettz("UTC-2")),
            datetime(2007, 8, 6, 2, 10, tzinfo=tzstr("UTC-2")),
        )


if __name__ == "__main__":
    unittest.main()

# vim:ts=4:sw=4

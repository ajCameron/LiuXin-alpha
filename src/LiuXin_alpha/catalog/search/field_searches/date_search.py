
"""
Search inside a date field.
"""

from __future__ import division, absolute_import, print_function, unicode_literals, annotations

import re
from datetime import timedelta

from typing import Pattern, Any, Iterable, Iterator, Optional, Tuple, Union, Callable, TypeVar, Type

from LiuXin_alpha.utils.date import parse_date, UNDEFINED_DATE, now, dt_as_local
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode as unicode, iteritems
from LiuXin_alpha.utils.localization import _
from LiuXin_alpha.utils.search_query_parser import ParseException
from LiuXin_alpha.utils.text.icu import lower as icu_lower



class DateSearch:
    """
    Preform a search in a date type column.
    """

    local_today: set[str]
    local_yesterday: set[str]
    local_thismonth: set[str]
    daysago_pat: Pattern[str]

    # {{{
    def __init__(self) -> None:
        """
        Startup a date search.
        """
        self.operators = {
            "=": (1, self.eq),
            "!=": (2, self.ne),
            ">": (1, self.gt),
            ">=": (2, self.ge),
            "<": (1, self.lt),
            "<=": (2, self.le),
        }
        self.local_today = {"_today", "today", icu_lower(_("today"))}
        self.local_yesterday = {"_yesterday", "yesterday", icu_lower(_("yesterday"))}
        self.local_thismonth = {"_thismonth", "thismonth", icu_lower(_("thismonth"))}
        self.daysago_pat = re.compile(r"(%s|daysago|_daysago)$" % _("daysago"))

    def eq(self, dbdate: Any, query: Any, field_count: int) -> bool:
        """
        Equality check.

        :param dbdate:
        :param query:
        :param field_count:

        :return:
        """
        if dbdate.year == query.year:
            if field_count == 1:
                return True
            if dbdate.month == query.month:
                if field_count == 2:
                    return True
                return dbdate.day == query.day
        return False

    def ne(self, dbdate: Any, query: Any, field_count: int) -> bool:
        """
        Not equal check.

        :param dbdate:
        :param query:
        :param field_count:

        :return:
        """
        return not self.eq(dbdate, query, field_count)

    def gt(self, dbdate: Any, query: Any, field_count: int) -> bool:
        """
        Greater than check.

        :param dbdate:
        :param query:
        :param field_count:
        :return:
        """
        if dbdate.year > query.year:
            return True
        if field_count > 1 and dbdate.year == query.year:
            if dbdate.month > query.month:
                return True
            return field_count == 3 and dbdate.month == query.month and dbdate.day > query.day
        return False

    def le(self, dbdate: Any, query: Any, field_count: int) -> bool:
        """
        Less than, equals to check.

        :param args:
        :return:
        """
        return not self.gt(dbdate, query, field_count)

    def lt(self, dbdate: Any, query: Any, field_count: int) -> bool:
        """
        Less than check.

        :param dbdate:
        :param query:
        :param field_count:
        :return:
        """
        if dbdate.year < query.year:
            return True
        if field_count > 1 and dbdate.year == query.year:
            if dbdate.month < query.month:
                return True
            return field_count == 3 and dbdate.month == query.month and dbdate.day < query.day
        return False

    def ge(self, dbdate: Any, query: Any, field_count: int) -> bool:
        """
        Greater than or equal check.

        :param args:
        :return:
        """
        return not self.lt(dbdate, query, field_count)

    def __call__(
            self,
            query: str,
            field_iter: Callable[[], Iterable[tuple[Any, Iterable[int]]]]) -> set[int]:
        matches = set()
        if len(query) < 2:
            return matches

        if query == "false":
            for v, book_ids in field_iter():
                if isinstance(v, (str, unicode)):
                    v = parse_date(v)
                if v is None or v <= UNDEFINED_DATE:
                    matches |= book_ids
            return matches

        if query == "true":
            for v, book_ids in field_iter():
                if isinstance(v, (str, unicode)):
                    v = parse_date(v)
                if v is not None and v > UNDEFINED_DATE:
                    matches |= book_ids
            return matches

        relop = self.operators["="][-1]
        for k, op in iteritems(self.operators):
            if query.startswith(k):
                p, relop = op
                query = query[p:]

        if query in self.local_today:
            qd = now()
            field_count = 3

        elif query in self.local_yesterday:
            qd = now() - timedelta(1)
            field_count = 3

        elif query in self.local_thismonth:
            qd = now()
            field_count = 2

        else:
            m = self.daysago_pat.search(query)
            if m is not None:
                num = query[: -len(m.group(1))]
                try:
                    qd = now() - timedelta(int(num))
                except (TypeError, ValueError, OverflowError):
                    raise ParseException(_("Number conversion error: {0}").format(num))
                field_count = 3
            else:
                try:
                    qd = parse_date(query, as_utc=False)
                except (TypeError, ValueError, OverflowError):
                    raise ParseException(_("Date conversion error: {0}").format(query))
                if "-" in query:
                    field_count = query.count("-") + 1
                else:
                    field_count = query.count("/") + 1

        for v, book_ids in field_iter():
            if isinstance(v, (str, unicode)):
                v = parse_date(v)
            if v is not None and relop(dt_as_local(v), qd, field_count):
                matches |= book_ids

        return matches

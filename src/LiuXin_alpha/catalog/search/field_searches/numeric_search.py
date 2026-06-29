
"""
Preform a numeric search of a field.
"""


from __future__ import division, absolute_import, print_function, unicode_literals, annotations

from typing import Union, Iterable, Callable, Any

from LiuXin_alpha.utils.localization import _
from LiuXin_alpha.utils.search_query_parser import ParseException
from LiuXin_alpha.utils.libraries.liuxin_six import iteritems


class NumericSearch:  # {{{
    """
    Search the database for a numeric object subject to certain constraints.
    """

    def __init__(self) -> None:
        """
        Startup the numeric search operator.
        """
        self.operators = {
            "=": (1, lambda r, q: r == q),
            ">": (1, lambda r, q: r is not None and r > q),
            "<": (1, lambda r, q: r is not None and r < q),
            "!=": (2, lambda r, q: r != q),
            ">=": (2, lambda r, q: r is not None and r >= q),
            "<=": (2, lambda r, q: r is not None and r <= q),
        }

    def __call__(
            self,
            query: str,
            field_iter: Callable[[], Iterable[tuple[Union[int, float], Iterable[int]]]],
            location: str,
            datatype: str,
            candidates: set[int],
            is_many=False):
        matches = set()
        if not query:
            return matches

        q = ""
        cast = adjust = lambda x: x
        dt = datatype

        if is_many and query in {"true", "false"}:
            valcheck = lambda x: True
            if datatype == "rating":
                valcheck = lambda x: x is not None and x > 0
            found = set()
            for val, book_ids in field_iter():
                if valcheck(val):
                    found |= book_ids
            return found if query == "true" else candidates - found

        if query == "false":
            if location == "cover":
                relop = lambda x, y: not bool(x)
            else:
                relop = lambda x, y: x is None

        elif query == "true":
            if location == "cover":
                relop = lambda x, y: bool(x)
            else:
                relop = lambda x, y: x is not None

        else:
            relop = None
            for k, op in iteritems(self.operators):
                if query.startswith(k):
                    p, relop = op
                    query = query[p:]
            if relop is None:
                p, relop = self.operators["="]

            cast = int
            if dt == "rating":
                cast = lambda x: 0 if x is None else int(x)
                adjust = lambda x: x / 2
            elif dt in ("float", "composite"):
                cast = float

            if len(query) > 1:
                mult = query[-1].lower()
                mult = {"k": 1024.0, "m": 1024.0**2, "g": 1024.0**3}.get(mult, 1.0)
                if mult != 1.0:
                    query = query[:-1]
            else:
                mult = 1.0

            try:
                q = cast(query) * mult
            except:
                raise ParseException(_("Non-numeric value in query: {0}").format(query))

        qfalse = query == "false"
        for val, book_ids in field_iter():
            if val is None:
                if qfalse:
                    matches |= book_ids
                continue
            try:
                v = cast(val)
            except:
                v = None
            if v:
                v = adjust(v)
            if relop(v, q):
                matches |= book_ids
        return matches

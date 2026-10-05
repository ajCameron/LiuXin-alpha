
"""
Evaluate numeric catalog fields with typed comparison operators.

The module keeps validation, normalization and host mutation boundaries explicit.

Example:
    Exercise numeric search through its owning regression module::

        python -m pytest -q tests/catalog/test_field_search_operators.py
"""


from __future__ import division, absolute_import, print_function, unicode_literals, annotations

from typing import Union, Iterable, Callable, Any

from LiuXin_alpha.utils.localization import _
from LiuXin_alpha.utils.search_query_parser import ParseException
from LiuXin_alpha.utils.libraries.liuxin_six import iteritems


class NumericSearch:  # {{{
    """
    Search the database for a numeric object subject to certain constraints.

    Example:
        Exercise NumericSearch through its owning regression module::

            python -m pytest -q tests/catalog/test_field_search_operators.py
    """

    def __init__(self) -> None:
        """
        Startup the numeric search operator.

        Example:
            Exercise NumericSearch.init through its owning regression module::

                python -m pytest -q tests/catalog/test_field_search_operators.py


        :return: None; the function records state or raises through its assertions.
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
        """
        Evaluate or build the NumericSearch operation.

        Example:
            Exercise NumericSearch.call through its owning regression module::

                python -m pytest -q tests/catalog/test_field_search_operators.py


        :param query: Parsed or textual catalog query to evaluate.
        :param field_iter: Value supplied for field iter under the catalog contract.
        :param location: Value supplied for location under the catalog contract.
        :param datatype: Value supplied for datatype under the catalog contract.
        :param candidates: Optional candidate identities restricting the search universe.
        :param is_many: Value supplied for is many under the catalog contract.
        :return: The deterministic value, row, identity or collection described above.
        """
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
            for k, op in sorted(
                iteritems(self.operators),
                key=lambda item: len(item[0]),
                reverse=True,
            ):
                if query.startswith(k):
                    p, relop = op
                    query = query[p:]
                    break
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
            except (TypeError, ValueError, OverflowError):
                raise ParseException(_("Non-numeric value in query: {0}").format(query))

        qfalse = query == "false"
        for val, book_ids in field_iter():
            if val is None:
                if qfalse:
                    matches |= book_ids
                continue
            try:
                v = cast(val)
            except (TypeError, ValueError, OverflowError):
                v = None
            if v:
                v = adjust(v)
            if relop(v, q):
                matches |= book_ids
        return matches

"""
xISBN helper.

Historically this queried OCLC's xISBN service for related ISBN pools. That
service is decommissioned, so network querying is disabled by default while
keeping the API surface for compatibility.
"""

from __future__ import annotations

import json
import re
import threading
from typing import Any

from LiuXin_alpha.metadata.web_sources.base import browser
from LiuXin_alpha.utils.logging import default_log

__license__ = "GPL v3"
__copyright__ = "2010, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"


class xISBN:
    """
    Find ISBN numbers for related editions of a book.
    """

    QUERY = "http://xisbn.worldcat.org/webservices/xid/isbn/%s?method=getEditions&format=json&fl=form,year,lang,ed"
    BOOK_FORMS = frozenset(("BA", "BC", "BB", "DA"))

    def __init__(self, enable_network: bool = False):
        self.lock = threading.RLock()
        self._data: list[list[dict[str, Any]]] = []
        self._map: dict[str, int] = {}
        self.isbn_pat = re.compile(r"[^0-9X]", re.IGNORECASE)

        # xISBN was decommissioned by OCLC in 2018. Keep disabled by default.
        self.enable_network = bool(enable_network)
        self.service_available = self.enable_network

    def purify(self, isbn) -> str:
        return self.isbn_pat.sub("", str(isbn or "").upper())

    def _fetch_raw(self, isbn: str, timeout: float = 20) -> bytes:
        url = self.QUERY % isbn
        return browser().open_novisit(url, timeout=timeout).read()

    def fetch_data(self, isbn: str) -> list[dict[str, Any]]:
        if not self.enable_network:
            return []

        payload = self._fetch_raw(isbn)
        data = json.loads(payload)
        if data.get("stat") != "ok":
            return []

        records = data.get("list", [])
        ans: list[dict[str, Any]] = []
        for rec in records:
            forms = [x for x in rec.get("form", []) if x in self.BOOK_FORMS]
            if forms:
                ans.append(rec)
        return ans

    def isbns_in_data(self, data):
        for rec in data:
            for raw in rec.get("isbn", []):
                isbn = self.purify(raw)
                if isbn:
                    yield isbn

    def get_data(self, isbn: str) -> list[dict[str, Any]]:
        pure = self.purify(isbn)
        if not pure:
            return []

        with self.lock:
            if pure not in self._map:
                try:
                    data = self.fetch_data(pure)
                except Exception as err:
                    default_log.log_exception(
                        "xISBN fetch failed.",
                        err,
                        "DEBUG",
                        ("isbn", pure),
                    )
                    data = []

                bucket = len(self._data)
                self._data.append(data)
                for related in self.isbns_in_data(data):
                    self._map[related] = bucket
                self._map[pure] = bucket

            return self._data[self._map[pure]]

    def get_associated_isbns(self, isbn: str):
        return set(self.isbns_in_data(self.get_data(isbn)))

    def get_isbn_pool(self, isbn: str):
        data = self.get_data(isbn)
        isbns = frozenset(self.isbns_in_data(data))

        min_year = None
        for rec in data:
            try:
                year = int(rec.get("year"))
            except Exception:
                continue
            min_year = year if min_year is None else min(min_year, year)

        return isbns, min_year


xisbn = xISBN()


__all__ = [
    "xISBN",
    "xisbn",
]

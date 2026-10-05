"""
Resolve related-edition ISBN pools with optional network access and process-wide caching.

The module keeps network, parsing, caching, cancellation and result-order behavior
explicit for callers.

Example:
    Exercise xisbn with the owning regression module::

        python -m pytest -q tests/metadata/web_sources/test_web_sources_xisbn.py
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

    Example:
        Exercise xISBN with the owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_xisbn.py
    """

    QUERY = "http://xisbn.worldcat.org/webservices/xid/isbn/%s?method=getEditions&format=json&fl=form,year,lang,ed"
    BOOK_FORMS = frozenset(("BA", "BC", "BB", "DA"))

    def __init__(self, enable_network: bool = False):
        """
        Initialize xisbn state while preserving shared source configuration and caches.

        Example:
            Exercise xISBN.  init   with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_xisbn.py


        :param enable_network: Allow live xISBN requests when true; otherwise use cached
            data only.
        :return: None.
        """
        self.lock = threading.RLock()
        self._data: list[list[dict[str, Any]]] = []
        self._map: dict[str, int] = {}
        self.isbn_pat = re.compile(r"[^0-9X]", re.IGNORECASE)

        # xISBN was decommissioned by OCLC in 2018. Keep disabled by default.
        self.enable_network = bool(enable_network)
        self.service_available = self.enable_network

    def purify(self, isbn) -> str:
        """
        Perform the xisbn purify operation with explicit ordering and failure behavior.

        Example:
            Exercise xISBN.purify with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_xisbn.py


        :param isbn: ISBN value used for direct lookup or related-edition resolution.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        return self.isbn_pat.sub("", str(isbn or "").upper())

    def _fetch_raw(self, isbn: str, timeout: float = 20) -> bytes:
        """
        Perform the provider fetch raw operation with explicit timeout and response policy.

        Example:
            Exercise xISBN. fetch raw with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_xisbn.py


        :param isbn: ISBN value used for direct lookup or related-edition resolution.
        :param timeout: Maximum duration in seconds for the network or worker operation.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        url = self.QUERY % isbn
        return browser().open_novisit(url, timeout=timeout).read()

    def fetch_data(self, isbn: str) -> list[dict[str, Any]]:
        """
        Perform the xisbn fetch data operation with explicit ordering and failure behavior.

        Example:
            Exercise xISBN.fetch data with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_xisbn.py


        :param isbn: ISBN value used for direct lookup or related-edition resolution.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
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
        """
        Perform the xisbn isbns in data operation with explicit ordering and failure behavior.

        Example:
            Exercise xISBN.isbns in data with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_xisbn.py


        :param data: Bytes, mapping or serialized cache data consumed by the operation.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        for rec in data:
            for raw in rec.get("isbn", []):
                isbn = self.purify(raw)
                if isbn:
                    yield isbn

    def get_data(self, isbn: str) -> list[dict[str, Any]]:
        """
        Return data under this provider's cache and fallback policy.

        Example:
            Exercise xISBN.get data with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_xisbn.py


        :param isbn: ISBN value used for direct lookup or related-edition resolution.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
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
        """
        Return associated isbns under this provider's cache and fallback policy.

        Example:
            Exercise xISBN.get associated isbns with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_xisbn.py


        :param isbn: ISBN value used for direct lookup or related-edition resolution.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
        return set(self.isbns_in_data(self.get_data(isbn)))

    def get_isbn_pool(self, isbn: str):
        """
        Return isbn pool under this provider's cache and fallback policy.

        Example:
            Exercise xISBN.get isbn pool with the owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_xisbn.py


        :param isbn: ISBN value used for direct lookup or related-edition resolution.
        :return: The normalized provider value, metadata result or collection described
            above.
        """
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

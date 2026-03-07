"""
Open Library metadata source.

Currently this source is cover-only (via ISBN lookup against the Open Library
cover API), matching calibre behavior.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping

from LiuXin_alpha.metadata.web_sources.base import Source
from LiuXin_alpha.utils.localization import trans as _

__license__ = "GPL v3"
__copyright__ = "2011, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"

_STRIP_NON_ISBN = re.compile(r"[^0-9X]", re.IGNORECASE)


def _first_value(raw):
    if raw is None:
        return None
    if isinstance(raw, (str, bytes)):
        return raw
    if isinstance(raw, Mapping):
        for key in raw:
            return key
        return None
    if isinstance(raw, Iterable):
        for item in raw:
            return item
    return raw


def _coerce_text(raw) -> str | None:
    if raw is None:
        return None
    if isinstance(raw, bytes):
        return raw.decode("utf-8", "replace")
    if isinstance(raw, str):
        return raw
    try:
        return str(raw)
    except Exception:
        return None


def _normalize_isbn(raw) -> str | None:
    text = _coerce_text(raw)
    if text is None:
        return None
    text = text.strip().upper()
    if not text:
        return None
    compact = _STRIP_NON_ISBN.sub("", text)
    if compact:
        return compact
    return None


def _isbn_from_identifiers(identifiers) -> str | None:
    if not isinstance(identifiers, Mapping):
        return None

    for key in ("isbn", "isbn13", "isbn10"):
        if key in identifiers:
            value = _first_value(identifiers.get(key))
            text = _normalize_isbn(value)
            if text:
                return text
    return None


class OpenLibrary(Source):
    name = "Open Library"
    version = (1, 0, 2)
    description = _("Downloads covers from The Open Library")
    capabilities = frozenset({"cover"})

    OPENLIBRARY = "https://covers.openlibrary.org/b/isbn/%s-L.jpg?default=false"

    def get_book_url(self, identifiers):
        isbn = _isbn_from_identifiers(identifiers or {})
        if not isbn:
            return None
        return ("isbn", isbn, f"https://openlibrary.org/isbn/{isbn}")

    def get_cached_cover_url(self, identifiers):
        isbn = _isbn_from_identifiers(identifiers or {})
        if not isbn:
            return None
        return self.OPENLIBRARY % isbn

    def download_cover(
        self,
        log,
        result_queue,
        abort,
        title=None,
        authors=None,
        identifiers=None,
        timeout=30,
        get_best_cover=False,
    ):
        del title, authors, get_best_cover
        identifiers = identifiers or {}

        if abort.is_set():
            return

        isbn = _isbn_from_identifiers(identifiers)
        if not isbn:
            return

        try:
            payload = self.browser().open_novisit(self.OPENLIBRARY % isbn, timeout=timeout).read()
            if payload:
                result_queue.put((self, payload))
        except Exception as err:
            if callable(getattr(err, "getcode", None)) and err.getcode() == 404:
                log.error("No cover for ISBN: %r found" % isbn)
            else:
                log.exception("Failed to download cover for ISBN:", isbn)


__all__ = ["OpenLibrary"]

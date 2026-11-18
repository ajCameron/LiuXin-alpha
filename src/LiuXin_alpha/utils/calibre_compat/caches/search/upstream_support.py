"""Request-local settings and clock for the attributed Calibre matcher."""

import unicodedata
from collections.abc import Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime

from LiuXin_alpha.utils.text.icu import primary_contains

_context = ContextVar("calibre_search_settings", default=None)
_clock = ContextVar("calibre_search_clock", default=None)
_DEFAULTS = {
    "use_primary_find_in_search": True,
    "case_sensitive": False,
    "limit_search_columns": False,
    "limit_search_columns_to": (),
}


class _Preferences(Mapping):
    def __getitem__(self, key):
        return (_context.get() or {}).get(key, _DEFAULTS.get(key))

    def __iter__(self):
        return iter(_DEFAULTS)

    def __len__(self):
        return len(_DEFAULTS)


prefs = _Preferences()


def now():
    return _clock.get() or datetime.now().astimezone()


@contextmanager
def search_context(settings, instant=None):
    setting_token = _context.set(settings)
    clock_token = _clock.set(instant or _clock.get() or datetime.now().astimezone())
    try:
        yield
    finally:
        _clock.reset(clock_token)
        _context.reset(setting_token)


def ascii_primary_no_punc_matcher(query):
    # Retain one collation path; the optional upstream ASCII optimization is omitted.
    return None


def primary_no_punc_contains(query, text):
    def strip(value):
        return "".join(
            c for c in value if not unicodedata.category(c).startswith(("P", "Z", "C"))
        )

    return primary_contains(strip(query), strip(text))

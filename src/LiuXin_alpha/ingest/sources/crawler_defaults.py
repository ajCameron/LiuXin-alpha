"""Shared preference defaults for HTML crawler discovery sources."""

from __future__ import annotations


CRAWLER_HTTP_MAX_REQUESTS_PER_HOUR_DEFAULT = 1200.0
CRAWLER_HTTP_MAX_REQUESTS_PER_HOUR_PREF_KEY = "crawler_http_max_requests_per_hour_default"
LEGACY_WGET_HTTP_MAX_REQUESTS_PER_HOUR_PREF_KEY = "wget_http_max_requests_per_hour_default"
LEGACY_NATIVE_HTML_MAX_REQUESTS_PER_HOUR_PREF_KEY = "native_html_max_requests_per_hour_default"
_MISSING = object()


def get_default_crawler_http_requests_per_hour(*legacy_pref_keys: str) -> float:
    default = float(CRAWLER_HTTP_MAX_REQUESTS_PER_HOUR_DEFAULT)
    try:
        from LiuXin_alpha.preferences import preferences

        raw = preferences.get(CRAWLER_HTTP_MAX_REQUESTS_PER_HOUR_PREF_KEY, _MISSING)
        if raw is _MISSING:
            for key in legacy_pref_keys:
                legacy_raw = preferences.get(str(key), _MISSING)
                if legacy_raw is _MISSING or legacy_raw is None:
                    continue
                return float(legacy_raw)
            return default
        if raw is None:
            return default
        return float(raw)
    except Exception:
        return default


__all__ = [
    "CRAWLER_HTTP_MAX_REQUESTS_PER_HOUR_DEFAULT",
    "CRAWLER_HTTP_MAX_REQUESTS_PER_HOUR_PREF_KEY",
    "LEGACY_WGET_HTTP_MAX_REQUESTS_PER_HOUR_PREF_KEY",
    "LEGACY_NATIVE_HTML_MAX_REQUESTS_PER_HOUR_PREF_KEY",
    "get_default_crawler_http_requests_per_hour",
]

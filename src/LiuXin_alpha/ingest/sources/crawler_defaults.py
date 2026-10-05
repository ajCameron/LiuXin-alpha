"""
Resolve the shared crawler rate preference with ordered legacy-key fallback.

The built-in fallback is 1200 requests per hour. Preference access is deferred
until the resolver is called; values are float-converted here without finite or
positive checks. Concrete crawlers interpret disabling and invalid rates later.
"""

from __future__ import annotations


CRAWLER_HTTP_MAX_REQUESTS_PER_HOUR_DEFAULT = 1200.0
CRAWLER_HTTP_MAX_REQUESTS_PER_HOUR_PREF_KEY = "crawler_http_max_requests_per_hour_default"
LEGACY_WGET_HTTP_MAX_REQUESTS_PER_HOUR_PREF_KEY = "wget_http_max_requests_per_hour_default"
LEGACY_NATIVE_HTML_MAX_REQUESTS_PER_HOUR_PREF_KEY = "native_html_max_requests_per_hour_default"
_MISSING = object()


def get_default_crawler_http_requests_per_hour(*legacy_pref_keys: str) -> float:
    """
    Read the modern rate preference, using legacy keys only when it is absent.

    An explicit modern None chooses the built-in default rather than legacy keys.
    For an absent modern key, use the first present non-None legacy value. Any
    import/access/conversion Exception returns the built-in default immediately,
    not the next legacy key. Negative and nonfinite floats are retained.

    Example:
        >>> rate = get_default_crawler_http_requests_per_hour()  # doctest: +SKIP


    :param legacy_pref_keys: Preference keys tried in caller order only if the modern key is missing.
    :return: Float preference value or the built-in requests-per-hour fallback.
    """
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

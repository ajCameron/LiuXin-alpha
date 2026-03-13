from .api import DiscoveredUrlCallback, DiscoverySourceAPI, LogLineCallback, ObservedUrlCallback
from .native_html import (
    NATIVE_HTML_MAX_REQUESTS_PER_HOUR_DEFAULT,
    NATIVE_HTML_MAX_REQUESTS_PER_HOUR_PREF_KEY,
    NativeHtmlBackendOptions,
    NativeHtmlDiscoverySource,
    get_default_native_html_requests_per_hour,
)
from .wget_html import (
    WGET_HTTP_MAX_REQUESTS_PER_HOUR_DEFAULT,
    WGET_HTTP_MAX_REQUESTS_PER_HOUR_PREF_KEY,
    WgetBackendOptions,
    WgetHtmlDiscoverySource,
    get_default_wget_http_requests_per_hour,
)

__all__ = [
    "DiscoveredUrlCallback",
    "DiscoverySourceAPI",
    "LogLineCallback",
    "ObservedUrlCallback",
    "NATIVE_HTML_MAX_REQUESTS_PER_HOUR_DEFAULT",
    "NATIVE_HTML_MAX_REQUESTS_PER_HOUR_PREF_KEY",
    "NativeHtmlBackendOptions",
    "NativeHtmlDiscoverySource",
    "WGET_HTTP_MAX_REQUESTS_PER_HOUR_DEFAULT",
    "WGET_HTTP_MAX_REQUESTS_PER_HOUR_PREF_KEY",
    "WgetBackendOptions",
    "WgetHtmlDiscoverySource",
    "get_default_native_html_requests_per_hour",
    "get_default_wget_http_requests_per_hour",
]

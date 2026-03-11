"""Exports for the wget-backed read-only HTML crawler store backend."""

from .wget_html_storage_backend import (
    WGET_HTTP_MAX_REQUESTS_PER_HOUR_DEFAULT,
    WGET_HTTP_MAX_REQUESTS_PER_HOUR_PREF_KEY,
    WgetBackendOptions,
    WgetHtmlReadOnlyStorageBackend,
    get_default_wget_http_requests_per_hour,
)
from .wget_utils import WgetNotInstalledError

__all__ = [
    "WGET_HTTP_MAX_REQUESTS_PER_HOUR_DEFAULT",
    "WGET_HTTP_MAX_REQUESTS_PER_HOUR_PREF_KEY",
    "WgetBackendOptions",
    "WgetHtmlReadOnlyStorageBackend",
    "WgetNotInstalledError",
    "get_default_wget_http_requests_per_hour",
]


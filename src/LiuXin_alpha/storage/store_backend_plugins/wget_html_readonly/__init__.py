"""
Export wget discovery controls, its configured HTTP Store, and lookup exception.

Exports retain their defining implementation objects, including the shared rate
preference aliases. Importing the package does not execute wget or open a Store.
Legacy Location/FileInfo names live in their dedicated compatibility modules.
"""

from .wget_html_storage_backend import (
    WGET_HTTP_MAX_REQUESTS_PER_HOUR_DEFAULT,
    WGET_HTTP_MAX_REQUESTS_PER_HOUR_PREF_KEY,
    WgetBackendOptions,
    WgetHtmlReadOnlyStorageBackend,
    get_default_crawler_http_requests_per_hour,
    get_default_wget_http_requests_per_hour,
)
from .wget_utils import WgetNotInstalledError

__all__ = [
    "WGET_HTTP_MAX_REQUESTS_PER_HOUR_DEFAULT",
    "WGET_HTTP_MAX_REQUESTS_PER_HOUR_PREF_KEY",
    "WgetBackendOptions",
    "WgetHtmlReadOnlyStorageBackend",
    "WgetNotInstalledError",
    "get_default_crawler_http_requests_per_hour",
    "get_default_wget_http_requests_per_hour",
]

"""
Expose wget discovery controls, the HTTP Store, and the tool-lookup exception.

Locations and observed file metadata use the shared storage API values.
The imported implementation owns backend construction and physical operations.
"""

from LiuXin_alpha.ingest.sources.wget_utils import WgetNotInstalledError

from .wget_html_storage_backend import (
    WGET_HTTP_MAX_REQUESTS_PER_HOUR_DEFAULT,
    WGET_HTTP_MAX_REQUESTS_PER_HOUR_PREF_KEY,
    WgetBackendOptions,
    WgetHtmlReadOnlyStorageBackend,
    get_default_crawler_http_requests_per_hour,
    get_default_wget_http_requests_per_hour,
)

__all__ = [
    "WGET_HTTP_MAX_REQUESTS_PER_HOUR_DEFAULT",
    "WGET_HTTP_MAX_REQUESTS_PER_HOUR_PREF_KEY",
    "WgetBackendOptions",
    "WgetHtmlReadOnlyStorageBackend",
    "WgetNotInstalledError",
    "get_default_crawler_http_requests_per_hour",
    "get_default_wget_http_requests_per_hour",
]

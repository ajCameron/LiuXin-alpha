"""
Expose native HTML discovery options and the configured read-only HTTP Store.

Locations and observed file metadata use the shared storage API values.
The imported implementation owns backend construction and physical operations.
"""

from .native_html_storage_backend import (
    NATIVE_HTML_MAX_REQUESTS_PER_HOUR_DEFAULT,
    NATIVE_HTML_MAX_REQUESTS_PER_HOUR_PREF_KEY,
    NativeHtmlBackendOptions,
    NativeHtmlReadOnlyStorageBackend,
    get_default_crawler_http_requests_per_hour,
    get_default_native_html_requests_per_hour,
)

__all__ = [
    "NATIVE_HTML_MAX_REQUESTS_PER_HOUR_DEFAULT",
    "NATIVE_HTML_MAX_REQUESTS_PER_HOUR_PREF_KEY",
    "NativeHtmlBackendOptions",
    "NativeHtmlReadOnlyStorageBackend",
    "get_default_crawler_http_requests_per_hour",
    "get_default_native_html_requests_per_hour",
]

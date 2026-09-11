"""
Export the native HTML discovery options and configured read-only HTTP Store.

Exports retain the implementation objects and shared preference aliases. Importing
this package does not construct a Store or start network discovery. Legacy
Location/FileInfo aliases remain in their dedicated compatibility modules.
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

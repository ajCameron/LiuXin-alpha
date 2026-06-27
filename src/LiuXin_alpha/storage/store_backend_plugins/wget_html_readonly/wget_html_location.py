"""Read-only location wrapper for URLs discovered via wget crawling."""

from __future__ import annotations

from LiuXin_alpha.storage.store_backend_plugins.native_html_readonly.native_html_location import (
    NativeHtmlReadOnlyStoreLocation,
)


class WgetHtmlReadOnlyStoreLocation(NativeHtmlReadOnlyStoreLocation):
    pass

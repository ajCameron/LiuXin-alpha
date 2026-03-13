"""Compatibility forwarders for wget discovery utilities."""

from LiuXin_alpha.ingest.sources.wget_utils import (
    WgetNotInstalledError,
    WgetResult,
    extract_http_urls_from_wget_output,
    run_wget,
    which_wget,
)

__all__ = [
    "WgetNotInstalledError",
    "WgetResult",
    "extract_http_urls_from_wget_output",
    "run_wget",
    "which_wget",
]

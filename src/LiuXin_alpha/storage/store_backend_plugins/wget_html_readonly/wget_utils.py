"""
Re-export wget process and URL-extraction helpers from the ingest source owner.

These are object aliases, not call wrappers. Their timeout, capture, error, and
URL-filtering contracts remain those of ingest.sources.wget_utils. Importing
this compatibility module neither locates nor runs a wget executable.
"""

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

"""
Retain the historical FTP location name as an exact alias of the shared Location.

FtpReadOnlyStoreLocation adds no FTP path methods or validation. Store identity and
opaque-key semantics are those of Location; the configured Store and raw driver
perform ownership checks and FTP path/URI parsing.
"""

from LiuXin_alpha.storage.api import Location


FtpReadOnlyStoreLocation = Location


__all__ = ["FtpReadOnlyStoreLocation"]

"""
Preserve the historical native-HTML single-file name as an alias of FileInfo.

No legacy constructor or HTTP I/O wrapper is supplied: both names refer to the
same storage API information class. Byte access belongs to the configured Store.
"""

from LiuXin_alpha.storage.api import FileInfo


NativeHtmlReadOnlySingleFile = FileInfo


__all__ = ["NativeHtmlReadOnlySingleFile"]

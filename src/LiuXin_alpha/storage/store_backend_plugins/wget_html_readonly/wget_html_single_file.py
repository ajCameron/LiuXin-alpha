"""
Preserve the historical wget-HTML single-file name as an alias of FileInfo.

The alias adds no byte access or legacy constructor translation. Callers receive
the same storage API information value under either import name.
"""

from LiuXin_alpha.storage.api import FileInfo


WgetHtmlReadOnlySingleFile = FileInfo


__all__ = ["WgetHtmlReadOnlySingleFile"]

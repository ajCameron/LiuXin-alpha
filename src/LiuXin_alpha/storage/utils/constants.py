"""
Provide the shared default chunk size for streamed storage operations.

DEFAULT_STORAGE_CHUNK_SIZE is one mebibyte per requested read chunk. It does not
bound total object size or total memory use, and individual callers may override it.

Example:
    >>> DEFAULT_STORAGE_CHUNK_SIZE
    1048576
"""

DEFAULT_STORAGE_CHUNK_SIZE = 1024 * 1024
"""Default number of bytes transferred per streamed utility operation."""


__all__ = ["DEFAULT_STORAGE_CHUNK_SIZE"]

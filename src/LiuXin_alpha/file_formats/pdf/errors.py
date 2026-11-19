"""
Name the ways reading a PDF's structure can fail.

Example:
    An encrypted file that cannot be opened is a kind of unreadable file::

        >>> issubclass(PdfEncryptedError, PdfReadError)
        True
"""

from __future__ import annotations

__all__ = ["PdfEncryptedError", "PdfReadError"]


class PdfReadError(ValueError):
    """
    Signal a PDF whose structure cannot be read, or that exceeds a reading limit.

    Example:
        >>> str(PdfReadError("no cross-reference table"))
        'no cross-reference table'
    """


class PdfEncryptedError(PdfReadError):
    """
    Signal an encrypted PDF that cannot be opened without a password or an optional backend.

    Example:
        >>> str(PdfEncryptedError("this PDF needs a password to open"))
        'this PDF needs a password to open'
    """

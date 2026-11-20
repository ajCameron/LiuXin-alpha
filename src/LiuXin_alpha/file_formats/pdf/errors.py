"""
Name the ways reading a PDF's structure can fail.

Example:
    An encrypted file that cannot be opened is a kind of unreadable file::

        >>> issubclass(PdfEncryptedError, PdfReadError)
        True
"""

from __future__ import annotations

__all__ = ["PdfEncryptedError", "PdfNeedsPoppler", "PdfReadError"]


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


class PdfNeedsPoppler(PdfEncryptedError):
    """
    Signal an AES-encrypted PDF that the ``aes`` plugin says to read with poppler.

    Without the ``cryptography`` package, and with poppler's ``pdfinfo`` on the
    PATH, the plugin's poppler layer serves: :func:`~LiuXin_alpha.file_formats.pdf.objects.read_metadata`
    catches this and reads the file with ``pdfinfo``, which decrypts it itself.

    Example:
        >>> issubclass(PdfNeedsPoppler, PdfEncryptedError)
        True
    """

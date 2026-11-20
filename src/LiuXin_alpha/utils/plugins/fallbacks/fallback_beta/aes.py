"""
Serve the ``aes`` plugin from poppler: the second of its three layers.

Poppler offers no cipher to call. This layer tells LiuXin's PDF reader to read
AES-encrypted files with poppler's ``pdfinfo``, which decrypts them itself, as
the PDF providers that run poppler do; its cipher functions refuse. It serves
where ``cryptography`` is not usable and ``pdfinfo`` is on the PATH, and steps
aside once ``cryptography`` works, so a remembered choice does not outlast
installing it. ``fallbacks.aes`` describes all three layers.

Example:
    >>> SOURCE, DECRYPTS_IN_PROCESS
    ('poppler', False)
"""

from __future__ import annotations

import shutil

from LiuXin_alpha.utils.plugins.fallbacks.aes import cryptography_works

__all__ = ["DECRYPTS_IN_PROCESS", "SOURCE", "cbc_decrypt", "cbc_encrypt"]

SOURCE = "poppler"
"""Where this layer's decryption happens."""

DECRYPTS_IN_PROCESS = False
"""Whether the cipher functions work: here they refuse, and poppler decrypts."""


def _refuse(*_args: object) -> bytes:
    """
    Refuse to run AES in this process.

    Example:
        >>> _refuse()
        Traceback (most recent call last):
        ...
        NotImplementedError: AES runs in poppler here: read the file with pdfinfo


    :param _args: Ignored.
    :return: Never.
    :raises NotImplementedError: Always.
    """
    raise NotImplementedError("AES runs in poppler here: read the file with pdfinfo")


cbc_encrypt = _refuse
cbc_decrypt = _refuse


def __liuxin_plugin_probe__() -> tuple[bool, str]:  # noqa: N807 - the resolver's hook name
    """
    Serve where ``cryptography`` is not usable and poppler's ``pdfinfo`` is on the PATH.

    Example:
        >>> __liuxin_plugin_probe__()[0] in (True, False)
        True


    :return: Whether this layer should serve the plugin, and why.
    """
    if cryptography_works():
        return False, "the cryptography package is installed"
    found = shutil.which("pdfinfo")
    if not found:
        return False, "poppler's pdfinfo is not on the PATH"
    return True, f"poppler's pdfinfo at {found}"

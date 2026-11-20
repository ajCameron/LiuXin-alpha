"""
Serve the ``aes`` plugin from the ``cryptography`` package: the first of its three layers.

``cryptography`` is the ``encryption`` extra. Where it is missing or broken this
layer's probe declines, and the resolver moves on to poppler
(``fallback_beta.aes``) and then to pure-Python AES (``fallbacks.aes``), whose
docstring describes all three.

Example:
    >>> from LiuXin_alpha.utils.plugins import plugins
    >>> plugins["aes"][0].cbc_encrypt(bytes(16), bytes(16), bytes(16)).hex()  # doctest: +SKIP
    '66e94bd4ef8a2c3b884cfa59ca342b2e'
"""

from __future__ import annotations

from LiuXin_alpha.utils.plugins.fallbacks.aes import cryptography_works

__all__ = ["SOURCE", "cbc_decrypt", "cbc_encrypt"]

SOURCE = "cryptography"
"""Where this layer's cipher comes from."""


def _run(key: bytes, iv: bytes, data: bytes, encrypt: bool) -> bytes:
    """
    Run AES-CBC over whole blocks with ``cryptography``.

    Example:
        >>> _run(bytes(16), bytes(16), bytes(16), True).hex()  # doctest: +SKIP
        '66e94bd4ef8a2c3b884cfa59ca342b2e'


    :param key: A 16-, 24- or 32-byte key.
    :param iv: The 16-byte initialisation vector.
    :param data: Whole 16-byte blocks.
    :param encrypt: Encrypt when true, decrypt otherwise.
    :return: The transformed blocks.
    :raises ValueError: The key, vector or data have the wrong length.
    """
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

    if len(data) % 16:
        raise ValueError(f"CBC data must be whole 16-byte blocks, not {len(data)} bytes")
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv))
    context = cipher.encryptor() if encrypt else cipher.decryptor()
    return context.update(data) + context.finalize()


def cbc_encrypt(key: bytes, iv: bytes, data: bytes) -> bytes:
    """
    Encrypt whole blocks with AES-CBC, without padding.

    Example:
        >>> cbc_encrypt(bytes(16), bytes(16), bytes(16)).hex()  # doctest: +SKIP
        '66e94bd4ef8a2c3b884cfa59ca342b2e'


    :param key: A 16-, 24- or 32-byte key.
    :param iv: The 16-byte initialisation vector.
    :param data: Whole 16-byte blocks.
    :return: The ciphertext.
    """
    return _run(key, iv, data, True)


def cbc_decrypt(key: bytes, iv: bytes, data: bytes) -> bytes:
    """
    Decrypt whole blocks with AES-CBC, leaving any padding in place.

    Example:
        >>> cbc_decrypt(bytes(16), bytes(16), cbc_encrypt(bytes(16), bytes(16), bytes(16)))  # doctest: +SKIP
        b'\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00'


    :param key: A 16-, 24- or 32-byte key.
    :param iv: The 16-byte initialisation vector.
    :param data: Whole 16-byte blocks.
    :return: The plaintext.
    """
    return _run(key, iv, data, False)


def __liuxin_plugin_probe__() -> tuple[bool, str]:  # noqa: N807 - the resolver's hook name
    """
    Serve AES from ``cryptography`` when it imports and gives AES's known answer.

    Example:
        >>> __liuxin_plugin_probe__()[0] in (True, False)
        True


    :return: Whether this layer can serve the plugin, and why.
    """
    if not cryptography_works():
        return False, "the cryptography package is missing or does not work"
    import cryptography

    return True, f"cryptography {getattr(cryptography, '__version__', 'unknown version')}"

"""
Open PDFs protected by the Standard security handler with an empty password, as readers do.

Many PDFs are encrypted only to carry permission flags: their user password is empty,
so anyone may open them, but every string and stream in the file is still ciphertext.
This module derives the file key from the empty user password, or from an empty owner
password when only the user password is set, and decrypts strings and streams with it:
RC4 (revisions 2 to 4) in pure Python, AES-128 and AES-256 (revisions 4 to 6) with the
optional ``cryptography`` package. When no empty password opens the file, or AES is
needed and ``cryptography`` is not installed, :class:`PdfEncryptedError` says so, so a
caller never mistakes ciphertext for text.

Example:
    RC4 is its own inverse::

        >>> rc4(b"Key", rc4(b"Key", b"Plaintext"))
        b'Plaintext'
"""

from __future__ import annotations

import hashlib
import struct
from collections.abc import Callable, Mapping
from typing import Any, Self

from LiuXin_alpha.file_formats.pdf.errors import PdfEncryptedError

__all__ = ["StandardSecurityHandler", "rc4"]

#: The padding string of the PDF specification's Algorithm 2.
PAD = bytes.fromhex("28BF4E5E4E758A4164004E56FFFA01082E2E00B6D0683E802F0CA9FE6453697A")

#: The crypt filter methods this module can decrypt with.
_METHODS = frozenset({"None", "Identity", "V2", "AESV2", "AESV3"})


def rc4(key: bytes, data: bytes) -> bytes:
    """
    Encrypt or decrypt with RC4, the stream cipher of PDF security handler revisions 2 to 4.

    Example:
        >>> rc4(b"Key", b"Plaintext").hex()
        'bbf316e8d940af0ad3'


    :param key: The key, 1 to 256 bytes.
    :param data: The bytes to transform.
    :return: The transformed bytes, as long as ``data``.
    """
    state = list(range(256))
    j = 0
    for i in range(256):
        j = (j + state[i] + key[i % len(key)]) & 0xFF
        state[i], state[j] = state[j], state[i]
    out = bytearray(len(data))
    i = j = 0
    for index, byte in enumerate(data):
        i = (i + 1) & 0xFF
        j = (j + state[i]) & 0xFF
        state[i], state[j] = state[j], state[i]
        out[index] = byte ^ state[(state[i] + state[j]) & 0xFF]
    return bytes(out)


def _md5(*parts: bytes) -> bytes:
    """
    Hash with MD5 for the key derivation the PDF specification prescribes, not for security.

    Example:
        >>> _md5(b"").hex()
        'd41d8cd98f00b204e9800998ecf8427e'


    :param parts: The bytes to hash, in order.
    :return: The 16-byte digest.
    """
    digest = hashlib.md5(usedforsecurity=False)
    for part in parts:
        digest.update(part)
    return digest.digest()


def _aes_backend() -> Callable[[bytes, bytes, bytes, bool], bytes]:
    """
    Load AES-CBC from the optional ``cryptography`` package, or say that it is missing.

    Example:
        >>> callable(_aes_backend())  # doctest: +SKIP
        True


    :return: A function ``(key, iv, data, encrypt)`` that runs AES-CBC without padding.
    :raises PdfEncryptedError: ``cryptography`` is not installed.
    """
    try:
        from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
    except ImportError as err:  # pragma: no cover - exercised by monkeypatching in tests
        raise PdfEncryptedError(
            "this PDF is encrypted with AES, which needs the optional 'cryptography' package "
            "(install LiuXin with the 'encryption' extra)"
        ) from err

    def run(key: bytes, iv: bytes, data: bytes, encrypt: bool) -> bytes:
        """
        Run AES-CBC over whole blocks.

        Example:
            >>> run(bytes(16), bytes(16), bytes(16), True).hex()  # doctest: +SKIP
            '66e94bd4ef8a2c3b884cfa59ca342b2e'


        :param key: A 16- or 32-byte key.
        :param iv: The 16-byte initialisation vector.
        :param data: Whole 16-byte blocks.
        :param encrypt: Encrypt when true, decrypt otherwise.
        :return: The transformed blocks.
        """
        cipher = Cipher(algorithms.AES(key), modes.CBC(iv))
        context = cipher.encryptor() if encrypt else cipher.decryptor()
        return context.update(data) + context.finalize()

    return run


def _unpad(data: bytes) -> bytes:
    """
    Remove PKCS #7 padding when it is well formed, and keep the bytes as they are when it is not.

    Example:
        >>> _unpad(b"text" + bytes([12]) * 12)
        b'text'
        >>> _unpad(b"no padding here!")
        b'no padding here!'


    :param data: Decrypted bytes.
    :return: The bytes without their padding.
    """
    if not data:
        return data
    count = data[-1]
    if 1 <= count <= 16 and count <= len(data) and data[-count:] == bytes([count]) * count:
        return data[:-count]
    return data


def _as_bytes(value: Any, size: int) -> bytes | None:
    """
    Take a string entry of the Encrypt dictionary when it is at least ``size`` bytes long.

    Example:
        >>> _as_bytes(b"abc", 2), _as_bytes(b"abc", 4), _as_bytes(7, 1)
        (b'abc', None, None)


    :param value: The entry's value.
    :param size: The fewest bytes it needs.
    :return: The bytes, or ``None`` when the entry is missing, not a string or too short.
    """
    return value if isinstance(value, bytes) and len(value) >= size else None


class StandardSecurityHandler:
    """
    Decrypt the strings and streams of a PDF encrypted by the Standard security handler.

    The handler tries the empty user password, then the empty owner password, as PDF
    viewers do before asking for one. It raises :class:`PdfEncryptedError` when neither
    opens the file, when the dictionary names a security handler or method this module
    does not know, and when AES is needed but ``cryptography`` is not installed.

    Example:
        A revision 2 dictionary needs only RC4::

            >>> handler = StandardSecurityHandler.for_test_vector()
            >>> handler.decrypt_string(handler.encrypt_string(b"Title", 4, 0), 4, 0)
            b'Title'
    """

    def __init__(self: Self, encrypt: Mapping[str, Any], file_id: bytes, *, aes: Callable[..., bytes] | None = None) -> None:
        """
        Check the Encrypt dictionary and derive the file key from an empty password.

        Example:
            >>> StandardSecurityHandler({"Filter": "Other"}, b"")
            Traceback (most recent call last):
            ...
            LiuXin_alpha.file_formats.pdf.errors.PdfEncryptedError: this PDF uses the 'Other' security handler, which LiuXin cannot open


        :param encrypt: The trailer's Encrypt dictionary, with references resolved.
        :param file_id: The first string of the trailer's ``/ID`` array, or ``b""``.
        :param aes: An AES-CBC function to use instead of loading ``cryptography``, for tests.
        :return: None.
        :raises PdfEncryptedError: The file cannot be opened without a password or a backend.
        """
        handler = encrypt.get("Filter")
        if handler != "Standard":
            raise PdfEncryptedError(f"this PDF uses the {handler!r} security handler, which LiuXin cannot open")
        self.version = _int(encrypt.get("V"), 0)
        self.revision = _int(encrypt.get("R"), 0)
        self.permissions = _int(encrypt.get("P"), 0)
        self.encrypt_metadata = encrypt.get("EncryptMetadata", True) is not False
        self.file_id = file_id
        self._aes = aes
        self.string_method, self.stream_method, length = self._methods(encrypt)
        self.key_length = length
        o_size, u_size = (48, 48) if self.revision >= 5 else (32, 32)
        owner, user = _as_bytes(encrypt.get("O"), o_size), _as_bytes(encrypt.get("U"), u_size)
        if owner is None or user is None or not 2 <= self.revision <= 6:
            raise PdfEncryptedError("this PDF's Encrypt dictionary is malformed")
        self.owner, self.user = owner[:o_size], user[:u_size]
        if self.revision >= 5:
            self.key = self._open_aes256(encrypt)
        else:
            self.key = self._open_rc4_family()

    @classmethod
    def for_test_vector(cls: type[Self]) -> StandardSecurityHandler:
        """
        Build a revision 2 handler whose key derives from fixed values, for examples and tests.

        Example:
            >>> StandardSecurityHandler.for_test_vector().key_length
            5


        :return: A handler opened with the empty user password.
        """
        file_id = b"0123456789abcdef"
        owner = rc4(_md5(PAD)[:5], PAD)
        key = _md5(PAD, owner, struct.pack("<i", -4), file_id)[:5]
        user = rc4(key, PAD)
        return cls({"Filter": "Standard", "V": 1, "R": 2, "P": -4, "O": owner, "U": user}, file_id)

    def _aes_cbc(self: Self) -> Callable[..., bytes]:
        """
        Return the AES-CBC function, loading ``cryptography`` the first time it is needed.

        Example:
            >>> callable(StandardSecurityHandler.for_test_vector()._aes_cbc())  # doctest: +SKIP
            True


        :return: The AES-CBC function.
        :raises PdfEncryptedError: ``cryptography`` is not installed.
        """
        if self._aes is None:
            self._aes = _aes_backend()
        return self._aes

    def _methods(self: Self, encrypt: Mapping[str, Any]) -> tuple[str, str, int]:
        """
        Work out the string and stream methods and the key length, in bytes, from V, Length and CF.

        Example:
            >>> handler = StandardSecurityHandler.for_test_vector()
            >>> handler._methods({"V": 2, "Length": 128})
            ('V2', 'V2', 16)


        :param encrypt: The Encrypt dictionary.
        :return: The string method, the stream method and the key length.
        :raises PdfEncryptedError: The dictionary names a version or method this module does not know.
        """
        version = _int(encrypt.get("V"), 0)
        if version in (1, 2):
            bits = _int(encrypt.get("Length"), 40) if version == 2 else 40
            if bits % 8 or not 40 <= bits <= 128:
                bits = 40
            return "V2", "V2", bits // 8
        if version in (4, 5):
            filters = encrypt.get("CF")
            filters = filters if isinstance(filters, Mapping) else {}

            def method(entry: str) -> str:
                """
                Find the method of the crypt filter an entry names.

                Example:
                    >>> method("StmF")  # doctest: +SKIP
                    'AESV2'


                :param entry: ``"StmF"`` or ``"StrF"``.
                :return: The filter's ``/CFM``, or ``"Identity"``.
                """
                name = encrypt.get(entry, "Identity")
                if name == "Identity":
                    return "Identity"
                crypt_filter = filters.get(name)
                if not isinstance(crypt_filter, Mapping):
                    raise PdfEncryptedError(f"this PDF names a crypt filter {name!r} it does not define")
                cfm = str(crypt_filter.get("CFM", "None"))
                if cfm not in _METHODS:
                    raise PdfEncryptedError(f"this PDF uses the {cfm!r} crypt method, which LiuXin cannot open")
                return cfm

            if version == 5:
                return method("StrF"), method("StmF"), 32
            bits = _int(encrypt.get("Length"), 128)
            if bits % 8 or not 40 <= bits <= 128:
                bits = 128
            return method("StrF"), method("StmF"), bits // 8
        raise PdfEncryptedError(f"this PDF uses version {version} of the Standard security handler")

    # --- revisions 2 to 4 -------------------------------------------------------------------------

    def _rc4_family_key(self: Self, password: bytes) -> bytes:
        """
        Derive the file key from a user password (Algorithm 2).

        Example:
            >>> len(StandardSecurityHandler.for_test_vector()._rc4_family_key(b""))
            5


        :param password: The user password, padded or truncated to 32 bytes here.
        :return: The file key.
        """
        size = 5 if self.revision == 2 else self.key_length
        padded = (password + PAD)[:32]
        extra = b"\xff\xff\xff\xff" if self.revision >= 4 and not self.encrypt_metadata else b""
        digest = _md5(padded, self.owner, struct.pack("<I", self.permissions & 0xFFFFFFFF), self.file_id, extra)
        if self.revision >= 3:
            for _ in range(50):
                digest = _md5(digest[:size])
        return digest[:size]

    def _user_key(self: Self, password: bytes) -> bytes | None:
        """
        Derive the file key from a user password and check it against ``/U`` (Algorithms 4 to 6).

        Example:
            >>> StandardSecurityHandler.for_test_vector()._user_key(b"wrong") is None
            True


        :param password: The candidate user password.
        :return: The file key when the password is right, ``None`` otherwise.
        """
        key = self._rc4_family_key(password)
        if self.revision == 2:
            return key if rc4(key, PAD) == self.user else None
        check = rc4(key, _md5(PAD, self.file_id))
        for round_number in range(1, 20):
            check = rc4(bytes(byte ^ round_number for byte in key), check)
        return key if check[:16] == self.user[:16] else None

    def _user_password_from_owner(self: Self, password: bytes) -> bytes:
        """
        Recover the user password from ``/O`` with an owner password (Algorithm 7).

        Example:
            >>> len(StandardSecurityHandler.for_test_vector()._user_password_from_owner(b""))
            32


        :param password: The candidate owner password.
        :return: The padded user password that ``/O`` holds if the owner password is right.
        """
        size = 5 if self.revision == 2 else self.key_length
        digest = _md5((password + PAD)[:32])
        if self.revision >= 3:
            for _ in range(50):
                digest = _md5(digest)
        key = digest[:size]
        if self.revision == 2:
            return rc4(key, self.owner)
        user = self.owner
        for round_number in range(19, -1, -1):
            user = rc4(bytes(byte ^ round_number for byte in key), user)
        return user

    def _open_rc4_family(self: Self) -> bytes:
        """
        Open a revision 2 to 4 file with the empty user password, then the empty owner password.

        Example:
            >>> len(StandardSecurityHandler.for_test_vector()._open_rc4_family())
            5


        :return: The file key.
        :raises PdfEncryptedError: Neither empty password opens the file.
        """
        key = self._user_key(b"")
        if key is None:
            key = self._user_key(self._user_password_from_owner(b""))
        if key is None:
            raise PdfEncryptedError("this PDF needs a password to open")
        return key

    # --- revisions 5 and 6 ------------------------------------------------------------------------

    def _hash_r6(self: Self, password: bytes, salt: bytes, user: bytes) -> bytes:
        """
        Hash a password as revision 6 does (Algorithm 2.B), or as revision 5 does with one SHA-256.

        Example:
            >>> handler = object.__new__(StandardSecurityHandler)
            >>> handler.revision = 5
            >>> handler._hash_r6(b"", b"salt0123", b"").hex()[:16]
            '891f2ed1da603628'


        :param password: The UTF-8 password, at most 127 bytes.
        :param salt: The 8-byte validation or key salt.
        :param user: The 48 bytes of ``/U`` when hashing an owner password, else ``b""``.
        :return: The 32-byte hash.
        """
        digest = hashlib.sha256(password + salt + user).digest()
        if self.revision == 5:
            return digest
        aes = self._aes_cbc()
        round_number = 0
        while True:
            block = (password + digest + user) * 64
            encrypted = aes(digest[:16], digest[16:32], block, True)
            choice = sum(encrypted[:16]) % 3
            digest = (hashlib.sha256, hashlib.sha384, hashlib.sha512)[choice](encrypted).digest()
            round_number += 1
            if round_number >= 64 and encrypted[-1] <= round_number - 32:
                return digest[:32]

    def _open_aes256(self: Self, encrypt: Mapping[str, Any]) -> bytes:
        """
        Open a revision 5 or 6 file with the empty user password, then the empty owner password.

        Example:
            >>> StandardSecurityHandler  # doctest: +SKIP


        :param encrypt: The Encrypt dictionary, for ``/UE`` and ``/OE``.
        :return: The 32-byte file key.
        :raises PdfEncryptedError: Neither empty password opens the file, or AES is unavailable.
        """
        user_key, owner_key = _as_bytes(encrypt.get("UE"), 32), _as_bytes(encrypt.get("OE"), 32)
        if user_key is None or owner_key is None:
            raise PdfEncryptedError("this PDF's Encrypt dictionary is malformed")
        aes = self._aes_cbc()
        if self._hash_r6(b"", self.user[32:40], b"") == self.user[:32]:
            intermediate = self._hash_r6(b"", self.user[40:48], b"")
            return aes(intermediate, bytes(16), user_key[:32], False)
        if self._hash_r6(b"", self.owner[32:40], self.user) == self.owner[:32]:
            intermediate = self._hash_r6(b"", self.owner[40:48], self.user)
            return aes(intermediate, bytes(16), owner_key[:32], False)
        raise PdfEncryptedError("this PDF needs a password to open")

    # --- decryption -------------------------------------------------------------------------------

    def _object_key(self: Self, number: int, generation: int, aes: bool) -> bytes:
        """
        Derive the key for one object's strings and streams (Algorithm 1).

        Example:
            >>> len(StandardSecurityHandler.for_test_vector()._object_key(4, 0, False))
            10


        :param number: The object number.
        :param generation: The generation number.
        :param aes: Add the ``sAlT`` suffix that AES-128 keys use.
        :return: The object key.
        """
        if self.revision >= 5:
            return self.key
        salt = b"sAlT" if aes else b""
        digest = _md5(self.key, (number & 0xFFFFFF).to_bytes(3, "little"), (generation & 0xFFFF).to_bytes(2, "little"), salt)
        return digest[: min(len(self.key) + 5, 16)]

    def _decrypt(self: Self, method: str, data: bytes, number: int, generation: int) -> bytes:
        """
        Decrypt bytes of one object with a crypt method.

        Example:
            >>> StandardSecurityHandler.for_test_vector()._decrypt("Identity", b"plain", 1, 0)
            b'plain'


        :param method: ``V2`` (RC4), ``AESV2``, ``AESV3``, ``None`` or ``Identity``.
        :param data: The ciphertext.
        :param number: The object number.
        :param generation: The generation number.
        :return: The plaintext.
        :raises PdfEncryptedError: AES is needed but unavailable.
        """
        if method in ("None", "Identity"):
            return data
        if method == "V2":
            return rc4(self._object_key(number, generation, False), data)
        if len(data) < 32 or len(data) % 16:
            # Shorter than an initialisation vector and one block: not AES ciphertext.
            return b""
        key = self._object_key(number, generation, method == "AESV2")
        return _unpad(self._aes_cbc()(key, data[:16], data[16:], False))

    def _encrypt(self: Self, method: str, data: bytes, number: int, generation: int) -> bytes:
        """
        Encrypt bytes of one object, for building test files.

        Example:
            >>> handler = StandardSecurityHandler.for_test_vector()
            >>> handler._decrypt("V2", handler._encrypt("V2", b"x", 2, 0), 2, 0)
            b'x'


        :param method: ``V2``, ``AESV2``, ``AESV3``, ``None`` or ``Identity``.
        :param data: The plaintext.
        :param number: The object number.
        :param generation: The generation number.
        :return: The ciphertext; AES ciphertext starts with a zero initialisation vector.
        """
        if method in ("None", "Identity"):
            return data
        if method == "V2":
            return rc4(self._object_key(number, generation, False), data)
        pad = 16 - len(data) % 16
        key = self._object_key(number, generation, method == "AESV2")
        return bytes(16) + self._aes_cbc()(key, bytes(16), data + bytes([pad]) * pad, True)

    def decrypt_string(self: Self, data: bytes, number: int, generation: int) -> bytes:
        """
        Decrypt a string of an object.

        Example:
            >>> handler = StandardSecurityHandler.for_test_vector()
            >>> handler.decrypt_string(handler.encrypt_string(b"caf\\xe9", 7, 0), 7, 0)
            b'caf\\xe9'


        :param data: The string's ciphertext.
        :param number: The object number.
        :param generation: The generation number.
        :return: The plaintext.
        """
        return self._decrypt(self.string_method, data, number, generation)

    def encrypt_string(self: Self, data: bytes, number: int, generation: int) -> bytes:
        """
        Encrypt a string of an object, for building test files.

        Example:
            >>> len(StandardSecurityHandler.for_test_vector().encrypt_string(b"abc", 1, 0))
            3


        :param data: The plaintext.
        :param number: The object number.
        :param generation: The generation number.
        :return: The ciphertext.
        """
        return self._encrypt(self.string_method, data, number, generation)

    def decrypt_stream(self: Self, data: bytes, number: int, generation: int) -> bytes:
        """
        Decrypt a stream's data, before its filters are applied.

        Example:
            >>> handler = StandardSecurityHandler.for_test_vector()
            >>> handler.decrypt_stream(handler.encrypt_stream(b"stream data", 9, 0), 9, 0)
            b'stream data'


        :param data: The stream's ciphertext.
        :param number: The object number.
        :param generation: The generation number.
        :return: The plaintext.
        """
        return self._decrypt(self.stream_method, data, number, generation)

    def encrypt_stream(self: Self, data: bytes, number: int, generation: int) -> bytes:
        """
        Encrypt a stream's data, for building test files.

        Example:
            >>> len(StandardSecurityHandler.for_test_vector().encrypt_stream(b"data", 1, 0))
            4


        :param data: The plaintext.
        :param number: The object number.
        :param generation: The generation number.
        :return: The ciphertext.
        """
        return self._encrypt(self.stream_method, data, number, generation)


def _int(value: Any, default: int) -> int:
    """
    Take an integer entry, or a default when it is missing or not an integer.

    Example:
        >>> _int(4, 0), _int("4", 0), _int(True, 0)
        (4, 0, 0)


    :param value: The entry's value.
    :param default: The value to use otherwise.
    :return: The integer.
    """
    return value if isinstance(value, int) and not isinstance(value, bool) else default

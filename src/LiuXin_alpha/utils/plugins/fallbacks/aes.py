"""
Serve the ``aes`` plugin with LiuXin's pure-Python AES: the last of its three layers.

The ``aes`` plugin gives AES-CBC to LiuXin's PDF reader, which needs it for files
encrypted with AES-128 or AES-256. Its layers, best first:

1. ``fallback_alpha.aes``: the ``cryptography`` package (the ``encryption`` extra).
2. ``fallback_beta.aes``: poppler. It has no cipher; it tells the PDF reader to
   read AES-encrypted files with ``pdfinfo``, which decrypts them itself.
3. This module: AES in pure Python (FIPS 197) with 32-bit lookup tables. It is
   slow next to ``cryptography``, but the PDF reader decrypts only a few small
   strings and streams, and derives an AES-256 key in about a second.

The resolver remembers the layer it chose. So that this layer does not outlast
installing ``cryptography`` or poppler, its probe declines whenever either is
usable, which sends the resolver back up.

Example:
    FIPS 197, appendix C.1::

        >>> cbc_encrypt(bytes(range(16)), bytes(16), bytes.fromhex("00112233445566778899aabbccddeeff")).hex()
        '69c4e0d86a7b0430d8cdb78070b4c55a'
"""

from __future__ import annotations

import shutil
import struct

__all__ = ["SOURCE", "cbc_decrypt", "cbc_encrypt", "cryptography_works", "decrypt_block", "encrypt_block"]

SOURCE = "pure-python"
"""Where this layer's cipher comes from."""


def _multiply(a: int, b: int) -> int:
    """
    Multiply two elements of GF(2^8) modulo AES's polynomial.

    Example:
        >>> hex(_multiply(0x57, 0x83))
        '0xc1'


    :param a: A byte.
    :param b: A byte.
    :return: The product.
    """
    product = 0
    while b:
        if b & 1:
            product ^= a
        a = ((a << 1) ^ 0x11B) if a & 0x80 else a << 1
        b >>= 1
    return product


def _tables() -> tuple[list[int], list[int], list[list[int]], list[list[int]]]:
    """
    Build the S-boxes and the four encryption and four decryption round tables.

    Example:
        >>> sbox, inverse, _, _ = _tables()
        >>> hex(sbox[0x53]), hex(inverse[0xED])
        ('0xed', '0x53')


    :return: The S-box, its inverse, and the encryption and decryption tables.
    """
    # Powers of the generator 3 give every non-zero element, and its logarithm.
    powers, logs = [0] * 255, [0] * 256
    value = 1
    for exponent in range(255):
        powers[exponent] = value
        logs[value] = exponent
        value = _multiply(value, 3)
    inverses = [0] + [powers[(255 - logs[x]) % 255] for x in range(1, 256)]
    sbox = [0] * 256
    for x in range(256):
        b = inverses[x]
        s = b
        for shift in range(1, 5):
            s ^= ((b << shift) | (b >> (8 - shift))) & 0xFF
        sbox[x] = s ^ 0x63
    inverse = [0] * 256
    for x, s in enumerate(sbox):
        inverse[s] = x
    te0 = [
        (_multiply(s, 2) << 24) | (s << 16) | (s << 8) | _multiply(s, 3)
        for s in sbox
    ]
    td0 = [
        (_multiply(s, 14) << 24) | (_multiply(s, 9) << 16) | (_multiply(s, 13) << 8) | _multiply(s, 11)
        for s in inverse
    ]

    def rotations(table: list[int]) -> list[list[int]]:
        """
        Give a table and its rotations by 8, 16 and 24 bits.

        Example:
            >>> rotations([0x01020304])[1]
            [67174915]


        :param table: The first table.
        :return: The four tables.
        """
        return [[((word >> (8 * n)) | (word << (32 - 8 * n))) & 0xFFFFFFFF for word in table] for n in range(4)]

    return sbox, inverse, rotations(te0), rotations(td0)


_SBOX, _INVERSE, _TE, _TD = _tables()
_TE0, _TE1, _TE2, _TE3 = _TE
_TD0, _TD1, _TD2, _TD3 = _TD


def _round_keys(key: bytes) -> tuple[list[int], list[int], int]:
    """
    Expand a key into the encryption round keys and the equivalent inverse cipher's round keys.

    Example:
        >>> encryption, decryption, rounds = _round_keys(bytes(32))
        >>> rounds, len(encryption), len(decryption)
        (14, 60, 60)


    :param key: A 16-, 24- or 32-byte key.
    :return: Encryption round keys, decryption round keys, and the number of rounds.
    :raises ValueError: The key has another length.
    """
    if len(key) not in (16, 24, 32):
        raise ValueError(f"an AES key is 16, 24 or 32 bytes, not {len(key)}")
    words = len(key) // 4
    rounds = words + 6
    schedule = list(struct.unpack(f">{words}I", key))
    rcon = 1
    for i in range(words, 4 * (rounds + 1)):
        temp = schedule[i - 1]
        if i % words == 0:
            temp = ((temp << 8) & 0xFFFFFFFF) | (temp >> 24)
            temp = (
                (_SBOX[temp >> 24] << 24)
                | (_SBOX[(temp >> 16) & 0xFF] << 16)
                | (_SBOX[(temp >> 8) & 0xFF] << 8)
                | _SBOX[temp & 0xFF]
            ) ^ (rcon << 24)
            rcon = _multiply(rcon, 2)
        elif words > 6 and i % words == 4:
            temp = (
                (_SBOX[temp >> 24] << 24)
                | (_SBOX[(temp >> 16) & 0xFF] << 16)
                | (_SBOX[(temp >> 8) & 0xFF] << 8)
                | _SBOX[temp & 0xFF]
            )
        schedule.append(schedule[i - words] ^ temp)
    decryption: list[int] = []
    for round_number in range(rounds, -1, -1):
        group = schedule[4 * round_number : 4 * round_number + 4]
        if 0 < round_number < rounds:
            group = [
                _TD0[_SBOX[w >> 24]] ^ _TD1[_SBOX[(w >> 16) & 0xFF]] ^ _TD2[_SBOX[(w >> 8) & 0xFF]] ^ _TD3[_SBOX[w & 0xFF]]
                for w in group
            ]
        decryption.extend(group)
    return schedule, decryption, rounds


def _encrypt_words(s0: int, s1: int, s2: int, s3: int, keys: list[int], rounds: int) -> tuple[int, int, int, int]:
    """
    Encrypt one block held as four big-endian words.

    Example:
        >>> keys, _, rounds = _round_keys(bytes(16))
        >>> [hex(w) for w in _encrypt_words(0, 0, 0, 0, keys, rounds)]
        ['0x66e94bd4', '0xef8a2c3b', '0x884cfa59', '0xca342b2e']


    :param s0: The first word.
    :param s1: The second word.
    :param s2: The third word.
    :param s3: The fourth word.
    :param keys: The encryption round keys.
    :param rounds: The number of rounds.
    :return: The ciphertext words.
    """
    te0, te1, te2, te3, sbox = _TE0, _TE1, _TE2, _TE3, _SBOX
    s0 ^= keys[0]
    s1 ^= keys[1]
    s2 ^= keys[2]
    s3 ^= keys[3]
    k = 4
    for _ in range(rounds - 1):
        t0 = te0[s0 >> 24] ^ te1[(s1 >> 16) & 0xFF] ^ te2[(s2 >> 8) & 0xFF] ^ te3[s3 & 0xFF] ^ keys[k]
        t1 = te0[s1 >> 24] ^ te1[(s2 >> 16) & 0xFF] ^ te2[(s3 >> 8) & 0xFF] ^ te3[s0 & 0xFF] ^ keys[k + 1]
        t2 = te0[s2 >> 24] ^ te1[(s3 >> 16) & 0xFF] ^ te2[(s0 >> 8) & 0xFF] ^ te3[s1 & 0xFF] ^ keys[k + 2]
        t3 = te0[s3 >> 24] ^ te1[(s0 >> 16) & 0xFF] ^ te2[(s1 >> 8) & 0xFF] ^ te3[s2 & 0xFF] ^ keys[k + 3]
        s0, s1, s2, s3 = t0, t1, t2, t3
        k += 4
    return (
        ((sbox[s0 >> 24] << 24) | (sbox[(s1 >> 16) & 0xFF] << 16) | (sbox[(s2 >> 8) & 0xFF] << 8) | sbox[s3 & 0xFF]) ^ keys[k],
        ((sbox[s1 >> 24] << 24) | (sbox[(s2 >> 16) & 0xFF] << 16) | (sbox[(s3 >> 8) & 0xFF] << 8) | sbox[s0 & 0xFF]) ^ keys[k + 1],
        ((sbox[s2 >> 24] << 24) | (sbox[(s3 >> 16) & 0xFF] << 16) | (sbox[(s0 >> 8) & 0xFF] << 8) | sbox[s1 & 0xFF]) ^ keys[k + 2],
        ((sbox[s3 >> 24] << 24) | (sbox[(s0 >> 16) & 0xFF] << 16) | (sbox[(s1 >> 8) & 0xFF] << 8) | sbox[s2 & 0xFF]) ^ keys[k + 3],
    )


def _decrypt_words(s0: int, s1: int, s2: int, s3: int, keys: list[int], rounds: int) -> tuple[int, int, int, int]:
    """
    Decrypt one block held as four big-endian words, with the equivalent inverse cipher.

    Example:
        >>> _, keys, rounds = _round_keys(bytes(16))
        >>> [hex(w) for w in _decrypt_words(0x66E94BD4, 0xEF8A2C3B, 0x884CFA59, 0xCA342B2E, keys, rounds)]
        ['0x0', '0x0', '0x0', '0x0']


    :param s0: The first word.
    :param s1: The second word.
    :param s2: The third word.
    :param s3: The fourth word.
    :param keys: The decryption round keys.
    :param rounds: The number of rounds.
    :return: The plaintext words.
    """
    td0, td1, td2, td3, inverse = _TD0, _TD1, _TD2, _TD3, _INVERSE
    s0 ^= keys[0]
    s1 ^= keys[1]
    s2 ^= keys[2]
    s3 ^= keys[3]
    k = 4
    for _ in range(rounds - 1):
        t0 = td0[s0 >> 24] ^ td1[(s3 >> 16) & 0xFF] ^ td2[(s2 >> 8) & 0xFF] ^ td3[s1 & 0xFF] ^ keys[k]
        t1 = td0[s1 >> 24] ^ td1[(s0 >> 16) & 0xFF] ^ td2[(s3 >> 8) & 0xFF] ^ td3[s2 & 0xFF] ^ keys[k + 1]
        t2 = td0[s2 >> 24] ^ td1[(s1 >> 16) & 0xFF] ^ td2[(s0 >> 8) & 0xFF] ^ td3[s3 & 0xFF] ^ keys[k + 2]
        t3 = td0[s3 >> 24] ^ td1[(s2 >> 16) & 0xFF] ^ td2[(s1 >> 8) & 0xFF] ^ td3[s0 & 0xFF] ^ keys[k + 3]
        s0, s1, s2, s3 = t0, t1, t2, t3
        k += 4
    return (
        ((inverse[s0 >> 24] << 24) | (inverse[(s3 >> 16) & 0xFF] << 16) | (inverse[(s2 >> 8) & 0xFF] << 8) | inverse[s1 & 0xFF]) ^ keys[k],
        ((inverse[s1 >> 24] << 24) | (inverse[(s0 >> 16) & 0xFF] << 16) | (inverse[(s3 >> 8) & 0xFF] << 8) | inverse[s2 & 0xFF]) ^ keys[k + 1],
        ((inverse[s2 >> 24] << 24) | (inverse[(s1 >> 16) & 0xFF] << 16) | (inverse[(s0 >> 8) & 0xFF] << 8) | inverse[s3 & 0xFF]) ^ keys[k + 2],
        ((inverse[s3 >> 24] << 24) | (inverse[(s2 >> 16) & 0xFF] << 16) | (inverse[(s1 >> 8) & 0xFF] << 8) | inverse[s0 & 0xFF]) ^ keys[k + 3],
    )


def encrypt_block(key: bytes, block: bytes) -> bytes:
    """
    Encrypt one 16-byte block.

    Example:
        >>> encrypt_block(bytes(16), bytes(16)).hex()
        '66e94bd4ef8a2c3b884cfa59ca342b2e'


    :param key: A 16-, 24- or 32-byte key.
    :param block: 16 bytes.
    :return: The ciphertext.
    """
    keys, _, rounds = _round_keys(key)
    s0, s1, s2, s3 = struct.unpack(">4I", block)
    return struct.pack(">4I", *_encrypt_words(s0, s1, s2, s3, keys, rounds))


def decrypt_block(key: bytes, block: bytes) -> bytes:
    """
    Decrypt one 16-byte block.

    Example:
        >>> decrypt_block(bytes(16), encrypt_block(bytes(16), b"sixteen byte msg"))
        b'sixteen byte msg'


    :param key: A 16-, 24- or 32-byte key.
    :param block: 16 bytes.
    :return: The plaintext.
    """
    _, keys, rounds = _round_keys(key)
    s0, s1, s2, s3 = struct.unpack(">4I", block)
    return struct.pack(">4I", *_decrypt_words(s0, s1, s2, s3, keys, rounds))


def _check(key: bytes, iv: bytes, data: bytes) -> None:
    """
    Refuse an initialisation vector or data that CBC cannot use.

    Example:
        >>> _check(bytes(16), bytes(16), b"short")
        Traceback (most recent call last):
        ...
        ValueError: CBC data must be whole 16-byte blocks, not 5 bytes


    :param key: The key; its length is checked by the key schedule.
    :param iv: The initialisation vector.
    :param data: The data.
    :return: None.
    :raises ValueError: The vector is not 16 bytes or the data not whole blocks.
    """
    if len(iv) != 16:
        raise ValueError(f"a CBC initialisation vector is 16 bytes, not {len(iv)}")
    if len(data) % 16:
        raise ValueError(f"CBC data must be whole 16-byte blocks, not {len(data)} bytes")


def cbc_encrypt(key: bytes, iv: bytes, data: bytes) -> bytes:
    """
    Encrypt whole blocks with AES-CBC, without padding.

    Example:
        NIST SP 800-38A, F.2.1 (CBC-AES128), first block::

            >>> key, iv = bytes.fromhex("2b7e151628aed2a6abf7158809cf4f3c"), bytes(range(16))
            >>> cbc_encrypt(key, iv, bytes.fromhex("6bc1bee22e409f96e93d7e117393172a")).hex()
            '7649abac8119b246cee98e9b12e9197d'


    :param key: A 16-, 24- or 32-byte key.
    :param iv: The 16-byte initialisation vector.
    :param data: Whole 16-byte blocks.
    :return: The ciphertext.
    :raises ValueError: The key, vector or data have the wrong length.
    """
    _check(key, iv, data)
    keys, _, rounds = _round_keys(key)
    p0, p1, p2, p3 = struct.unpack(">4I", iv)
    words = struct.unpack(f">{len(data) // 4}I", data)
    out: list[int] = []
    for at in range(0, len(words), 4):
        p0, p1, p2, p3 = _encrypt_words(
            words[at] ^ p0, words[at + 1] ^ p1, words[at + 2] ^ p2, words[at + 3] ^ p3, keys, rounds
        )
        out += (p0, p1, p2, p3)
    return struct.pack(f">{len(out)}I", *out)


def cbc_decrypt(key: bytes, iv: bytes, data: bytes) -> bytes:
    """
    Decrypt whole blocks with AES-CBC, leaving any padding in place.

    Example:
        >>> key, iv = bytes(range(32)), bytes(16)
        >>> cbc_decrypt(key, iv, cbc_encrypt(key, iv, b"two blocks, thirty-two bytes ok!"))
        b'two blocks, thirty-two bytes ok!'


    :param key: A 16-, 24- or 32-byte key.
    :param iv: The 16-byte initialisation vector.
    :param data: Whole 16-byte blocks.
    :return: The plaintext.
    :raises ValueError: The key, vector or data have the wrong length.
    """
    _check(key, iv, data)
    _, keys, rounds = _round_keys(key)
    p0, p1, p2, p3 = struct.unpack(">4I", iv)
    words = struct.unpack(f">{len(data) // 4}I", data)
    out: list[int] = []
    for at in range(0, len(words), 4):
        c0, c1, c2, c3 = words[at : at + 4]
        d0, d1, d2, d3 = _decrypt_words(c0, c1, c2, c3, keys, rounds)
        out += (d0 ^ p0, d1 ^ p1, d2 ^ p2, d3 ^ p3)
        p0, p1, p2, p3 = c0, c1, c2, c3
    return struct.pack(f">{len(out)}I", *out)


def cryptography_works() -> bool:
    """
    Tell whether the ``cryptography`` package imports and gives AES's known answer.

    Example:
        >>> isinstance(cryptography_works(), bool)
        True


    :return: Whether the first layer can serve the plugin.
    """
    try:
        from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

        encryptor = Cipher(algorithms.AES(bytes(range(16))), modes.ECB()).encryptor()
        answer = encryptor.update(bytes.fromhex("00112233445566778899aabbccddeeff")) + encryptor.finalize()
    except Exception:
        return False
    return answer.hex() == "69c4e0d86a7b0430d8cdb78070b4c55a"


def __liuxin_plugin_probe__() -> tuple[bool, str]:  # noqa: N807 - the resolver's hook name
    """
    Serve pure-Python AES only while neither ``cryptography`` nor poppler can be used.

    Example:
        >>> __liuxin_plugin_probe__()[0] in (True, False)
        True


    :return: Whether this layer should serve the plugin, and why.
    """
    if cryptography_works():
        return False, "the cryptography package is installed"
    if shutil.which("pdfinfo"):
        return False, "poppler's pdfinfo is on the PATH"
    return True, SOURCE

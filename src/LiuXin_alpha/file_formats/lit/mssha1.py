"""
Compute the Microsoft-compatible SHA-1 variant used by LIT structures.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise mssha1 through a consuming regression::

        python -m pytest -q tests/file_formats/lit/test_lit_modernized.py
"""
from __future__ import annotations

import typing as _typing

import copy
import struct

__license__ = "GPL v3"
__copyright__ = "2008, Marshall T. Vandegrift <llasram@gmail.com>"

def _ensure_bytes(data: _typing.Any) -> _typing.Any:
    """
    Perform the ensure bytes operation under explicit file-format and conversion rules.

    Example:
        Exercise  ensure bytes through a consuming regression::

            python -m pytest -q tests/file_formats/lit/test_lit_modernized.py


    :param data: Value supplied for data under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if data is None:
        return b""
    if isinstance(data, bytes):
        return data
    if isinstance(data, bytearray):
        return bytes(data)
    if isinstance(data, str):
        return data.encode("latin-1")
    try:
        return bytes(data)
    except Exception as exc:
        raise TypeError("Expected bytes-like input, got %r" % (type(data),)) from exc


def _byte_value(value: _typing.Any) -> _typing.Any:
    """
    Perform the byte value operation under explicit file-format and conversion rules.

    Example:
        Exercise  byte value through a consuming regression::

            python -m pytest -q tests/file_formats/lit/test_lit_modernized.py


    :param value: Value normalized, stored, formatted or returned.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if isinstance(value, int):
        return value
    return ord(value)


# ======================================================================
# Bit-Manipulation helpers
#
#   _long2bytes() was contributed by Barry Warsaw
#   and is reused here with tiny modifications.
# ======================================================================


def _long2bytesBigEndian(n: _typing.Any, blocksize: int = 0) -> _typing.Any:
    """
    Convert a long integer to a byte string.

    Example:
        Exercise  long2bytesBigEndian through a consuming regression::

            python -m pytest -q tests/file_formats/lit/test_lit_modernized.py


    :param n: Value supplied for n under the utility contract.
    :param blocksize: Value supplied for blocksize under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    # After much testing, this algorithm was deemed to be the fastest.
    s = b""
    pack = struct.pack
    while n > 0:
        s = pack(">I", n & 0xFFFFFFFF) + s
        n = n >> 32

    if not s:
        s = b"\x00"
    else:
        i = 0
        while i < len(s) and s[i] == 0:
            i += 1
        if i == len(s):
            i -= 1
        s = s[i:]

    # Add back some pad bytes. This could be done more efficiently
    # w.r.t. the de-padding being done above, but sigh...
    if blocksize > 0 and len(s) % blocksize:
        s = (blocksize - len(s) % blocksize) * b"\x00" + s

    return s


def _bytelist2longBigEndian(data: _typing.Any) -> _typing.Any:
    """
    Transform a list of characters into a list of longs.

    Example:
        Exercise  bytelist2longBigEndian through a consuming regression::

            python -m pytest -q tests/file_formats/lit/test_lit_modernized.py


    :param data: Value supplied for data under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    b = _ensure_bytes(data)
    imax = len(b) // 4
    hl = [0] * imax

    j = 0
    i = 0
    while i < imax:
        b0 = _byte_value(b[j]) << 24
        b1 = _byte_value(b[j + 1]) << 16
        b2 = _byte_value(b[j + 2]) << 8
        b3 = _byte_value(b[j + 3])
        hl[i] = b0 | b1 | b2 | b3
        i = i + 1
        j = j + 4

    return hl


def _rotateLeft(x: _typing.Any, n: _typing.Any) -> _typing.Any:
    """
    Rotate x (32 bit) left n bits circularly.

    Example:
        Exercise  rotateLeft through a consuming regression::

            python -m pytest -q tests/file_formats/lit/test_lit_modernized.py


    :param x: Value supplied for x under the utility contract.
    :param n: Value supplied for n under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return ((x << n) | (x >> (32 - n))) & 0xFFFFFFFF


# ======================================================================
# The SHA transformation functions
#
# ======================================================================


def f0_19(B: _typing.Any, C: _typing.Any, D: _typing.Any) -> _typing.Any:
    """
    Perform the f0 19 operation under explicit file-format and conversion rules.

    Example:
        Exercise f0 19 through a consuming regression::

            python -m pytest -q tests/file_formats/lit/test_lit_modernized.py


    :param B: Value supplied for B under the utility contract.
    :param C: Value supplied for C under the utility contract.
    :param D: Value supplied for D under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return (B & (C ^ D)) ^ D


def f20_39(B: _typing.Any, C: _typing.Any, D: _typing.Any) -> _typing.Any:
    """
    Perform the f20 39 operation under explicit file-format and conversion rules.

    Example:
        Exercise f20 39 through a consuming regression::

            python -m pytest -q tests/file_formats/lit/test_lit_modernized.py


    :param B: Value supplied for B under the utility contract.
    :param C: Value supplied for C under the utility contract.
    :param D: Value supplied for D under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return B ^ C ^ D


def f40_59(B: _typing.Any, C: _typing.Any, D: _typing.Any) -> _typing.Any:
    """
    Perform the f40 59 operation under explicit file-format and conversion rules.

    Example:
        Exercise f40 59 through a consuming regression::

            python -m pytest -q tests/file_formats/lit/test_lit_modernized.py


    :param B: Value supplied for B under the utility contract.
    :param C: Value supplied for C under the utility contract.
    :param D: Value supplied for D under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return ((B | C) & D) | (B & C)


def f60_79(B: _typing.Any, C: _typing.Any, D: _typing.Any) -> _typing.Any:
    """
    Perform the f60 79 operation under explicit file-format and conversion rules.

    Example:
        Exercise f60 79 through a consuming regression::

            python -m pytest -q tests/file_formats/lit/test_lit_modernized.py


    :param B: Value supplied for B under the utility contract.
    :param C: Value supplied for C under the utility contract.
    :param D: Value supplied for D under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return B ^ C ^ D


# Microsoft's lovely addition...
def f6_42(B: _typing.Any, C: _typing.Any, D: _typing.Any) -> _typing.Any:
    """
    Perform the f6 42 operation under explicit file-format and conversion rules.

    Example:
        Exercise f6 42 through a consuming regression::

            python -m pytest -q tests/file_formats/lit/test_lit_modernized.py


    :param B: Value supplied for B under the utility contract.
    :param C: Value supplied for C under the utility contract.
    :param D: Value supplied for D under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return (B + C) ^ C


f = [f0_19] * 20 + [f20_39] * 20 + [f40_59] * 20 + [f60_79] * 20

# ...and delightful changes
f[3] = f20_39
f[6] = f6_42
f[10] = f20_39
f[15] = f20_39
f[26] = f0_19
f[31] = f40_59
f[42] = f6_42
f[51] = f20_39
f[68] = f0_19


# Constants to be used
K = [
    0x5A827999,  # ( 0 <= t <= 19)
    0x6ED9EBA1,  # (20 <= t <= 39)
    0x8F1BBCDC,  # (40 <= t <= 59)
    0xCA62C1D6,  # (60 <= t <= 79)
]


class mssha1(object):
    """
    An implementation of the MD5 hash function in pure Python.

    Example:
        Exercise mssha1 through a consuming regression::

            python -m pytest -q tests/file_formats/lit/test_lit_modernized.py
    """

    def __init__(self: _typing.Self) -> None:
        """
        Initialisation.

        Example:
            Exercise mssha1.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lit/test_lit_modernized.py


        :return: None; validated state is stored on the receiving object.
        """

        # Initial message length in bits(!).
        self.length = 0
        self.count = [0, 0]

        # Initial empty message as a sequence of bytes (8 bit characters).
        self.input = []

        # Call a separate init function, that can be used repeatedly
        # to start from scratch on the same object.
        self.init()

    def init(self: _typing.Self) -> None:
        """
        Initialize the message-digest and set all fields to zero.

        Example:
            Exercise mssha1.init through a consuming regression::

                python -m pytest -q tests/file_formats/lit/test_lit_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

        self.length = 0
        self.input = []

        # Initial 160 bit message digest (5 times 32 bit).
        # Also changed by Microsoft from standard.
        self.H0 = 0x32107654
        self.H1 = 0x23016745
        self.H2 = 0xC4E680A2
        self.H3 = 0xDC679823
        self.H4 = 0xD0857A34

    def _transform(self: _typing.Self, W: _typing.Any) -> None:
        """
        Perform the transform operation under explicit file-format and conversion rules.

        Example:
            Exercise mssha1. transform through a consuming regression::

                python -m pytest -q tests/file_formats/lit/test_lit_modernized.py


        :param W: Value supplied for W under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for t in range(16, 80):
            W.append(_rotateLeft(W[t - 3] ^ W[t - 8] ^ W[t - 14] ^ W[t - 16], 1) & 0xFFFFFFFF)

        A = self.H0
        B = self.H1
        C = self.H2
        D = self.H3
        E = self.H4

        for t in range(0, 80):
            TEMP = _rotateLeft(A, 5) + f[t](B, C, D) + E + W[t] + K[t // 20]
            E = D
            D = C
            C = _rotateLeft(B, 30) & 0xFFFFFFFF
            B = A
            A = TEMP & 0xFFFFFFFF

        self.H0 = (self.H0 + A) & 0xFFFFFFFF
        self.H1 = (self.H1 + B) & 0xFFFFFFFF
        self.H2 = (self.H2 + C) & 0xFFFFFFFF
        self.H3 = (self.H3 + D) & 0xFFFFFFFF
        self.H4 = (self.H4 + E) & 0xFFFFFFFF

    # Down from here all methods follow the Python Standard Library API of the sha module.

    def update(self: _typing.Self, inBuf: _typing.Any) -> None:
        """
        Add to the current message.

        Example:
            Exercise mssha1.update through a consuming regression::

                python -m pytest -q tests/file_formats/lit/test_lit_modernized.py


        :param inBuf: Value supplied for inBuf under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

        inBuf = _ensure_bytes(inBuf)
        leninBuf = len(inBuf)

        # Compute number of bytes mod 64.
        index = (self.count[1] >> 3) & 0x3F

        # Update number of bits.
        self.count[1] = self.count[1] + (leninBuf << 3)
        if self.count[1] < (leninBuf << 3):
            self.count[0] = self.count[0] + 1
        self.count[0] = self.count[0] + (leninBuf >> 29)

        partLen = 64 - index

        if leninBuf >= partLen:
            self.input[index:] = list(inBuf[:partLen])
            self._transform(_bytelist2longBigEndian(self.input))
            i = partLen
            while i + 63 < leninBuf:
                self._transform(_bytelist2longBigEndian(list(inBuf[i : i + 64])))
                i = i + 64
            else:
                self.input = list(inBuf[i:leninBuf])
        else:
            i = 0
            self.input = self.input + list(inBuf)

    def digest(self: _typing.Self) -> _typing.Any:
        """
        Terminate the message-digest computation and return digest.

        Example:
            Exercise mssha1.digest through a consuming regression::

                python -m pytest -q tests/file_formats/lit/test_lit_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        H0 = self.H0
        H1 = self.H1
        H2 = self.H2
        H3 = self.H3
        H4 = self.H4
        input = [] + self.input
        count = [] + self.count

        index = (self.count[1] >> 3) & 0x3F

        if index < 56:
            padLen = 56 - index
        else:
            padLen = 120 - index

        padding = b"\x80" + (b"\x00" * 63)
        self.update(padding[:padLen])

        # Append length (before padding).
        bits = _bytelist2longBigEndian(self.input[:56]) + count

        self._transform(bits)

        # Store state in digest.
        digest = (
            _long2bytesBigEndian(self.H0, 4)
            + _long2bytesBigEndian(self.H1, 4)
            + _long2bytesBigEndian(self.H2, 4)
            + _long2bytesBigEndian(self.H3, 4)
            + _long2bytesBigEndian(self.H4, 4)
        )

        self.H0 = H0
        self.H1 = H1
        self.H2 = H2
        self.H3 = H3
        self.H4 = H4
        self.input = input
        self.count = count

        return digest

    def hexdigest(self: _typing.Self) -> _typing.Any:
        """
        Terminate and return digest in HEX form.

        Example:
            Exercise mssha1.hexdigest through a consuming regression::

                python -m pytest -q tests/file_formats/lit/test_lit_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.digest().hex()

    def copy(self: _typing.Self) -> _typing.Any:
        """
        Return a clone object.

        Example:
            Exercise mssha1.copy through a consuming regression::

                python -m pytest -q tests/file_formats/lit/test_lit_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        return copy.deepcopy(self)


# ======================================================================
# Mimic Python top-level functions from standard library API
# for consistency with the md5 module of the standard library.
# ======================================================================

# These are mandatory variables in the module. They have constant values
# in the SHA standard.

digest_size = digestsize = 20
blocksize = 1


def new(arg: _typing.Any = None) -> _typing.Any:
    """
    Return a new mssha1 crypto object.

    Example:
        Exercise new through a consuming regression::

            python -m pytest -q tests/file_formats/lit/test_lit_modernized.py


    :param arg: Value supplied for arg under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """

    crypto = mssha1()
    if arg:
        crypto.update(arg)

    return crypto


if __name__ == "__main__":

    def main() -> None:
        """
        Perform the main operation under explicit file-format and conversion rules.

        Example:
            Exercise main through a consuming regression::

                python -m pytest -q tests/file_formats/lit/test_lit_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        import sys

        file = None
        if len(sys.argv) > 2:
            print("usage: %s [FILE]" % sys.argv[0])
            return
        elif len(sys.argv) < 2:
            file = sys.stdin
        else:
            file = open(sys.argv[1], "rb")
        context = new()
        data = file.read(16384)
        while data:
            context.update(data)
            data = file.read(16384)
        file.close()
        digest = context.hexdigest().upper()
        for i in range(0, 40, 8):
            print(
                digest[i : i + 8],
            )
        print()

    main()

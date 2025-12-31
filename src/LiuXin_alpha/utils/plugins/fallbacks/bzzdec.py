# src/LiuXin_alpha/utils/plugins/fallbacks/bzzdec.py
"""
Pure-Python fallback for calibre/LiuXin's `bzzdec` compiled extension.

Implements: decompress(data: bytes) -> bytes

This is a direct port of calibre's `bzzdecoder.c` (Kovid Goyal),
which decodes DjVu BZZ-compressed byte strings.

Performance: significantly slower than the C extension, but intended to be correct.
"""

from __future__ import annotations

import base64
import struct
from dataclasses import dataclass
from typing import List


# ---- Default ZP coder table (packed) ----
# The original C table has 251 explicit entries; the remaining entries are zeros.
_P_B64 = "AIAAgACAvWu9a0VdRV25UblRE0gTSNU/1T+xOLE4dTJ1Mv0s/SwlKCUoqyOrI4cfhx+7G7sbRRhFGCMVIxVTElMSzw/PD5UNlQ2dC50L4wnjCWEIYQgRBxEH8QXxBfkE+QQlBCUEcQNxA9kC2QJZAlkC7QHtAZMBkwFJAUkBCwELAdUA1QClAKUAewB7AFcAVwA7ADsAIwAjABMAEwAHAAcAAQABAJVW7iQAgDANGkiBBHk1egHvJHsAeBkoAMoQDQBdCzQAigegAA8FFwFYA+oBNAJEAXMBNAL1AFMDoQDFBRoBzwOqAYUChgKrAdMDGgHFBboArQh6AMwM6wECE+YCgRteBO8kkAZlKN4JhznIDZksyhBfO10LlVaKBwCADwXuJFgDMA00AoEEcwF6AfUAewChACgAGgENAKoBNACGAqAA0wMXAcUF6gGtCEQBzAw0AgITUwOBG8UF7yTPA3QrhQIdIKsBFRcaAbcPugBnCusB5wbmApYEXgQNA5AGBgLeCVUByA3hAHQrlAAdIIgBFRdSArcPgwNnCkcF5wbiB5YEwAsNA3gRBgLaGVUB7yThAA4ylAAqQ4gBfURSAs5egwMAgEcFGkjiB3k1wAvvJHgReBnaGWUo7ySHOQ4ymSwqQ187fUSVVs5eAIAAgJVWGkgaSAAAAAAAAAAAAAA="
_M_B64 = "AAAAAAAApRClECgfKB/TK9Mr4zbjNoxAjED9SP1IXVBdUNBW0FZxXHFcW2FbYaVlpWViaWJpomyibHRvdG/mceZxBHQEdNZ11nVod2h3wnjCeOp56nnneud6vnu+e3V8dXwPfQ99kX2Rff59/n1aflp+pn6mfuZ+5n4afxp/RX9Ff2t/a3+Nf41/qn+qf8N/w3/Xf9d/53/nf/J/8n/6f/p//3//fwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
_UP_B64 = "VAMEBQYHCAkKCwwNDg8QERITFBUWFxgZGhscHR4fICEiIyQlJicoKSorLC0uLzAxMjM0NTY3ODk6Ozw9Pj9AQUJDREVGR0hJSktMTU5PUFFSUVIJVgVYWVpbXF1eX2BhUmNMZUZnQmlqa0JtPG84RXJBdD12OXg1ejF8K0gnPCE4HTQXMBcqiSYVjA+OCZCNkpOUlZaXmJmam0adQlE+SzpFNkEypyxBKDsiNx6vGLGys7S1tre4Rbo7vDe+M8AvwinEJcbHSMk+yzrNNs8y0S7TKNUk1x7ZGtsURw49DjkINeQx5i3oJ+ojih0YGfATFg0QDQoH9PkKWeYAAAAAAA=="
_DN_B64 = "kQQDAQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyAhIiMkJSYnKCkqKywtLi8wMTIzNDU2Nzg5Ojs8PT4/QEFCQ0RFRkdISUpLTE1OT1BV4gawj4qNcIdohWSBYn9IfWZ7PHlud2x1NnMwcYY7hDeCM4Avfik+JUIfNhkygy4RKA+IByCLrAmqVaj4pvekxaJfoK2epZyhPJ84RzSjMDsqqyapIDUaL67BEr/evdq72LnWPdQ10jHQLc4nzMPKH8jzQO847TTrMOks5yblIuMc4RbfEN3cPwg34DMCL1cr9iX0Ie4b7BUQDwjx8gcK9QIBU/oCj/YAAAAAAA=="

_P: List[int] = list(struct.unpack("<256H", base64.b64decode(_P_B64)))
_M: List[int] = list(struct.unpack("<256H", base64.b64decode(_M_B64)))
_UP: bytes = base64.b64decode(_UP_B64)
_DN: bytes = base64.b64decode(_DN_B64)

# Machine independent ffz table: number of leading 1 bits in an 8-bit value.
_FFZT = [0] * 256
for _i in range(256):
    _j = _i
    _c = 0
    while _j & 0x80:
        _c += 1
        _j = (_j << 1) & 0xFFFFFFFF
    _FFZT[_i] = _c
del _i, _j, _c


def _ffz16(x: int) -> int:
    # Port of: ffz(ffzt, x) ((x>=0xff00) ? (ffzt[x&0xff]+8) : (ffzt[(x>>8)&0xff]))
    x &= 0xFFFF
    if x >= 0xFF00:
        return _FFZT[x & 0xFF] + 8
    return _FFZT[(x >> 8) & 0xFF]


MAXBLOCK = 4096
FREQMAX = 4
CTXIDS = 3


@dataclass
class _State:
    raw: bytes
    end: int
    pos: int = 0

    # decoder tables
    p: List[int] = None  # type: ignore[assignment]
    m: List[int] = None  # type: ignore[assignment]
    up: bytes = b""
    dn: bytes = b""

    # ffz
    ffzt: List[int] = None  # type: ignore[assignment]

    # bitstream state
    byte: int = 0
    scount: int = 0
    delay: int = 0
    a: int = 0
    code: int = 0
    fence: int = 0
    buffer: int = 0

    # block state
    is_eof: bool = False
    xsize: int = 0
    buf: bytearray = None  # type: ignore[assignment]


def _read_byte(st: _State) -> bool:
    if st.pos <= st.end:
        st.byte = st.raw[st.pos]
        st.pos += 1
        return True
    return False


def _preload(st: _State) -> None:
    while st.scount <= 24:
        if not _read_byte(st):
            st.byte = 0xFF
            st.delay -= 1
            if st.delay < 1:
                raise ValueError("Unexpected end of input")
        st.buffer = ((st.buffer << 8) | st.byte) & 0xFFFFFFFF
        st.scount += 8


def _init_state(st: _State) -> None:
    st.p = _P
    st.m = _M
    st.up = _UP
    st.dn = _DN
    st.ffzt = _FFZT

    # Read first 16 bits of code
    if not _read_byte(st):
        st.byte = 0xFF
    st.code = (st.byte << 8) & 0xFFFF
    if not _read_byte(st):
        st.byte = 0xFF
    st.code = (st.code | st.byte) & 0xFFFF

    # preload buffer
    st.delay = 25
    st.scount = 0
    st.buffer = 0
    _preload(st)
    st.fence = min(st.code, 0x7FFF)

    # allocate block buffer
    st.buf = bytearray(MAXBLOCK * 1024)


def _decode_sub_simple(st: _State, mps: int, z: int) -> int:
    # Test MPS/LPS
    if z > st.code:
        # LPS branch
        z = 0x10000 - z
        st.a = st.a + z
        st.code = st.code + z

        # LPS renormalization
        shift = _ffz16(st.a)
        st.scount -= shift
        st.a = ((st.a << shift) & 0xFFFF)
        st.code = (((st.code << shift) & 0xFFFF) | ((st.buffer >> st.scount) & ((1 << shift) - 1))) & 0xFFFF
        if st.scount < 16:
            _preload(st)
        st.fence = min(st.code, 0x7FFF)
        return mps ^ 1

    # MPS renormalization
    st.scount -= 1
    st.a = ((z << 1) & 0xFFFF)
    st.code = (((st.code << 1) & 0xFFFF) | ((st.buffer >> st.scount) & 1)) & 0xFFFF
    if st.scount < 16:
        _preload(st)
    st.fence = min(st.code, 0x7FFF)
    return mps


def _zpcodec_decoder(st: _State) -> int:
    return _decode_sub_simple(st, 0, 0x8000 + (st.a >> 1))


def _decode_raw(st: _State, bits: int) -> int:
    n = 1
    m = 1 << bits
    while n < m:
        b = _zpcodec_decoder(st)
        n = (n << 1) | b
    return n - m


def _decode_sub(st: _State, ctx: bytearray, index: int, z: int) -> int:
    bit = ctx[index] & 1

    # Avoid interval reversion
    d = 0x6000 + ((z + st.a) >> 2)
    if z > d:
        z = d

    if z > st.code:
        # LPS
        z = 0x10000 - z
        st.a = st.a + z
        st.code = st.code + z

        # LPS adaptation
        ctx[index] = st.dn[ctx[index]]

        # renormalize
        shift = _ffz16(st.a)
        st.scount -= shift
        st.a = ((st.a << shift) & 0xFFFF)
        st.code = (((st.code << shift) & 0xFFFF) | ((st.buffer >> st.scount) & ((1 << shift) - 1))) & 0xFFFF
        if st.scount < 16:
            _preload(st)
        st.fence = min(st.code, 0x7FFF)
        return bit ^ 1

    # MPS
    if st.a >= st.m[ctx[index]]:
        ctx[index] = st.up[ctx[index]]

    st.scount -= 1
    st.a = ((z << 1) & 0xFFFF)
    st.code = (((st.code << 1) & 0xFFFF) | ((st.buffer >> st.scount) & 1)) & 0xFFFF
    if st.scount < 16:
        _preload(st)
    st.fence = min(st.code, 0x7FFF)
    return bit


def _zpcodec_decode(st: _State, ctx: bytearray, index: int) -> int:
    z = st.a + st.p[ctx[index]]
    if z <= st.fence:
        st.a = z
        return ctx[index] & 1
    return _decode_sub(st, ctx, index, z)


def _decode_binary(st: _State, ctx: bytearray, index: int, bits: int) -> int:
    n = 1
    m = 1 << bits
    while n < m:
        b = _zpcodec_decode(st, ctx, index + n)
        n = (n << 1) | b
    return n - m


def _decode_block(st: _State, ctx: bytearray) -> bool:
    mtf = list(range(256))
    freq = [0, 0, 0, 0]
    fadd = 4
    mtfno = 3
    markerpos = -1
    fshift = 0

    xsize = _decode_raw(st, 24)
    st.xsize = xsize
    if not xsize:
        return False
    if xsize > MAXBLOCK * 1024:
        raise ValueError("Corrupt bitstream (block too large)")

    # Decode Estimation Speed
    if _zpcodec_decoder(st):
        fshift += 1
        if _zpcodec_decoder(st):
            fshift += 1

    # Decode MTF-coded bytes into st.buf[0:xsize]
    for i in range(xsize):
        ctxid = CTXIDS - 1
        if ctxid > mtfno:
            ctxid = mtfno
        if _zpcodec_decode(st, ctx, ctxid):
            mtfno = 0
            st.buf[i] = mtf[mtfno]
            # rotate
        else:
            ctxid += CTXIDS
            if _zpcodec_decode(st, ctx, ctxid):
                mtfno = 1
                st.buf[i] = mtf[mtfno]
                # rotate
            else:
                ctxid = 2 * CTXIDS
                found = False
                for j in range(1, 8):
                    if _zpcodec_decode(st, ctx, ctxid):
                        mtfno = (1 << j) + _decode_binary(st, ctx, ctxid, j)
                        st.buf[i] = mtf[mtfno]
                        found = True
                        break
                    ctxid += 1 << j
                if not found:
                    mtfno = 256
                    st.buf[i] = 0
                    markerpos = i
                    continue  # no rotate for marker

        # ---- rotate mtf according to empirical frequencies ----
        fadd = fadd + (fadd >> fshift)
        if fadd > 0x10000000:
            fadd >>= 24
            for k in range(FREQMAX):
                freq[k] >>= 24

        fc = fadd + (freq[mtfno] if mtfno < FREQMAX else 0)

        # Shift the mtf list down (k from mtfno .. 4)
        if mtfno >= FREQMAX:
            for k in range(mtfno, FREQMAX - 1, -1):
                mtf[k] = mtf[k - 1]
            k = FREQMAX - 1
        else:
            k = mtfno

        # Bubble into freq-ordered front section (k <= 3)
        while k > 0 and fc >= freq[k - 1]:
            mtf[k] = mtf[k - 1]
            freq[k] = freq[k - 1]
            k -= 1

        mtf[k] = st.buf[i]
        freq[k] = fc

    # -------- Reconstruct the string (undo sort transform) --------
    if markerpos < 1 or markerpos >= xsize:
        raise ValueError("Corrupt bitstream (bad marker)")

    posn = [0] * xsize
    count = [0] * 256

    for i in range(0, markerpos):
        c = st.buf[i]
        posn[i] = (c << 24) | (count[c] & 0xFFFFFF)
        count[c] += 1
    for i in range(markerpos + 1, xsize):
        c = st.buf[i]
        posn[i] = (c << 24) | (count[c] & 0xFFFFFF)
        count[c] += 1

    last = 1
    for i in range(256):
        tmp = count[i]
        count[i] = last
        last += tmp

    i = 0
    last = xsize - 1
    while last > 0:
        n = posn[i]
        c = (n >> 24) & 0xFF
        last -= 1
        st.buf[last] = c
        i = count[c] + (n & 0xFFFFFF)

    if i != markerpos:
        raise ValueError("Corrupt bitstream (marker mismatch)")

    return st.xsize != 0


def decompress(data: bytes) -> bytes:
    """Decompress a DjVu BZZ compressed byte string."""
    if not isinstance(data, (bytes, bytearray, memoryview)):
        raise TypeError("decompress() expects a bytes-like object")
    raw = bytes(data)
    st = _State(raw=raw, end=len(raw) - 1)
    ctx = bytearray(300)

    _init_state(st)

    out = bytearray()
    while not st.is_eof:
        if not st.xsize:
            try:
                ok = _decode_block(st, ctx)
            except Exception:
                # propagate ValueError, etc.
                raise
            if not ok:
                st.xsize = 1
                st.is_eof = True
            st.xsize -= 1

        if st.xsize > 0:
            out.extend(st.buf[:st.xsize])
        st.xsize = 0

    if len(out) < 3:
        raise ValueError("Corrupt bitstream (missing output header)")
    expected = (out[0] << 16) | (out[1] << 8) | out[2]
    payload = out[3:]
    if expected <= len(payload):
        return bytes(payload[:expected])
    # If the stream is truncated, return what we have (safer than the C extension's behavior).
    return bytes(payload)

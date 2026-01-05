
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Tuple

# Return codes (mirrors lzx.h)
DECR_OK = 0
DECR_DATAFORMAT = 1
DECR_ILLEGALDATA = 2
DECR_NOMEMORY = 3

# Some constants from lzx.c
LZX_MIN_MATCH = 2
LZX_MAX_MATCH = 257
LZX_NUM_CHARS = 256

LZX_BLOCKTYPE_INVALID = 0
LZX_BLOCKTYPE_VERBATIM = 1
LZX_BLOCKTYPE_ALIGNED = 2
LZX_BLOCKTYPE_UNCOMPRESSED = 3

LZX_PRETREE_NUM_ELEMENTS = 20
LZX_ALIGNED_NUM_ELEMENTS = 8
LZX_NUM_PRIMARY_LENGTHS = 7
LZX_NUM_SECONDARY_LENGTHS = 249

# Huffman table sizes (same as C)
LZX_PRETREE_MAXSYMBOLS = LZX_PRETREE_NUM_ELEMENTS
LZX_PRETREE_TABLEBITS = 6
LZX_MAINTREE_MAXSYMBOLS = LZX_NUM_CHARS + 50 * 8
LZX_MAINTREE_TABLEBITS = 12
LZX_LENGTH_MAXSYMBOLS = LZX_NUM_SECONDARY_LENGTHS + 1
LZX_LENGTH_TABLEBITS = 12
LZX_ALIGNED_MAXSYMBOLS = LZX_ALIGNED_NUM_ELEMENTS
LZX_ALIGNED_TABLEBITS = 7

LZX_LENTABLE_SAFETY = 64

_ULONG_BITS = 32
_MASK32 = (1 << _ULONG_BITS) - 1

# position slot tables
extra_bits = (
     0,  0,  0,  0,  1,  1,  2,  2,  3,  3,  4,  4,  5,  5,  6,  6,
     7,  7,  8,  8,  9,  9, 10, 10, 11, 11, 12, 12, 13, 13, 14, 14,
    15, 15, 16, 16, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17,
    17, 17, 17
)

position_base = (
          0,       1,       2,      3,      4,      6,      8,     12,     16,     24,     32,       48,      64,      96,     128,     192,
        256,     384,     512,    768,   1024,   1536,   2048,   3072,   4096,   6144,   8192,    12288,   16384,   24576,   32768,   49152,
      65536,   98304,  131072, 196608, 262144, 393216, 524288, 655360, 786432, 917504, 1048576, 1179648, 1310720, 1441792, 1572864, 1703936,
    1835008, 1966080, 2097152
)


class LZXError(ValueError):
    def __init__(self, code: int, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass
class _BitStream:
    data: bytes
    ip: int = 0
    bitbuf: int = 0
    bitsleft: int = 0

    def init(self) -> None:
        self.bitbuf = 0
        self.bitsleft = 0

    def ensure(self, n: int) -> None:
        # Mirrors ENSURE_BITS; we permit up to a word over-read by injecting zeros.
        while self.bitsleft < n:
            if self.ip + 1 < len(self.data):
                word = (self.data[self.ip + 1] << 8) | self.data[self.ip]
            elif self.ip < len(self.data):
                word = self.data[self.ip]
            else:
                word = 0
            shift = _ULONG_BITS - 16 - self.bitsleft
            self.bitbuf = (self.bitbuf | ((word << shift) & _MASK32)) & _MASK32
            self.bitsleft += 16
            self.ip += 2

    def peek(self, n: int) -> int:
        return (self.bitbuf >> (_ULONG_BITS - n)) & ((1 << n) - 1)

    def remove(self, n: int) -> None:
        self.bitbuf = ((self.bitbuf << n) & _MASK32)
        self.bitsleft -= n

    def read_bits(self, n: int) -> int:
        self.ensure(n)
        v = self.peek(n)
        self.remove(n)
        return v

    def read_huffsym(self, table: List[int], tablebits: int, maxsyms: int, lens: List[int]) -> int:
        self.ensure(16)
        i = table[self.peek(tablebits)]
        if i >= maxsyms:
            j = 1 << (_ULONG_BITS - tablebits)
            # Follow the binary tree path
            while True:
                j >>= 1
                i = (i << 1) | (1 if (self.bitbuf & j) else 0)
                if j == 0:
                    raise LZXError(DECR_ILLEGALDATA, "Illegal Huffman code (ran off end)")
                i = table[i]
                if i < maxsyms:
                    break
        sym = i
        n = lens[sym]
        self.remove(n)
        return sym


def _make_decode_table(nsyms: int, nbits: int, length: List[int], table: List[int]) -> int:
    """
    Port of make_decode_table() from lzx.c.
    Returns 0 for OK, 1 for error.
    """
    pos = 0
    table_mask = 1 << nbits
    bit_mask = table_mask >> 1
    next_symbol = bit_mask

    bit_num = 1
    while bit_num <= nbits:
        for sym in range(nsyms):
            if length[sym] == bit_num:
                leaf = pos
                pos += bit_mask
                if pos > table_mask:
                    return 1
                for _ in range(bit_mask):
                    table[leaf] = sym
                    leaf += 1
        bit_mask >>= 1
        bit_num += 1

    if pos != table_mask:
        # clear remainder
        for sym in range(pos, table_mask):
            table[sym] = 0

        pos <<= 16
        table_mask <<= 16
        bit_mask = 1 << 15

        while bit_num <= 16:
            for sym in range(nsyms):
                if length[sym] == bit_num:
                    leaf = pos >> 16
                    for fill in range(bit_num - nbits):
                        if table[leaf] == 0:
                            table[next_symbol << 1] = 0
                            table[(next_symbol << 1) + 1] = 0
                            table[leaf] = next_symbol
                            next_symbol += 1
                        leaf = table[leaf] << 1
                        if (pos >> (15 - fill)) & 1:
                            leaf += 1
                    table[leaf] = sym
                    pos += bit_mask
                    if pos > table_mask:
                        return 1
            bit_mask >>= 1
            bit_num += 1

    if pos == table_mask:
        return 0

    # either erroneous table, or all elements are 0
    for sym in range(nsyms):
        if length[sym]:
            return 1
    return 0


def _build_table(nsyms: int, nbits: int, lens: List[int], table: List[int]) -> None:
    if _make_decode_table(nsyms, nbits, lens, table):
        raise LZXError(DECR_ILLEGALDATA, "Illegal Huffman table")


def _lzx_read_lens(
    bs: _BitStream,
    pretree_len: List[int],
    pretree_table: List[int],
    lens: List[int],
    first: int,
    last: int,
) -> None:
    # read pretree lengths (20 symbols, 4 bits each)
    for x in range(20):
        pretree_len[x] = bs.read_bits(4)
    _build_table(LZX_PRETREE_MAXSYMBOLS, LZX_PRETREE_TABLEBITS, pretree_len, pretree_table)

    x = first
    while x < last:
        z = bs.read_huffsym(pretree_table, LZX_PRETREE_TABLEBITS, LZX_PRETREE_MAXSYMBOLS, pretree_len)
        if z == 17:
            y = bs.read_bits(4) + 4
            for _ in range(y):
                lens[x] = 0
                x += 1
        elif z == 18:
            y = bs.read_bits(5) + 20
            for _ in range(y):
                lens[x] = 0
                x += 1
        elif z == 19:
            y = bs.read_bits(1) + 4
            z2 = bs.read_huffsym(pretree_table, LZX_PRETREE_TABLEBITS, LZX_PRETREE_MAXSYMBOLS, pretree_len)
            val = lens[x] - z2
            if val < 0:
                val += 17
            for _ in range(y):
                lens[x] = val
                x += 1
        else:
            val = lens[x] - z
            if val < 0:
                val += 17
            lens[x] = val
            x += 1


class LZXState:
    """
    Pure-python port of the lzx.c state machine (as used in chmlib/cabextract).

    Note: The compiled calibre extension may expose a different surface. This
    fallback provides both a pythonic API and C-style wrapper functions.
    """

    def __init__(self, window: int) -> None:
        if window < 15 or window > 21:
            raise ValueError("LZX window must be between 15 and 21 (inclusive)")
        wndsize = 1 << window

        if window == 20:
            posn_slots = 42
        elif window == 21:
            posn_slots = 50
        else:
            posn_slots = window << 1

        self.window = bytearray(wndsize)
        self.window_size = wndsize
        self.actual_size = wndsize
        self.window_posn = 0

        self.R0 = 1
        self.R1 = 1
        self.R2 = 1

        self.main_elements = LZX_NUM_CHARS + (posn_slots << 3)

        self.header_read = 0
        self.block_type = LZX_BLOCKTYPE_INVALID
        self.block_length = 0
        self.block_remaining = 0
        self.frames_read = 0
        self.intel_filesize = 0
        self.intel_curpos = 0
        self.intel_started = 0

        # Huffman tables
        self.PRETREE_table = [0] * ((1 << LZX_PRETREE_TABLEBITS) + (LZX_PRETREE_MAXSYMBOLS << 1))
        self.PRETREE_len = [0] * (LZX_PRETREE_MAXSYMBOLS + LZX_LENTABLE_SAFETY)

        self.MAINTREE_table = [0] * ((1 << LZX_MAINTREE_TABLEBITS) + (LZX_MAINTREE_MAXSYMBOLS << 1))
        self.MAINTREE_len = [0] * (LZX_MAINTREE_MAXSYMBOLS + LZX_LENTABLE_SAFETY)

        self.LENGTH_table = [0] * ((1 << LZX_LENGTH_TABLEBITS) + (LZX_LENGTH_MAXSYMBOLS << 1))
        self.LENGTH_len = [0] * (LZX_LENGTH_MAXSYMBOLS + LZX_LENTABLE_SAFETY)

        self.ALIGNED_table = [0] * ((1 << LZX_ALIGNED_TABLEBITS) + (LZX_ALIGNED_MAXSYMBOLS << 1))
        self.ALIGNED_len = [0] * (LZX_ALIGNED_MAXSYMBOLS + LZX_LENTABLE_SAFETY)

        # Initialize tables to 0 (deltas applied)
        for i in range(LZX_MAINTREE_MAXSYMBOLS):
            self.MAINTREE_len[i] = 0
        for i in range(LZX_LENGTH_MAXSYMBOLS):
            self.LENGTH_len[i] = 0

    def reset(self) -> int:
        self.R0 = self.R1 = self.R2 = 1
        self.header_read = 0
        self.frames_read = 0
        self.block_remaining = 0
        self.block_type = LZX_BLOCKTYPE_INVALID
        self.intel_curpos = 0
        self.intel_started = 0
        self.window_posn = 0
        for i in range(LZX_MAINTREE_MAXSYMBOLS + LZX_LENTABLE_SAFETY):
            self.MAINTREE_len[i] = 0
        for i in range(LZX_LENGTH_MAXSYMBOLS + LZX_LENTABLE_SAFETY):
            self.LENGTH_len[i] = 0
        return DECR_OK

    def decompress_status(self, indata: bytes, outlen: int) -> Tuple[int, bytes]:
        """
        Decompress a single LZX frame worth of output (exactly outlen bytes),
        updating the sliding window and LRU offsets.

        Returns (status_code, output_bytes).
        """
        try:
            out = self._decompress(indata, outlen)
            return DECR_OK, out
        except LZXError as e:
            return e.code, b""
        except Exception as e:
            # Treat unexpected runtime issues as illegal data
            return DECR_ILLEGALDATA, b""

    def decompress(self, indata: bytes, outlen: int) -> bytes:
        """
        Decompress and return output bytes. Raises LZXError on failure.
        """
        return self._decompress(indata, outlen)

    def _decompress(self, indata: bytes, outlen: int) -> bytes:
        if outlen < 0:
            raise ValueError("outlen must be >= 0")
        if outlen > self.window_size:
            raise LZXError(DECR_DATAFORMAT, "outlen exceeds window size")
        data = bytes(indata)
        end = len(data)
        bs = _BitStream(data)
        bs.init()

        # Read header if needed
        if not self.header_read:
            i = j = 0
            k = bs.read_bits(1)
            if k:
                i = bs.read_bits(16)
                j = bs.read_bits(16)
            self.intel_filesize = ((i << 16) | j) & 0xFFFFFFFF
            self.header_read = 1

        window = self.window
        window_posn = self.window_posn
        window_size = self.window_size
        R0, R1, R2 = self.R0, self.R1, self.R2

        togo = outlen

        while togo > 0:
            if self.block_remaining == 0:
                if self.block_type == LZX_BLOCKTYPE_UNCOMPRESSED:
                    if self.block_length & 1:
                        bs.ip += 1  # realign to word boundary
                    bs.init()

                self.block_type = bs.read_bits(3)
                i = bs.read_bits(16)
                j = bs.read_bits(8)
                self.block_length = (i << 8) | j
                self.block_remaining = self.block_length

                if self.block_type == LZX_BLOCKTYPE_ALIGNED:
                    for idx in range(8):
                        self.ALIGNED_len[idx] = bs.read_bits(3)
                    _build_table(LZX_ALIGNED_MAXSYMBOLS, LZX_ALIGNED_TABLEBITS, self.ALIGNED_len, self.ALIGNED_table)
                    # fallthrough to verbatim header

                if self.block_type in (LZX_BLOCKTYPE_ALIGNED, LZX_BLOCKTYPE_VERBATIM):
                    _lzx_read_lens(bs, self.PRETREE_len, self.PRETREE_table, self.MAINTREE_len, 0, 256)
                    _lzx_read_lens(bs, self.PRETREE_len, self.PRETREE_table, self.MAINTREE_len, 256, self.main_elements)
                    _build_table(self.main_elements, LZX_MAINTREE_TABLEBITS, self.MAINTREE_len, self.MAINTREE_table)
                    if self.MAINTREE_len[0xE8] != 0:
                        self.intel_started = 1

                    _lzx_read_lens(bs, self.PRETREE_len, self.PRETREE_table, self.LENGTH_len, 0, LZX_NUM_SECONDARY_LENGTHS)
                    _build_table(LZX_LENGTH_MAXSYMBOLS, LZX_LENGTH_TABLEBITS, self.LENGTH_len, self.LENGTH_table)

                elif self.block_type == LZX_BLOCKTYPE_UNCOMPRESSED:
                    self.intel_started = 1
                    bs.ensure(16)
                    if bs.bitsleft > 16:
                        bs.ip -= 2  # align to word

                    if bs.ip + 12 > end:
                        raise LZXError(DECR_ILLEGALDATA, "Truncated uncompressed header (R0/R1/R2)")
                    R0 = int.from_bytes(data[bs.ip:bs.ip+4], "little")
                    R1 = int.from_bytes(data[bs.ip+4:bs.ip+8], "little")
                    R2 = int.from_bytes(data[bs.ip+8:bs.ip+12], "little")
                    bs.ip += 12

                else:
                    raise LZXError(DECR_ILLEGALDATA, "Invalid block type")

            # Buffer exhaustion check (mirrors the C commentary)
            if bs.ip > end:
                if bs.ip > (end + 2) or bs.bitsleft < 16:
                    raise LZXError(DECR_ILLEGALDATA, "Input buffer exhausted")

            while self.block_remaining > 0 and togo > 0:
                this_run = self.block_remaining
                if this_run > togo:
                    this_run = togo

                togo -= this_run
                self.block_remaining -= this_run

                window_posn &= (window_size - 1)
                if window_posn + this_run > window_size:
                    raise LZXError(DECR_DATAFORMAT, "Run would wrap decoding window")

                if self.block_type == LZX_BLOCKTYPE_UNCOMPRESSED:
                    if bs.ip + this_run > end:
                        raise LZXError(DECR_ILLEGALDATA, "Truncated uncompressed data")
                    window[window_posn:window_posn + this_run] = data[bs.ip:bs.ip + this_run]
                    bs.ip += this_run
                    window_posn += this_run
                    continue

                # VERBATIM or ALIGNED decode loop
                while this_run > 0:
                    main_element = bs.read_huffsym(
                        self.MAINTREE_table, LZX_MAINTREE_TABLEBITS, self.main_elements, self.MAINTREE_len
                    )

                    if main_element < LZX_NUM_CHARS:
                        window[window_posn] = main_element
                        window_posn += 1
                        this_run -= 1
                        continue

                    main_element -= LZX_NUM_CHARS
                    match_length = main_element & LZX_NUM_PRIMARY_LENGTHS
                    if match_length == LZX_NUM_PRIMARY_LENGTHS:
                        length_footer = bs.read_huffsym(
                            self.LENGTH_table, LZX_LENGTH_TABLEBITS, LZX_LENGTH_MAXSYMBOLS, self.LENGTH_len
                        )
                        match_length += length_footer
                    match_length += LZX_MIN_MATCH

                    match_offset = main_element >> 3

                    if match_offset > 2:
                        if self.block_type == LZX_BLOCKTYPE_ALIGNED:
                            extra = extra_bits[match_offset]
                            match_offset = position_base[match_offset] - 2
                            if extra > 3:
                                extra -= 3
                                verbatim_bits = bs.read_bits(extra)
                                match_offset += (verbatim_bits << 3)
                                aligned_bits = bs.read_huffsym(
                                    self.ALIGNED_table, LZX_ALIGNED_TABLEBITS, LZX_ALIGNED_MAXSYMBOLS, self.ALIGNED_len
                                )
                                match_offset += aligned_bits
                            elif extra == 3:
                                aligned_bits = bs.read_huffsym(
                                    self.ALIGNED_table, LZX_ALIGNED_TABLEBITS, LZX_ALIGNED_MAXSYMBOLS, self.ALIGNED_len
                                )
                                match_offset += aligned_bits
                            elif extra > 0:
                                verbatim_bits = bs.read_bits(extra)
                                match_offset += verbatim_bits
                            else:
                                match_offset = 1
                        else:
                            if match_offset != 3:
                                extra = extra_bits[match_offset]
                                verbatim_bits = bs.read_bits(extra) if extra else 0
                                match_offset = position_base[match_offset] - 2 + verbatim_bits
                            else:
                                match_offset = 1

                        R2, R1, R0 = R1, R0, match_offset
                    elif match_offset == 0:
                        match_offset = R0
                    elif match_offset == 1:
                        match_offset = R1
                        R1, R0 = R0, match_offset
                    else:  # 2
                        match_offset = R2
                        R2, R0 = R0, match_offset

                    if match_offset <= 0 or match_offset > window_size:
                        raise LZXError(DECR_ILLEGALDATA, "Invalid match offset")

                    if match_length > this_run:
                        # C would underflow and likely error later; be explicit
                        raise LZXError(DECR_ILLEGALDATA, "Match length exceeds remaining run")

                    # Copy match
                    src = window_posn - match_offset
                    dst = window_posn
                    window_posn += match_length
                    if window_posn > window_size:
                        raise LZXError(DECR_ILLEGALDATA, "Window overflow")

                    # fast paths
                    if match_offset == 1:
                        b = window[(dst - 1) % window_size]
                        window[dst:dst + match_length] = bytes([b]) * match_length
                    else:
                        # Safe slice copy when no overlap and no wrap
                        if src >= 0 and (src + match_length) <= window_size and (dst + match_length) <= window_size and match_offset >= match_length:
                            window[dst:dst + match_length] = window[src:src + match_length]
                        else:
                            s = src
                            d = dst
                            for _ in range(match_length):
                                window[d] = window[s % window_size]
                                d += 1
                                s += 1

                    this_run -= match_length

        if togo != 0:
            raise LZXError(DECR_ILLEGALDATA, "Output underrun")

        # Copy last outlen bytes from window
        pos = window_posn if window_posn != 0 else window_size
        start = pos - outlen
        if start < 0:
            # Wrap case should not occur given run constraints, but guard anyway
            out = bytes(window[start % window_size:] + window[:pos])
        else:
            out = bytes(window[start:pos])

        # Persist state
        self.window_posn = window_posn
        self.R0, self.R1, self.R2 = R0, R1, R2

        # Intel E8 transform (as in C)
        if (self.frames_read < 32768) and (self.intel_filesize != 0):
            if outlen <= 6 or not self.intel_started:
                self.intel_curpos += outlen
            else:
                data_bytes = bytearray(out)
                dataend = outlen - 10
                curpos = self.intel_curpos
                filesize = int(self.intel_filesize)
                self.intel_curpos = curpos + outlen

                i = 0
                while i < dataend:
                    if data_bytes[i] != 0xE8:
                        i += 1
                        curpos += 1
                        continue
                    i += 1
                    abs_off = int.from_bytes(data_bytes[i:i+4], "little", signed=True)
                    if (abs_off >= -curpos) and (abs_off < filesize):
                        rel_off = abs_off - curpos if abs_off >= 0 else abs_off + filesize
                        data_bytes[i:i+4] = int(rel_off & 0xFFFFFFFF).to_bytes(4, "little", signed=False)
                    i += 4
                    curpos += 5
                out = bytes(data_bytes)

        self.frames_read += 1
        return out


# ---- C-style wrapper functions (friendlier for compatibility shims) ----

def LZXinit(window: int) -> LZXState:
    return LZXState(window)


def LZXteardown(pState: LZXState) -> None:
    # Nothing special required in Python
    return None


def LZXreset(pState: LZXState) -> int:
    return pState.reset()


def LZXdecompress(pState: LZXState, inpos: bytes, outlen: int, inlen: Optional[int] = None) -> bytes:
    if inlen is not None:
        inpos = inpos[:inlen]
    return pState.decompress(inpos, outlen)


"""
Provide lzx utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise lzx through a consuming regression::

        python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py
"""
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

#: Output is produced in frames of 32 KiB (the last may be shorter); the input bit
#: stream is realigned to a 16-bit word after each.
LZX_FRAME_SIZE = 32768

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
    """
    Report the LZXError Calibre compatibility failure.

    Example:
        Exercise LZXError through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py
    """
    def __init__(self, code: int, message: str) -> None:
        """
        Initialize and validate the LZXError state.

        Example:
            Exercise LZXError.  init   through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


        :param code: Value supplied for code under the utility contract.
        :param message: Value supplied for message under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        super().__init__(message)
        self.code = code


@dataclass
class _BitStream:
    """
    Provide the BitStream utility contract with explicit state and cleanup behavior.

    Example:
        Exercise  BitStream through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py
    """
    data: bytes
    ip: int = 0
    bitbuf: int = 0
    bitsleft: int = 0

    def init(self) -> None:
        """
        Perform the init utility operation under explicit compatibility rules.

        Example:
            Exercise  BitStream.init through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.bitbuf = 0
        self.bitsleft = 0

    def ensure(self, n: int) -> None:
        # Mirrors ENSURE_BITS; we permit up to a word over-read by injecting zeros.
        """
        Perform the ensure utility operation under explicit compatibility rules.

        Example:
            Exercise  BitStream.ensure through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


        :param n: Value supplied for n under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
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
        """
        Perform the peek utility operation under explicit compatibility rules.

        Example:
            Exercise  BitStream.peek through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


        :param n: Value supplied for n under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return (self.bitbuf >> (_ULONG_BITS - n)) & ((1 << n) - 1)

    def remove(self, n: int) -> None:
        """
        Perform the remove utility operation under explicit compatibility rules.

        Example:
            Exercise  BitStream.remove through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


        :param n: Value supplied for n under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.bitbuf = ((self.bitbuf << n) & _MASK32)
        self.bitsleft -= n

    def read_bits(self, n: int) -> int:
        """
        Read bits under the documented compatibility and safety rules.

        Example:
            Exercise  BitStream.read bits through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


        :param n: Value supplied for n under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        self.ensure(n)
        v = self.peek(n)
        self.remove(n)
        return v

    def read_huffsym(self, table: List[int], tablebits: int, maxsyms: int, lens: List[int]) -> int:
        """
        Read huffsym under the documented compatibility and safety rules.

        Example:
            Exercise  BitStream.read huffsym through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


        :param table: Value supplied for table under the utility contract.
        :param tablebits: Value supplied for tablebits under the utility contract.
        :param maxsyms: Value supplied for maxsyms under the utility contract.
        :param lens: Value supplied for lens under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
    Port of make_decode_table() from lzx.c. Returns 0 for OK, 1 for error.

    Example:
        Exercise  make decode table through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param nsyms: Value supplied for nsyms under the utility contract.
    :param nbits: Value supplied for nbits under the utility contract.
    :param length: Value supplied for length under the utility contract.
    :param table: Value supplied for table under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
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
    """
    Perform the build table utility operation under explicit compatibility rules.

    Example:
        Exercise  build table through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param nsyms: Value supplied for nsyms under the utility contract.
    :param nbits: Value supplied for nbits under the utility contract.
    :param lens: Value supplied for lens under the utility contract.
    :param table: Value supplied for table under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
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
    """
    Perform the lzx read lens utility operation under explicit compatibility rules.

    Example:
        Exercise  lzx read lens through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param bs: Value supplied for bs under the utility contract.
    :param pretree_len: Value supplied for pretree len under the utility contract.
    :param pretree_table: Value supplied for pretree table under the utility contract.
    :param lens: Value supplied for lens under the utility contract.
    :param first: Value supplied for first under the utility contract.
    :param last: Value supplied for last under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
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

    Example:
        Exercise LZXState through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py
    """

    def __init__(self, window: int) -> None:
        """
        Initialize and validate the LZXState state.

        Example:
            Exercise LZXState.  init   through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


        :param window: Value supplied for window under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
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
        self.frame_posn = 0

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
        """
        Perform the reset utility operation under explicit compatibility rules.

        Example:
            Exercise LZXState.reset through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        self.R0 = self.R1 = self.R2 = 1
        self.header_read = 0
        self.frames_read = 0
        self.block_remaining = 0
        self.block_type = LZX_BLOCKTYPE_INVALID
        self.intel_curpos = 0
        self.intel_started = 0
        self.window_posn = 0
        self.frame_posn = 0
        for i in range(LZX_MAINTREE_MAXSYMBOLS + LZX_LENTABLE_SAFETY):
            self.MAINTREE_len[i] = 0
        for i in range(LZX_LENGTH_MAXSYMBOLS + LZX_LENTABLE_SAFETY):
            self.LENGTH_len[i] = 0
        return DECR_OK

    def decompress_status(self, indata: bytes, outlen: int) -> Tuple[int, bytes]:
        """
        Decompress ``outlen`` bytes of output, frame by frame, updating the sliding window and LRU offsets.

        Example:
            Exercise LZXState.decompress status through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


        :param indata: Value supplied for indata under the utility contract.
        :param outlen: Value supplied for outlen under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
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

        Example:
            Exercise LZXState.decompress through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


        :param indata: Value supplied for indata under the utility contract.
        :param outlen: Value supplied for outlen under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self._decompress(indata, outlen)

    def _decompress(self, indata: bytes, outlen: int) -> bytes:
        """
        Decompress ``outlen`` bytes from ``indata``, frame by frame, as libmspack's ``lzxd_decompress`` does.

        The output comes in frames of 32 KiB; the last may be shorter. After each frame
        the input bit stream is realigned to a 16-bit word, and the frame's Intel E8
        translation, if any, is undone. A match may run on past the end of a frame, as
        it may in calibre's copy of libmspack, but not past the end of its block. Given
        one frame's worth of input per call (as chmlib gives it), this decodes as
        chmlib's ``LZXdecompress`` did.

        Example:
            Exercise LZXState. decompress through a consuming regression::

                python -m pytest -q tests/utils/plugins/fallbacks/test_lzx_frames.py


        :param indata: The compressed bytes, starting at a 16-bit word.
        :param outlen: How many bytes to produce.
        :return: The decompressed bytes.
        :raises LZXError: For damaged or truncated input.
        """
        if outlen < 0:
            raise ValueError("outlen must be >= 0")
        data = bytes(indata)
        bs = _BitStream(data)
        bs.init()
        out = bytearray()
        while len(out) < outlen:
            frame_size = min(LZX_FRAME_SIZE, outlen - len(out))
            if not self.header_read:
                self._read_intel_header(bs)
            self._decode_frame(bs, data, frame_size)
            # Re-align the input bit stream to a 16-bit word, as lzxd does after each frame.
            if bs.bitsleft > 0:
                bs.ensure(16)
            if bs.bitsleft & 15:
                bs.remove(bs.bitsleft & 15)
            out += self._finish_frame(frame_size)
        return bytes(out)

    def _read_intel_header(self, bs: _BitStream) -> None:
        """
        Read the stream header: whether Intel E8 translation was applied, and the file size it used.

        Example:
            Exercise the header through the frame regression::

                python -m pytest -q tests/utils/plugins/fallbacks/test_lzx_frames.py


        :param bs: The input bit stream.
        :return: None.
        """
        i = j = 0
        if bs.read_bits(1):
            i = bs.read_bits(16)
            j = bs.read_bits(16)
        self.intel_filesize = ((i << 16) | j) & 0xFFFFFFFF
        self.header_read = 1

    def _read_block_header(self, bs: _BitStream, data: bytes) -> None:
        """
        Start a new block: read its type, length and trees, or the stored R0, R1 and R2 of an uncompressed block.

        Example:
            Exercise the block header through the frame regression::

                python -m pytest -q tests/utils/plugins/fallbacks/test_lzx_frames.py


        :param bs: The input bit stream.
        :param data: The compressed bytes.
        :return: None.
        :raises LZXError: For an unknown block type or a truncated header.
        """
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

            if bs.ip + 12 > len(data):
                raise LZXError(DECR_ILLEGALDATA, "Truncated uncompressed header (R0/R1/R2)")
            self.R0 = int.from_bytes(data[bs.ip:bs.ip+4], "little")
            self.R1 = int.from_bytes(data[bs.ip+4:bs.ip+8], "little")
            self.R2 = int.from_bytes(data[bs.ip+8:bs.ip+12], "little")
            bs.ip += 12
            bs.init()

        else:
            raise LZXError(DECR_ILLEGALDATA, "Invalid block type")

    def _decode_frame(self, bs: _BitStream, data: bytes, frame_size: int) -> None:
        """
        Decode blocks until the window holds the current frame.

        Example:
            Exercise the frame loop through its regression::

                python -m pytest -q tests/utils/plugins/fallbacks/test_lzx_frames.py


        :param bs: The input bit stream.
        :param data: The compressed bytes.
        :param frame_size: The frame's length.
        :return: None.
        :raises LZXError: For damaged or truncated input.
        """
        end = len(data)
        bytes_todo = self.frame_posn + frame_size - self.window_posn
        while bytes_todo > 0:
            if self.block_remaining == 0:
                self._read_block_header(bs, data)

            # Buffer exhaustion check (mirrors the C commentary)
            if bs.ip > end:
                if bs.ip > (end + 2) or bs.bitsleft < 16:
                    raise LZXError(DECR_ILLEGALDATA, "Input buffer exhausted")

            this_run = min(self.block_remaining, bytes_todo)
            bytes_todo -= this_run
            self.block_remaining -= this_run

            if self.window_posn + this_run > self.window_size:
                raise LZXError(DECR_DATAFORMAT, "Run would wrap decoding window")

            if self.block_type == LZX_BLOCKTYPE_UNCOMPRESSED:
                if bs.ip + this_run > end:
                    raise LZXError(DECR_ILLEGALDATA, "Truncated uncompressed data")
                self.window[self.window_posn:self.window_posn + this_run] = data[bs.ip:bs.ip + this_run]
                bs.ip += this_run
                self.window_posn += this_run
                continue

            overrun = -self._decode_run(bs, this_run)
            if overrun > 0:
                # The last match ran past what this run needed; it may not leave its block.
                if overrun > self.block_remaining:
                    raise LZXError(DECR_ILLEGALDATA, "Match runs past the end of its block")
                self.block_remaining -= overrun

    def _match_offset(self, bs: _BitStream, slot: int) -> int:
        """
        Read a match's offset from its position slot, updating the repeated-offset queue.

        Example:
            Exercise the offsets through the frame regression::

                python -m pytest -q tests/utils/plugins/fallbacks/test_lzx_frames.py


        :param bs: The input bit stream.
        :param slot: The position slot from the main tree.
        :return: The offset.
        """
        if slot == 0:
            return self.R0
        if slot == 1:
            offset = self.R1
            self.R1, self.R0 = self.R0, offset
            return offset
        if slot == 2:
            offset = self.R2
            self.R2, self.R0 = self.R0, offset
            return offset
        extra = extra_bits[slot]
        offset = position_base[slot] - 2
        if self.block_type == LZX_BLOCKTYPE_ALIGNED:
            if extra > 3:
                offset += bs.read_bits(extra - 3) << 3
                offset += bs.read_huffsym(
                    self.ALIGNED_table, LZX_ALIGNED_TABLEBITS, LZX_ALIGNED_MAXSYMBOLS, self.ALIGNED_len
                )
            elif extra == 3:
                offset += bs.read_huffsym(
                    self.ALIGNED_table, LZX_ALIGNED_TABLEBITS, LZX_ALIGNED_MAXSYMBOLS, self.ALIGNED_len
                )
            elif extra > 0:
                offset += bs.read_bits(extra)
            else:
                offset = 1  # not defined by the LZX specification; libmspack's choice
        elif slot == 3:
            offset = 1
        else:
            offset += bs.read_bits(extra) if extra else 0
        self.R2, self.R1, self.R0 = self.R1, self.R0, offset
        return offset

    def _decode_run(self, bs: _BitStream, this_run: int) -> int:
        """
        Decode at least ``this_run`` bytes of a verbatim or aligned block into the window.

        Example:
            Exercise the run decoder through the frame regression::

                python -m pytest -q tests/utils/plugins/fallbacks/test_lzx_frames.py


        :param bs: The input bit stream.
        :param this_run: How many bytes the run needs.
        :return: What is left of the run: zero, or minus how far the last match ran past it.
        :raises LZXError: For a bad offset or a match past the window's end.
        """
        window = self.window
        window_size = self.window_size
        window_posn = self.window_posn
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
                match_length += bs.read_huffsym(
                    self.LENGTH_table, LZX_LENGTH_TABLEBITS, LZX_LENGTH_MAXSYMBOLS, self.LENGTH_len
                )
            match_length += LZX_MIN_MATCH
            match_offset = self._match_offset(bs, main_element >> 3)

            if match_offset <= 0 or match_offset > window_size:
                raise LZXError(DECR_ILLEGALDATA, "Invalid match offset")
            if window_posn + match_length > window_size:
                raise LZXError(DECR_ILLEGALDATA, "Match runs past the end of the window")

            src = window_posn - match_offset
            if match_offset == 1:
                window[window_posn:window_posn + match_length] = bytes([window[src % window_size]]) * match_length
            elif src >= 0 and match_offset >= match_length:
                window[window_posn:window_posn + match_length] = window[src:src + match_length]
            else:
                # Overlapping, or starting before the window's start and wrapping round its end.
                for d in range(window_posn, window_posn + match_length):
                    window[d] = window[src % window_size]
                    src += 1
            window_posn += match_length
            this_run -= match_length
        self.window_posn = window_posn
        return this_run

    def _finish_frame(self, frame_size: int) -> bytes:
        """
        Take the decoded frame from the window, undo its Intel E8 translation, and move on to the next.

        Example:
            Exercise the frame output through the frame regression::

                python -m pytest -q tests/utils/plugins/fallbacks/test_lzx_frames.py


        :param frame_size: The frame's length.
        :return: The frame's bytes.
        """
        frame = bytearray(self.window[self.frame_posn:self.frame_posn + frame_size])
        if self.intel_started and self.intel_filesize and self.frames_read <= 32768 and frame_size > 10:
            _undo_e8(frame, self.intel_curpos, self.intel_filesize)
        if self.intel_filesize:
            self.intel_curpos += frame_size
        self.frames_read += 1
        self.frame_posn += frame_size
        if self.window_posn == self.window_size:
            self.window_posn = 0
        if self.frame_posn == self.window_size:
            self.frame_posn = 0
        return bytes(frame)


def _undo_e8(frame: bytearray, curpos: int, filesize: int) -> None:
    """
    Turn the absolute call targets written after each ``0xE8`` byte back into relative ones, in place.

    Example:
        >>> frame = bytearray(b"\\xe8" + (100).to_bytes(4, "little") + bytes(11))
        >>> _undo_e8(frame, 0, 1000)
        >>> int.from_bytes(frame[1:5], "little")
        100


    :param frame: The frame's bytes.
    :param curpos: The frame's position in the uncompressed stream.
    :param filesize: The translation's file size, from the stream header.
    :return: None.
    """
    filesize = filesize - (1 << 32) if filesize >= 1 << 31 else filesize
    i = 0
    dataend = len(frame) - 10
    while i < dataend:
        if frame[i] != 0xE8:
            i += 1
            curpos += 1
            continue
        i += 1
        abs_off = int.from_bytes(frame[i:i+4], "little", signed=True)
        if -curpos <= abs_off < filesize:
            rel_off = abs_off - curpos if abs_off >= 0 else abs_off + filesize
            frame[i:i+4] = (rel_off & 0xFFFFFFFF).to_bytes(4, "little")
        i += 4
        curpos += 5


# ---- C-style wrapper functions (friendlier for compatibility shims) ----

def LZXinit(window: int) -> LZXState:
    """
    Perform the LZXinit utility operation under explicit compatibility rules.

    Example:
        Exercise LZXinit through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param window: Value supplied for window under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return LZXState(window)


def LZXteardown(pState: LZXState) -> None:
    # Nothing special required in Python
    """
    Perform the LZXteardown utility operation under explicit compatibility rules.

    Example:
        Exercise LZXteardown through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param pState: Value supplied for pState under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return None


def LZXreset(pState: LZXState) -> int:
    """
    Perform the LZXreset utility operation under explicit compatibility rules.

    Example:
        Exercise LZXreset through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param pState: Value supplied for pState under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return pState.reset()


def LZXdecompress(pState: LZXState, inpos: bytes, outlen: int, inlen: Optional[int] = None) -> bytes:
    """
    Perform the LZXdecompress utility operation under explicit compatibility rules.

    Example:
        Exercise LZXdecompress through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_plugin_layer_resolution.py


    :param pState: Value supplied for pState under the utility contract.
    :param inpos: Value supplied for inpos under the utility contract.
    :param outlen: Value supplied for outlen under the utility contract.
    :param inlen: Value supplied for inlen under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if inlen is not None:
        inpos = inpos[:inlen]
    return pState.decompress(inpos, outlen)

"""
Check that the pure-Python LZX decoder decodes several 32 KiB frames in one call, as calibre's does.

calibre's ``lzx`` extension wraps libmspack's ``lzxd``: each call decodes as many
frames as asked, realigning the input bit stream to a 16-bit word after each. The
fallback was a port of chmlib's decoder, which decodes one frame per call, so it
failed on LIT text, which calibre's readers ask for a whole window at a time.

Example:
    Run the checks::

        python -m pytest -q tests/utils/plugins/fallbacks/test_lzx_frames.py
"""

from __future__ import annotations

import hashlib
import random
from pathlib import Path
from typing import Any

import pytest

from LiuXin_alpha.utils.plugins.fallbacks import lzx

FIXTURES = Path(__file__).resolve().parents[3] / "fixtures" / "lzx"

#: SHA-256 of each window calibre's compiled decoder gives for the LIT samples' text.
LIT_WINDOWS = {
    "lit_md_test_file_1.lit": sorted(
        [
            "fb7ed026a29fa2761dc4dc27616c9219fce8b949e095930df2c3a09cd9dc204d",
            "3b2a7ccae4f04291ed9f4ef615142f7d85d4e374f93fe544fa3efc21cea779c5",
            "771f454b28af536f3dd04db7e552be264a248a98babb5b2ba8c8fc5e135552ed",
            "3c7e5d4ec72bb2fddf8e5ca91c0cb63e1620304bd5cc2818223586eea14bc236",
            "9ad46211f73adab8261b88e24084355da859c04f2b002dd2bb33ff8d1db6d8aa",
        ]
    ),
    "lit_md_test_file_2.lit": sorted(
        [
            "0bfd67718365bed4f64d543c6d55b56fd6812a8ba54b4361e8d46af1a1be118e",
            "1271ac4af1c5b507aaa1bd3c37bbac6c3803f5caab7262fedbbbf386bdd759b6",
            "5182a9b6e838437cd3dbf47a48627f8339b7d3ef115a8805408636fbc2c71521",
            "d8576d167d459ba51bf88d029ab2b0d3d543055878565237c7c5c7b701c7b68f",
            "9179c5feffc7e60bf9327015e346d648274cf5aa3098f4ace89e8ea2d5719eac",
        ]
    ),
}


def frames_text(n: int = 100_000) -> bytes:
    """
    Make the text compressed in ``three_frames.lzx``: three full frames and part of a fourth.

    Example:
        >>> frames_text(20)
        b'Line 0: owl and cat '


    :param n: How many bytes.
    :return: The text.
    """
    out = bytearray()
    i = 0
    while len(out) < n:
        out += b"Line %d: %s and %s went to sea in a beautiful pea-green boat.\n" % (
            i,
            b"owl" * (i % 3 + 1),
            b"cat" * (i % 5 + 1),
        )
        i += 1
    return bytes(out[:n])


def decode(data: bytes, outlen: int, window_bits: int = 17) -> bytes:
    """
    Decode a stream in one call, from a fresh state, as calibre's readers ask for it.

    Example:
        >>> decode(b"", 0)
        b''


    :param data: The compressed bytes.
    :param outlen: How many bytes to produce.
    :param window_bits: The window size, as a power of two.
    :return: The decompressed bytes.
    """
    state = lzx.LZXinit(window_bits)
    lzx.LZXreset(state)
    return lzx.LZXdecompress(state, data, outlen)


class BitWriter:
    """
    Write bits as LZX reads them: most significant first, in 16-bit little-endian words.

    Example:
        >>> writer = BitWriter()
        >>> writer.write(1, 1)
        >>> writer.words()
        b'\\x00\\x80'
    """

    def __init__(self) -> None:
        """
        Start with no bits.

        Example:
            See :class:`BitWriter`.


        :return: None.
        """
        self.out = bytearray()
        self.word = 0
        self.count = 0

    def write(self, value: int, bits: int) -> None:
        """
        Write a value in so many bits.

        Example:
            See :class:`BitWriter`.


        :param value: The value.
        :param bits: How many bits.
        :return: None.
        """
        for shift in range(bits - 1, -1, -1):
            self.word = (self.word << 1) | ((value >> shift) & 1)
            self.count += 1
            if self.count == 16:
                self.out += self.word.to_bytes(2, "little")
                self.word = self.count = 0

    def words(self) -> bytes:
        """
        Pad the last word with zero bits and give the bytes, as an uncompressed block's header is padded.

        Example:
            See :class:`BitWriter`.


        :return: The bytes so far.
        """
        if self.count:
            self.write(0, 16 - self.count)
        return bytes(self.out)


def translate_e8(data: bytes, filesize: int) -> bytes:
    """
    Apply the Intel E8 translation an LZX compressor applies: call targets after ``0xE8`` made absolute.

    Like the decoder, it works frame by frame and leaves the last 10 bytes of each
    frame alone. A target in ``[-curpos, filesize)`` is mapped one to one onto that
    range, and the decoder maps it back; others are left as they are.

    Example:
        >>> translate_e8(b"\\xe8" + (5).to_bytes(4, "little") + bytes(11), 100)[:5]
        b'\\xe8\\x05\\x00\\x00\\x00'


    :param data: The bytes to compress.
    :param filesize: The translation's file size, written in the stream header.
    :return: The translated bytes.
    """
    out = bytearray(data)
    for start in range(0, len(out), lzx.LZX_FRAME_SIZE):
        size = min(lzx.LZX_FRAME_SIZE, len(out) - start)
        if size <= 10:
            continue
        i, end = start, start + size - 10
        while i < end:
            if out[i] != 0xE8:
                i += 1
                continue
            curpos = i
            i += 1
            relative = int.from_bytes(out[i : i + 4], "little", signed=True)
            if -curpos <= relative < filesize - curpos:
                out[i : i + 4] = (relative + curpos).to_bytes(4, "little")
            elif filesize - curpos <= relative < filesize:
                out[i : i + 4] = ((relative - filesize) & 0xFFFFFFFF).to_bytes(4, "little")
            i += 4
    return bytes(out)


def uncompressed_stream(blocks: list[bytes], filesize: int = 0) -> bytes:
    """
    Make an LZX stream of uncompressed blocks, with an Intel E8 header when a file size is given.

    Example:
        >>> decode(uncompressed_stream([b"abc"]), 3)
        b'abc'


    :param blocks: Each block's bytes, already translated.
    :param filesize: The Intel E8 translation's file size; zero for none.
    :return: The stream.
    """
    writer = BitWriter()
    if filesize:
        writer.write(1, 1)
        writer.write(filesize >> 16, 16)
        writer.write(filesize & 0xFFFF, 16)
    else:
        writer.write(0, 1)
    stream = bytearray()
    for block in blocks:
        writer.write(3, 3)
        writer.write(len(block) >> 8, 16)
        writer.write(len(block) & 0xFF, 8)
        stream += writer.words()
        stream += (1).to_bytes(4, "little") * 3  # R0, R1 and R2
        stream += block
        if len(block) & 1:
            stream += b"\0"
        writer = BitWriter()
    return bytes(stream)


def test_a_calibre_stream_of_several_frames_decodes_in_one_call() -> None:
    """
    Decode 100,000 bytes compressed by calibre's LZX compressor, as a LIT writer would, in one call.

    The old decoder stopped at the first frame's end ("Match length exceeds remaining run").

    Example:
        >>> test_a_calibre_stream_of_several_frames_decodes_in_one_call()


    :return: None; the assertions are the check.
    """
    packed = (FIXTURES / "three_frames.lzx").read_bytes()
    assert decode(packed, 100_000) == frames_text()


def test_uncompressed_blocks_span_frames_and_their_e8_translation_is_undone() -> None:
    """
    Decode uncompressed blocks across frames, an odd-sized one included, undoing the Intel E8 translation.

    Example:
        >>> test_uncompressed_blocks_span_frames_and_their_e8_translation_is_undone()


    :return: None; the assertions are the check.
    """
    rng = random.Random(758)
    original = bytes(rng.choice(b"\xe8\x00\x01abc") for _ in range(70_006))
    filesize = 1 << 20
    translated = translate_e8(original, filesize)
    assert translated != original
    stream = uncompressed_stream([translated[:70_001], translated[70_001:]], filesize)
    assert decode(stream, len(original)) == original
    assert decode(uncompressed_stream([original[:70_001], original[70_001:]]), len(original)) == original


def test_a_stream_decodes_the_same_in_one_call_or_two() -> None:
    """
    Decode a stream in two calls, with the state carried between them, as chmlib's callers do.

    Example:
        >>> test_a_stream_decodes_the_same_in_one_call_or_two()


    :return: None; the assertions are the check.
    """
    rng = random.Random(649)
    original = bytes(rng.getrandbits(8) for _ in range(3 * lzx.LZX_FRAME_SIZE))
    stream = uncompressed_stream([original])
    state = lzx.LZXinit(17)
    lzx.LZXreset(state)
    first = lzx.LZXdecompress(state, stream, 100)
    assert first + lzx.LZXdecompress(state, stream[4 + 12 + 100 :], len(original) - 100) == original
    assert decode(stream, len(original)) == original


def _lit_windows(path: Path, monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """
    Read every file of a LIT book through the ported reader, decoding with the fallback, and hash each LZX window.

    Example:
        Hash a book's windows::

            digests = _lit_windows(path, monkeypatch)


    :param path: The book.
    :param monkeypatch: Pytest fixture, to put the fallback behind the reader.
    :return: The SHA-256 of each window, sorted.
    """
    import logging

    from LiuXin_alpha.metadata.upstream_calibre import _compat, lit_reader

    digests: list[str] = []

    class Recording:
        """
        The fallback, hashing what it decodes.

        Example:
            See :func:`_lit_windows`.
        """

        LZXError = lzx.LZXError
        LZXinit = staticmethod(lzx.LZXinit)
        LZXreset = staticmethod(lzx.LZXreset)

        @staticmethod
        def LZXdecompress(state: Any, data: bytes, outlen: int) -> bytes:  # noqa: N802 - the fallback's name
            """
            Decode, and hash the result.

            Example:
                See :func:`_lit_windows`.


            :param state: The decoder state.
            :param data: The compressed bytes.
            :param outlen: How many bytes to produce.
            :return: The decompressed bytes.
            """
            out = lzx.LZXdecompress(state, data, outlen)
            digests.append(hashlib.sha256(out).hexdigest())
            return out

    monkeypatch.setattr(_compat._Lzx, "_module", staticmethod(lambda: Recording))
    log = logging.getLogger("tests.lzx-frames")
    monkeypatch.setattr(log, "warn", log.warning, raising=False)
    with path.open("rb") as stream:
        book = lit_reader.LitFile(stream, log)
        for name in list(book.entries):
            if not name.endswith("/"):  # the root directory holds no data
                book.get_file(name)
    return sorted(digests)


@pytest.mark.parametrize("name", sorted(LIT_WINDOWS))
def test_the_lit_samples_decode_as_calibre_s_compiled_decoder_does(
    name: str, md_test_files_by_ext: dict[str, list[Path]], monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    Decode each LZX window of the LIT samples to the bytes calibre's compiled decoder gives.

    The windows are 128 KiB; two matches in them run on past a frame's end.

    Example:
        Run with the data repository checked out::

            python -m pytest -q tests/utils/plugins/fallbacks/test_lzx_frames.py


    :param name: The sample book.
    :param md_test_files_by_ext: The data repository's test books, by extension.
    :param monkeypatch: Pytest fixture, to put the fallback behind the reader.
    :return: None; the assertions are the check.
    """
    path = next((p for p in md_test_files_by_ext.get("lit", []) if p.name == name), None)
    if path is None:
        pytest.skip(f"{name} is not available")
    assert _lit_windows(path, monkeypatch) == LIT_WINDOWS[name]

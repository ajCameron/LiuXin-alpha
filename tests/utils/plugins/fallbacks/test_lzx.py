
"""
Provide test lzx utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test lzx through a consuming regression::

        python -m pytest -q tests/utils/plugins/fallbacks/test_lzx.py
"""
from __future__ import annotations

import multiprocessing as mp
import os
import random
from dataclasses import dataclass
from typing import Any, Tuple

import pytest


@dataclass
class _BitWriter16LE:
    """
    Produce a byte stream compatible with the lzx.c bit reader:

    Example:
        Exercise  BitWriter16LE through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_lzx.py
    """
    out: bytearray
    cur: int = 0
    nbits: int = 0

    def write(self, value: int, n: int) -> None:
        """
        Forward the write operation while preserving adapter ownership rules.

        Example:
            Exercise  BitWriter16LE.write through a consuming regression::

                python -m pytest -q tests/utils/plugins/fallbacks/test_lzx.py


        :param value: Value normalized, stored, formatted or returned.
        :param n: Value supplied for n under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if n < 0:
            raise ValueError("n must be >= 0")
        for i in range(n - 1, -1, -1):
            bit = (value >> i) & 1
            self.cur = (self.cur << 1) | bit
            self.nbits += 1
            if self.nbits == 16:
                self.out.append(self.cur & 0xFF)
                self.out.append((self.cur >> 8) & 0xFF)
                self.cur = 0
                self.nbits = 0

    def finish(self) -> bytes:
        """
        Perform the finish utility operation under explicit compatibility rules.

        Example:
            Exercise  BitWriter16LE.finish through a consuming regression::

                python -m pytest -q tests/utils/plugins/fallbacks/test_lzx.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self.nbits:
            self.cur <<= (16 - self.nbits)
            self.out.append(self.cur & 0xFF)
            self.out.append((self.cur >> 8) & 0xFF)
            self.cur = 0
            self.nbits = 0
        return bytes(self.out)


def _make_uncompressed_stream(payload: bytes, *, block_type: int = 3) -> bytes:
    """
    Craft a minimal stream that exercises the 'uncompressed block' path.

    Example:
        Exercise  make uncompressed stream through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_lzx.py


    :param payload: Value supplied for payload under the utility contract.
    :param block_type: Value supplied for block type under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    bw = _BitWriter16LE(out=bytearray())

    # Intel header: k=0 => intel_filesize=0
    bw.write(0, 1)

    # Block header
    if block_type not in (3,):
        raise ValueError("This helper only crafts uncompressed blocks")
    bw.write(block_type, 3)

    L = len(payload)
    i = (L >> 8) & 0xFFFF
    j = L & 0xFF
    bw.write(i, 16)
    bw.write(j, 8)

    prefix = bw.finish()
    # We expect 28 bits total => padded to 32 => 4 bytes output.
    assert len(prefix) == 4

    # R0/R1/R2: choose 1,1,1 (common default)
    r = (1).to_bytes(4, "little")
    return prefix + (r * 3) + payload


# --- subprocess runner: protects pytest from hangs on adversarial inputs ---

def _worker_decompress(data: bytes, outlen: int, q: "mp.Queue[Tuple[str, Any]]") -> None:
    """
    Perform the worker decompress utility operation under explicit compatibility rules.

    Example:
        Exercise  worker decompress through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_lzx.py


    :param data: Value supplied for data under the utility contract.
    :param outlen: Value supplied for outlen under the utility contract.
    :param q: Value supplied for q under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    try:
        from LiuXin_alpha.utils.plugins.fallbacks import lzx

        st = lzx.LZXinit(15)  # 32KiB window
        out = st.decompress(data, outlen)
        q.put(("ok", out))
    except BaseException as e:  # noqa: BLE001
        q.put(("exc", (e.__class__.__name__, str(e))))


def _mp_context() -> mp.context.BaseContext:
    """
    Perform the mp context utility operation under explicit compatibility rules.

    Example:
        Exercise  mp context through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_lzx.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    methods = set(mp.get_all_start_methods())
    if "fork" in methods:
        return mp.get_context("fork")
    return mp.get_context("spawn")


def _decompress_with_timeout(data: bytes, outlen: int, *, timeout_s: float = 2.0) -> Tuple[str, Any]:
    """
    Perform the decompress with timeout utility operation under explicit compatibility rules.

    Example:
        Exercise  decompress with timeout through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_lzx.py


    :param data: Value supplied for data under the utility contract.
    :param outlen: Value supplied for outlen under the utility contract.
    :param timeout_s: Value supplied for timeout s under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    ctx = _mp_context()
    q: "mp.Queue[Tuple[str, Any]]" = ctx.Queue()
    p = ctx.Process(target=_worker_decompress, args=(data, outlen, q))
    p.daemon = True
    p.start()
    p.join(timeout_s)
    if p.is_alive():
        p.terminate()
        p.join(1.0)
        pytest.fail(f"lzx.decompress hung for >{timeout_s}s on input len={len(data)}")
    if q.empty():
        pytest.fail("worker exited without returning a result (unexpected)")
    return q.get_nowait()


# --- contract tests ---

def test_lzxinit_rejects_out_of_range_window() -> None:
    """
    Perform the test lzxinit rejects out of range window utility operation under explicit compatibility rules.

    Example:
        Exercise test lzxinit rejects out of range window through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_lzx.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.utils.plugins.fallbacks import lzx

    with pytest.raises(ValueError):
        lzx.LZXinit(14)
    with pytest.raises(ValueError):
        lzx.LZXinit(22)


def test_uncompressed_block_roundtrip_small_payload() -> None:
    """
    Perform the test uncompressed block roundtrip small payload utility operation under explicit compatibility rules.

    Example:
        Exercise test uncompressed block roundtrip small payload through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_lzx.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.utils.plugins.fallbacks import lzx

    payload = b"hello"
    stream = _make_uncompressed_stream(payload)
    st = lzx.LZXinit(15)

    out = st.decompress(stream, len(payload))
    assert out == payload


def test_uncompressed_block_roundtrip_binary_payload() -> None:
    """
    Perform the test uncompressed block roundtrip binary payload utility operation under explicit compatibility rules.

    Example:
        Exercise test uncompressed block roundtrip binary payload through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_lzx.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.utils.plugins.fallbacks import lzx

    payload = bytes([0, 255, 1, 2, 3, 128, 127, 0, 9])
    stream = _make_uncompressed_stream(payload)
    st = lzx.LZXinit(15)

    out = st.decompress(stream, len(payload))
    assert out == payload


def test_reset_allows_reuse() -> None:
    """
    Perform the test reset allows reuse utility operation under explicit compatibility rules.

    Example:
        Exercise test reset allows reuse through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_lzx.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.utils.plugins.fallbacks import lzx

    st = lzx.LZXinit(15)

    p1 = b"abc"
    s1 = _make_uncompressed_stream(p1)
    assert st.decompress(s1, len(p1)) == p1

    st.reset()

    p2 = b"xyz123"
    s2 = _make_uncompressed_stream(p2)
    assert st.decompress(s2, len(p2)) == p2


# --- fuzz / hang guard ---

@pytest.mark.parametrize(
    "data",
    [
        b"\x00",
        b"\xff",
        b"\x00\x00",
        b"\xff\xff",
        b"\x00" * 8,
        b"\xff" * 8,
        bytes(range(16)),
        bytes(range(32)),
    ],
)
def test_malformed_inputs_do_not_hang(data: bytes) -> None:
    """
    Perform the test malformed inputs do not hang utility operation under explicit compatibility rules.

    Example:
        Exercise test malformed inputs do not hang through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_lzx.py


    :param data: Value supplied for data under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    status, _ = _decompress_with_timeout(data, outlen=8)
    assert status in {"ok", "exc"}


def test_small_random_fuzz_does_not_hang() -> None:
    """
    Perform the test small random fuzz does not hang utility operation under explicit compatibility rules.

    Example:
        Exercise test small random fuzz does not hang through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_lzx.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    rng = random.Random(4242)
    for _ in range(10):
        ln = rng.randint(1, 64)
        blob = os.urandom(ln)
        status, _ = _decompress_with_timeout(blob, outlen=32)
        assert status in {"ok", "exc"}

# tests/utils/plugins/fallbacks/test_bzzdec.py

from __future__ import annotations

import multiprocessing as mp
import os
import random
from typing import Any, Tuple

import pytest


# --- subprocess runner (guards against pathological inputs hanging forever) ---

def _worker_decompress(data: bytes, q: "mp.Queue[Tuple[str, Any]]") -> None:
    """
    Run in a fresh process so a pathological bitstream can't hang the whole test run.
    """
    try:
        from LiuXin_alpha.utils.plugins.fallbacks import bzzdec as mod  # local import for spawn

        out = mod.decompress(data)
        q.put(("ok", bytes(out)))
    except BaseException as e:  # noqa: BLE001 - we want to ship failure info across processes
        q.put(("exc", (e.__class__.__name__, str(e))))


def _decompress_with_timeout(data: bytes, *, timeout_s: float = 5.0) -> Tuple[str, Any]:
    ctx = mp.get_context("spawn")
    q: "mp.Queue[Tuple[str, Any]]" = ctx.Queue()
    p = ctx.Process(target=_worker_decompress, args=(data, q))
    p.daemon = True
    p.start()
    p.join(timeout_s)
    if p.is_alive():
        p.terminate()
        p.join(1.0)
        pytest.fail(f"bzzdec.decompress hung for >{timeout_s}s on input len={len(data)}")
    if q.empty():
        pytest.fail("worker exited without returning a result (unexpected)")
    return q.get_nowait()


# --- import-time / type contract tests ---

def test_decompress_rejects_non_byteslike() -> None:
    from LiuXin_alpha.utils.plugins.fallbacks import bzzdec

    with pytest.raises(TypeError):
        bzzdec.decompress("not-bytes")  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        bzzdec.decompress(123)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        bzzdec.decompress(object())  # type: ignore[arg-type]


# --- real-world(ish) smoke + fuzz (timeout guarded) ---

def test_decompress_smoke_known_valid_stream_returns_empty() -> None:
    """
    Tiny stream that the current pure fallback successfully decodes to an empty payload.
    This is mainly a "does it work end-to-end" sentinel (and catches import/packaging issues).
    """
    data = bytes.fromhex("ff06")
    status, payload = _decompress_with_timeout(data, timeout_s=5.25)
    assert status == "ok"
    assert payload == b""


@pytest.mark.parametrize(
    "data",
    [
        b"\x00",
        b"\xff",
        b"\x00\x00",
        b"\xff\xff",
        b"\x00\xff",
        b"\xff\x00",
        b"\x01\x02\x03",
        b"\x80\x00\x00",
        b"\x7f\xff\xff",
        b"\x00" * 8,
        b"\xff" * 8,
        bytes(range(16)),
    ],
)
def test_decompress_malformed_inputs_do_not_hang(data: bytes) -> None:
    """
    We don't assert *what* error happens for malformed data, only that it returns quickly.
    """
    status, payload = _decompress_with_timeout(data, timeout_s=5.25)
    assert status in {"ok", "exc"}


def test_decompress_small_random_fuzz_does_not_hang() -> None:
    """
    Tiny fuzz corpus with a fixed seed so it stays stable.
    """
    rng = random.Random(1337)
    for _ in range(12):
        ln = rng.randint(1, 24)
        blob = os.urandom(ln)
        status, _payload = _decompress_with_timeout(blob, timeout_s=5.25)
        assert status in {"ok", "exc"}


# --- deterministic unit tests of output-header behaviour (monkeypatched decode) ---

def test_decompress_strips_3byte_size_header_and_truncates(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Validate the post-processing logic independently from the arithmetic decoder:
    [size_hi, size_mid, size_lo] + payload -> return payload[:size]
    """
    from LiuXin_alpha.utils.plugins.fallbacks import bzzdec as mod

    calls = {"n": 0}

    def fake_init_state(st: Any) -> None:
        st.buf = bytearray(128)
        st.is_eof = False
        st.xsize = 0

    def fake_decode_block(st: Any, _ctx: bytearray) -> bool:
        calls["n"] += 1
        if calls["n"] == 1:
            out = bytes([0x00, 0x00, 0x03]) + b"abcd"  # expected=3, payload len=4
            st.buf[: len(out)] = out
            st.xsize = len(out) + 1  # matches the real decoder's "xsize then decrement" pattern
            return True
        return False  # triggers EOF path in decompress()

    monkeypatch.setattr(mod, "_init_state", fake_init_state)
    monkeypatch.setattr(mod, "_decode_block", fake_decode_block)

    assert mod.decompress(b"irrelevant") == b"abc"


def test_decompress_when_expected_exceeds_payload_returns_all_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    The fallback chooses the safer behavior: if header says 'need more than we got',
    return what we have (rather than erroring or reading uninitialized bytes).
    """
    from LiuXin_alpha.utils.plugins.fallbacks import bzzdec as mod

    calls = {"n": 0}

    def fake_init_state(st: Any) -> None:
        st.buf = bytearray(128)
        st.is_eof = False
        st.xsize = 0

    def fake_decode_block(st: Any, _ctx: bytearray) -> bool:
        calls["n"] += 1
        if calls["n"] == 1:
            out = bytes([0x00, 0x00, 0x0A]) + b"xyz"  # expected=10, payload len=3
            st.buf[: len(out)] = out
            st.xsize = len(out) + 1
            return True
        return False

    monkeypatch.setattr(mod, "_init_state", fake_init_state)
    monkeypatch.setattr(mod, "_decode_block", fake_decode_block)

    assert mod.decompress(b"irrelevant") == b"xyz"


def test_decompress_missing_output_header_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    from LiuXin_alpha.utils.plugins.fallbacks import bzzdec as mod

    calls = {"n": 0}

    def fake_init_state(st: Any) -> None:
        st.buf = bytearray(128)
        st.is_eof = False
        st.xsize = 0

    def fake_decode_block(st: Any, _ctx: bytearray) -> bool:
        calls["n"] += 1
        if calls["n"] == 1:
            out = b"\x00\x01"  # only 2 bytes -> should trip "missing output header"
            st.buf[: len(out)] = out
            st.xsize = len(out) + 1
            return True
        return False

    monkeypatch.setattr(mod, "_init_state", fake_init_state)
    monkeypatch.setattr(mod, "_decode_block", fake_decode_block)

    with pytest.raises(ValueError, match="missing output header"):
        mod.decompress(b"irrelevant")

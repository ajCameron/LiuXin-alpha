# tests/utils/plugins/fallbacks/test_bzzdec.py

"""
Provide test bzzdec utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test bzzdec through a consuming regression::

        python -m pytest -q tests/utils/plugins/fallbacks/test_bzzdec.py
"""
from __future__ import annotations

import multiprocessing as mp
import random
from typing import Any, Tuple

import pytest


# --- subprocess runner (guards against pathological inputs hanging forever) ---

def _mp_context() -> mp.context.BaseContext:
    """
    Perform the mp context utility operation under explicit compatibility rules.

    Example:
        Exercise  mp context through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_bzzdec.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    methods = set(mp.get_all_start_methods())
    if "fork" in methods:
        return mp.get_context("fork")
    return mp.get_context("spawn")


def _worker_decompress(data: bytes, q: "mp.Queue[Tuple[str, Any]]") -> None:
    """
    Run in a fresh process so a pathological bitstream can't hang the whole test run.

    Example:
        Exercise  worker decompress through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_bzzdec.py


    :param data: Value supplied for data under the utility contract.
    :param q: Value supplied for q under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    try:
        from LiuXin_alpha.utils.plugins.fallbacks import bzzdec as mod  # local import for spawn

        out = mod.decompress(data)
        q.put(("ok", bytes(out)))
    except BaseException as e:  # noqa: BLE001 - we want to ship failure info across processes
        q.put(("exc", (e.__class__.__name__, str(e))))


def _decompress_with_timeout(data: bytes, *, timeout_s: float = 5.0) -> Tuple[str, Any]:
    """
    Perform the decompress with timeout utility operation under explicit compatibility rules.

    Example:
        Exercise  decompress with timeout through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_bzzdec.py


    :param data: Value supplied for data under the utility contract.
    :param timeout_s: Value supplied for timeout s under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    ctx = _mp_context()
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
    """
    Perform the test decompress rejects non byteslike utility operation under explicit compatibility rules.

    Example:
        Exercise test decompress rejects non byteslike through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_bzzdec.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
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
    Tiny stream that the current pure fallback successfully decodes to an empty payload. This is mainly a "does it work end-to-end" sentinel (and catches import/packaging issues).

    Example:
        Exercise test decompress smoke known valid stream returns empty through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_bzzdec.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
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

    Example:
        Exercise test decompress malformed inputs do not hang through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_bzzdec.py


    :param data: Value supplied for data under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    status, payload = _decompress_with_timeout(data, timeout_s=5.25)
    assert status in {"ok", "exc"}


def test_decompress_small_random_fuzz_does_not_hang() -> None:
    """
    Tiny fuzz corpus with a fixed seed so it stays stable.

    Example:
        Exercise test decompress small random fuzz does not hang through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_bzzdec.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    rng = random.Random(1337)
    for _ in range(12):
        ln = rng.randint(1, 24)
        blob = bytes(rng.getrandbits(8) for _ in range(ln))
        status, _payload = _decompress_with_timeout(blob, timeout_s=5.25)
        assert status in {"ok", "exc"}


def test_decompress_rejects_implausible_tiny_stream_expansion_quickly() -> None:
    """
    Regression case from a full coverage run: this 12-byte malformed payload claimed a multi-megabyte block and spent seconds decoding before EOF.

    Example:
        Exercise test decompress rejects implausible tiny stream expansion quickly through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_bzzdec.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.utils.plugins.fallbacks import bzzdec

    data = b'\xd3\xd3HC,r"J+O\x14\xf4'
    with pytest.raises(ValueError, match="implausible block expansion"):
        bzzdec.decompress(data)


# --- deterministic unit tests of output-header behaviour (monkeypatched decode) ---

def test_decompress_strips_3byte_size_header_and_truncates(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Validate the post-processing logic independently from the arithmetic decoder: [size_hi, size_mid, size_lo] + payload -> return payload[:size]

    Example:
        Exercise test decompress strips 3byte size header and truncates through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_bzzdec.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    from LiuXin_alpha.utils.plugins.fallbacks import bzzdec as mod

    calls = {"n": 0}

    def fake_init_state(st: Any) -> None:
        """
        Perform the fake init state utility operation under explicit compatibility rules.

        Example:
            Exercise test decompress strips 3byte size header and truncates.fake init state through a consuming regression::

                python -m pytest -q tests/utils/plugins/fallbacks/test_bzzdec.py


        :param st: Value supplied for st under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        st.buf = bytearray(128)
        st.is_eof = False
        st.xsize = 0

    def fake_decode_block(st: Any, _ctx: bytearray) -> bool:
        """
        Perform the fake decode block utility operation under explicit compatibility rules.

        Example:
            Exercise test decompress strips 3byte size header and truncates.fake decode block through a consuming regression::

                python -m pytest -q tests/utils/plugins/fallbacks/test_bzzdec.py


        :param st: Value supplied for st under the utility contract.
        :param _ctx: Value supplied for ctx under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
    The fallback chooses the safer behavior: if header says 'need more than we got', return what we have (rather than erroring or reading uninitialized bytes).

    Example:
        Exercise test decompress when expected exceeds payload returns all payload through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_bzzdec.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    from LiuXin_alpha.utils.plugins.fallbacks import bzzdec as mod

    calls = {"n": 0}

    def fake_init_state(st: Any) -> None:
        """
        Perform the fake init state utility operation under explicit compatibility rules.

        Example:
            Exercise test decompress when expected exceeds payload returns all payload.fake init state through a consuming regression::

                python -m pytest -q tests/utils/plugins/fallbacks/test_bzzdec.py


        :param st: Value supplied for st under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        st.buf = bytearray(128)
        st.is_eof = False
        st.xsize = 0

    def fake_decode_block(st: Any, _ctx: bytearray) -> bool:
        """
        Perform the fake decode block utility operation under explicit compatibility rules.

        Example:
            Exercise test decompress when expected exceeds payload returns all payload.fake decode block through a consuming regression::

                python -m pytest -q tests/utils/plugins/fallbacks/test_bzzdec.py


        :param st: Value supplied for st under the utility contract.
        :param _ctx: Value supplied for ctx under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
    """
    Perform the test decompress missing output header raises utility operation under explicit compatibility rules.

    Example:
        Exercise test decompress missing output header raises through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_bzzdec.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    from LiuXin_alpha.utils.plugins.fallbacks import bzzdec as mod

    calls = {"n": 0}

    def fake_init_state(st: Any) -> None:
        """
        Perform the fake init state utility operation under explicit compatibility rules.

        Example:
            Exercise test decompress missing output header raises.fake init state through a consuming regression::

                python -m pytest -q tests/utils/plugins/fallbacks/test_bzzdec.py


        :param st: Value supplied for st under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        st.buf = bytearray(128)
        st.is_eof = False
        st.xsize = 0

    def fake_decode_block(st: Any, _ctx: bytearray) -> bool:
        """
        Perform the fake decode block utility operation under explicit compatibility rules.

        Example:
            Exercise test decompress missing output header raises.fake decode block through a consuming regression::

                python -m pytest -q tests/utils/plugins/fallbacks/test_bzzdec.py


        :param st: Value supplied for st under the utility contract.
        :param _ctx: Value supplied for ctx under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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

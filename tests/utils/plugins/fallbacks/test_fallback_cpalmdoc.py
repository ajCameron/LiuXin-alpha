from __future__ import annotations

import os

import pytest


def test_cpalmdoc_roundtrip_small_text() -> None:
    from LiuXin_alpha.utils.plugins.fallbacks import cPalmdoc

    data = b"Hello  world!  This is PalmDOC.\n\n" * 3
    comp = cPalmdoc.compress(data)
    assert isinstance(comp, (bytes, bytearray))
    decomp = cPalmdoc.decompress(comp)
    assert decomp == data


def test_cpalmdoc_roundtrip_random_bytes() -> None:
    from LiuXin_alpha.utils.plugins.fallbacks import cPalmdoc

    # PalmDOC was designed for text; we still want it to be stable on arbitrary bytes.
    data = os.urandom(512)
    comp = cPalmdoc.compress(data)
    decomp = cPalmdoc.decompress(comp)
    assert decomp == data


def test_cpalmdoc_decompress_corrupt_stream_is_best_effort_not_crash() -> None:
    from LiuXin_alpha.utils.plugins.fallbacks import cPalmdoc

    # A truncated backref (0x80-0xBF) should not raise.
    out = cPalmdoc.decompress(b"\x80")
    assert isinstance(out, (bytes, bytearray))


def test_cpalmdoc_type_contract() -> None:
    from LiuXin_alpha.utils.plugins.fallbacks import cPalmdoc

    with pytest.raises(TypeError):
        cPalmdoc.compress("nope")  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        cPalmdoc.decompress("nope")  # type: ignore[arg-type]

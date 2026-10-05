"""
Provide test fallback cpalmdoc utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test fallback cpalmdoc through a consuming regression::

        python -m pytest -q tests/utils/plugins/fallbacks/test_fallback_cpalmdoc.py
"""
from __future__ import annotations

import os

import pytest


def test_cpalmdoc_roundtrip_small_text() -> None:
    """
    Perform the test cpalmdoc roundtrip small text utility operation under explicit compatibility rules.

    Example:
        Exercise test cpalmdoc roundtrip small text through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_fallback_cpalmdoc.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.utils.plugins.fallbacks import cPalmdoc

    data = b"Hello  world!  This is PalmDOC.\n\n" * 3
    comp = cPalmdoc.compress(data)
    assert isinstance(comp, (bytes, bytearray))
    decomp = cPalmdoc.decompress(comp)
    assert decomp == data


def test_cpalmdoc_roundtrip_random_bytes() -> None:
    """
    Perform the test cpalmdoc roundtrip random bytes utility operation under explicit compatibility rules.

    Example:
        Exercise test cpalmdoc roundtrip random bytes through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_fallback_cpalmdoc.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.utils.plugins.fallbacks import cPalmdoc

    # PalmDOC was designed for text; we still want it to be stable on arbitrary bytes.
    data = os.urandom(512)
    comp = cPalmdoc.compress(data)
    decomp = cPalmdoc.decompress(comp)
    assert decomp == data


def test_cpalmdoc_decompress_corrupt_stream_is_best_effort_not_crash() -> None:
    """
    Perform the test cpalmdoc decompress corrupt stream is best effort not crash utility operation under explicit compatibility rules.

    Example:
        Exercise test cpalmdoc decompress corrupt stream is best effort not crash through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_fallback_cpalmdoc.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.utils.plugins.fallbacks import cPalmdoc

    # A truncated backref (0x80-0xBF) should not raise.
    out = cPalmdoc.decompress(b"\x80")
    assert isinstance(out, (bytes, bytearray))


def test_cpalmdoc_type_contract() -> None:
    """
    Perform the test cpalmdoc type contract utility operation under explicit compatibility rules.

    Example:
        Exercise test cpalmdoc type contract through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_fallback_cpalmdoc.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.utils.plugins.fallbacks import cPalmdoc

    with pytest.raises(TypeError):
        cPalmdoc.compress("nope")  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        cPalmdoc.decompress("nope")  # type: ignore[arg-type]

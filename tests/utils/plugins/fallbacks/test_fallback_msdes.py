"""
Provide test fallback msdes utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test fallback msdes through a consuming regression::

        python -m pytest -q tests/utils/plugins/fallbacks/test_fallback_msdes.py
"""
from __future__ import annotations

import pytest


def test_msdes_known_vector_encrypt_decrypt() -> None:
    """
    NIST classic DES test vector.

    Example:
        Exercise test msdes known vector encrypt decrypt through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_fallback_msdes.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """

    from LiuXin_alpha.utils.plugins.fallbacks import msdes

    key = bytes.fromhex("133457799BBCDFF1")
    pt = bytes.fromhex("0123456789ABCDEF")
    expected_ct = bytes.fromhex("85E813540F0AB405")

    msdes.deskey(key, msdes.EN0)
    ct = msdes.des(pt)
    assert ct == expected_ct

    msdes.deskey(key, msdes.DE1)
    pt2 = msdes.des(ct)
    assert pt2 == pt


def test_msdes_errors_when_no_key_schedule() -> None:
    """
    Perform the test msdes errors when no key schedule utility operation under explicit compatibility rules.

    Example:
        Exercise test msdes errors when no key schedule through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_fallback_msdes.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.utils.plugins.fallbacks import msdes
    # Ensure we start from a "no key" state even if other tests ran first.
    msdes._subkeys = []  # type: ignore[attr-defined]


    with pytest.raises(msdes.MsDesError, match="call deskey"):
        msdes.des(b"\x00" * 8)


def test_msdes_rejects_wrong_key_length_and_data_length() -> None:
    """
    Perform the test msdes rejects wrong key length and data length utility operation under explicit compatibility rules.

    Example:
        Exercise test msdes rejects wrong key length and data length through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_fallback_msdes.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.utils.plugins.fallbacks import msdes

    with pytest.raises(msdes.MsDesError, match="Key length"):
        msdes.deskey(b"short", msdes.EN0)

    msdes.deskey(b"\x00" * 8, msdes.EN0)
    with pytest.raises(msdes.MsDesError, match="multiple"):
        msdes.des(b"123")


def test_msdes_rejects_invalid_direction() -> None:
    """
    Perform the test msdes rejects invalid direction utility operation under explicit compatibility rules.

    Example:
        Exercise test msdes rejects invalid direction through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_fallback_msdes.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.utils.plugins.fallbacks import msdes

    with pytest.raises(msdes.MsDesError, match="direction"):
        msdes.deskey(b"\x00" * 8, 999)

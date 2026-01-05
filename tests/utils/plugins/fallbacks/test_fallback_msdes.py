from __future__ import annotations

import pytest


def test_msdes_known_vector_encrypt_decrypt() -> None:
    """NIST classic DES test vector."""

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
    from LiuXin_alpha.utils.plugins.fallbacks import msdes
    # Ensure we start from a "no key" state even if other tests ran first.
    msdes._subkeys = []  # type: ignore[attr-defined]


    with pytest.raises(msdes.MsDesError, match="call deskey"):
        msdes.des(b"\x00" * 8)


def test_msdes_rejects_wrong_key_length_and_data_length() -> None:
    from LiuXin_alpha.utils.plugins.fallbacks import msdes

    with pytest.raises(msdes.MsDesError, match="Key length"):
        msdes.deskey(b"short", msdes.EN0)

    msdes.deskey(b"\x00" * 8, msdes.EN0)
    with pytest.raises(msdes.MsDesError, match="multiple"):
        msdes.des(b"123")


def test_msdes_rejects_invalid_direction() -> None:
    from LiuXin_alpha.utils.plugins.fallbacks import msdes

    with pytest.raises(msdes.MsDesError, match="direction"):
        msdes.deskey(b"\x00" * 8, 999)

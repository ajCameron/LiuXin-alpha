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

#: Keys, directions, inputs and outputs from calibre's compiled msdes extension.
CALIBRE_VECTORS = (
    ("133457799bbcdff1", 0, "0123456789abcdef", "b95a0c9072207039"),
    (
        "52f22665a60c12d2",
        1,
        "89185d950ee8813609166f6b113d178d",
        "2506706a9be0a7af9fbe484d8d7cd955",
    ),
    (
        "0fd3901ff239a1a0",
        0,
        "95f20f9395650cf9380b8edb224a6b24",
        "31a8e552dfeaa5c8a78326f0435115d4",
    ),
    (
        "924e8fd0ae2e1a94",
        1,
        "92a3305f188cb610900f9e347fae886d",
        "3d0133a4c07994d7a03a8640f65517ab",
    ),
    (
        "7795ec745c4c3fcb",
        0,
        "2eb2c73e14934c867ee057ba72499bfa",
        "7cd75052d5c171e7c1a543e339a23fc7",
    ),
)


@pytest.mark.parametrize(("key", "edf", "data", "expected"), CALIBRE_VECTORS)
def test_msdes_gives_calibre_s_answers(
    key: str, edf: int, data: str, expected: str
) -> None:
    """
    The fallback gives what calibre's compiled extension gives: LIT's DES, not the standard's.

    calibre builds its extension with openclit's S-box tables, so the NIST vector
    (85e813540f0ab405 for the first key here) does not hold; LIT books need these
    answers for their title keys to decrypt.

    Example:
        >>> test_msdes_gives_calibre_s_answers(*CALIBRE_VECTORS[0])


    :param key: The key, in hex.
    :param edf: ``EN0`` (0) or ``DE1`` (1).
    :param data: The input blocks, in hex.
    :param expected: calibre's output, in hex.
    :return: None after the output is checked.
    """
    from LiuXin_alpha.utils.plugins.fallbacks import msdes

    msdes.deskey(bytes.fromhex(key), edf)
    assert msdes.des(bytes.fromhex(data)).hex() == expected
    msdes.deskey(bytes.fromhex(key), 1 - edf)
    assert msdes.des(bytes.fromhex(expected)).hex() == data


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
    msdes._KnL = None

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

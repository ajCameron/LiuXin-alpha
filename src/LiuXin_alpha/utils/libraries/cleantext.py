"""
Normalize markup-derived text, whitespace and character entities.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise cleantext through a consuming regression::

        python -m pytest -q tests/scripts/test_docstring_migration.py
"""

from __future__ import annotations

import re
from html.entities import name2codepoint
from typing import Iterable, Union

__license__ = "GPL 3"
__copyright__ = "2010, sengian <sengian1@gmail.com>"
__docformat__ = "restructuredtext en"

__all__ = [
    "clean_ascii_chars",
    "allowed_xml_char",
    "clean_xml_chars",
    "unescape",
]

_TextLike = Union[str, bytes, bytearray, memoryview]

# Cached translate table for default ASCII control-char removal
_ASCII_DELETE_TRANS: dict[int, None] | None = None

# Compiled pattern for entity-ish sequences (kept close to original behavior)
_ENTITY_PAT = re.compile(r"&#?\w+;")


def _coerce_text(
    txt: _TextLike,
    *,
    encoding: str = "utf-8",
    errors: str = "replace",
) -> str:
    """
    Coerce text under the documented compatibility and safety rules.

    Example:
        Exercise  coerce text through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_migration.py


    :param txt: Value supplied for txt under the utility contract.
    :param encoding: Value supplied for encoding under the utility contract.
    :param errors: Value supplied for errors under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if isinstance(txt, str):
        return txt
    return bytes(txt).decode(encoding, errors)


def _build_default_ascii_delete_trans() -> dict[int, None]:
    """
    Perform the build default ascii delete trans utility operation under explicit compatibility rules.

    Example:
        Exercise  build default ascii delete trans through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_migration.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    chars = set(range(32))
    chars.add(127)
    for x in (9, 10, 13):
        chars.discard(x)
    return {c: None for c in chars}


def clean_ascii_chars(
    txt: _TextLike,
    charlist: Iterable[int] | None = None,
    *,
    encoding: str = "utf-8",
    errors: str = "replace",
) -> str:
    """
    Perform the clean ascii chars utility operation under explicit compatibility rules.

    Example:
        Exercise clean ascii chars through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_migration.py


    :param txt: Value supplied for txt under the utility contract.
    :param charlist: Value supplied for charlist under the utility contract.
    :param encoding: Value supplied for encoding under the utility contract.
    :param errors: Value supplied for errors under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if not txt:
        return ""

    s = _coerce_text(txt, encoding=encoding, errors=errors)

    if charlist is None:
        global _ASCII_DELETE_TRANS
        if _ASCII_DELETE_TRANS is None:
            _ASCII_DELETE_TRANS = _build_default_ascii_delete_trans()
        return s.translate(_ASCII_DELETE_TRANS)

    delete_map: dict[int, None] = {}
    for cp in charlist:
        if not isinstance(cp, int):
            raise TypeError(f"charlist must contain ints (codepoints), got {type(cp)!r}")
        if cp < 0 or cp > 0x10FFFF:
            raise ValueError(f"Invalid Unicode codepoint in charlist: {cp}")
        delete_map[cp] = None

    return s.translate(delete_map)


def allowed_xml_char(ch: str, *, remove_del: bool = True) -> bool:
    """
    Returns True if `ch` is allowed by XML 1.0 character rules.

    Example:
        Exercise allowed xml char through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_migration.py


    :param ch: Value supplied for ch under the utility contract.
    :param remove_del: Value supplied for remove del under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    cp = ord(ch)
    if remove_del and cp == 0x7F:
        return False
    return (
        cp in (0x9, 0xA, 0xD)
        or (0x20 <= cp <= 0xD7FF)
        or (0xE000 <= cp <= 0xFFFD)
        or (0x10000 <= cp <= 0x10FFFF)
    )


def clean_xml_chars(
    unicode_string: _TextLike,
    *,
    remove_del: bool = True,
    encoding: str = "utf-8",
    errors: str = "replace",
) -> str:
    """
    Remove characters that are not allowed in XML 1.0.

    Example:
        Exercise clean xml chars through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_migration.py


    :param unicode_string: Value supplied for unicode string under the utility contract.
    :param remove_del: Value supplied for remove del under the utility contract.
    :param encoding: Value supplied for encoding under the utility contract.
    :param errors: Value supplied for errors under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if not unicode_string:
        return ""
    s = _coerce_text(unicode_string, encoding=encoding, errors=errors)
    return "".join(ch for ch in s if allowed_xml_char(ch, remove_del=remove_del))


def unescape(
    text: _TextLike,
    rm: bool = False,
    rchar: str = "",
    *,
    encoding: str = "utf-8",
    errors: str = "replace",
) -> str:
    """
    Removes HTML/XML character references and entities from a text string.

    Example:
        Exercise unescape through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_migration.py


    :param text: Text parsed, normalized or rendered.
    :param rm: Value supplied for rm under the utility contract.
    :param rchar: Value supplied for rchar under the utility contract.
    :param encoding: Value supplied for encoding under the utility contract.
    :param errors: Value supplied for errors under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    s = _coerce_text(text, encoding=encoding, errors=errors)

    def fixup(m: re.Match[str]) -> str:
        """
        Perform the fixup utility operation under explicit compatibility rules.

        Example:
            Exercise unescape.fixup through a consuming regression::

                python -m pytest -q tests/scripts/test_docstring_migration.py


        :param m: Value supplied for m under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ent = m.group(0)
        if ent.startswith("&#"):
            try:
                if ent.startswith("&#x") or ent.startswith("&#X"):
                    return chr(int(ent[3:-1], 16))
                return chr(int(ent[2:-1], 10))
            except ValueError:
                return rchar if rm else ent

        # named entity
        key = ent[1:-1]
        try:
            return chr(name2codepoint[key])
        except KeyError:
            return rchar if rm else ent

    return _ENTITY_PAT.sub(fixup, s)

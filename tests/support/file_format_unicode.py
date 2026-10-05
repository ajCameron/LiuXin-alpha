"""
Build deterministic UNICODE fixtures and test doubles.

The module keeps generated data, ordering and failure modes explicit so consumers
can assert stable behavior.

Example:
    Exercise file format unicode through a consuming regression::

        python -m pytest -q tests/metadata/test_standardization_torture.py
"""
from __future__ import annotations

import codecs
import random
from dataclasses import dataclass
from typing import Callable, Iterable, Sequence, TypeVar


_T = TypeVar("_T")


@dataclass(frozen=True)
class UnicodeCase:
    """
    Carry the deterministic UnicodeCase inputs and expected values used by format tests.

    Example:
        Exercise UnicodeCase through a consuming regression::

            python -m pytest -q tests/metadata/test_standardization_torture.py
    """
    case_id: str
    text: str
    fragments: tuple[str, ...]
    description: str = ""


@dataclass(frozen=True)
class EncodedUnicodeCase:
    """
    Carry the deterministic EncodedUnicodeCase inputs and expected values used by format tests.

    Example:
        Exercise EncodedUnicodeCase through a consuming regression::

            python -m pytest -q tests/metadata/test_standardization_torture.py
    """
    case_id: str
    encoding: str
    payload: bytes
    text: str
    fragments: tuple[str, ...]
    description: str = ""


MULTISCRIPT_CASES: tuple[UnicodeCase, ...] = (
    UnicodeCase("latin", "Latin cafe naive facade deja vu", ("Latin cafe", "facade")),
    UnicodeCase("latin_diacritics", "Latin café naïve coöperate façade déjà vu", ("café", "façade")),
    UnicodeCase("greek", "Greek Καλημέρα κόσμε", ("Καλημέρα", "κόσμε")),
    UnicodeCase("cyrillic", "Cyrillic Здравствуйте, мир", ("Здравствуйте", "мир")),
    UnicodeCase("arabic", "Arabic مرحبا بالعالم", ("مرحبا", "بالعالم")),
    UnicodeCase("hebrew", "Hebrew שלום עולם", ("שלום", "עולם")),
    UnicodeCase("devanagari", "Hindi नमस्ते दुनिया", ("नमस्ते", "दुनिया")),
    UnicodeCase("thai", "Thai สวัสดีโลก", ("สวัสดีโลก",)),
    UnicodeCase(
        "cjk",
        "CJK 你好，世界 / こんにちは世界 / 안녕하세요 세계",
        ("你好，世界", "こんにちは世界", "안녕하세요 세계"),
    ),
    UnicodeCase("emoji", "Emoji 👩🏽‍💻🧪📚🧬", ("👩🏽‍💻", "🧪", "📚", "🧬")),
    UnicodeCase("combining", "Combining cafe\u0301 co\u0308operate A\u030a", ("cafe\u0301", "co\u0308operate", "A\u030a")),
    UnicodeCase("bidi_zwj", "Bidi \u200fمرحبا\u200f and ZWJ A\u200dB", ("\u200fمرحبا\u200f", "A\u200dB")),
)

MULTISCRIPT_TEXT = "\n".join(case.text for case in MULTISCRIPT_CASES)
MULTISCRIPT_FRAGMENTS = tuple(fragment for case in MULTISCRIPT_CASES for fragment in case.fragments)

COMMON_TEXT_FRAGMENTS = (
    "café",
    "Καλημέρα",
    "Здравствуйте",
    "مرحبا",
    "שלום",
    "नमस्ते",
    "สวัสดีโลก",
    "你好，世界",
    "こんにちは世界",
    "안녕하세요 세계",
    "👩🏽‍💻",
    "cafe\u0301",
    "A\u200dB",
)

FUZZ_ALPHABET = (
    "abcXYZ0123456789"
    " cafénaïvefaçade"
    "Καλημέρακόσμε"
    "Здравствуйте"
    "שלום"
    "مرحبا"
    "नमस्ते"
    "สวัสดีโลก"
    "你好世界こんにちは안녕하세요"
    "👩🏽‍💻🧪📚🧬"
    "\u0301\u0308\u030a\u200d\u200f"
    " _*#[]()!\"':;,.?/\\-+=~|\n"
)

KNOWN_BOMS = (
    codecs.BOM_UTF8,
    codecs.BOM_UTF16_LE,
    codecs.BOM_UTF16_BE,
    codecs.BOM_UTF32_LE,
    codecs.BOM_UTF32_BE,
)


def assert_fragments_present(rendered: str, fragments: Sequence[str] = MULTISCRIPT_FRAGMENTS, *, context: str = "") -> None:
    """
    Assert fragments present under the fixture contract.

    Example:
        Exercise assert fragments present through a consuming regression::

            python -m pytest -q tests/metadata/test_standardization_torture.py


    :param rendered: Value supplied for rendered under the deterministic fixture
        contract.
    :param fragments: Value supplied for fragments under the deterministic fixture
        contract.
    :param context: Value supplied for context under the deterministic fixture contract.
    :return: None; fixture state or the supplied destination is updated in place.
    """
    missing = [fragment for fragment in fragments if fragment not in rendered]
    if missing:
        detail = f" for {context}" if context else ""
        raise AssertionError(f"missing Unicode fragments{detail}: {missing!r}")


def assert_no_replacement_chars(rendered: str, *, context: str = "") -> None:
    """
    Assert no replacement chars under the fixture contract.

    Example:
        Exercise assert no replacement chars through a consuming regression::

            python -m pytest -q tests/metadata/test_standardization_torture.py


    :param rendered: Value supplied for rendered under the deterministic fixture
        contract.
    :param context: Value supplied for context under the deterministic fixture contract.
    :return: None; fixture state or the supplied destination is updated in place.
    """
    if "\ufffd" in rendered:
        detail = f" for {context}" if context else ""
        raise AssertionError(f"unexpected replacement character{detail}")


def assert_output_deterministic(renderer: Callable[[_T], str], source: _T, *, context: str = "") -> str:
    """
    Assert output deterministic under the fixture contract.

    Example:
        Exercise assert output deterministic through a consuming regression::

            python -m pytest -q tests/metadata/test_standardization_torture.py


    :param renderer: Value supplied for renderer under the deterministic fixture
        contract.
    :param source: Value supplied for source under the deterministic fixture contract.
    :param context: Value supplied for context under the deterministic fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    first = renderer(source)
    second = renderer(source)
    if first != second:
        detail = f" for {context}" if context else ""
        raise AssertionError(f"renderer output changed between runs{detail}")
    return first


def deterministic_unicode_fuzz(*, seed: int = 20260520, length: int = 512, alphabet: str = FUZZ_ALPHABET) -> str:
    """
    Perform the deterministic unicode fuzz step with deterministic fixture inputs.

    Example:
        Exercise deterministic unicode fuzz through a consuming regression::

            python -m pytest -q tests/metadata/test_standardization_torture.py


    :param seed: Stable seed controlling deterministic generated values.
    :param length: Value supplied for length under the deterministic fixture contract.
    :param alphabet: Value supplied for alphabet under the deterministic fixture
        contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    rng = random.Random(seed)
    return "".join(rng.choice(alphabet) for _ in range(length))


def strip_known_bom(payload: bytes) -> bytes:
    """
    Perform the strip known bom step with deterministic fixture inputs.

    Example:
        Exercise strip known bom through a consuming regression::

            python -m pytest -q tests/metadata/test_standardization_torture.py


    :param payload: Binary or structured payload encoded into the fixture.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    for bom in KNOWN_BOMS:
        if payload.startswith(bom):
            return payload[len(bom) :]
    return payload


def encoded_unicode_cases(
    text: str = MULTISCRIPT_TEXT,
    fragments: Sequence[str] = COMMON_TEXT_FRAGMENTS,
) -> tuple[EncodedUnicodeCase, ...]:
    """
    Perform the encoded unicode cases step with deterministic fixture inputs.

    Example:
        Exercise encoded unicode cases through a consuming regression::

            python -m pytest -q tests/metadata/test_standardization_torture.py


    :param text: Text encoded, parsed or embedded in the fixture.
    :param fragments: Value supplied for fragments under the deterministic fixture
        contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    fragment_tuple = tuple(fragments)
    return (
        EncodedUnicodeCase(
            "utf_8",
            "utf-8",
            text.encode("utf-8"),
            text,
            fragment_tuple,
            "plain UTF-8 without a BOM",
        ),
        EncodedUnicodeCase(
            "utf_8_bom",
            "utf-8",
            codecs.BOM_UTF8 + text.encode("utf-8"),
            text,
            fragment_tuple,
            "UTF-8 payload with a BOM stripped by format readers",
        ),
        EncodedUnicodeCase(
            "utf_16_le_bom",
            "utf-16-le",
            codecs.BOM_UTF16_LE + text.encode("utf-16-le"),
            text,
            fragment_tuple,
            "UTF-16 little-endian payload with a BOM stripped by format readers",
        ),
        EncodedUnicodeCase(
            "utf_16_be_bom",
            "utf-16-be",
            codecs.BOM_UTF16_BE + text.encode("utf-16-be"),
            text,
            fragment_tuple,
            "UTF-16 big-endian payload with a BOM stripped by format readers",
        ),
    )


def case_ids(cases: Iterable[UnicodeCase | EncodedUnicodeCase]) -> tuple[str, ...]:
    """
    Perform the case ids step with deterministic fixture inputs.

    Example:
        Exercise case ids through a consuming regression::

            python -m pytest -q tests/metadata/test_standardization_torture.py


    :param cases: Value supplied for cases under the deterministic fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    return tuple(case.case_id for case in cases)

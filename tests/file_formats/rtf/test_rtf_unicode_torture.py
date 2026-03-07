from __future__ import annotations

import importlib
import random

import pytest


UNICODE_CASES = [
    "English café naïve façade coöperate",
    "Ελληνικά με τόνους και σημεία στίξης.",
    "हिन्दी पाठ मात्रा संयुक्ताक्षर",
    "日本語テキストと全角スペース　あり",
    "العربية مع تشكيلٍ ورموزٍ",
    "Emoji 😀😃😄 + ZWJ 👩‍💻👨‍👩‍👧‍👦",
    "Combining e\u0301 a\u0308 n\u0303 o\u0302 Z\u0351",
    "Math ∑∫√∞≈≠≤≥ ←→↔↦",
    "Mixed кириллица عربى हिन्दी 漢字 한글",
]


@pytest.mark.parametrize("payload", UNICODE_CASES)
def test_txt2rtf_unicode_torture_has_ascii_safe_output(payload: str) -> None:
    mod = importlib.import_module("LiuXin_alpha.file_formats.rtf.rtfml")
    out = mod.txt2rtf(payload)
    encoded = out.encode("ascii", "strict")
    assert isinstance(encoded, bytes)
    assert len(encoded) > 0
    # Non-ASCII data should have at least one unicode escape.
    if any(ord(ch) > 127 for ch in payload):
        assert r"\u" in out


def test_txt2rtf_fuzz_deterministic_stability() -> None:
    mod = importlib.import_module("LiuXin_alpha.file_formats.rtf.rtfml")
    rng = random.Random(20260303)
    alphabet = list("abcXYZ0123 ,.;:!?-_/{}\\") + [
        "é",
        "Ω",
        "Ж",
        "你",
        "語",
        "😀",
        "👩‍💻",
        "ا",
        "ह",
        "゙",
    ]

    for _ in range(80):
        size = rng.randint(20, 240)
        payload = "".join(rng.choice(alphabet) for _ in range(size))
        a = mod.txt2rtf(payload)
        b = mod.txt2rtf(payload)
        assert a == b
        assert a.encode("ascii", "strict")

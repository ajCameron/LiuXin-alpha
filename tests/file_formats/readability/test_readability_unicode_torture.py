from __future__ import annotations

import importlib
import random

import pytest


class _Log:
    def debug(self, *parts) -> None:
        pass

    def info(self, *parts) -> None:
        pass

    def warning(self, *parts) -> None:
        pass

    def warn(self, *parts) -> None:
        pass

    def error(self, *parts) -> None:
        pass

    def exception(self, *parts) -> None:
        pass


UNICODE_CASES = [
    "English café naïve façade coöperate",
    "Ελληνικά κείμενο με τόνους και σημεία στίξης, για έλεγχο.",
    "हिन्दी पाठ, मात्रा, संयुक्ताक्षर, और विराम चिह्नों के साथ।",
    "日本語テキスト、句読点、そして全角スペース　を含む。",
    "العربية نص، مع التشكيلِ وبعضِ الرموزِ.",
    "עברית טקסט, ניקוד קל, וסימני פיסוק.",
    "Emoji stream 😀😃😄😁😆😅😂🤣🙂🙃 plus ZWJ 👩‍💻👨‍👩‍👧‍👦",
    "Combining marks: e\u0301 a\u0308 n\u0303 o\u0302 ; stacked: Z\u0351",
    "Math: ∑∫√∞≈≠≤≥ and arrows ←→↔↦",
    "Mixed scripts: Latin кириллица عربى हिन्दी 漢字 한글",
]


def _make_html(payload: str) -> bytes:
    return (
        "<html><head><title>Unicode Stress | Extraction Target</title></head>"
        "<body>"
        "<div class='sidebar'>nav, ads, subscribe now</div>"
        "<div class='article'>"
        "<h1>Extraction Target</h1>"
        "<p>{payload}, with commas, so scoring logic keeps this paragraph.</p>"
        "<p>Second paragraph repeats payload: {payload}</p>"
        "</div>"
        "<div class='footer'>footer links links</div>"
        "</body></html>"
    ).format(payload=payload).encode("utf-8", "replace")


@pytest.mark.parametrize("payload", UNICODE_CASES)
def test_unicode_torture_summary_preserves_content(payload: str) -> None:
    readability = importlib.import_module("LiuXin_alpha.file_formats.readability.readability")
    summary = readability.Document(_make_html(payload), _Log()).summary()
    assert "Extraction Target" in summary
    # Ensure the core payload signal survives extraction/sanitization.
    anchor = payload.split()[0]
    assert anchor in summary


def test_unicode_torture_short_title() -> None:
    readability = importlib.import_module("LiuXin_alpha.file_formats.readability.readability")
    raw = (
        "<html><head><title>Κατηγορία | عنوان طويل مع Unicode 😀</title></head>"
        "<body><h1>عنوان طويل مع Unicode 😀</h1></body></html>"
    ).encode("utf-8")
    doc = readability.Document(raw, _Log())
    assert doc.short_title() == "عنوان طويل مع Unicode 😀"


def test_deterministic_fuzz_readability_summary_stability() -> None:
    readability = importlib.import_module("LiuXin_alpha.file_formats.readability.readability")
    rng = random.Random(20260303)
    pool = list("abcXYZ0123 ,.;:!?-_/") + [
        "é",
        "Ω",
        "Ж",
        "你",
        "語",
        "😀",
        "👩‍💻",
        "ا",
        "ह",
        "゙",  # combining mark
    ]

    for _ in range(40):
        length = rng.randint(80, 220)
        payload = "".join(rng.choice(pool) for _ in range(length))
        raw = _make_html(payload)
        first = readability.Document(raw, _Log()).summary()
        second = readability.Document(raw, _Log()).summary()
        assert first == second
        assert isinstance(first, str)
        assert len(first) > 0


def test_broken_and_inconsistent_encodings_do_not_crash() -> None:
    readability = importlib.import_module("LiuXin_alpha.file_formats.readability.readability")
    cp1251_payload = "Тестовая строка, с запятыми, и длиной для scoring.".encode("cp1251")
    raw = (
        b"<html><head><meta charset='utf-8'><title>Broken</title></head><body><div><p>"
        + cp1251_payload
        + b"\xff\xfe\xfa"
        + b"</p></div></body></html>"
    )
    summary = readability.Document(raw, _Log()).summary()
    assert isinstance(summary, str)
    assert "Broken" in summary or "scoring" in summary

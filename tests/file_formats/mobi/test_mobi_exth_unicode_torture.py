from __future__ import annotations

import struct
from collections.abc import Mapping

from LiuXin_alpha.file_formats.mobi.reader.headers import EXTHHeader


def _values(raw):
    if raw is None:
        return []
    if isinstance(raw, Mapping):
        return list(raw.keys())
    if isinstance(raw, str):
        return [raw]
    try:
        return list(raw)
    except TypeError:
        return [raw]


def _record(code: int, payload: bytes) -> bytes:
    return struct.pack(">II", code, len(payload) + 8) + payload


def _build_exth(records: list[tuple[int, bytes]]) -> bytes:
    body = b"".join(_record(code, payload) for code, payload in records)
    return b"EXTH" + struct.pack(">II", len(body) + 12, len(records)) + body


def test_exth_header_unicode_torture_roundtrip() -> None:
    long_title = "主題 🙂 — Καλημέρα — مرحبا — नमस्ते — 漢字"
    comments = "Combining: cafe\u0301 co\u0308perate A\u030A. Emoji: 👩🏽\u200d🔬"
    records = [
        (100, "Renée Faßbinder".encode("utf-8")),
        (100, "李白".encode("utf-8")),
        (101, "Éditions Δ".encode("utf-8")),
        (103, comments.encode("utf-8")),
        (105, "タグ;Κατηγορία;Тег;العربية".encode("utf-8")),
        (503, long_title.encode("utf-8")),
        (524, b"ja"),
        (527, b"rtl"),
    ]

    exth = EXTHHeader(_build_exth(records), "utf-8", "seed title")

    assert exth.mi.title == long_title
    assert set(_values(exth.mi.authors)) == {"Renée Faßbinder", "李白"}
    assert exth.mi.publisher == "Éditions Δ"
    assert exth.mi.comments == comments
    assert set(_values(exth.mi.tags)) == {"タグ", "Κατηγορία", "Тег", "العربية"}
    assert str(exth.mi.language).lower() in {"ja", "japanese"}
    assert exth.page_progression_direction == "rtl"


def test_exth_header_malformed_utf8_does_not_crash_and_replaces_bytes() -> None:
    records = [
        (100, b"Alice\xffBob"),
        (503, b"Bad\xffTitle"),
        (103, b"Comment\xfeLine"),
    ]
    exth = EXTHHeader(_build_exth(records), "utf-8", "fallback")

    authors = _values(exth.mi.authors)
    assert authors and "\ufffd" in authors[0]
    assert "\ufffd" in exth.mi.title
    assert "\ufffd" in exth.mi.comments


def test_exth_header_cp1252_payload_decodes_accents() -> None:
    records = [
        (100, b"Jos\xe9 da Silva"),
        (101, b"Caf\xe9 Press"),
        (503, b"Caf\xe9 na\xefve"),
        (103, b"R\xe9sum\xe9"),
        (524, b"EN_us"),
    ]
    exth = EXTHHeader(_build_exth(records), "cp1252", "fallback")

    assert _values(exth.mi.authors) == ["José da Silva"]
    assert exth.mi.publisher == "Café Press"
    assert exth.mi.title == "Café naïve"
    assert exth.mi.comments == "Résumé"
    assert str(exth.mi.language).lower() in {"en", "english"}

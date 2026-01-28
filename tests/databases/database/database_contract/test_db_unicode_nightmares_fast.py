"""Database contract: targeted unicode nightmares (fast) (chunk 14).

This slice keeps runtime low while stress-testing the *Database* surface for:

* Exact round-trip preservation (write -> sync -> read) for tricky unicode.
* Exact-match searching via Database.search() (uses parameter binding).
* Distinguishing visually-similar strings (NFC vs NFD, ZWJ/ZWNJ, VS16 emoji).
* Inert handling of SQL-injection-shaped strings (must remain data).
* Bytes input coercion behavior (valid UTF-8 bytes should work; invalid bytes should raise).

These tests intentionally use a per-suite dedicated contract table with uniquely-named
columns to avoid ambiguity in driver-side table identification.
"""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha1
from typing import Iterable, Sequence

import pytest

from LiuXin_alpha.errors import InputIntegrityError


@dataclass(frozen=True)
class ContractTable:
    name: str
    id_col: str
    scratch_col: str
    text_col: str
    unique_col: str
    notes_col: str


def _exec_sql(db, stmt: str, bindings: tuple | None = None) -> None:
    """Execute SQL using a short-lived driver connection to avoid stale aliases."""
    driver = getattr(db, "driver", None)
    if driver is None or not hasattr(driver, "get_connection"):
        raise RuntimeError("Database has no driver with get_connection()")

    conn = driver.get_connection()
    try:
        cur = conn.cursor()
        if bindings is None:
            cur.execute(stmt)
        else:
            cur.execute(stmt, bindings)
        try:
            conn.commit()
        except Exception:
            try:
                conn.execute("COMMIT")
            except Exception:
                pass
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _u(i: int, s: str) -> str:
    """Deterministic small unique token derived from payload."""
    digest = sha1(s.encode("utf-8", "surrogatepass")).hexdigest()[:10]
    return f"u14_{i:04d}_{digest}"


@pytest.fixture
def contract_table(open_db) -> ContractTable:
    t = ContractTable(
        name="db_contract_l14",
        id_col="db_contract_l14_id",
        scratch_col="db_contract_l14_scratch",
        text_col="db_contract_l14_text",
        unique_col="db_contract_l14_unique",
        notes_col="db_contract_l14_notes",
    )

    _exec_sql(
        open_db,
        (
            "CREATE TABLE IF NOT EXISTS db_contract_l14 ("
            "db_contract_l14_id INTEGER PRIMARY KEY,"
            "db_contract_l14_scratch TEXT NOT NULL DEFAULT '',"
            "db_contract_l14_text TEXT,"
            "db_contract_l14_unique TEXT UNIQUE,"
            "db_contract_l14_notes TEXT"
            ");"
        ),
    )

    # Ensure metadata caches include our contract table.
    try:
        open_db.refresh_db_metadata()
    except Exception:
        pass
    return t


@pytest.fixture
def nightmares_fast(torture_strings: Sequence[str]) -> Sequence[str]:
    """Curated, stable subset + a few must-have edge cases."""
    must_have = [
        "Hello, world!",
        "mañana",                       # Latin-1 accent
        "e\u0301",                      # NFD
        "é",                            # NFC
        "Z\u0336a\u0336l\u0336g\u0336o\u0336",  # combining overstrikes
        "👩🏽‍🚒",                         # emoji + skin tone + ZWJ
        "☃",                            # snowman
        "☃️",                            # snowman + VS16
        "العربية",                       # Arabic
        "עברית",                         # Hebrew
        "中文測試",                        # CJK
        "हिन्दी",                        # Devanagari
        "ไทย",                           # Thai
        "𝔘𝔫𝔦𝔠𝔬𝔡𝔢",                       # Fraktur plane 1
        "zero\u200bwidth\u200djoiner",   # ZWSP + ZWJ
        "LRM\u200eRLM\u200fEND",         # direction marks
        "line1\nline2\r\nline3\tend",    # newlines + tab
        "O'Reilly; DROP TABLE books;--", # injection-shaped
        "NULL\0BYTE? (not included)",    # will be filtered out below
    ]

    # Use a small deterministic slice from torture_strings too (varies by suite but stable order).
    extra = list(torture_strings[:20])
    all_vals = must_have + extra

    # Filter out actual NUL bytes (sqlite bindings can reject these).
    all_vals = [s for s in all_vals if "\x00" not in s]
    # De-dupe while preserving order.
    seen = set()
    out = []
    for s in all_vals:
        if s not in seen:
            out.append(s)
            seen.add(s)
    return tuple(out)


@pytest.mark.parametrize("i", list(range(0, 18)))
def test_unicode_roundtrip_exact_text(open_db, contract_table: ContractTable, nightmares_fast: Sequence[str], i: int):
    payload = nightmares_fast[i % len(nightmares_fast)]

    row = open_db.get_blank_row(contract_table.name)
    row[contract_table.text_col] = payload
    row[contract_table.unique_col] = _u(i, payload)
    row[contract_table.notes_col] = f"n::{_u(i, payload)}"
    row.sync()

    row_id = row[contract_table.id_col]
    got = open_db.get_row_from_id(contract_table.name, row_id)

    assert got[contract_table.text_col] == payload
    assert got[contract_table.unique_col] == _u(i, payload)
    assert got[contract_table.notes_col].startswith("n::")


@pytest.mark.parametrize("i", list(range(0, 14)))
def test_unicode_search_exact_matches_row(open_db, contract_table: ContractTable, nightmares_fast: Sequence[str], i: int):
    payload = nightmares_fast[i % len(nightmares_fast)]

    row = open_db.get_blank_row(contract_table.name)
    row[contract_table.text_col] = payload
    row[contract_table.unique_col] = _u(1000 + i, payload)
    row.sync()
    row_id = row[contract_table.id_col]

    hits = open_db.search(contract_table.name, contract_table.text_col, payload)
    assert isinstance(hits, list)
    assert hits, "Expected at least one hit for an exact-match search()"
    assert any(h[contract_table.id_col] == row_id for h in hits)


def test_unicode_normalization_variants_are_distinct(open_db, contract_table: ContractTable):
    nfd = "e\u0301"
    nfc = "é"
    assert nfd != nfc  # different codepoints

    r1 = open_db.get_blank_row(contract_table.name)
    r1[contract_table.text_col] = nfd
    r1[contract_table.unique_col] = nfd
    r1.sync()

    r2 = open_db.get_blank_row(contract_table.name)
    r2[contract_table.text_col] = nfc
    r2[contract_table.unique_col] = nfc
    r2.sync()

    got1 = open_db.search(contract_table.name, contract_table.unique_col, nfd)
    got2 = open_db.search(contract_table.name, contract_table.unique_col, nfc)

    assert len(got1) == 1
    assert len(got2) == 1
    assert got1[0][contract_table.unique_col] == nfd
    assert got2[0][contract_table.unique_col] == nfc
    assert got1[0][contract_table.id_col] != got2[0][contract_table.id_col]


def test_unicode_zero_width_characters_preserved_and_searchable(open_db, contract_table: ContractTable):
    with_zw = "hello\u200bworld"
    without = "helloworld"
    assert with_zw != without

    r1 = open_db.get_blank_row(contract_table.name)
    r1[contract_table.text_col] = with_zw
    r1[contract_table.unique_col] = _u(2001, with_zw)
    r1.sync()

    r2 = open_db.get_blank_row(contract_table.name)
    r2[contract_table.text_col] = without
    r2[contract_table.unique_col] = _u(2002, without)
    r2.sync()

    h1 = open_db.search(contract_table.name, contract_table.text_col, with_zw)
    h2 = open_db.search(contract_table.name, contract_table.text_col, without)

    assert len(h1) >= 1 and any(x[contract_table.text_col] == with_zw for x in h1)
    assert len(h2) >= 1 and any(x[contract_table.text_col] == without for x in h2)


def test_unicode_emoji_variation_selector_distinct(open_db, contract_table: ContractTable):
    base = "☃"
    vs16 = "☃️"
    assert base != vs16

    a = open_db.get_blank_row(contract_table.name)
    a[contract_table.text_col] = base
    a[contract_table.unique_col] = base
    a.sync()

    b = open_db.get_blank_row(contract_table.name)
    b[contract_table.text_col] = vs16
    b[contract_table.unique_col] = vs16
    b.sync()

    got_base = open_db.search(contract_table.name, contract_table.unique_col, base)
    got_vs16 = open_db.search(contract_table.name, contract_table.unique_col, vs16)

    assert len(got_base) == 1
    assert len(got_vs16) == 1
    assert got_base[0][contract_table.id_col] != got_vs16[0][contract_table.id_col]


def test_unicode_rtl_marks_roundtrip(open_db, contract_table: ContractTable):
    payload = "العربية\u200f / עברית\u200e END"
    r = open_db.get_blank_row(contract_table.name)
    r[contract_table.text_col] = payload
    r[contract_table.unique_col] = _u(3001, payload)
    r.sync()

    rid = r[contract_table.id_col]
    got = open_db.get_row_from_id(contract_table.name, rid)
    assert got[contract_table.text_col] == payload


def test_unicode_multiline_and_tabs_preserved(open_db, contract_table: ContractTable):
    payload = "line1\nline2\r\nline3\tend"
    r = open_db.get_blank_row(contract_table.name)
    r[contract_table.text_col] = payload
    r[contract_table.unique_col] = _u(3002, payload)
    r.sync()

    rid = r[contract_table.id_col]
    got = open_db.get_row_from_id(contract_table.name, rid)
    assert got[contract_table.text_col] == payload


def test_unicode_long_string_roundtrip(open_db, contract_table: ContractTable):
    # Keep this large but not ridiculous for fast suites.
    core = "中文👩🏽‍🚒ée\u0301—العربية—עברית—"
    payload = core * 400  # ~ (len(core)*400) chars
    r = open_db.get_blank_row(contract_table.name)
    r[contract_table.text_col] = payload
    r[contract_table.unique_col] = _u(4001, payload)
    r.sync()

    rid = r[contract_table.id_col]
    got = open_db.get_row_from_id(contract_table.name, rid)
    assert got[contract_table.text_col] == payload
    assert len(got[contract_table.text_col]) == len(payload)


def test_injection_shaped_payload_is_inert_data(open_db, contract_table: ContractTable):
    payload = "'); DROP TABLE db_contract_l14;--"
    r = open_db.get_blank_row(contract_table.name)
    r[contract_table.text_col] = payload
    r[contract_table.unique_col] = _u(5001, payload)
    r.sync()

    # Table should still exist and searching should work.
    tables = set(open_db.get_tables())
    assert contract_table.name in tables
    hits = open_db.search(contract_table.name, contract_table.text_col, payload)
    assert hits and any(h[contract_table.text_col] == payload for h in hits)


def test_search_accepts_valid_utf8_bytes(open_db, contract_table: ContractTable):
    s = "café"
    r = open_db.get_blank_row(contract_table.name)
    r[contract_table.text_col] = s
    r[contract_table.unique_col] = _u(6001, s)
    r.sync()

    hits = open_db.search(contract_table.name, contract_table.text_col, s.encode("utf-8"))
    assert hits and any(h[contract_table.text_col] == s for h in hits)


def test_search_rejects_invalid_utf8_bytes(open_db, contract_table: ContractTable):
    s = "ok"
    r = open_db.get_blank_row(contract_table.name)
    r[contract_table.text_col] = s
    r[contract_table.unique_col] = _u(6002, s)
    r.sync()

    with pytest.raises(InputIntegrityError):
        open_db.search(contract_table.name, contract_table.text_col, b"\xff\xfe\xff")


def test_get_values_set_contains_tricky_members(open_db, contract_table: ContractTable):
    vals = ["é", "e\u0301", "hello\u200bworld", "☃️", "👩🏽‍🚒"]
    for i, v in enumerate(vals):
        r = open_db.get_blank_row(contract_table.name)
        r[contract_table.text_col] = v
        # put values in the unique column so get_values_set can find them unambiguously
        r[contract_table.unique_col] = v
        r.sync()

    got = open_db.get_values_set(contract_table.unique_col)
    assert isinstance(got, set)
    for v in vals:
        assert v in got


def test_casefold_is_not_applied_for_exact_search(open_db, contract_table: ContractTable):
    # German ß: STRASSE is not equal to straße under exact equality.
    v = "straße"
    r = open_db.get_blank_row(contract_table.name)
    r[contract_table.text_col] = v
    r[contract_table.unique_col] = _u(7001, v)
    r.sync()

    hits = open_db.search(contract_table.name, contract_table.text_col, "STRASSE")
    assert hits == []


def test_row_repr_does_not_crash_on_unicode(open_db, contract_table: ContractTable):
    payload = "👩🏽‍🚒 — العربية — 中文 — e\u0301"
    r = open_db.get_blank_row(contract_table.name)
    r[contract_table.text_col] = payload
    r[contract_table.unique_col] = _u(8001, payload)
    r.sync()
    # Just ensure __repr__ doesn't blow up
    s = repr(r)
    assert isinstance(s, str)
    assert "Row" in s or s

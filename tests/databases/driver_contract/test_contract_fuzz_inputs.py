"""Driver contract: fuzzed unicode/whitespace/control-char handling.

This module is intentionally "long": it generates a deterministic corpus of
"random-ish" unicode strings and repeatedly round-trips them through a handful
of core driver methods.

Why?
- Unicode handling tends to fail in subtle ways (normalization, trimming,
  lossy conversions, control characters, bidi marks).
- Many drivers accidentally coerce/strip/normalize values during insert/read.

Contract expectations (strict):
- Values inserted into TEXT columns must be retrieved *exactly* (byte-for-byte
  at the Python str level).
- The driver must not apply unicode normalization implicitly.
- Odd whitespace and control characters must remain intact.

We keep this deterministic so failures are reproducible across platforms and
future drivers.
"""

from __future__ import annotations

import os
import random
import unicodedata
from dataclasses import dataclass
from typing import Iterable, List, Sequence, Tuple

import pytest


_CONTRACT_TABLE = "contract_fuzz_inputs"

# Tunables (keep deterministic defaults, but allow making it heavier/lighter).
_DEFAULT_CASES = int(os.environ.get("LIUXIN_CONTRACT_FUZZ_CASES", "320"))
_DEFAULT_MAXLEN = int(os.environ.get("LIUXIN_CONTRACT_FUZZ_MAXLEN", "12000"))
_FUZZ_SEED = int(os.environ.get("LIUXIN_CONTRACT_FUZZ_SEED", str(0xC0FFEE)))


@dataclass(frozen=True)
class FuzzRow:
    raw: str
    nfc: str
    tag: str
    kind: str


def _interesting_atoms() -> List[str]:
    # A curated set of atoms that, when combined, produce lots of edge cases.
    whitespace = [
        " ",
        "\t",
        "\n",
        "\r\n",
        "\v",
        "\f",
        "\u00A0",  # NBSP
        "\u1680",  # ogham space mark
        "\u2000",  # en quad
        "\u2001",  # em quad
        "\u2002",  # en space
        "\u2003",  # em space
        "\u2007",  # figure space
        "\u202F",  # narrow no-break
        "\u205F",  # medium mathematical space
        "\u3000",  # ideographic space
        "\uFEFF",  # BOM / ZWNBSP
        "\u200B",  # zero-width space
        "\u200C",  # ZWNJ
        "\u200D",  # ZWJ
        "\u2060",  # word joiner
        "\u2028",  # line separator
        "\u2029",  # paragraph separator
    ]

    bidi = [
        "\u200E",  # LRM
        "\u200F",  # RLM
        "\u202A",  # LRE
        "\u202B",  # RLE
        "\u202D",  # LRO
        "\u202E",  # RLO
        "\u2066",  # LRI
        "\u2067",  # RLI
        "\u2068",  # FSI
        "\u2069",  # PDI
    ]

    combining = [
        "\u0300", "\u0301", "\u0302", "\u0303", "\u0308", "\u0327", "\u0336", "\u034F"
    ]

    # C0 control chars (excluding \x00 handled separately in explicit edge cases).
    controls = ["".join(chr(i) for i in range(1, 32)), chr(0x7F), chr(0x85), chr(0x9F)]

    scripts = [
        "ASCII", "latin", "Grüße", "São", "naïve", "coöperate",
        "Ελληνικά",  # Greek
        "кириллица",  # Cyrillic
        "עברית",  # Hebrew
        "العربية",  # Arabic
        "हिन्दी",  # Devanagari
        "ไทย",  # Thai
        "汉字",  # CJK
        "かな",  # Hiragana
        "カナ",  # Katakana
        "한국어",  # Hangul
    ]

    emoji = [
        "😀", "🤖", "🧠", "🧪", "✨", "🪄",
        "❤️", "❤️\uFE0F",  # variation selector
        "👩\u200D💻",  # ZWJ sequence
        "👨\u200D👩\u200D👧\u200D👦",  # family ZWJ
        "🏳️\u200D🌈",  # rainbow flag
        "👍", "👍🏽", "👍🏿",  # skin tones
    ]

    punctuation = [
        "'", '"', "`", ";", ":", "\\", "/", "..", "...", "—", "–", "•",
        "<tag>", "</tag>", "{json}", "[brackets]", "(paren)", "=", "==",
    ]

    injection_shaped = [
        "' OR '1'='1",
        "'); DROP TABLE titles; --",
        "-- not a comment when bound",
        "/* also not a comment */",
        "'||(SELECT name FROM sqlite_master)||'",
    ]

    # Keep ordering deterministic.
    atoms: List[str] = []
    atoms.extend(whitespace)
    atoms.extend(bidi)
    atoms.extend(scripts)
    atoms.extend(emoji)
    atoms.extend(punctuation)
    atoms.extend(injection_shaped)
    atoms.extend(combining)
    atoms.extend(controls)

    return atoms


def _explicit_edge_cases() -> List[str]:
    # Strings we always want, regardless of RNG.
    return [
        "",  # empty
        " ",
        "\t",
        "\n",
        "\r\n",
        "\u00A0",  # NBSP alone
        "nul\x00byte\x00inside",  # embedded NULs
        "leading space ",
        " trailing space",
        "  both  ",
        "e\u0301",  # combining
        "\u00e9",   # precomposed
        "zero-width:\u200b\u200d\u200c",
        "bidi:\u202EABC\u202C",  # RLO ... PDF
        "line\u2028sep\u2029para",
        "Zalgo:" + "a" + "\u0300\u0301\u0302\u0303\u0308" * 8,
        "CJK: 漢字かなカナ",
        "RTL: עברית العربية",
        "emoji: 👨\u200D👩\u200D👧\u200D👦",
    ]


def _make_fuzz_rows(n: int, *, seed: int, maxlen: int) -> List[FuzzRow]:
    rng = random.Random(seed)
    atoms = _interesting_atoms()

    rows: List[FuzzRow] = []

    # Start with explicit edge cases to ensure stable coverage.
    for i, s in enumerate(_explicit_edge_cases()):
        rows.append(
            FuzzRow(
                raw=s,
                nfc=unicodedata.normalize("NFC", s),
                tag=f"edge-{i % 11}",
                kind="edge",
            )
        )

    # Then generate random-ish combinations.
    while len(rows) < n:
        parts: List[str] = []
        k = rng.randint(1, 10)
        for _ in range(k):
            parts.append(rng.choice(atoms))

        s = "".join(parts)

        # Occasionally wrap in weird whitespace.
        if rng.random() < 0.70:
            w1 = rng.choice(atoms[:25])  # mostly whitespace/bidi
            w2 = rng.choice(atoms[:25])
            s = f"{w1}{s}{w2}"

        # Occasionally add heavy combining sequences.
        if rng.random() < 0.18:
            base = rng.choice(["a", "e", "i", "o", "u", "n", "漢", "ق", "א"])
            marks = "".join(rng.choice(["\u0300", "\u0301", "\u0303", "\u0327", "\u0336"]) for _ in range(rng.randint(5, 40)))
            s = s + base + marks

        # Occasionally repeat chunks to create long strings.
        if rng.random() < 0.12:
            s = s * rng.randint(2, 14)

        # Bound length to keep runtime reasonable.
        if len(s) > maxlen:
            s = s[:maxlen]

        rows.append(
            FuzzRow(
                raw=s,
                nfc=unicodedata.normalize("NFC", s),
                tag=f"tag-{len(rows) % 31}",
                kind=f"k{len(rows) % 7}",
            )
        )

    return rows


@pytest.fixture(scope="session")
def fuzz_rows() -> Sequence[FuzzRow]:
    # Ensure determinism even if global random is used elsewhere.
    return tuple(_make_fuzz_rows(_DEFAULT_CASES, seed=_FUZZ_SEED, maxlen=_DEFAULT_MAXLEN))


@pytest.fixture
def fuzz_table(driver) -> str:
    table = _CONTRACT_TABLE
    sql = f"""
    DROP TABLE IF EXISTS `{table}`;
    CREATE TABLE `{table}` (
        `{table}_id` INTEGER PRIMARY KEY AUTOINCREMENT,
        `{table}_raw` TEXT NOT NULL,
        `{table}_nfc` TEXT NOT NULL,
        `{table}_tag` TEXT NOT NULL,
        `{table}_kind` TEXT NOT NULL,
        `{table}_datestamp` TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    driver.direct_executescript(sql)
    assert table in set(driver.direct_get_tables(force_refresh=True))
    return table


@pytest.fixture
def fuzz_cols(fuzz_table: str) -> dict:
    t = fuzz_table
    return {
        "id": f"{t}_id",
        "raw": f"{t}_raw",
        "nfc": f"{t}_nfc",
        "tag": f"{t}_tag",
        "kind": f"{t}_kind",
        "datestamp": f"{t}_datestamp",
    }


def _insert_row(driver, cols: dict, row: FuzzRow) -> int:
    driver.direct_add_simple_row_dict(
        {
            cols["raw"]: row.raw,
            cols["nfc"]: row.nfc,
            cols["tag"]: row.tag,
            cols["kind"]: row.kind,
        }
    )
    row_id = driver.direct_get_highest_id(_CONTRACT_TABLE)
    assert row_id is not None
    return int(row_id)


def test_fuzz_insert_fetch_exact_roundtrip(driver, fuzz_table: str, fuzz_cols: dict, fuzz_rows: Sequence[FuzzRow], assert_integrity):
    """Insert many fuzz rows; each must read back exactly."""

    # Insert and immediately round-trip (catches encoding issues early).
    for row in fuzz_rows:
        row_id = _insert_row(driver, fuzz_cols, row)
        got = driver.direct_get_row_dict_from_id(fuzz_table, row_id)
        assert got is not False
        assert got[fuzz_cols["raw"]] == row.raw
        assert got[fuzz_cols["nfc"]] == row.nfc
        assert got[fuzz_cols["tag"]] == row.tag
        assert got[fuzz_cols["kind"]] == row.kind

    # Sanity: datestamp should exist and never overwrite raw text.
    any_row_id = driver.direct_get_highest_id(fuzz_table)
    got = driver.direct_get_row_dict_from_id(fuzz_table, any_row_id)
    assert got is not False
    assert got.get(fuzz_cols["datestamp"]) is not None

    assert_integrity(driver)


def test_fuzz_no_implicit_unicode_normalization(driver, fuzz_table: str, fuzz_cols: dict):
    """The driver must not normalize unicode strings implicitly."""

    decomposed = "e\u0301"  # e + combining acute
    composed = "\u00e9"     # é

    assert decomposed != composed
    assert unicodedata.normalize("NFC", decomposed) == composed

    id1 = _insert_row(driver, fuzz_cols, FuzzRow(raw=decomposed, nfc=unicodedata.normalize("NFC", decomposed), tag="norm", kind="decomp"))
    id2 = _insert_row(driver, fuzz_cols, FuzzRow(raw=composed, nfc=unicodedata.normalize("NFC", composed), tag="norm", kind="comp"))

    r1 = driver.direct_get_row_dict_from_id(fuzz_table, id1)
    r2 = driver.direct_get_row_dict_from_id(fuzz_table, id2)

    assert r1[fuzz_cols["raw"]] == decomposed
    assert r2[fuzz_cols["raw"]] == composed

    # They must remain distinct values.
    assert r1[fuzz_cols["raw"]] != r2[fuzz_cols["raw"]]


def test_fuzz_whitespace_and_controls_survive(driver, fuzz_table: str, fuzz_cols: dict):
    """Leading/trailing whitespace, bidi marks, and NUL/control chars must persist."""

    payloads = [
        "  both  ",
        "\tTabbed\t",
        "\nNewline\n",
        "NBSP\u00A0END",
        "ZWS\u200bMID\u200dEND",
        "bidi:\u202EABC\u202C",
        "nul\x00byte\x00inside",
        "c0:" + "".join(chr(i) for i in range(1, 8)),
    ]

    ids: List[int] = []
    for i, p in enumerate(payloads):
        rid = _insert_row(driver, fuzz_cols, FuzzRow(raw=p, nfc=unicodedata.normalize("NFC", p), tag=f"ws-{i}", kind="ws"))
        ids.append(rid)

    for rid, p in zip(ids, payloads):
        got = driver.direct_get_row_dict_from_id(fuzz_table, rid)
        assert got is not False
        assert got[fuzz_cols["raw"]] == p
        assert len(got[fuzz_cols["raw"]]) == len(p)


def test_fuzz_exact_match_search_is_safe_and_correct(driver, fuzz_table: str, fuzz_cols: dict, fuzz_rows: Sequence[FuzzRow]):
    """direct_search_table should round-trip exact matches even for weird strings."""

    # Insert a smaller subset to keep this test faster.
    subset = list(fuzz_rows[:80]) + list(fuzz_rows[-25:])

    id_by_raw: dict[str, int] = {}
    for row in subset:
        rid = _insert_row(driver, fuzz_cols, row)
        # keep the first id for that raw value
        id_by_raw.setdefault(row.raw, rid)

    # Pick a deterministic sample of keys.
    keys = sorted(id_by_raw.keys(), key=lambda s: (len(s), s))
    sample = keys[:: max(1, len(keys) // 35)]  # ~35 samples

    for raw in sample:
        results = driver.direct_search_table(fuzz_table, fuzz_cols["raw"], raw)
        assert isinstance(results, list)
        assert any(r.get(fuzz_cols["raw"]) == raw for r in results), f"search did not return exact match for {raw!r}"


def test_fuzz_update_roundtrip(driver, fuzz_table: str, fuzz_cols: dict, fuzz_rows: Sequence[FuzzRow], assert_integrity):
    """Updating fuzz rows must preserve exact values."""

    # Seed a handful of rows.
    base_rows = list(fuzz_rows[10:50])
    ids: List[int] = []
    for r in base_rows:
        ids.append(_insert_row(driver, fuzz_cols, r))

    id_col = driver.direct_get_id_column(fuzz_table)

    # Update every third row with a later fuzz payload.
    updates = list(fuzz_rows[200:260])
    for idx, row_id in enumerate(ids[::3]):
        new = updates[idx]
        driver.direct_update_row_dict(
            {
                id_col: row_id,
                fuzz_cols["raw"]: new.raw,
                fuzz_cols["nfc"]: new.nfc,
                fuzz_cols["tag"]: "updated",
                fuzz_cols["kind"]: "updated",
            }
        )

        got = driver.direct_get_row_dict_from_id(fuzz_table, row_id)
        assert got is not False
        assert got[fuzz_cols["raw"]] == new.raw
        assert got[fuzz_cols["nfc"]] == new.nfc
        assert got[fuzz_cols["tag"]] == "updated"
        assert got[fuzz_cols["kind"]] == "updated"

    assert_integrity(driver)


def test_fuzz_unique_values_set_and_iterator_agree(driver, fuzz_table: str, fuzz_cols: dict, fuzz_rows: Sequence[FuzzRow]):
    """Unique-value helpers must behave consistently under unicode pressure."""

    # Insert rows with a controlled (but unicode-rich) set of tags.
    tags = [
        "tag:ascii",
        "tag:汉字",
        "tag:العربية",
        "tag:עברית",
        "tag:emoji😀",
        "tag:space\u00A0nb",
        "tag:bidi\u202E",
    ]

    for i in range(140):
        row = fuzz_rows[i]
        tag = tags[i % len(tags)]
        _insert_row(driver, fuzz_cols, FuzzRow(raw=row.raw, nfc=row.nfc, tag=tag, kind="u"))

    col = fuzz_cols["tag"]
    got_set = driver.direct_get_unique_values_set(col)
    got_iter = set(driver.direct_get_unique_values_iterator(col))

    assert set(tags).issubset(set(got_set))
    assert got_iter == set(got_set)


def test_fuzz_random_row_does_not_crash(driver, fuzz_table: str, fuzz_cols: dict, fuzz_rows: Sequence[FuzzRow]):
    """Random-row retrieval should always return a sane dict with fuzzed content."""

    # Seed enough rows for randomness to be meaningful.
    for i in range(60):
        _insert_row(driver, fuzz_cols, fuzz_rows[i])

    for _ in range(25):
        row = driver.direct_get_random_row_dict(fuzz_table)
        assert isinstance(row, dict)
        assert fuzz_cols["raw"] in row
        assert isinstance(row[fuzz_cols["raw"]], str)


def test_fuzz_delete_rows_and_recheck_integrity(driver, fuzz_table: str, fuzz_cols: dict, fuzz_rows: Sequence[FuzzRow], assert_integrity):
    """Deletes should work even when ids were created by heavy unicode inserts."""

    ids: List[int] = []
    for i in range(90):
        ids.append(_insert_row(driver, fuzz_cols, fuzz_rows[i]))

    # Delete every 4th row.
    victims = ids[::4]
    for vid in victims:
        driver.direct_delete_row_by_id(fuzz_table, vid)

    for vid in victims:
        assert driver.direct_get_row_dict_from_id(fuzz_table, vid) is False

    # Survivors should remain.
    survivors = [i for i in ids if i not in set(victims)]
    for sid in survivors[:15]:
        assert driver.direct_get_row_dict_from_id(fuzz_table, sid) is not False

    assert_integrity(driver)

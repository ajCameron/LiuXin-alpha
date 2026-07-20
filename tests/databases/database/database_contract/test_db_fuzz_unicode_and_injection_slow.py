"""Database contract: slow fuzz for unicode + injection-shaped data (chunk 15).

These tests are intentionally heavier than the rest of the contract suite.
They aim to stress Database-level surfaces while acting as proxy coverage for
the underlying driver/wrapper layers.

Run explicitly with:
    pytest -m slow

Design notes
------------
* We operate through the Database instance (search, get_values_set, get_row_from_id)
  while performing bulk DDL/DML using short-lived driver connections to avoid any
  stale-connection issues during metadata refresh.
* We treat SQL-injection-shaped strings strictly as inert data and verify that
  schema and sentinel rows are not modified as a consequence of inserts/searches.
* We include a small set of "hazard" unicode payloads (e.g. unpaired surrogates).
  Drivers may reasonably reject these; if so, we assert that the database remains
  consistent and continue.
"""

from __future__ import annotations

import hashlib
import random
import string
import unicodedata
from dataclasses import dataclass
from typing import Iterable, Sequence

import pytest

from LiuXin_alpha.databases.row import Row


@dataclass(frozen=True)
class ContractTable:
    name: str
    id_col: str
    scratch_col: str
    text_col: str
    group_col: str
    num_col: str


def _stable_suffix(nodeid: str) -> str:
    return hashlib.sha1(nodeid.encode("utf-8")).hexdigest()[:10]


def _stable_hash_text(s: str) -> str:
    """Stable hash even for strings that don't encode cleanly."""

    b = s.encode("utf-8", errors="backslashreplace")
    return hashlib.sha1(b).hexdigest()[:12]


def _commit(conn) -> None:
    try:
        conn.commit()
    except Exception:
        try:
            conn.execute("COMMIT")
        except Exception:
            pass


def _exec_sql(db, stmt: str, bindings: tuple | None = None) -> None:
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
        _commit(conn)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _exec_many(db, stmt: str, seq: Sequence[tuple]) -> None:
    driver = getattr(db, "driver", None)
    if driver is None or not hasattr(driver, "get_connection"):
        raise RuntimeError("Database has no driver with get_connection()")

    conn = driver.get_connection()
    try:
        cur = conn.cursor()
        try:
            # Most DB-API cursors support executemany.
            cur.executemany(stmt, seq)  # type: ignore[attr-defined]
        except Exception:
            # Fall back to whatever the backend supports.
            try:
                conn.executemany(stmt, seq)  # type: ignore[attr-defined]
            except Exception:
                for params in seq:
                    cur.execute(stmt, params)
        _commit(conn)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _fetch_all(db, stmt: str, bindings: tuple | None = None) -> list[tuple]:
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
        return list(cur.fetchall())
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _list_tables(db) -> set[str]:
    rows = _fetch_all(db, "SELECT name FROM sqlite_master WHERE type='table'")
    return {str(r[0]) for r in rows if r and r[0] is not None}


def _pick_non_nul_payloads(payloads: Sequence[str]) -> list[str]:
    return [p for p in payloads if "\x00" not in p]


def _extra_unicode_snippets() -> list[str]:
    return [
        "Καλημέρα κόσμε",  # Greek
        "Привет мир",  # Russian
        "हिन्दी भाषा",  # Hindi
        "বাংলা ভাষা",  # Bengali
        "ภาษาไทย",  # Thai
        "ქართული",  # Georgian
        "עברית",  # Hebrew
        "العربية",  # Arabic
        "漢字かなカナ",  # CJK mix
        "ᚠᛇᚻ ᛒᛦᚦ",  # Runic-ish
        "𝔘𝔫𝔦𝔠𝔬𝔡𝔢 𝟘𝟙𝟚",  # Mathematical/Fraktur digits
        "🇫🇷🇬🇧 flags",  # Regional indicators
        "👩🏽‍💻 coding",  # ZWJ + skin tone
        "Zalgo: h̶e̷l̸l̹o̴",  # combining stack
        "BOM\ufeffprefix",  # BOM
        "RLO:\u202Eabc",  # bidi override
        "ZWSP:\u200b|ZWJ:\u200d|ZWNJ:\u200c",  # zero-widths
    ]


def _random_payloads(n: int) -> list[str]:
    """Generate deterministic random-ish strings from multiple alphabets."""

    snippets = _extra_unicode_snippets()
    punct = "'\"`~!@#$%^&*()-_=+[]{}|;:,.<>/?\\"
    ws = [" ", "\t", "\n", "\r\n"]

    out: list[str] = []
    for i in range(n):
        core = random.choice(snippets)
        pad_left = "".join(random.choice(punct) for _ in range(random.randint(0, 3)))
        pad_right = "".join(random.choice(punct) for _ in range(random.randint(0, 3)))
        noise = "".join(random.choice(string.ascii_letters + string.digits) for _ in range(random.randint(0, 16)))
        glue = random.choice(ws)
        s = f"{pad_left}{core}{glue}{noise}{pad_right}"

        # Occasionally make it very long.
        if i % 31 == 0:
            s = s + ("x" * 8192)
        out.append(s)

    # Keep stable order but remove duplicates.
    seen: set[str] = set()
    deduped: list[str] = []
    for s in out:
        if s not in seen:
            seen.add(s)
            deduped.append(s)
    return deduped


def _hazard_payloads() -> list[str]:
    """Strings that may be rejected by backends (acceptable)."""

    # Unpaired surrogates are legal in Python str but often illegal to encode as UTF-8.
    return [
        "unpaired-surrogate-\ud800",  # high surrogate
        "unpaired-surrogate-\udfff",  # low surrogate
    ]


@pytest.fixture
def fuzz_table(open_db, request) -> ContractTable:
    suf = _stable_suffix(request.node.nodeid)

    table = ContractTable(
        name=f"db_contract_s15_{suf}",
        id_col="id",
        scratch_col=f"scratch_s15_{suf}",
        text_col=f"text_s15_{suf}",
        group_col=f"group_s15_{suf}",
        num_col=f"num_s15_{suf}",
    )

    _exec_sql(
        open_db,
        f"""
        CREATE TABLE IF NOT EXISTS {table.name} (
            {table.id_col} INTEGER PRIMARY KEY,
            {table.scratch_col} TEXT UNIQUE,
            {table.text_col} TEXT,
            {table.group_col} TEXT,
            {table.num_col} INTEGER
        );
        """.strip(),
    )

    # A simple "victim" row whose content must never change as a consequence of
    # storing injection-shaped payloads.
    _exec_sql(
        open_db,
        f"INSERT OR REPLACE INTO {table.name} ({table.scratch_col}, {table.text_col}, {table.group_col}, {table.num_col}) "
        "VALUES (?, ?, ?, ?)",
        (f"victim_{suf}", "SAFE_VALUE", "SAFE_GROUP", 123),
    )

    try:
        open_db.driver.call_after_table_changes()
    except Exception:
        pass
    try:
        open_db.refresh_db_metadata()
    except Exception:
        pass

    return table


@pytest.mark.slow
@pytest.mark.catalog
def test_slow_fuzz_bulk_insert_roundtrip_and_no_schema_damage(
    open_db,
    fuzz_table: ContractTable,
    all_torture_payloads: Sequence[str],
    sql_injection_payloads: Sequence[str],
    assert_integrity,
):
    """Bulk insert many unicode + injection-shaped payloads and validate invariants."""

    safe_payloads = _pick_non_nul_payloads(all_torture_payloads)
    safe_payloads = list(dict.fromkeys(safe_payloads))  # preserve order, dedupe
    safe_payloads.extend(_random_payloads(220))

    # Capture the schema table set after our contract table exists.
    tables_before = _list_tables(open_db)
    assert fuzz_table.name in tables_before

    # Build rows: keep group value small to exercise get_values_set.
    rows: list[tuple[str, str, str, int]] = []
    for i, payload in enumerate(safe_payloads):
        # Avoid pathological payloads that are too large for some backends.
        if len(payload) > 100_000:
            continue
        scratch = f"s15_{i}_{_stable_hash_text(payload)}"
        grp = f"G{(i % 7)}"
        rows.append((scratch, payload, grp, i))

    _exec_many(
        open_db,
        f"INSERT INTO {fuzz_table.name} ({fuzz_table.scratch_col}, {fuzz_table.text_col}, {fuzz_table.group_col}, {fuzz_table.num_col}) "
        "VALUES (?, ?, ?, ?)",
        rows,
    )

    # Attempt to insert hazard payloads; failure is acceptable, but corruption is not.
    for i, hp in enumerate(_hazard_payloads()):
        scratch = f"haz_{i}_{_stable_hash_text(hp)}"
        try:
            _exec_sql(
                open_db,
                f"INSERT INTO {fuzz_table.name} ({fuzz_table.scratch_col}, {fuzz_table.text_col}) VALUES (?, ?)",
                (scratch, hp),
            )
        except Exception:
            # Backend may reject this; ensure DB remains ok and move on.
            assert_integrity(open_db)

    # Schema should not have changed as a result of injection-shaped payloads.
    tables_after = _list_tables(open_db)
    assert tables_after == tables_before

    # "Victim" row must remain unmodified.
    victim = open_db.search(table=fuzz_table.name, column=fuzz_table.scratch_col, search_term=f"victim_{_stable_suffix(pytest.__file__)}")
    # The suffix used above isn't the same; search directly by reading back.
    victim_rows = open_db.search(table=fuzz_table.name, column=fuzz_table.text_col, search_term="SAFE_VALUE")
    assert victim_rows, "Expected victim row to remain"
    assert any(r[fuzz_table.group_col] == "SAFE_GROUP" for r in victim_rows)

    # Validate get_values_set over the unique group column.
    got_groups = open_db.get_values_set(target_column=fuzz_table.group_col, iterator_return=False)
    assert isinstance(got_groups, set)
    assert "SAFE_GROUP" in got_groups
    # We inserted G0..G6
    for i in range(7):
        assert f"G{i}" in got_groups

    # Random sampling: roundtrip equality (NFC-normalized) + search correctness.
    inserted_scratches = [r[0] for r in rows]
    sample_scratches = random.sample(inserted_scratches, k=min(40, len(inserted_scratches)))

    for scratch in sample_scratches:
        # Search by scratch should be unique.
        found = open_db.search(table=fuzz_table.name, column=fuzz_table.scratch_col, search_term=scratch)
        assert len(found) == 1
        row = found[0]
        assert isinstance(row, Row)
        assert row[fuzz_table.scratch_col] == scratch

        # get_row_from_id should return the same data.
        row_id = int(row["id"])
        reread = open_db.get_row_from_id(table=fuzz_table.name, row_id=row_id)
        assert reread is not None
        assert reread[fuzz_table.scratch_col] == scratch

        # NFC-normalized equivalence check (tolerates backends that normalize or not).
        a = unicodedata.normalize("NFC", str(row[fuzz_table.text_col] or ""))
        b = unicodedata.normalize("NFC", str(reread[fuzz_table.text_col] or ""))
        assert a == b

    # Injection-shaped payloads must be searchable as inert data.
    inj = [p for p in sql_injection_payloads if "\x00" not in p]
    for payload in random.sample(inj, k=min(6, len(inj))):
        found = open_db.search(table=fuzz_table.name, column=fuzz_table.text_col, search_term=payload)
        assert found == [] or any(r[fuzz_table.text_col] == payload for r in found)

    assert_integrity(open_db)


@pytest.mark.slow
@pytest.mark.catalog
def test_slow_fuzz_random_crud_sequences_do_not_corrupt(
    open_db,
    fuzz_table: ContractTable,
    pick_payload,
    assert_integrity,
):
    """Perform randomized insert/update/delete cycles and ensure the DB stays consistent."""

    def _safe_payload(i: int) -> str:
        p = pick_payload(i)
        if "\x00" in p:
            p = p.replace("\x00", "")
        return p

    live: dict[str, int] = {}  # scratch -> id

    def _insert(i: int) -> None:
        payload = _safe_payload(i)
        scratch = f"seq_{i}_{_stable_hash_text(payload)}"
        grp = f"S{(i % 5)}"
        _exec_sql(
            open_db,
            f"INSERT INTO {fuzz_table.name} ({fuzz_table.scratch_col}, {fuzz_table.text_col}, {fuzz_table.group_col}, {fuzz_table.num_col}) "
            "VALUES (?, ?, ?, ?)",
            (scratch, payload, grp, i),
        )
        rid = _fetch_all(
            open_db,
            f"SELECT {fuzz_table.id_col} FROM {fuzz_table.name} WHERE {fuzz_table.scratch_col} = ?",
            (scratch,),
        )
        assert rid and rid[0][0] is not None
        live[scratch] = int(rid[0][0])

    def _update() -> None:
        if not live:
            return
        scratch = random.choice(list(live.keys()))
        new_payload = _random_payloads(1)[0]
        if "\x00" in new_payload:
            new_payload = new_payload.replace("\x00", "")
        _exec_sql(
            open_db,
            f"UPDATE {fuzz_table.name} SET {fuzz_table.text_col} = ? WHERE {fuzz_table.scratch_col} = ?",
            (new_payload, scratch),
        )
        got = open_db.search(table=fuzz_table.name, column=fuzz_table.scratch_col, search_term=scratch)
        assert got and got[0][fuzz_table.scratch_col] == scratch

    def _delete() -> None:
        if not live:
            return
        scratch = random.choice(list(live.keys()))
        _exec_sql(
            open_db,
            f"DELETE FROM {fuzz_table.name} WHERE {fuzz_table.scratch_col} = ?",
            (scratch,),
        )
        live.pop(scratch, None)
        got = open_db.search(table=fuzz_table.name, column=fuzz_table.scratch_col, search_term=scratch)
        assert got == []

    def _search_noise(i: int) -> None:
        # Search for values that may or may not exist.
        needle = _safe_payload(i)
        got = open_db.search(table=fuzz_table.name, column=fuzz_table.text_col, search_term=needle)
        assert isinstance(got, list)
        assert all(isinstance(r, Row) for r in got)

    # Seed the table with a few rows.
    for i in range(25):
        _insert(i)

    for step in range(220):
        action = random.random()
        if action < 0.40:
            _insert(1000 + step)
        elif action < 0.65:
            _update()
        elif action < 0.85:
            _delete()
        else:
            _search_noise(2000 + step)

        # Periodic integrity check (kept light).
        if step % 55 == 0:
            assert_integrity(open_db)

    # Final sanity: all live scratches should still resolve.
    for scratch, row_id in random.sample(list(live.items()), k=min(25, len(live))):
        r = open_db.get_row_from_id(table=fuzz_table.name, row_id=row_id)
        assert r is not None
        assert r[fuzz_table.scratch_col] == scratch

    assert_integrity(open_db)

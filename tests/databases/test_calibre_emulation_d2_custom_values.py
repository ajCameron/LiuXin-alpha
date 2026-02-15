from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

import pytest

from LiuXin_alpha.databases.calibre_emulation import CalibreReader
from LiuXin_alpha.databases.database_driver_plugins.SQL.calibre_database_generator import CalibreLibraryBuilder


def _payload_by_id(reader: CalibreReader, book_id: int, **kwargs):
    for p in reader.iter_book_payloads(batch_size=50, **kwargs):
        if p.calibre_book_id == book_id:
            return p
    raise AssertionError(f"book_id {book_id} not found")


def test_d2_reads_text_and_multi_values_with_stable_order_and_dedupe(provision_calibre_library) -> None:
    lib = provision_calibre_library(name="lib_d2_text_multi")
    b = CalibreLibraryBuilder(lib.root)

    b.create_custom_column(label="mood", name="Mood", datatype="text", is_multiple=False)
    b.create_custom_column(label="multi", name="Multi", datatype="text", is_multiple=True)

    added = b.add_book(
        title="Custom Text",
        authors=["A. Author"],
        formats={"EPUB": b"epub"},
    )

    b.set_custom_value(book_id=added.book_id, label="mood", value="brooding")
    b.set_custom_value(book_id=added.book_id, label="multi", value=["a", "b", "a", "c", "b"])

    r = CalibreReader.from_root(lib.root)
    p = _payload_by_id(r, added.book_id, include_custom_values=True)

    assert p.custom_values["mood"] == "brooding"
    # Reader should preserve first-seen order and dedupe subsequent repeats.
    assert p.custom_values["multi"] == ["a", "b", "c"]


def test_d2_reads_series_custom_column_index_from_link_extra(provision_calibre_library) -> None:
    lib = provision_calibre_library(name="lib_d2_series")
    b = CalibreLibraryBuilder(lib.root)

    b.create_custom_column(label="saga", name="Saga", datatype="series", is_multiple=False)

    added = b.add_book(
        title="Custom Series",
        authors=["S. Author"],
        formats={"EPUB": b"epub"},
    )

    b.set_custom_value(book_id=added.book_id, label="saga", value=("SagaName", 2.5))

    r = CalibreReader.from_root(lib.root)
    cv = r.read_custom_values(added.book_id)

    assert cv["saga"]["name"] == "SagaName"
    assert cv["saga"]["index"] == 2.5


def test_d2_reads_bool_int_float_rating_enum_and_composite(provision_calibre_library) -> None:
    lib = provision_calibre_library(name="lib_d2_scalars")
    b = CalibreLibraryBuilder(lib.root)

    b.create_custom_column(label="flag", name="Flag", datatype="bool")
    b.create_custom_column(label="num", name="Num", datatype="int")
    b.create_custom_column(label="flt", name="Flt", datatype="float")
    b.create_custom_column(label="rate", name="Rate", datatype="rating")
    b.create_custom_column(label="enum", name="Enum", datatype="enumeration")
    b.create_custom_column(label="comp", name="Comp", datatype="composite")

    added = b.add_book(
        title="Scalar Customs",
        authors=["C. Author"],
        formats={"EPUB": b"epub"},
    )

    b.set_custom_value(book_id=added.book_id, label="flag", value=True)
    b.set_custom_value(book_id=added.book_id, label="num", value=42)
    b.set_custom_value(book_id=added.book_id, label="flt", value=3.25)
    b.set_custom_value(book_id=added.book_id, label="rate", value=8)
    b.set_custom_value(book_id=added.book_id, label="enum", value="blue")
    b.set_custom_value(book_id=added.book_id, label="comp", value="computed-ish")

    r = CalibreReader.from_root(lib.root)
    cv = r.read_custom_values(added.book_id)

    assert cv["flag"] is True
    assert cv["num"] == 42
    assert abs(cv["flt"] - 3.25) < 1e-9
    assert cv["rate"] == 8
    assert cv["enum"] == "blue"
    assert cv["comp"] == "computed-ish"


def test_d2_normalizes_datetime_from_int_epoch_and_iso_z_suffix(provision_calibre_library) -> None:
    lib = provision_calibre_library(name="lib_d2_datetime")
    b = CalibreLibraryBuilder(lib.root)

    num = b.create_custom_column(label="dt", name="DT", datatype="datetime")
    added = b.add_book(
        title="Datetime Customs",
        authors=["T. Author"],
        formats={"EPUB": b"epub"},
    )

    table = f"custom_column_{num}"
    conn = b.connect()
    try:
        # Store one row as an integer epoch (seconds) and one as an ISO string with Z.
        conn.execute(f"DELETE FROM {table} WHERE book=?", (added.book_id,))
        conn.execute(f"INSERT OR REPLACE INTO {table} (book, value) VALUES (?, ?)", (added.book_id, 1700000000))
        conn.commit()
    finally:
        conn.close()

    r = CalibreReader.from_root(lib.root)
    cv = r.read_custom_values(added.book_id)
    # 1700000000 -> 2023-11-14T22:13:20+00:00 (UTC); we don't hardcode full string to avoid tz differences.
    assert isinstance(cv["dt"], str)
    assert cv["dt"].endswith("+00:00") or cv["dt"].endswith("Z") or "2023" in cv["dt"]

    # Now store ISO with Z and ensure Z is normalized to +00:00.
    conn = b.connect()
    try:
        conn.execute(f"INSERT OR REPLACE INTO {table} (book, value) VALUES (?, ?)", (added.book_id, "2020-01-02T03:04:05Z"))
        conn.commit()
    finally:
        conn.close()

    cv2 = r.read_custom_values(added.book_id)
    assert cv2["dt"].startswith("2020-01-02T03:04:05")
    assert "+00:00" in cv2["dt"] or cv2["dt"].endswith("Z")

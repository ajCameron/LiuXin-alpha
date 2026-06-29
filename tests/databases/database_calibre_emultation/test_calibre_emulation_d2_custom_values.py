from __future__ import annotations

import pytest

from LiuXin_alpha.utils.calibre_compat.calibre_database_emulation import CalibreReader
from LiuXin_alpha.databases.database_driver_plugins.SQL.calibre_database_generator import CalibreLibraryBuilder


def _payload_by_id(reader: CalibreReader, book_id: int, **kwargs):
    for p in reader.iter_book_payloads(batch_size=50, **kwargs):
        if p.calibre_book_id == book_id:
            return p
    raise AssertionError(f"book_id {book_id} not found")


def _assert_roundtrip_value(*, datatype: str, actual, expected) -> None:
    if datatype == "float":
        assert abs(actual - expected) < 1e-9
        return
    assert actual == expected


@pytest.mark.parametrize(
    ("datatype", "value", "expected"),
    (
        pytest.param("text", "brooding", "brooding", id="text"),
        pytest.param("bool", True, True, id="bool"),
        pytest.param("int", 42, 42, id="int"),
        pytest.param("float", 3.25, 3.25, id="float"),
        pytest.param("rating", 8, 8, id="rating"),
        pytest.param("enumeration", "blue", "blue", id="enumeration"),
        pytest.param("comments", "<p>rich note</p>", "<p>rich note</p>", id="comments"),
        pytest.param("composite", "computed-ish", "computed-ish", id="composite"),
    ),
)
def test_d2_roundtrips_single_custom_value_matrix(
    provision_calibre_library,
    datatype: str,
    value,
    expected,
) -> None:
    lib = provision_calibre_library(name=f"lib_d2_{datatype}")
    b = CalibreLibraryBuilder(lib.root)

    b.create_custom_column(label="cc_case", name="Case", datatype=datatype, is_multiple=False)

    added = b.add_book(
        title=f"Custom {datatype}",
        authors=["A. Author"],
        formats={"EPUB": b"epub"},
    )

    b.set_custom_value(book_id=added.book_id, label="cc_case", value=value)

    r = CalibreReader.from_root(lib.root)
    cv = r.read_custom_values(added.book_id)
    p = _payload_by_id(r, added.book_id, include_custom_values=True)

    _assert_roundtrip_value(datatype=datatype, actual=cv["cc_case"], expected=expected)
    _assert_roundtrip_value(datatype=datatype, actual=p.custom_values["cc_case"], expected=expected)


def test_d2_reads_text_multi_values_with_stable_order_and_dedupe(provision_calibre_library) -> None:
    lib = provision_calibre_library(name="lib_d2_text_multi")
    b = CalibreLibraryBuilder(lib.root)

    b.create_custom_column(label="multi", name="Multi", datatype="text", is_multiple=True)

    added = b.add_book(
        title="Custom Text Multi",
        authors=["A. Author"],
        formats={"EPUB": b"epub"},
    )

    b.set_custom_value(book_id=added.book_id, label="multi", value=["a", "b", "a", "c", "b"])

    r = CalibreReader.from_root(lib.root)
    cv = r.read_custom_values(added.book_id)
    p = _payload_by_id(r, added.book_id, include_custom_values=True)

    # Reader should preserve first-seen order and dedupe subsequent repeats.
    assert cv["multi"] == ["a", "b", "c"]
    assert p.custom_values["multi"] == ["a", "b", "c"]


@pytest.mark.parametrize(
    ("value", "extra", "expected_name", "expected_index"),
    (
        pytest.param(("SagaName", 2.5), None, "SagaName", 2.5, id="tuple"),
        pytest.param({"name": "SagaName", "index": 3.0}, None, "SagaName", 3.0, id="dict"),
        pytest.param("SagaName", None, "SagaName", 1.0, id="plain-default-index"),
        pytest.param("SagaName", 4.5, "SagaName", 4.5, id="plain-extra-index"),
    ),
)
def test_d2_series_custom_column_accepts_supported_input_shapes(
    provision_calibre_library,
    value,
    extra,
    expected_name: str,
    expected_index: float,
) -> None:
    lib = provision_calibre_library(name=f"lib_d2_series_{expected_index}".replace(".", "_"))
    b = CalibreLibraryBuilder(lib.root)

    b.create_custom_column(label="saga", name="Saga", datatype="series", is_multiple=False)

    added = b.add_book(
        title="Custom Series",
        authors=["S. Author"],
        formats={"EPUB": b"epub"},
    )

    b.set_custom_value(book_id=added.book_id, label="saga", value=value, extra=extra)

    r = CalibreReader.from_root(lib.root)
    cv = r.read_custom_values(added.book_id)
    p = _payload_by_id(r, added.book_id, include_custom_values=True)

    assert cv["saga"]["name"] == expected_name
    assert cv["saga"]["index"] == expected_index
    assert p.custom_values["saga"]["name"] == expected_name
    assert p.custom_values["saga"]["index"] == expected_index


@pytest.mark.parametrize(
    ("raw_value", "mode"),
    (
        pytest.param(1700000000, "epoch", id="epoch-int"),
        pytest.param("2020-01-02T03:04:05Z", "iso-z", id="iso-z"),
    ),
)
def test_d2_normalizes_datetime_custom_column_values(
    provision_calibre_library,
    raw_value,
    mode: str,
) -> None:
    lib = provision_calibre_library(name=f"lib_d2_datetime_{mode}")
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
        conn.execute(f"DELETE FROM {table} WHERE book=?", (added.book_id,))
        conn.execute(f"INSERT OR REPLACE INTO {table} (book, value) VALUES (?, ?)", (added.book_id, raw_value))
        conn.commit()
    finally:
        conn.close()

    r = CalibreReader.from_root(lib.root)
    cv = r.read_custom_values(added.book_id)
    p = _payload_by_id(r, added.book_id, include_custom_values=True)

    for value in (cv["dt"], p.custom_values["dt"]):
        assert isinstance(value, str)
        if mode == "epoch":
            assert value.endswith("+00:00") or value.endswith("Z") or "2023" in value
        else:
            assert value.startswith("2020-01-02T03:04:05")
            assert "+00:00" in value or value.endswith("Z")

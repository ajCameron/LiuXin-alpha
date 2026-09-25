"""
Check Calibre custom-value decoding through direct reads and streamed book payloads for scalar, multi-text, series, and datetime inputs.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_d2_custom_values.py
"""
from __future__ import annotations

import pytest

from LiuXin_alpha.utils.calibre_compat.calibre_database_emulation import CalibreReader
from LiuXin_alpha.databases.database_driver_plugins.SQL.calibre_database_generator import CalibreLibraryBuilder


def _payload_by_id(reader: CalibreReader, book_id: int, **kwargs):
    """
    Return the first streamed payload whose calibre_book_id matches the requested ID.

    Use batch_size=50; kwargs must not supply a second batch_size value.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_d2_custom_values.py


    :param reader: Reader whose payload iterator is searched.
    :param book_id: Calibre book ID to match exactly.
    :param kwargs: Keyword options forwarded to iter_book_payloads.
    :return: Matching payload; raises AssertionError after exhaustion if the ID is
        absent.
    """
    for p in reader.iter_book_payloads(batch_size=50, **kwargs):
        if p.calibre_book_id == book_id:
            return p
    raise AssertionError(f"book_id {book_id} not found")


def _assert_roundtrip_value(*, datatype: str, actual, expected) -> None:
    """
    Compare float values with absolute error below 1e-9 and all other datatypes with equality.

    Example:
        >>> _assert_roundtrip_value(datatype='float', actual=3.25, expected=3.25)
        >>> _assert_roundtrip_value(datatype='text', actual='café', expected='café')


    :param datatype: Parametrized custom-column datatype.
    :param actual: Decoded value to compare.
    :param expected: Expected decoded value.
    :return: None; failed comparisons raise AssertionError and incompatible arithmetic
        errors propagate.
    """
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
    """
    Check eight scalar custom datatypes decode to the expected values through both read_custom_values and payload iteration.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_d2_custom_values.py::test_d2_roundtrips_single_custom_value_matrix


    :param provision_calibre_library: Fixture factory creating an isolated blank
        library; skips when SQLite lacks the required FTS5 support.
    :param datatype: Parametrized custom-column datatype.
    :param value: Builder input value for the custom column.
    :param expected: Expected decoded value.
    :return: None; failed expectations raise AssertionError.
    """
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
    """
    Write repeated text values and check both read paths return the exact first-seen sequence a, b, c.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_d2_custom_values.py::test_d2_reads_text_multi_values_with_stable_order_and_dedupe


    :param provision_calibre_library: Fixture factory creating an isolated blank
        library; skips when SQLite lacks the required FTS5 support.
    :return: None; failed expectations raise AssertionError.
    """
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
    """
    Check tuple, mapping, and plain series inputs produce the expected name/index through both read paths.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_d2_custom_values.py::test_d2_series_custom_column_accepts_supported_input_shapes


    :param provision_calibre_library: Fixture factory creating an isolated blank
        library; skips when SQLite lacks the required FTS5 support.
    :param value: Builder input value for the custom column.
    :param extra: Optional explicit series index passed alongside the builder value.
    :param expected_name: Expected decoded series name.
    :param expected_index: Expected decoded numeric series index.
    :return: None; failed expectations raise AssertionError.
    """
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
    """
    Insert raw datetime values directly and check string-shaped normalized output through both read paths.

    The epoch branch accepts a UTC suffix or any string containing 2023; the ISO branch
    checks the timestamp prefix and a UTC marker rather than whole-string equality.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_d2_custom_values.py::test_d2_normalizes_datetime_custom_column_values


    :param provision_calibre_library: Fixture factory creating an isolated blank
        library; skips when SQLite lacks the required FTS5 support.
    :param raw_value: Parametrized epoch integer or ISO timestamp inserted into the
        dynamic value table.
    :param mode: Parametrized epoch or iso-z branch selecting the output assertions.
    :return: None; failed expectations raise AssertionError.
    """
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

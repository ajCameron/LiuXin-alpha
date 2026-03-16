"""Database contract: add.title() compatibility path splits metadata into WEMI core rows."""

from __future__ import annotations

import datetime

import pytest

from LiuXin_alpha.databases.metadata_tools.add import Add


def _resolve_work_id(row) -> int:
    for key in ("work_id", "title_id", "book_work_id"):
        try:
            value = row[key]
        except Exception:
            continue
        if value is None:
            continue
        return int(value)
    raise AssertionError(f"Unable to resolve work id from row: {row!r}")


def test_add_title_creates_work_expression_manifestation_items(open_db) -> None:
    if "works" not in set(open_db.get_tables()):
        pytest.skip("Schema does not expose FRBR/WEMI tables")

    add = Add(open_db)
    title_row = add.title(
        title="How Much For Just The Planet?",
        title_sort="How Much For Just The Planet?",
        title_creator_sort="Roberts, Eric",
        title_pub_date=datetime.date(1987, 10, 1),
        title_copyright_date=datetime.date(1987, 10, 1),
        title_wikipedia="https://en.wikipedia.org/wiki/How_Much_for_Just_the_Planet%3F",
        title_fiction_length_category="novel",
        title_type="novel",
        title_source="contract_test",
        title_source_path="/tmp/planet_1.epub(#BREAK#)/tmp/planet_2.epub",
        title_source_name="planet_1.epub(#BREAK#)planet_2.epub",
        title_wordcount=132560,
    )

    work_id = _resolve_work_id(title_row)
    work_row = open_db.get_row_from_id("works", work_id)
    assert work_row is not None
    assert work_row["work_title"] == "How Much For Just The Planet?"
    assert work_row["work_sort_title"] == "How Much For Just The Planet?"
    assert work_row["work_type"] == "novel"
    assert work_row["work_discovery_note"] == "contract_test"

    expression_rows = open_db.get_interlinked_rows(primary_row=work_row, secondary_table="expressions")
    assert len(expression_rows) == 1
    expression_row = expression_rows[0]
    assert expression_row["expression_wordcount"] == 132560
    assert expression_row["expression_fiction_length_category"] == "novel"

    manifestation_rows = open_db.get_interlinked_rows(primary_row=expression_row, secondary_table="manifestations")
    assert len(manifestation_rows) == 1
    manifestation_row = manifestation_rows[0]
    assert manifestation_row["manifestation_pub_year"] == 1987
    assert manifestation_row["manifestation_format_detail"] == "EPUB"
    assert manifestation_row["manifestation_carrier_type"] == "ebook"

    item_rows = open_db.search(
        table="items",
        column="item_manifestation_id",
        search_term=manifestation_row["manifestation_id"],
    )
    assert len(item_rows) == 2
    item_paths = sorted(row["item_source_path"] for row in item_rows)
    assert item_paths == ["/tmp/planet_1.epub", "/tmp/planet_2.epub"]
    assert set(row["item_source"] for row in item_rows) == {"contract_test"}

    book_row = add.book(title_row=title_row)
    assert _resolve_work_id(book_row) == work_id


def test_add_title_override_updates_existing_work_chain(open_db) -> None:
    if "works" not in set(open_db.get_tables()):
        pytest.skip("Schema does not expose FRBR/WEMI tables")

    add = Add(open_db)
    first = add.title(title="Override Me", title_source="first_pass")
    first_work_id = _resolve_work_id(first)

    second = add.title(
        title="Override Applied",
        title_source="second_pass",
        title_type="novel",
        override_title_row=first,
    )
    second_work_id = _resolve_work_id(second)

    assert second_work_id == first_work_id

    work_row = open_db.get_row_from_id("works", first_work_id)
    assert work_row is not None
    assert work_row["work_title"] == "Override Applied"
    assert work_row["work_discovery_note"] == "second_pass"
    assert work_row["work_type"] == "novel"

    expression_rows = open_db.get_interlinked_rows(primary_row=work_row, secondary_table="expressions")
    assert len(expression_rows) == 1
    manifestation_rows = open_db.get_interlinked_rows(primary_row=expression_rows[0], secondary_table="manifestations")
    assert len(manifestation_rows) == 1

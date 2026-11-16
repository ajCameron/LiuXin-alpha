"""Protect text normalization and cached custom-value reads on Python 3."""

import pytest

from LiuXin_alpha.utils.calibre_compat.custom_columns import (
    CustomColumns,
)
from LiuXin_alpha.databases.utils import cleanup_tags
from tests.databases.custom_field_fixtures import sqlite_db as sqlite_db
from tests.databases.custom_field_fixtures import sqlite_template as sqlite_template
from tests.databases.test_custom_column_repairs import service


@pytest.mark.parametrize("normalize", [cleanup_tags, CustomColumns.cleanup_tags])
def test_cleanup_tags_decodes_before_cleanup_and_keeps_first_spelling(normalize):
    source = [
        " Travel ",
        b"travel",
        "Space  Opera",
        b"sci-fi, fantasy",
        " ",
        "日本語",
        "日本語",
    ]
    assert normalize(iter(source)) == [
        "Travel",
        "Space Opera",
        "sci-fi; fantasy",
        "日本語",
    ]
    assert source[0] == " Travel "
    assert normalize([]) == []


@pytest.mark.parametrize("sort_alpha", [False, True])
@pytest.mark.parametrize("value", ["Zulu|alpha|ALPHA|日本語", ""])
@pytest.mark.parametrize("index_is_id", [False, True])
def test_both_custom_value_readers_sort_without_mutating_cached_storage(
    sqlite_db, sort_alpha, value, index_is_id
):
    db = sqlite_db
    facade, number, owners = service(
        db, multiple=True, display={"sort_alpha": sort_alpha}, cached=True
    )
    facade.set_custom(owners[0], value.split("|") if value else [], num=number)
    expected = (
        []
        if not value
        else (
            ["alpha", "Zulu", "日本語"] if sort_alpha else ["Zulu", "alpha", "日本語"]
        )
    )
    before = list(facade.data._data[owners[0]])
    index = owners[0] if index_is_id else 0
    assert facade.get_custom(index, num=number, index_is_id=index_is_id) == expected
    assert facade.get_custom_and_extra(index, num=number, index_is_id=index_is_id) == (
        expected,
        None,
    )
    assert facade.data._data[owners[0]] == before

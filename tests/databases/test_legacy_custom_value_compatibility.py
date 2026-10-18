"""Protect text normalization and cached custom-value reads on Python 3."""

from types import SimpleNamespace

import pytest

from LiuXin_alpha.databases.custom_columns import CustomColumns
from LiuXin_alpha.databases.custom_columns.cc_get_methods_mixin import CCGetMethodsMixin
from LiuXin_alpha.databases.utils import cleanup_tags


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
    sort_alpha, value, index_is_id
):
    class Cache:
        _data = {7: [value]}

        def __getitem__(self, position):
            assert position == 0
            return self._data[7]

    host = SimpleNamespace(
        custom_column_num_map={
            1: {
                "num": 1,
                "datatype": "text",
                "is_multiple": True,
                "multiple_seps": {"cache_to_list": "|"},
                "display": {"sort_alpha": sort_alpha},
            }
        },
        FIELD_MAP={1: 0},
        data=Cache(),
        id=lambda position: 7,
    )
    expected = (
        []
        if not value
        else ["alpha", "ALPHA", "Zulu", "日本語"]
        if sort_alpha
        else value.split("|")
    )
    index = 7 if index_is_id else 0
    assert (
        CCGetMethodsMixin.get_custom(host, index, num=1, index_is_id=index_is_id)
        == expected
    )
    assert CCGetMethodsMixin.get_custom_and_extra(
        host, index, num=1, index_is_id=index_is_id
    ) == (expected, None)
    assert host.data._data[7][0] == value

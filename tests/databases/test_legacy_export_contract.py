"""Exercise retained legacy export reads using a separate configuration backend."""

from types import SimpleNamespace

from LiuXin_alpha.databases.utils import get_data_as_dict


class _ExportCache:
    def __init__(self):
        fields = (
            "id",
            "cover",
            "title",
            "sort",
            "authors",
            "author_sort",
            "publisher",
            "rating",
            "timestamp",
            "size",
            "tags",
            "comments",
            "series",
            "series_index",
            "uuid",
            "pubdate",
            "last_modified",
            "identifiers",
            "languages",
            1,
            "1_index",
        )
        self.FIELD_MAP = {field: index for index, field in enumerate(fields)}
        self.library_path = "/library"
        self.backend = SimpleNamespace(
            library_path=self.library_path,
            custom_column_num_map={1: {"datatype": "series"}},
        )
        values = {
            "id": 7,
            "cover": False,
            "title": "Novel",
            "authors": "A| B,C",
            "tags": " One| Two ,Three",
            1: "Saga",
            "1_index": 2.0,
        }
        self.data = [None, [values.get(field) for field in fields]]

    def isbn(self, idx, index_is_id=False):
        assert (idx, index_is_id) == (7, True)
        return None

    def path(self, idx, index_is_id=False):
        assert (idx, index_is_id) == (7, True)
        return "Author/Novel"

    def formats(self, idx, index_is_id=False):
        assert (idx, index_is_id) == (7, True)
        return "EPUB,PDF"

    def format_abspath(self, idx, fmt, index_is_id=False):
        assert (idx, index_is_id) == (7, True)
        return "/library/Author/Novel/book.epub" if fmt == "EPUB" else None


def test_legacy_export_preserves_custom_series_and_remaps_available_paths():
    cache = _ExportCache()
    result = get_data_as_dict(
        cache, prefix="/export", ids={7}, convert_to_local_tz=False
    )
    assert len(result) == 1
    row = result[0]
    assert row[1] == "Saga" and row["1_index"] == 2.0
    assert row["authors"] == ["A, B", "C"]
    assert row["tags"] == ["One, Two", "Three"]
    assert row["cover"] is None and row["isbn"] == ""
    assert row["formats"] == ["/export/Author/Novel/book.epub"]
    assert row["available_formats"] == ["EPUB", "PDF"]
    assert get_data_as_dict(cache, ids=set(), convert_to_local_tz=False) == []
    assert get_data_as_dict(cache, convert_to_local_tz=False)[0]["formats"] == [
        "/library/Author/Novel/book.epub"
    ]

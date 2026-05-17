from __future__ import annotations

import io
import json
from datetime import date, datetime, timezone

from LiuXin_alpha.metadata.book.base import calibreMetadata
from LiuXin_alpha.metadata.book.json_codec import (
    JsonCodec,
    datetime_to_string,
    decode_is_multiple,
    decode_thumbnail,
    encode_is_multiple,
    encode_thumbnail,
    object_to_unicode,
    string_to_datetime,
)


class _DecodedBook:
    def __init__(self, prefix: str, lpath: str | None) -> None:
        self.prefix = prefix
        self.lpath = lpath
        self.user_metadata: dict[str, object] = {}

    def set_all_user_metadata(self, metadata: dict[str, object]) -> None:
        self.user_metadata = metadata


def _datetime_user_metadata(value: datetime) -> dict[str, object]:
    return {
        "#when": {
            "name": "When",
            "datatype": "datetime",
            "is_multiple": {},
            "display": {},
            "#value#": value,
        },
        "#tags": {
            "name": "Tags",
            "datatype": "text",
            "is_multiple": {
                "cache_to_list": "|",
                "ui_to_list": ",",
                "list_to_ui": ", ",
            },
            "display": {},
            "#value#": ["One", "Two"],
        },
    }


def test_datetime_thumbnail_and_unicode_helpers_roundtrip() -> None:
    moment = datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc)

    assert string_to_datetime("None") is None
    assert string_to_datetime("not a date") is None
    assert string_to_datetime(datetime_to_string(moment)).year == 2024
    assert datetime_to_string(None) == "None"
    assert datetime_to_string(date(2024, 1, 2)) != "None"

    encoded_thumbnail = encode_thumbnail((12, 34, b"cover bytes"))
    assert encoded_thumbnail == (12, 34, "Y292ZXIgYnl0ZXM=")
    assert decode_thumbnail(encoded_thumbnail) == (12, 34, b"cover bytes")
    assert encode_thumbnail((1, 1, "text")) == (1, 1, "dGV4dA==")
    assert encode_thumbnail(None) is None
    assert decode_thumbnail(None) is None

    assert object_to_unicode(b"caf\xe9", enc="latin-1") == "café"
    assert object_to_unicode([b"a", {"nested": b"\xe9"}], enc="latin-1") == [
        "a",
        {"nested": "é"},
    ]
    assert object_to_unicode({b"k": [b"v"]}) == {"k": ["v"]}


def test_is_multiple_metadata_migration_paths() -> None:
    composite = {"datatype": "composite", "is_multiple": {"ui_to_list": ";"}}
    encode_is_multiple(composite)
    assert composite["is_multiple"] == ","
    assert composite["is_multiple2"] == {"ui_to_list": ";"}

    decode_is_multiple(composite)
    assert composite["is_multiple"] == {"ui_to_list": ";"}
    assert "is_multiple2" not in composite

    names = {
        "datatype": "text",
        "display": {"is_names": True},
        "is_multiple": "|",
    }
    decode_is_multiple(names)
    assert names["is_multiple"] == {
        "cache_to_list": "|",
        "ui_to_list": "&",
        "list_to_ui": ", ",
    }

    plain = {"datatype": "text", "is_multiple": None}
    encode_is_multiple(plain)
    assert plain == {"datatype": "text", "is_multiple": None, "is_multiple2": {}}
    decode_is_multiple(plain)
    assert plain["is_multiple"] == {}


def test_json_codec_encodes_and_decodes_book_metadata() -> None:
    moment = datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    metadata = calibreMetadata("Café Book", ["Author One"])
    metadata.tags = ["unicode", "測試"]
    metadata.lpath = b"folder/book.epub"
    metadata.timestamp = moment
    metadata.thumbnail = (1, 2, b"thumb")
    metadata.set_identifier("isbn", "9780000000001")
    metadata.set_all_user_metadata(_datetime_user_metadata(moment))

    codec = JsonCodec()
    encoded = codec.encode_book_metadata(metadata)

    assert encoded["title"] == "Café Book"
    assert encoded["tags"] == ["unicode", "測試"]
    assert encoded["lpath"] == "folder/book.epub"
    assert encoded["thumbnail"] == (1, 2, "dGh1bWI=")
    assert encoded["user_metadata"]["#when"]["#value#"] != moment
    assert encoded["user_metadata"]["#tags"]["is_multiple"] == "|"

    stream = io.StringIO()
    codec.encode_to_file(stream, [metadata])
    decoded_json = json.loads(stream.getvalue())
    assert decoded_json[0]["title"] == "Café Book"

    decoded_books: list[_DecodedBook] = []
    codec.decode_from_file(io.StringIO(stream.getvalue()), decoded_books, _DecodedBook, "prefix")

    assert len(decoded_books) == 1
    decoded = decoded_books[0]
    assert decoded.prefix == "prefix"
    assert decoded.title == "Café Book"
    assert decoded.identifiers == {"isbn": "9780000000001"}
    assert decoded.thumbnail == (1, 2, b"thumb")
    assert decoded.user_metadata["#when"]["#value#"].year == 2024
    assert decoded.user_metadata["#tags"]["is_multiple"] == {
        "cache_to_list": "|",
        "ui_to_list": ",",
        "list_to_ui": ", ",
    }


def test_json_codec_preserves_unicode_torture_without_ascii_escaping() -> None:
    metadata = calibreMetadata(
        "Bibliothèque 東京 普通话 简体中文 日本語 こんにちは العربية Звёзды हिन्दी 🚀",
        ["李 白", "王小明", "山田太郎", "Renée"],
    )
    metadata.tags = [
        "café",
        "東京",
        "普通话",
        "简体中文",
        "日本語",
        "こんにちは",
        "العربية",
        "Звёзды",
        "हिन्दी",
        "e\u0301",
        "🚀",
    ]
    metadata.comments = "RTL العربية next to Mandarin 普通话, Simplified Chinese 简体中文, Japanese 日本語 and combining e\u0301"
    metadata.publisher = "出版社 / 简体中文出版社 / 日本の出版社 / دار النشر"
    metadata.languages = ["jpn", "zho", "cmn", "ara", "rus", "hin"]
    metadata.lpath = "日本語/普通话/简体中文/مرحبا/звезды.epub".encode("utf-8")
    metadata.set_all_user_metadata(
        {
            "#labels": {
                "name": "标签 / وسوم",
                "datatype": "text",
                "is_multiple": {
                    "cache_to_list": "|",
                    "ui_to_list": ",",
                    "list_to_ui": ", ",
                },
                "display": {},
                "#value#": ["niño", "漢字", "العربية", "हिन्दी"],
            },
        }
    )

    codec = JsonCodec()
    stream = io.StringIO()
    codec.encode_to_file(stream, [metadata])
    payload = stream.getvalue()

    assert "Bibliothèque 東京 普通话 简体中文 日本語 こんにちは العربية Звёзды हिन्दी 🚀" in payload
    assert "\\u6771" not in payload
    assert "\\u7b80" not in payload
    assert "\\ud83d" not in payload

    decoded_books: list[_DecodedBook] = []
    codec.decode_from_file(io.StringIO(payload), decoded_books, _DecodedBook, "root")

    decoded = decoded_books[0]
    assert decoded.title == "Bibliothèque 東京 普通话 简体中文 日本語 こんにちは العربية Звёзды हिन्दी 🚀"
    assert decoded.lpath == "日本語/普通话/简体中文/مرحبا/звезды.epub"
    assert decoded.tags == [
        "café",
        "東京",
        "普通话",
        "简体中文",
        "日本語",
        "こんにちは",
        "العربية",
        "Звёзды",
        "हिन्दी",
        "e\u0301",
        "🚀",
    ]
    assert decoded.user_metadata["#labels"]["#value#"] == ["niño", "漢字", "العربية", "हिन्दी"]


def test_json_codec_decodes_legacy_classifiers_and_contains_bad_json(caplog) -> None:
    codec = JsonCodec()
    book = codec.raw_to_book(
        {"lpath": "book.epub", "classifiers": {"isbn": "9780000000001"}},
        _DecodedBook,
        "root",
    )

    assert book is not None
    assert book.identifiers == {"isbn": "9780000000001"}

    books: list[_DecodedBook] = []
    codec.decode_from_file(io.StringIO("{bad"), books, _DecodedBook, "root")

    assert books == []
    assert "Exception during JSON decode_from_file" in caplog.text

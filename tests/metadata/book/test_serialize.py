from __future__ import annotations

import base64
from pathlib import Path

from LiuXin_alpha.metadata.book.base import calibreMetadata
from LiuXin_alpha.metadata.book.serialize import (
    ensure_unicode,
    metadata_as_dict,
    metadata_from_dict,
    read_cover,
)


PNG_BYTES = (
    b"\x89PNG\r\n\x1a\n"
    b"\x00\x00\x00\rIHDR"
    b"\x00\x00\x00\x01\x00\x00\x00\x01"
    b"\x08\x02\x00\x00\x00\x90wS\xde"
    b"\x00\x00\x00\x00IEND\xaeB`\x82"
)


def _custom_meta() -> dict[str, object]:
    return {
        "name": "Custom",
        "datatype": "text",
        "is_multiple": {
            "cache_to_list": "|",
            "ui_to_list": ",",
            "list_to_ui": ", ",
        },
        "display": {},
        "#value#": ["One"],
    }


def test_ensure_unicode_recurses_through_nested_values() -> None:
    payload = {
        b"title": b"caf\xe9",
        "items": [b"one", {"nested": b"two"}],
        "count": 3,
    }

    assert ensure_unicode(payload, enc="latin-1") == {
        "title": "café",
        "items": ["one", {"nested": "two"}],
        "count": 3,
    }


def test_read_cover_loads_missing_cover_data(tmp_path: Path) -> None:
    cover = tmp_path / "cover.png"
    cover.write_bytes(PNG_BYTES)

    metadata = calibreMetadata("Cover Book", ["Author"])
    metadata.cover = str(cover)
    metadata.cover_data = (None, None)

    assert read_cover(metadata) is metadata
    assert metadata.cover_data == ("png", PNG_BYTES)

    metadata.cover_data = ("png", b"existing")
    assert read_cover(metadata).cover_data == ("png", b"existing")

    metadata.cover_data = (None, None)
    metadata.cover = str(tmp_path / "missing.png")
    assert read_cover(metadata).cover_data == (None, None)


def test_metadata_as_dict_and_from_dict_roundtrip_serializable_fields() -> None:
    metadata = calibreMetadata("Serialized", ["Author"])
    metadata.tags = ["One", "Two"]
    metadata.languages = ["eng"]
    metadata.set_identifier("isbn", "9780000000001")
    metadata.cover_data = ("png", PNG_BYTES)
    metadata.set_user_metadata("#custom", _custom_meta())

    as_dict = metadata_as_dict(metadata, encode_cover_data=True)

    assert as_dict["title"] == "Serialized"
    assert as_dict["authors"] == ["Author"]
    assert as_dict["tags"] == ["One", "Two"]
    assert as_dict["identifiers"] == {"isbn": "9780000000001"}
    assert as_dict["cover_data"] == ["png", base64.standard_b64encode(PNG_BYTES).decode("ascii")]
    assert as_dict["user_metadata"]["#custom"]["#value#"] == ["One"]

    restored = metadata_from_dict(as_dict)

    assert restored.title == "Serialized"
    assert restored.authors == ["Author"]
    assert restored.tags == ["One", "Two"]
    assert restored.get_identifiers() == {"isbn": "9780000000001"}
    assert restored.get("#custom") == ["One"]


def test_metadata_dict_roundtrip_preserves_unicode_torture_values() -> None:
    metadata = calibreMetadata(
        "ספר / كتاب / 本 / 普通话 / 简体中文 / 日本語 / Книга / पुस्तक 🚀",
        ["李 白", "王小明", "山田太郎", "أحمد", "Renée"],
    )
    metadata.tags = [
        "café",
        "東京",
        "普通话",
        "简体中文",
        "日本語",
        "العربية",
        "עברית",
        "Звёзды",
        "हिन्दी",
        "e\u0301",
    ]
    metadata.languages = ["heb", "ara", "jpn", "zho", "cmn", "rus", "hin"]
    metadata.publisher = "出版社 / 简体中文出版社 / 日本の出版社 / دار النشر / Издательство"
    metadata.comments = (
        "Mixed RTL العربية עברית, Mandarin 普通话, Simplified Chinese 简体中文, "
        "Japanese 日本語, accents naïve, combining e\u0301, emoji 🚀"
    )
    metadata.set_identifier("doi", "10.1234/كتاب-東京")
    metadata.set_user_metadata(
        "#custom",
        {
            "name": "Custom / مخصص / カスタム",
            "datatype": "text",
            "is_multiple": {
                "cache_to_list": "|",
                "ui_to_list": ",",
                "list_to_ui": ", ",
            },
            "display": {},
            "#value#": ["niño", "漢字", "العربية", "हिन्दी"],
        },
    )

    as_dict = metadata_as_dict(metadata)
    restored = metadata_from_dict(as_dict)

    assert as_dict["title"] == "ספר / كتاب / 本 / 普通话 / 简体中文 / 日本語 / Книга / पुस्तक 🚀"
    assert (
        as_dict["comments"]
        == "Mixed RTL العربية עברית, Mandarin 普通话, Simplified Chinese 简体中文, Japanese 日本語, "
        "accents naïve, combining e\u0301, emoji 🚀"
    )
    assert restored.title == metadata.title
    assert restored.authors == metadata.authors
    assert restored.tags == metadata.tags
    assert restored.languages == metadata.languages
    assert restored.publisher == metadata.publisher
    assert restored.get_identifiers() == {"doi": "10.1234/كتاب-東京"}
    assert restored.get("#custom") == ["niño", "漢字", "العربية", "हिन्दी"]


def test_metadata_as_dict_accepts_wrapper_objects_and_raw_cover_data() -> None:
    metadata = calibreMetadata("Wrapped", ["Author"])
    metadata.cover_data = ("jpeg", b"raw")

    class Wrapper:
        def to_book_metadata(self) -> calibreMetadata:
            return metadata

    as_dict = metadata_as_dict(Wrapper(), encode_cover_data=False)

    assert as_dict["title"] == "Wrapped"
    assert as_dict["cover_data"] == ("jpeg", b"raw")

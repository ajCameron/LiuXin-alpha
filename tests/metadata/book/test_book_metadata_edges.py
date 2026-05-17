from __future__ import annotations

from datetime import datetime, timezone

import pytest

from LiuXin_alpha.metadata import opf_tools
from LiuXin_alpha.metadata.book import base as book_base
from LiuXin_alpha.metadata.book.base import calibreMetadata, field_from_string, reset_field_metadata


def _meta(
    *,
    name: str,
    datatype: str,
    is_multiple: object = None,
    value: object = None,
    extra: object = None,
    display: dict[str, object] | None = None,
) -> dict[str, object]:
    metadata: dict[str, object] = {
        "name": name,
        "datatype": datatype,
        "is_multiple": is_multiple or {},
        "display": display or {},
        "#value#": value,
    }
    if extra is not None:
        metadata["#extra#"] = extra
    return metadata


def test_metadata_housekeeping_and_lazy_composite_evaluation() -> None:
    class Formatter:
        def safe_format(self, *args: object, **kwargs: object) -> str:
            return " computed composite "

    reset_field_metadata()
    metadata = calibreMetadata(None, [], formatter=Formatter())

    assert metadata.get_identifiers() == {}
    assert metadata.standard_field_keys() == book_base.STANDARD_METADATA_FIELDS
    assert metadata.get_data()["title"] == "Unknown"

    metadata.identifiers = None
    assert metadata.identifiers == {}
    metadata.set_user_metadata(
        "#broken_extra",
        _meta(name="Broken Extra", datatype="text", is_multiple={}),
    )
    assert metadata.get_extra("#broken_extra", "fallback") == "fallback"
    with pytest.raises(AttributeError):
        metadata.get_extra("#missing")

    metadata.set_user_metadata(
        "#composite",
        _meta(
            name="Composite",
            datatype="composite",
            display={"composite_template": "{title}"},
        ),
    )
    assert metadata.get("#composite") == "computed composite"
    assert metadata.get("#composite") == "computed composite"


def test_deepcopy_and_metadata_field_descriptions_are_isolated() -> None:
    metadata = calibreMetadata("Field Book", ["Author"])
    metadata.set_user_metadata(
        "#custom",
        _meta(name="Custom", datatype="text", is_multiple={}, value="value"),
    )

    clone = metadata.deepcopy()
    assert clone is not metadata
    assert clone.title == "Field Book"
    clone.title = "Changed"
    assert metadata.title == "Field Book"

    assert metadata.metadata_for_field("#custom")["name"] == "Custom"
    assert metadata.metadata_for_field("title")["name"] == "Title"
    assert metadata.get_standard_metadata("title", make_copy=False)["name"] == "Title"

    copied_standard = metadata.get_standard_metadata("title", make_copy=True)
    copied_standard["name"] = "Changed"
    assert metadata.get_standard_metadata("title", make_copy=False)["name"] == "Title"

    all_standard = calibreMetadata.get_all_standard_metadata(make_copy=True)
    assert "title" in all_standard
    all_standard["title"]["name"] = "Changed"
    assert calibreMetadata.get_all_standard_metadata(make_copy=False)["title"]["name"] == "Title"


def test_opf_and_database_delegation(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    source_metadata = calibreMetadata("From OPF", ["OPF Author"])
    calls: dict[str, object] = {}

    monkeypatch.setattr(opf_tools, "calibre_metadata_from_opf", lambda source: source_metadata)
    monkeypatch.setattr(
        opf_tools,
        "metadata_to_opf_bytes",
        lambda metadata, *, default_lang=None: b"opf:" + (default_lang or "").encode("ascii"),
    )

    def fake_to_opf_file(metadata, path, *, default_lang=None):
        calls["opf_file"] = (metadata, path, default_lang)
        return "written"

    monkeypatch.setattr(opf_tools, "metadata_to_opf_file", fake_to_opf_file)

    class SubMetadata(calibreMetadata):
        pass

    assert calibreMetadata.from_opf("source") is source_metadata
    cloned = SubMetadata.from_opf("source")
    assert isinstance(cloned, SubMetadata)
    assert cloned.title == "From OPF"
    assert source_metadata.to_opf_bytes(default_lang="eng") == b"opf:eng"

    path = tmp_path / "metadata.opf"
    assert source_metadata.write_to_opf(path, default_lang="fra") == "written"
    assert calls["opf_file"] == (source_metadata, path, "fra")

    class FakeWriter:
        def __init__(self, database):
            calls["database"] = database

        def write(self, metadata, **kwargs):
            calls["write"] = (metadata, kwargs)
            return "report"

    monkeypatch.setattr(
        "LiuXin_alpha.metadata.containers.metadata_containers.liuxin_wemi_metadata_writer.LiuXinWEMIMetadataWriter",
        FakeWriter,
    )

    assert source_metadata.write_to_database("db", fields=["tags"], item_id=7) == "report"
    assert calls["database"] == "db"
    assert calls["write"][0] is source_metadata
    assert calls["write"][1]["fields"] == ["tags"]
    assert calls["write"][1]["item_id"] == 7


def test_set_user_metadata_defaults_validation_and_template_copy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    metadata = calibreMetadata("Template Target", ["Unknown"])

    metadata.set_all_user_metadata(
        {
            "#multi": {
                "name": "Multi",
                "datatype": "text",
                "is_multiple": {"ui_to_list": ",", "list_to_ui": ", "},
                "display": {},
            },
            "#plain": {
                "name": "Plain",
                "datatype": "text",
                "is_multiple": {},
                "display": {},
            },
        }
    )
    assert metadata.get("#multi") == []
    assert metadata.get("#plain") is None

    with pytest.raises(AttributeError):
        metadata.set_user_metadata("bad", _meta(name="Bad", datatype="text"))

    class FakeFormatter:
        def safe_format(self, template, *_args, **_kwargs):
            values = {
                "{tags}": "One, Two",
                "{authors}": "Ada & Grace",
                "{title}": "Copied Title",
                "{bad}": RuntimeError("bad template"),
            }
            value = values[template]
            if isinstance(value, Exception):
                raise value
            return value

    monkeypatch.setattr("LiuXin_alpha.metadata.book.formatter.SafeFormat", FakeFormatter)

    metadata.template_to_attribute(
        object(),
        [
            ("{tags}", "tags"),
            ("{authors}", "authors"),
            ("{title}", "title"),
            ("{bad}", "publisher"),
        ],
    )

    assert metadata.tags == ["One", "Two"]
    assert metadata.authors == ["Ada", "Grace"]
    assert metadata.title == "Copied Title"
    assert metadata.publisher is None


def test_smart_update_replace_and_plain_object_identifier_paths() -> None:
    target = calibreMetadata("Target", ["Old"])
    target.series = "Old Series"
    source = calibreMetadata("Source", ["New"])
    source.tags = ["Tag"]
    source.languages = ["spa"]
    source.comments = "source comments"
    source.lpath = "book.epub"
    source.size = 123
    source.thumbnail = "thumb"
    source.set_identifier("isbn", "9780000000001")
    source.set_user_metadata(
        "#custom",
        _meta(name="Custom", datatype="text", is_multiple={}, value="source"),
    )

    target.smart_update(source, replace_metadata=True)

    assert target.title == "Source"
    assert target.authors == ["New"]
    assert target.tags == ["Tag"]
    assert target.languages == ["spa"]
    assert target.comments == "source comments"
    assert target.lpath == "book.epub"
    assert target.size == 123
    assert target.thumbnail == "thumb"
    assert target.get_identifiers() == {"isbn": "9780000000001"}
    assert target.get("#custom") == "source"
    assert target.series_index is None

    class PlainOther:
        title = "Plain"
        title_sort = "Plain Sort"
        authors = ["Plain Author"]
        author_sort_map = {}
        author_sort = "Author, Plain"
        tags: list[str] = []
        cover_data = None
        comments = ""
        languages = []
        series = None
        isbn = "plain-isbn"

    target.smart_update(PlainOther())
    assert target.title == "Plain"
    assert target.isbn == "plain-isbn"


def test_smart_update_handles_custom_multiple_type_mismatch() -> None:
    target = calibreMetadata("Target", ["Author"])
    target.set_user_metadata(
        "#labels",
        _meta(name="Labels", datatype="text", is_multiple={"list_to_ui": ", "}, value="not-a-list"),
    )

    source = calibreMetadata("Source", ["Author"])
    source.set_user_metadata(
        "#labels",
        _meta(
            name="Labels",
            datatype="text",
            is_multiple={"list_to_ui": ", "},
            value=["Alpha", "Beta"],
        ),
    )

    target.smart_update(source)

    assert target.get("#labels") == ["Alpha", "Beta"]


def test_extended_formatting_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(book_base, "sort_key", lambda value: str(value).casefold())

    metadata = calibreMetadata("Format Paths", ["Author"])
    metadata.title_sort = "Format Paths Sort"
    metadata.timestamp = datetime(2024, 1, 2, tzinfo=timezone.utc)
    metadata.pubdate = datetime(2024, 1, 3, tzinfo=timezone.utc)
    metadata.series = "Main Series"
    metadata.series_index = 3
    metadata.tags = ["Beta", "alpha"]
    metadata.identifiers = {"isbn": "9780000000001", "doi": "10.1234/demo"}
    metadata.size = 2 * 1024 * 1024
    metadata.rating = None

    metadata.set_user_metadata("#empty_series", _meta(name="Empty Series", datatype="series"))
    metadata.set_user_metadata("#empty", _meta(name="Empty", datatype="text", value=""))
    metadata.set_user_metadata(
        "#when",
        _meta(
            name="When",
            datatype="datetime",
            value=datetime(2024, 1, 4, tzinfo=timezone.utc),
            display={"date_format": "yyyy"},
        ),
    )
    metadata.set_user_metadata("#flag", _meta(name="Flag", datatype="bool", value=True))
    metadata.set_user_metadata("#rating", _meta(name="Custom Rating", datatype="rating", value=8))
    metadata.set_user_metadata(
        "#number",
        _meta(
            name="Number",
            datatype="float",
            value=3.14159,
            display={"number_format": "{:.1f}"},
        ),
    )

    assert metadata.format_rating() == "None"
    assert metadata.format_field_extended("#empty_series_index") == (
        "Empty Series_index",
        "",
        "",
        metadata.get_user_metadata("#empty_series", False),
    )
    assert metadata.format_field_extended("#empty") == ("Empty", "", None, None)
    assert metadata.format_field("#when")[1]
    assert metadata.format_field("#flag") == ("Flag", "Yes")
    assert metadata.format_field("#rating") == ("Custom Rating", "4")
    assert metadata.format_field("#number") == ("Number", "3.1")
    assert metadata.format_field("isbn") == ("isbn", "9780000000001")
    assert metadata.format_field("series_index")[1] == "3"
    assert metadata.format_field("identifiers") == (
        "Identifiers",
        "doi:10.1234/demo, isbn:9780000000001",
    )
    assert metadata.format_field("pubdate")[1]
    assert metadata.format_field("size") == ("Size", "2.00MB")

    rendered = str(metadata)
    assert "Title sort" in rendered
    assert "Timestamp" in rendered


def test_rendering_bool_and_print_helpers(monkeypatch: pytest.MonkeyPatch) -> None:
    metadata = calibreMetadata("Renderable", ["Author"])
    calls: list[object] = []

    monkeypatch.setattr(book_base, "prints", lambda *args: calls.append(args))
    metadata.print_all_attributes()
    assert calls

    monkeypatch.setattr(
        "LiuXin_alpha.surfaces.renderers.calibre_metadata.calibre_metadata_to_html",
        lambda mi: "<p>" + mi.title + "</p>",
    )
    assert metadata.to_html() == "<p>Renderable</p>"
    assert metadata.__nonzero__() is True


def test_author_parsing_and_datetime_field_from_string() -> None:
    metadata = calibreMetadata("Authors", ["Unknown"])
    metadata.authors_from_string("Ada Lovelace & Grace Hopper")

    assert metadata.authors == ["Ada Lovelace", "Grace Hopper"]
    parsed = field_from_string("pubdate", "2024-01-02", {"datatype": "datetime"})
    assert parsed.year == 2024

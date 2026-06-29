from __future__ import annotations

from datetime import datetime, timezone

import LiuXin_alpha.metadata.book.render as legacy_render
import LiuXin_alpha.surfaces.renderers.calibre_metadata as renderer

from LiuXin_alpha.metadata.book.base import calibreMetadata
from LiuXin_alpha.surfaces.renderers.calibre_metadata import (
    calibre_metadata_to_html,
    displayable_field_keys,
    field_sort,
    get_field_list,
    mi_to_html,
    search_href,
)


def _field_metadata(
    *,
    name: str,
    datatype: str | None,
    kind: str = "field",
    is_custom: bool = False,
    is_multiple: object = None,
    display: dict[str, object] | None = None,
    search_terms: list[str] | None = None,
) -> dict[str, object]:
    return {
        "kind": kind,
        "datatype": datatype,
        "name": name,
        "is_custom": is_custom,
        "is_multiple": is_multiple,
        "display": display or {},
        "search_terms": search_terms if search_terms is not None else [name.lower()],
    }


class _RendererMetadata:
    path = "/library/Series/Book Title"
    formats = ["EPUB", "PDF"]
    format_files = {"EPUB": "book", "PDF": "book scan"}
    identifiers = {"isbn": "9780000000001", "doi": "10.1234/demo"}
    authors = ["Linked Author", "Search Author"]
    author_link_map = {
        "Linked Author": "https://authors.example/linked",
        "Search Author": "",
    }
    author_sort_map = {"Search Author": "Author, Search"}
    languages = ["eng", "fra"]
    device_collections = ["zeta", "alpha"]
    comments = "Plain comment"
    rating = 8
    composite_html = "<b>Trusted</b>"
    composite_single = "Solo Value"
    composite_multiple = "One; Two ; "
    series = "Saga"
    pubdate = datetime(2024, 1, 2, tzinfo=timezone.utc)
    status = "Read"
    title_sort = "Sorted Title"

    def __init__(self) -> None:
        self.id = 42
        self.field_metadata: dict[str, dict[str, object] | None] = {
            "empty": None,
            "custom_bool": _field_metadata(
                name="Flag",
                datatype="bool",
                is_custom=True,
            ),
            "comments": _field_metadata(name="Comments", datatype="comments"),
            "rating": _field_metadata(name="Rating", datatype="rating"),
            "composite_html": _field_metadata(
                name="HTML Composite",
                datatype="composite",
                display={"contains_html": True},
            ),
            "composite_single": _field_metadata(
                name="Single Composite",
                datatype="composite",
            ),
            "composite_multiple": _field_metadata(
                name="Multiple Composite",
                datatype="composite",
                is_multiple={"list_to_ui": "; "},
            ),
            "path": _field_metadata(name="Path", datatype="text"),
            "formats": _field_metadata(name="Formats", datatype="text"),
            "identifiers": _field_metadata(name="Identifiers", datatype="text"),
            "authors": _field_metadata(name="Authors", datatype="text"),
            "languages": _field_metadata(name="Languages", datatype="text"),
            "series": _field_metadata(
                name="Series",
                datatype="series",
                search_terms=["series"],
            ),
            "pubdate": _field_metadata(name="Published", datatype="datetime"),
            "tags": _field_metadata(
                name="Tags",
                datatype="text",
                is_multiple={"list_to_ui": ", "},
                search_terms=["tags"],
            ),
            "status": _field_metadata(
                name="Status",
                datatype="enumeration",
                search_terms=["status"],
            ),
            "sort": _field_metadata(name="", datatype="text"),
            "title_sort": _field_metadata(name="Title Sort", datatype="text"),
            "cover": _field_metadata(name="Cover", datatype="text"),
            "series_index": _field_metadata(name="Series Index", datatype="float"),
            "not_a_field": _field_metadata(name="Not A Field", datatype="text", kind="category"),
            "no_datatype": _field_metadata(name="No Datatype", datatype=None),
        }
        self.values = {
            "custom_bool": None,
            "series_index": 2,
            "tags": ["zeta", "alpha"],
        }
        self.formatted = {
            "series": ("Series", "Saga"),
            "pubdate": ("Published", "2024-01-02"),
            "tags": ("Tags", "alpha, zeta"),
            "status": ("Status", "Read"),
            "title_sort": ("Title Sort", "Sorted Title"),
        }

    def all_field_keys(self) -> list[str]:
        return [
            "tags",
            "title_sort",
            "cover",
            "series_index",
            "not_a_field",
            "no_datatype",
            "broken",
        ]

    def metadata_for_field(self, field: str) -> dict[str, object] | None:
        if field == "broken":
            raise RuntimeError("bad metadata")
        return self.field_metadata[field]

    def is_null(self, field: str) -> bool:
        return False

    def get(self, field: str, default: object = None) -> object:
        return self.values.get(field, default)

    def format_field(self, field: str) -> tuple[str, object]:
        if field in self.formatted:
            return self.formatted[field]
        return field, getattr(self, field)


def test_calibre_metadata_renderer_matches_compat_method() -> None:
    metadata = calibreMetadata("Renderer Book", ["Author One"])
    metadata.publisher = "Publisher"
    metadata.tags = ["tag two", "tag one"]
    metadata.series = "Saga"
    metadata.series_index = 2

    rendered = calibre_metadata_to_html(metadata)

    assert rendered == metadata.to_html()
    assert "Renderer Book" in rendered
    assert "Author One" in rendered
    assert "Saga #2" in rendered


def test_metadata_book_render_delegates_to_surface_renderer(monkeypatch) -> None:
    calls = {}

    def fake_renderer(*args, **kwargs):
        calls["args"] = args
        calls["kwargs"] = kwargs
        return "<table>delegated</table>", ["comment"]

    monkeypatch.setattr(
        "LiuXin_alpha.surfaces.renderers.calibre_metadata.mi_to_html",
        fake_renderer,
    )

    sentinel = object()
    result = legacy_render.mi_to_html(
        sentinel,
        field_list=[("title", True)],
        default_author_link="search-calibre",
        use_roman_numbers=False,
        rating_font="Test Font",
    )

    assert result == ("<table>delegated</table>", ["comment"])
    assert calls["args"] == (sentinel,)
    assert calls["kwargs"] == {
        "field_list": [("title", True)],
        "default_author_link": "search-calibre",
        "use_roman_numbers": False,
        "rating_font": "Test Font",
    }


def test_search_href_hex_encodes_text_for_calibre_search_links() -> None:
    href = search_href("authors", 'Author "Quoted"')

    assert href.startswith("search:")
    assert "617574686f7273" in href
    assert isinstance(href, str)


def test_displayable_field_keys_filters_hidden_and_orders_default_fields(monkeypatch) -> None:
    metadata = _RendererMetadata()
    monkeypatch.setattr(renderer, "sort_key", lambda value: str(value).casefold())

    assert list(displayable_field_keys(metadata)) == ["tags", "title_sort"]
    assert list(get_field_list(metadata)) == [("title_sort", True), ("tags", True)]
    assert field_sort(metadata, "title") == (0, None)
    assert field_sort(metadata, "broken")[0] == 10000


def test_mi_to_html_renders_calibre_field_types(monkeypatch) -> None:
    metadata = _RendererMetadata()
    monkeypatch.setattr(renderer, "sort_key", lambda value: str(value).casefold())
    monkeypatch.setattr(
        renderer,
        "calibre_langcode_to_name",
        lambda language: {"eng": "English", "fra": "French"}.get(language),
    )
    monkeypatch.setattr(
        renderer,
        "urls_from_identifiers",
        lambda identifiers: [
            (
                "ISBN",
                "isbn",
                identifiers["isbn"],
                "https://www.worldcat.org/isbn/" + identifiers["isbn"],
            ),
            (
                "DOI",
                "doi",
                identifiers["doi"],
                "https://dx.doi.org/" + identifiers["doi"],
            ),
        ],
    )

    html, comment_fields = mi_to_html(
        metadata,
        field_list=[
            ("broken", True),
            ("empty", True),
            ("custom_bool", True),
            ("comments", True),
            ("rating", True),
            ("composite_html", True),
            ("composite_single", True),
            ("composite_multiple", True),
            ("path", True),
            ("formats", True),
            ("identifiers", True),
            ("authors", True),
            ("languages", True),
            ("series", True),
            ("pubdate", True),
            ("tags", True),
            ("status", True),
            ("sort", True),
            ("rating", False),
        ],
        default_author_link="search-calibre",
        rating_font="Test Serif",
    )

    assert html.startswith('<table class="fields">')
    assert "datatype_rating" in html
    assert "Test Serif" in html
    assert "Single Composite" in html
    assert "Solo Value" in html
    assert "Multiple Composite" in html
    assert "One" in html
    assert "Two" in html
    assert 'href="path:42"' in html
    assert "format:42:EPUB" in html
    assert "format:42:PDF" in html
    assert "worldcat.org/isbn/9780000000001" in html
    assert "dx.doi.org/10.1234/demo" in html
    assert "https://authors.example/linked" in html
    assert 'calibre-data="authors"' in html
    assert "English" in html
    assert "French" in html
    assert "series_name" in html
    assert "2024-01-02" in html
    assert "device_collections" in html
    assert "alpha, zeta" in html
    assert "Sorted Title" in html
    assert html.index("alpha") < html.index("zeta")
    assert len(comment_fields) == 1
    assert "Plain comment" in comment_fields[0]


def test_mi_to_html_renders_fallbacks_and_empty_values(monkeypatch) -> None:
    class FallbackMetadata(_RendererMetadata):
        authors = ["Template Author", "Plain Author"]
        author_link_map = {"Template Author": "", "Plain Author": ""}
        author_sort_map: dict[str, str] = {}
        languages: list[str] = []
        series = "Fallback Saga"
        pubdate = datetime(101, 1, 1, tzinfo=timezone.utc)

        def __init__(self) -> None:
            super().__init__()
            self.field_metadata["series"] = _field_metadata(
                name="Series",
                datatype="series",
                search_terms=[],
            )
            self.field_metadata["tags"] = _field_metadata(
                name="Tags",
                datatype="text",
                is_multiple={"list_to_ui": ", "},
                search_terms=[],
            )
            self.field_metadata["status"] = _field_metadata(
                name="Status",
                datatype="enumeration",
                search_terms=[],
            )
            self.field_metadata["subtitle"] = _field_metadata(
                name="Subtitle",
                datatype="text",
            )
            self.values["series_index"] = None
            self.formatted["subtitle"] = ("Subtitle", None)

    metadata = FallbackMetadata()
    monkeypatch.setattr(renderer, "sort_key", lambda value: str(value).casefold())

    template_html, _ = mi_to_html(
        metadata,
        field_list=[
            ("authors", True),
            ("languages", True),
            ("series", True),
            ("pubdate", True),
            ("tags", True),
            ("status", True),
            ("subtitle", True),
        ],
        default_author_link="https://authors.example/{author_sort}/{author}",
    )
    plain_html, _ = mi_to_html(metadata, field_list=[("authors", True)])
    generated_html, _ = mi_to_html(metadata)

    assert "https://authors.example/Template+Author/Template+Author" in template_html
    assert "Fallback Saga" in template_html
    assert "Languages" not in template_html
    assert "101-01-01" not in template_html
    assert "Subtitle" not in template_html
    assert "Plain Author" in plain_html
    assert "calibre-data" not in plain_html
    assert "Sorted Title" in generated_html


def test_mi_to_html_renders_device_path_and_skips_formats() -> None:
    class DeviceMetadata(_RendererMetadata):
        def __init__(self) -> None:
            super().__init__()
            del self.id
            self.path = "mtp:::device:::Books/File.epub"
            self.device_collections = []

    html, comment_fields = mi_to_html(
        DeviceMetadata(),
        field_list=[("path", True), ("formats", True)],
    )

    assert comment_fields == []
    assert 'href="devpath:mtp:::device:::Books/File.epub"' in html
    assert "Books/File.epub" in html
    assert "format:" not in html


def test_calibre_metadata_to_html_renders_optional_dates_rights_and_custom_fields() -> None:
    class Metadata:
        title = "Optional Book"
        authors = ["One Author"]
        publisher = "Publisher"
        book_producer = "Producer"
        comments = "Comment"
        isbn = "1234567890"
        tags = ["tag"]
        series = None
        languages = ["eng"]
        timestamp = datetime(2024, 1, 2, 3, 4, tzinfo=timezone.utc)
        pubdate = datetime(2023, 5, 6, tzinfo=timezone.utc)
        rights = "Public domain"

        def custom_field_keys(self) -> list[str]:
            return ["#custom", "#empty"]

        def get(self, key: str, default: object = None) -> object:
            return {"#custom": "Custom Value"}.get(key, default)

        def format_field(self, key: str) -> tuple[str, str]:
            return ("Custom Field", "Custom Value")

    html = calibre_metadata_to_html(Metadata())

    assert "Optional Book" in html
    assert "2024-01-02" in html
    assert "2023-05-06" in html
    assert "Public domain" in html
    assert "Custom Field" in html
    assert "Custom Value" in html

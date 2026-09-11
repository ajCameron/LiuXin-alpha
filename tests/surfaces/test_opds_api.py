"""
Exercise OPDS root, search, and category acquisition routes with deterministic in-memory host responses.

The host asserts expected selectors and supplies fixed metadata; no database,
network server, raster renderer, or downloadable publication is needed.
"""

from __future__ import annotations

from dataclasses import dataclass

from LiuXin_alpha.surfaces.opds.api import OpdsApi, encode_compat_token, opds_item_token, opds_nav_token
from LiuXin_alpha.surfaces.web_readonly.app import _Response


@dataclass(frozen=True)
class _DummyConfig:
    """
    Supply immutable title, pagination, and grouping settings for the OPDS host double.

    Example:
        >>> _DummyConfig().default_page_size
        10
    """

    title: str = "Dummy OPDS"
    default_page_size: int = 10
    max_page_size: int = 25
    opds_max_ungrouped_items: int = 100


class _DummyHost:
    """
    Provide a single fixed work and author category through the host methods consumed by OpdsApi.

    Example:
        >>> _DummyHost().opds_category_rows("authors")[0]["label"]
        'Alice'
    """

    def __init__(self) -> None:
        """
        Initialize fixed configuration and a shared one-work mapping without external resources.

        Example:
            >>> _DummyHost().work_row
            {'id': 1}


        :return: None after storing the configuration and work identity fixture.
        """
        self.config = _DummyConfig()
        self.work_row = {"id": 1}

    def opds_xml_response(self, xml_text: str, *, status: str = "200 OK") -> _Response:
        """
        Encode XML text into a one-chunk Atom response without parsing or validating the document.

        Example:
            >>> _DummyHost().opds_xml_response("<feed/>").body
            [b'<feed/>']


        :param xml_text: Rendered XML text encoded as UTF-8.
        :param status: Response status retained unchanged, defaulting to 200 OK.
        :return: Response with the Atom/UTF-8 Content-Type and a single byte chunk.
        """
        return _Response(status=status, headers=[("Content-Type", "application/atom+xml; charset=utf-8")], body=[xml_text.encode("utf-8")])

    def opds_text_response(self, status: str, text: str, *, content_type: str) -> _Response:
        """
        Encode a text/error response using the requested status and content type.

        Example:
            >>> _DummyHost().opds_text_response("404 Not Found", "missing", content_type="text/plain").status
            '404 Not Found'


        :param status: HTTP status text retained unchanged.
        :param text: Response text encoded as UTF-8.
        :param content_type: Media-type header value supplied by the route under test.
        :return: One-chunk response without header/body validation.
        """
        return _Response(status=status, headers=[("Content-Type", content_type)], body=[text.encode("utf-8")])

    def opds_search_work_rows(self, query_text: str) -> list[object]:
        """
        Require the fixture search term and return the shared work in a new list.

        Example:
            >>> _DummyHost().opds_search_work_rows("Dummy")
            [{'id': 1}]


        :param query_text: Exact Dummy query expected by this route fixture.
        :return: Single-work list; unexpected queries fail an assertion.
        """
        assert query_text == "Dummy"
        return [self.work_row]

    def opds_work_rows(self, *, sorted_by: str) -> list[object]:
        """
        Accept the supported title/recent sort tokens and return the fixed one-work list.

        Example:
            >>> _DummyHost().opds_work_rows(sorted_by="recent")
            [{'id': 1}]


        :param sorted_by: Required title or recent ordering selector, not an actual sort operation here.
        :return: New list containing the shared work mapping.
        """
        assert sorted_by in {"title", "recent"}
        return [self.work_row]

    def opds_category_rows(self, category: str) -> list[dict[str, object]]:
        """
        Require authors and supply one Alice category item linked to one title.

        Example:
            >>> _DummyHost().opds_category_rows("authors")
            [{'id': 1, 'label': 'Alice', 'count': 1}]


        :param category: Exact authors selector required by this test host.
        :return: Fresh single-item category list with fixed ID, label, and count.
        """
        assert category == "authors"
        return [{"id": 1, "label": "Alice", "count": 1}]

    def opds_category_display_name(self, category: str) -> str:
        """
        Title-case the stringified category without using production aliases or localization.

        Example:
            >>> _DummyHost().opds_category_display_name("authors")
            'Authors'


        :param category: Category value stringified before title casing.
        :return: Simple fixture display name.
        """
        return str(category).title()

    def opds_rows_for_category_item(self, category: str, item_token: str) -> list[object]:
        """
        Assert the decoded authors/one item selection and return the fixture work.

        Example:
            >>> _DummyHost().opds_rows_for_category_item("authors", "1")
            [{'id': 1}]


        :param category: Expected normalized authors key.
        :param item_token: Expected decoded item text '1', not an integer.
        :return: New list containing the original shared work mapping.
        """
        assert category == "authors"
        assert item_token == "1"
        return [self.work_row]

    def opds_work_metadata_payload(self, row) -> dict[str, object]:
        """
        Require the fixture work and supply the full metadata shape needed for one EPUB acquisition entry.

        Example:
            >>> host = _DummyHost()
            >>> host.opds_work_metadata_payload(host.work_row)["title"]
            'Dummy Book'


        :param row: Work mapping required to compare equal to this host's work_row.
        :return: Fresh fixed metadata with Alice, one tag, image routes, and an EPUB route/declared size.
        """
        assert row == self.work_row
        return {
            "id": 1,
            "uuid": "dummy-uuid",
            "title": "Dummy Book",
            "authors": ["Alice"],
            "summary": "Dummy summary",
            "tags": ["Tag"],
            "cover": "/get/cover/1/main",
            "thumbnail": "/get/thumb/1/main?sz=60x80",
            "formats_detail": [{"format": "EPUB", "download_url": "/get/epub/1/main"}],
            "format_metadata": {"EPUB": {"size": 123}},
        }


def _decode_response(response: _Response) -> tuple[str, dict[str, str], str]:
    """
    Consume response chunks as UTF-8 text and collapse headers into a dict for assertions.

    Example:
        >>> _decode_response(_DummyHost().opds_xml_response("<feed/>"))[2]
        '<feed/>'


    :param response: In-memory response whose chunks are joined without invoking close.
    :return: Status, last-value header dict, and decoded body text; decode errors propagate.
    """
    body = b"".join(response.body).decode("utf-8")
    return response.status, dict(response.headers), body


def test_opds_api_root_and_search_routes() -> None:
    """
    Verify root navigation and hex-token search produce Atom responses with the expected work/acquisition link.

    Example:
        >>> test_opds_api_root_and_search_routes()


    :return: None after response status/type, root title/navigation, and search-entry assertions.
    """
    api = OpdsApi(_DummyHost())

    status, headers, body = _decode_response(api.serve("/opds", {}))
    assert status == "200 OK"
    assert headers["Content-Type"].startswith("application/atom+xml")
    assert "<title>Dummy OPDS</title>" in body
    assert "/opds/navcatalog/{}".format(opds_nav_token("authors")) in body

    status, headers, body = _decode_response(api.serve("/opds/search/{}".format(encode_compat_token("Dummy")), {}))
    assert status == "200 OK"
    assert "<title>Dummy Book</title>" in body
    assert "/get/epub/1/main" in body


def test_opds_api_category_acquisition_route() -> None:
    """
    Decode an authors navigation/item route and render its title and EPUB acquisition link.

    Example:
        >>> test_opds_api_category_acquisition_route()


    :return: None after successful Atom response, decoded category/item title, and format-link assertions.
    """
    api = OpdsApi(_DummyHost())
    path = "/opds/category/{}/{}".format(opds_nav_token("authors"), opds_item_token(category="authors", item_id=1))
    status, headers, body = _decode_response(api.serve(path, {}))
    assert status == "200 OK"
    assert headers["Content-Type"].startswith("application/atom+xml")
    assert "<title>Authors 1</title>" in body
    assert "/get/epub/1/main" in body

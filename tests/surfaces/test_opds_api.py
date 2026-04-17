from __future__ import annotations

from dataclasses import dataclass

from LiuXin_alpha.surfaces.opds.api import OpdsApi, encode_compat_token, opds_item_token, opds_nav_token
from LiuXin_alpha.surfaces.web_readonly.app import _Response


@dataclass(frozen=True)
class _DummyConfig:
    title: str = "Dummy OPDS"
    default_page_size: int = 10
    max_page_size: int = 25
    opds_max_ungrouped_items: int = 100


class _DummyHost:
    def __init__(self) -> None:
        self.config = _DummyConfig()
        self.work_row = {"id": 1}

    def opds_xml_response(self, xml_text: str, *, status: str = "200 OK") -> _Response:
        return _Response(status=status, headers=[("Content-Type", "application/atom+xml; charset=utf-8")], body=[xml_text.encode("utf-8")])

    def opds_text_response(self, status: str, text: str, *, content_type: str) -> _Response:
        return _Response(status=status, headers=[("Content-Type", content_type)], body=[text.encode("utf-8")])

    def opds_search_work_rows(self, query_text: str) -> list[object]:
        assert query_text == "Dummy"
        return [self.work_row]

    def opds_work_rows(self, *, sorted_by: str) -> list[object]:
        assert sorted_by in {"title", "recent"}
        return [self.work_row]

    def opds_category_rows(self, category: str) -> list[dict[str, object]]:
        assert category == "authors"
        return [{"id": 1, "label": "Alice", "count": 1}]

    def opds_category_display_name(self, category: str) -> str:
        return str(category).title()

    def opds_rows_for_category_item(self, category: str, item_token: str) -> list[object]:
        assert category == "authors"
        assert item_token == "1"
        return [self.work_row]

    def opds_work_metadata_payload(self, row) -> dict[str, object]:
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
    body = b"".join(response.body).decode("utf-8")
    return response.status, dict(response.headers), body


def test_opds_api_root_and_search_routes() -> None:
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
    api = OpdsApi(_DummyHost())
    path = "/opds/category/{}/{}".format(opds_nav_token("authors"), opds_item_token(category="authors", item_id=1))
    status, headers, body = _decode_response(api.serve(path, {}))
    assert status == "200 OK"
    assert headers["Content-Type"].startswith("application/atom+xml")
    assert "<title>Authors 1</title>" in body
    assert "/get/epub/1/main" in body

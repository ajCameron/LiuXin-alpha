from __future__ import annotations

from types import SimpleNamespace

from LiuXin_alpha.file_formats.oeb.polish.check.links import (
    BadDestinationType,
    check_link_destination,
    check_link_destinations,
    check_links,
)
from LiuXin_alpha.file_formats.oeb.polish.cover import (
    find_cover_image,
    find_cover_image_in_page,
    get_azw3_raster_cover_name,
    remove_cover_image_in_page,
    set_epub_cover,
)
from LiuXin_alpha.utils.libraries.liuxin_etree import etree


def _xhtml(body_inner: str) -> etree._Element:
    raw = (
        '<html xmlns="http://www.w3.org/1999/xhtml"><body>'
        + body_inner
        + "</body></html>"
    )
    return etree.fromstring(raw.encode("utf-8"))


def test_get_azw3_raster_cover_name_handles_invalid_href() -> None:
    class _Container:
        opf_name = "content.opf"

        def opf_xpath(self, expr):
            return [SimpleNamespace(get=lambda key: "C:/outside.xhtml")]

        def href_to_name(self, href, base=None):
            raise ValueError("invalid absolute path")

    assert get_azw3_raster_cover_name(_Container()) is None


def test_find_cover_image_ignores_unstatable_paths() -> None:
    class _Container:
        manifest_id_map = {}
        mime_map = {"images/cover.jpg": "image/jpeg"}
        guide_type_map = {"other.ms-coverimage": "images/cover.jpg"}
        name_path_map = {"images/cover.jpg": "/definitely/not/real.jpg"}

        def opf_xpath(self, expr):
            return []

    assert find_cover_image(_Container(), strict=False) is None


def test_find_cover_image_in_page_skips_bad_links_and_finds_valid_one() -> None:
    root = _xhtml('<img src="C:/outside.jpg"/><img src="images/ok.jpg"/>')

    class _Container:
        def parsed(self, name):
            return root

        def href_to_name(self, href, base=None):
            if href.startswith("C:/"):
                raise ValueError("invalid absolute path")
            return "images/ok.jpg"

    assert find_cover_image_in_page(_Container(), "cover.xhtml") == "images/ok.jpg"


def test_remove_cover_image_in_page_ignores_invalid_href_without_crashing() -> None:
    root = _xhtml('<img src="C:/outside.jpg"/>')

    class _Container:
        def parsed(self, name):
            return root

        def href_to_name(self, href, base=None):
            raise ValueError("invalid absolute path")

    remove_cover_image_in_page(_Container(), "page.xhtml", {"images/ok.jpg"})
    assert len(root.xpath('//*[local-name()="img"]')) == 1


def test_set_epub_cover_handles_empty_spine_items(monkeypatch) -> None:
    import LiuXin_alpha.file_formats.oeb.polish.cover as cover_mod

    monkeypatch.setattr(cover_mod, "find_cover_image", lambda container: None)
    monkeypatch.setattr(cover_mod, "find_cover_page", lambda container: None)
    monkeypatch.setattr(cover_mod, "clean_opf", lambda container: iter(()))
    monkeypatch.setattr(
        cover_mod,
        "create_epub_cover",
        lambda container, cover_path, existing_image, options=None: ("images/new.jpg", "titlepage.xhtml"),
    )

    reports = []

    class _Container:
        spine_items = ()
        log = lambda *args, **kwargs: None

    set_epub_cover(_Container(), "/tmp/new-cover.jpg", reports.append, options={})
    assert any("Cover inserted" in x for x in reports)


def test_check_link_destination_handles_failed_target_parse() -> None:
    link_elem = etree.Element("a", href="dest.xhtml")

    class _Container:
        mime_map = {"dest.xhtml": "application/xhtml+xml"}

        def href_to_name(self, href, base=None):
            return "dest.xhtml"

        def parsed(self, name):
            raise RuntimeError("parse failed")

    errors = []
    check_link_destination(_Container(), {}, "index.xhtml", "dest.xhtml", link_elem, errors)
    assert len(errors) == 1
    assert isinstance(errors[0], BadDestinationType)


def test_check_links_skips_unreadable_iterlinks_sources() -> None:
    class _Container:
        mime_map = {"index.xhtml": "application/xhtml+xml"}
        spine_names = [("index.xhtml", True)]
        guide_type_map = {}
        manifest_id_map = {"id-1": "index.xhtml"}
        book_type = "epub"

        def iterlinks(self, name):
            raise RuntimeError("cannot parse links")

        def ok_to_be_unmanifested(self, name):
            return True

    assert check_links(_Container()) == []


def test_check_link_destinations_skips_unreadable_sources() -> None:
    class _Container:
        mime_map = {"index.xhtml": "application/xhtml+xml"}
        book_type = "epub"

        def parsed(self, name):
            raise RuntimeError("cannot parse")

        def opf_xpath(self, expr):
            return []

    assert check_link_destinations(_Container()) == []

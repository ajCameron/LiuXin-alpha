"""
Provide test oeb polish cover links regressions utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test oeb polish cover links regressions through a consuming regression::

        python -m pytest -q tests/file_formats/oeb/test_oeb_polish_cover_links_regressions.py
"""
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
    """
    Perform the xhtml operation under explicit file-format and conversion rules.

    Example:
        Exercise  xhtml through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_cover_links_regressions.py


    :param body_inner: Value supplied for body inner under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    raw = (
        '<html xmlns="http://www.w3.org/1999/xhtml"><body>'
        + body_inner
        + "</body></html>"
    )
    return etree.fromstring(raw.encode("utf-8"))


def test_get_azw3_raster_cover_name_handles_invalid_href() -> None:
    """
    Perform the test get azw3 raster cover name handles invalid href operation under explicit file-format and conversion rules.

    Example:
        Exercise test get azw3 raster cover name handles invalid href through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_cover_links_regressions.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    class _Container:
        """
        Provide the container contract for validated ebook processing.

        Example:
            Exercise test get azw3 raster cover name handles invalid href. Container through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_cover_links_regressions.py
        """
        opf_name = "content.opf"

        def opf_xpath(self, expr):
            """
            Perform the opf xpath operation under explicit file-format and conversion rules.

            Example:
                Exercise test get azw3 raster cover name handles invalid href. Container.opf xpath through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_polish_cover_links_regressions.py


            :param expr: Value supplied for expr under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return [SimpleNamespace(get=lambda key: "C:/outside.xhtml")]

        def href_to_name(self, href, base=None):
            """
            Perform the href to name operation under explicit file-format and conversion rules.

            Example:
                Exercise test get azw3 raster cover name handles invalid href. Container.href to name through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_polish_cover_links_regressions.py


            :param href: Value supplied for href under the utility contract.
            :param base: Value supplied for base under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            raise ValueError("invalid absolute path")

    assert get_azw3_raster_cover_name(_Container()) is None


def test_find_cover_image_ignores_unstatable_paths() -> None:
    """
    Perform the test find cover image ignores unstatable paths operation under explicit file-format and conversion rules.

    Example:
        Exercise test find cover image ignores unstatable paths through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_cover_links_regressions.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    class _Container:
        """
        Provide the container contract for validated ebook processing.

        Example:
            Exercise test find cover image ignores unstatable paths. Container through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_cover_links_regressions.py
        """
        manifest_id_map = {}
        mime_map = {"images/cover.jpg": "image/jpeg"}
        guide_type_map = {"other.ms-coverimage": "images/cover.jpg"}
        name_path_map = {"images/cover.jpg": "/definitely/not/real.jpg"}

        def opf_xpath(self, expr):
            """
            Perform the opf xpath operation under explicit file-format and conversion rules.

            Example:
                Exercise test find cover image ignores unstatable paths. Container.opf xpath through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_polish_cover_links_regressions.py


            :param expr: Value supplied for expr under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return []

    assert find_cover_image(_Container(), strict=False) is None


def test_find_cover_image_in_page_skips_bad_links_and_finds_valid_one() -> None:
    """
    Perform the test find cover image in page skips bad links and finds valid one operation under explicit file-format and conversion rules.

    Example:
        Exercise test find cover image in page skips bad links and finds valid one through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_cover_links_regressions.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    root = _xhtml('<img src="C:/outside.jpg"/><img src="images/ok.jpg"/>')

    class _Container:
        """
        Provide the container contract for validated ebook processing.

        Example:
            Exercise test find cover image in page skips bad links and finds valid one. Container through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_cover_links_regressions.py
        """
        def parsed(self, name):
            """
            Perform the parsed operation under explicit file-format and conversion rules.

            Example:
                Exercise test find cover image in page skips bad links and finds valid one. Container.parsed through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_polish_cover_links_regressions.py


            :param name: Field, file, function or resource name addressed by the operation.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return root

        def href_to_name(self, href, base=None):
            """
            Perform the href to name operation under explicit file-format and conversion rules.

            Example:
                Exercise test find cover image in page skips bad links and finds valid one. Container.href to name through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_polish_cover_links_regressions.py


            :param href: Value supplied for href under the utility contract.
            :param base: Value supplied for base under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            if href.startswith("C:/"):
                raise ValueError("invalid absolute path")
            return "images/ok.jpg"

    assert find_cover_image_in_page(_Container(), "cover.xhtml") == "images/ok.jpg"


def test_remove_cover_image_in_page_ignores_invalid_href_without_crashing() -> None:
    """
    Perform the test remove cover image in page ignores invalid href without crashing operation under explicit file-format and conversion rules.

    Example:
        Exercise test remove cover image in page ignores invalid href without crashing through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_cover_links_regressions.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    root = _xhtml('<img src="C:/outside.jpg"/>')

    class _Container:
        """
        Provide the container contract for validated ebook processing.

        Example:
            Exercise test remove cover image in page ignores invalid href without crashing. Container through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_cover_links_regressions.py
        """
        def parsed(self, name):
            """
            Perform the parsed operation under explicit file-format and conversion rules.

            Example:
                Exercise test remove cover image in page ignores invalid href without crashing. Container.parsed through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_polish_cover_links_regressions.py


            :param name: Field, file, function or resource name addressed by the operation.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return root

        def href_to_name(self, href, base=None):
            """
            Perform the href to name operation under explicit file-format and conversion rules.

            Example:
                Exercise test remove cover image in page ignores invalid href without crashing. Container.href to name through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_polish_cover_links_regressions.py


            :param href: Value supplied for href under the utility contract.
            :param base: Value supplied for base under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            raise ValueError("invalid absolute path")

    remove_cover_image_in_page(_Container(), "page.xhtml", {"images/ok.jpg"})
    assert len(root.xpath('//*[local-name()="img"]')) == 1


def test_set_epub_cover_handles_empty_spine_items(monkeypatch) -> None:
    """
    Perform the test set epub cover handles empty spine items operation under explicit file-format and conversion rules.

    Example:
        Exercise test set epub cover handles empty spine items through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_cover_links_regressions.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
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
        """
        Provide the container contract for validated ebook processing.

        Example:
            Exercise test set epub cover handles empty spine items. Container through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_cover_links_regressions.py
        """
        spine_items = ()
        log = lambda *args, **kwargs: None

    set_epub_cover(_Container(), "/tmp/new-cover.jpg", reports.append, options={})
    assert any("Cover inserted" in x for x in reports)


def test_check_link_destination_handles_failed_target_parse() -> None:
    """
    Perform the test check link destination handles failed target parse operation under explicit file-format and conversion rules.

    Example:
        Exercise test check link destination handles failed target parse through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_cover_links_regressions.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    link_elem = etree.Element("a", href="dest.xhtml")

    class _Container:
        """
        Provide the container contract for validated ebook processing.

        Example:
            Exercise test check link destination handles failed target parse. Container through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_cover_links_regressions.py
        """
        mime_map = {"dest.xhtml": "application/xhtml+xml"}

        def href_to_name(self, href, base=None):
            """
            Perform the href to name operation under explicit file-format and conversion rules.

            Example:
                Exercise test check link destination handles failed target parse. Container.href to name through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_polish_cover_links_regressions.py


            :param href: Value supplied for href under the utility contract.
            :param base: Value supplied for base under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return "dest.xhtml"

        def parsed(self, name):
            """
            Perform the parsed operation under explicit file-format and conversion rules.

            Example:
                Exercise test check link destination handles failed target parse. Container.parsed through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_polish_cover_links_regressions.py


            :param name: Field, file, function or resource name addressed by the operation.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            raise RuntimeError("parse failed")

    errors = []
    check_link_destination(_Container(), {}, "index.xhtml", "dest.xhtml", link_elem, errors)
    assert len(errors) == 1
    assert isinstance(errors[0], BadDestinationType)


def test_check_links_skips_unreadable_iterlinks_sources() -> None:
    """
    Perform the test check links skips unreadable iterlinks sources operation under explicit file-format and conversion rules.

    Example:
        Exercise test check links skips unreadable iterlinks sources through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_cover_links_regressions.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    class _Container:
        """
        Provide the container contract for validated ebook processing.

        Example:
            Exercise test check links skips unreadable iterlinks sources. Container through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_cover_links_regressions.py
        """
        mime_map = {"index.xhtml": "application/xhtml+xml"}
        spine_names = [("index.xhtml", True)]
        guide_type_map = {}
        manifest_id_map = {"id-1": "index.xhtml"}
        book_type = "epub"

        def iterlinks(self, name):
            """
            Perform the iterlinks operation under explicit file-format and conversion rules.

            Example:
                Exercise test check links skips unreadable iterlinks sources. Container.iterlinks through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_polish_cover_links_regressions.py


            :param name: Field, file, function or resource name addressed by the operation.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            raise RuntimeError("cannot parse links")

        def ok_to_be_unmanifested(self, name):
            """
            Perform the ok to be unmanifested operation under explicit file-format and conversion rules.

            Example:
                Exercise test check links skips unreadable iterlinks sources. Container.ok to be unmanifested through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_polish_cover_links_regressions.py


            :param name: Field, file, function or resource name addressed by the operation.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return True

    assert check_links(_Container()) == []


def test_check_link_destinations_skips_unreadable_sources() -> None:
    """
    Perform the test check link destinations skips unreadable sources operation under explicit file-format and conversion rules.

    Example:
        Exercise test check link destinations skips unreadable sources through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_cover_links_regressions.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    class _Container:
        """
        Provide the container contract for validated ebook processing.

        Example:
            Exercise test check link destinations skips unreadable sources. Container through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_cover_links_regressions.py
        """
        mime_map = {"index.xhtml": "application/xhtml+xml"}
        book_type = "epub"

        def parsed(self, name):
            """
            Perform the parsed operation under explicit file-format and conversion rules.

            Example:
                Exercise test check link destinations skips unreadable sources. Container.parsed through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_polish_cover_links_regressions.py


            :param name: Field, file, function or resource name addressed by the operation.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            raise RuntimeError("cannot parse")

        def opf_xpath(self, expr):
            """
            Perform the opf xpath operation under explicit file-format and conversion rules.

            Example:
                Exercise test check link destinations skips unreadable sources. Container.opf xpath through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_polish_cover_links_regressions.py


            :param expr: Value supplied for expr under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return []

    assert check_link_destinations(_Container()) == []

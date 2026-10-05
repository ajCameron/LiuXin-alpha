"""
Provide test lit parser framework utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test lit parser framework through a consuming regression::

        python -m pytest -q tests/file_formats/lit/test_lit_parser_framework.py
"""
from __future__ import annotations

from LiuXin_alpha.utils.libraries.liuxin_etree import etree

from tests.support.file_format_lit import (
    LitManifestRecord,
    build_lit_manifest_payload,
    build_lit_namelist_payload,
    lit_binary_element,
    lit_internal_href,
    lit_manifest_item,
    read_manifest_from_payload,
    read_namelist_from_payload,
    render_unbinary_html,
)
from tests.support.file_format_unicode import (
    COMMON_TEXT_FRAGMENTS,
    MULTISCRIPT_TEXT,
    assert_fragments_present,
    assert_no_replacement_chars,
)


def test_lit_manifest_fixture_builder_parses_unicode_paths_and_states() -> None:
    """
    Perform the test lit manifest fixture builder parses unicode paths and states operation under explicit file-format and conversion rules.

    Example:
        Exercise test lit manifest fixture builder parses unicode paths and states through a consuming regression::

            python -m pytest -q tests/file_formats/lit/test_lit_parser_framework.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    payload = build_lit_manifest_payload(
        (
            LitManifestRecord(
                "chapter_世界",
                "OPS/chapters/Καλημέρα 世界.xhtml",
                "application/xhtml+xml",
                offset=17,
                state="spine",
            ),
            LitManifestRecord(
                "style_基本",
                "OPS/styles/基本.css",
                "text/css",
                state="css",
            ),
            LitManifestRecord(
                "cover_画像",
                "OPS/images/表紙.png",
                "image/png",
                state="images",
            ),
        )
    )

    lit = read_manifest_from_payload(payload, opf_path="book.opf")

    chapter = lit.manifest["chapter_世界"]
    assert chapter.root == "\\"
    assert chapter.offset == 17
    assert chapter.mime_type == "application/xhtml+xml"
    assert chapter.state == "spine"
    assert chapter.path == "chapters/Καλημέρα 世界.xhtml"
    assert lit.paths["chapters/Καλημέρα 世界.xhtml"] is chapter
    assert lit.paths["styles/基本.css"] is lit.manifest["style_基本"]
    assert lit.paths["images/表紙.png"] is lit.manifest["cover_画像"]
    assert lit.paths["book.opf"] is None


def test_lit_namelist_fixture_builder_feeds_reader_section_names() -> None:
    """
    Perform the test lit namelist fixture builder feeds reader section names operation under explicit file-format and conversion rules.

    Example:
        Exercise test lit namelist fixture builder feeds reader section names through a consuming regression::

            python -m pytest -q tests/file_formats/lit/test_lit_parser_framework.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    lit = read_namelist_from_payload(
        build_lit_namelist_payload(
            (
                "Uncompressed",
                "MSCompressed",
                "Καλημέρα",
                "世界",
            )
        )
    )

    assert lit.section_names == ["Uncompressed", "MSCompressed", "Καλημέρα", "世界"]
    assert lit.section_data == [None, None, None, None]


def test_lit_unbinary_fixture_roundtrips_multiscript_text_and_attrs() -> None:
    """
    Perform the test lit unbinary fixture roundtrips multiscript text and attrs operation under explicit file-format and conversion rules.

    Example:
        Exercise test lit unbinary fixture roundtrips multiscript text and attrs through a consuming regression::

            python -m pytest -q tests/file_formats/lit/test_lit_parser_framework.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    body_text = MULTISCRIPT_TEXT + "\nReserved 5 < 6 & café"
    rendered = render_unbinary_html(
        lit_binary_element("p", body_text, attrs={"id": "intro_世界"})
    )
    root = etree.fromstring(rendered.encode("utf-8"))
    text = "".join(root.itertext())

    assert root.tag == "p"
    assert root.get("id") == "intro_世界"
    assert "Reserved 5 < 6 & café" in text
    assert_fragments_present(text, COMMON_TEXT_FRAGMENTS, context="LIT UnBinary fixture")
    assert_no_replacement_chars(text, context="LIT UnBinary fixture")


def test_lit_unbinary_fixture_resolves_internal_hrefs_against_manifest() -> None:
    """
    Perform the test lit unbinary fixture resolves internal hrefs against manifest operation under explicit file-format and conversion rules.

    Example:
        Exercise test lit unbinary fixture resolves internal hrefs against manifest through a consuming regression::

            python -m pytest -q tests/file_formats/lit/test_lit_parser_framework.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    target = lit_manifest_item(
        internal="chapter_2",
        original="OPS/chapters/δεύτερο.xhtml",
    )
    rendered = render_unbinary_html(
        lit_binary_element(
            "a",
            "Next Καλημέρα",
            attrs={"href": lit_internal_href("chapter_2#section")},
        ),
        path="OPS/current.xhtml",
        manifest={"chapter_2": target},
    )
    root = etree.fromstring(rendered.encode("utf-8"))

    assert root.get("href") == "chapters/δεύτερο.xhtml#section"
    assert "".join(root.itertext()) == "Next Καλημέρα"

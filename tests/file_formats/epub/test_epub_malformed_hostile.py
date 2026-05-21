from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from tests.support.file_format_epub import NullLog, build_unicode_epub, rewrite_epub_zip


def _assert_epub_input_rejects_without_partial_output(
    monkeypatch,
    archive: Path,
    workdir: Path,
    match: str,
) -> None:
    from LiuXin_alpha.file_formats.conversion.plugins.epub_input import EPUBInput

    workdir.mkdir()
    monkeypatch.chdir(workdir)

    with archive.open("rb") as stream:
        with pytest.raises(ValueError, match=match):
            EPUBInput(None).convert(stream, SimpleNamespace(), "epub", NullLog(), {})

    assert not (workdir / "content.opf").exists()
    assert not (workdir / "OPS").exists()
    assert not (workdir / "META-INF").exists()


def _container_xml(opf_path: str = "OPS/content.opf", media_type: str = "application/oebps-package+xml") -> bytes:
    media_attr = f' media-type="{media_type}"' if media_type else ""
    return (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container">'
        "<rootfiles>"
        f'<rootfile full-path="{opf_path}"{media_attr}/>'
        "</rootfiles>"
        "</container>"
    ).encode("utf-8")


def _opf_without_manifest() -> bytes:
    return (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<package xmlns="http://www.idpf.org/2007/opf" version="2.0">'
        "<metadata/>"
        '<spine><itemref idref="chapter"/></spine>'
        "</package>"
    ).encode("utf-8")


def _opf_without_spine() -> bytes:
    return (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<package xmlns="http://www.idpf.org/2007/opf" version="2.0">'
        "<metadata/>"
        '<manifest><item id="chapter" href="text/chapter.xhtml" media-type="application/xhtml+xml"/></manifest>'
        "</package>"
    ).encode("utf-8")


def _opf_without_manifest_items() -> bytes:
    return (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<package xmlns="http://www.idpf.org/2007/opf" version="2.0">'
        "<metadata/>"
        "<manifest/>"
        '<spine><itemref idref="chapter"/></spine>'
        "</package>"
    ).encode("utf-8")


def _opf_without_spine_itemrefs() -> bytes:
    return (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<package xmlns="http://www.idpf.org/2007/opf" version="2.0">'
        "<metadata/>"
        '<manifest><item id="chapter" href="text/chapter.xhtml" media-type="application/xhtml+xml"/></manifest>'
        "<spine/>"
        "</package>"
    ).encode("utf-8")


@pytest.mark.parametrize(
    ("case_id", "remove", "replace", "match"),
    (
        ("missing_mimetype", ("mimetype",), {}, "missing required member"),
        ("invalid_mimetype", (), {"mimetype": b"text/plain"}, "invalid mimetype"),
        ("missing_container_xml", ("META-INF/container.xml",), {}, "missing required member"),
        ("malformed_container_xml", (), {"META-INF/container.xml": b"<container"}, "malformed META-INF/container.xml"),
        (
            "container_without_opf_rootfile",
            (),
            {"META-INF/container.xml": _container_xml(media_type="text/plain")},
            "does not reference an OPF package",
        ),
        (
            "container_missing_opf_package",
            (),
            {"META-INF/container.xml": _container_xml(opf_path="OPS/missing.opf")},
            "missing OPF package",
        ),
        (
            "malformed_opf_package",
            (),
            {"OPS/content.opf": b"<?xml version='1.0'?><package><metadata>"},
            "malformed OPF package",
        ),
        (
            "opf_wrong_root",
            (),
            {"OPS/content.opf": b"<?xml version='1.0'?><not-package/>"},
            "root is not <package>",
        ),
        (
            "opf_missing_manifest",
            (),
            {"OPS/content.opf": _opf_without_manifest()},
            "missing manifest",
        ),
        (
            "opf_missing_manifest_items",
            (),
            {"OPS/content.opf": _opf_without_manifest_items()},
            "missing manifest items",
        ),
        (
            "opf_missing_spine",
            (),
            {"OPS/content.opf": _opf_without_spine()},
            "missing spine",
        ),
        (
            "opf_missing_spine_itemrefs",
            (),
            {"OPS/content.opf": _opf_without_spine_itemrefs()},
            "missing spine itemrefs",
        ),
    ),
)
def test_epub_input_rejects_malformed_container_before_extraction(
    tmp_path: Path,
    monkeypatch,
    case_id: str,
    remove: tuple[str, ...],
    replace: dict[str, bytes],
    match: str,
) -> None:
    base = build_unicode_epub(tmp_path / "base.epub")
    hostile = tmp_path / f"{case_id}.epub"
    rewrite_epub_zip(base.path, hostile, remove=remove, replace=replace)

    _assert_epub_input_rejects_without_partial_output(
        monkeypatch,
        hostile,
        tmp_path / f"{case_id}_work",
        match,
    )


def test_epub_input_rejects_non_zip_payload_before_extraction(tmp_path: Path, monkeypatch) -> None:
    hostile = tmp_path / "not_an_epub.epub"
    hostile.write_bytes("not an EPUB zip: Καλημέρα".encode("utf-8"))

    _assert_epub_input_rejects_without_partial_output(
        monkeypatch,
        hostile,
        tmp_path / "not_zip_work",
        "invalid ZIP",
    )

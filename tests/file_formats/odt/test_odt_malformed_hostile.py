from __future__ import annotations

import io
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests.support.file_format_odt import NullLog, build_unicode_odt, rewrite_odt_zip


@pytest.mark.parametrize(
    ("case_id", "remove", "replace"),
    (
        ("missing_meta_xml", ("meta.xml",), {}),
        ("missing_manifest", ("META-INF/manifest.xml",), {}),
        ("missing_content_xml", ("content.xml",), {}),
        (
            "malformed_content_xml",
            (),
            {"content.xml": "<?xml version='1.0'?><broken><text>Καλημέρα".encode("utf-8")},
        ),
        ("invalid_utf8_content_xml", (), {"content.xml": b"<?xml version='1.0' encoding='UTF-8'?><root>\xff</root>"}),
    ),
)
def test_odt_extract_rejects_malformed_container_members(
    tmp_path: Path,
    case_id: str,
    remove: tuple[str, ...],
    replace: dict[str, bytes],
) -> None:
    from LiuXin_alpha.file_formats.odt.input import Extract

    base = build_unicode_odt(tmp_path / "base.odt")
    hostile = tmp_path / f"{case_id}.odt"
    rewrite_odt_zip(base.path, hostile, remove=remove, replace=replace)
    out_dir = tmp_path / f"{case_id}_out"

    with hostile.open("rb") as stream:
        with pytest.raises(Exception):
            Extract()(stream, str(out_dir), NullLog())

    assert not (out_dir / "index.xhtml").exists()
    assert not (out_dir / "metadata.opf").exists()


def test_odt_input_rejects_non_zip_payload_without_partial_output(tmp_path: Path, monkeypatch) -> None:
    from LiuXin_alpha.file_formats.conversion.plugins.odt_input import ODTInput

    workdir = tmp_path / "plugin_work"
    workdir.mkdir()
    monkeypatch.chdir(workdir)

    with pytest.raises(Exception):
        ODTInput(None).convert(
            io.BytesIO("not an ODT zip: Καλημέρα".encode("utf-8")),
            SimpleNamespace(),
            "odt",
            NullLog(),
            {},
        )

    assert not list(workdir.glob("**/metadata.opf"))
    assert not list(workdir.glob("**/index.xhtml"))

from __future__ import annotations

import io
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests.support.file_format_odt import NullLog, build_unicode_odt, rewrite_odt_zip


def _assert_extract_rejects_without_partial_output(extract, archive: Path, out_dir: Path, match: str) -> None:
    with archive.open("rb") as stream:
        with pytest.raises(ValueError, match=match):
            extract(stream, str(out_dir), NullLog())

    assert not (out_dir / "index.xhtml").exists()
    assert not (out_dir / "metadata.opf").exists()


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


def test_odt_extract_rejects_too_many_archive_members_without_partial_output(tmp_path: Path) -> None:
    from LiuXin_alpha.file_formats.odt.input import Extract

    class StrictExtract(Extract):
        max_archive_members = 8

    base = build_unicode_odt(tmp_path / "small.odt", lines=("small",))
    hostile = tmp_path / "too_many_members.odt"
    rewrite_odt_zip(
        base.path,
        hostile,
        add={f"Pictures/many/{i}.bin": b"x" for i in range(12)},
    )

    _assert_extract_rejects_without_partial_output(
        StrictExtract(),
        hostile,
        tmp_path / "too_many_out",
        "too many archive members",
    )


def test_odt_extract_rejects_oversized_archive_member_without_partial_output(tmp_path: Path) -> None:
    from LiuXin_alpha.file_formats.odt.input import Extract

    class StrictExtract(Extract):
        max_member_uncompressed_size = 10 * 1024

    base = build_unicode_odt(tmp_path / "small.odt", lines=("small",))
    hostile = tmp_path / "oversized_member.odt"
    rewrite_odt_zip(base.path, hostile, add={"Pictures/big.bin": b"x" * (20 * 1024)})

    _assert_extract_rejects_without_partial_output(
        StrictExtract(),
        hostile,
        tmp_path / "oversized_out",
        "member is too large",
    )


def test_odt_extract_rejects_excessive_total_expansion_without_partial_output(tmp_path: Path) -> None:
    from LiuXin_alpha.file_formats.odt.input import Extract

    class StrictExtract(Extract):
        max_member_uncompressed_size = 100 * 1024
        max_total_uncompressed_size = 30 * 1024

    base = build_unicode_odt(tmp_path / "small.odt", lines=("small",))
    hostile = tmp_path / "large_total.odt"
    rewrite_odt_zip(
        base.path,
        hostile,
        add={f"Pictures/chunk-{i}.bin": b"x" * (8 * 1024) for i in range(6)},
    )

    _assert_extract_rejects_without_partial_output(
        StrictExtract(),
        hostile,
        tmp_path / "large_total_out",
        "expands to too much data",
    )


def test_odt_extract_rejects_suspicious_compression_ratio_without_partial_output(tmp_path: Path) -> None:
    from LiuXin_alpha.file_formats.odt.input import Extract

    class StrictExtract(Extract):
        max_compression_ratio = 20
        min_compression_ratio_check_size = 32 * 1024

    base = build_unicode_odt(tmp_path / "small.odt", lines=("small",))
    hostile = tmp_path / "ratio_bomb_shape.odt"
    rewrite_odt_zip(
        base.path,
        hostile,
        add={"Pictures/repeated.bin": b"0" * (128 * 1024)},
        add_compression=zipfile.ZIP_DEFLATED,
    )

    _assert_extract_rejects_without_partial_output(
        StrictExtract(),
        hostile,
        tmp_path / "ratio_out",
        "suspicious compression ratio",
    )


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

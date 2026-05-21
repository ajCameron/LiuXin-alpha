from __future__ import annotations

import re
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests.support.file_format_comic import (
    NullLog,
    cbz_bytes,
    build_unicode_cbc,
    build_unicode_cbz,
    rewrite_comic_zip,
)


def _comic_options() -> SimpleNamespace:
    return SimpleNamespace(
        no_sort=False,
        verbose=False,
        no_process=True,
        dont_add_comic_pages_to_toc=False,
    )


def _assert_comic_convert_rejects_without_local_output(
    archive: Path,
    file_ext: str,
    workdir: Path,
    monkeypatch,
    match: str,
) -> None:
    from LiuXin_alpha.file_formats.conversion.plugins.comic_input import ComicInput

    workdir.mkdir()
    monkeypatch.chdir(workdir)
    log = NullLog()

    with archive.open("rb") as stream:
        with pytest.raises(Exception, match=match):
            ComicInput(None).convert(stream, _comic_options(), file_ext, log, {})

    assert not (workdir / "metadata.opf").exists()
    assert not (workdir / "toc.ncx").exists()
    assert not list(workdir.glob("page_*.xhtml"))


def _assert_comic_preflight_rejects_without_local_output(
    archive: Path,
    file_ext: str,
    workdir: Path,
    monkeypatch,
    match: str,
    input_cls=None,
) -> NullLog:
    from LiuXin_alpha.file_formats.conversion.plugins.comic_input import ComicInput

    workdir.mkdir()
    monkeypatch.chdir(workdir)
    plugin_cls = input_cls or ComicInput
    log = NullLog()

    with archive.open("rb") as stream:
        with pytest.raises(ValueError, match=match):
            plugin_cls(None).convert(stream, _comic_options(), file_ext, log, {})

    assert not (workdir / "metadata.opf").exists()
    assert not (workdir / "toc.ncx").exists()
    assert not list(workdir.glob("page_*.xhtml"))

    preflight_messages = [
        message for message in log.messages if "Comic preflight rejected" in message
    ]
    assert preflight_messages
    assert re.search(match, preflight_messages[-1])
    return log


def test_comic_input_rejects_cbz_without_image_pages(
    tmp_path: Path,
    monkeypatch,
) -> None:
    fixture = build_unicode_cbz(
        tmp_path / "no_pages.cbz",
        page_members=(),
        extra_members={"notes/readme_שלום.txt": "שלום only".encode("utf-8")},
    )

    _assert_comic_convert_rejects_without_local_output(
        fixture.path,
        "cbz",
        tmp_path / "no_pages_work",
        monkeypatch,
        "Could not find any pages in the comic",
    )


def test_comic_input_rejects_wrong_format_cbz_without_local_output(
    tmp_path: Path,
    monkeypatch,
) -> None:
    archive = tmp_path / "not_a_zip.cbz"
    archive.write_bytes("not a comic archive: Καλημέρα".encode("utf-8"))

    _assert_comic_convert_rejects_without_local_output(
        archive,
        "cbz",
        tmp_path / "wrong_format_work",
        monkeypatch,
        "invalid ZIP|File is not a zip file|Unknown archive type",
    )


def test_comic_input_rejects_cbc_without_comics_txt(
    tmp_path: Path,
    monkeypatch,
) -> None:
    base = build_unicode_cbc(tmp_path / "base.cbc")
    hostile = tmp_path / "missing_comics_txt.cbc"
    rewrite_comic_zip(base.path, hostile, remove=(base.comics_txt_member,))

    _assert_comic_convert_rejects_without_local_output(
        hostile,
        "cbc",
        tmp_path / "missing_comics_txt_work",
        monkeypatch,
        "no comics.txt",
    )


def test_comic_input_rejects_cbc_with_invalid_comics_txt_encoding(
    tmp_path: Path,
    monkeypatch,
) -> None:
    base = build_unicode_cbc(tmp_path / "base.cbc")
    hostile = tmp_path / "bad_comics_txt.cbc"
    rewrite_comic_zip(base.path, hostile, replace={base.comics_txt_member: b"\xff\xff\xff"})

    _assert_comic_convert_rejects_without_local_output(
        hostile,
        "cbc",
        tmp_path / "bad_comics_txt_work",
        monkeypatch,
        "not valid UTF-8 or UTF-16",
    )


def test_comic_input_reports_missing_cbc_member_and_uses_remaining_comics(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from LiuXin_alpha.file_formats.conversion.plugins.comic_input import ComicInput

    fixture = build_unicode_cbc(tmp_path / "collection.cbc")
    missing_spec = fixture.comic_specs[1]
    hostile = tmp_path / "missing_one_listed_comic.cbc"
    rewrite_comic_zip(fixture.path, hostile, remove=(missing_spec.member_name,))

    workdir = tmp_path / "missing_one_work"
    workdir.mkdir()
    monkeypatch.chdir(workdir)
    log = NullLog()
    plugin = ComicInput(None)

    with hostile.open("rb") as stream:
        out = plugin.convert(stream, _comic_options(), "cbc", log, {})

    assert Path(out).exists()
    assert any(
        "CBC listed comic file was not found: %s" % missing_spec.member_name in message
        for message in log.messages
    )

    image_names = [Path(path).name for path in plugin.get_images()]
    expected_remaining = [Path(member).name for member in fixture.comic_specs[0].page_members]
    missing_names = [Path(member).name for member in missing_spec.page_members]
    assert image_names == expected_remaining
    for missing_name in missing_names:
        assert missing_name not in image_names


def test_comic_collection_parser_reports_missing_listed_member(
    tmp_path: Path,
) -> None:
    from LiuXin_alpha.file_formats.conversion.plugins.comic_input import ComicInput

    fixture = build_unicode_cbc(tmp_path / "collection.cbc")
    missing_spec = fixture.comic_specs[1]
    hostile = tmp_path / "parser_missing_one.cbc"
    rewrite_comic_zip(fixture.path, hostile, remove=(missing_spec.member_name,))
    log = NullLog()
    plugin = ComicInput(None)
    plugin.log = log

    with hostile.open("rb") as stream:
        comics = plugin.get_comics_from_collection(stream)

    assert [title for title, _path in comics] == [fixture.comic_specs[0].title]
    assert any(
        "CBC listed comic file was not found: %s" % missing_spec.member_name in message
        for message in log.messages
    )


def test_comic_input_rejects_cbc_when_all_listed_comics_are_missing(
    tmp_path: Path,
    monkeypatch,
) -> None:
    base = build_unicode_cbc(tmp_path / "base.cbc")
    hostile = tmp_path / "all_listed_missing.cbc"
    rewrite_comic_zip(base.path, hostile, remove=base.comic_members)

    _assert_comic_convert_rejects_without_local_output(
        hostile,
        "cbc",
        tmp_path / "all_missing_work",
        monkeypatch,
        "has no comics",
    )


def test_comic_input_rejects_non_zip_cbz_before_extraction(
    tmp_path: Path,
    monkeypatch,
) -> None:
    archive = tmp_path / "not_a_zip.cbz"
    archive.write_bytes("not a comic archive: Καλημέρα".encode("utf-8"))

    _assert_comic_preflight_rejects_without_local_output(
        archive,
        "cbz",
        tmp_path / "not_zip_preflight_work",
        monkeypatch,
        "invalid ZIP",
    )


@pytest.mark.parametrize(
    ("case_id", "member_name"),
    (
        ("parent_escape", "../escape.png"),
        ("nested_parent_escape", "pages/../../escape.png"),
        ("internal_parent_component", "pages/../escape.png"),
        ("absolute_path", "/absolute.png"),
        ("drive_path", "C:/absolute.png"),
    ),
)
def test_comic_input_rejects_unsafe_cbz_member_paths_before_extraction(
    tmp_path: Path,
    monkeypatch,
    case_id: str,
    member_name: str,
) -> None:
    base = build_unicode_cbz(tmp_path / "base.cbz")
    hostile = tmp_path / f"{case_id}.cbz"
    rewrite_comic_zip(base.path, hostile, add={member_name: b"\x89PNG unsafe"})

    _assert_comic_preflight_rejects_without_local_output(
        hostile,
        "cbz",
        tmp_path / f"{case_id}_work",
        monkeypatch,
        "unsafe path",
    )


@pytest.mark.parametrize(
    ("case_id", "member_name"),
    (
        ("parent_escape", "../escape.cbz"),
        ("nested_parent_escape", "comics/../../escape.cbz"),
        ("internal_parent_component", "comics/../escape.cbz"),
        ("absolute_path", "/absolute.cbz"),
        ("drive_path", "C:/absolute.cbz"),
    ),
)
def test_comic_input_rejects_unsafe_cbc_member_paths_before_extraction(
    tmp_path: Path,
    monkeypatch,
    case_id: str,
    member_name: str,
) -> None:
    base = build_unicode_cbc(tmp_path / "base.cbc")
    hostile = tmp_path / f"{case_id}.cbc"
    rewrite_comic_zip(base.path, hostile, add={member_name: b"unsafe"})

    _assert_comic_preflight_rejects_without_local_output(
        hostile,
        "cbc",
        tmp_path / f"{case_id}_work",
        monkeypatch,
        "unsafe path",
    )


def test_comic_input_rejects_nested_cbc_cbz_unsafe_member_before_page_extraction(
    tmp_path: Path,
    monkeypatch,
) -> None:
    fixture = build_unicode_cbc(tmp_path / "base.cbc")
    hostile = tmp_path / "nested_unsafe.cbc"
    rewrite_comic_zip(
        fixture.path,
        hostile,
        replace={fixture.comic_specs[0].member_name: cbz_bytes(page_members=("../escape.png",))},
        add_compression=zipfile.ZIP_DEFLATED,
    )

    _assert_comic_preflight_rejects_without_local_output(
        hostile,
        "cbc",
        tmp_path / "nested_unsafe_work",
        monkeypatch,
        "unsafe path",
    )


def test_comic_input_rejects_too_many_cbz_members_without_partial_output(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from LiuXin_alpha.file_formats.conversion.plugins.comic_input import ComicInput

    class StrictComicInput(ComicInput):
        max_archive_members = 4

    base = build_unicode_cbz(tmp_path / "small.cbz", page_members=("pages/01.png",))
    hostile = tmp_path / "too_many_members.cbz"
    rewrite_comic_zip(
        base.path,
        hostile,
        add={f"pages/many/{i}.png": b"\x89PNG extra" for i in range(8)},
    )

    _assert_comic_preflight_rejects_without_local_output(
        hostile,
        "cbz",
        tmp_path / "too_many_work",
        monkeypatch,
        "too many archive members",
        input_cls=StrictComicInput,
    )


def test_comic_input_rejects_oversized_cbz_member_without_partial_output(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from LiuXin_alpha.file_formats.conversion.plugins.comic_input import ComicInput

    class StrictComicInput(ComicInput):
        max_member_uncompressed_size = 10 * 1024

    base = build_unicode_cbz(tmp_path / "small.cbz", page_members=("pages/01.png",))
    hostile = tmp_path / "oversized_member.cbz"
    rewrite_comic_zip(base.path, hostile, add={"pages/big.png": b"x" * (20 * 1024)})

    _assert_comic_preflight_rejects_without_local_output(
        hostile,
        "cbz",
        tmp_path / "oversized_work",
        monkeypatch,
        "member is too large",
        input_cls=StrictComicInput,
    )


def test_comic_input_rejects_excessive_cbz_total_expansion_without_partial_output(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from LiuXin_alpha.file_formats.conversion.plugins.comic_input import ComicInput

    class StrictComicInput(ComicInput):
        max_member_uncompressed_size = 100 * 1024
        max_total_uncompressed_size = 30 * 1024

    base = build_unicode_cbz(tmp_path / "small.cbz", page_members=("pages/01.png",))
    hostile = tmp_path / "large_total.cbz"
    rewrite_comic_zip(
        base.path,
        hostile,
        add={f"pages/chunk-{i}.png": b"x" * (8 * 1024) for i in range(6)},
    )

    _assert_comic_preflight_rejects_without_local_output(
        hostile,
        "cbz",
        tmp_path / "large_total_work",
        monkeypatch,
        "expands to too much data",
        input_cls=StrictComicInput,
    )


def test_comic_input_rejects_suspicious_cbz_compression_ratio_without_partial_output(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from LiuXin_alpha.file_formats.conversion.plugins.comic_input import ComicInput

    class StrictComicInput(ComicInput):
        max_compression_ratio = 20
        min_compression_ratio_check_size = 32 * 1024

    base = build_unicode_cbz(tmp_path / "small.cbz", page_members=("pages/01.png",))
    hostile = tmp_path / "ratio_bomb_shape.cbz"
    rewrite_comic_zip(
        base.path,
        hostile,
        add={"pages/repeated.png": b"0" * (128 * 1024)},
        add_compression=zipfile.ZIP_DEFLATED,
    )

    _assert_comic_preflight_rejects_without_local_output(
        hostile,
        "cbz",
        tmp_path / "ratio_work",
        monkeypatch,
        "suspicious compression ratio",
        input_cls=StrictComicInput,
    )

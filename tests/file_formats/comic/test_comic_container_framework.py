from __future__ import annotations

import io
import zipfile
from pathlib import Path
from types import SimpleNamespace
from xml.etree import ElementTree as ET

from tests.support.file_format_comic import (
    COMIC_PAGE_FRAGMENTS,
    COMIC_PAGE_MEMBERS,
    COMIC_TITLE,
    FakeRarInfo,
    NullLog,
    build_unicode_cbc,
    build_unicode_cbz,
    patch_rarfile_failure,
    patch_rarfile_infolist,
    patch_unrar_names,
    png_bytes,
    read_comic_member,
    rewrite_comic_zip,
    vendored_rar_fixture,
    write_stub_cbr,
)
from tests.support.file_format_unicode import assert_no_replacement_chars


def _comic_options() -> SimpleNamespace:
    return SimpleNamespace(
        no_sort=False,
        verbose=False,
        no_process=True,
        dont_add_comic_pages_to_toc=False,
    )


def _read_valid_opf(path: Path) -> str:
    assert path.exists(), f"missing output OPF: {path}"
    root = ET.parse(path).getroot()
    assert root.tag.endswith("package")
    rendered = path.read_text("utf-8", "replace")
    assert_no_replacement_chars(rendered, context="comic metadata.opf")
    return rendered


def test_comic_cbz_fixture_builds_valid_unicode_page_archive(tmp_path: Path) -> None:
    fixture = build_unicode_cbz(tmp_path / "comic_Καλημέρα_世界.cbz")

    with zipfile.ZipFile(fixture.path, "r") as zf:
        infos = zf.infolist()
        members = {info.filename for info in infos}
        assert all(info.compress_type == zipfile.ZIP_DEFLATED for info in infos)

    assert set(COMIC_PAGE_MEMBERS).issubset(members)
    assert fixture.page_members == COMIC_PAGE_MEMBERS
    assert fixture.extra_members == ()

    for member_name in fixture.page_members:
        payload = read_comic_member(fixture.path, member_name)
        assert payload.startswith(b"\x89PNG")

    rendered_members = "\n".join(members)
    for fragment in COMIC_PAGE_FRAGMENTS:
        assert fragment in rendered_members


def test_comic_cbz_fixture_supports_optional_extra_members(tmp_path: Path) -> None:
    extra_members = {
        "notes/readme_שלום.txt": "שלום comic note".encode("utf-8"),
        "metadata/credits_世界.json": b'{"role": "artist"}',
    }
    fixture = build_unicode_cbz(
        tmp_path / "extra_members.cbz",
        extra_members=extra_members,
    )

    with zipfile.ZipFile(fixture.path, "r") as zf:
        members = {info.filename for info in zf.infolist()}
        assert set(fixture.page_members).issubset(members)
        assert set(extra_members).issubset(members)
        for member_name, payload in extra_members.items():
            assert zf.read(member_name) == payload

    assert fixture.extra_members == tuple(extra_members)


def test_comic_cbc_fixture_builds_valid_collection_with_nested_cbz_members(tmp_path: Path) -> None:
    fixture = build_unicode_cbc(tmp_path / "collection_Καλημέρα_世界.cbc")

    with zipfile.ZipFile(fixture.path, "r") as zf:
        infos = zf.infolist()
        members = {info.filename for info in infos}
        assert all(info.compress_type == zipfile.ZIP_DEFLATED for info in infos)

    assert fixture.comics_txt_member in members
    assert set(fixture.comic_members).issubset(members)
    assert fixture.titles[0] == COMIC_TITLE
    assert "第二巻" in fixture.titles[1]

    comics_txt = read_comic_member(fixture.path, fixture.comics_txt_member).decode("utf-8")
    inner_names: list[str] = []
    for spec in fixture.comic_specs:
        assert f"{spec.member_name}:{spec.title}" in comics_txt
        inner_payload = read_comic_member(fixture.path, spec.member_name)
        with zipfile.ZipFile(io.BytesIO(inner_payload), "r") as inner:
            inner_members = {info.filename for info in inner.infolist()}
            inner_names.extend(inner_members)
            assert set(spec.page_members).issubset(inner_members)
            assert inner.read(spec.page_members[0]).startswith(b"\x89PNG")

    rendered = comics_txt + "\n".join(members) + "\n".join(inner_names)
    for fragment in fixture.path_fragments:
        assert fragment in rendered


def test_comic_collection_parser_accepts_unicode_fixture_titles_and_paths(tmp_path: Path) -> None:
    from LiuXin_alpha.file_formats.conversion.plugins.comic_input import ComicInput

    fixture = build_unicode_cbc(tmp_path / "bundle_Καλημέρα_世界.cbc")
    plugin = ComicInput(None)

    with fixture.path.open("rb") as stream:
        comics = plugin.get_comics_from_collection(stream)

    assert [title for title, _path in comics] == list(fixture.titles)
    for spec, (_title, extracted_path) in zip(fixture.comic_specs, comics):
        extracted = Path(extracted_path)
        assert extracted.exists()
        assert extracted.name == Path(spec.member_name).name
        assert extracted.read_bytes() == read_comic_member(fixture.path, spec.member_name)


def test_comic_input_accepts_unicode_cbz_fixture_through_plugin_path(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from LiuXin_alpha.file_formats.conversion.plugins.comic_input import ComicInput

    fixture = build_unicode_cbz(tmp_path / "comic_Καλημέρα_世界.cbz")
    work = tmp_path / "cbz_work"
    work.mkdir()
    monkeypatch.chdir(work)
    plugin = ComicInput(None)

    with fixture.path.open("rb") as stream:
        out = plugin.convert(stream, _comic_options(), "cbz", NullLog(), {})

    out_path = Path(out)
    assert out_path.is_absolute()
    assert out_path.name == "metadata.opf"
    assert out_path.parent != work
    assert not (work / "metadata.opf").exists()

    opf_text = _read_valid_opf(out_path)
    assert "comic_Καλημέρα_世界" in opf_text
    for member_name in fixture.page_members:
        page_name = Path(member_name).name
        assert page_name in opf_text
        assert (out_path.parent / page_name).read_bytes().startswith(b"\x89PNG")

    wrapper_paths = sorted(out_path.parent.glob("page_*.xhtml"))
    assert len(wrapper_paths) == len(fixture.page_members)
    for wrapper_path, member_name in zip(wrapper_paths, fixture.page_members):
        wrapper_text = wrapper_path.read_text("utf-8", "replace")
        assert Path(member_name).name in wrapper_text
        assert_no_replacement_chars(wrapper_text, context=wrapper_path.name)

    image_names = [Path(path).name for path in plugin.get_images()]
    assert image_names == [Path(member_name).name for member_name in fixture.page_members]


def test_comic_input_accepts_unicode_cbc_collection_through_plugin_path(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from LiuXin_alpha.file_formats.conversion.plugins.comic_input import ComicInput

    fixture = build_unicode_cbc(tmp_path / "collection_Καλημέρα_世界.cbc")
    work = tmp_path / "cbc_work"
    work.mkdir()
    monkeypatch.chdir(work)
    plugin = ComicInput(None)

    with fixture.path.open("rb") as stream:
        out = plugin.convert(stream, _comic_options(), "cbc", NullLog(), {})

    out_path = Path(out)
    assert out_path.is_absolute()
    assert out_path.name == "metadata.opf"
    assert out_path.parent != work

    opf_text = _read_valid_opf(out_path)
    assert "collection_Καλημέρα_世界" in opf_text
    for spec in fixture.comic_specs:
        for member_name in spec.page_members:
            page_name = Path(member_name).name
            assert page_name in opf_text
            assert (out_path.parent / page_name).exists() or list(out_path.parent.glob(f"comic_*/{page_name}"))

    toc_text = out_path.with_name("toc.ncx").read_text("utf-8", "replace")
    for title in fixture.titles:
        assert title in toc_text
    assert_no_replacement_chars(toc_text, context="comic toc.ncx")

    image_names = [Path(path).name for path in plugin.get_images()]
    expected_names = [
        Path(member_name).name
        for spec in fixture.comic_specs
        for member_name in spec.page_members
    ]
    assert image_names == expected_names
    assert len(list(out_path.parent.glob("comic_*/page_*.xhtml"))) == len(expected_names)


def test_comic_input_accepts_preflighted_unicode_cbr_through_plugin_path(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from LiuXin_alpha.file_formats.conversion.plugins.comic_input import ComicInput
    import LiuXin_alpha.file_formats.comic.input as comic_input_impl

    archive = write_stub_cbr(tmp_path / "comic_Καλημέρα_世界.cbr")
    page_name = "01_Καλημέρα_世界.png"
    patch_rarfile_infolist(
        monkeypatch,
        (FakeRarInfo(f"pages/{page_name}", file_size=2048, compress_size=1024),),
    )

    extracted = tmp_path / "extracted_cbr"

    def _extract_comic(_path):
        page_dir = extracted / "pages"
        page_dir.mkdir(parents=True, exist_ok=True)
        (page_dir / page_name).write_bytes(png_bytes())
        return str(extracted)

    monkeypatch.setattr(comic_input_impl, "extract_comic", _extract_comic)

    work = tmp_path / "cbr_work"
    work.mkdir()
    monkeypatch.chdir(work)
    plugin = ComicInput(None)

    with archive.open("rb") as stream:
        out = plugin.convert(stream, _comic_options(), "cbr", NullLog(), {})

    out_path = Path(out)
    opf_text = _read_valid_opf(out_path)
    assert "comic_Καλημέρα_世界" in opf_text
    assert page_name in opf_text
    assert (out_path.parent / page_name).read_bytes().startswith(b"\x89PNG")
    assert [Path(path).name for path in plugin.get_images()] == [page_name]


def test_comic_cbr_preflight_accepts_real_vendored_unicode_rar_listing() -> None:
    from LiuXin_alpha.file_formats.conversion.plugins.comic_input import ComicInput

    archive = vendored_rar_fixture("unicode.rar")
    plugin = ComicInput(None)

    assert plugin.should_preflight_rar_archive(archive, ext_hint="cbr")
    infos = plugin._rar_archive_infos(str(archive))
    names = {str(info.filename).replace("\\", "/") for info in infos}

    assert {"уииоотивл.txt", "𝐀𝐁𝐁𝐂.txt"}.issubset(names)
    plugin.validate_rar_archive_members(archive, "CBR")


def test_comic_cbr_preflight_accepts_names_only_external_rar_listing(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from LiuXin_alpha.file_formats.conversion.plugins.comic_input import ComicInput

    archive = write_stub_cbr(tmp_path / "fallback_Καλημέρα_世界.cbr")
    patch_rarfile_failure(monkeypatch)
    patch_unrar_names(
        monkeypatch,
        (
            "pages/01_Καλημέρα.png",
            "pages/深/02_世界.png",
            "pages/03_café.png",
        ),
    )
    plugin = ComicInput(None)

    infos = plugin._rar_archive_infos(str(archive))
    names = [info.filename for info in infos]

    assert names == [
        "pages/01_Καλημέρα.png",
        "pages/深/02_世界.png",
        "pages/03_café.png",
    ]
    assert all(info.file_size is None for info in infos)
    plugin.validate_rar_archive_members(archive, "CBR")


def test_comic_fixture_rewrite_helper_removes_replaces_and_adds_members(tmp_path: Path) -> None:
    fixture = build_unicode_cbz(tmp_path / "base.cbz")
    rewritten = tmp_path / "rewritten.cbz"
    replacement_page = b"\x89PNG replacement"

    rewrite_comic_zip(
        fixture.path,
        rewritten,
        remove=(fixture.page_members[1],),
        replace={fixture.page_members[0]: replacement_page},
        add={"pages/extra_世界.png": b"\x89PNG extra"},
        add_compression=zipfile.ZIP_DEFLATED,
    )

    with zipfile.ZipFile(rewritten, "r") as zf:
        members = {info.filename for info in zf.infolist()}
        extra_info = zf.getinfo("pages/extra_世界.png")
        assert fixture.page_members[1] not in members
        assert "pages/extra_世界.png" in members
        assert extra_info.compress_type == zipfile.ZIP_DEFLATED
        assert zf.read(fixture.page_members[0]) == replacement_page

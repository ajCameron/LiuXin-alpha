from __future__ import annotations

import io
import importlib
import types
import zipfile
from pathlib import Path

import pytest


class _FakeTOCNode:
    def __init__(self):
        self.children = []

    def add_item(self, href, fragment, text, play_order=None):
        child = _FakeTOCNode()
        self.children.append((href, fragment, text, play_order, child))
        return child


class _FakeOPFCreator:
    def __init__(self, base_path, mi):
        self.base_path = base_path
        self.mi = mi
        self.manifest = None
        self.spine = None
        self.toc = None

    def create_manifest(self, entries):
        self.manifest = list(entries)

    def create_spine(self, entries):
        self.spine = list(entries)

    def set_toc(self, toc):
        self.toc = toc

    def render(self, opf_stream, ncx_stream, ncx_name):
        opf_stream.write(b"<opf/>")
        ncx_stream.write(b"<ncx/>")


def test_comic_modules_import_smoke() -> None:
    importlib.import_module("LiuXin_alpha.file_formats.comic")
    importlib.import_module("LiuXin_alpha.file_formats.comic.input")
    importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.comic_input")


def test_find_pages_numeric_sort(tmp_path: Path) -> None:
    from LiuXin_alpha.file_formats.comic.input import find_pages

    for name in ("10.png", "2.png", "01.png", "note.txt"):
        (tmp_path / name).write_bytes(b"x")

    pages = find_pages(str(tmp_path), sort_on_mtime=False, verbose=False)
    names = [Path(p).name for p in pages]

    assert names == ["01.png", "2.png", "10.png"]


def test_get_comics_from_collection_parses_comics_txt(tmp_path: Path) -> None:
    from LiuXin_alpha.file_formats.conversion.plugins.comic_input import ComicInput

    cbc = tmp_path / "bundle.cbc"
    with zipfile.ZipFile(cbc, "w") as zf:
        zf.writestr("comics.txt", "set/a.cbz:Alpha\nset/b.cbz:\n")
        zf.writestr("set/a.cbz", b"PK\x03\x04")
        zf.writestr("set/b.cbz", b"PK\x03\x04")

    plugin = ComicInput(None)
    with cbc.open("rb") as stream:
        comics = plugin.get_comics_from_collection(stream)

    assert [title for title, _ in comics] == ["Alpha", "b"]
    assert len(comics) == 2


def test_comic_convert_glue_with_fakes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import LiuXin_alpha.file_formats.conversion.plugins.comic_input as comic_plugin_mod
    import LiuXin_alpha.file_formats.opf.opf2 as opf2_mod
    import LiuXin_alpha.file_formats.toc as toc_mod

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(opf2_mod, "OPFCreator", _FakeOPFCreator)
    monkeypatch.setattr(toc_mod, "TOC", _FakeTOCNode)

    plugin = comic_plugin_mod.ComicInput(None)

    def fake_get_pages(self, comic, cdir):
        page1 = Path(cdir) / "1.png"
        page2 = Path(cdir) / "2.png"
        page1.write_bytes(b"png")
        page2.write_bytes(b"png")
        return [str(page1), str(page2)]

    def fake_create_wrappers(self, pages):
        wrappers = []
        for i, page in enumerate(pages, start=1):
            wrapper = Path(page).with_name(f"page_{i}.xhtml")
            wrapper.write_text("<html/>", encoding="utf-8")
            wrappers.append(str(wrapper))
        return wrappers

    monkeypatch.setattr(comic_plugin_mod.ComicInput, "get_pages", fake_get_pages)
    monkeypatch.setattr(comic_plugin_mod.ComicInput, "create_wrappers", fake_create_wrappers)

    options = types.SimpleNamespace(dont_add_comic_pages_to_toc=False)
    in_file = tmp_path / "comic.cbz"
    in_file.write_bytes(b"fake")

    with in_file.open("rb") as stream:
        out = plugin.convert(stream, options, "cbz", log=types.SimpleNamespace(warning=lambda *a: None), accelerators={})

    out_path = Path(out)
    assert out_path.is_absolute()
    assert out_path.name == "metadata.opf"
    assert out_path.exists()
    assert out_path.parent != tmp_path
    assert not (tmp_path / "metadata.opf").exists()
    assert not (tmp_path / "toc.ncx").exists()
    assert out_path.with_name("toc.ncx").exists()
    assert len(plugin.get_images()) == 2


def test_process_pages_uses_job_backend_and_preserves_order(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import LiuXin_alpha.file_formats.comic.input as comic_input_mod

    calls = []

    def fake_fork_job(module_name, function_name, args=(), **kwargs):
        calls.append((module_name, function_name, kwargs))
        tasks, _dest, _opts_payload = args
        rendered = [f"{num}_0.png" for num, _ in tasks]
        return {"result": (rendered, [])}

    monkeypatch.setattr("LiuXin_alpha.utils.ipc.simple_worker.fork_job", fake_fork_job)

    updates = []
    opts = types.SimpleNamespace(
        comic_job_backend="process",
        comic_job_workers=2,
        comic_job_chunk_size=2,
        comic_job_timeout=30,
        output_format="png",
    )
    pages = [str(tmp_path / f"{i}.png") for i in range(5)]

    rendered, failures = comic_input_mod.process_pages(
        pages, opts, lambda fraction, msg: updates.append((fraction, msg)), str(tmp_path)
    )

    assert failures == []
    assert rendered == ["0_0.png", "1_0.png", "2_0.png", "3_0.png", "4_0.png"]
    assert len(calls) == 3
    assert all(call[0] == "LiuXin_alpha.file_formats.comic.input" for call in calls)
    assert all(call[1] == "_render_pages_job" for call in calls)
    assert updates and updates[-1][0] == pytest.approx(1.0)


def test_process_pages_serial_path_uses_local_renderer(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import LiuXin_alpha.file_formats.comic.input as comic_input_mod

    seen = {}

    def fake_render_pages(tasks, dest, opts, notification=lambda *_: None):
        seen["tasks"] = list(tasks)
        seen["dest"] = dest
        notification(1.0, "done")
        return ["ok.png"], []

    monkeypatch.setattr(comic_input_mod, "render_pages", fake_render_pages)

    updates = []
    opts = types.SimpleNamespace(comic_job_backend="serial", output_format="png")
    pages = [str(tmp_path / "0.png"), str(tmp_path / "1.png")]

    rendered, failures = comic_input_mod.process_pages(
        pages, opts, lambda fraction, msg: updates.append((fraction, msg)), str(tmp_path)
    )

    assert rendered == ["ok.png"]
    assert failures == []
    assert seen["tasks"] == [(0, pages[0]), (1, pages[1])]
    assert seen["dest"] == str(tmp_path)
    assert updates and updates[-1][0] == pytest.approx(0.5)

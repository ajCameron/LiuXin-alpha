"""
Provide test comic modernized utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test comic modernized through a consuming regression::

        python -m pytest -q tests/file_formats/comic/test_comic_modernized.py
"""
from __future__ import annotations

import io
import importlib
import types
import zipfile
from pathlib import Path

import pytest


class _FakeTOCNode:
    """
    Provide the faketocnode contract for validated ebook processing.

    Example:
        Exercise  FakeTOCNode through a consuming regression::

            python -m pytest -q tests/file_formats/comic/test_comic_modernized.py
    """
    def __init__(self):
        """
        Initialize and validate the faketocnode state.

        Example:
            Exercise  FakeTOCNode.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/comic/test_comic_modernized.py


        :return: None; validated state is stored on the receiving object.
        """
        self.children = []

    def add_item(self, href, fragment, text, play_order=None):
        """
        Perform the add item operation under explicit file-format and conversion rules.

        Example:
            Exercise  FakeTOCNode.add item through a consuming regression::

                python -m pytest -q tests/file_formats/comic/test_comic_modernized.py


        :param href: Value supplied for href under the utility contract.
        :param fragment: Value supplied for fragment under the utility contract.
        :param text: Text parsed, normalized or rendered.
        :param play_order: Value supplied for play order under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        child = _FakeTOCNode()
        self.children.append((href, fragment, text, play_order, child))
        return child


class _FakeOPFCreator:
    """
    Provide the fakeopfcreator contract for validated ebook processing.

    Example:
        Exercise  FakeOPFCreator through a consuming regression::

            python -m pytest -q tests/file_formats/comic/test_comic_modernized.py
    """
    def __init__(self, base_path, mi):
        """
        Initialize and validate the fakeopfcreator state.

        Example:
            Exercise  FakeOPFCreator.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/comic/test_comic_modernized.py


        :param base_path: Value supplied for base path under the utility contract.
        :param mi: Metadata object exposed to the template function.
        :return: None; validated state is stored on the receiving object.
        """
        self.base_path = base_path
        self.mi = mi
        self.manifest = None
        self.spine = None
        self.toc = None

    def create_manifest(self, entries):
        """
        Create the OPF manifest from normalized resource paths.

        Example:
            Exercise  FakeOPFCreator.create manifest through a consuming regression::

                python -m pytest -q tests/file_formats/comic/test_comic_modernized.py


        :param entries: Value supplied for entries under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.manifest = list(entries)

    def create_spine(self, entries):
        """
        Create the OPF spine in the requested reading order.

        Example:
            Exercise  FakeOPFCreator.create spine through a consuming regression::

                python -m pytest -q tests/file_formats/comic/test_comic_modernized.py


        :param entries: Value supplied for entries under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.spine = list(entries)

    def set_toc(self, toc):
        """
        Set toc under the format's safety and compatibility rules.

        Example:
            Exercise  FakeOPFCreator.set toc through a consuming regression::

                python -m pytest -q tests/file_formats/comic/test_comic_modernized.py


        :param toc: Value supplied for toc under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.toc = toc

    def render(self, opf_stream, ncx_stream, ncx_name):
        """
        Perform the render operation under explicit file-format and conversion rules.

        Example:
            Exercise  FakeOPFCreator.render through a consuming regression::

                python -m pytest -q tests/file_formats/comic/test_comic_modernized.py


        :param opf_stream: Value supplied for opf stream under the utility contract.
        :param ncx_stream: Value supplied for ncx stream under the utility contract.
        :param ncx_name: Value supplied for ncx name under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        opf_stream.write(b"<opf/>")
        ncx_stream.write(b"<ncx/>")


def test_comic_modules_import_smoke() -> None:
    """
    Perform the test comic modules import smoke operation under explicit file-format and conversion rules.

    Example:
        Exercise test comic modules import smoke through a consuming regression::

            python -m pytest -q tests/file_formats/comic/test_comic_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    importlib.import_module("LiuXin_alpha.file_formats.comic")
    importlib.import_module("LiuXin_alpha.file_formats.comic.input")
    importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.comic_input")


def test_find_pages_numeric_sort(tmp_path: Path) -> None:
    """
    Perform the test find pages numeric sort operation under explicit file-format and conversion rules.

    Example:
        Exercise test find pages numeric sort through a consuming regression::

            python -m pytest -q tests/file_formats/comic/test_comic_modernized.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.comic.input import find_pages

    for name in ("10.png", "2.png", "01.png", "note.txt"):
        (tmp_path / name).write_bytes(b"x")

    pages = find_pages(str(tmp_path), sort_on_mtime=False, verbose=False)
    names = [Path(p).name for p in pages]

    assert names == ["01.png", "2.png", "10.png"]


def test_get_comics_from_collection_parses_comics_txt(tmp_path: Path) -> None:
    """
    Perform the test get comics from collection parses comics txt operation under explicit file-format and conversion rules.

    Example:
        Exercise test get comics from collection parses comics txt through a consuming regression::

            python -m pytest -q tests/file_formats/comic/test_comic_modernized.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
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
    """
    Perform the test comic convert glue with fakes operation under explicit file-format and conversion rules.

    Example:
        Exercise test comic convert glue with fakes through a consuming regression::

            python -m pytest -q tests/file_formats/comic/test_comic_modernized.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    import LiuXin_alpha.file_formats.conversion.plugins.comic_input as comic_plugin_mod
    import LiuXin_alpha.file_formats.opf.opf2 as opf2_mod
    import LiuXin_alpha.file_formats.toc as toc_mod

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(opf2_mod, "OPFCreator", _FakeOPFCreator)
    monkeypatch.setattr(toc_mod, "TOC", _FakeTOCNode)

    plugin = comic_plugin_mod.ComicInput(None)

    def fake_get_pages(self, comic, cdir):
        """
        Perform the fake get pages operation under explicit file-format and conversion rules.

        Example:
            Exercise test comic convert glue with fakes.fake get pages through a consuming regression::

                python -m pytest -q tests/file_formats/comic/test_comic_modernized.py


        :param self: Value supplied for self under the utility contract.
        :param comic: Value supplied for comic under the utility contract.
        :param cdir: Value supplied for cdir under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        page1 = Path(cdir) / "1.png"
        page2 = Path(cdir) / "2.png"
        page1.write_bytes(b"png")
        page2.write_bytes(b"png")
        return [str(page1), str(page2)]

    def fake_create_wrappers(self, pages):
        """
        Perform the fake create wrappers operation under explicit file-format and conversion rules.

        Example:
            Exercise test comic convert glue with fakes.fake create wrappers through a consuming regression::

                python -m pytest -q tests/file_formats/comic/test_comic_modernized.py


        :param self: Value supplied for self under the utility contract.
        :param pages: Value supplied for pages under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
    with zipfile.ZipFile(in_file, "w") as zf:
        zf.writestr("placeholder.txt", b"fake")

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
    """
    Perform the test process pages uses job backend and preserves order operation under explicit file-format and conversion rules.

    Example:
        Exercise test process pages uses job backend and preserves order through a consuming regression::

            python -m pytest -q tests/file_formats/comic/test_comic_modernized.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    import LiuXin_alpha.file_formats.comic.input as comic_input_mod

    calls = []

    def fake_fork_job(module_name, function_name, args=(), **kwargs):
        """
        Perform the fake fork job operation under explicit file-format and conversion rules.

        Example:
            Exercise test process pages uses job backend and preserves order.fake fork job through a consuming regression::

                python -m pytest -q tests/file_formats/comic/test_comic_modernized.py


        :param module_name: Value supplied for module name under the utility contract.
        :param function_name: Value supplied for function name under the utility contract.
        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
    """
    Perform the test process pages serial path uses local renderer operation under explicit file-format and conversion rules.

    Example:
        Exercise test process pages serial path uses local renderer through a consuming regression::

            python -m pytest -q tests/file_formats/comic/test_comic_modernized.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    import LiuXin_alpha.file_formats.comic.input as comic_input_mod

    seen = {}

    def fake_render_pages(tasks, dest, opts, notification=lambda *_: None):
        """
        Perform the fake render pages operation under explicit file-format and conversion rules.

        Example:
            Exercise test process pages serial path uses local renderer.fake render pages through a consuming regression::

                python -m pytest -q tests/file_formats/comic/test_comic_modernized.py


        :param tasks: Value supplied for tasks under the utility contract.
        :param dest: Value supplied for dest under the utility contract.
        :param opts: Value supplied for opts under the utility contract.
        :param notification: Value supplied for notification under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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

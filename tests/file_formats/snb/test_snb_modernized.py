from __future__ import annotations

import importlib
import sys
import types
from pathlib import Path


class _Log:
    def __init__(self) -> None:
        self.messages: list[tuple[str, str]] = []

    def _record(self, level: str, *parts) -> None:
        self.messages.append((level, " ".join(str(x) for x in parts)))

    def __call__(self, *parts) -> None:
        self._record("call", *parts)

    def debug(self, *parts) -> None:
        self._record("debug", *parts)

    def info(self, *parts) -> None:
        self._record("info", *parts)

    def warn(self, *parts) -> None:
        self._record("warn", *parts)

    def warning(self, *parts) -> None:
        self._record("warning", *parts)

    def error(self, *parts) -> None:
        self._record("error", *parts)


def _fixture_payloads() -> dict[str, bytes]:
    return {
        "snbf/book.snbf": (
            "<book-snbf><head><name>SNB Unicode Ω Test</name><author>Alice</author>"
            "<language>en</language><generator>LiuXin</generator><publisher>Unit Test</publisher>"
            "<cover>images_cover.jpg</cover></head></book-snbf>"
        ).encode("utf-8"),
        "snbf/toc.snbf": (
            "<toc-snbf><head/><body><chapter src='chapter_1.snbc'>Chapter 1</chapter></body></toc-snbf>"
        ).encode("utf-8"),
        "snbc/chapter_1.snbc": (
            "<snbc><head><title>Chapter 1</title></head><body><text>Hello 世界</text>"
            "<img>images_cover.jpg</img></body></snbc>"
        ).encode("utf-8"),
        "snbc/images/cover.jpg": b"\xff\xd8\xff\xe0fake-jpeg",
        "snbc/images/figure.png": b"\x89PNG\r\n\x1a\nfake-png",
    }


def _write_tree(base: Path, payloads: dict[str, bytes]) -> Path:
    root = base / "snb_src"
    root.mkdir()
    for rel, data in payloads.items():
        p = root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)
    return root


def test_snb_modules_import_smoke() -> None:
    modules = (
        "LiuXin_alpha.file_formats.snb",
        "LiuXin_alpha.file_formats.snb.snbfile",
        "LiuXin_alpha.file_formats.snb.snbml",
        "LiuXin_alpha.file_formats.conversion.plugins.snb_input",
        "LiuXin_alpha.file_formats.conversion.plugins.snb_output",
    )
    for module_name in modules:
        importlib.import_module(module_name)


def test_snb_process_file_name_normalization() -> None:
    mod = importlib.import_module("LiuXin_alpha.file_formats.snb.snbml")
    out = mod.ProcessFileName("Dir\\Sub/Chapter#1.PNG")
    assert out == "dir_sub_chapter_1.jpg"


def test_snbfile_roundtrip_parse_and_extract_images(tmp_path: Path) -> None:
    mod = importlib.import_module("LiuXin_alpha.file_formats.snb.snbfile")
    payloads = _fixture_payloads()
    src_root = _write_tree(tmp_path, payloads)
    out_snb = tmp_path / "book.snb"

    writer = mod.SNBFile()
    writer.FromDir(str(src_root))
    writer.Output(str(out_snb))

    parsed = mod.SNBFile(str(out_snb))
    assert parsed.IsValid()
    for rel, expected in payloads.items():
        assert parsed.GetFileStream(rel) == expected

    out_images = tmp_path / "images"
    out_images.mkdir()
    images = sorted(parsed.OutputImageFiles(str(out_images)))
    assert [name for name, _mime in images] == ["cover.jpg", "figure.png"]
    assert (out_images / "cover.jpg").read_bytes() == payloads["snbc/images/cover.jpg"]
    assert (out_images / "figure.png").read_bytes() == payloads["snbc/images/figure.png"]


def test_snbfile_output_is_deterministic_independent_of_append_order(tmp_path: Path) -> None:
    mod = importlib.import_module("LiuXin_alpha.file_formats.snb.snbfile")
    payloads = _fixture_payloads()
    src_root = _write_tree(tmp_path, payloads)

    sample = [
        "snbc/chapter_1.snbc",
        "snbf/toc.snbf",
        "snbc/images/figure.png",
        "snbf/book.snbf",
        "snbc/images/cover.jpg",
    ]

    a = mod.SNBFile()
    for rel in sample:
        if rel.endswith((".snbf", ".snbc")):
            a.AppendPlain(rel, str(src_root))
        else:
            a.AppendBinary(rel, str(src_root))

    b = mod.SNBFile()
    for rel in reversed(sample):
        if rel.endswith((".snbf", ".snbc")):
            b.AppendPlain(rel, str(src_root))
        else:
            b.AppendBinary(rel, str(src_root))

    out_a = tmp_path / "a.snb"
    out_b = tmp_path / "b.snb"
    a.Output(str(out_a))
    b.Output(str(out_b))
    assert out_a.read_bytes() == out_b.read_bytes()


def test_snb_input_convert_end_to_end(tmp_path: Path, monkeypatch) -> None:
    snbfile_mod = importlib.import_module("LiuXin_alpha.file_formats.snb.snbfile")
    snb_input_mod = importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.snb_input")
    plumber_mod = importlib.import_module("LiuXin_alpha.file_formats.conversion.plumber")

    src_root = _write_tree(tmp_path, _fixture_payloads())
    out_snb = tmp_path / "input.snb"
    snb = snbfile_mod.SNBFile()
    snb.FromDir(str(src_root))
    snb.Output(str(out_snb))

    class _Metadata:
        def __init__(self) -> None:
            self.identifier = []
            self.title = []

        def add(self, name, value, attrib=None, **kwargs):
            attrs = dict(attrib or {})
            attrs.update(kwargs)
            if name == "identifier":
                self.identifier.append(types.SimpleNamespace(attrib=attrs, value=value))
            elif name == "title":
                self.title = [value]

    class _Guide:
        def __init__(self) -> None:
            self.items = []

        def add(self, kind, title, href):
            self.items.append((kind, title, href))

    class _TOC:
        def __init__(self) -> None:
            self.items = []

        def add(self, title, href):
            self.items.append((title, href))

    class _Manifest:
        def __init__(self) -> None:
            self.items = []
            self._i = 0

        def generate(self, id="item", href="item"):
            self._i += 1
            return f"{id}_{self._i}", href

        def add(self, item_id, href, media_type):
            item = types.SimpleNamespace(id=item_id, href=href, media_type=media_type, html_input_href=None)
            self.items.append(item)
            return item

    class _Spine:
        def __init__(self) -> None:
            self.items = []

        def add(self, item, linear):
            self.items.append((item, linear))

        def __len__(self):
            return len(self.items)

    fake_oeb = types.SimpleNamespace(
        metadata=_Metadata(),
        guide=_Guide(),
        toc=_TOC(),
        manifest=_Manifest(),
        spine=_Spine(),
        container=None,
        uid=None,
    )
    monkeypatch.setattr(plumber_mod, "create_oebbook", lambda *a, **k: fake_oeb)

    plugin = snb_input_mod.SNBInput(None)
    opts = types.SimpleNamespace(input_encoding=None)
    log = _Log()

    with out_snb.open("rb") as stream:
        oeb = plugin.convert(stream, opts, "snb", log, {})

    assert len(oeb.spine) >= 1
    assert str(oeb.metadata.title[0]) == "SNB Unicode Ω Test"


def test_snb_output_handle_image_invalid_data_is_non_fatal(monkeypatch, tmp_path: Path) -> None:
    mod = importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.snb_output")

    class _FakeImage:
        def load(self, _data):
            raise ValueError("invalid image")

    monkeypatch.setitem(sys.modules, "LiuXin_alpha.utils.magick", types.SimpleNamespace(Image=_FakeImage))

    plugin = mod.SNBOutput(None)
    plugin.opts = None
    out_path = tmp_path / "cover.jpg"
    plugin.HandleImage(b"broken", str(out_path))
    assert not out_path.exists()

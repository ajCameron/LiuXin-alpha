from __future__ import annotations

import sys
import tempfile
import types

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any

EXAMPLES_ROOT = Path(__file__).resolve().parents[1]
if str(EXAMPLES_ROOT) not in sys.path:
    sys.path.insert(0, str(EXAMPLES_ROOT))

from _example_utils import bootstrap_src_path, dump_json

bootstrap_src_path()

from LiuXin_alpha.file_formats.conversion.plugins.oeb_output import OEBOutput
from LiuXin_alpha.file_formats.oeb.base import OEBBook
from LiuXin_alpha.file_formats.oeb.reader import OEBReader
from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.utils.ptempfiles import (
    get_base_scratch_folders,
    set_base_scratch_folders,
)


@contextmanager
def isolated_conversion_scratch() -> Iterator[Path]:
    """Route legacy conversion temporaries through an existing temp folder."""

    previous = get_base_scratch_folders()
    with tempfile.TemporaryDirectory(
        prefix="liuxin-alpha-conversion-scratch-"
    ) as scratch:
        set_base_scratch_folders(scratch)
        try:
            yield Path(scratch)
        finally:
            set_base_scratch_folders(previous)


class ExampleLog:
    def __init__(self, *, verbose: bool = False) -> None:
        self.verbose = verbose

    def __call__(self, *args, **kwargs) -> None:
        if self.verbose:
            print(*args)

    def info(self, *args, **kwargs) -> None:
        if self.verbose:
            print("[info]", *args)

    def debug(self, *args, **kwargs) -> None:
        if self.verbose:
            print("[debug]", *args)

    def warning(self, *args, **kwargs) -> None:
        if self.verbose:
            print("[warning]", *args)

    warn = warning

    def error(self, *args, **kwargs) -> None:
        if self.verbose:
            print("[error]", *args)

    def exception(self, *args, **kwargs) -> None:
        if self.verbose:
            print("[exception]", *args)


class MetadataStub:
    def __init__(
        self,
        *,
        title: str = "Unknown",
        authors: list[str] | None = None,
        cover: str | None = None,
    ) -> None:
        self.title = title
        self.authors = list(authors or ["Unknown"])
        self.title_sort = None
        self.author_sort = None
        self.book_producer = None
        self.comments = None
        self.publisher = None
        self.series = None
        self.series_index = None
        self.rating = None
        self.tags: list[str] = []
        self.pubdate = None
        self.timestamp = None
        self.rights = None
        self.publication_type = None
        self.languages = ["en"]
        self.cover = cover
        self.cover_data = None
        self.uuid = None
        self.application_id = None
        self.isbn = None
        self.identifiers: dict[str, str] = {}

    def is_null(self, name: str) -> bool:
        value = getattr(self, name, None)
        if value is None:
            return True
        if isinstance(value, str):
            return value.strip() == ""
        if isinstance(value, (list, tuple, set, frozenset, dict)):
            return len(value) == 0
        return False

    def get_identifiers(self) -> dict[str, str]:
        identifiers = dict(self.identifiers)
        if self.isbn:
            identifiers.setdefault("isbn", self.isbn)
        return identifiers

    def format_series_index(self) -> str:
        if self.series_index is None:
            return "1"
        return str(self.series_index)


def install_customize_ui_stub(*, html_input_support: bool = False) -> None:
    fake_ui = types.ModuleType("LiuXin_alpha.customize.ui")
    cbz_plugin_sentinel = object()

    def plugin_for_output_format(fmt: str):
        if fmt == "oeb":
            return OEBOutput(None)
        raise ValueError(f"Unsupported output format in example stub: {fmt!r}")

    def plugin_for_input_format(fmt: str):
        if fmt == "cbz":
            return cbz_plugin_sentinel
        if html_input_support and fmt == "html":
            from LiuXin_alpha.file_formats.conversion.plugins.html_input import HTMLInput

            return HTMLInput(None)
        return object()

    def get_file_type_metadata(stream: Any, file_ext: str, calibre: bool = True) -> MetadataStub:
        stream_name = Path(getattr(stream, "name", "input")).name
        stem = Path(stream_name).stem or "Unknown"
        return MetadataStub(title=stem, authors=["Unknown"])

    fake_ui.plugin_for_output_format = plugin_for_output_format
    fake_ui.plugin_for_input_format = plugin_for_input_format
    fake_ui.get_file_type_metadata = get_file_type_metadata
    fake_ui.run_plugins_on_import = lambda *args, **kwargs: None
    fake_ui.run_plugins_on_postimport = lambda *args, **kwargs: None
    fake_ui.run_plugins_on_postadd = lambda *args, **kwargs: None
    fake_ui.run_import_plugins = lambda *args, **kwargs: None
    sys.modules["LiuXin_alpha.customize.ui"] = fake_ui


def conversion_profile() -> SimpleNamespace:
    fnums = {8: 3, 10: 4, 12: 5, 14: 6, 16: 7}
    return SimpleNamespace(
        width=600,
        height=800,
        dpi=96,
        fbase=16,
        fnums=fnums,
        mobi_ems_per_blockquote=2,
        short_name="default",
        epub_periodical_format="sony",
    )


def make_epub_output_opts(*, extract_to: str | None = None) -> SimpleNamespace:
    profile = conversion_profile()
    return SimpleNamespace(
        epub_inline_toc=False,
        epub_toc_at_end=False,
        epub_flatten=False,
        dont_split_on_page_breaks=False,
        flow_size=260,
        no_default_epub_cover=False,
        no_svg_cover=False,
        preserve_cover_aspect_ratio=False,
        pretty_print=False,
        extract_to=extract_to,
        output_profile=profile,
        mobi_toc_at_start=False,
        mobi_passthrough=False,
        no_inline_toc=False,
        toc_title=None,
        expand_css=False,
        source=profile,
        dest=profile,
        margin_left=5,
        margin_right=5,
        margin_top=5,
        margin_bottom=5,
        line_height=0,
        remove_paragraph_spacing=False,
        remove_paragraph_spacing_indent_size=1.5,
        insert_blank_line=False,
        insert_blank_line_size=0.5,
        keep_ligatures=False,
        subset_embedded_fonts=False,
        embed_all_fonts=False,
        minimum_line_height=120.0,
        change_justification="original",
        html_unwrap_factor=0.4,
        base_font_size=0.0,
        disable_font_rescaling=False,
        font_size_mapping="12,12,12,12,12,12,12,12",
        sr1_search="",
        sr1_replace="",
        sr2_search="",
        sr2_replace="",
        sr3_search="",
        sr3_replace="",
        transform_css_rules="",
        extra_css="",
    )


def make_mobi_output_opts(*, mobi_file_type: str = "old", extract_to: str | None = None) -> SimpleNamespace:
    profile = conversion_profile()
    return SimpleNamespace(
        mobi_file_type=mobi_file_type,
        prefer_author_sort=False,
        no_inline_toc=False,
        toc_title=None,
        dont_compress=False,
        mobi_ignore_margins=False,
        mobi_toc_at_start=False,
        extract_to=extract_to,
        share_not_sync=False,
        mobi_keep_original_images=False,
        linearize_tables=False,
        pretty_print=False,
        mobi_periodical=False,
        expand_css=False,
        source=profile,
        dest=profile,
    )


def resolve_oeb_input(input_opf: str | None, *, workspace: Path) -> tuple[Path, bool]:
    if input_opf:
        resolved = Path(input_opf).expanduser().resolve()
        if not resolved.exists():
            raise FileNotFoundError(f"Input OPF does not exist: {resolved}")
        if not resolved.is_file():
            raise ValueError(f"Input OPF must be a file: {resolved}")
        return resolved, False

    sample_root = workspace / "sample_oeb"
    sample_root.mkdir(parents=True, exist_ok=True)
    return write_demo_oeb(sample_root), True


def write_demo_oeb(root: Path) -> Path:
    title = "Example Conversion Title — Καλημέρα — 你好"
    author = "Example Author"
    chapter = """<html xmlns="http://www.w3.org/1999/xhtml">
  <head><title>Example Conversion</title></head>
  <body>
    <h1 id="top">Example Conversion</h1>
    <p>This file was generated by LiuXin_alpha examples.</p>
    <p>Unicode smoke: naïve café — Здравствуйте — مرحبا — नमस्ते — こんにちは — 🙂</p>
  </body>
</html>
"""
    opf = f"""<?xml version="1.0" encoding="utf-8"?>
<package xmlns="http://www.idpf.org/2007/opf" version="2.0" unique-identifier="BookId">
  <metadata xmlns:dc="http://purl.org/dc/elements/1.1/">
    <dc:title>{title}</dc:title>
    <dc:creator xmlns:opf="http://www.idpf.org/2007/opf" opf:role="aut">{author}</dc:creator>
    <dc:language>en</dc:language>
    <dc:identifier id="BookId">urn:uuid:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee</dc:identifier>
  </metadata>
  <manifest>
    <item id="chap" href="chapter.xhtml" media-type="application/xhtml+xml"/>
    <item id="ncx" href="toc.ncx" media-type="application/x-dtbncx+xml"/>
  </manifest>
  <spine toc="ncx">
    <itemref idref="chap"/>
  </spine>
</package>
"""
    ncx = """<?xml version="1.0"?>
<ncx xmlns="http://www.daisy.org/z3986/2005/ncx/" version="2005-1">
  <head/>
  <docTitle><text>Example Conversion</text></docTitle>
  <navMap>
    <navPoint id="n1" playOrder="1">
      <navLabel><text>Start</text></navLabel>
      <content src="chapter.xhtml#top"/>
    </navPoint>
  </navMap>
</ncx>
"""
    (root / "chapter.xhtml").write_text(chapter, encoding="utf-8")
    (root / "toc.ncx").write_text(ncx, encoding="utf-8")
    opf_path = root / "metadata.opf"
    opf_path.write_text(opf, encoding="utf-8")
    return opf_path


def load_oeb_from_opf(opf_path: Path) -> OEBBook:
    oeb = OEBBook(default_log, lambda x: x)
    OEBReader()(oeb, str(opf_path))
    return oeb

from __future__ import annotations

from dataclasses import dataclass, field
from types import SimpleNamespace
from urllib.parse import urldefrag

from lxml import etree

from LiuXin_alpha.file_formats.oeb.base import XHTML


@dataclass
class MinimalTOCNode:
    title: str
    href: str
    nodes: list["MinimalTOCNode"] = field(default_factory=list)


@dataclass
class MinimalOEBItem:
    href: str
    data: object | None = None
    media_type: str = "application/xhtml+xml"

    def abshref(self, url: str) -> str:
        if "://" in url:
            return url
        if url.startswith("#"):
            return f"{self.href}{url}"
        href, fragment = urldefrag(url)
        if fragment:
            return f"{href}#{fragment}"
        return href


@dataclass
class MinimalOEBBook:
    spine: list[MinimalOEBItem]
    manifest: list[MinimalOEBItem]
    toc: list[MinimalTOCNode]


class NullStyle:
    marginTop = 0
    marginBottom = 0
    fontSize = 12
    width = 600

    _defaults = {
        "display": "inline",
        "visibility": "visible",
        "font-style": "normal",
        "font-weight": "normal",
        "text-decoration": "none",
        "font-variant": "normal",
        "text-align": "auto",
        "vertical-align": "middle",
        "color": "black",
    }

    def __getitem__(self, key: str) -> str:
        return self._defaults.get(key, "auto")

    def cssdict(self) -> dict[str, str]:
        return {}


class NullStylizer:
    def __init__(self, root, href, oeb_book, opts, output_profile=None):
        self.profile = output_profile or getattr(opts, "output_profile", SimpleNamespace(dpi=96, fbase=12))

    def style(self, elem) -> NullStyle:
        return NullStyle()


def null_log():
    return SimpleNamespace(info=lambda *a, **k: None, debug=lambda *a, **k: None)


def text_output_options(**overrides):
    values = {
        "txt_output_formatting": "plain",
        "newline": "unix",
        "txt_output_encoding": "utf-8",
        "inline_toc": False,
        "remove_paragraph_spacing": False,
        "max_line_length": 0,
        "force_max_line_length": False,
        "keep_links": True,
        "keep_image_references": True,
        "keep_color": False,
        "unsmarten_punctuation": False,
        "output_profile": SimpleNamespace(dpi=96, fbase=12),
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def build_xhtml_document(body_markup: str):
    source = (
        '<html xmlns="http://www.w3.org/1999/xhtml">'
        "<head><title>Serializer</title></head>"
        f"<body>{body_markup}</body>"
        "</html>"
    )
    return etree.fromstring(source.encode("utf-8"))


def build_text_output_book() -> MinimalOEBBook:
    body = """
    <h1 id="intro">Shared Καλημέρα 世界 👩🏽‍💻</h1>
    <p>
      Latin café naïve coöperate façade déjà vu.
      Greek Καλημέρα κόσμε.
      Cyrillic Здравствуйте, мир.
      Arabic مرحبا بالعالم.
      Hebrew שלום עולם.
      Hindi नमस्ते दुनिया.
      Thai สวัสดีโลก.
      CJK 你好，世界 / こんにちは世界 / 안녕하세요 세계.
      Emoji 👩🏽‍💻🧪📚🧬.
      Combining café cöoperate Å.
      Bidi ‏مرحبا‏ and ZWJ A‍B.
    </p>
    <p>
      Styled <strong>bold Ω</strong>, <em>italic שלום</em>,
      <code>code_世界</code>, and
      <a href="https://example.com/路径?鍵=值" title="资料">参照</a>.
      <img src="cover.png" alt="封面 世界" />
    </p>
    <ul>
      <li>First नमस्ते</li>
      <li>Second สวัสดีโลก</li>
    </ul>
    <blockquote><p>Quote مرحبا</p></blockquote>
    <table><tr><th>Head</th><td>你好，世界</td></tr></table>
    <pre>pre café
cöoperate</pre>
    """
    chapter = MinimalOEBItem("chapter.xhtml", build_xhtml_document(body))
    image = MinimalOEBItem("cover.png", data=b"png", media_type="image/png")
    toc = [MinimalTOCNode("Shared Καλημέρα 世界 👩🏽‍💻", "chapter.xhtml#intro")]
    return MinimalOEBBook(spine=[chapter], manifest=[chapter, image], toc=toc)


def install_minimal_stylizers(monkeypatch) -> None:
    import LiuXin_alpha.file_formats.oeb.stylizer as oeb_stylizer
    import LiuXin_alpha.file_formats.txt.markdownml as markdownml
    import LiuXin_alpha.file_formats.txt.textileml as textileml

    monkeypatch.setattr(oeb_stylizer, "Stylizer", NullStylizer)
    monkeypatch.setattr(markdownml, "Stylizer", NullStylizer)
    monkeypatch.setattr(textileml, "Stylizer", NullStylizer)

"""
Build deterministic OEB fixtures and test doubles.

The module keeps generated data, ordering and failure modes explicit so consumers
can assert stable behavior.

Example:
    Exercise file format oeb through a consuming regression::

        python -m pytest -q tests/metadata/file_sources/test_opf_metadata_source.py
"""
from __future__ import annotations

from dataclasses import dataclass, field
from types import SimpleNamespace
from urllib.parse import urldefrag

from lxml import etree

from LiuXin_alpha.file_formats.oeb.base import XHTML
from tests.support.file_format_unicode import MULTISCRIPT_TEXT


@dataclass
class MinimalTOCNode:
    """
    Provide the MinimalTOCNode test double with a deliberately bounded compatibility surface.

    Example:
        Exercise MinimalTOCNode through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_opf_metadata_source.py
    """
    title: str
    href: str
    nodes: list["MinimalTOCNode"] = field(default_factory=list)


@dataclass
class MinimalOEBItem:
    """
    Provide the MinimalOEBItem test double with a deliberately bounded compatibility surface.

    Example:
        Exercise MinimalOEBItem through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_opf_metadata_source.py
    """
    href: str
    data: object | None = None
    media_type: str = "application/xhtml+xml"
    id: str | None = None
    spine_position: int | None = None
    linear: bool = True
    page_breaks: list[object] = field(default_factory=list)

    def abshref(self, url: str) -> str:
        """
        Perform the abshref step with deterministic fixture inputs.

        Example:
            Exercise MinimalOEBItem.abshref through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_opf_metadata_source.py


        :param url: Value supplied for url under the deterministic fixture contract.
        :return: The deterministic fixture value, path, bytes, record or collection
            described above.
        """
        if "://" in url:
            return url
        if url.startswith("#"):
            return f"{self.href}{url}"
        href, fragment = urldefrag(url)
        if fragment:
            return f"{href}#{fragment}"
        return href


class MinimalManifest(list):
    """
    Provide the MinimalManifest test double with a deliberately bounded compatibility surface.

    Example:
        Exercise MinimalManifest through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_opf_metadata_source.py
    """
    def __init__(self, items=()):
        """
        Initialize and validate the MinimalManifest test-support state.

        Example:
            Exercise MinimalManifest.  init   through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_opf_metadata_source.py


        :param items: Value supplied for items under the deterministic fixture contract.
        :return: None; completion is expressed through state changes or assertions.
        """
        super().__init__(items)
        self.ids = {}
        self.hrefs = {}
        for index, item in enumerate(self):
            item_id = item.id or "item%d" % index
            self.ids[item_id] = item
            self.hrefs[item.href] = item

    def values(self):
        """
        Perform the values step with deterministic fixture inputs.

        Example:
            Exercise MinimalManifest.values through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_opf_metadata_source.py


        :return: The deterministic fixture value, path, bytes, record or collection
            described above.
        """
        return list(self)


@dataclass
class MinimalMetadataValue:
    """
    Provide the MinimalMetadataValue test double with a deliberately bounded compatibility surface.

    Example:
        Exercise MinimalMetadataValue through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_opf_metadata_source.py
    """
    value: str

    def __str__(self) -> str:
        """
        Perform the str step with deterministic fixture inputs.

        Example:
            Exercise MinimalMetadataValue.  str   through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_opf_metadata_source.py


        :return: The deterministic fixture value, path, bytes, record or collection
            described above.
        """
        return self.value

    def get(self, key, default=None):
        """
        Perform the get step with deterministic fixture inputs.

        Example:
            Exercise MinimalMetadataValue.get through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_opf_metadata_source.py


        :param key: Value supplied for key under the deterministic fixture contract.
        :param default: Value supplied for default under the deterministic fixture contract.
        :return: The deterministic fixture value, path, bytes, record or collection
            described above.
        """
        return default


class MinimalIdentifier(MinimalMetadataValue):
    """
    Provide the MinimalIdentifier test double with a deliberately bounded compatibility surface.

    Example:
        Exercise MinimalIdentifier through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_opf_metadata_source.py
    """
    def __init__(self, value: str, scheme: str | None = None) -> None:
        """
        Initialize and validate the MinimalIdentifier test-support state.

        Example:
            Exercise MinimalIdentifier.  init   through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_opf_metadata_source.py


        :param value: Fixture value normalized, encoded, stored or returned.
        :param scheme: Value supplied for scheme under the deterministic fixture contract.
        :return: None; completion is expressed through state changes or assertions.
        """
        super().__init__(value)
        self.scheme = scheme

    def get(self, key, default=None):
        """
        Perform the get step with deterministic fixture inputs.

        Example:
            Exercise MinimalIdentifier.get through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_opf_metadata_source.py


        :param key: Value supplied for key under the deterministic fixture contract.
        :param default: Value supplied for default under the deterministic fixture contract.
        :return: The deterministic fixture value, path, bytes, record or collection
            described above.
        """
        if self.scheme and str(key).lower().endswith("scheme"):
            return self.scheme
        return default


class MinimalMetadata:
    """
    Provide the MinimalMetadata test double with a deliberately bounded compatibility surface.

    Example:
        Exercise MinimalMetadata through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_opf_metadata_source.py
    """
    def __init__(
        self,
        *,
        title: str,
        creators: tuple[str, ...],
        language: str = "en",
        subjects: tuple[str, ...] = (),
        publisher: str | None = None,
        date: str | None = None,
        identifiers: tuple[MinimalIdentifier, ...] = (),
        cover: str | None = None,
        series: str | None = None,
        series_index: str | None = None,
    ) -> None:
        """
        Initialize and validate the MinimalMetadata test-support state.

        Example:
            Exercise MinimalMetadata.  init   through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_opf_metadata_source.py


        :param title: Value supplied for title under the deterministic fixture contract.
        :param creators: Value supplied for creators under the deterministic fixture
            contract.
        :param language: Value supplied for language under the deterministic fixture
            contract.
        :param subjects: Value supplied for subjects under the deterministic fixture
            contract.
        :param publisher: Value supplied for publisher under the deterministic fixture
            contract.
        :param date: Value supplied for date under the deterministic fixture contract.
        :param identifiers: Value supplied for identifiers under the deterministic fixture
            contract.
        :param cover: Value supplied for cover under the deterministic fixture contract.
        :param series: Value supplied for series under the deterministic fixture contract.
        :param series_index: Value supplied for series index under the deterministic fixture
            contract.
        :return: None; completion is expressed through state changes or assertions.
        """
        self.title = [MinimalMetadataValue(title)]
        self.creator = [MinimalMetadataValue(creator) for creator in creators]
        self.language = [MinimalMetadataValue(language)] if language else []
        self.subject = [MinimalMetadataValue(subject) for subject in subjects]
        self.publisher = [MinimalMetadataValue(publisher)] if publisher else []
        self.date = [MinimalMetadataValue(date)] if date else []
        self.identifier = list(identifiers)
        self.cover = [cover] if cover else []
        self.series = [MinimalMetadataValue(series)] if series else []
        self.series_index = [series_index] if series_index else []

    def __getitem__(self, key: str):
        """
        Perform the getitem step with deterministic fixture inputs.

        Example:
            Exercise MinimalMetadata.  getitem   through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_opf_metadata_source.py


        :param key: Value supplied for key under the deterministic fixture contract.
        :return: The deterministic fixture value, path, bytes, record or collection
            described above.
        """
        return getattr(self, key, [])


@dataclass
class MinimalOEBBook:
    """
    Provide the MinimalOEBBook test double with a deliberately bounded compatibility surface.

    Example:
        Exercise MinimalOEBBook through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_opf_metadata_source.py
    """
    spine: list[MinimalOEBItem]
    manifest: list[MinimalOEBItem]
    toc: list[MinimalTOCNode]
    guide: dict[str, object] = field(default_factory=dict)
    metadata: object = field(
        default_factory=lambda: SimpleNamespace(
            title=[SimpleNamespace(value="Unicode Καλημέρα 世界 👩🏽‍💻")],
            creator=[SimpleNamespace(value="José Иван")],
        )
    )


class NullStyle:
    """
    Provide the NullStyle test double with a deliberately bounded compatibility surface.

    Example:
        Exercise NullStyle through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_opf_metadata_source.py
    """
    marginTop = 0
    marginBottom = 0
    fontSize = 12
    width = 600

    def __init__(self, overrides: dict[str, str] | None = None) -> None:
        """
        Initialize and validate the NullStyle test-support state.

        Example:
            Exercise NullStyle.  init   through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_opf_metadata_source.py


        :param overrides: Value supplied for overrides under the deterministic fixture
            contract.
        :return: None; completion is expressed through state changes or assertions.
        """
        self._overrides = overrides or {}

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
        """
        Perform the getitem step with deterministic fixture inputs.

        Example:
            Exercise NullStyle.  getitem   through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_opf_metadata_source.py


        :param key: Value supplied for key under the deterministic fixture contract.
        :return: The deterministic fixture value, path, bytes, record or collection
            described above.
        """
        if key in self._overrides:
            return self._overrides[key]
        return self._defaults.get(key, "auto")

    def cssdict(self) -> dict[str, str]:
        """
        Perform the cssdict step with deterministic fixture inputs.

        Example:
            Exercise NullStyle.cssdict through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_opf_metadata_source.py


        :return: The deterministic fixture value, path, bytes, record or collection
            described above.
        """
        return {}


class NullStylizer:
    """
    Provide the NullStylizer test double with a deliberately bounded compatibility surface.

    Example:
        Exercise NullStylizer through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_opf_metadata_source.py
    """
    def __init__(self, root, href, oeb_book, opts, output_profile=None):
        """
        Initialize and validate the NullStylizer test-support state.

        Example:
            Exercise NullStylizer.  init   through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_opf_metadata_source.py


        :param root: Root directory containing the fixture corpus or generated tree.
        :param href: Value supplied for href under the deterministic fixture contract.
        :param oeb_book: Value supplied for oeb book under the deterministic fixture
            contract.
        :param opts: Value supplied for opts under the deterministic fixture contract.
        :param output_profile: Value supplied for output profile under the deterministic
            fixture contract.
        :return: None; completion is expressed through state changes or assertions.
        """
        self.profile = output_profile or getattr(opts, "output_profile", SimpleNamespace(dpi=96, fbase=12))

    def style(self, elem) -> NullStyle:
        """
        Perform the style step with deterministic fixture inputs.

        Example:
            Exercise NullStylizer.style through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_opf_metadata_source.py


        :param elem: Value supplied for elem under the deterministic fixture contract.
        :return: The deterministic fixture value, path, bytes, record or collection
            described above.
        """
        tag = etree.QName(elem).localname if isinstance(elem.tag, str) else ""
        if tag in {"b", "strong"}:
            return NullStyle({"font-weight": "bold"})
        if tag in {"i", "em", "cite"}:
            return NullStyle({"font-style": "italic"})
        return NullStyle()


def null_log():
    """
    Perform the null log step with deterministic fixture inputs.

    Example:
        Exercise null log through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_opf_metadata_source.py


    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    return SimpleNamespace(info=lambda *a, **k: None, debug=lambda *a, **k: None)


def text_output_options(**overrides):
    """
    Perform the text output options step with deterministic fixture inputs.

    Example:
        Exercise text output options through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_opf_metadata_source.py


    :param overrides: Value supplied for overrides under the deterministic fixture
        contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
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
    """
    Build xhtml document for deterministic fixture consumers.

    Example:
        Exercise build xhtml document through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_opf_metadata_source.py


    :param body_markup: Value supplied for body markup under the deterministic fixture
        contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    source = (
        '<html xmlns="http://www.w3.org/1999/xhtml">'
        "<head><title>Serializer</title></head>"
        f"<body>{body_markup}</body>"
        "</html>"
    )
    return etree.fromstring(source.encode("utf-8"))


def build_text_output_book() -> MinimalOEBBook:
    """
    Build text output book for deterministic fixture consumers.

    Example:
        Exercise build text output book through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_opf_metadata_source.py


    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
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


def build_rich_oeb_output_book(
    *,
    title: str = "OEB Output Καλημέρα 世界",
    creators: tuple[str, ...] = ("José Niño", "Иван Петров"),
    publisher: str = "Éditions Δ",
    subjects: tuple[str, ...] = ("Κατηγορία", "タグ", "cafe\u0301"),
    body_lines: tuple[str, ...] | None = None,
    include_image: bool = True,
) -> MinimalOEBBook:
    """
    Build rich oeb output book for deterministic fixture consumers.

    Example:
        Exercise build rich oeb output book through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_opf_metadata_source.py


    :param title: Value supplied for title under the deterministic fixture contract.
    :param creators: Value supplied for creators under the deterministic fixture
        contract.
    :param publisher: Value supplied for publisher under the deterministic fixture
        contract.
    :param subjects: Value supplied for subjects under the deterministic fixture
        contract.
    :param body_lines: Value supplied for body lines under the deterministic fixture
        contract.
    :param include_image: Value supplied for include image under the deterministic
        fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    lines = body_lines or tuple(MULTISCRIPT_TEXT.splitlines())
    paragraphs = "\n".join("<p>%s</p>" % line for line in lines)
    image_markup = '<p><img src="images/cover_世界.png" alt="封面 世界" /></p>' if include_image else ""
    body = """
    <h1 id="intro">%s</h1>
    %s
    <p>Styled <strong>bold Ω</strong> and <em>italic שלום</em>.</p>
    %s
    """ % (
        title,
        paragraphs,
        image_markup,
    )
    chapter = MinimalOEBItem("chapter.xhtml", build_xhtml_document(body), id="chapter")
    image = MinimalOEBItem(
        "images/cover_世界.png",
        data=b"\x89PNG\r\n\x1a\nfb2-output-cover",
        media_type="image/png",
        id="cover-image",
    )
    manifest_items = [chapter] + ([image] if include_image else [])
    toc = [MinimalTOCNode(title, "chapter.xhtml#intro")]
    metadata = MinimalMetadata(
        title=title,
        creators=creators,
        publisher=publisher,
        subjects=subjects,
        date="2026-05-21",
        identifiers=(
            MinimalIdentifier("urn:uuid:33333333-4444-5555-6666-777777777777", "uuid"),
            MinimalIdentifier("9781402894626", "isbn"),
        ),
        cover="cover-image" if include_image else None,
        series="Series Καλημέρα",
        series_index="2",
    )
    return MinimalOEBBook(
        spine=[chapter],
        manifest=MinimalManifest(manifest_items),
        toc=toc,
        metadata=metadata,
    )


def install_minimal_stylizers(monkeypatch) -> None:
    """
    Perform the install minimal stylizers step with deterministic fixture inputs.

    Example:
        Exercise install minimal stylizers through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_opf_metadata_source.py


    :param monkeypatch: Pytest fixture used to isolate dependencies and mutable process
        state.
    :return: None; fixture state or the supplied destination is updated in place.
    """
    import LiuXin_alpha.file_formats.oeb.stylizer as oeb_stylizer
    import LiuXin_alpha.file_formats.txt.markdownml as markdownml
    import LiuXin_alpha.file_formats.txt.textileml as textileml

    monkeypatch.setattr(oeb_stylizer, "Stylizer", NullStylizer)
    monkeypatch.setattr(markdownml, "Stylizer", NullStylizer)
    monkeypatch.setattr(textileml, "Stylizer", NullStylizer)

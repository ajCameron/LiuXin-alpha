"""
Build deterministic HTMLZ fixtures and test doubles.

The module keeps generated data, ordering and failure modes explicit so consumers
can assert stable behavior.

Example:
    Exercise file format htmlz through a consuming regression::

        python -m pytest -q tests/metadata/file_sources/test_html_metadata_source.py
"""
from __future__ import annotations

import binascii
import struct
import sys
import types
import zipfile
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence
from xml.sax.saxutils import escape

from tests.support.file_format_unicode import COMMON_TEXT_FRAGMENTS, MULTISCRIPT_TEXT


HTMLZ_TITLE = "HTMLZ Καλημέρα 世界"
HTMLZ_AUTHORS = ("José", "Иван")
HTMLZ_DESCRIPTION = "HTMLZ container description: مرحبا שלום नमस्ते 你好 cafe\u0301"
HTMLZ_IDENTIFIER = "urn:uuid:66666666-7777-8888-9999-aaaaaaaaaaaa"
HTMLZ_PUBLISHER = "Éditions Δ"
HTMLZ_SUBJECT = "containers, unicode, Κατηγορία, タグ"
HTMLZ_HTML_MEMBER = "index.html"
HTMLZ_OPF_MEMBER = "metadata.opf"
HTMLZ_CSS_MEMBER = "styles/main_κόσμος.css"
HTMLZ_IMAGE_MEMBER = "images/深/cover_世界.png"


@dataclass(frozen=True)
class HTMLZFixture:
    """
    Carry the deterministic HTMLZFixture inputs and expected values used by format tests.

    Example:
        Exercise HTMLZFixture through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_html_metadata_source.py
    """
    path: Path
    html_member: str
    opf_member: str | None
    css_members: tuple[str, ...]
    asset_members: tuple[str, ...]
    text_fragments: tuple[str, ...]


@dataclass(frozen=True)
class HTMLInputCall:
    """
    Represent the HTMLInputCall state used by deterministic test-support operations.

    Example:
        Exercise HTMLInputCall through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_html_metadata_source.py
    """
    name: str
    payload: bytes
    file_ext: str
    accelerators: dict


class NullLog:
    """
    Record or discard NullLog messages without requiring the production logging stack.

    Example:
        Exercise NullLog through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_html_metadata_source.py
    """
    def __init__(self) -> None:
        """
        Initialize and validate the NullLog test-support state.

        Example:
            Exercise NullLog.  init   through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_html_metadata_source.py


        :return: None; completion is expressed through state changes or assertions.
        """
        self.messages: list[str] = []

    def __call__(self, message: str = "", *args) -> None:
        """
        Execute the configured fixture builder or test double operation.

        Example:
            Exercise NullLog.  call   through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_html_metadata_source.py


        :param message: Diagnostic message recorded or discarded by the test logger.
        :param args: Positional arguments forwarded to the bounded test double.
        :return: None; fixture state or the supplied destination is updated in place.
        """
        self.messages.append(message % args if args else message)

    def debug(self, message: str = "", *args) -> None:
        """
        Record or discard a debug message for assertions without external logging.

        Example:
            Exercise NullLog.debug through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_html_metadata_source.py


        :param message: Diagnostic message recorded or discarded by the test logger.
        :param args: Positional arguments forwarded to the bounded test double.
        :return: None; fixture state or the supplied destination is updated in place.
        """
        self(message, *args)

    def info(self, message: str = "", *args) -> None:
        """
        Record or discard a info message for assertions without external logging.

        Example:
            Exercise NullLog.info through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_html_metadata_source.py


        :param message: Diagnostic message recorded or discarded by the test logger.
        :param args: Positional arguments forwarded to the bounded test double.
        :return: None; fixture state or the supplied destination is updated in place.
        """
        self(message, *args)

    def warning(self, message: str = "", *args) -> None:
        """
        Record or discard a warning message for assertions without external logging.

        Example:
            Exercise NullLog.warning through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_html_metadata_source.py


        :param message: Diagnostic message recorded or discarded by the test logger.
        :param args: Positional arguments forwarded to the bounded test double.
        :return: None; fixture state or the supplied destination is updated in place.
        """
        self(message, *args)

    warn = warning

    def exception(self, message: str = "", *args) -> None:
        """
        Record or discard a exception message for assertions without external logging.

        Example:
            Exercise NullLog.exception through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_html_metadata_source.py


        :param message: Diagnostic message recorded or discarded by the test logger.
        :param args: Positional arguments forwarded to the bounded test double.
        :return: None; fixture state or the supplied destination is updated in place.
        """
        self(message, *args)


class RecordedManifest:
    """
    Provide the RecordedManifest test double with a deliberately bounded compatibility surface.

    Example:
        Exercise RecordedManifest through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_html_metadata_source.py
    """
    def __init__(self) -> None:
        """
        Initialize and validate the RecordedManifest test-support state.

        Example:
            Exercise RecordedManifest.  init   through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_html_metadata_source.py


        :return: None; completion is expressed through state changes or assertions.
        """
        self.generated: list[tuple[str, str]] = []
        self.added: list[types.SimpleNamespace] = []

    def generate(self, item_id: str, href: str) -> tuple[str, str]:
        """
        Perform the generate step with deterministic fixture inputs.

        Example:
            Exercise RecordedManifest.generate through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_html_metadata_source.py


        :param item_id: Value supplied for item id under the deterministic fixture contract.
        :param href: Value supplied for href under the deterministic fixture contract.
        :return: The deterministic fixture value, path, bytes, record or collection
            described above.
        """
        self.generated.append((item_id, href))
        return item_id, href

    def add(self, item_id: str, href: str, media_type: str, data: bytes | None = None):
        """
        Perform the add step with deterministic fixture inputs.

        Example:
            Exercise RecordedManifest.add through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_html_metadata_source.py


        :param item_id: Value supplied for item id under the deterministic fixture contract.
        :param href: Value supplied for href under the deterministic fixture contract.
        :param media_type: Value supplied for media type under the deterministic fixture
            contract.
        :param data: Bytes or structured data consumed by the operation.
        :return: The deterministic fixture value, path, bytes, record or collection
            described above.
        """
        item = types.SimpleNamespace(id=item_id, href=href, media_type=media_type, data=data)
        self.added.append(item)
        return item


class RecordedGuide:
    """
    Provide the RecordedGuide test double with a deliberately bounded compatibility surface.

    Example:
        Exercise RecordedGuide through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_html_metadata_source.py
    """
    def __init__(self) -> None:
        """
        Initialize and validate the RecordedGuide test-support state.

        Example:
            Exercise RecordedGuide.  init   through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_html_metadata_source.py


        :return: None; completion is expressed through state changes or assertions.
        """
        self.added: list[tuple[str, str, str]] = []

    def add(self, guide_type: str, title: str, href: str) -> None:
        """
        Perform the add step with deterministic fixture inputs.

        Example:
            Exercise RecordedGuide.add through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_html_metadata_source.py


        :param guide_type: Value supplied for guide type under the deterministic fixture
            contract.
        :param title: Value supplied for title under the deterministic fixture contract.
        :param href: Value supplied for href under the deterministic fixture contract.
        :return: None; fixture state or the supplied destination is updated in place.
        """
        self.added.append((guide_type, title, href))


class RecordedHTMLInput:
    """
    Provide the RecordedHTMLInput test double with a deliberately bounded compatibility surface.

    Example:
        Exercise RecordedHTMLInput through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_html_metadata_source.py
    """
    def __init__(self, returned_oeb) -> None:
        """
        Initialize and validate the RecordedHTMLInput test-support state.

        Example:
            Exercise RecordedHTMLInput.  init   through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_html_metadata_source.py


        :param returned_oeb: Value supplied for returned oeb under the deterministic fixture
            contract.
        :return: None; completion is expressed through state changes or assertions.
        """
        self.options = (
            _option("breadth_first", False),
            _option("max_levels", 5),
            _option("dont_package", False),
        )
        self.calls: list[HTMLInputCall] = []
        self.returned_oeb = returned_oeb

    def convert(self, stream, options, file_ext, log, accelerators):
        """
        Perform the convert step with deterministic fixture inputs.

        Example:
            Exercise RecordedHTMLInput.convert through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_html_metadata_source.py


        :param stream: Value supplied for stream under the deterministic fixture contract.
        :param options: Value supplied for options under the deterministic fixture contract.
        :param file_ext: Value supplied for file ext under the deterministic fixture
            contract.
        :param log: Value supplied for log under the deterministic fixture contract.
        :param accelerators: Value supplied for accelerators under the deterministic fixture
            contract.
        :return: The deterministic fixture value, path, bytes, record or collection
            described above.
        """
        self.calls.append(
            HTMLInputCall(
                name=stream.name,
                payload=stream.read(),
                file_ext=file_ext,
                accelerators=dict(accelerators),
            )
        )
        return self.returned_oeb


class HTMLZInputPipelineRecorder:
    """
    Represent the HTMLZInputPipelineRecorder state used by deterministic test-support operations.

    Example:
        Exercise HTMLZInputPipelineRecorder through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_html_metadata_source.py
    """
    def __init__(self, metadata_info=None) -> None:
        """
        Initialize and validate the HTMLZInputPipelineRecorder test-support state.

        Example:
            Exercise HTMLZInputPipelineRecorder.  init   through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_html_metadata_source.py


        :param metadata_info: Value supplied for metadata info under the deterministic
            fixture contract.
        :return: None; completion is expressed through state changes or assertions.
        """
        self.oeb = types.SimpleNamespace(
            metadata=types.SimpleNamespace(),
            manifest=RecordedManifest(),
            guide=RecordedGuide(),
        )
        self.html_input = RecordedHTMLInput(self.oeb)
        self.metadata_info = metadata_info or types.SimpleNamespace(
            title=HTMLZ_TITLE,
            authors=list(HTMLZ_AUTHORS),
        )
        self.metadata_calls: list[types.SimpleNamespace] = []
        self.metadata_transform_calls: list[types.SimpleNamespace] = []


def _option(name: str, recommended_value):
    """
    Perform the option step with deterministic fixture inputs.

    Example:
        Exercise  option through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_html_metadata_source.py


    :param name: Stable fixture, profile, member or field name.
    :param recommended_value: Value supplied for recommended value under the
        deterministic fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    return types.SimpleNamespace(
        option=types.SimpleNamespace(name=name),
        recommended_value=recommended_value,
    )


def install_htmlz_input_pipeline_stubs(monkeypatch, metadata_info=None) -> HTMLZInputPipelineRecorder:
    """
    Perform the install htmlz input pipeline stubs step with deterministic fixture inputs.

    Example:
        Exercise install htmlz input pipeline stubs through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_html_metadata_source.py


    :param monkeypatch: Pytest fixture used to isolate dependencies and mutable process
        state.
    :param metadata_info: Value supplied for metadata info under the deterministic
        fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    recorder = HTMLZInputPipelineRecorder(metadata_info=metadata_info)

    fake_ui = types.ModuleType("LiuXin_alpha.customize.ui")

    def plugin_for_input_format(fmt: str):
        """
        Perform the plugin for input format step with deterministic fixture inputs.

        Example:
            Exercise install htmlz input pipeline stubs.plugin for input format through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_html_metadata_source.py


        :param fmt: Value supplied for fmt under the deterministic fixture contract.
        :return: The deterministic fixture value, path, bytes, record or collection
            described above.
        """
        if fmt == "html":
            return recorder.html_input
        return None

    def get_file_type_metadata(stream, file_ext, *args, **kwargs):
        """
        Return file type metadata under the fixture contract.

        Example:
            Exercise install htmlz input pipeline stubs.get file type metadata through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_html_metadata_source.py


        :param stream: Value supplied for stream under the deterministic fixture contract.
        :param file_ext: Value supplied for file ext under the deterministic fixture
            contract.
        :param args: Positional arguments forwarded to the bounded test double.
        :param kwargs: Keyword arguments forwarded to the bounded test double or builder.
        :return: The deterministic fixture value, path, bytes, record or collection
            described above.
        """
        recorder.metadata_calls.append(
            types.SimpleNamespace(file_ext=file_ext, position=stream.tell())
        )
        return recorder.metadata_info

    fake_ui.plugin_for_input_format = plugin_for_input_format
    fake_ui.get_file_type_metadata = get_file_type_metadata

    fake_meta_transform = types.ModuleType("LiuXin_alpha.file_formats.oeb.transforms.metadata")

    def meta_info_to_oeb_metadata(mi, metadata, log) -> None:
        """
        Perform the meta info to oeb metadata step with deterministic fixture inputs.

        Example:
            Exercise install htmlz input pipeline stubs.meta info to oeb metadata through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_html_metadata_source.py


        :param mi: Value supplied for mi under the deterministic fixture contract.
        :param metadata: Value supplied for metadata under the deterministic fixture
            contract.
        :param log: Value supplied for log under the deterministic fixture contract.
        :return: None; fixture state or the supplied destination is updated in place.
        """
        recorder.metadata_transform_calls.append(
            types.SimpleNamespace(metadata_info=mi, metadata=metadata)
        )

    fake_meta_transform.meta_info_to_oeb_metadata = meta_info_to_oeb_metadata

    monkeypatch.setitem(sys.modules, "LiuXin_alpha.customize.ui", fake_ui)
    monkeypatch.setitem(
        sys.modules,
        "LiuXin_alpha.file_formats.oeb.transforms.metadata",
        fake_meta_transform,
    )
    return recorder


def png_bytes(width: int = 16, height: int = 16, rgb: tuple[int, int, int] = (70, 130, 170)) -> bytes:
    """
    Return deterministic PNG bytes for the requested dimensions and colour.

    Example:
        Exercise png bytes through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_html_metadata_source.py


    :param width: Image width in pixels.
    :param height: Image height in pixels.
    :param rgb: RGB colour embedded in the generated image.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    signature = b"\x89PNG\r\n\x1a\n"

    def chunk(tag: bytes, payload: bytes) -> bytes:
        """
        Return the encoded binary chunk required by the fixture container.

        Example:
            Exercise png bytes.chunk through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_html_metadata_source.py


        :param tag: Value supplied for tag under the deterministic fixture contract.
        :param payload: Binary or structured payload encoded into the fixture.
        :return: The deterministic fixture value, path, bytes, record or collection
            described above.
        """
        return (
            struct.pack(">I", len(payload))
            + tag
            + payload
            + struct.pack(">I", binascii.crc32(tag + payload) & 0xFFFFFFFF)
        )

    ihdr = struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0)
    row = bytes([0]) + bytes(rgb) * width
    raw = row * height
    return signature + chunk(b"IHDR", ihdr) + chunk(b"IDAT", zlib.compress(raw, 9)) + chunk(b"IEND", b"")


def _xml_text(text: str) -> str:
    """
    Perform the xml text step with deterministic fixture inputs.

    Example:
        Exercise  xml text through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_html_metadata_source.py


    :param text: Text encoded, parsed or embedded in the fixture.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    return escape(text, {'"': "&quot;"})


def build_unicode_htmlz(
    path: Path,
    *,
    lines: Sequence[str] | None = None,
    html_member: str = HTMLZ_HTML_MEMBER,
    opf_member: str | None = HTMLZ_OPF_MEMBER,
    include_css: bool = True,
    include_image: bool = True,
    extra_assets: Mapping[str, tuple[str, bytes]] | None = None,
) -> HTMLZFixture:
    """
    Build unicode htmlz for deterministic fixture consumers.

    Example:
        Exercise build unicode htmlz through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_html_metadata_source.py


    :param path: Filesystem path read, written or validated by the fixture operation.
    :param lines: Value supplied for lines under the deterministic fixture contract.
    :param html_member: Value supplied for html member under the deterministic fixture
        contract.
    :param opf_member: Value supplied for opf member under the deterministic fixture
        contract.
    :param include_css: Value supplied for include css under the deterministic fixture
        contract.
    :param include_image: Value supplied for include image under the deterministic
        fixture contract.
    :param extra_assets: Value supplied for extra assets under the deterministic fixture
        contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    body_lines = tuple(lines or MULTISCRIPT_TEXT.splitlines())
    extra_assets = dict(extra_assets or {})
    css_members = (HTMLZ_CSS_MEMBER,) if include_css else ()
    image_members = (HTMLZ_IMAGE_MEMBER,) if include_image else ()

    css_markup = (
        f'<link rel="stylesheet" type="text/css" href="{_xml_text(HTMLZ_CSS_MEMBER)}" />'
        if include_css
        else ""
    )
    image_markup = (
        f'<p><img src="{_xml_text(HTMLZ_IMAGE_MEMBER)}" alt="封面 世界" /></p>'
        if include_image
        else ""
    )
    paragraph_markup = "\n".join(f"<p>{_xml_text(line)}</p>" for line in body_lines)
    html = f"""<?xml version="1.0" encoding="utf-8"?>
<html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en">
  <head>
    <meta charset="utf-8" />
    <title>{_xml_text(HTMLZ_TITLE)}</title>
    {css_markup}
  </head>
  <body>
    <h1 id="intro">{_xml_text(HTMLZ_TITLE)} 👩🏽‍💻</h1>
    {paragraph_markup}
    {image_markup}
  </body>
</html>
""".encode("utf-8")

    manifest_css = (
        f'<item id="style" href="{_xml_text(HTMLZ_CSS_MEMBER)}" media-type="text/css"/>'
        if include_css
        else ""
    )
    manifest_image = (
        f'<item id="cover-image" href="{_xml_text(HTMLZ_IMAGE_MEMBER)}" media-type="image/png"/>'
        if include_image
        else ""
    )
    cover_meta = '<meta name="cover" content="cover-image"/>' if include_image else ""
    guide_cover = (
        f'<guide><reference type="cover" title="Cover" href="{_xml_text(HTMLZ_IMAGE_MEMBER)}"/></guide>'
        if include_image
        else "<guide/>"
    )
    creators = "".join(
        f'<dc:creator opf:role="aut">{_xml_text(author)}</dc:creator>' for author in HTMLZ_AUTHORS
    )
    extra_manifest_items = "\n".join(
        (
            '<item id="extra-%d" href="%s" media-type="%s"/>'
            % (idx, _xml_text(member_name), _xml_text(media_type))
        )
        for idx, (member_name, (media_type, _data)) in enumerate(extra_assets.items(), start=1)
    )
    opf = f"""<?xml version="1.0" encoding="utf-8"?>
<package xmlns="http://www.idpf.org/2007/opf" version="2.0" unique-identifier="BookId">
  <metadata xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:opf="http://www.idpf.org/2007/opf">
    <dc:title>{_xml_text(HTMLZ_TITLE)}</dc:title>
    {creators}
    <dc:language>en</dc:language>
    <dc:identifier id="BookId">{_xml_text(HTMLZ_IDENTIFIER)}</dc:identifier>
    <dc:description>{_xml_text(HTMLZ_DESCRIPTION)}</dc:description>
    <dc:publisher>{_xml_text(HTMLZ_PUBLISHER)}</dc:publisher>
    <dc:subject>{_xml_text(HTMLZ_SUBJECT)}</dc:subject>
    {cover_meta}
  </metadata>
  <manifest>
    <item id="html" href="{_xml_text(html_member)}" media-type="text/html"/>
    {manifest_css}
    {manifest_image}
    {extra_manifest_items}
  </manifest>
  <spine>
    <itemref idref="html"/>
  </spine>
  {guide_cover}
</package>
""".encode("utf-8")

    css = "body { font-family: serif; } h1 { color: #24476b; }\n".encode("utf-8")

    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w") as zf:
        for name, data in ((html_member, html),):
            info = zipfile.ZipInfo(name)
            info.compress_type = zipfile.ZIP_DEFLATED
            zf.writestr(info, data)

        if opf_member:
            info = zipfile.ZipInfo(opf_member)
            info.compress_type = zipfile.ZIP_DEFLATED
            zf.writestr(info, opf)

        if include_css:
            info = zipfile.ZipInfo(HTMLZ_CSS_MEMBER)
            info.compress_type = zipfile.ZIP_DEFLATED
            zf.writestr(info, css)

        if include_image:
            info = zipfile.ZipInfo(HTMLZ_IMAGE_MEMBER)
            info.compress_type = zipfile.ZIP_DEFLATED
            zf.writestr(info, png_bytes())

        for member_name, (_media_type, data) in extra_assets.items():
            info = zipfile.ZipInfo(member_name)
            info.compress_type = zipfile.ZIP_DEFLATED
            zf.writestr(info, data)

    return HTMLZFixture(
        path=path,
        html_member=html_member,
        opf_member=opf_member,
        css_members=css_members,
        asset_members=image_members + tuple(extra_assets),
        text_fragments=tuple(COMMON_TEXT_FRAGMENTS),
    )


def zip_members(path: Path) -> tuple[str, ...]:
    """
    Return the normalized members stored in the generated archive fixture.

    Example:
        Exercise zip members through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_html_metadata_source.py


    :param path: Filesystem path read, written or validated by the fixture operation.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    with zipfile.ZipFile(path, "r") as zf:
        return tuple(info.filename for info in zf.infolist())


def read_htmlz_member(path: Path, member: str) -> bytes:
    """
    Read htmlz member under the fixture contract.

    Example:
        Exercise read htmlz member through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_html_metadata_source.py


    :param path: Filesystem path read, written or validated by the fixture operation.
    :param member: Archive or container member addressed by the operation.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    with zipfile.ZipFile(path, "r") as zf:
        return zf.read(member)


def rewrite_htmlz_zip(
    src: Path,
    dst: Path,
    *,
    remove: Sequence[str] = (),
    replace: Mapping[str, bytes] | None = None,
    add: Mapping[str, bytes] | None = None,
    add_compression: int = zipfile.ZIP_STORED,
) -> None:
    """
    Perform the rewrite htmlz zip step with deterministic fixture inputs.

    Example:
        Exercise rewrite htmlz zip through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_html_metadata_source.py


    :param src: Source path or value copied into the fixture.
    :param dst: Destination path or object receiving generated fixture data.
    :param remove: Value supplied for remove under the deterministic fixture contract.
    :param replace: Value supplied for replace under the deterministic fixture contract.
    :param add: Value supplied for add under the deterministic fixture contract.
    :param add_compression: Value supplied for add compression under the deterministic
        fixture contract.
    :return: None; fixture state or the supplied destination is updated in place.
    """
    replacements = dict(replace or {})
    additions = dict(add or {})
    removed = set(remove)
    with zipfile.ZipFile(src, "r") as zin, zipfile.ZipFile(dst, "w") as zout:
        for info in zin.infolist():
            if info.filename in removed:
                continue
            data = replacements.pop(info.filename, zin.read(info.filename))
            zout.writestr(info, data)
        for name, data in {**replacements, **additions}.items():
            info = zipfile.ZipInfo(name)
            info.compress_type = add_compression
            zout.writestr(info, data)

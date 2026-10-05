"""
Provide test rb modernized utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test rb modernized through a consuming regression::

        python -m pytest -q tests/file_formats/rb/test_rb_modernized.py
"""
from __future__ import annotations

import importlib
import io
import struct
import types
import zlib
from pathlib import Path

import pytest


def _build_rb_bytes(entries: list[tuple[str, int, bytes]]) -> bytes:
    """
    Perform the build rb bytes operation under explicit file-format and conversion rules.

    Example:
        Exercise  build rb bytes through a consuming regression::

            python -m pytest -q tests/file_formats/rb/test_rb_modernized.py


    :param entries: Value supplied for entries under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    from LiuXin_alpha.file_formats.rb import HEADER

    toc_offset = 0x128
    out = io.BytesIO()
    out.write(HEADER)
    out.write(struct.pack("<I", 0))
    out.write(struct.pack("<IH", 0, 0))
    out.write(struct.pack("<I", toc_offset))
    out.write(struct.pack("<I", 0))
    for _ in range(0x20, toc_offset, 4):
        out.write(struct.pack("<I", 0))

    out.write(struct.pack("<I", len(entries)))
    offset = toc_offset + 4 + (44 * len(entries))
    for name, _flags, payload in entries:
        out.write(name.encode("utf-8", "replace")[:32].ljust(32, b"\x00"))
        out.write(struct.pack("<I", len(payload)))
        out.write(struct.pack("<I", offset))
        out.write(struct.pack("<I", _flags))
        offset += len(payload)

    for _name, _flags, payload in entries:
        out.write(payload)

    total_size = out.tell()
    out.seek(0x1C)
    out.write(struct.pack("<I", total_size))
    return out.getvalue()


def _compressed_text_record(text: str) -> bytes:
    """
    Perform the compressed text record operation under explicit file-format and conversion rules.

    Example:
        Exercise  compressed text record through a consuming regression::

            python -m pytest -q tests/file_formats/rb/test_rb_modernized.py


    :param text: Text parsed, normalized or rendered.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    raw = text.encode("cp1252", "replace")
    compressed = zlib.compress(raw)
    return struct.pack("<I", 1) + struct.pack("<I", len(raw)) + struct.pack("<I", len(compressed)) + compressed


class _Log:
    """
    Provide the log contract for validated ebook processing.

    Example:
        Exercise  Log through a consuming regression::

            python -m pytest -q tests/file_formats/rb/test_rb_modernized.py
    """
    def __init__(self) -> None:
        """
        Initialize and validate the log state.

        Example:
            Exercise  Log.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/rb/test_rb_modernized.py


        :return: None; validated state is stored on the receiving object.
        """
        self.messages: list[tuple[str, str]] = []

    def debug(self, message: str) -> None:
        """
        Perform the debug operation under explicit file-format and conversion rules.

        Example:
            Exercise  Log.debug through a consuming regression::

                python -m pytest -q tests/file_formats/rb/test_rb_modernized.py


        :param message: Value supplied for message under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.messages.append(("debug", message))

    def info(self, message: str) -> None:
        """
        Perform the info operation under explicit file-format and conversion rules.

        Example:
            Exercise  Log.info through a consuming regression::

                python -m pytest -q tests/file_formats/rb/test_rb_modernized.py


        :param message: Value supplied for message under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.messages.append(("info", message))

    def warning(self, message: str) -> None:
        """
        Perform the warning operation under explicit file-format and conversion rules.

        Example:
            Exercise  Log.warning through a consuming regression::

                python -m pytest -q tests/file_formats/rb/test_rb_modernized.py


        :param message: Value supplied for message under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.messages.append(("warning", message))

    def warn(self, message: str) -> None:
        """
        Perform the warn operation under explicit file-format and conversion rules.

        Example:
            Exercise  Log.warn through a consuming regression::

                python -m pytest -q tests/file_formats/rb/test_rb_modernized.py


        :param message: Value supplied for message under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.warning(message)

    def error(self, message: str) -> None:
        """
        Perform the error operation under explicit file-format and conversion rules.

        Example:
            Exercise  Log.error through a consuming regression::

                python -m pytest -q tests/file_formats/rb/test_rb_modernized.py


        :param message: Value supplied for message under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.messages.append(("error", message))


def test_rb_modules_import_smoke() -> None:
    """
    Perform the test rb modules import smoke operation under explicit file-format and conversion rules.

    Example:
        Exercise test rb modules import smoke through a consuming regression::

            python -m pytest -q tests/file_formats/rb/test_rb_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    modules = (
        "LiuXin_alpha.file_formats.rb",
        "LiuXin_alpha.file_formats.rb.rbml",
        "LiuXin_alpha.file_formats.rb.reader",
        "LiuXin_alpha.file_formats.rb.writer",
        "LiuXin_alpha.file_formats.conversion.plugins.rb_input",
        "LiuXin_alpha.file_formats.conversion.plugins.rb_output",
        "LiuXin_alpha.metadata.file_sources.rb",
    )
    for module_name in modules:
        importlib.import_module(module_name)


def test_rb_unique_name_enforces_limit_and_extension() -> None:
    """
    Perform the test rb unique name enforces limit and extension operation under explicit file-format and conversion rules.

    Example:
        Exercise test rb unique name enforces limit and extension through a consuming regression::

            python -m pytest -q tests/file_formats/rb/test_rb_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.rb import unique_name

    used = {"0000-very-long-name-that-wi.png"}
    out = unique_name("very-long-name-that-will-not-fit-in-rb-table.png", used)
    assert len(out) <= 32
    assert out.endswith(".png")
    assert out not in used


def test_rb_metadata_reads_title_and_authors() -> None:
    """
    Perform the test rb metadata reads title and authors operation under explicit file-format and conversion rules.

    Example:
        Exercise test rb metadata reads title and authors through a consuming regression::

            python -m pytest -q tests/file_formats/rb/test_rb_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.metadata.file_sources.rb import get_metadata

    info_payload = b"TYPE=2\nTITLE=RocketBook Test\nAUTHOR=Alice & Bob\n"
    rb_data = _build_rb_bytes([("info.info", 2, info_payload)])
    mi = get_metadata(io.BytesIO(rb_data))

    assert mi.title == "RocketBook Test"
    assert "Alice" in " ".join(getattr(mi, "authors", []))


def test_rb_reader_extracts_text_and_image_payloads(tmp_path: Path) -> None:
    """
    Perform the test rb reader extracts text and image payloads operation under explicit file-format and conversion rules.

    Example:
        Exercise test rb reader extracts text and image payloads through a consuming regression::

            python -m pytest -q tests/file_formats/rb/test_rb_modernized.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.rb.reader import Reader

    html_payload = _compressed_text_record("<HTML><HEAD><TITLE></TITLE></HEAD><BODY>Hi</BODY></HTML>")
    png_payload = b"\x89PNG\r\n\x1a\nfake"
    info_payload = b"TYPE=2\nTITLE=RB\nAUTHOR=Tester\n"
    rb_data = _build_rb_bytes(
        [
            ("info.info", 2, info_payload),
            ("index.html", 8, html_payload),
            ("cover.png", 0, png_payload),
        ]
    )

    reader = Reader(io.BytesIO(rb_data), _Log(), None)
    assert [item.name for item in reader.toc] == ["info.info", "index.html", "cover.png"]

    html_item = next(item for item in reader.toc if item.name == "index.html")
    png_item = next(item for item in reader.toc if item.name == "cover.png")
    reader.get_text(html_item, str(tmp_path))
    reader.get_image(png_item, str(tmp_path))

    html_path = tmp_path / "index.html"
    png_path = tmp_path / "cover.png"
    assert html_path.exists()
    assert png_path.exists()
    assert b"<TITLE> " in html_path.read_bytes()
    assert png_path.read_bytes() == png_payload


def test_rb_reader_rejects_invalid_header() -> None:
    """
    Perform the test rb reader rejects invalid header operation under explicit file-format and conversion rules.

    Example:
        Exercise test rb reader rejects invalid header through a consuming regression::

            python -m pytest -q tests/file_formats/rb/test_rb_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.rb import RocketBookError
    from LiuXin_alpha.file_formats.rb.reader import Reader

    with pytest.raises(RocketBookError):
        Reader(io.BytesIO(b"not-a-valid-rb"), _Log(), None)


def test_rb_writer_text_chunking_uses_integer_math(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Perform the test rb writer text chunking uses integer math operation under explicit file-format and conversion rules.

    Example:
        Exercise test rb writer text chunking uses integer math through a consuming regression::

            python -m pytest -q tests/file_formats/rb/test_rb_modernized.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    writer_mod = importlib.import_module("LiuXin_alpha.file_formats.rb.writer")

    class _FakeRBMLizer:
        """
        Provide the fakerbmlizer contract for validated ebook processing.

        Example:
            Exercise test rb writer text chunking uses integer math. FakeRBMLizer through a consuming regression::

                python -m pytest -q tests/file_formats/rb/test_rb_modernized.py
        """
        def __init__(self, _log, name_map=None):
            """
            Initialize and validate the fakerbmlizer state.

            Example:
                Exercise test rb writer text chunking uses integer math. FakeRBMLizer.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/rb/test_rb_modernized.py


            :param _log: Value supplied for log under the utility contract.
            :param name_map: Value supplied for name map under the utility contract.
            :return: None; validated state is stored on the receiving object.
            """
            self.name_map = {} if name_map is None else name_map

        def extract_content(self, _oeb_book, _opts):
            """
            Extract content under the format's safety and compatibility rules.

            Example:
                Exercise test rb writer text chunking uses integer math. FakeRBMLizer.extract content through a consuming regression::

                    python -m pytest -q tests/file_formats/rb/test_rb_modernized.py


            :param _oeb_book: Value supplied for oeb book under the utility contract.
            :param _opts: Value supplied for opts under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return "A" * (writer_mod.TEXT_RECORD_SIZE + 5)

    monkeypatch.setattr(writer_mod, "RBMLizer", _FakeRBMLizer)
    writer = writer_mod.RBWriter(types.SimpleNamespace(), _Log())
    size, chunks = writer._text(object())
    assert size == writer_mod.TEXT_RECORD_SIZE + 5
    assert len(chunks) == 2
    assert all(isinstance(chunk, bytes) for chunk in chunks)


def test_rb_writer_skips_images_without_pillow(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Perform the test rb writer skips images without pillow operation under explicit file-format and conversion rules.

    Example:
        Exercise test rb writer skips images without pillow through a consuming regression::

            python -m pytest -q tests/file_formats/rb/test_rb_modernized.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    writer_mod = importlib.import_module("LiuXin_alpha.file_formats.rb.writer")
    monkeypatch.setattr(writer_mod, "_PILImage", None)

    writer = writer_mod.RBWriter(types.SimpleNamespace(), _Log())
    images = writer._images([types.SimpleNamespace(media_type="image/png", href="cover.png", data=b"raw")])
    assert images == []

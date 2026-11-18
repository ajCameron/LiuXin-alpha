"""
Check that crafted input files cannot read or write outside their own folder.

Each test builds a small hostile input and asserts on the filesystem afterwards,
covering findings FFS01 to FFS04 of the 2026-11-16 file_formats risk sweep.
"""

from __future__ import annotations

import os
import types
from pathlib import Path

import pytest

from LiuXin_alpha.file_formats.archive_preflight import is_path_within


class _Log:
    """
    Collect warnings so a test can see that an unsafe path was reported.

    Example:
        >>> log = _Log()
        >>> log.warn('skipped')
        >>> log.messages
        ['skipped']
    """

    def __init__(self) -> None:
        """
        Start with no recorded messages.

        Example:
            >>> _Log().messages
            []


        :return: ``None``.
        """
        self.messages: list[str] = []

    def warn(self, *args: object, **_kwargs: object) -> None:
        """
        Record one warning as text.

        Example:
            >>> log = _Log()
            >>> log.warn('a', 'b')
            >>> log.messages
            ['a b']


        :param args: Message parts, joined with spaces.
        :param _kwargs: Ignored logging options.
        :return: ``None``.
        """
        self.messages.append(" ".join(str(arg) for arg in args))

    warning = info = debug = exception = __call__ = warn

    def log_exception(self, message: str = "", **_kwargs: object) -> None:
        """
        Record the message of a logged exception.

        Example:
            >>> log = _Log()
            >>> log.log_exception(message='failed')
            >>> log.messages
            ['failed']


        :param message: Text describing what was being attempted.
        :param _kwargs: Ignored exception and level arguments.
        :return: ``None``.
        """
        self.messages.append(message)


def test_is_path_within_rejects_every_way_out_of_the_root(tmp_path: Path) -> None:
    """
    Accept paths under the root and reject parents, siblings and links elsewhere.

    Example:
        >>> test_is_path_within_rejects_every_way_out_of_the_root(tmp_path)  # doctest: +SKIP


    :param tmp_path: Scratch directory holding the root and an outside folder.
    :return: ``None`` after checking each kind of path.
    """
    root = tmp_path / "book"
    (root / "images").mkdir(parents=True)
    outside = tmp_path / "bookshelf"
    outside.mkdir()
    (outside / "notes.txt").write_text("private")

    assert is_path_within(root, root)
    assert is_path_within(root, root / "images" / "cover.png")
    assert is_path_within(root, root / "images" / ".." / "index.html")
    assert not is_path_within(root, tmp_path)
    assert not is_path_within(root, root / ".." / "bookshelf" / "notes.txt")
    # A sibling whose name merely starts with the root's name is not inside it.
    assert not is_path_within(root, outside / "notes.txt")
    assert not is_path_within(root, os.path.abspath(os.sep))

    link = root / "shortcut"
    try:
        link.symlink_to(outside, target_is_directory=True)
    except (OSError, NotImplementedError):
        return
    assert not is_path_within(root, link / "notes.txt")


def test_stored_conversion_options_are_parsed_and_never_executed(
    tmp_path: Path,
) -> None:
    """
    Read stored recommendations as literals; ignore code and non-dictionary text.

    Example:
        >>> test_stored_conversion_options_are_parsed_and_never_executed(tmp_path)  # doctest: +SKIP


    :param tmp_path: Scratch directory where executed code would leave a marker.
    :return: ``None`` after checking the round trip and the rejected inputs.
    """
    from LiuXin_alpha.file_formats.conversion.config import GuiRecommendations

    original = GuiRecommendations()
    original.update(
        {"margin_top": 5.0, "title": "A 'quoted' title", "flags": [1, None, True]}
    )
    restored = GuiRecommendations()
    restored.from_string(str(original))
    assert dict(restored) == dict(original)

    marker = tmp_path / "executed"
    hostile = (
        f"__import__('pathlib').Path({str(marker)!r}).write_text('x')"
        " or {'margin_top': 1}"
    )
    restored.from_string(hostile)
    assert not marker.exists()
    assert dict(restored) == dict(original)

    for ignored in ("[1, 2, 3]", "'text'", "{not valid", "", None, "{'a': object()}"):
        restored.from_string(ignored)
    assert dict(restored) == dict(original)


def test_html_input_refuses_linked_files_outside_the_document_folder(
    tmp_path: Path,
) -> None:
    """
    Resolve links inside the input folder and refuse ones that lead elsewhere.

    Example:
        >>> test_html_input_refuses_linked_files_outside_the_document_folder(tmp_path)  # doctest: +SKIP


    :param tmp_path: Scratch directory holding the document folder and a secret.
    :return: ``None`` after checking refused links, allowed links and the opt-out.
    """
    from LiuXin_alpha.file_formats.conversion.plugins.html_input import HTMLInput

    doc_dir = tmp_path / "doc"
    (doc_dir / "images").mkdir(parents=True)
    inside = doc_dir / "images" / "cover.png"
    inside.write_bytes(b"png")
    secret = tmp_path / "secret.txt"
    secret.write_text("private")

    plugin = HTMLInput(None)
    plugin.log = _Log()
    plugin.set_root_dir_of_input(str(doc_dir), types.SimpleNamespace())

    link, _frag = plugin.link_to_local_path("images/cover.png", base=str(doc_dir))
    assert link is not None and os.path.samefile(link, inside)

    for hostile in (
        str(secret),
        secret.as_uri(),
        "../secret.txt",
        "images/../../secret.txt",
    ):
        assert plugin.link_to_local_path(hostile, base=str(doc_dir)) == (None, None)
    assert any(
        "outside the document root" in message for message in plugin.log.messages
    )

    plugin.set_root_dir_of_input(
        str(doc_dir), types.SimpleNamespace(allow_local_files_outside_root=True)
    )
    link, _frag = plugin.link_to_local_path("../secret.txt", base=str(doc_dir))
    assert link is not None and os.path.samefile(link, secret)


def test_chm_extraction_skips_entries_that_leave_the_output_folder(
    tmp_path: Path,
) -> None:
    """
    Extract ordinary CHM entries and skip ones whose names escape the output folder.

    Example:
        >>> test_chm_extraction_skips_entries_that_leave_the_output_folder(tmp_path)  # doctest: +SKIP


    :param tmp_path: Scratch directory holding the output folder and its parent.
    :return: ``None`` after checking which files were written.
    """
    from LiuXin_alpha.file_formats.chm import reader as reader_mod

    out = tmp_path / "out"
    out.mkdir()
    entries = {
        "docs/page.txt": b"kept",
        "../escaped.txt": b"escaped",
        "docs/../../also-escaped.txt": b"escaped",
        str(tmp_path / "absolute.txt"): b"escaped",
    }

    reader = object.__new__(reader_mod.CHMReader)
    reader.log = _Log()
    reader.hhc_path = "docs/page.txt"
    reader.get_encoding = lambda: "utf-8"
    reader.Contents = lambda: list(entries)
    reader.GetFile = lambda name: entries[name]

    reader.ExtractFiles(output_dir=str(out))

    assert (out / "docs" / "page.txt").read_bytes() == b"kept"
    assert not (tmp_path / "escaped.txt").exists()
    assert not (tmp_path / "also-escaped.txt").exists()
    assert not (tmp_path / "absolute.txt").exists()
    assert (
        sum("outside the output folder" in message for message in reader.log.messages)
        == 3
    )


def test_epub_font_deobfuscation_leaves_files_outside_the_book_alone(
    tmp_path: Path,
) -> None:
    """
    De-obfuscate a font inside the EPUB and leave a referenced outside file untouched.

    Example:
        >>> test_epub_font_deobfuscation_leaves_files_outside_the_book_alone(tmp_path)  # doctest: +SKIP


    :param tmp_path: Scratch directory holding an extracted EPUB and a neighbour file.
    :return: ``None`` after comparing both files with their original bytes.
    """
    pytest.importorskip("lxml")
    from LiuXin_alpha.file_formats.conversion.plugins import epub_input as epub_mod

    book = tmp_path / "book"
    (book / "META-INF").mkdir(parents=True)
    (book / "fonts").mkdir()
    original = bytes(range(256)) * 8
    font = book / "fonts" / "font.otf"
    font.write_bytes(original)
    neighbour = tmp_path / "neighbour.bin"
    neighbour.write_bytes(original)

    def reference(uri: str) -> str:
        """
        Build one encrypted-data entry naming a font by URI.

        Example:
            >>> 'fonts/a.otf' in reference('fonts/a.otf')
            True


        :param uri: Value for the ``CipherReference`` URI attribute.
        :return: XML text of one ``EncryptedData`` element.
        """
        return (
            "<enc:EncryptedData>"
            f'<enc:EncryptionMethod Algorithm="{epub_mod.ADOBE_OBFUSCATION}"/>'
            f'<enc:CipherData><enc:CipherReference URI="{uri}"/></enc:CipherData>'
            "</enc:EncryptedData>"
        )

    encfile = book / "META-INF" / "encryption.xml"
    encfile.write_text(
        '<encryption xmlns="urn:oasis:names:tc:opendocument:xmlns:container" '
        'xmlns:enc="http://www.w3.org/2001/04/xmlenc#">'
        + reference("../neighbour.bin")
        + reference("fonts/font.otf")
        + "</encryption>"
    )
    identifier = types.SimpleNamespace(
        attrib={"scheme": "uuid"},
        get=lambda _key: "uuid",
        text="urn:uuid:12345678-1234-5678-1234-567812345678",
    )
    opf = types.SimpleNamespace(
        raw_unique_identifier="urn:uuid:12345678-1234-5678-1234-567812345678",
        identifier_iter=lambda: [identifier],
    )

    plugin = epub_mod.EPUBInput(None)
    plugin._encrypted_font_uris = []
    assert plugin.process_encryption(str(encfile), opf, _Log()) is True

    assert neighbour.read_bytes() == original
    assert font.read_bytes() != original
    assert plugin._encrypted_font_uris == ["fonts/font.otf"]

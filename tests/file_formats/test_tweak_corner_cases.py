"""
Provide test tweak corner cases utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test tweak corner cases through a consuming regression::

        python -m pytest -q tests/file_formats/test_tweak_corner_cases.py
"""
from __future__ import annotations

import os
import sys
import types
import zipfile

import pytest

from LiuXin_alpha.file_formats import tweak


def test_ask_cli_question_accepts_unix_text_input(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Perform the test ask cli question accepts unix text input operation under explicit file-format and conversion rules.

    Example:
        Exercise test ask cli question accepts unix text input through a consuming regression::

            python -m pytest -q tests/file_formats/test_tweak_corner_cases.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    class _FakeStdin:
        """
        Provide the fakestdin contract for validated ebook processing.

        Example:
            Exercise test ask cli question accepts unix text input. FakeStdin through a consuming regression::

                python -m pytest -q tests/file_formats/test_tweak_corner_cases.py
        """
        def fileno(self):
            """
            Perform the fileno operation under explicit file-format and conversion rules.

            Example:
                Exercise test ask cli question accepts unix text input. FakeStdin.fileno through a consuming regression::

                    python -m pytest -q tests/file_formats/test_tweak_corner_cases.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return 0

        def read(self, _n):
            """
            Perform the read operation under explicit file-format and conversion rules.

            Example:
                Exercise test ask cli question accepts unix text input. FakeStdin.read through a consuming regression::

                    python -m pytest -q tests/file_formats/test_tweak_corner_cases.py


            :param _n: Value supplied for n under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return "Y"

    fake_termios = types.SimpleNamespace(tcgetattr=lambda _fd: [1, 2, 3], tcsetattr=lambda *_a, **_k: None, TCSADRAIN=0)
    fake_tty = types.SimpleNamespace(setraw=lambda _fd: None)

    monkeypatch.setattr(tweak, "iswindows", False)
    monkeypatch.setattr(tweak.sys, "stdin", _FakeStdin())
    monkeypatch.setitem(sys.modules, "termios", fake_termios)
    monkeypatch.setitem(sys.modules, "tty", fake_tty)

    assert tweak.ask_cli_question("Continue?") is True


def test_ask_cli_question_accepts_windows_bytes(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Perform the test ask cli question accepts windows bytes operation under explicit file-format and conversion rules.

    Example:
        Exercise test ask cli question accepts windows bytes through a consuming regression::

            python -m pytest -q tests/file_formats/test_tweak_corner_cases.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    fake_msvcrt = types.SimpleNamespace(getch=lambda: b"y")
    monkeypatch.setattr(tweak, "iswindows", True)
    monkeypatch.setitem(sys.modules, "msvcrt", fake_msvcrt)

    assert tweak.ask_cli_question("Continue?") is True


def test_zip_exploder_wraps_unpack_errors(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    """
    Perform the test zip exploder wraps unpack errors operation under explicit file-format and conversion rules.

    Example:
        Exercise test zip exploder wraps unpack errors through a consuming regression::

            python -m pytest -q tests/file_formats/test_tweak_corner_cases.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    monkeypatch.setattr(tweak, "zipextract", lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("boom")))

    with pytest.raises(tweak.Error, match="Failed to unpack"):
        tweak.zip_exploder("broken.epub", str(tmp_path))


def test_zip_rebuilder_orders_entries_and_skips_output_file(tmp_path) -> None:
    """
    Perform the test zip rebuilder orders entries and skips output file operation under explicit file-format and conversion rules.

    Example:
        Exercise test zip rebuilder orders entries and skips output file through a consuming regression::

            python -m pytest -q tests/file_formats/test_tweak_corner_cases.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    root = tmp_path / "book"
    root.mkdir()
    (root / "mimetype").write_text("application/epub+zip", encoding="utf-8")
    (root / "b.xhtml").write_text("b", encoding="utf-8")
    (root / "a.xhtml").write_text("a", encoding="utf-8")

    output = root / "book.epub"
    tweak.zip_rebuilder(str(root), str(output))

    with zipfile.ZipFile(output, "r") as zf:
        names = zf.namelist()

    assert "book.epub" not in names
    assert names[0] == "mimetype"
    assert names[1:] == sorted(names[1:])


def test_tweak_handles_blank_editor_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Perform the test tweak handles blank editor env operation under explicit file-format and conversion rules.

    Example:
        Exercise test tweak handles blank editor env through a consuming regression::

            python -m pytest -q tests/file_formats/test_tweak_corner_cases.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    state = {"rebuilt": 0}

    def fake_exploder(_ebook_file, _tdir, question=None):
        """
        Perform the fake exploder operation under explicit file-format and conversion rules.

        Example:
            Exercise test tweak handles blank editor env.fake exploder through a consuming regression::

                python -m pytest -q tests/file_formats/test_tweak_corner_cases.py


        :param _ebook_file: Value supplied for ebook file under the utility contract.
        :param _tdir: Value supplied for tdir under the utility contract.
        :param question: Value supplied for question under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return "content.opf"

    def fake_rebuilder(_tdir, _ebook_file):
        """
        Perform the fake rebuilder operation under explicit file-format and conversion rules.

        Example:
            Exercise test tweak handles blank editor env.fake rebuilder through a consuming regression::

                python -m pytest -q tests/file_formats/test_tweak_corner_cases.py


        :param _tdir: Value supplied for tdir under the utility contract.
        :param _ebook_file: Value supplied for ebook file under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        state["rebuilt"] += 1

    monkeypatch.setattr(tweak, "get_tools", lambda _fmt: (fake_exploder, fake_rebuilder))
    monkeypatch.setattr(tweak, "ask_cli_question", lambda _msg: False)
    monkeypatch.setenv("EDITOR", "   ")

    tweak.tweak("book.epub")

    assert state["rebuilt"] == 0


def test_tweak_catches_generic_rebuild_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Perform the test tweak catches generic rebuild errors operation under explicit file-format and conversion rules.

    Example:
        Exercise test tweak catches generic rebuild errors through a consuming regression::

            python -m pytest -q tests/file_formats/test_tweak_corner_cases.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    def fake_exploder(_ebook_file, _tdir, question=None):
        """
        Perform the fake exploder operation under explicit file-format and conversion rules.

        Example:
            Exercise test tweak catches generic rebuild errors.fake exploder through a consuming regression::

                python -m pytest -q tests/file_formats/test_tweak_corner_cases.py


        :param _ebook_file: Value supplied for ebook file under the utility contract.
        :param _tdir: Value supplied for tdir under the utility contract.
        :param question: Value supplied for question under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return "content.opf"

    def fake_rebuilder(_tdir, _ebook_file):
        """
        Perform the fake rebuilder operation under explicit file-format and conversion rules.

        Example:
            Exercise test tweak catches generic rebuild errors.fake rebuilder through a consuming regression::

                python -m pytest -q tests/file_formats/test_tweak_corner_cases.py


        :param _tdir: Value supplied for tdir under the utility contract.
        :param _ebook_file: Value supplied for ebook file under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise RuntimeError("rebuild failed")

    monkeypatch.setattr(tweak, "get_tools", lambda _fmt: (fake_exploder, fake_rebuilder))
    monkeypatch.setattr(tweak, "ask_cli_question", lambda _msg: True)
    monkeypatch.setenv("EDITOR", "dummy")

    with pytest.raises(SystemExit) as exc:
        tweak.tweak("book.epub")
    assert exc.value.code == 1


def test_get_tools_accepts_none() -> None:
    """
    Perform the test get tools accepts none operation under explicit file-format and conversion rules.

    Example:
        Exercise test get tools accepts none through a consuming regression::

            python -m pytest -q tests/file_formats/test_tweak_corner_cases.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    assert tweak.get_tools(None) == (None, None)


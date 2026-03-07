from __future__ import annotations

import os
import sys
import types
import zipfile

import pytest

from LiuXin_alpha.file_formats import tweak


def test_ask_cli_question_accepts_unix_text_input(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FakeStdin:
        def fileno(self):
            return 0

        def read(self, _n):
            return "Y"

    fake_termios = types.SimpleNamespace(tcgetattr=lambda _fd: [1, 2, 3], tcsetattr=lambda *_a, **_k: None, TCSADRAIN=0)
    fake_tty = types.SimpleNamespace(setraw=lambda _fd: None)

    monkeypatch.setattr(tweak, "iswindows", False)
    monkeypatch.setattr(tweak.sys, "stdin", _FakeStdin())
    monkeypatch.setitem(sys.modules, "termios", fake_termios)
    monkeypatch.setitem(sys.modules, "tty", fake_tty)

    assert tweak.ask_cli_question("Continue?") is True


def test_ask_cli_question_accepts_windows_bytes(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_msvcrt = types.SimpleNamespace(getch=lambda: b"y")
    monkeypatch.setattr(tweak, "iswindows", True)
    monkeypatch.setitem(sys.modules, "msvcrt", fake_msvcrt)

    assert tweak.ask_cli_question("Continue?") is True


def test_zip_exploder_wraps_unpack_errors(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setattr(tweak, "zipextract", lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("boom")))

    with pytest.raises(tweak.Error, match="Failed to unpack"):
        tweak.zip_exploder("broken.epub", str(tmp_path))


def test_zip_rebuilder_orders_entries_and_skips_output_file(tmp_path) -> None:
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
    state = {"rebuilt": 0}

    def fake_exploder(_ebook_file, _tdir, question=None):
        return "content.opf"

    def fake_rebuilder(_tdir, _ebook_file):
        state["rebuilt"] += 1

    monkeypatch.setattr(tweak, "get_tools", lambda _fmt: (fake_exploder, fake_rebuilder))
    monkeypatch.setattr(tweak, "ask_cli_question", lambda _msg: False)
    monkeypatch.setenv("EDITOR", "   ")

    tweak.tweak("book.epub")

    assert state["rebuilt"] == 0


def test_tweak_catches_generic_rebuild_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_exploder(_ebook_file, _tdir, question=None):
        return "content.opf"

    def fake_rebuilder(_tdir, _ebook_file):
        raise RuntimeError("rebuild failed")

    monkeypatch.setattr(tweak, "get_tools", lambda _fmt: (fake_exploder, fake_rebuilder))
    monkeypatch.setattr(tweak, "ask_cli_question", lambda _msg: True)
    monkeypatch.setenv("EDITOR", "dummy")

    with pytest.raises(SystemExit) as exc:
        tweak.tweak("book.epub")
    assert exc.value.code == 1


def test_get_tools_accepts_none() -> None:
    assert tweak.get_tools(None) == (None, None)


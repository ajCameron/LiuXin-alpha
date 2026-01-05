from __future__ import annotations

import importlib
import sys
from types import ModuleType
from typing import Optional

import pytest


class FakeWandImage:
    def __init__(self, *, filename: Optional[str] = None, blob: Optional[bytes] = None):
        self.filename = filename
        self.blob = blob
        self.width = 123
        self.height = 456
        self.format = "PNG"
        self.resize_calls: list[tuple[int, int]] = []
        self.transform_calls: list[str] = []
        self.saved_to: list[str] = []

    def make_blob(self) -> bytes:
        return (f"BLOB:{self.format}".encode("utf-8"))

    def resize(self, w: int, h: int) -> None:
        self.resize_calls.append((w, h))
        self.width = w
        self.height = h

    def transform(self, *, resize: str) -> None:
        self.transform_calls.append(resize)

    def save(self, *, filename: str) -> None:
        self.saved_to.append(filename)

    def close(self) -> None:
        pass


def _install_fake_wand(monkeypatch: pytest.MonkeyPatch) -> None:
    wand_mod = ModuleType("wand")
    wand_image_mod = ModuleType("wand.image")
    wand_image_mod.Image = FakeWandImage  # type: ignore[attr-defined]

    monkeypatch.setitem(sys.modules, "wand", wand_mod)
    monkeypatch.setitem(sys.modules, "wand.image", wand_image_mod)


def _import_magick_fresh() -> ModuleType:
    import LiuXin_alpha.utils.plugins.fallbacks.magick as m

    return importlib.reload(m)


def test_identify_prefers_wand(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_wand(monkeypatch)

    import subprocess

    def boom(*args, **kwargs):  # type: ignore[no-untyped-def]
        raise AssertionError("subprocess.run should not be called when Wand is available")

    monkeypatch.setattr(subprocess, "run", boom)

    m = _import_magick_fresh()
    img = m.Image(b"fake")
    info = img.identify()
    assert info["width"] == 123
    assert info["height"] == 456
    assert info["format"] == "PNG"
    assert info["size"] == 4


def test_to_bytes_uses_wand_and_sets_format(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_wand(monkeypatch)
    m = _import_magick_fresh()

    img = m.Image(b"fake")
    out = img.to_bytes(format="jpeg")
    assert out == b"BLOB:JPEG"


def test_resize_calls_wand_resize(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_wand(monkeypatch)
    m = _import_magick_fresh()

    img = m.Image(b"fake")
    img.resize(10, 20)
    info = img.identify()
    assert info["width"] == 10
    assert info["height"] == 20


def test_thumbnail_calls_wand_transform(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_wand(monkeypatch)
    m = _import_magick_fresh()

    img = m.Image(b"fake")
    img.identify()  # force wand instance
    assert img._wand_img is not None
    wand_obj = img._wand_img

    img.thumbnail(111, 222)
    assert wand_obj.transform_calls == ["111x222>"]


def test_save_calls_wand_save(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    _install_fake_wand(monkeypatch)
    m = _import_magick_fresh()

    out = tmp_path / "out.png"
    img = m.Image(b"fake")
    img.identify()  # force wand
    wand_obj = img._wand_img

    img.save(out, format="png")
    assert str(out) in wand_obj.saved_to


class _CP:
    def __init__(self, *, returncode: int, stdout: bytes = b"", stderr: bytes = b""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


def test_cli_identify_used_when_no_wand(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setitem(sys.modules, "wand", None)
    monkeypatch.setitem(sys.modules, "wand.image", None)

    m = _import_magick_fresh()

    monkeypatch.setattr(m, "_magick_identify", lambda: "identify")

    def fake_run(cmd, stdout, stderr, check):  # type: ignore[no-untyped-def]
        assert cmd[0] == "identify"
        return _CP(returncode=0, stdout=b"100 200 PNG 123B")

    import subprocess

    monkeypatch.setattr(subprocess, "run", fake_run)

    p = tmp_path / "x.bin"
    p.write_bytes(b"abc")

    info = m.Image(p).identify()
    assert info["width"] == 100
    assert info["height"] == 200
    assert info["format"] == "PNG"


def test_cli_convert_used_when_no_wand(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "wand", None)
    monkeypatch.setitem(sys.modules, "wand.image", None)

    m = _import_magick_fresh()

    monkeypatch.setattr(m, "_magick_convert", lambda: "magick")

    def fake_run(cmd, stdout, stderr, check):  # type: ignore[no-untyped-def]
        assert cmd[0] == "magick"
        assert cmd[1] == "convert"
        assert cmd[-1] == "png:-"
        return _CP(returncode=0, stdout=b"OUT")

    import subprocess

    monkeypatch.setattr(subprocess, "run", fake_run)

    out = m.Image(b"fake").to_bytes(format="png")
    assert out == b"OUT"

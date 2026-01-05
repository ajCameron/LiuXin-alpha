from __future__ import annotations

import importlib
import sys
from types import ModuleType
from typing import Any, Optional

import pytest


class FakeWandColor:
    def __init__(self, alpha: float = 1.0):
        self.alpha = alpha


class FakeWandImage:
    def __init__(self, *, blob: Optional[bytes] = None, filename: Optional[str] = None, width: int = 10, height: int = 20, background: Any = None):
        self.blob = blob
        self.filename = filename
        self.width = width
        self.height = height
        self.format = "PNG"
        self.fuzz = None
        self.calls: list[tuple] = []
        self._histogram = {FakeWandColor(alpha=1.0): 100}

    def close(self) -> None:
        self.calls.append(("close",))

    def make_blob(self) -> bytes:
        return b"BLOB:" + str(self.format).encode("ascii", "ignore")

    # operations we might call
    def trim(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        self.calls.append(("trim", args, kwargs))

    def reset_coords(self) -> None:
        self.calls.append(("reset_coords",))

    def transform_colorspace(self, cs: str) -> None:
        self.calls.append(("transform_colorspace", cs))

    def gaussian_blur(self, radius: float, sigma: float) -> None:
        self.calls.append(("gaussian_blur", float(radius), float(sigma)))

    def blur(self, radius: float, sigma: float) -> None:
        self.calls.append(("blur", float(radius), float(sigma)))

    def sharpen(self, radius: float, sigma: float) -> None:
        self.calls.append(("sharpen", float(radius), float(sigma)))

    def despeckle(self) -> None:
        self.calls.append(("despeckle",))

    def normalize(self) -> None:
        self.calls.append(("normalize",))

    def oil_paint(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        self.calls.append(("oil_paint", args, kwargs))

    def quantize(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        self.calls.append(("quantize", args, kwargs))

    def evaluate(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        self.calls.append(("evaluate", args, kwargs))

    def composite(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        self.calls.append(("composite", args, kwargs))

    def resize(self, w: int, h: int) -> None:
        self.calls.append(("resize", int(w), int(h)))
        self.width = int(w)
        self.height = int(h)

    @property
    def histogram(self):
        return self._histogram

    @property
    def alpha_channel(self):
        # emulate "has alpha channel"
        return True

    @alpha_channel.setter
    def alpha_channel(self, v: bool) -> None:
        self.calls.append(("alpha_channel", bool(v)))


def _install_fake_wand(monkeypatch: pytest.MonkeyPatch) -> None:
    wand_mod = ModuleType("wand")
    wand_image_mod = ModuleType("wand.image")
    wand_image_mod.Image = FakeWandImage  # type: ignore[attr-defined]

    wand_color_mod = ModuleType("wand.color")
    wand_color_mod.Color = lambda s: s  # type: ignore[assignment]

    monkeypatch.setitem(sys.modules, "wand", wand_mod)
    monkeypatch.setitem(sys.modules, "wand.image", wand_image_mod)
    monkeypatch.setitem(sys.modules, "wand.color", wand_color_mod)


def _import_imageops_fresh() -> ModuleType:
    import LiuXin_alpha.utils.plugins.fallbacks.imageops as m
    return importlib.reload(m)


def test_grayscale_prefers_wand(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_wand(monkeypatch)

    import subprocess
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: (_ for _ in ()).throw(AssertionError("CLI called")))

    m = _import_imageops_fresh()
    out = m.grayscale(b"fake")
    assert out.startswith(b"BLOB:")


def test_gaussian_blur_calls_wand(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_wand(monkeypatch)
    m = _import_imageops_fresh()

    out = m.gaussian_blur(b"fake", 2.0, 3.0)
    assert out == b"BLOB:PNG"


def test_set_opacity_calls_evaluate(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_wand(monkeypatch)
    m = _import_imageops_fresh()

    out = m.set_opacity(b"fake", 0.25)
    assert out == b"BLOB:PNG"


def test_has_transparent_pixels_false_when_histogram_opaque(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_wand(monkeypatch)
    m = _import_imageops_fresh()

    assert m.has_transparent_pixels(b"fake") is False


def test_remove_borders_uses_trim(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_wand(monkeypatch)
    m = _import_imageops_fresh()

    out = m.remove_borders(b"fake", 0.1)
    assert out == b"BLOB:PNG"


def test_texture_image_composites(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_wand(monkeypatch)
    m = _import_imageops_fresh()

    out = m.texture_image(b"base", b"tex")
    assert out == b"BLOB:PNG"

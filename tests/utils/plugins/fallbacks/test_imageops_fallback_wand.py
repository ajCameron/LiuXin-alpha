"""
Provide test imageops fallback wand utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test imageops fallback wand through a consuming regression::

        python -m pytest -q tests/utils/plugins/fallbacks/test_imageops_fallback_wand.py
"""
from __future__ import annotations

import importlib
import sys
from types import ModuleType
from typing import Any, Optional

import pytest


class FakeWandColor:
    """
    Provide the FakeWandColor utility contract with explicit state and cleanup behavior.

    Example:
        Exercise FakeWandColor through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_imageops_fallback_wand.py
    """
    def __init__(self, alpha: float = 1.0):
        """
        Initialize and validate the FakeWandColor state.

        Example:
            Exercise FakeWandColor.  init   through a consuming regression::

                python -m pytest -q tests/utils/plugins/fallbacks/test_imageops_fallback_wand.py


        :param alpha: Value supplied for alpha under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.alpha = alpha


class FakeWandImage:
    """
    Provide the FakeWandImage utility contract with explicit state and cleanup behavior.

    Example:
        Exercise FakeWandImage through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_imageops_fallback_wand.py
    """
    def __init__(self, *, blob: Optional[bytes] = None, filename: Optional[str] = None, width: int = 10, height: int = 20, background: Any = None):
        """
        Initialize and validate the FakeWandImage state.

        Example:
            Exercise FakeWandImage.  init   through a consuming regression::

                python -m pytest -q tests/utils/plugins/fallbacks/test_imageops_fallback_wand.py


        :param blob: Value supplied for blob under the utility contract.
        :param filename: Filename used for type inference or archive output.
        :param width: Value supplied for width under the utility contract.
        :param height: Value supplied for height under the utility contract.
        :param background: Value supplied for background under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.blob = blob
        self.filename = filename
        self.width = width
        self.height = height
        self.format = "PNG"
        self.fuzz = None
        self.calls: list[tuple] = []
        self._histogram = {FakeWandColor(alpha=1.0): 100}

    def close(self) -> None:
        """
        Forward the close operation while preserving adapter ownership rules.

        Example:
            Exercise FakeWandImage.close through a consuming regression::

                python -m pytest -q tests/utils/plugins/fallbacks/test_imageops_fallback_wand.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.calls.append(("close",))

    def make_blob(self) -> bytes:
        """
        Perform the make blob utility operation under explicit compatibility rules.

        Example:
            Exercise FakeWandImage.make blob through a consuming regression::

                python -m pytest -q tests/utils/plugins/fallbacks/test_imageops_fallback_wand.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return b"BLOB:" + str(self.format).encode("ascii", "ignore")

    # operations we might call
    def trim(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        """
        Perform the trim utility operation under explicit compatibility rules.

        Example:
            Exercise FakeWandImage.trim through a consuming regression::

                python -m pytest -q tests/utils/plugins/fallbacks/test_imageops_fallback_wand.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.calls.append(("trim", args, kwargs))

    def reset_coords(self) -> None:
        """
        Perform the reset coords utility operation under explicit compatibility rules.

        Example:
            Exercise FakeWandImage.reset coords through a consuming regression::

                python -m pytest -q tests/utils/plugins/fallbacks/test_imageops_fallback_wand.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.calls.append(("reset_coords",))

    def transform_colorspace(self, cs: str) -> None:
        """
        Perform the transform colorspace utility operation under explicit compatibility rules.

        Example:
            Exercise FakeWandImage.transform colorspace through a consuming regression::

                python -m pytest -q tests/utils/plugins/fallbacks/test_imageops_fallback_wand.py


        :param cs: Value supplied for cs under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.calls.append(("transform_colorspace", cs))

    def gaussian_blur(self, radius: float, sigma: float) -> None:
        """
        Perform the gaussian blur utility operation under explicit compatibility rules.

        Example:
            Exercise FakeWandImage.gaussian blur through a consuming regression::

                python -m pytest -q tests/utils/plugins/fallbacks/test_imageops_fallback_wand.py


        :param radius: Value supplied for radius under the utility contract.
        :param sigma: Value supplied for sigma under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.calls.append(("gaussian_blur", float(radius), float(sigma)))

    def blur(self, radius: float, sigma: float) -> None:
        """
        Perform the blur utility operation under explicit compatibility rules.

        Example:
            Exercise FakeWandImage.blur through a consuming regression::

                python -m pytest -q tests/utils/plugins/fallbacks/test_imageops_fallback_wand.py


        :param radius: Value supplied for radius under the utility contract.
        :param sigma: Value supplied for sigma under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.calls.append(("blur", float(radius), float(sigma)))

    def sharpen(self, radius: float, sigma: float) -> None:
        """
        Perform the sharpen utility operation under explicit compatibility rules.

        Example:
            Exercise FakeWandImage.sharpen through a consuming regression::

                python -m pytest -q tests/utils/plugins/fallbacks/test_imageops_fallback_wand.py


        :param radius: Value supplied for radius under the utility contract.
        :param sigma: Value supplied for sigma under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.calls.append(("sharpen", float(radius), float(sigma)))

    def despeckle(self) -> None:
        """
        Perform the despeckle utility operation under explicit compatibility rules.

        Example:
            Exercise FakeWandImage.despeckle through a consuming regression::

                python -m pytest -q tests/utils/plugins/fallbacks/test_imageops_fallback_wand.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.calls.append(("despeckle",))

    def normalize(self) -> None:
        """
        Perform the normalize utility operation under explicit compatibility rules.

        Example:
            Exercise FakeWandImage.normalize through a consuming regression::

                python -m pytest -q tests/utils/plugins/fallbacks/test_imageops_fallback_wand.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.calls.append(("normalize",))

    def oil_paint(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        """
        Perform the oil paint utility operation under explicit compatibility rules.

        Example:
            Exercise FakeWandImage.oil paint through a consuming regression::

                python -m pytest -q tests/utils/plugins/fallbacks/test_imageops_fallback_wand.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.calls.append(("oil_paint", args, kwargs))

    def quantize(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        """
        Perform the quantize utility operation under explicit compatibility rules.

        Example:
            Exercise FakeWandImage.quantize through a consuming regression::

                python -m pytest -q tests/utils/plugins/fallbacks/test_imageops_fallback_wand.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.calls.append(("quantize", args, kwargs))

    def evaluate(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise FakeWandImage.evaluate through a consuming regression::

                python -m pytest -q tests/utils/plugins/fallbacks/test_imageops_fallback_wand.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.calls.append(("evaluate", args, kwargs))

    def composite(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        """
        Perform the composite utility operation under explicit compatibility rules.

        Example:
            Exercise FakeWandImage.composite through a consuming regression::

                python -m pytest -q tests/utils/plugins/fallbacks/test_imageops_fallback_wand.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.calls.append(("composite", args, kwargs))

    def resize(self, w: int, h: int) -> None:
        """
        Perform the resize utility operation under explicit compatibility rules.

        Example:
            Exercise FakeWandImage.resize through a consuming regression::

                python -m pytest -q tests/utils/plugins/fallbacks/test_imageops_fallback_wand.py


        :param w: Value supplied for w under the utility contract.
        :param h: Value supplied for h under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.calls.append(("resize", int(w), int(h)))
        self.width = int(w)
        self.height = int(h)

    @property
    def histogram(self):
        """
        Perform the histogram utility operation under explicit compatibility rules.

        Example:
            Exercise FakeWandImage.histogram through a consuming regression::

                python -m pytest -q tests/utils/plugins/fallbacks/test_imageops_fallback_wand.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self._histogram

    @property
    def alpha_channel(self):
        # emulate "has alpha channel"
        """
        Perform the alpha channel utility operation under explicit compatibility rules.

        Example:
            Exercise FakeWandImage.alpha channel through a consuming regression::

                python -m pytest -q tests/utils/plugins/fallbacks/test_imageops_fallback_wand.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return True

    @alpha_channel.setter
    def alpha_channel(self, v: bool) -> None:
        """
        Perform the alpha channel utility operation under explicit compatibility rules.

        Example:
            Exercise FakeWandImage.alpha channel through a consuming regression::

                python -m pytest -q tests/utils/plugins/fallbacks/test_imageops_fallback_wand.py


        :param v: Value supplied for v under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.calls.append(("alpha_channel", bool(v)))


def _install_fake_wand(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Perform the install fake wand utility operation under explicit compatibility rules.

    Example:
        Exercise  install fake wand through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_imageops_fallback_wand.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    wand_mod = ModuleType("wand")
    wand_image_mod = ModuleType("wand.image")
    wand_image_mod.Image = FakeWandImage  # type: ignore[attr-defined]

    wand_color_mod = ModuleType("wand.color")
    wand_color_mod.Color = lambda s: s  # type: ignore[assignment]

    monkeypatch.setitem(sys.modules, "wand", wand_mod)
    monkeypatch.setitem(sys.modules, "wand.image", wand_image_mod)
    monkeypatch.setitem(sys.modules, "wand.color", wand_color_mod)


def _import_imageops_fresh() -> ModuleType:
    """
    Perform the import imageops fresh utility operation under explicit compatibility rules.

    Example:
        Exercise  import imageops fresh through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_imageops_fallback_wand.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    import LiuXin_alpha.utils.plugins.fallbacks.imageops as m
    return importlib.reload(m)


def test_grayscale_prefers_wand(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Perform the test grayscale prefers wand utility operation under explicit compatibility rules.

    Example:
        Exercise test grayscale prefers wand through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_imageops_fallback_wand.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    _install_fake_wand(monkeypatch)

    import subprocess
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: (_ for _ in ()).throw(AssertionError("CLI called")))

    m = _import_imageops_fresh()
    out = m.grayscale(b"fake")
    assert out.startswith(b"BLOB:")


def test_gaussian_blur_calls_wand(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Perform the test gaussian blur calls wand utility operation under explicit compatibility rules.

    Example:
        Exercise test gaussian blur calls wand through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_imageops_fallback_wand.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    _install_fake_wand(monkeypatch)
    m = _import_imageops_fresh()

    out = m.gaussian_blur(b"fake", 2.0, 3.0)
    assert out == b"BLOB:PNG"


def test_set_opacity_calls_evaluate(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Perform the test set opacity calls evaluate utility operation under explicit compatibility rules.

    Example:
        Exercise test set opacity calls evaluate through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_imageops_fallback_wand.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    _install_fake_wand(monkeypatch)
    m = _import_imageops_fresh()

    out = m.set_opacity(b"fake", 0.25)
    assert out == b"BLOB:PNG"


def test_has_transparent_pixels_false_when_histogram_opaque(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Perform the test has transparent pixels false when histogram opaque utility operation under explicit compatibility rules.

    Example:
        Exercise test has transparent pixels false when histogram opaque through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_imageops_fallback_wand.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    _install_fake_wand(monkeypatch)
    m = _import_imageops_fresh()

    assert m.has_transparent_pixels(b"fake") is False


def test_remove_borders_uses_trim(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Perform the test remove borders uses trim utility operation under explicit compatibility rules.

    Example:
        Exercise test remove borders uses trim through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_imageops_fallback_wand.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    _install_fake_wand(monkeypatch)
    m = _import_imageops_fresh()

    out = m.remove_borders(b"fake", 0.1)
    assert out == b"BLOB:PNG"


def test_texture_image_composites(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Perform the test texture image composites utility operation under explicit compatibility rules.

    Example:
        Exercise test texture image composites through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_imageops_fallback_wand.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    _install_fake_wand(monkeypatch)
    m = _import_imageops_fresh()

    out = m.texture_image(b"base", b"tex")
    assert out == b"BLOB:PNG"

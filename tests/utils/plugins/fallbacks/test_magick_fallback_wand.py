"""
Provide test magick fallback wand utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test magick fallback wand through a consuming regression::

        python -m pytest -q tests/utils/plugins/fallbacks/test_magick_fallback_wand.py
"""
from __future__ import annotations

import importlib
import sys
from types import ModuleType
from typing import Optional

import pytest


class FakeWandImage:
    """
    Provide the FakeWandImage utility contract with explicit state and cleanup behavior.

    Example:
        Exercise FakeWandImage through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_magick_fallback_wand.py
    """
    def __init__(self, *, filename: Optional[str] = None, blob: Optional[bytes] = None):
        """
        Initialize and validate the FakeWandImage state.

        Example:
            Exercise FakeWandImage.  init   through a consuming regression::

                python -m pytest -q tests/utils/plugins/fallbacks/test_magick_fallback_wand.py


        :param filename: Filename used for type inference or archive output.
        :param blob: Value supplied for blob under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.filename = filename
        self.blob = blob
        self.width = 123
        self.height = 456
        self.format = "PNG"
        self.resize_calls: list[tuple[int, int]] = []
        self.transform_calls: list[str] = []
        self.saved_to: list[str] = []

    def make_blob(self) -> bytes:
        """
        Perform the make blob utility operation under explicit compatibility rules.

        Example:
            Exercise FakeWandImage.make blob through a consuming regression::

                python -m pytest -q tests/utils/plugins/fallbacks/test_magick_fallback_wand.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return (f"BLOB:{self.format}".encode("utf-8"))

    def resize(self, w: int, h: int) -> None:
        """
        Perform the resize utility operation under explicit compatibility rules.

        Example:
            Exercise FakeWandImage.resize through a consuming regression::

                python -m pytest -q tests/utils/plugins/fallbacks/test_magick_fallback_wand.py


        :param w: Value supplied for w under the utility contract.
        :param h: Value supplied for h under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.resize_calls.append((w, h))
        self.width = w
        self.height = h

    def transform(self, *, resize: str) -> None:
        """
        Perform the transform utility operation under explicit compatibility rules.

        Example:
            Exercise FakeWandImage.transform through a consuming regression::

                python -m pytest -q tests/utils/plugins/fallbacks/test_magick_fallback_wand.py


        :param resize: Value supplied for resize under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.transform_calls.append(resize)

    def save(self, *, filename: str) -> None:
        """
        Perform the save utility operation under explicit compatibility rules.

        Example:
            Exercise FakeWandImage.save through a consuming regression::

                python -m pytest -q tests/utils/plugins/fallbacks/test_magick_fallback_wand.py


        :param filename: Filename used for type inference or archive output.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.saved_to.append(filename)

    def close(self) -> None:
        """
        Forward the close operation while preserving adapter ownership rules.

        Example:
            Exercise FakeWandImage.close through a consuming regression::

                python -m pytest -q tests/utils/plugins/fallbacks/test_magick_fallback_wand.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pass


def _install_fake_wand(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Perform the install fake wand utility operation under explicit compatibility rules.

    Example:
        Exercise  install fake wand through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_magick_fallback_wand.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    wand_mod = ModuleType("wand")
    wand_image_mod = ModuleType("wand.image")
    wand_image_mod.Image = FakeWandImage  # type: ignore[attr-defined]

    monkeypatch.setitem(sys.modules, "wand", wand_mod)
    monkeypatch.setitem(sys.modules, "wand.image", wand_image_mod)


def _import_magick_fresh() -> ModuleType:
    """
    Perform the import magick fresh utility operation under explicit compatibility rules.

    Example:
        Exercise  import magick fresh through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_magick_fallback_wand.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    import LiuXin_alpha.utils.plugins.fallbacks.magick as m

    return importlib.reload(m)


def test_identify_prefers_wand(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Perform the test identify prefers wand utility operation under explicit compatibility rules.

    Example:
        Exercise test identify prefers wand through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_magick_fallback_wand.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    _install_fake_wand(monkeypatch)

    import subprocess

    def boom(*args, **kwargs):  # type: ignore[no-untyped-def]
        """
        Perform the boom utility operation under explicit compatibility rules.

        Example:
            Exercise test identify prefers wand.boom through a consuming regression::

                python -m pytest -q tests/utils/plugins/fallbacks/test_magick_fallback_wand.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
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
    """
    Perform the test to bytes uses wand and sets format utility operation under explicit compatibility rules.

    Example:
        Exercise test to bytes uses wand and sets format through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_magick_fallback_wand.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    _install_fake_wand(monkeypatch)
    m = _import_magick_fresh()

    img = m.Image(b"fake")
    out = img.to_bytes(format="jpeg")
    assert out == b"BLOB:JPEG"


def test_resize_calls_wand_resize(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Perform the test resize calls wand resize utility operation under explicit compatibility rules.

    Example:
        Exercise test resize calls wand resize through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_magick_fallback_wand.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    _install_fake_wand(monkeypatch)
    m = _import_magick_fresh()

    img = m.Image(b"fake")
    img.resize(10, 20)
    info = img.identify()
    assert info["width"] == 10
    assert info["height"] == 20


def test_thumbnail_calls_wand_transform(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Perform the test thumbnail calls wand transform utility operation under explicit compatibility rules.

    Example:
        Exercise test thumbnail calls wand transform through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_magick_fallback_wand.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    _install_fake_wand(monkeypatch)
    m = _import_magick_fresh()

    img = m.Image(b"fake")
    img.identify()  # force wand instance
    assert img._wand_img is not None
    wand_obj = img._wand_img

    img.thumbnail(111, 222)
    assert wand_obj.transform_calls == ["111x222>"]


def test_save_calls_wand_save(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    """
    Perform the test save calls wand save utility operation under explicit compatibility rules.

    Example:
        Exercise test save calls wand save through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_magick_fallback_wand.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    _install_fake_wand(monkeypatch)
    m = _import_magick_fresh()

    out = tmp_path / "out.png"
    img = m.Image(b"fake")
    img.identify()  # force wand
    wand_obj = img._wand_img

    img.save(out, format="png")
    assert str(out) in wand_obj.saved_to


class _CP:
    """
    Provide the CP utility contract with explicit state and cleanup behavior.

    Example:
        Exercise  CP through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_magick_fallback_wand.py
    """
    def __init__(self, *, returncode: int, stdout: bytes = b"", stderr: bytes = b""):
        """
        Initialize and validate the CP state.

        Example:
            Exercise  CP.  init   through a consuming regression::

                python -m pytest -q tests/utils/plugins/fallbacks/test_magick_fallback_wand.py


        :param returncode: Value supplied for returncode under the utility contract.
        :param stdout: Value supplied for stdout under the utility contract.
        :param stderr: Value supplied for stderr under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


def test_cli_identify_used_when_no_wand(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    """
    Perform the test cli identify used when no wand utility operation under explicit compatibility rules.

    Example:
        Exercise test cli identify used when no wand through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_magick_fallback_wand.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    monkeypatch.setitem(sys.modules, "wand", None)
    monkeypatch.setitem(sys.modules, "wand.image", None)

    m = _import_magick_fresh()

    monkeypatch.setattr(m, "_magick_identify", lambda: "identify")

    def fake_run(cmd, stdout, stderr, check):  # type: ignore[no-untyped-def]
        """
        Perform the fake run utility operation under explicit compatibility rules.

        Example:
            Exercise test cli identify used when no wand.fake run through a consuming regression::

                python -m pytest -q tests/utils/plugins/fallbacks/test_magick_fallback_wand.py


        :param cmd: Value supplied for cmd under the utility contract.
        :param stdout: Value supplied for stdout under the utility contract.
        :param stderr: Value supplied for stderr under the utility contract.
        :param check: Value supplied for check under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
    """
    Perform the test cli convert used when no wand utility operation under explicit compatibility rules.

    Example:
        Exercise test cli convert used when no wand through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_magick_fallback_wand.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    monkeypatch.setitem(sys.modules, "wand", None)
    monkeypatch.setitem(sys.modules, "wand.image", None)

    m = _import_magick_fresh()

    monkeypatch.setattr(m, "_magick_convert", lambda: "magick")

    def fake_run(cmd, stdout, stderr, check):  # type: ignore[no-untyped-def]
        """
        Perform the fake run utility operation under explicit compatibility rules.

        Example:
            Exercise test cli convert used when no wand.fake run through a consuming regression::

                python -m pytest -q tests/utils/plugins/fallbacks/test_magick_fallback_wand.py


        :param cmd: Value supplied for cmd under the utility contract.
        :param stdout: Value supplied for stdout under the utility contract.
        :param stderr: Value supplied for stderr under the utility contract.
        :param check: Value supplied for check under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        assert cmd[0] == "magick"
        assert cmd[1] == "convert"
        assert cmd[-1] == "png:-"
        return _CP(returncode=0, stdout=b"OUT")

    import subprocess

    monkeypatch.setattr(subprocess, "run", fake_run)

    out = m.Image(b"fake").to_bytes(format="png")
    assert out == b"OUT"
